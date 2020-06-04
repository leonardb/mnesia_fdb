-module(mnesia_fdb_error_handling).
-include_lib("eunit/include/eunit.hrl").

-export([run/0,
         run/3]).


run() ->
    setup(),
    %% run only one test for 'fatal', to save time.
    [run(Type, Op, L) || Type <- [set, bag],
                         Op <- [insert, delete],
                         L <- levels()]
        ++ [run(set, insert, fatal)].

run(Type, Op, Level) ->
    setup(),
    {ok, Tab} = create_tab(Type, Level),
    mnesia:dirty_write({Tab, a, 1}), % pre-existing data
    with_mock(Level, Op, Tab, fun() ->
                                      ?debugFmt("~ntry_write(~p, ~p, ~p)~n", [Op, Type, Tab]),
                                      WR = try_write(Op, Type, Tab),
                                      ?debugFmt("~ntry_write -> ~p~n", [WR]),
                                      R = expect_error(Level, Tab),
                                      ?debugFmt("~nexpect_error -> ~p~n", [R]),
                                      R
                              end).

levels() ->
    [debug, verbose, warning, error].

setup() ->
    mnesia:stop(),
    start_mnesia().

create_tab(Type, Level) ->
    TabName = tab_name(Type, Level),
    %% create error store before the table
    case ets:info(?MODULE) of
        undefined ->
            ?MODULE = ets:new(?MODULE, [bag, public, named_table]),
            ok;
        _ ->
            ok
    end,
    UserProps = user_props(Level),
    {atomic, ok} = mnesia:create_table(TabName, [{fdb, [node()]},
                                                 {user_properties, UserProps}]),
    {ok, TabName}.

tab_name(Type, Level) ->
    binary_to_atom(iolist_to_binary(
                     ["t" | [["_", atom_to_list(A)]
                             || A <- [?MODULE, Type, Level]]]), utf8).

user_props(Level) ->
    [{fdb_opts, [ {on_write_error, Level}
                , {on_write_error_store, ?MODULE} ]}].

start_mnesia() ->
    mnesia_fdb_tlib:start_mnesia(reset),
    ok.

with_mock(Level, Op, Tab, F) ->
    mnesia:subscribe(system),
    mnesia:set_debug_level(debug),
    meck:new(mnesia_fdb_lib, [passthrough]),
    meck:expect(mnesia_fdb_lib, mput, 3, {error, some_put_error}),
    meck:expect(mnesia_fdb_lib, mdelete, 2, {error,some_delete_error}),
    try {Level, Op, Tab, F()} of
        {_, _, _, ok} ->
            ok;
        Other ->
            io:fwrite("OTHER: ~p~n", [Other]),
            ok
    catch
        exit:{{aborted,_},_} ->
            Level = error,
            ok
    after
        mnesia:set_debug_level(none),
        mnesia:unsubscribe(system),
        meck:unload(mnesia_fdb_lib)
    end.

try_write(insert, set, Tab) ->
    mnesia:dirty_write({Tab, b, 2});
try_write(insert, bag, Tab) ->
    mnesia:dirty_write({Tab, a, 2});
try_write(update, _, Tab) ->
    mnesia:dirty_write({Tab, a, 1});
try_write(delete, _, Tab) ->
    mnesia:dirty_delete({Tab, a}).


expect_error(Level, Tab) ->
    Tag = rpt_tag(Level),
    receive
        {mnesia_system_event, {mnesia_fatal, Fmt, Args, _Core}} ->
            Tag = mnesia_fatal,
            io:fwrite("EVENT(~p, ~p):~n  ~s", [Tag, Tab, io_lib:fwrite(Fmt, Args)]),
            ok;
        {mnesia_system_event, {Tag, Fmt, Args}} ->
            io:fwrite("EVENT(~p, ~p):~n  ~s", [Tag, Tab, io_lib:fwrite(Fmt, Args)]),
            ok
    after 1000 ->
            error({expected_error, [Level, Tab]})

    end,
    %% Also verify that an error entry has been written into the error store.
    1 = ets:select_delete(?MODULE, [{{{Tab, '_'}, '_', '_'}, [], [true]}]),
    ok.

rpt_tag(fatal  ) -> mnesia_fatal;
rpt_tag(error  ) -> mnesia_error;
rpt_tag(warning) -> mnesia_warning;
rpt_tag(verbose) -> mnesia_info;
rpt_tag(debug  ) -> mnesia_info.
