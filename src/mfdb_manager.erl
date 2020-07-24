%%----------------------------------------------------------------
%% Copyright (c) 2013-2016 Klarna AB
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%----------------------------------------------------------------
%% Support of FoundationDB:
%%  Copyright 2020 Leonard Boyce <leonard.boyce@lucidlayer.com>

-module(mfdb_manager).

-behaviour(gen_server).

-export([load_table/2,
         create_table/2,
         delete_table/1,
         load_if_exists/1,
         st/1,
         set_ttl/2]).

-export([read_info/3,
         write_info/3]).

-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% private
-export([db_conn_/0]).

-include("mfdb.hrl").

%%%% API for mnesia_fdb module %%%%%
create_table(Tab, Props) ->
    ?dbg("CREATE TABLE ~p~n", [Tab]),
    gen_server:call(?MODULE, {create_table, Tab, Props}).

delete_table(Tab0) ->
    ?dbg("DELETE TABLE ~p~n", [Tab0]),
    gen_server:call(?MODULE, {delete_table, Tab0}, infinity).

load_table(Tab0, _Default) ->
    Tab = tab_name_(Tab0),
    ?dbg("LOOKUP TABLE ~p~n", [Tab0]),
    try ets:lookup(?MODULE, Tab) of
        [#st{table_id = TableId, tab = Tab, info = Infos, ttl = TTL} = Rec] ->
            ok = reaper_start(Tab0, TableId, TTL),
            write_ets_infos_(Infos),
            Rec;
        [] ->
            badarg
    catch error:badarg ->
            badarg
    end.

load_if_exists(Tab0) ->
    Tab = tab_name_(Tab0),
    try ets:lookup(?MODULE, Tab) of
        [#st{table_id = TableId, tab = Tab, info = Infos, ttl = TTL}] ->
            ok = reaper_start(Tab0, TableId, TTL),
            write_ets_infos_(Infos),
            ok;
        [] ->
            gen_server:call(?MODULE, {load_table, Tab0})
    catch error:badarg ->
            badarg
    end.

st({Tab0, index, {Pos, _}}) ->
    Tab = tab_name_(Tab0),
    case ets:lookup(?MODULE, Tab) of
        [#st{index = Indexes} = St] ->
            #idx{table_id = ITabId, tab = ITab, mtab = IMtab} = element(Pos, Indexes),
            St#st{table_id = ITabId, tab = ITab, mtab = IMtab};
        _ ->
            badarg
    end;
st(Tab0) ->
    Tab = tab_name_(Tab0),
    case ets:lookup(?MODULE, Tab) of
        [#st{} = St] ->
            St;
        _ ->
            badarg
    end.

set_ttl(Tab0, TTL) when is_atom(Tab0) ->
    Tab = tab_name_(Tab0),
    PTabKey = <<"tbl_", Tab/binary, "_settings">>,
    case ets:lookup(?MODULE, Tab) of
        [#st{ttl = TTL}] ->
            %% do nothing, TTL has not changed
            ok;
        [#st{db = Db, table_id = TableId, ttl = undefined} = St] when is_integer(TTL) ->
            %% Add TTL to table and start the reaper
            NSt = St#st{ttl = TTL},
            ets:insert(?MODULE, {Tab, NSt}),
            ok = erlfdb:set(Db, PTabKey, term_to_binary(NSt)),
            ok = reaper_start(Tab0, TableId, TTL),
            ok;
        [#st{db = Db, table_id = TableId} = St] when TTL =:= undefined ->
            %% remove TTL from table, including removing all
            %% related TTL entries for existing records
            NSt = St#st{ttl = undefined},
            ets:insert(?MODULE, {Tab, NSt}),
            ok = erlfdb:set(Db, PTabKey, term_to_binary(NSt)),
            %% reaper shutdown removes all ttl related
            %% records then shuts itself down
            ok = reaper_shutdown(Tab0),
            ok;
        [#st{db = Db, table_id = TableId, ttl = OTTL} = St] when is_integer(OTTL) andalso is_integer(TTL) ->
            %% change the TTL setting in the reaper by restarting it
            NSt = St#st{ttl = undefined},
            ets:insert(?MODULE, {Tab, NSt}),
            ok = erlfdb:set(Db, PTabKey, term_to_binary(NSt)),
            ok = reaper_stop(Tab0),
            ok = reaper_start(Tab0, TableId, TTL),
            ok;
        _ ->
            badarg
    end.


write_info({Parent, index, {Pos, _}}, index_consistent, Value) ->
    ParentTab = tab_name_(Parent),
    case ets:lookup(?MODULE, ParentTab) of
        [#st{db = Db}] ->
            PTabKey = <<"tbl_", ParentTab/binary, "_settings">>,
            case erlfdb:get(Db, PTabKey) of
                not_found ->
                    ets:delete(?MODULE, ParentTab),
                    badarg;
                ParentBin ->
                    #st{index = Indexes0} = ParentSt = binary_to_term(ParentBin),
                    #idx{} = Idx = element(Pos, Indexes0),
                    NIdx = Idx#idx{index_consistent = Value},
                    NParentSt = ParentSt#st{index = setelement(Pos, Indexes0, NIdx)},
                    true = ets:insert(?MODULE, NParentSt),
                    ok = erlfdb:set(Db, PTabKey, term_to_binary(NParentSt))
            end;
        _ ->
            ?dbg("No state for ~p", [Parent]),
            badarg
    end;
write_info(Tab0, Key, Value) when is_atom(Tab0) ->
    Tab = tab_name_(Tab0),
    case ets:lookup(?MODULE, Tab) of
        [#st{info = Info0, db = Db} = St] ->
            TabKey = <<"tbl_", Tab/binary, "_settings">>,
            InfoKey = {Tab0, Key},
            Info = kr_(InfoKey, 1, Info0, {InfoKey, Value}),
            true = ets:insert(?MODULE, #info{k = InfoKey, v = Value}),
            ok = erlfdb:set(Db, TabKey, term_to_binary(St#st{info = Info})),
            ok;
        _ ->
            ?dbg("No state for ~p", [Tab]),
            badarg
    end.

read_info({Tab0, index, {Pos, _}}, index_consistent, Default) ->
    case ets:lookup(?MODULE, tab_name_(Tab0)) of
        [#st{index = Indexes}] ->
            case element(Pos, Indexes) of
                undefined ->
                    Default;
                #idx{index_consistent = Res} ->
                    Res
            end;
        [] ->
            Default
    end;
read_info(Tab0, Key, Default) when is_atom(Tab0) ->
    case ets:lookup(?MODULE, {Tab0, Key}) of
        [#info{v = Val}] ->
            Val;
        [] ->
            Default
    end.

write_ets_infos_([]) ->
    ok;
write_ets_infos_([{K, V} | Rest]) ->
    Rec = #info{k = K, v = V},
    ?dbg("Write info: ~p~n", [Rec]),
    ets:insert(?MODULE, Rec),
    write_ets_infos_(Rest).

%%%% Genserver
start_link() ->
    case ets:info(?MODULE, name) of
        undefined ->
            ets:new(?MODULE, [ordered_set, public, named_table, {keypos, 2}]);
        _ ->
            ok
    end,
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    process_flag(trap_exit, true),
    ok = init_connection_(),
    {ok, []}.

handle_call({delete_table, Tab0}, _From, S) ->
    R = delete_table_(Tab0),
    ?dbg("fdb manager received delete. replying with ~p~n", [R]),
    {reply, R, S};
handle_call({create_table, MTab, Props}, _From, S) ->
    R = create_table_(MTab, Props),
    ?dbg("fdb manager received create. replying with ~p~n", [R]),
    {reply, R, S};
handle_call({load_table, {Tab, index, _}}, _From, S) ->
    R = load_table_(Tab),
    ?dbg("fdb manager received load. replying with ~p~n", [R]),
    {reply, R, S};
handle_call({load_table, Tab}, _From, S) ->
    R = load_table_(Tab),
    ?dbg("fdb manager received load for ~p. replying with ~p~n", [Tab, R]),
    {reply, R, S};
handle_call(_, _, S) -> {reply, error, S}.
handle_cast(_, S)    -> {noreply, S}.
handle_info(_UNKNOWN, St) ->
    ?dbg("fdb manager received unknown message ~p~n", [_UNKNOWN]),
    {noreply, St}.
terminate(_, _)      -> ok.
code_change(_, S, _) -> {ok, S}.

init_connection_() ->
    case application:get_all_env(mnesia_fdb) of
        [] ->
            {error, missing_mnesia_fdb_settings};
        Settings ->
            Conn = #conn{
                      cluster       = proplists:get_value(cluster, Settings),
                      tls_key_path  = proplists:get_value(tls_key_path, Settings, undefined),
                      tls_cert_path = proplists:get_value(tls_cert_path, Settings, undefined),
                      tls_ca_path   = proplists:get_value(tls_ca_path, Settings, undefined)
                     },
            ets:insert(?MODULE, Conn),
            ok = load_fdb_nif_(Conn)
    end.

%% Load the NIF (try and ensure it's only loaded only once)
%% There must be a better way of checking if it's been initialized
load_fdb_nif_(#conn{tls_key_path = undefined}) ->
    try erlfdb_nif:init(), ok
    catch error:{reload, _} ->
            io:format("NIF already loaded~n"),
            ok
    end;
load_fdb_nif_(#conn{tls_key_path = KeyPath, tls_cert_path = CertPath, tls_ca_path = CAPath}) ->
    {ok, CABytes} = file:read_file(binary_to_list(CAPath)),
    FdbNetworkOptions = [{tls_ca_bytes, CABytes},
                         {tls_key_path, KeyPath},
                         {tls_cert_path, CertPath}],
    try erlfdb_nif:init(FdbNetworkOptions), ok
    catch
        error:{reload, _} ->
            io:format("NIF already loaded~n"),
            ok
    end.

db_conn_() ->
    [Conn0] = ets:lookup(?MODULE, conn),
    db_conn_(Conn0).

db_conn_(#conn{cluster = Cluster} = Conn) ->
    ?dbg("Opening cluster: ~p", [Conn]),
    ok = load_fdb_nif_(Conn),
    {erlfdb_database, _} = Db = erlfdb:open(Cluster),
    Db.

delete_table_({Parent0, index, {Pos, _}}) ->
    ?dbg("Delete index at pos ~p on ~p~n", [Pos, Parent0]),
    %% deleting an index
    Parent = tab_name_(Parent0),
    Db = db_conn_(),
    ParentTabKey = <<"tbl_", Parent/binary, "_settings">>,
    ParentBin = erlfdb:get(Db, ParentTabKey),
    #st{index = Indexes0} = ParentSt = binary_to_term(ParentBin),
    case element(Pos, Indexes0) of
        undefined ->
            ?dbg("Index at pos ~p on ~p is missing~n", [Pos, Parent0]),
            ok;
        #idx{table_id = IdxTabId} ->
            %% delete all index values from DB
            ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(IdxTabId, {?FDB_WC})),
            ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(IdxTabId, {?FDB_WC, ?FDB_WC})),
            ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(IdxTabId, {?FDB_WC, ?FDB_WC, ?FDB_WC})),
            NParentSt = ParentSt#st{index = setelement(Pos, Indexes0, undefined)},
            ok = erlfdb:set(Db, ParentTabKey, term_to_binary(NParentSt)),
            true = ets:insert(?MODULE, NParentSt),
            ok
    end;
delete_table_(Tab0) ->
    Tab = tab_name_(Tab0),
    Db = db_conn_(),
    reaper_stop(Tab0),
    [#st{index = Indexes0, table_id = TableId}] = ets:lookup(?MODULE, Tab),
    %% Remove indexes
    [clear_index(Db, IdxTabId) || #idx{table_id = IdxTabId} <- tuple_to_list(Indexes0)],
    ok = clear_table(Db, TableId),
    ok = erlfdb:clear(Db, <<"tbl_", Tab/binary>>),
    ok = erlfdb:clear(Db, <<"tbl_", Tab/binary, "_settings">>),
    ets:select_delete(?MODULE, [{#info{k = {Tab, ?FDB_WC}, _ = '_'}, [], [true]}]),
    ets:select_delete(?MODULE, [{#info{k = {Tab, ?FDB_WC, ?FDB_WC}, _ = '_'}, [], [true]}]),
    ets:delete(?MODULE, Tab),
    ok.

clear_index(Db, TableId) ->
    IdxDataStart = mfdb_lib:encode_key(TableId, {<<"di">>, ?FDB_WC}),
    IdxDataEnd = mfdb_lib:encode_key(TableId, {<<"di">>, ?FDB_END}),
    ok = erlfdb:clear_range(Db, IdxDataStart, IdxDataEnd),
    IdxStart = mfdb_lib:encode_key(TableId, {<<"i">>, ?FDB_WC}),
    IdxEnd = mfdb_lib:encode_key(TableId, {<<"i">>, ?FDB_END}),
    ok = erlfdb:clear_range(Db, IdxStart, IdxEnd),
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TableId, {?FDB_WC, ?FDB_WC})).

clear_table(Db, TableId) ->
    ok = erlfdb:clear(Db, mfdb_lib:encode_key(TableId, {<<"c">>})),
    ok = erlfdb:clear(Db, mfdb_lib:encode_key(TableId, {<<"s">>})),
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TableId, {?FDB_WC, ?FDB_WC, ?FDB_WC, ?FDB_WC})),
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TableId, {?FDB_WC, ?FDB_WC, ?FDB_WC})),
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TableId, {?FDB_WC, ?FDB_WC})),
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TableId, {?FDB_WC})).

mk_tab_(Db, TableId, Tab, MTab, Props) ->
    Alias = proplists:get_value(alias, Props, fdb_copies),
    RecordName = proplists:get_value(record_name, Props, Tab),
    {attributes, Attributes} = lists:keyfind(attributes, 1, Props),
    HcaRef = erlfdb_hca:create(<<TableId/binary, "_hca_ref">>),
    IdxTuple = list_to_tuple(lists:duplicate(length(Attributes) + 1, undefined)),
    Indexes = proplists:get_value(index, Props, []),
    UserProperties = proplists:get_value(user_properties, Props, []),
    TTL = proplists:get_value(ttl, UserProperties, undefined),
    St = #st{
            tab                  = Tab,
            mtab                 = MTab,
            alias                = Alias,
            record_name          = RecordName,
            attributes           = Attributes,
            index                = IdxTuple,
            ttl                  = TTL,
            db                   = Db,
            table_id             = TableId,
            hca_ref              = HcaRef,
            info                 = []
           },
    %% Now add the indexes to the state
    lists:foldl(
      fun(Idx, InSt) ->
              create_index_({MTab, index, Idx}, Db, InSt)
      end, St, Indexes).

tab_name_({Tab, index, {{Pos},_}}) ->
    <<(atom_to_binary(Tab, utf8))/binary, "-=", (atom_to_binary(Pos, utf8))/binary, "=-_ix">>;
tab_name_({Tab, index, {Pos,_}}) ->
    <<(atom_to_binary(Tab, utf8))/binary, "-", (integer_to_binary(Pos))/binary, "-_ix">>;
tab_name_({Tab, retainer, Name}) ->
    <<(atom_to_binary(Tab, utf8))/binary, "-", (retainername_(Name))/binary, "-_RET">>;
tab_name_(Tab) when is_atom(Tab) ->
    <<(atom_to_binary(Tab, utf8))/binary, "-_tab">>.

retainername_(Name) when is_atom(Name) ->
    atom_to_binary(Name, utf8);
retainername_(Name) when is_list(Name) ->
    try list_to_binary(Name)
    catch
        error:_ ->
            unicode:characters_to_binary(lists:flatten(io_lib:write(Name)))
    end;
retainername_(Name) ->
    unicode:characters_to_binary(lists:flatten(io_lib:write(Name))).

create_table_({Parent0, index, _} = MTab, _Props) ->
    %% Create an index on the parent table
    Db = db_conn_(),
    ParentName = tab_name_(Parent0),
    ParentTabKey = <<"tbl_", ParentName/binary, "_settings">>,
    case erlfdb:get(Db, ParentTabKey) of
        not_found ->
            badarg;
        ParentBin ->
            Parent = binary_to_term(ParentBin),
            NParent = create_index_(MTab, Db, Parent),
            true = ets:insert(?MODULE, NParent),
            ok = erlfdb:set(Db, ParentTabKey, term_to_binary(NParent))
    end;
create_table_(Tab0, Props) when is_atom(Tab0) ->
    %% This is the actual table
    Tab = tab_name_(Tab0),
    Db = db_conn_(),
    TabKey = <<"tbl_", Tab/binary, "_settings">>,
    case load_table_(Tab0) of
        {error, not_found} ->
            Hca = erlfdb_hca:create(<<"hca_table">>),
            TableId0 = erlfdb_hca:allocate(Hca, Db),
            ?dbg("MAKE TABLE ~p~n", [Tab]),
            #st{info = Infos, ttl = TTL} = Table0 = mk_tab_(Db, TableId0, Tab, Tab0, Props),
            ok = erlfdb:set(Db, TabKey, term_to_binary(Table0)),
            true = ets:insert(?MODULE, Table0),
            write_ets_infos_(Infos),
            ok = reaper_start(Tab0, TableId0, TTL),
            ok;
        ok ->
            ok
    end.

reaper_start(_TabName, _TableId, undefined) ->
    ok;
reaper_start(TabName, TableId, TTL) ->
    mfdb_reaper_sup:add_reaper(TabName, TableId, TTL).

reaper_stop(TabName) ->
    Reaper = list_to_atom("mfdb_reaper_" ++ atom_to_list(TabName)),
    case whereis(Reaper) of
        undefined ->
            ok;
        _RPid ->
            try gen_server:call(Reaper, stop, infinity) of
                ok ->
                 supervisor:delete_child(mfdb_reaper_sup, Reaper)
            catch
                _:_ ->
                    ok
            end
    end,
    ok.

reaper_shutdown(TabName) ->
    Reaper = list_to_atom("mfdb_reaper_" ++ atom_to_list(TabName)),
    case whereis(Reaper) of
        undefined ->
            ok;
        _RPid ->
            gen_server:cast(Reaper, shutdown)
    end.

create_index_({_, index, {Pos, _}} = Tab, Db, #st{index = Indexes0} = Parent) ->
    case element(Pos, Indexes0) of
        #idx{} ->
            %% Index already exists
            Parent;
        undefined ->
            %% index does not yet exist, so create
            Hca = erlfdb_hca:create(<<"hca_table">>),
            IdxTableId = erlfdb_hca:allocate(Hca, Db),
            IdxTabName = tab_name_(Tab),
            Index = #idx{mtab = Tab,
                         tab = IdxTabName,
                         table_id = IdxTableId,
                         pos = Pos,
                         index_consistent = false},
            Indexes = setelement(Pos, Indexes0, Index),
            Parent#st{index = Indexes}
    end.

load_table_(Tab0) ->
    Tab = tab_name_(Tab0),
    Db = db_conn_(),
    TabKey = <<"tbl_", Tab/binary, "_settings">>,
    case erlfdb:get(Db, TabKey) of
        not_found ->
            {error, not_found};
        Table0 ->
            #st{table_id = TableId, ttl = TTL, info = Infos} = Table1 = binary_to_term(Table0),
            Table2 = Table1#st{db = Db},
            ok = erlfdb:set(Db, TabKey, term_to_binary(Table2)),
            true = ets:insert(?MODULE, Table2),
            write_ets_infos_(Infos),
            reaper_start(Tab0, TableId, TTL),
            ok
    end.

kr_(K,X,L,KV) ->
    case lists:keyfind(K, X, L) of
        false ->
            [KV | L];
        _ ->
            lists:keyreplace(K,X,L,KV)
    end.
