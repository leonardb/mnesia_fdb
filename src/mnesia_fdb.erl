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
%%  Leonard Boyce <leonard.boyce@lucidlayer.com>
%%
%% @doc fdb storage backend for Mnesia.

%% Initialization: register() or register(Alias)
%% Usage: mnesia:create_table(Tab, [{fdb_copies, Nodes}, ...]).

-module(mnesia_fdb).

%% ----------------------------------------------------------------------------
%% BEHAVIOURS
%% ----------------------------------------------------------------------------

-behaviour(mnesia_backend_type).
-behaviour(gen_server).

%% ----------------------------------------------------------------------------
%% EXPORTS
%% ----------------------------------------------------------------------------

%%
%% CONVENIENCE API
%%

-export([register/0,
         register/1,
         default_alias/0]).

%%
%% DEBUG API
%%

-export([show_table/1,
         show_table/2,
         show_table/3,
         fold/6]).

%%
%% BACKEND CALLBACKS
%%

%% backend management
-export([init_backend/0,
         add_aliases/1,
         remove_aliases/1]).

%% schema level callbacks
-export([semantics/2,
         check_definition/4,
         create_table/3,
         load_table/4,
         close_table/2,
         sync_close_table/2,
         delete_table/2,
         info/3]).

%% table synch calls
-export([sender_init/4,
         sender_handle_info/5,
         receiver_first_message/4,
         receive_data/5,
         receive_done/4]).

%% low-level accessor callbacks.
-export([delete/3,
         first/2,
         fixtable/3,
         insert/3,
         last/2,
         lookup/3,
         match_delete/3,
         next/3,
         prev/3,
         repair_continuation/2,
         select/1,
         select/3,
         select/4,
         slot/3,
         update_counter/4]).

%% Index consistency
-export([index_is_consistent/3,
         is_index_consistent/2]).

%% record and key validation
-export([validate_key/6,
         validate_record/6]).

%% file extension callbacks
-export([real_suffixes/0,
         tmp_suffixes/0]).

%%
%% GEN SERVER CALLBACKS AND CALLS
%%

-export([start_proc/1,
         init/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         terminate/2,
         code_change/3]).

-export([ix_prefixes/3]).


%% ----------------------------------------------------------------------------
%% DEFINES
%% ----------------------------------------------------------------------------

%% Data and meta data (a.k.a. info) are stored in the same table.
%% This is a table of the first byte in data
%% 0    = before meta data
%% 1    = meta data
%% 2    = before data
%% >= 8 = data

-define(INFO_START, 0).
-define(INFO_TAG, 1).
-define(DATA_START, 2).
-define(BAG_CNT, 32).   % Number of bits used for bag object counter
-define(MAX_BAG, 16#FFFFFFFF).

%% enable debugging messages through mnesia:set_debug_level(debug)
-ifndef(MNESIA_FDB_NO_DBG).
-define(dbg(Fmt, Args),
        %% avoid evaluating Args if the message will be dropped anyway
        case mnesia_monitor:get_env(debug) of
            none -> ok;
            verbose -> ok;
            _ -> mnesia_lib:dbg_out("~p:~p: "++(Fmt)++"~n",[?MODULE,?LINE|Args])
        end).
-else.
-define(dbg(Fmt, Args), ok).
-endif.

-include("mnesia_fdb.hrl").

%% ----------------------------------------------------------------------------
%% CONVENIENCE API
%% ----------------------------------------------------------------------------

register() ->
    register(default_alias()).

register(Alias) ->
    Module = ?MODULE,
    case mnesia:add_backend_type(Alias, Module) of
        {atomic, ok} ->
            {ok, Alias};
        {aborted, {backend_type_already_exists, _}} ->
            {ok, Alias};
        {aborted, Reason} ->
            {error, Reason}
    end.

default_alias() ->
    fdb_copies.

%% ----------------------------------------------------------------------------
%% DEBUG API
%% ----------------------------------------------------------------------------

%% A debug function that shows the fdb table content
show_table(Tab) ->
    show_table(default_alias(), Tab).

show_table(Alias, Tab) ->
    show_table(Alias, Tab, 100).

show_table(Alias, Tab, Limit) ->
    OpFun = fun(#st{db = Db, table_id = TableId, type = Type}) ->
                    DPfx = ?DATA_PREFIX(Type),
                    StartKey = erlfdb_tuple:pack({DPfx}, TableId),
                    i_show_table(Db, TableId, StartKey, Limit)
            end,
    perform(Alias, Tab, OpFun).

%% PRIVATE

i_show_table(_, _, _, 0) ->
    ?dbg("iShow table",[]),
    {error, skipped_some};
i_show_table(Db, TableId, Start, Limit) ->
    ?dbg("iShow table: ~p ~p ~p",[Db, Start, Limit]),
    case erlfdb:get_range_startswith(Db, Start, [{limit, Limit}]) of
        [] ->
            ok;
        Rows ->
            [begin
                 K = mnesia_fdb_lib:decode_key(EncKey, TableId),
                 V = decode_val(Db, EncVal),
                 Val = setelement(2, V, K),
                 io:fwrite("~p~n", [Val])
             end || {EncKey, EncVal} <- Rows],
            ok
    end.


%% ----------------------------------------------------------------------------
%% BACKEND CALLBACKS
%% ----------------------------------------------------------------------------

%% backend management

init_backend() ->
    ?dbg("init backend",[]),
    application:ensure_all_started(mnesia_fdb),
    ok.

add_aliases(_Aliases) ->
    ?dbg("add_aliases(~p)",[_Aliases]),
    ok.

remove_aliases(_Aliases) ->
    ?dbg("remove_aliases(~p)",[_Aliases]),
    ok.

%% schema level callbacks

%% This function is used to determine what the plugin supports
%% semantics(Alias, storage)   ->
%%    ram_copies | disc_copies | disc_only_copies  (mandatory)
%% semantics(Alias, types)     ->
%%    [bag | set | ordered_set]                    (mandatory)
%% semantics(Alias, index_fun) ->
%%    fun(Alias, Tab, Pos, Obj) -> [IxValue]       (optional)
%% semantics(Alias, _) ->
%%    undefined.
%%
semantics(_Alias, storage) -> disc_only_copies;
semantics(_Alias, types  ) -> [set, ordered_set, bag];
semantics(_Alias, index_types) -> [ordered];
semantics(_Alias, index_fun) -> fun index_f/4;
semantics(_Alias, _) -> undefined.

is_index_consistent(Alias, {Tab, index, PosInfo}) ->
    ?dbg("is_index_consistent ~p ~p ",[Alias, {Tab, index, PosInfo}]),
    case info(Alias, Tab, {index_consistent, PosInfo}) of
        true -> true;
        _ -> false
    end.

index_is_consistent(Alias, {Tab, index, PosInfo}, Bool)
  when is_boolean(Bool) ->
    ?dbg("index_is_consistent ~p ~p ~p",[Alias, {Tab, index, PosInfo}, Bool]),
    write_info(Alias, Tab, {index_consistent, PosInfo}, Bool).


%% PRIVATE FUN
index_f(_Alias, _Tab, Pos, Obj) ->
    ?dbg("index_f ~p",[{_Alias, _Tab, Pos, Obj}]),
    [element(Pos, Obj)].

ix_prefixes(_Tab, _Pos, Obj) ->
    ?dbg("ix_prefixes ~p",[{_Tab, _Pos, Obj}]),
    lists:foldl(
      fun(V, Acc) when is_list(V) ->
              try Pfxs = prefixes(list_to_binary(V)),
                   Pfxs ++ Acc
              catch
                  error:_ ->
                      Acc
              end;
         (V, Acc) when is_binary(V) ->
              Pfxs = prefixes(V),
              Pfxs ++ Acc;
         (_, Acc) ->
              Acc
      end, [], tl(tuple_to_list(Obj))).

prefixes(<<P:3/binary, _/binary>>) ->
    [P];
prefixes(_) ->
    [].

%% For now, only verify that the type is set or ordered_set.
%% set is OK as ordered_set is a kind of set.
check_definition(Alias, Tab, Nodes, Props) ->
    ?dbg("~p: check_definition(~p, ~p, ~p, ~p);~n Trace: ~s~n",
         [self(), Alias, Tab, Nodes, Props, pp_stack()]),
    Id = {Alias, Nodes},
    try
        Props1 = lists:map(fun(E) -> check_definition_entry(Tab, Id, E) end, Props),
        {ok, Props1}
    catch
        throw:Error ->
            Error
    end.

check_definition_entry(_Tab, _Id, {type, T} = P) when T==set; T==ordered_set; T==bag ->
    ?dbg("~p: check_definition_entry(~p, ~p, ~p);~n Trace: ~s~n",
         [self(), _Tab, _Id, {type, T}, pp_stack()]),
    P;
check_definition_entry(Tab, Id, {type, T}) ->
    mnesia:abort({combine_error, Tab, [Id, {type, T}]});
check_definition_entry(_Tab, _Id, {user_properties, UPs} = P) ->
    FdbOpts = proplists:get_value(fdb_opts, UPs, []),
    OWE = proplists:get_value(on_write_error, FdbOpts, ?WRITE_ERR_DEFAULT),
    OWEStore = proplists:get_value(on_write_error_store, FdbOpts, ?WRITE_ERR_STORE_DEFAULT),
    case valid_mnesia_op(OWE) of
        true ->
            case OWEStore of
                undefined ->
                    P;
                V when is_atom(V) ->
                    P;
                V ->
                    throw({error, {invalid_configuration, {on_write_error_store, V}}})
            end;
        false ->
            throw({error, {invalid_configuration, {on_write_error, OWE}}})
    end;
check_definition_entry(_Tab, _Id, P) ->
    P.

%% -> ok | {error, exists}
create_table(_Alias, Tab, Props) ->
    mnesia_fdb_manager:create(Tab, Props).

load_table(Alias, Tab, _LoadReason, Opts) ->
    ?dbg("~p : load_table(~p, ~p, ~p, ~p)", [self(), Alias, Tab, _LoadReason, Opts]),
    FdbUserProps0 = proplists:get_value(user_properties, Opts, []),
    FdbUserProps = proplists:get_value(fdb_opts, FdbUserProps0, []),
    StorageProps0 = proplists:get_value(storage_properties, Opts, []),
    StorageProps = proplists:get_value(fdb, StorageProps0, FdbUserProps),
    NewSt = mnesia_fdb_manager:lookup(Tab, StorageProps),
    ProcName = proc_name(Alias, Tab),
    case whereis(ProcName) of
        undefined ->
            load_table_(NewSt);
        Pid ->
            gen_server:call(Pid, {load, NewSt}, infinity)
    end.

load_table_(#st{tab = Tab} = St) ->
    ShutdownTime = 120000,
    case mnesia_ext_sup:start_proc(
           Tab, ?MODULE, start_proc, [St],
           [{shutdown, ShutdownTime}]) of
        {ok, _Pid} ->
            ?dbg("Mnesia Proc started: ~p ~p~n", [Tab, _Pid]),
            ok;
        %% TODO: This reply is according to the manual, but we dont get it.
        {error, {already_started, _Pid}} ->
            %% TODO: Is it an error if the table already is
            %% loaded. This printout is triggered when running
            %% transform_table on a fdb_table that has indexing.
            ?dbg("ERR: table:~p already loaded pid:~p~n",
                 [Tab, _Pid]),
            ok;
        %% TODO: This reply is not according to the manual, but we get it.
        {error, {{already_started, _Pid}, _Stack}} ->
            %% TODO: Is it an error if the table already is
            %% loaded. This printout is triggered when running
            %% transform_table on a fdb_table that has indexing.
            ?dbg("ERR: table:~p already loaded pid:~p stack:~p~n",
                 [Tab, _Pid, _Stack]),
            ok;
        {error, Other} ->
            ?dbg("Mnesia Proc FAILED: ~p ~p ~p~n", [Tab, Other]),
            mnesia:abort(Other)
    end.

close_table(Alias, Tab) ->
    ?dbg("~p: close_table_(~p, ~p);~n Trace: ~s~n",
         [self(), Alias, Tab, pp_stack()]),
    case opt_call(Alias, Tab, close_table) of
        {error, noproc} ->
            ok;
        {ok, _} ->
            ok;
        _Other ->
            mnesia_ext_sup:stop_proc(Tab),
            ok
    end.

-ifndef(MNESIA_FDB_NO_DBG).
pp_stack() ->
    Trace = try throw(true)
            catch
                _:_ ->
                    case erlang:get_stacktrace() of
                        [_|T] -> T;
                        [] -> []
                    end
            end,
    pp_calls(10, Trace).

pp_calls(I, [{M,F,A,Pos} | T]) ->
    Spc = lists:duplicate(I, $\s),
    Pp = fun(Mx,Fx,Ax,Px) ->
                 [atom_to_list(Mx),":",atom_to_list(Fx),"/",integer_to_list(Ax),
                  pp_pos(Px)]
         end,
    [Pp(M,F,A,Pos)|[["\n",Spc,Pp(M1,F1,A1,P1)] || {M1,F1,A1,P1} <- T]].

pp_pos([]) -> "";
pp_pos(L) when is_integer(L) ->
    [" (", integer_to_list(L), ")"];
pp_pos([{file,_},{line,L}]) ->
    [" (", integer_to_list(L), ")"].
-endif.

sync_close_table(Alias, Tab) ->
    ?dbg("~p: sync_close_table(~p, ~p);~n Trace: ~s~n",
         [self(), Alias, Tab, pp_stack()]),
    close_table(Alias, Tab).

delete_table(Alias, Tab) ->
    ?dbg("~p: delete_table(~p, ~p);~n Trace: ~s~n",
         [self(), Alias, Tab, pp_stack()]),
    case opt_call(Alias, Tab, delete_table) of
        {error, noproc} ->
            do_delete_table(Tab);
        {ok, _} ->
            ok
    end.

do_delete_table(Tab) ->
    ok = mnesia_fdb_manager:delete(Tab).

info(_Alias, _Tab, memory) ->
    0;
info(_Alias, _Tab, size) ->
    0;
info(_Alias, _Tab, _Item) ->
    %% io:format("Info req: ~p ~p ~p~n", [_Alias, _Tab, _Item]),
    undefined.

write_info(Alias, Tab, Key, Value) ->
    call(Alias, Tab, {write_info, Key, Value}).

%% table sync calls

%% ===========================================================
%% Table sync protocol
%% Callbacks are
%% Sender side:
%%  1. sender_init(Alias, Tab, RemoteStorage, ReceiverPid) ->
%%        {standard, InitFun, ChunkFun} | {InitFun, ChunkFun} when
%%        InitFun :: fun() -> {Recs, Cont} | '$end_of_table'
%%        ChunkFun :: fun(Cont) -> {Recs, Cont1} | '$end_of_table'
%%
%%       If {standard, I, C} is returned, the standard init message will be
%%       sent to the receiver. Matching on RemoteStorage can reveal if a
%%       different protocol can be used.
%%
%%  2. InitFun() is called
%%  3a. ChunkFun(Cont) is called repeatedly until done
%%  3b. sender_handle_info(Msg, Alias, Tab, ReceiverPid, Cont) ->
%%        {ChunkFun, NewCont}
%%
%% Receiver side:
%% 1. receiver_first_message(SenderPid, Msg, Alias, Tab) ->
%%        {Size::integer(), State}
%% 2. receive_data(Data, Alias, Tab, _Sender, State) ->
%%        {more, NewState} | {{more, Msg}, NewState}
%% 3. receive_done(_Alias, _Tab, _Sender, _State) ->
%%        ok
%%
%% The receiver can communicate with the Sender by returning
%% {{more, Msg}, St} from receive_data/4. The sender will be called through
%% sender_handle_info(Msg, ...), where it can adjust its ChunkFun and
%% Continuation. Note that the message from the receiver is sent once the
%% receive_data/4 function returns. This is slightly different from the
%% normal mnesia table synch, where the receiver acks immediately upon
%% reception of a new chunk, then processes the data.
%%

sender_init(Alias, Tab, _RemoteStorage, _Pid) ->
    %% Need to send a message to the receiver. It will be handled in
    %% receiver_first_message/4 below. There could be a volley of messages...
    {standard,
     fun() ->
             select(Alias, Tab, [{'_',[],['$_']}], 100)
     end,
     chunk_fun()}.

sender_handle_info(_Msg, _Alias, _Tab, _ReceiverPid, Cont) ->
    ?dbg("~p : sender_handle_info(~p, ~p, ~p, ~p, ~p)", [self(),_Msg, _Alias, _Tab, _ReceiverPid, Cont]),
    %% ignore - we don't expect any message from the receiver
    {chunk_fun(), Cont}.

receiver_first_message(_Pid, {first, Size} = _Msg, _Alias, _Tab) ->
    ?dbg("~p : receiver_first_message(~p, ~p, ~p, ~p)", [self(), _Pid, _Msg, _Alias, _Tab]),
    {Size, _State = []}.

receive_data(Data, Alias, Tab, _Sender, State) ->
    ?dbg("~p : receive_data(~p, ~p, ~p, ~p, ~p)", [self(), Data, Alias, Tab, _Sender, State]),
    [insert(Alias, Tab, Obj) || Obj <- Data],
    {more, State}.

receive_done(_Alias, _Tab, _Sender, _State) ->
    ok.

%% End of table synch protocol
%% ===========================================================

%% PRIVATE

chunk_fun() ->
    fun(Cont) ->
            select(Cont)
    end.

%% low-level accessor callbacks.

delete(Alias, Tab, Key) ->
    ?dbg("~p delete(~p, ~p, ~p)", [self(), Alias, Tab, Key]),
    opt_call(Alias, Tab, {delete, Key}),
    ok.

first(Alias, Tab) ->
    ?dbg("~p : first(~p, ~p)", [self(), Alias, Tab]),
    OpFun = fun(#st{db = Db, table_id = TableId, type = Type}) ->
                    DPfx = ?DATA_PREFIX(Type),
                    Key = erlfdb_tuple:pack({DPfx}, TableId),
                    Reverse = 0,
                    Fun = fun(Tx) ->
                                  erlfdb:wait(
                                    erlfdb_nif:transaction_get_range(
                                      Tx,
                                      erlfdb_key:to_selector(Key), %% Start
                                      erlfdb_key:to_selector(erlfdb_key:strinc(Key)), %% End
                                      1, %% limit
                                      0,
                                      want_all,
                                      1,
                                      false,
                                      Reverse))
                          end,
                    case erlfdb:transactional(Db, Fun) of
                        {[{EncKey, _Val}],_,_} ->
                            ResKey = mnesia_fdb_lib:decode_key(EncKey, TableId),
                            ResKey;
                        _Other ->
                            '$end_of_table'
                    end
            end,
    perform(Alias, Tab, OpFun).

%% Not relevant for an ordered_set
fixtable(_Alias, _Tab, _Bool) ->
    ?dbg("~p : fixtable(~p, ~p, ~p)", [self(),_Alias, _Tab, _Bool]),
    true.

%% To save storage space, we avoid storing the key twice. We replace the key
%% in the record with []. It has to be put back in lookup/3.
insert(Alias, Tab, Obj) ->
    ?dbg("~p : insert(~p, ~p, ~p)", [self(), Alias, Tab, Obj]),
    Pos = keypos(Tab),
    Key = element(Pos, Obj),
    Val = setelement(Pos, Obj, []),
    call(Alias, Tab, {insert, Key, Val}).

last(Alias, Tab) ->
    ?dbg("~p : last(~p, ~p)", [self(), Alias, Tab]),
    OpFun = fun(#st{db= Db, table_id = TableId, type = Type}) ->
                    DPfx = ?DATA_PREFIX(Type),
                    Key = erlfdb_tuple:pack({DPfx}, TableId),
                    Reverse = 1,
                    Fun = fun(Tx) ->
                                  erlfdb:wait(
                                    erlfdb_nif:transaction_get_range(
                                      Tx,
                                      erlfdb_key:to_selector(Key), %% Start
                                      erlfdb_key:to_selector(erlfdb_key:strinc(Key)), %% End
                                      1, %% limit
                                      0,
                                      want_all,
                                      1,
                                      false,
                                      Reverse))
                          end,
                    case erlfdb:transactional(Db, Fun) of
                        {[{EncKey, _EncVal}],_,_} ->
                            mnesia_fdb_lib:decode_key(EncKey, TableId);
                        _ ->
                            '$end_of_table'
                    end
            end,
    perform(Alias, Tab, OpFun).

%% Since we replace the key with [] in the record, we have to put it back
%% into the found record.
lookup(Alias, Tab, Key) ->
    ?dbg("~p : lookup(~p, ~p, ~p)", [self(), Alias, Tab, Key]),
    {Db, TableId, Type} = call(Alias, Tab, get_ref),
    case Type of
        bag ->
            lookup_bag(Db, TableId, Key, keypos(Tab));
        _ ->
            EncKey = erlfdb_tuple:pack({?DATA_PREFIX(Type), sext:encode(Key)}, TableId),
            case erlfdb:get(Db, EncKey) of
                not_found ->
                    [];
                EVal ->
                    [setelement(keypos(Tab), decode_val(Db, EVal), Key)]
            end
    end.

lookup_bag(Db, TableId, Key, KeyPos) ->
    EncKey = erlfdb_tuple:pack({?DATA_PREFIX(bag), sext:encode(Key)}, TableId),
    case erlfdb:get_range_startswith(Db, EncKey) of
        not_found ->
            [];
        [] ->
            [];
        Rows ->
            [begin
                 K = mnesia_fdb_lib:decode_key(EKey, TableId),
                 V = decode_val(Db, EVal),
                 setelement(KeyPos, V, K)
             end || {EKey, EVal} <- Rows]
    end.

match_delete(Alias, Tab, Pat) when is_atom(Pat) ->
    ?dbg("~p : match_delete(~p, ~p, ~p)", [self(), Alias, Tab, Pat]),
    %%do_match_delete(Alias, Tab, '_'),
    case is_wild(Pat) of
        true ->
            call(Alias, Tab, clear_table),
            ok;
        false ->
            %% can this happen??
            ?dbg("is_wild failed on Pat ~p", [Pat]),
            error(badarg)
    end;
match_delete(Alias, Tab, Pat) when is_tuple(Pat) ->
    KP = keypos(Tab),
    Key = element(KP, Pat),
    case is_wild(Key) of
        true ->
            call(Alias, Tab, clear_table);
        false ->
            call(Alias, Tab, {match_delete, Pat})
    end,
    ok.

next(Alias, Tab, Key) ->
    ?dbg("~p : next(~p, ~p, ~p)", [self(), Alias, Tab, Key]),
    OpFun = fun(#st{db = Db, table_id = TableId, type = Type}) ->
                    DPfx = ?DATA_PREFIX(Type),
                    EncKey = erlfdb_tuple:pack({DPfx, sext:encode(Key)}, TableId),
                    EndKey = erlfdb_key:strinc(erlfdb_tuple:pack({DPfx}, TableId)),
                    Reverse = 0,
                    %% io:format("EncKey = ~p, EndKey = ~p.~n", [EncKey, EndKey]),
                    Fun = fun(Tx) ->
                                  erlfdb:wait(
                                    erlfdb_nif:transaction_get_range(
                                      Tx,
                                      erlfdb_key:to_selector(EncKey), %% Start
                                      erlfdb_key:to_selector(EndKey), %% End (full range)
                                      2, %% limit, since we get back the Start key
                                      0,
                                      want_all,
                                      1,
                                      false,
                                      Reverse))
                          end,
                    case erlfdb:transactional(Db, Fun) of
                        {[{EncKey, _}, %% Start KeyVal
                          {OutEncKey, _Val}],_,_} ->
                            mnesia_fdb_lib:decode_key(OutEncKey, TableId);
                        {[{EncKey, _Val}], _, _} ->
                            '$end_of_table';
                        {[{OutEncKey, _Val} | _], _, _} ->
                            mnesia_fdb_lib:decode_key(OutEncKey, TableId);
                        _ ->
                            '$end_of_table'
                    end
            end,
    perform(Alias, Tab, OpFun).

prev(Alias, Tab, Key) ->
    ?dbg("~p : prev(~p, ~p, ~p)", [self(), Alias, Tab, Key]),
    %%{Db, TableId, Type} = get_ref(Alias, Tab),
    OpFun = fun(#st{db = Db, table_id = TableId, type = Type}) ->
                    DPfx = ?DATA_PREFIX(Type),
                    EncKey   = erlfdb_tuple:pack({DPfx, sext:encode(Key)}, TableId),
                    StartKey = erlfdb_tuple:pack({DPfx}, TableId),
                    EndKey   = erlfdb_key:strinc(EncKey),
                    Reply = try erlfdb:get_range(Db, StartKey, EndKey, [{reverse, true}, {limit, 2}]) of
                                not_found ->
                                    '$end_of_table';
                                [] ->
                                    '$end_of_table';
                                [{OutEncKey, _}] when OutEncKey =:= EncKey ->
                                    '$end_of_table';
                                [{EncKey, _}, {PrevEncKey, _}] ->
                                    Res = mnesia_fdb_lib:decode_key(PrevEncKey, TableId),
                                    Res;
                                [{PrevEncKey, _}, _] ->
                                    %% EncKey does not exist
                                    Res = mnesia_fdb_lib:decode_key(PrevEncKey, TableId),
                                    Res;
                                [{OutEncKey, _} | _] ->
                                    Res = mnesia_fdb_lib:decode_key(OutEncKey, TableId),
                                    Res
                            catch
                                E:M ->
                                    ?dbg("~p : ERROR in prev/3 ~p ~p ~p -> ~p ~p ", [self(), EncKey, StartKey, EndKey, E, M]),
                                    mnesia:abort(badarg)
                            end,
                    Reply
            end,
    perform(Alias, Tab, OpFun).

repair_continuation(Cont, _Ms) ->
    ?dbg("~p : repair_continuation(~p, ~p)", [self(), Cont, _Ms]),
    Cont.

select(Cont) ->
    ?dbg("~p : select(~p)", [self(), Cont]),
    %% Handle {ModOrAlias, Cont} wrappers for backwards compatibility with
    %% older versions of mnesia_ext (before OTP 20).
    case Cont of
        {_, '$end_of_table'} -> '$end_of_table';
        {_, Cont1}           -> Cont1();
        '$end_of_table'      -> '$end_of_table';
        _                    -> Cont()
    end.

select(Alias, Tab, Ms) ->
    ?dbg("select( ~p, ~p, ~p)~n", [Alias, Tab, Ms]),
    case select(Alias, Tab, Ms, infinity) of
        {Res, '$end_of_table'} ->
            Res;
        '$end_of_table' ->
            '$end_of_table'
    end.

select(Alias, Tab, Ms, Limit) when Limit =:= infinity orelse is_integer(Limit) ->
    ?dbg("~p : select(~p, ~p, ~p, ~p)", [self(),Alias, Tab, Ms, Limit]),
    %% {Db, TableId, Type} = get_ref(Alias, Tab),
    OpFun = fun(#st{db = Db, table_id = TableId, type = Type}) ->
                    do_select(Db, TableId, Tab, Type, Ms, Limit)
            end,
    perform(Alias, Tab, OpFun).

slot(_A, _B, _C) ->
    ?dbg("~p : slot(~p, ~p, ~p)", [self(),_A, _B, _C]),
    mnesia:abort(slot_not_supported).

update_counter(Alias, Tab, C, Val) when is_integer(Val) ->
    ?dbg("~p : update_counter(~p, ~p, ~p)", [self(),Alias, Tab, C, Val]),
    call(Alias, Tab, {update_counter, C, Val}).

%% server-side part
do_update_counter(_Key, _Incr, #st{type = bag}) ->
    mnesia:abort({error, counter_not_supported_for_bag});
do_update_counter(Key, Incr, #st{db = Db, table_id = TableId}) when is_integer(Incr) ->
    %% Atomic counter increment
    %% Mnesia counters are dirty only, and cannot go below zero
    %%  dirty_update_counter({Tab, Key}, Incr) -> NewVal | exit({aborted, Reason})
    %%    Calls mnesia:dirty_update_counter(Tab, Key, Incr).
    %%
    %%  dirty_update_counter(Tab, Key, Incr) -> NewVal | exit({aborted, Reason})
    %%    Mnesia has no special counter records. However, records of
    %%    the form {Tab, Key, Integer} can be used as (possibly disc-resident)
    %%    counters when Tab is a set. This function updates a counter with a positive
    %%    or negative number. However, counters can never become less than zero.
    %%    There are two significant differences between this function and the action
    %%    of first reading the record, performing the arithmetics, and then writing the record:
    %%
    %%    It is much more efficient. mnesia:dirty_update_counter/3 is performed as an
    %%    atomic operation although it is not protected by a transaction.
    %%    If two processes perform mnesia:dirty_update_counter/3 simultaneously, both
    %%    updates take effect without the risk of losing one of the updates. The new
    %%    value NewVal of the counter is returned.
    %%
    %%    If Key do not exists, a new record is created with value Incr if it is larger
    %%    than 0, otherwise it is set to 0.
    %%
    %% This implementation tries to follow the same pattern.
    %% Increment the counter and then read the new value
    %%  - if the new value is < 0 (not allowed), abort
    %%  - if the new value is > 0, return the new value
    %% The exception:
    %%   if the counter does not exist and is created with a Incr < 0
    %%    we abort instead of creating a counter with value of '0'
    EncKey = erlfdb_tuple:pack({<<"c">>, sext:encode(Key)}, TableId),
    Tx = erlfdb:create_transaction(Db),
    ok = erlfdb:wait(erlfdb:add(Tx, EncKey, Incr)),
    case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
        not_found ->
            ok = erlfdb:wait(erlfdb:reset(Tx)),
            %% Really should never hit this case
            mnesia:abort(missing_counter);
        <<NewVal:64/unsigned-little-integer>> when NewVal < 0 ->
            %% Value for mnesia counter cannot be < 0
            ok = erlfdb:wait(erlfdb:reset(Tx)),
            mnesia:abort(counter_less_than_zero);
        <<NewVal:64/unsigned-little-integer>> ->
            ok = erlfdb:wait(erlfdb:commit(Tx)),
            %% Counter incremented, return new value
            %% This could very well not be what was expected
            %% since the counter may have been incremented
            %% by other transactions
            NewVal
    end.

%% PRIVATE

%% record and key validation
validate_key(_Alias, _Tab, RecName, Arity, Type, _Key) ->
    ?dbg("~p : validate_key(~p, ~p, ~p, ~p, ~p, ~p)", [self(),_Alias, _Tab, RecName, Arity, Type, _Key]),
    {RecName, Arity, Type}.

validate_record(_Alias, _Tab, RecName, Arity, Type, _Obj) ->
    ?dbg("~p : validate_record(~p, ~p, ~p, ~p, ~p, ~p)", [self(),_Alias, _Tab, RecName, Arity, Type, _Obj]),
    {RecName, Arity, Type}.

%% file extension callbacks

%% Extensions for files that are permanent. Needs to be cleaned up
%% e.g. at deleting the schema.
real_suffixes() ->
    [].

%% Extensions for temporary files. Can be cleaned up when mnesia
%% cleans up other temporary files.
tmp_suffixes() ->
    [].


%% ----------------------------------------------------------------------------
%% GEN SERVER CALLBACKS AND CALLS
%% ----------------------------------------------------------------------------

start_proc(#st{alias = Alias, tab = Tab} = FdbSt) ->
    ProcName = proc_name(Alias, Tab),
    gen_server:start_link({local, ProcName}, ?MODULE,
                          FdbSt, []).

init(FdbSt) ->
    process_flag(trap_exit, true),
    ?dbg("init with state:~n~p~n)", [FdbSt]),
    {ok, FdbSt}.

handle_call({load, #st{tab = Tab} = NewSt}, _From, #st{tab = Tab}) ->
    %% Replace the state
    {reply, ok, NewSt};
handle_call({perform, OpFun}, _From, #st{} = St) ->
    Reply = OpFun(St),
    {reply, Reply, St};
handle_call(get_ref, _From, #st{db = Db, table_id = TableId, type = Type} = St) ->
    {reply, {Db, TableId, Type}, St};
handle_call({write_info, _Key, _Value}, _From, #st{} = St) ->
    %% noop
    {reply, ok, St};
handle_call({update_counter, Key, Incr}, _From, #st{} = St) ->
    {reply, do_update_counter(Key, Incr, St), St};
handle_call({insert, Key, Val}, _From, St) ->
    Res = do_insert(Key, Val, St),
    ?dbg("handle_call({insert, ~p, ~p}, _From, ~p) -> ~p", [Key, Val, St, Res]),
    {reply, Res, St};
handle_call({delete, Key}, _From, St) ->
    Res = do_delete(Key, St),
    {reply, Res, St};
handle_call(clear_table, _From, #st{tab = Tab, db = Db, table_id = TableId} = St) ->
    ?dbg("Attempting clear_table(~p)~n", [Tab]),
    %% Remove all keys with matching table prefix
    %% @TODO need to verify if the 'parts' of large values are also removed
    erlfdb:clear_range_startswith(Db, erlfdb_tuple:pack({}, TableId)),
    {reply, ok, St};
handle_call({match_delete, Pat}, _From, #st{} = St) ->
    Res = do_match_delete(Pat, St),
    {reply, Res, St};
handle_call(close_table, _From, #st{} = St) ->
    {reply, ok, St#st{db = undefined}};
handle_call(delete_table, _From, #st{tab = Tab} = St) ->
    ?dbg("Delete table ~p~n", [Tab]),
    ok = mnesia_fdb_manager:delete(Tab),
    {stop, normal, ok, St#st{db = undefined}}.

handle_cast(_, St) ->
    {noreply, St}.

handle_info({'EXIT', _, _} = _EXIT, St) ->
    ?dbg("fdb owner received ~p~n", [_EXIT]),
    {noreply, St};
handle_info(_UNKNOWN, St) ->
    ?dbg("fdb owner received unknown message ~p~n", [_UNKNOWN]),
    {noreply, St}.

code_change(_FromVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, #st{}) ->
    ok.


%% ----------------------------------------------------------------------------
%% GEN SERVER PRIVATE
%% ----------------------------------------------------------------------------

opt_call(Alias, Tab, Req) ->
    ProcName = proc_name(Alias, Tab),
    case whereis(ProcName) of
        undefined ->
            ?dbg("proc_name(~p, ~p): ~p; NO PROCESS~n",
                 [Alias, Tab, ProcName]),
            {error, noproc};
        Pid when is_pid(Pid) ->
            ?dbg("proc_name(~p, ~p): ~p; Pid = ~p~n",
                 [Alias, Tab, ProcName, Pid]),
            {ok, gen_server:call(Pid, Req, infinity)}
    end.

call(Alias, Tab, Req) ->
    ProcName = proc_name(Alias, Tab),
    try gen_server:call(ProcName, Req, infinity) of
        badarg ->
            ?dbg("~p CRITICAL ~p not running", [self(), Tab]),
            mnesia:abort(missing_process);
        {abort, _} = Err ->
            ?dbg("~p CRITICAL ~p abort", [self(), Err]),
            mnesia:abort(Err);
        Reply ->
            Reply
    catch
        exit:{noproc, _} ->
            ?dbg("~p CRITICAL ~p not running", [self(), Tab]),
            mnesia:abort(missing_process);
        E:M ->
            ?dbg("~p : ~p Req ~p Failed: ~p", [self(), Tab, Req, {E, M}]),
            mnesia:abort(missing_process)
    end.

%% server-side end of insert/3.
do_insert(K, V, #st{} = St) ->
    return_catch(fun() -> db_put(St, K, V) end).

%% server-side part
do_delete(Key, #st{} = St) ->
    return_catch(fun() -> db_delete(St, Key) end).

do_match_delete(Pat, #st{table_id = TableId, tab = Tab, type = Type} = St) ->
    MS = [{Pat,[],['$_']}],
    Keypat = keypat(MS, keypos(Tab)),
    CompiledMS = ets:match_spec_compile(MS),
    DPfx = ?DATA_PREFIX(Type),
    StartKey = erlfdb_tuple:pack({DPfx, Keypat}, TableId),
    EndKey = erlfdb_key:strinc(erlfdb_tuple:pack({DPfx}, TableId)),
    do_fold_delete_(St, StartKey, EndKey, DPfx, Keypat, CompiledMS).

do_fold_delete_(#st{db = Db, table_id = TableId} = St,
                StartKey, EndKey, DPfx, Keypat, CompiledMS) ->
    Tx = erlfdb:create_transaction(Db),
    case erlfdb:wait(erlfdb:get_range(Tx, StartKey, EndKey, [{limit, 100}])) of
        not_found ->
            ok = erlfdb:wait(erlfdb:reset(Tx)),
            ok;
        [] ->
            ok = erlfdb:wait(erlfdb:reset(Tx)),
            [];
        Recs ->
            case do_fold_delete2_(St, Recs, Tx, Keypat, CompiledMS, undefined) of
                undefined ->
                    ok = erlfdb:wait(erlfdb:reset(Tx)),
                    '$end_of_table';
                LastKey ->
                    ok = erlfdb:wait(erlfdb:commit(Tx)),
                    NStartKey = erlfdb_tuple:pack({DPfx, LastKey}, TableId),
                    case NStartKey =:= StartKey of
                        true ->
                            '$end_of_table';
                        false ->
                            do_fold_delete_(St, NStartKey, EndKey, DPfx, Keypat, CompiledMS)
                    end
            end
    end.

do_fold_delete2_(_St, [], _Tx, _Keypat, _CompiledMS, LastKey) ->
    LastKey;
do_fold_delete2_(#st{table_id = TableId, tab = Tab} = St, [{EncKey, EncV} | Rest], Tx, Keypat, MS, _LK) ->
    K = mnesia_fdb_lib:decode_key(EncKey, TableId),
    case is_prefix(Keypat, K) of
        true ->
            Rec = setelement(keypos(Tab), decode_val(Tx, EncV), decode_key(K)),
            case ets:match_spec_run([Rec], MS) of
                [] ->
                    do_fold_delete_(St, Rest, Tx, Keypat, MS, K);
                [_Match] ->
                    %% Keep the delete op within the same transaction
                    ok = mnesia_fdb_lib:delete(St#st{db = Tx}, K),
                    do_fold_delete2_(St, Rest, Tx, Keypat, MS, K)
            end;
        false ->
            do_fold_delete2_(St, Rest, Tx, Keypat, MS, K)
    end.

proc_name(_Alias, Tab) ->
    list_to_atom("mnesia_ext_proc_" ++ tabname(Tab)).


%% ----------------------------------------------------------------------------
%% PRIVATE SELECT MACHINERY
%% ----------------------------------------------------------------------------

do_select(Db, TableId, Tab, Type, MS, Limit) ->
    do_select(Db, TableId, Tab, Type, MS, false, Limit).

do_select(Db, TableId, Tab, Type, MS, AccKeys, Limit) when is_boolean(AccKeys) ->
    Keypat = keypat(MS, 2),
    DPfx = ?DATA_PREFIX(Type),
    PackedKeypat = case Keypat of
                       <<>> ->
                           erlfdb_tuple:pack({DPfx}, TableId);
                       Keypat ->
                           erlfdb_tuple:pack({DPfx, Keypat}, TableId)
                   end,
    Sel = #sel{tab = Tab,
               table_id = TableId,
               db = Db,
               keypat = PackedKeypat,
               ms = MS,
               compiled_ms = ets:match_spec_compile(MS),
               key_only = needs_key_only(MS),
               limit = Limit},
    do_select(Db, Sel, Type, AccKeys, []).

do_select(Db, #sel{keypat = FirstKey,
                   lastval = LastVal,
                   table_id = TableId,
                   tab = Tab,
                   compiled_ms = MS,
                   limit = Limit} = Sel,
          Type, AccKeys, Acc) ->
    case do_select_(Db, TableId, Type, Tab, FirstKey, LastVal, AccKeys, Limit, MS, Acc) of
        {error, _} = E ->
            mnesia:abort(E);
        {FirstKey, _, NAcc} ->
            {lists:reverse(NAcc), '$end_of_table'};
        {NFirstKey, NLastVal, NAcc} ->
            do_select(Db, Sel#sel{keypat = NFirstKey, lastval = NLastVal}, Type, AccKeys, NAcc)
    end.

do_select_(Db, TableId, Type, Tab, StartKey, LastVal, AccKeys, Limit, MS, Acc) ->
    Fun = fun({EncKey, _EncVal}, {LastKey, _LastEncVal, _InnerAcc} = Next)
                when Type =/= bag andalso EncKey =:= LastKey ->
                  Next;
             ({EncKey, EncVal}, {LastKey, LastEncVal, _InnerAcc} = Next)
                when Type =:= bag andalso EncKey =:= LastKey andalso EncVal =:= LastEncVal ->
                  Next;
             ({EncKey, EncVal}, {_LastKey, _LastEncVal, InnerAcc}) ->
                  Key = mnesia_fdb_lib:decode_key(EncKey, TableId),
                  Val = decode_val(Db, EncVal),
                  Rec = setelement(keypos(Tab), Val, Key),
                  case ets:match_spec_run([Rec], MS) of
                      [] ->
                          {EncKey, EncVal, InnerAcc};
                      [Match] when AccKeys =:= true ->
                          {EncKey, EncVal, [{Key, Match} | InnerAcc]};
                      [Match] when AccKeys =:= false ->
                          {EncKey, EncVal, [Match | InnerAcc]}
                  end
          end,
    EndKey = erlfdb_key:strinc(StartKey),
    LimitArgs = case Limit of
                    infinity -> [];
                    Limit -> [{limit, Limit}]
                end,
    erlfdb:fold_range(Db, StartKey, EndKey, Fun, {StartKey, LastVal, Acc}, LimitArgs).

needs_key_only([{HP,_,Body}]) ->
    BodyVars = lists:flatmap(fun extract_vars/1, Body),
    %% Note that we express the conditions for "needs more than key" and negate.
    not(wild_in_body(BodyVars) orelse
        case bound_in_headpat(HP) of
            {all,V} -> lists:member(V, BodyVars);
            none    -> false;
            Vars    -> any_in_body(lists:keydelete(2,1,Vars), BodyVars)
        end);
needs_key_only(_) ->
    %% don't know
    false.

extract_vars([H|T]) ->
    extract_vars(H) ++ extract_vars(T);
extract_vars(T) when is_tuple(T) ->
    extract_vars(tuple_to_list(T));
extract_vars(T) when T=='$$'; T=='$_' ->
    [T];
extract_vars(T) when is_atom(T) ->
    case is_wild(T) of
        true ->
            [T];
        false ->
            []
    end;
extract_vars(_) ->
    [].

any_in_body(Vars, BodyVars) ->
    lists:any(fun({_,Vs}) ->
                      intersection(Vs, BodyVars) =/= []
              end, Vars).

intersection(A,B) when is_list(A), is_list(B) ->
    A -- (A -- B).

wild_in_body(BodyVars) ->
    intersection(BodyVars, ['$$','$_']) =/= [].

bound_in_headpat(HP) when is_atom(HP) ->
    {all, HP};
bound_in_headpat(HP) when is_tuple(HP) ->
    [_|T] = tuple_to_list(HP),
    map_vars(T, 2);
bound_in_headpat(_) ->
    %% this is not the place to throw an exception
    none.

map_vars([H|T], P) ->
    case extract_vars(H) of
        [] ->
            map_vars(T, P+1);
        Vs ->
            [{P, Vs}|map_vars(T, P+1)]
    end;
map_vars([], _) ->
    [].

is_prefix(A, B) when is_binary(A), is_binary(B) ->
    Sa = byte_size(A),
    case B of
        <<A:Sa/binary, _/binary>> ->
            true;
        _ ->
            false
    end.

keypat([H|T], KeyPos) ->
    keypat(T, KeyPos, keypat_pfx(H, KeyPos)).

keypat(_, _, <<>>) -> <<>>;
keypat([H|T], KeyPos, Pfx0) ->
    Pfx = keypat_pfx(H, KeyPos),
    keypat(T, KeyPos, common_prefix(Pfx, Pfx0));
keypat([], _, Pfx) ->
    Pfx.

common_prefix(<<H, T/binary>>, <<H, T1/binary>>) ->
    <<H, (common_prefix(T, T1))/binary>>;
common_prefix(_, _) ->
    <<>>.

keypat_pfx({HeadPat,_Gs,_}, KeyPos) when is_tuple(HeadPat) ->
    KP      = element(KeyPos, HeadPat),
    sext:prefix(KP);
keypat_pfx(_, _) ->
    <<>>.


%% ----------------------------------------------------------------------------
%% Db wrappers
%% ----------------------------------------------------------------------------

return_catch(F) when is_function(F, 0) ->
    try F()
    catch
        throw:badarg ->
            ?dbg("badarg in return_catch F() run", []),
            badarg
    end.

db_delete(#st{db = Db} = St, K) ->
    Res = mnesia_fdb_lib:delete(St, K),
    write_result(Res, delete, [Db, K], St).

db_put(#st{db = Db} = St, K, V) ->
    Res = mnesia_fdb_lib:put(St, K, V),
    write_result(Res, put, [Db, K, V], St).

write_result(ok, _, _, _) ->
    ok;
write_result(Res, Op, Args, #st{tab = Tab, on_write_error = Rpt, on_write_error_store = OWEStore}) ->
    RptOp = rpt_op(Rpt),
    maybe_store_error(OWEStore, Res, Tab, Op, Args, erlang:system_time(millisecond)),
    mnesia_lib:RptOp("FAILED mnesia_fdb_lib:~p(" ++ rpt_fmt(Args) ++ ") -> ~p~n",
                     [Op | Args] ++ [Res]),
    ?dbg("FAILED mnesia_fdb_lib:~p(" ++ rpt_fmt(Args) ++ ") -> ~p~n",
         [Op | Args] ++ [Res]),
    if Rpt == fatal; Rpt == error ->
            throw(badarg);
       true ->
            ok
    end.

maybe_store_error(undefined, _, _, _, _, _) ->
    ok;
maybe_store_error(Table, Err, IntTable, put, [_, K, _, _], Time) ->
    insert_error(Table, IntTable, K, Err, Time);
maybe_store_error(Table, Err, IntTable, delete, [_, K, _], Time) ->
    insert_error(Table, IntTable, K, Err, Time);
maybe_store_error(Table, Err, IntTable, write, [_, List, _], Time) ->
    lists:map(fun
                  ({put, K, _}) ->
                     insert_error(Table, IntTable, K, Err, Time);
                  ({delete, K}) ->
                     insert_error(Table, IntTable, K, Err, Time)
             end, List).

insert_error(Table, {Type, _, _}, K, Err, Time) ->
    {_, K1} = decode_key(K),
    ets:insert(Table, {{Type, K1}, Err, Time});
insert_error(Table, Type, K, Err, Time) when is_atom(Type) ->
    ets:insert(Table, {{Type, K}, Err, Time}).

rpt_fmt([_|T]) ->
    lists:append(["~p" | [", ~p" || _ <- T]]).

rpt_op(debug) ->
    dbg_out;
rpt_op(Op) ->
    Op.

valid_mnesia_op(Op) ->
    if Op==debug
       ; Op==verbose
       ; Op==warning
       ; Op==error
       ; Op==fatal ->
            true;
       true ->
            false
    end.

%% ----------------------------------------------------------------------------
%% COMMON PRIVATE
%% ----------------------------------------------------------------------------

%% Note that since a callback can be used as an indexing backend, we
%% cannot assume that keypos will always be 2. For indexes, the tab
%% name will be {Tab, index, Pos}, and The object structure will be
%% {{IxKey,Key}} for an ordered_set index, and {IxKey,Key} for a bag
%% index.
%%
keypos({_, index, _}) ->
    1;
keypos({_, retainer, _}) ->
    2;
keypos(Tab) when is_atom(Tab) ->
    2.

decode_key(CodedKey) ->
    case sext:partial_decode(CodedKey) of
        {full, Result, _} ->
            Result;
        _ ->
            error(badarg, CodedKey)
    end.

decode_val(Db, <<"mfdb_ref", Key/binary>>) ->
    Parts = erlfdb:get_range_startswith(Db, Key),
    binary_to_term(mnesia_fdb_lib:bin_join(Parts));
decode_val(_Db, CodedVal) ->
    binary_to_term(CodedVal).

tabname({Tab, index, {{Pos},_}}) ->
    atom_to_list(Tab) ++ "-=" ++ atom_to_list(Pos) ++ "=-_ix";
tabname({Tab, index, {Pos,_}}) ->
    atom_to_list(Tab) ++ "-" ++ integer_to_list(Pos) ++ "-_ix";
tabname({Tab, retainer, Name}) ->
    atom_to_list(Tab) ++ "-" ++ retainername(Name) ++ "-_RET";
tabname(Tab) when is_atom(Tab) ->
    atom_to_list(Tab) ++ "-_tab".

retainername(Name) when is_atom(Name) ->
    atom_to_list(Name);
retainername(Name) when is_list(Name) ->
    try binary_to_list(list_to_binary(Name))
    catch
        error:_ ->
            lists:flatten(io_lib:write(Name))
    end;
retainername(Name) ->
    lists:flatten(io_lib:write(Name)).

perform(Alias, Tab, OpFun) ->
    call(Alias, Tab, {perform, OpFun}).

fold(Alias, Tab, Fun, Acc, MS, N) ->
    OpFun = fun(#st{db = Db, table_id = TableId, type = Type}) ->
                    do_fold(Db, TableId, Tab, Type, Fun, Acc, MS, N)
            end,
    perform(Alias, Tab, OpFun).

%% can be run on the server side.
do_fold(Db, TableId, Tab, Type, Fun, Acc, MS, N) ->
    {AccKeys, F} =
        if is_function(Fun, 3) ->
                {true, fun({K,Obj}, Acc1) ->
                               Fun(Obj, K, Acc1)
                       end};
           is_function(Fun, 2) ->
                {false, Fun}
        end,
    do_fold1(do_select(Db, TableId, Tab, Type, MS, AccKeys, N), F, Acc).

do_fold1('$end_of_table', _, Acc) ->
    Acc;
do_fold1(L, Fun, Acc) ->
    lists:foldl(Fun, Acc, L).

is_wild('_') ->
    true;
is_wild(A) when is_atom(A) ->
    case atom_to_list(A) of
        "\$" ++ S ->
            try begin
                    _ = list_to_integer(S),
                    true
                end
            catch
                error:_ ->
                    false
            end;
        _ ->
            false
    end;
is_wild(_) ->
    false.
