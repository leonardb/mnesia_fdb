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

-export([ix_prefixes/3]).

%%
%% DEBUG API
%%
-export([iter_/10, iter_/11, iter_cont/1, do_indexed_select/5]).

-include("mfdb.hrl").

%% ----------------------------------------------------------------------------
%% CONVENIENCE API
%% ----------------------------------------------------------------------------

register() ->
    register(default_alias()).

register(Alias) ->
    ?dbg("~p : register(~p)", [self(), Alias]),
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
semantics(_Alias, types  ) -> [ordered_set];
semantics(_Alias, index_types) -> [ordered];
semantics(_Alias, index_fun) -> fun index_f/4;
semantics(_Alias, _) -> undefined.

is_index_consistent(Alias, Tab) ->
    ?dbg("is_index_consistent ~p ~p ",[Alias, Tab]),
    case info(Alias, Tab, index_consistent) of
        true -> true;
        _ -> false
    end.

index_is_consistent(Alias, Tab, Bool)
  when is_boolean(Bool) ->
    ?dbg("index_is_consistent ~p ~p ~p",[Alias, Tab, Bool]),
    mfdb_manager:write_info(Tab, index_consistent, Bool).

%% PRIVATE FUN
index_f(_Alias, _Tab, Pos, Obj) ->
    %% ?dbg("index_f ~p",[{_Alias, _Tab, Pos, Obj}]),
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

check_definition_entry(_Tab, _Id, {type, ordered_set = T} = P) ->
    ?dbg("~p: check_definition_entry(~p, ~p, ~p);~n Trace: ~s~n",
         [self(), _Tab, _Id, {type, T}, pp_stack()]),
    P;
check_definition_entry(Tab, Id, {type, T}) ->
    ?dbg("~p: check_definition_entry(~p, ~p, ~p);~n Trace: ~s~n",
         [self(), Tab, Id, {type, T}, pp_stack()]),
    mnesia:abort({combine_error, Tab, [Id, {type, T}]});
check_definition_entry(Tab, _Id, {user_properties, UPs} = P) ->
    TTL = proplists:get_value(ttl, UPs, undefined),
    case mfdb_manager:st(Tab) of
        #st{ttl = OTTL} ->
            case OTTL =/= TTL of
                true ->
                    mfdb_manager:set_ttl(Tab, TTL);
                false ->
                    ok
            end,
            P;
        _ ->
            P
    end;
check_definition_entry(_Tab, _Id, P) ->
    P.

create_table(_Alias, Tab, Props) ->
    ?dbg("~p :: create_table(~p, ~p, ~p)", [self(), _Alias, Tab, Props]),
    mfdb_manager:create_table(Tab, Props).

load_table(_Alias, Tab, restore, Props) ->
    mfdb_manager:create_table(Tab, Props);
load_table(_Alias, Tab, LoadReason, Opts) ->
    ?dbg("~p :: load_table(~p, ~p, ~p, ~p)", [self(), _Alias, Tab, LoadReason, Opts]),
    case mfdb_manager:load_table(Tab, Opts) of
        #st{} ->
            ok;
        _ ->
            badarg
    end.

close_table(_Alias, _Tab) ->
    ?dbg("~p: close_table(~p, ~p)~n", [self(), _Alias, _Tab]),
    ok.

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
    case mfdb_manager:load_if_exists(Tab) of
        ok ->
            mfdb_manager:delete_table(Tab);
        {error, not_found} ->
            ok
    end.

info(_Alias, Tab, memory) ->
    ?dbg("~p: info(~p, ~p, ~p)~n", [self(), _Alias, Tab, memory]),
    St = mfdb_manager:st(Tab),
    mfdb_lib:table_data_size(St);
info(_Alias, Tab, size) ->
    ?dbg("~p: info(~p, ~p, ~p)~n", [self(), _Alias, Tab, size]),
    St = mfdb_manager:st(Tab),
    mfdb_lib:table_count(St);
info(_Alias, Tab, Key) ->
    ?dbg("~p: info(~p, ~p, ~p)~n", [self(), _Alias, Tab, Key]),
    mfdb_manager:read_info(Tab, Key, undefined).

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
    ?dbg("~p: sender_init(~p, ~p, ~p, ~p)~n", [self(), Alias, Tab, _RemoteStorage, _Pid]),
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
    ?dbg("~p : receiver_done(~p, ~p, ~p, ~p)", [self(), _Alias, _Tab, _Sender, _State]),
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
    case mfdb_manager:st(Tab) of
        #st{} = St ->
            try db_delete(St, Key)
        catch
                E:M ->
                    io:format("Delete error ~p ~p", [E,M]),
                    badarg
            end;
        _ ->
            ok
    end.

%% Not relevant for an ordered_set
fixtable(_Alias, _Tab, _Bool) ->
    ?dbg("~p : fixtable(~p, ~p, ~p)", [self(), _Alias, _Tab, _Bool]),
    ok.

%% To save storage space, we avoid storing the key twice. The key
%% in the record is replaced with []. It has to be put back in lookup/3.
insert(_Alias, Tab0, Obj) ->
    ?dbg("~p : insert(~p, ~p, ~p)", [self(),_Alias, Tab0, Obj]),
    #st{} = St = mfdb_manager:st(Tab0),
    Pos = keypos(Tab0),
    Key = element(Pos, Obj),
    Val = setelement(Pos, Obj, []),
    try mfdb_lib:put(St, Key, Val)
    catch
        E:M ->
            io:format("Insert error: ~p ~p~n", [E,M]),
            badarg
    end.

%% Since the key is replaced with [] in the record, we have to put it back
%% into the found record.
lookup(Alias, Tab0, Key) ->
    ?dbg("~p : lookup(~p, ~p, ~p)", [self(), Alias, Tab0, Key]),
    #st{db = Db, mtab = Tab0, table_id = TableId} = mfdb_manager:st(Tab0),
    EncKey = mfdb_lib:encode_key(TableId, {?DATA_PREFIX, Key}),
            case erlfdb:get(Db, EncKey) of
                not_found ->
                    [];
                EVal ->
                    DVal = mfdb_lib:decode_val(Db, TableId, EVal),
                    Out = setelement(keypos(Tab0), DVal, Key),
                    [Out]
            end.

match_delete(Alias, Tab0, Pat) when is_atom(Pat) ->
    ?dbg("~p : match_delete(~p, ~p, ~p)", [self(), Alias, Tab0, Pat]),
    %%do_match_delete(Alias, Tab, ?FDB_START),
    case is_wild(Pat) of
        true ->
            #st{db = Db, table_id = TableId} = mfdb_manager:st(Tab0),
            %% @TODO need to verify if the 'parts' of large values are also removed
            Prefixes = [{?FDB_WC},
                        {?FDB_WC, ?FDB_WC},
                        {?FDB_WC, ?FDB_WC, ?FDB_WC}],
            [ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TableId, Pfx)) || Pfx <- Prefixes],
            ok;
        false ->
            %% can this happen??
            ?dbg("is_wild failed on Pat ~p", [Pat]),
            error(badarg)
    end;
match_delete(_Alias, Tab0, Pat) when is_tuple(Pat) ->
    #st{db = Db, mtab = Tab0, table_id = TableId} = St = mfdb_manager:st(Tab0),
    KP = keypos(Tab0),
    Key = element(KP, Pat),
    case is_wild(Key) of
        true ->
            %% @TODO need to verify if the 'parts' of large values are also removed
            Prefixes = [{?FDB_WC},
                        {?FDB_WC, ?FDB_WC},
                        {?FDB_WC, ?FDB_WC, ?FDB_WC}],
            [ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TableId, Pfx)) || Pfx <- Prefixes],
            ok;
        false ->
            ok = do_match_delete(Pat, St)
    end,
    ok.

first(Alias, Tab0) ->
    ?dbg("~p : first(~p, ~p)", [self(), Alias, Tab0]),
    #st{db = Db, table_id = TableId} = mfdb_manager:st(Tab0),
    StartKey = mfdb_lib:encode_prefix(TableId, {?DATA_PREFIX, ?FDB_WC}),
    EndKey = erlfdb_key:strinc(StartKey),
    case erlfdb:get_range(Db, StartKey, EndKey, [{limit,1}]) of
        [] ->
            '$end_of_table';
        [{EncKey, _}] ->
            mfdb_lib:decode_key(TableId, EncKey)
    end.

next(Alias, Tab0, Key) ->
    ?dbg("~p : next(~p, ~p, ~p)", [self(), Alias, Tab0, Key]),
    #st{db = Db, table_id = TableId} = mfdb_manager:st(Tab0),
    SKey = mfdb_lib:encode_key(TableId, {?DATA_PREFIX, Key}),
    EKey = erlfdb_key:strinc(mfdb_lib:encode_prefix(TableId, {?DATA_PREFIX, ?FDB_WC})),
    case erlfdb:get_range(Db, SKey, EKey, [{limit, 2}]) of
        [] ->
            '$end_of_table';
        [{K,_}] ->
            case mfdb_lib:decode_key(TableId, K) of
                Key ->
                    '$end_of_table';
                OutKey ->
                    OutKey
            end;
        [{K0,_}, {K1, _}] ->
            Key0 = mfdb_lib:decode_key(TableId, K0),
            Key1 = mfdb_lib:decode_key(TableId, K1),
            hd(lists:delete(Key, [Key0, Key1]))
    end.

prev(Alias, Tab0, Key) ->
    ?dbg("~p : prev(~p, ~p, ~p)", [self(), Alias, Tab0, Key]),
    #st{db = Db, table_id = TableId} = mfdb_manager:st(Tab0),
    SKey = mfdb_lib:encode_prefix(TableId, {?DATA_PREFIX, ?FDB_WC}),
    EKey = erlfdb_key:strinc(mfdb_lib:encode_prefix(TableId, {?DATA_PREFIX, Key})),
    case erlfdb:get_range(Db, SKey, EKey, [{reverse, true}, {limit, 2}]) of
        [] ->
            '$end_of_table';
        [{K,_}] ->
            case mfdb_lib:decode_key(TableId, K) of
                Key ->
                    '$end_of_table';
                OutKey ->
                    OutKey
            end;
        [{K0,_}, {K1, _}] ->
            Key0 = mfdb_lib:decode_key(TableId, K0),
            Key1 = mfdb_lib:decode_key(TableId, K1),
            hd(lists:delete(Key, [Key0, Key1]))
    end.

last(Alias, Tab0) ->
    ?dbg("~p : last(~p, ~p)", [self(), Alias, Tab0]),
    #st{db = Db, table_id = TableId} = mfdb_manager:st(Tab0),
    SKey = mfdb_lib:encode_prefix(TableId, {?DATA_PREFIX, ?FDB_WC}),
    EKey = erlfdb_key:strinc(SKey),
    case erlfdb:get_range(Db, SKey, EKey, [{reverse, true}, {limit, 1}]) of
        [] ->
            '$end_of_table';
        [{K, _}] ->
            mfdb_lib:decode_key(TableId, K)
    end.

repair_continuation(Cont, _Ms) ->
    ?dbg("~p : repair_continuation(~p, ~p)", [self(), Cont, _Ms]),
    Cont.

select(Cont) ->
    ?dbg("~p : select(~p) mnesia_activity: ~p", [self(), Cont, get(mnesia_activity_state)]),
    %% Handle {ModOrAlias, Cont} wrappers for backwards compatibility with
    %% older versions of mnesia_ext (before OTP 20).
    case Cont of
        {_, '$end_of_table'} -> '$end_of_table';
        {_, Cont1}           -> Cont1();
        '$end_of_table'      -> '$end_of_table';
        _                    -> Cont()
    end.

select(Alias, Tab, Ms) ->
    ?dbg("select( ~p, ~p, ~p) mnesia_activity: ~p~n", [Alias, Tab, Ms, get(mnesia_activity_state)]),
    case select(Alias, Tab, Ms, 0) of
        {Res, '$end_of_table'} ->
            Res;
        '$end_of_table' ->
            '$end_of_table'
    end.

select(Alias, Tab0, Ms0, Limit) when is_integer(Limit) ->
    ?dbg("~p : select(~p, ~p, ~p, ~p)", [self(),Alias, Tab0, Ms0, Limit]),
    #st{db = Db, table_id = TableId, tab = Tab, mtab = MTab, index = Indexes0} = St = mfdb_manager:st(Tab0),
    case Tab0 of
        {_, index, _Idx} ->
            ?dbg("Selecting from index: ~p~n", [_Idx]),
            #st{db = Db, table_id = TableId, tab = Tab, mtab = MTab} = mfdb_manager:st(Tab0),
            do_select(Db, TableId, Tab, MTab, Ms0, undefined, undefined, Limit);
        _ ->
            {Guards, Binds, Ms} =
                case Ms0 of
                    [{ {{_V, '$1'}} , G, _}] ->
                        {G, [{1, ['$1']}], Ms0};
                    [{HP, G, MsRet}] ->
                        {NewHP, NewGuards} = ms_rewrite(HP, G),
                        NewMs = [{NewHP, NewGuards, MsRet}],
                        {NewGuards, bound_in_headpat(NewHP), NewMs};
                    _ ->
                        {[], [], Ms0}
                end,
            ?dbg("Guards: ~p Binds: ~p Ms: ~p~n", [Guards, Binds, Ms]),
            ?dbg("FilterIndexes: ~p~n", [Indexes0]),
            Indexes = [I || #idx{index_consistent = true} = I <- tuple_to_list(Indexes0)],
            RangeGuards = range_guards(Guards, Binds, Indexes, []),
            ?dbg("RangeGuards: ~p ~p~n", [RangeGuards, Indexes]),
            {PkStart, PkEnd} = primary_table_range_(RangeGuards),
            ?dbg("PkStart: ~p PkSend: ~p~n", [PkStart, PkEnd]),
            case idx_sel(RangeGuards, Indexes, St) of
                {use_index, IdxParams} = IdxSel ->
                    ?dbg("IdxSel: ~p~n", [IdxSel]),
                    ?dbg("mnesia_fdb:do_indexed_select(~p, ~p, ~p, ~p).~n",
                         [Tab0, Ms, IdxParams, Limit]),
                    do_indexed_select(Tab0, Ms, IdxParams, false, Limit);
                no_index ->
                    ?dbg("no_index: using MS ~p~n", [Ms]),
                    do_select(Db, TableId, Tab, MTab, Ms, PkStart, PkEnd, Limit)
            end
    end.

-spec range_guards(list({atom(), atom(), any()}), list(), list(), list()) -> list().
range_guards([], _Binds, _Indexes, Acc) ->
    lists:keysort(1, Acc);
range_guards([{Comp, Bind, Val} | Rest], Binds, Indexes, Acc) ->
    case lists:member(Comp, ['>', '>=', '<', '=<', '=:=']) of
        true ->
            %% Binds {rec_idx, [bind_id]} eg: [{2,['$1']}, {3,['$2']}, {4,['$3']}]
            case lists:keyfind([Bind], 2, Binds) of
                false ->
                    ?dbg("Bind ~p not in binds ~p", [Bind, Binds]),
                    range_guards(Rest, Binds, Indexes, Acc);
                {Idx, _} ->
                    ?dbg("Idx ~p in binds", [Idx]),
                    case Idx =:= 2 orelse lists:keyfind(Idx, #idx.pos, Indexes) of
                        true ->
                            ?dbg("Column Idx ~p indexed: ~p", [Idx, Indexes]),
                            %% Column indexed
                            range_guards(Rest, Binds, Indexes, [{Idx, Comp, Val} | Acc]);
                        false ->
                            ?dbg("Column Idx ~p not indexed: ~p", [Idx, Indexes]),
                            %% Column not indexed
                            range_guards(Rest, Binds, Indexes, Acc);
                        #idx{index_consistent = false} ->
                            ?dbg("Index inconsistent: Not adding to guards: ~p", [{Idx, Comp, Val}]),
                            range_guards(Rest, Binds, Indexes, Acc);
                        #idx{index_consistent = true} ->
                            ?dbg("Add to guards: ~p", [{Idx, Comp, Val}]),
                            range_guards(Rest, Binds, Indexes, [{Idx, Comp, Val} | Acc])
                    end
            end;
        false ->
                    ?dbg("Unmatched comp ~p", [{Comp, Bind, Val}]),
            range_guards(Rest, Binds, Indexes, Acc)
    end.

primary_table_range_(Guards) ->
    primary_table_range_(Guards, undefined, undefined).

primary_table_range_([], Start, End) ->
    {replace_(Start, ?FDB_WC),
     replace_(End, ?FDB_END)};
primary_table_range_([{2, '>=', V} | Rest], undefined, End) ->
    primary_table_range_(Rest, {gte, V}, End);
primary_table_range_([{2, '>', V} | Rest], undefined, End) ->
    primary_table_range_(Rest, {gt, V}, End);
primary_table_range_([{2, '=<', V} | Rest], Start, undefined) ->
    primary_table_range_(Rest, Start, {lte, V});
primary_table_range_([{2, '<', V} | Rest], Start, undefined) ->
    primary_table_range_(Rest, Start, {lt, V});
primary_table_range_([_ | Rest], Start, End) ->
    primary_table_range_(Rest, Start, End).

idx_sel(Guards, Indexes, #st{mtab = Tab} = St0) ->
    ?dbg("idx_sel Indexes: ~p~n", [Indexes]),
    IdxSel0 = [begin
                   #st{table_id = TableId} = mfdb_manager:st({Tab, index, {KeyPos, ordered}}),
                   I = idx_table_params_(Guards, KeyPos, TableId),
                   IdxValCount = case I of
                       {_, _, {{'$1', '$2'}}, _} ->
                           undefined;
                       {_, _, {{M, '$2'}}, _} ->
                           %% we have an exact match on in indexed value
                           mfdb_lib:idx_matches(St0, KeyPos, M);
                       _ ->
                           undefined
                   end,
                   ?dbg("IdxParams [~p]: idx_table_params_(~p, ~p) -> ~p~n", [KeyPos, Guards, KeyPos, I]),
                   {KeyPos, I, IdxValCount}
               end || #idx{pos = KeyPos, index_consistent = true} <- Indexes],
    ?dbg("IdxSel0: ~p~n", [IdxSel0]),
    case IdxSel0 of
        [] ->
            no_index;
        IdxSel0 ->
            AvailIdx = [{idx_val_(I), Kp, I, IdxVCount} || {Kp, I, IdxVCount} <- IdxSel0, I =/= undefined],
            case idx_pick(AvailIdx) of
                undefined ->
                    no_index;
                {_, IdxId, IdxSel, _} ->
                    ?dbg("IdxSel: ~p~n", [IdxSel]),
                    {use_index, {IdxId, IdxSel}}
            end
    end.

idx_pick(Idx) ->
    idx_pick(Idx, undefined).

idx_pick([], Res) ->
    Res;
idx_pick([First | Rest], undefined) ->
    idx_pick(Rest, First);
idx_pick([{_IdxVal0, _, _, ValCount0} = Idx | Rest], {_IdxVal1, _, _, ValCount1})
    when is_integer(ValCount0) andalso
         is_integer(ValCount1) andalso
         ValCount0 < ValCount1 ->
    %% less keys to scan through
    idx_pick(Rest, Idx);
idx_pick([{_IdxVal0, _, _, ValCount0} | Rest], {_IdxVal1, _, _, ValCount1} = Idx)
    when is_integer(ValCount0) andalso
         is_integer(ValCount1) andalso
         ValCount1 < ValCount0 ->
    idx_pick(Rest, Idx);
idx_pick([{IdxVal0, _, _, ValCount0} | Rest], {IdxVal1, _, _, ValCount1} = Idx)
    when is_integer(ValCount0) andalso
         is_integer(ValCount1) andalso
         ValCount0 =:= ValCount1 andalso
         IdxVal1 >= IdxVal0 ->
    idx_pick(Rest, Idx);
idx_pick([{IdxVal0, _, _, ValCount0} = Idx | Rest], {IdxVal1, _, _, ValCount1})
    when is_integer(ValCount0) andalso
         is_integer(ValCount1) andalso
         ValCount0 =:= ValCount1 andalso
         IdxVal0 >= IdxVal1 ->
    idx_pick(Rest, Idx);
idx_pick([{_IdxVal0, _, _, ValCount0} = Idx | Rest], {_IdxVal1, _, _, undefined})
    when is_integer(ValCount0) ->
    %% explicit index vs scan
    idx_pick(Rest, Idx);
idx_pick([{_IdxVal0, _, _, undefined} | Rest], {_IdxVal1, _, _, ValCount1} = Idx)
    when is_integer(ValCount1) ->
    %% explicit index vs scan
    idx_pick(Rest, Idx);
idx_pick([{IdxVal0, _, _, undefined} = Idx | Rest], {IdxVal1, _, _, undefined})
    when IdxVal0 >= IdxVal1 ->
    %% explicit index vs scan
    idx_pick(Rest, Idx);
idx_pick([{IdxVal0, _, _, undefined} | Rest], {IdxVal1, _, _, undefined} = Idx)
    when IdxVal1 >= IdxVal0 ->
    idx_pick(Rest, Idx).

%% @doc convert guards into index-table specific selectors
%% Guards are the guards extracted from the MatchSpec.
%% This is very basic/naive implementation
%% @todo :: deal with multi-conditional guards with 'andalso', 'orelse' etc
%% @end
idx_table_params_(Guards, Keypos, TableId) ->
    ?dbg("idx_table_params_(~p, ~p, ~p)", [Guards, Keypos, TableId]),
    case lists:keyfind(Keypos, 1, Guards) of
        false ->
            undefined;
        _ ->
            idx_table_params_(Guards, Keypos, TableId, undefined, undefined, {{'$1', '$2'}}, [])
    end.

idx_table_params_([], _Keypos, TableId, Start, End, Match, Guards) ->
    ?dbg("Start: ~p~nEnd:~p~nMatch:~p~nGuards:~p~n",[Start, End, Match, Guards]),
    PfxStart = index_pfx(start, ?FDB_WC, true),
    PfxEnd = index_pfx('end', ?FDB_END, true),
    {replace_(Start, {fdb, mfdb_lib:encode_prefix(TableId, PfxStart)}),
     replace_(End, {fdb, erlfdb_key:strinc(mfdb_lib:encode_prefix(TableId, PfxEnd))}),
     Match,
     Guards};
idx_table_params_([{Keypos, '=:=', Val} | Rest], Keypos, TableId, _Start, _End, _Match, Guards) ->
    Match = {{Val, '$2'}},
    PfxStart = index_pfx(start, Val, true),
    PfxEnd = index_pfx('end', Val, true),
    ?dbg("Setting Start [~p]: mfdb_lib:encode_prefix(~p, ~p)~n",
         [Keypos, TableId, PfxStart]),
    Start = {fdbr, mfdb_lib:encode_prefix(TableId, PfxStart)},
    ?dbg("Setting End [~p]: erlfdb_key:strinc(mfdb_lib:encode_key(~p, ~p))~n",
         [Keypos, TableId, PfxEnd]),
    End = {fdbr, erlfdb_key:strinc(mfdb_lib:encode_prefix(TableId, PfxEnd))},
    idx_table_params_(Rest, Keypos, TableId, Start, End, Match, Guards);
idx_table_params_([{Keypos, Comp, Val} | Rest], Keypos, TableId, Start, End, Match, Guards)
  when Comp =:= '>=' orelse Comp =:= '>' ->
    NGuards = [{Comp, '$1', Val} | Guards],
    PfxStart = index_pfx(start, Val, true),
    ?dbg("Setting Start [~p]: mfdb_lib:encode_prefix(~p, ~p)~n",
         [Keypos, TableId, PfxStart]),
    NStart0 = {fdbr, mfdb_lib:encode_prefix(TableId, PfxStart)},
    NStart = replace_(Start, NStart0),
    idx_table_params_(Rest, Keypos, TableId, NStart, End, Match, NGuards);
idx_table_params_([{Keypos, Comp, Val} | Rest], Keypos, TableId, Start, End, Match, Guards)
  when Comp =:= '=<' orelse Comp =:= '<' ->
    NGuards = [{Comp, '$1', Val} | Guards],
    PfxEnd = index_pfx('end', Val, true),
    ?dbg("Setting End [~p]: erlfdb_key:strinc(mfdb_lib:encode_prefix(~p, ~p))~n",
         [Keypos, TableId, PfxEnd]),
    NEnd0 = {fdbr, erlfdb_key:strinc(mfdb_lib:encode_prefix(TableId, PfxEnd))},
    NEnd = replace_(End, NEnd0),
    idx_table_params_(Rest, Keypos, TableId, Start, NEnd, Match, NGuards);
idx_table_params_([_ | Rest], Keypos, TableId, Start, End, Match, Guards) ->
    idx_table_params_(Rest, Keypos, TableId, Start, End, Match, Guards).

replace_(undefined, Val) ->
    Val;
replace_(Orig, _Val) ->
    Orig.

v_(undefined) -> 0;
v_({fdb, B}) when is_binary(B) -> 1; %% pre-calculated range
v_({fdbr, B}) when is_binary(B) -> 2; %% pre-calculated range
v_({{_, '$1'}}) -> 10;
v_({{'$1', '$2'}}) -> 0; %% indicates guards will be processed
v_({{_, '$2'}}) -> 20; %% indicates a head-bound match
v_({_, _}) -> 1;
v_(?FDB_WC) -> 0; %% scan from start of table
v_(?FDB_END) -> 0; %% scan to end of table
v_(B) when is_binary(B) -> 1;
v_(L) when is_list(L) ->
    %% number of guards
    length(L) * 0.5.

idx_val_(undefined) ->
    0;
idx_val_({S,E,C,G}) ->
    lists:sum([v_(X) || X <- [S,E,C,G]]).

%%92> ets:fun2ms(fun(#test{id = I, expires = E, value = V}) when E =< 100 -> I end).
%%[{#test{id = '$1',value = '$2',expires = '$3'},
%%  [{'=<','$3',100}],
%%  ['$1']}]
%%93> ets:fun2ms(fun(#test{id = I, expires = E, value = V}) when E =< 100; E > 0 -> I end).
%%[{#test{id = '$1',value = '$2',expires = '$3'},
%%  [{'=<','$3',100}],
%%  ['$1']},
%% {#test{id = '$1',value = '$2',expires = '$3'},
%%  [{'>','$3',0}],
%%  ['$1']}]
%%94> ets:fun2ms(fun(#test{id = I, expires = E, value = V}) when E =< 100 andalso E > 0 -> I end).
%%[{#test{id = '$1',value = '$2',expires = '$3'},
%%  [{'andalso',{'=<','$3',100},{'>','$3',0}}],
%%  ['$1']}]
%%95> ets:fun2ms(fun(#test{id = I, expires = E, value = V}) when E =< 100, E > 0 -> I end).
%%[{#test{id = '$1',value = '$2',expires = '$3'},
%%  [{'=<','$3',100},{'>','$3',0}],
%%  ['$1']}]
%%96> ets:fun2ms(fun(#test{id = I, expires = E, value = V}) when E =< 100 orelse E > 0 -> I end).
%%[{#test{id = '$1',value = '$2',expires = '$3'},
%%  [{'orelse',{'=<','$3',100},{'>','$3',0}}],
%%  ['$1']}]
%%97> ets:fun2ms(fun(#test{id = I, expires = E, value = V}) when (E =< 100 orelse E > 0); (E =< 100 andalso E > 0) -> I end).
%%[{#test{id = '$1',value = '$2',expires = '$3'},
%%  [{'orelse',{'=<','$3',100},{'>','$3',0}}],
%%  ['$1']},
%% {#test{id = '$1',value = '$2',expires = '$3'},
%%  [{'andalso',{'=<','$3',100},{'>','$3',0}}],
%%  ['$1']}]
%%98> ets:fun2ms(fun(#test{id = I, expires = E, value = V}) when (E =< 100 orelse E > 0) andalso (E =< 100 andalso E > 0) -> I end).
%%[{#test{id = '$1',value = '$2',expires = '$3'},
%%  [{'andalso',{'orelse',{'=<','$3',100},{'>','$3',0}},
%%              {'andalso',{'=<','$3',100},{'>','$3',0}}}],
%%  ['$1']}]
%%99> ets:fun2ms(fun(#test{id = I, expires = E, value = V}) when (E =< 100 orelse E > 0) orelse (E =< 100 andalso E > 0) -> I end).
%%[{#test{id = '$1',value = '$2',expires = '$3'},
%%  [{'orelse',{'orelse',{'=<','$3',100},{'>','$3',0}},
%%             {'andalso',{'=<','$3',100},{'>','$3',0}}}],
%%  ['$1']}]


slot(_A, _B, _C) ->
    ?dbg("~p : slot(~p, ~p, ~p)", [self(),_A, _B, _C]),
    '$end_of_table'.

update_counter(Alias, Tab, Key, Incr) when is_integer(Incr) ->
    ?dbg("~p : update_counter(~p, ~p, ~p)", [self(),Alias, Tab, Key, Incr]),
    #st{} = St = mfdb_manager:st(Tab),
    do_update_counter(Key, Incr, St).

%% server-side part
do_update_counter(Key, Incr, #st{db = Db, table_id = TableId}) when is_integer(Incr) ->
    Tx = erlfdb:create_transaction(Db),
    {ok, NewVal} = mfdb_lib:update_counter(Tx, TableId, Key, Incr),
    erlfdb:wait(erlfdb:commit(Tx)),
    NewVal.

%% PRIVATE

%% record and key validation
validate_key(_Alias, _Tab, RecName, Arity, Type, _Key) ->
    %%?dbg("~p : validate_key(~p, ~p, ~p, ~p, ~p, ~p)", [self(),_Alias, _Tab, RecName, Arity, Type, _Key]),
    {RecName, Arity, Type}.

%% Ensure any values are not too large for any indexed columns
validate_record(_Alias, Tab0, RecName, Arity, Type, Obj) ->
    #st{mtab = Tab0, index = Indexes} = St = mfdb_manager:st(Tab0),
    InTrans = mnesia:is_transaction(),
    case value_size_guard_(tuple_to_list(Indexes), St, Obj) of
        ok ->
            {RecName, Arity, Type};
        {error, IdxCol} when InTrans =:= true ->
            mnesia:abort({value_too_large_for_field, IdxCol});
        %%{value_too_large_for_field, IdxCol};
        {error, IdxCol} when InTrans =:= false ->
            {error, {value_too_large_for_field, IdxCol}}
    end.

value_size_guard_([], _St, _Obj) ->
    ok;
value_size_guard_([undefined | Rest], #st{} = St, Obj) ->
    value_size_guard_(Rest, St, Obj);
value_size_guard_([#idx{pos = P} | Rest], #st{table_id = TableId, attributes = Attrib} = St, {{_V,_Id}} = Idx) ->
    EncKey = mfdb_lib:encode_key(TableId, {?DATA_PREFIX, Idx}),
    ByteSize = byte_size(EncKey),
    case ByteSize of
        X when X > ?MAX_KEY_SIZE ->
            IdxCol = lists:nth(P - 1, Attrib),
            {error, IdxCol};
        _ ->
            value_size_guard_(Rest, St, Idx)
    end;
value_size_guard_([#idx{pos = P} | Rest], #st{table_id = TableId, attributes = Attrib} = St, Obj) ->
    IdxVal = element(P, Obj),
    IdxId = element(2, Obj),
    Idx = {{IdxVal, IdxId}},
    EncKey = mfdb_lib:encode_key(TableId, {?DATA_PREFIX, Idx}),
    ByteSize = byte_size(EncKey),
    case ByteSize of
        X when X > ?MAX_KEY_SIZE ->
            IdxCol = lists:nth(P - 1, Attrib),
            {error, IdxCol};
        _ ->
            value_size_guard_(Rest, St, Obj)
    end.

%% file extension callbacks

%% Extensions for files that are permanent. Needs to be cleaned up
%% e.g. at deleting the schema.
real_suffixes() ->
    ?dbg("~p : real_suffixes()", [self()]),
    [].

%% Extensions for temporary files. Can be cleaned up when mnesia
%% cleans up other temporary files.
tmp_suffixes() ->
    ?dbg("~p : tmp_suffixes()", [self()]),
    [].

%% server-side part

do_match_delete(Pat, #st{table_id = TableId, mtab = MTab} = St) ->
    MS = [{Pat,[],['$_']}],
    IsIndex = is_index(MTab),
    Keypat = keypat(MS, TableId, IsIndex, keypos(MTab)),
    CompiledMS = ets:match_spec_compile(MS),
    DPfx = ?DATA_PREFIX,
    StartKey = mfdb_lib:encode_prefix(TableId, {DPfx, Keypat}),
    EndKey = erlfdb_key:strinc(mfdb_lib:encode_prefix(TableId, {DPfx, ?FDB_END})),
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
                    NStartKey = mfdb_lib:encode_prefix(TableId, {DPfx, LastKey}),
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
do_fold_delete2_(#st{table_id = TableId, mtab = MTab} = St, [{EncKey, EncV} | Rest], Tx, Keypat, MS, _LK) ->
    K = mfdb_lib:decode_key(TableId, EncKey),
    case is_prefix(Keypat, K) of
        true ->
            Rec = setelement(keypos(MTab),
                             mfdb_lib:decode_val(Tx, TableId, EncV),
                             mfdb_lib:decode_key(TableId, K)),
            case ets:match_spec_run([Rec], MS) of
                [] ->
                    do_fold_delete_(St, Rest, Tx, Keypat, MS, K);
                [_Match] ->
                    %% Keep the delete op within the same transaction
                    ok = mfdb_lib:delete(St#st{db = Tx}, K),
                    do_fold_delete2_(St, Rest, Tx, Keypat, MS, K)
            end;
        false ->
            do_fold_delete2_(St, Rest, Tx, Keypat, MS, K)
    end.

%% ----------------------------------------------------------------------------
%% PRIVATE SELECT MACHINERY
%% ----------------------------------------------------------------------------
%% 2-step select using the picked index for the inner select from an index table
%% and wrapping in an outer select function to allow matching other guards
%% @todo: use multiple indexes, possibly with a parallel query to get all ids, and use intersection for final id list
do_indexed_select(Tab0, MS, {IdxPos, {Start, End, Match, Guard}}, AccKeys, Limit) when is_boolean(AccKeys) ->
    #st{db = Db, table_id = OTableId, mtab = OMTab, index = Indexes} = mfdb_manager:st(Tab0),
    #idx{table_id = ITableId, tab = ITab, mtab = IMTab} = element(IdxPos, Indexes),
    KeyMs = [{Match, Guard, ['$_']}],
    OKeysOnly = needs_key_only(MS),
    InTransaction = mnesia:is_transaction(),
    ICompiledKeyMs = ets:match_spec_compile(KeyMs),
    OCompiledKeyMs = ets:match_spec_compile(MS),
    OuterMatchFun = outer_match_fun_(OTableId, OMTab, OCompiledKeyMs, AccKeys),
    ?dbg("KeyMs: ~p~n", [KeyMs]),
    DataFun = index_match_fun_(ICompiledKeyMs, OuterMatchFun),
    OAcc = [],
    IRange = {range, Start, End},
    Iter = iter_(Db, ITableId, ITab, IMTab, IRange, AccKeys, OKeysOnly, undefined, DataFun, OAcc, Limit),
    do_iter(InTransaction, Iter, Limit, []).

outer_match_fun_(OTableId, OMTab, OCompiledKeyMs, AccKeys) ->
    fun(Tx, Id, Acc0) ->
                    ?dbg("Outer match fun", []),
                    K = mfdb_lib:encode_key(OTableId, {?DATA_PREFIX, Id}),
                    case erlfdb:wait(erlfdb:get(Tx, K)) of
                        not_found ->
                            %% This should only happen with a dead index
                            %% entry (data deleted after we got index)
                            Acc0;
                        V ->
                            Value = mfdb_lib:decode_val(Tx, OTableId, V),
                            Rec = setelement(keypos(OMTab), Value, Id),
                            case OCompiledKeyMs =/= undefined andalso
                                ets:match_spec_run([Rec], OCompiledKeyMs) of
                                [] ->
                                    %% Did not match specification
                                    Acc0;
                                [Matched] when AccKeys =:= true ->
                                    %% Matched specification
                                    [{Id, Matched} | Acc0];
                                [Matched] when AccKeys =:= false ->
                                    %% Matched specification
                                    [Matched | Acc0];
                                false when AccKeys =:= true ->
                                    %% No match specification
                                    [{Id, Rec} | Acc0];
                                false when AccKeys =:= false ->
                                    %% No match specification
                                    [Rec | Acc0]
                            end
                    end
    end.

index_match_fun_(ICompiledKeyMs, OuterMatchFun) ->
    fun(Tx, {{_, Id}} = R, IAcc) ->
            ?dbg("index match : ~p", [R]),
            case ICompiledKeyMs =/= undefined andalso
                ets:match_spec_run([R], ICompiledKeyMs) of
                [] ->
                    ?dbg("index_match_fun_ ~p not matched", [R]),
                    %% Did not match
                    IAcc;
                [{{_, Id}}] ->
                    ?dbg("index_match_fun_ ~p matched", [R]),
                    %% Matched
                    OuterMatchFun(Tx, Id, IAcc);
                false ->
                    ?dbg("index_match_fun_ ~p no match spec", [R]),
                    %% No match specification
                    OuterMatchFun(Tx, Id, IAcc)
            end
    end.

is_index(MTab) when is_atom(MTab) ->
    false;
is_index(_MTab) ->
    true.

do_select(Db, TableId, Tab, MTab, MS, PkStart, PkEnd, Limit) ->
    do_select(Db, TableId, Tab, MTab, MS, PkStart, PkEnd, false, Limit).

do_select(Db, TableId, Tab, MTab, MS, PkStart, PkEnd, AccKeys, Limit) when is_boolean(AccKeys) ->
    IsIndex = is_index(MTab),
    Keypat0 = keypat(MS, TableId, IsIndex, 2),
    KeysOnly = needs_key_only(MS),
    InTransaction = mnesia:is_transaction(),
    Keypat = case Keypat0 of
                 <<>> when IsIndex =:= true ->
                     mfdb_lib:encode_prefix(TableId, {<<"di">>, ?FDB_WC});
                 _ when IsIndex =:= false andalso (PkStart =/= undefined orelse PkEnd =/= undefined) ->
                     {range,
                         pk_to_range(TableId, start, PkStart),
                         pk_to_range(TableId, 'end', PkEnd)
                     };
                 <<>> when IsIndex =:= false ->
                     mfdb_lib:encode_prefix(TableId, {<<"d">>, ?FDB_WC});
                 Keypat0 -> Keypat0
             end,
    CompiledMs = ets:match_spec_compile(MS),
    DataFun = undefined,
    InAcc = [],
    ?dbg("KeyPat: ~p~n", [Keypat]),
    Iter = iter_(Db, TableId, Tab, MTab, Keypat, AccKeys, KeysOnly, CompiledMs, DataFun, InAcc, Limit),
    do_iter(InTransaction, Iter, Limit, []).

%% ordered set tables
pk_to_range(TableId, start, {gt, X}) ->
    {fdbr, erlfdb_key:strinc(mfdb_lib:encode_key(TableId, {?DATA_PREFIX, X}))};
pk_to_range(TableId, start, {gte, X}) ->
    {fdbr, mfdb_lib:encode_key(TableId, {?DATA_PREFIX, X})};
pk_to_range(TableId, 'end', {lt, X}) ->
    {fdbr, mfdb_lib:encode_key(TableId, {?DATA_PREFIX, X})};
pk_to_range(TableId, 'end', {lte, X}) ->
    {fdbr, erlfdb_key:strinc(mfdb_lib:encode_key(TableId, {?DATA_PREFIX, X}))};
pk_to_range(TableId, start, _) ->
    {fdbr, mfdb_lib:encode_prefix(TableId, {?DATA_PREFIX, ?FDB_WC})};
pk_to_range(TableId, 'end', _) ->
    {fdbr, erlfdb_key:strinc(mfdb_lib:encode_prefix(TableId, {?DATA_PREFIX, ?FDB_END}))}.

do_iter(_InTransaction, '$end_of_table', Limit, Acc) when Limit =:= 0 ->
    {?SORT(Acc), '$end_of_table'};
do_iter(_InTransaction, {Data, '$end_of_table'}, Limit, Acc) when Limit =:= 0 ->
    {?SORT(lists:append(Acc, Data)), '$end_of_table'};
do_iter(_InTransaction, {Data, Iter}, Limit, Acc) when Limit =:= 0 andalso is_function(Iter) ->
    NAcc = ?SORT(lists:append(Acc, Data)),
    case Iter() of
        '$end_of_table' ->
            NAcc;
        {_, '$end_of_table'} = NIter ->
            do_iter(_InTransaction, NIter, Limit, NAcc);
        {_, ?IS_ITERATOR} = NIter ->
            do_iter(_InTransaction, NIter, Limit, NAcc)
    end;
do_iter(InTransaction, '$end_of_table', Limit, Acc) when Limit > 0 ->
    case InTransaction of
        true ->
            {?SORT(Acc), '$end_of_table'};
        false ->
            Acc
    end;
do_iter(InTransaction, {Data, '$end_of_table'}, Limit, Acc) when Limit > 0 ->
    case InTransaction of
        true ->
            {?SORT(lists:append(Acc, Data)), '$end_of_table'};
        false ->
            lists:append(Acc, Data)
    end;
do_iter(_InTransaction, {Data, Iter}, Limit, Acc) when Limit > 0 andalso is_function(Iter) ->
    NAcc = ?SORT(lists:append(Acc, Data)),
    {NAcc, Iter}.

needs_key_only([{HP,_,Body}]) ->
    BodyVars = lists:flatmap(fun extract_vars/1, Body),
    %% Note that we express the conditions for "needs more than key" and negate.
    InHead = bound_in_headpat(HP),
    ?dbg("BoundInHead: ~p", [InHead]),
    not(wild_in_body(BodyVars) orelse
        case InHead of
            {all,V} -> lists:member(V, BodyVars);
            none    -> false;
            Vars    -> any_in_body(lists:keydelete(2,1,Vars), BodyVars)
        end);
needs_key_only(_) ->
    %% don't know
    false.

any_in_body(Vars, BodyVars) ->
    lists:any(fun({_,Vs}) ->
                      intersection(Vs, BodyVars) =/= []
              end, Vars).

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

ms_rewrite(HP, Guards) when is_atom(HP) ->
    {HP, Guards};
ms_rewrite(HP, Guards) when is_tuple(HP) ->
    [_|T] = tuple_to_list(HP),
    {_, Used, Matches} =
        lists:foldl(
                     fun('_', {Inc, U, Acc}) ->
                             {Inc + 1, U, Acc};
                        (M, {Inc, U, Acc}) when is_atom(M) ->
                            case atom_to_binary(M, utf8) of
                                <<"$", _/binary>> ->
                                    {Inc + 1, [M | U], Acc};
                                _ ->
                                    {Inc + 1, U, [{Inc, M} | Acc]}
                            end;
                        (Match, {Inc, U, Acc}) ->
                             {Inc + 1, U, [{Inc, Match} | Acc]}
                     end,
                     {2, [], []}, T),
    headpat_matches_to_guards(HP, Guards, Used, Matches).

headpat_matches_to_guards(HP, Guards, _Used, []) ->
    {HP, Guards};
headpat_matches_to_guards(HP, Guards, Used0, [{Idx, Val} | Rest]) ->
    {Used, Bind} = next_bind(Used0, Idx),
    NewHP = setelement(Idx, HP, Bind),
    NewGuards = [{'=:=', Bind, Val} | Guards],
    headpat_matches_to_guards(NewHP, NewGuards, Used, Rest).

next_bind(Used, X) ->
    Bind = list_to_atom("$" ++ integer_to_list(X)),
    case lists:member(Bind, Used) of
        true ->
            next_bind(Used, X+1);
        false ->
            {[Bind | Used], Bind}
    end.

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

keypat([H|T], TableId, IsIndex, KeyPos) ->
    keypat(T, TableId, IsIndex, KeyPos, keypat_pfx(H, TableId, IsIndex, KeyPos)).

keypat(_, _TableId, _IsIndex, _, <<>>) -> <<>>;
keypat([H|T], TableId, IsIndex, KeyPos, Pfx0) ->
    Pfx = keypat_pfx(H, TableId, IsIndex, KeyPos),
    keypat(T, TableId, IsIndex, KeyPos, common_prefix(Pfx, Pfx0));
keypat([], _TableId, _IsIndex, _, Pfx) ->
    Pfx.

common_prefix(<<H, T/binary>>, <<H, T1/binary>>) ->
    <<H, (common_prefix(T, T1))/binary>>;
common_prefix(_, _) ->
    <<>>.

keypat_pfx({{HeadPat},_Gs,_}, TableId, IsIndex, KeyPos) when is_tuple(HeadPat) ->
    ?dbg("element(~p, ~p)", [KeyPos, HeadPat]),
    KP      = element(KeyPos, HeadPat),
    KeyPat = index_pfx('start', KP, IsIndex),
    mfdb_lib:encode_prefix(TableId, KeyPat);
keypat_pfx({HeadPat,_Gs,_}, TableId, IsIndex, KeyPos) when is_tuple(HeadPat) ->
    ?dbg("element(~p, ~p)", [KeyPos, HeadPat]),
    KP      = element(KeyPos, HeadPat),
    KeyPat = index_pfx('start', KP, IsIndex),
    mfdb_lib:encode_prefix(TableId, KeyPat);
keypat_pfx(_, _, _, _) ->
    <<>>.

index_pfx(start, V, true) ->
    Pfx = {<<(?DATA_PREFIX)/binary, "i">>, {V, ?FDB_WC}},
    ?dbg("prefix: ~p", [Pfx]),
    Pfx;
index_pfx('end', V, true) ->
    Pfx = {<<(?DATA_PREFIX)/binary, "i">>, {V, ?FDB_END}},
    ?dbg("prefix: ~p", [Pfx]),
    Pfx;
index_pfx(_D, V, false) ->
    %%io:format("Type: ~p D: ~p V: ~p~n", [Type, D, V]),
    V.


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

db_delete(#st{} = St, K) ->
    mfdb_lib:delete(St, K).

db_put(#st{} = St, K, V) ->
    mfdb_lib:put(St, K, V).

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

iter_cont(?IS_ITERATOR = Iterator) ->
    iter_int_(Iterator);
iter_cont(_) ->
    '$end_of_table'.

-spec iter_(Db :: ?IS_DB, TableId :: binary(), Tab :: binary(), MTab :: any(), StartKey :: any(), AccKeys :: boolean(), KeysOnly :: boolean(), Ms :: ets:comp_match_spec(), DataFun :: undefined | function(), InAcc :: list()) ->
                   {list(), '$end_of_table'} | {list(), ?IS_ITERATOR}.
iter_(?IS_DB = Db, TableId, Tab, MTab, StartKey, AccKeys, KeysOnly, Ms, DataFun, InAcc) ->
    iter_(?IS_DB = Db, TableId, Tab, MTab, StartKey, AccKeys, KeysOnly, Ms, DataFun, InAcc, 0).

-spec iter_(Db :: ?IS_DB, TableId :: binary(), Tab :: binary(), MTab :: any(), StartKey :: any(), AccKeys :: boolean(), KeysOnly :: boolean(), Ms :: ets:comp_match_spec(), DataFun :: undefined | function(), InAcc :: list(), DataLimit :: pos_integer()) ->
                   {list(), '$end_of_table'} | {list(), ?IS_ITERATOR}.
iter_(?IS_DB = Db, TableId, Tab, MTab, StartKey0, AccKeys, KeysOnly, Ms, DataFun, InAcc, DataLimit) ->
    Reverse = 0, %% we're not iterating in reverse
    ?dbg("Iter: ~p ~p~n", [TableId, StartKey0]),
    {SKey, EKey} = iter_start_end_(TableId, StartKey0),
    ?dbg("Iter: Start ~p End ~p~n", [SKey, EKey]),
    St0 = #iter_st{
             db = Db,
             table_id = TableId,
             tab = Tab,
             mtab = MTab,
             data_limit = DataLimit,
             data_acc = InAcc,
             data_fun = DataFun,
             acc_keys = AccKeys,
             keys_only = KeysOnly,
             compiled_ms = Ms,
             start_key = SKey,
             start_sel = erlfdb_key:to_selector(SKey),
             end_sel = erlfdb_key:to_selector(EKey),
             limit = 100, %% we use a fix limit of 100 for the number of KVs to pull
             target_bytes = 0,
             streaming_mode = iterator,
             iteration = 1,
             snapshot = true,
             reverse = Reverse
            },
    St = iter_transaction_(St0),
    iter_int_({cont, St}).

iter_int_({cont, #iter_st{tx = Tx,
                          table_id = TableId, mtab = MTab,
                          acc_keys = AccKeys, keys_only = KeysOnly,
                          compiled_ms = Ms,
                          data_limit = DataLimit, %% Max rec in accum per continuation
                          data_count = DataCount, %% count in continuation accumulator
                          data_acc = DataAcc, %% accum for continuation
                          data_fun = DataFun, %% Fun applied to selected data when not key-only
                          iteration = Iteration} = St0}) ->
    {{RawRows, Count, HasMore0}, St} = iter_future_(St0),
    case Count of
        0 ->
            {DataAcc, '$end_of_table'};
        _ ->
            {Rows, HasMore, LastKey} = rows_more_last_(DataLimit, DataCount, RawRows,
                                                       Count, HasMore0),
            {NewDataAcc, AddCount} = iter_append_(Rows, Tx, TableId,
                                                  MTab, AccKeys, KeysOnly, Ms,
                                                  DataFun, 0, DataAcc),
            ?dbg("Count: ~p ~p ~p~n", [Count, HasMore, mfdb_lib:decode_key(TableId, LastKey)]),
            NewDataCount = DataCount + AddCount,
            HitLimit = hit_data_limit_(NewDataCount, DataLimit),
            Done = RawRows =:= []
                orelse HitLimit =:= true
                orelse (HitLimit =:= false andalso HasMore =:= false),
            case Done of
                true when HasMore =:= false ->
                    %% no more rows
                    iter_commit_(Tx),
                    {lists:reverse(NewDataAcc), '$end_of_table'};
                true when HasMore =:= true ->
                    %% there are more rows, return accumulated data and a continuation fun
                    NSt0 = St#iter_st{
                             start_sel = erlfdb_key:first_greater_than(LastKey),
                             iteration = Iteration + 1,
                             data_count = 0,
                             data_acc = []
                            },
                    NSt = iter_transaction_(NSt0),
                    {lists:reverse(NewDataAcc), fun() -> iter_cont({cont, NSt}) end};
                false ->
                    %% there are more rows, but we need to continue accumulating
                    %% This loops internally, so no fun, just the continuation
                    NSt0 = St#iter_st{
                             start_sel = erlfdb_key:first_greater_than(LastKey),
                             iteration = Iteration + 1,
                             data_count = NewDataCount,
                             data_acc = NewDataAcc
                            },
                    NSt = iter_transaction_(NSt0),
                    iter_int_({cont, NSt})
            end
    end.

iter_append_([], _Tx, _TableId, _MTab, _AccKeys, _KeysOnly, _Ms, _DataFun, AddCount, Acc) ->
    {Acc, AddCount};
iter_append_([{K, V} | Rest], Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount, Acc) ->
    IsIndexTbl = not is_atom(MTab),
    case mfdb_lib:decode_key(TableId, K) of
        {idx, {{IK, IV}} = Idx} when IsIndexTbl andalso is_function(DataFun, 3) ->
            %% Record matched specification, is a fold operation, apply the supplied DataFun
            case not lists:member({IK,IV}, Acc) of
                true ->
                    NAcc = DataFun(Tx, Idx, Acc),
                    ?dbg("match with Fun: ~p => ~p~n~p~n", [Idx, NAcc =/= Acc, NAcc]),
                    iter_append_(Rest, Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
                false ->
                    iter_append_(Rest, Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount + 1, Acc)
            end;
        {idx, {{IK, IV}} = Idx} when IsIndexTbl ->
            case not lists:member({IK,IV}, Acc) andalso Ms =/= undefined andalso ets:match_spec_run([Idx], Ms) of
                false ->
                    ?dbg("No MS Acc: ~p ~p", [Idx, Acc]),
                    iter_append_(Rest, Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount + 1, Acc);
                [] ->
                    ?dbg("Not matched Acc: ~p ~p", [Idx, Acc]),
                    iter_append_(Rest, Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount + 1, Acc);
                [Match] ->
                    NAcc = [Match | Acc],
                    ?dbg("Index ~p Acc: ~p", [Idx, NAcc]),
                    iter_append_(Rest, Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount + 1, NAcc)
            end;
        Key when not IsIndexTbl ->
            ?dbg("decoded_key: ~p", [Key]),
            Value = mfdb_lib:decode_val(Tx, TableId, V),
            Rec = setelement(keypos(MTab), Value, Key),
            case Ms =/= undefined andalso ets:match_spec_run([Rec], Ms) of
                false  when is_function(DataFun, 2) ->
                    %% Record matched specification, is a fold operation, apply the supplied DataFun
                    NAcc = DataFun(Rec, Acc),
                    ?dbg("match with Fun: ~p => ~p", [Rec, NAcc =/= Acc]),
                    iter_append_(Rest, Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
                false  when is_function(DataFun, 3) ->
                    %% Record matched specification, is a fold operation, apply the supplied DataFun
                    NAcc = DataFun(Tx, Rec, Acc),
                    ?dbg("match with Fun: ~p => ~p", [Rec, NAcc =/= Acc]),
                    iter_append_(Rest, Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
                false ->
                    NAcc = [iter_val_(KeysOnly, Key, Rec) | Acc],
                    ?dbg("got match: ~p", [Rec]),
                    iter_append_(Rest, Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
                [] ->
                    %% Record did not match specification
                    iter_append_(Rest, Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount, Acc);
                [Match] when DataFun =:= undefined ->
                    ?dbg("got match: ~p ~p ~p", [Match, AccKeys, KeysOnly]),
                    %% Record matched specification, but not a fold operation
                    NAcc = case AccKeys of
                               true ->
                                   [{Key, Match} | Acc];
                               false ->
                                   [Match | Acc]
                           end,
                    iter_append_(Rest, Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
                [Match] when is_function(DataFun, 2) ->
                    %% Record matched specification, is a fold operation, apply the supplied DataFun
                    ?dbg("got match with Fun: ~p", [Match]),
                    NAcc = DataFun(Match, Acc),
                    iter_append_(Rest, Tx, TableId, MTab, AccKeys, KeysOnly, Ms, DataFun, AddCount + 1, NAcc)
            end
    end.

iter_val_(true, Key, _Rec) ->
    Key;
iter_val_(false, _Key, Rec) ->
    Rec.

iter_future_(#iter_st{tx = Tx, start_sel = StartKey,
                      end_sel = EndKey, limit = Limit,
                      target_bytes = TargetBytes, streaming_mode = StreamingMode,
                      iteration = Iteration, snapshot = Snapshot,
                      reverse = Reverse} = St0) ->
    try erlfdb:wait(erlfdb_nif:transaction_get_range(
                      Tx,
                      StartKey,
                      EndKey,
                      Limit,
                      TargetBytes,
                      StreamingMode,
                      Iteration,
                      Snapshot,
                      Reverse
                     )) of
        {_RawRows, _Count, _HasMore} = R ->
            {R, St0}
    catch
        error:{erlfdb_error, Code} ->
            io:format("FDB error: ~p~n", [Code]),
            ok = erlfdb:wait(erlfdb:on_error(Tx, Code)),
            St = iter_transaction_(St0),
            iter_future_(St)
    end.

iter_transaction_(#iter_st{db = Db, tx = Tx} = St) ->
    iter_commit_(Tx),
    NTx = erlfdb:create_transaction(Db),
    erlfdb_nif:transaction_set_option(NTx, disallow_writes, 1),
    St#iter_st{tx = NTx}.

iter_commit_(undefined) ->
    ok;
iter_commit_(?IS_TX = Tx) ->
    catch erlfdb:wait(erlfdb:commit(Tx)),
    ok.

iter_start_end_(TableId, StartKey0) ->
    case StartKey0 of
        {range, S0, E0} ->
            S = case S0 of
                    {S1k, S1} when S1k =:= fdb orelse S1k =:= fdbr ->
                        erlfdb_key:first_greater_or_equal(S1);
                    S0 ->
                        mfdb_lib:encode_prefix(TableId, {?DATA_PREFIX, S0})
                end,
            E = case E0 of
                    {E1k, E1} when E1k =:= fdb orelse E1k =:= fdbr ->
                        E1;
                    E0 ->
                        erlfdb_key:strinc(mfdb_lib:encode_prefix(TableId, {?DATA_PREFIX, E0}))
                end,
            {S, E};
        StartKey0 ->
            {erlfdb_key:first_greater_than(StartKey0),
             erlfdb_key:strinc(StartKey0)}
    end.

hit_data_limit_(_DataCount, 0) ->
    false;
hit_data_limit_(DataCount, IterLimit) ->
    DataCount + 1 > IterLimit.

rows_more_last_(0, _DataCount, RawRows, _Count, HasMore0) ->
    LastKey0 = element(1, lists:last(RawRows)),
    {RawRows, HasMore0, LastKey0};
rows_more_last_(DataLimit, DataCount, RawRows, Count, HasMore0)
  when (DataLimit - DataCount) > Count ->
    LastKey0 = element(1, lists:last(RawRows)),
    {RawRows, HasMore0, LastKey0};
rows_more_last_(DataLimit, DataCount, RawRows, _Count, HasMore0) ->
    Rows0 = lists:sublist(RawRows, DataLimit - DataCount),
    NHasMore = HasMore0 orelse length(RawRows) > (DataLimit - DataCount),
    LastKey0 = element(1, lists:last(Rows0)),
    {Rows0, NHasMore, LastKey0}.
