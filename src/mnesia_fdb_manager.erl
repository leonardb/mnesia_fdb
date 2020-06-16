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

-module(mnesia_fdb_manager).

-behaviour(gen_server).

-export([load_table/2,
         create/2,
         delete/1,
         load_if_exists/1,
         st/1]).

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

-include("mnesia_fdb.hrl").

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

%%%% API for mnesia_fdb module %%%%%
create(Tab0, Props) ->
    Tab = tab_name_(Tab0),
    ?dbg("CREATE TABLE ~p -> ~p~n", [Tab0, Tab]),
    gen_server:call(?MODULE, {create, Tab, Tab0, Props}).

delete(Tab0) ->
    ?dbg("DELETE TABLE ~p~n", [Tab0]),
    gen_server:call(?MODULE, {delete, Tab0}).

load_table(Tab0, _Default) ->
    Tab = tab_name_(Tab0),
    ?dbg("LOOKUP TABLE ~p~n", [Tab0]),
    try ets:lookup(?MODULE, Tab) of
        [#st{tab = Tab, info = Infos} = Rec] ->
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
        [#st{tab = Tab, info = Infos}] ->
            write_ets_infos_(Infos),
            ok;
        [] ->
            gen_server:call(?MODULE, {load, Tab})
    catch error:badarg ->
            badarg
    end.

st(Tab0) ->
    Tab = tab_name_(Tab0),
    case ets:lookup(?MODULE, Tab) of
        [#st{} = St] ->
            St;
        _ ->
            badarg
    end.

write_info({Tab0, index, {Idx, _}}, Key, Value) ->
    Tab = tab_name_(Tab0),
    case ets:lookup(?MODULE, Tab) of
        [#st{info = Info0, db = Db} = St] ->
            TabKey = <<"tbl_", Tab/binary, "_settings">>,
            InfoKey = {Tab0, Idx, Key},
            Info = kr_(InfoKey, 1, Info0, {InfoKey, Value}),
            true = ets:insert(?MODULE, #info{k = InfoKey, v = Value}),
            ok = erlfdb:set(Db, TabKey, term_to_binary(St#st{info = Info})),
            ok;
        _ ->
            ?dbg("No state for ~p", [Tab]),
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

read_info({Tab0, index, {Idx, _}}, Key, Default) ->
    case ets:lookup(?MODULE, {Tab0, Idx, Key}) of
        [#info{v = Val}] ->
            Val;
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

handle_call({delete, Tab0}, _From, S) ->
    R = delete_(Tab0),
    ?dbg("fdb manager received delete. replying with ~p~n", [R]),
    {reply, R, S};
handle_call({create, Tab, MTab, Props}, _From, S) ->
    R = create_(Tab, MTab, Props),
    ?dbg("fdb manager received create. replying with ~p~n", [R]),
    {reply, R, S};
handle_call({load, Tab}, _From, S) ->
    R = load_(Tab),
    ?dbg("fdb manager received load. replying with ~p~n", [R]),
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

delete_(Tab0) ->
    Tab = tab_name_(Tab0),
    Db = db_conn_(),
    Tx = erlfdb:create_transaction(Db),
    case erlfdb:wait(erlfdb:get(Tx, <<"tbl_", Tab/binary>>)) of
        not_found ->
            ok;
        TableId ->
            %% Remove all keys with matching table prefix
            Prefixes = [{TableId},
                        {TableId, ?FDB_WC},
                        {TableId, ?FDB_WC, ?FDB_WC},
                        {TableId, ?FDB_WC, ?FDB_WC, ?FDB_WC}],
            [ok = erlfdb:wait(erlfdb:clear_range_startswith(Db, sext:prefix(Pfx))) || Pfx <- Prefixes]
    end,
    ok = erlfdb:wait(erlfdb:clear(Tx, <<"tbl_", Tab/binary>>)),
    ok = erlfdb:wait(erlfdb:clear(Tx, <<"tbl_", Tab/binary, "_settings">>)),
    ok = erlfdb:wait(erlfdb:commit(Tx)),
    ets:select_delete(?MODULE, [{#info{k = {Tab, '_'}, _ = '_'}, [], [true]}]),
    ets:delete(?MODULE, Tab),
    case Tab0 of
        {Parent0, index, {Idx, _} = Idx0} ->
            Parent = tab_name_(Parent0),
            ?dbg("Removing index for ~p from ~p~n", [Tab0, Parent]),
            case ets:lookup(?MODULE, Parent) of
                [#st{tab = ParentTab, info = Info0, db = PDb, index = Indexes0} = PSt] ->
                    ets:select_delete(?MODULE, [{#info{k = {Parent0, Idx, '_'}, _ = '_'}, [], [true]}]),
                    Key = {Parent0, Idx, index_consistent},
                    Info = lists:keydelete(Key, 1, Info0),
                    Indexes = lists:delete(Idx0, Indexes0),
                    NPSt = PSt#st{info = Info, index = Indexes},
                    true = ets:insert(?MODULE, NPSt),
                    TabKey = <<"tbl_", ParentTab/binary, "_settings">>,
                    ok = erlfdb:set(PDb, TabKey, term_to_binary(NPSt)),
                    ok;
                _ ->
                    ?dbg("No state for ~p", [Parent]),
                    badarg
            end;
        _ ->
            ok
    end,
    ok.

mk_tab_(Db, TableId, Tab, MTab, Props) ->
    Type = proplists:get_value(type, Props, set),
    Alias = proplists:get_value(alias, Props, fdb_copies),
    RecordName = proplists:get_value(record_name, Props, Tab),
    {attributes, Attributes} = lists:keyfind(attributes, 1, Props),
    OnWriteError = proplists:get_value(on_write_error, Props, ?WRITE_ERR_DEFAULT),
    OnWriteErrorStore = proplists:get_value(on_write_error_store, Props, ?WRITE_ERR_STORE_DEFAULT),
    HcaRef = erlfdb_hca:create(<<TableId/binary, "_hca_ref">>),
    HcaBag = case Type of
                 bag -> erlfdb_hca:create(<<TableId/binary, "_hca_bag">>);
                 _ -> undefined
             end,
    {Indexes, Infos} =
        case MTab of
            {Parent, index, {Idx, IType} = IdxFull} ->
                ok = index_add_(Parent, IdxFull),
                {[{Idx, IType}],
                 [{{Parent, Idx, index_consistent}, true}]};
            Parent ->
                Indexes0 = proplists:get_value(index, Props, []),
                {Indexes0,
                 [{{Parent, Idx, index_consistent}, true} || {Idx, _} <- Indexes0]}
        end,
    #st{
       tab                  = Tab,
       mtab                 = MTab,
       type                 = Type,
       alias                = Alias,
       record_name          = RecordName,
       attributes           = Attributes,
       index                = Indexes,
       on_write_error       = OnWriteError,
       on_write_error_store = OnWriteErrorStore,
       db                   = Db,
       table_id             = TableId,
       hca_ref              = HcaRef,
       hca_bag              = HcaBag,
       info                 = Infos
      }.

index_add_(Tab0, {Idx, _} = IdxFull) ->
    ?dbg("Add index ~p to ~p~n", [IdxFull, Tab0]),
    Tab = tab_name_(Tab0),
    case ets:lookup(?MODULE, Tab) of
        [#st{db = Db, index = Indexes0, info = Info0} = St] ->
            Key = {Tab0, Idx, index_consistent},
            ?dbg("BEFORE INFOS/INDEXES ~p ~p nkey = ~p~n", [Info0, Indexes0, Key]),
            Infos = kr_(Key, 1, Info0, {Key, true}),
            Indexes = kr_(Idx, 1, Indexes0, IdxFull),
            ?dbg("NEW INFOS/INDEXES ~p ~p~n", [Infos, Indexes]),
            NSt = St#st{info = Infos, index = Indexes},
            true = ets:insert(?MODULE, NSt),
            TabKey = <<"tbl_", Tab/binary, "_settings">>,
            ok = erlfdb:set(Db, TabKey, term_to_binary(NSt)),
            write_ets_infos_(Infos),
            ok;
        _ ->
            badarg
    end.

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

create_(Tab, MTab, Props) ->
    Db = db_conn_(),
    TabKey = <<"tbl_", Tab/binary, "_settings">>,
    case load_(Tab) of
        {error, not_found} ->
            Hca = erlfdb_hca:create(<<"hca_table">>),
            TableId0 = erlfdb_hca:allocate(Hca, Db),
            ?dbg("MAKE TABLE ~p ~p~n", [Tab, MTab]),
            #st{info = Infos} = Table0 = mk_tab_(Db, TableId0, Tab, MTab, Props),
            ok = erlfdb:set(Db, TabKey, term_to_binary(Table0)),
            true = ets:insert(?MODULE, Table0),
            write_ets_infos_(Infos),
            ok;
        ok ->
            ok
    end.

load_(Tab) ->
    Db = db_conn_(),
    TabKey = <<"tbl_", Tab/binary, "_settings">>,
    case erlfdb:get(Db, TabKey) of
        not_found ->
            {error, not_found};
        Table0 ->
            #st{info = Infos} = Table1 = binary_to_term(Table0),
            Table2 = Table1#st{db = Db},
            ok = erlfdb:set(Db, TabKey, term_to_binary(Table2)),
            true = ets:insert(?MODULE, Table2),
            write_ets_infos_(Infos),
            ok
    end.

kr_(K,X,L,KV) ->
    case lists:keyfind(K, X, L) of
        false ->
            [KV | L];
        _ ->
          lists:keyreplace(K,X,L,KV)
    end.
