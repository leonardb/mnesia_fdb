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

-export([lookup/2,
         store/2,
         delete/1,
         create/2]).

-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("mnesia_fdb.hrl").

-ifdef(DEBUG).
-define(dbg(Fmt, Args), io:fwrite(user,"~p:~p: "++(Fmt),[?MODULE,?LINE|Args])).
-else.
-define(dbg(Fmt, Args), ok).
-endif.

lookup(Tab, Default) ->
    gen_server:call(?MODULE, {lookup, Tab, Default}).

store(Tab, MetaData) ->
    ets:insert(?MODULE, {Tab, MetaData}).

delete(Tab) ->
    gen_server:call(?MODULE, {delete, Tab}).

create(Name, Props) ->
    gen_server:call(?MODULE, {create, Name, Props}).

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
    ok = bootstrap_(),
    %% io:format("FDB Manager started with Pid: ~p~n", [self()]),
    {ok, []}.

handle_call({lookup, Tab, Default}, _From, S) ->
    R = lookup_(Tab, Default),
    {reply, R, S};
handle_call({delete, Tab}, _From, S) ->
    R = delete_(Tab),
    {reply, R, S};
handle_call({create, Tab, Props}, _From, S) ->
    R = create_(Tab, Props),
    {reply, R, S};
handle_call(_, _, S) -> {reply, error, S}.
handle_cast(_, S)    -> {noreply, S}.
handle_info(_UNKNOWN, St) ->
    ?dbg("fdb manager received unknown message ~p~n", [_UNKNOWN]),
    {noreply, St}.
terminate(_, _)      -> ok.
code_change(_, S, _) -> {ok, S}.

bootstrap_() ->
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
            ok = init_fdb_(Conn)
    end.

%% Load the NIF (try and ensure it's only loaded only once)
%% There must be a better way of checking if it's been initialized
init_fdb_(#conn{tls_key_path = undefined}) ->
    case get(fdb) of
        undefined ->
            erlfdb_nif:init(),
            put(fdb, started),
            ok;
        started ->
            ok
    end;
init_fdb_(#conn{tls_key_path = KeyPath, tls_cert_path = CertPath, tls_ca_path = CAPath}) ->
    case get(fdb) of
        undefined ->
            {ok, CABytes} = file:read_file(binary_to_list(CAPath)),
            FdbNetworkOptions = [{tls_ca_bytes, CABytes},
                                 {tls_key_path, KeyPath},
                                 {tls_cert_path, CertPath}],
            ok = erlfdb_nif:init(FdbNetworkOptions),
            put(fdb, started),
            ok;
        started ->
            ok
    end.

lookup_(Tab, Default) ->
    try ets:lookup(?MODULE, Tab) of
        [#st{tab       = Tab} = Rec] ->
            Rec;
        [] ->
            create_(Tab, Default),
            lookup_(Tab, [])
    catch error:badarg ->
            create_(Tab, Default),
            lookup_(Tab, [])
    end.

db_conn_() ->
    [Conn0] = ets:lookup(?MODULE, conn),
    db_conn_(Conn0).

db_conn_(#conn{cluster = Cluster} = Conn) ->
    ?dbg("Opening cluster: ~p", [Conn]),
    ok = init_fdb_(Conn),
    {erlfdb_database, _} = Db = erlfdb:open(Cluster),
    Db.

delete_(Tab) ->
    Db = db_conn_(),
    Tx = erlfdb:create_transaction(Db),
    TabBin = atom_to_binary(Tab, utf8),
    case erlfdb:wait(erlfdb:get(Tx, <<"tbl_", TabBin/binary>>)) of
        not_found ->
            ok;
        TableId ->
            %% Remove all keys with matching table prefix
            ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, erlfdb_tuple:pack({}, TableId))),
            ok = erlfdb:wait(erlfdb:clear(Tx, <<"tbl_", TabBin/binary>>)),
            ok = erlfdb:wait(erlfdb:clear(Tx, <<"tbl_", TabBin/binary, "_settings">>))
    end,
    ok = erlfdb:wait(erlfdb:commit(Tx)),
    ets:delete(?MODULE, Tab),
    ok.

mk_tab_(Db, TableId, Tab, TabBin, Props) ->
    Type = proplists:get_value(type, Props, set),
    Alias = proplists:get_value(alias, Props, fdb_copies),
    RecordName = proplists:get_value(record_name, Props, Tab),
    {attributes, Attributes} = lists:keyfind(attributes, 1, Props),
    Index = proplists:get_value(index, Props, []),
    OnWriteError = proplists:get_value(on_write_error, Props, ?WRITE_ERR_DEFAULT),
    OnWriteErrorStore = proplists:get_value(on_write_error_store, Props, ?WRITE_ERR_STORE_DEFAULT),
    HcaRef = erlfdb_hca:create(<<TableId/binary, "_hca_ref">>),
    HcaBag = case Type of
                 bag -> erlfdb_hca:create(<<TableId/binary, "_hca_bag">>);
                 _ -> undefined
             end,
    #st{
       tab                     = Tab,
       type                    = Type,
       alias                   = Alias,
       record_name             = RecordName,
       attributes              = Attributes,
       index                   = Index,
       on_write_error          = OnWriteError,
       on_write_error_store    = OnWriteErrorStore,
       db                      = Db,
       tab_bin                 = TabBin,
       table_id                = TableId,
       hca_ref                 = HcaRef,
       hca_bag                 = HcaBag
      }.

create_(Tab, Props) ->
    TabBin = atom_to_binary(Tab, utf8),
    Db = db_conn_(),
    Table = case erlfdb:get(Db, <<"tbl_", TabBin/binary, "_settings">>) of
                not_found ->
                    Hca = erlfdb_hca:create(<<"hca_table">>),
                    TableId0 = erlfdb_hca:allocate(Hca, Db),
                    Table0 = mk_tab_(Db, TableId0, Tab, TabBin, Props),
                    erlfdb:set(Db, <<"tbl_", TableId0/binary, "_settings">>, term_to_binary(Table0)),
                    Table0;
                Table0 ->
                    #st{} = Table1 = binary_to_term(Table0),
                    NewDb = db_conn_(),
                    Table1#st{
                      db = NewDb
                     }
            end,
    true = ets:insert(?MODULE, Table),
    ok.
