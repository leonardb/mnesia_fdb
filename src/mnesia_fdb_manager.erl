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
         st/1,
         store/2]).

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
    gen_server:call(?MODULE, {create, Tab, Props}).

delete(Tab0) ->
    Tab = tab_name_(Tab0),
    ?dbg("DELETE TABLE ~p~n", [Tab]),
    gen_server:call(?MODULE, {delete, Tab}).

load_table(Tab0, _Default) ->
    Tab = tab_name_(Tab0),
    ?dbg("LOOKUP TABLE ~p~n", [Tab0]),
    try ets:lookup(?MODULE, Tab) of
        [#st{tab = Tab} = Rec] ->
            Rec;
        [] ->
            badarg
    catch error:badarg ->
            badarg
    end.

load_if_exists(Tab0) ->
    Tab = tab_name_(Tab0),
    try ets:lookup(?MODULE, Tab) of
        [#st{tab = Tab}] ->
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
            ?dbg("Got state for ~p", [Tab]),
            St;
        _ ->
            ?dbg("No state for ~p", [Tab]),
            badarg
    end.

store(Tab0, MetaData) ->
    Tab = tab_name_(Tab0),
    ?dbg("STORE TABLE ~p ~p~n", [Tab, MetaData]),
    ets:insert(?MODULE, {Tab, MetaData}).

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

handle_call({delete, Tab}, _From, S) ->
    R = delete_(Tab),
    ?dbg("fdb manager received delete. replying with ~p~n", [R]),
    {reply, R, S};
handle_call({create, Tab, Props}, _From, S) ->
    R = create_(Tab, Props),
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

tab_name_({TabName, index, _}) ->
    TabName;
tab_name_({TabName, retainer, {Id, _Node}}) ->
    list_to_atom(atom_to_list(TabName) ++ "_retainer_" ++ integer_to_list(Id));
tab_name_(Tab) when is_atom(Tab) ->
    Tab.

create_(Tab, Props) ->
    TabBin = atom_to_binary(Tab, utf8),
    Db = db_conn_(),
    TabKey = <<"tbl_", TabBin/binary, "_settings">>,
    case load_(Tab) of
        {error, not_found} ->
            Hca = erlfdb_hca:create(<<"hca_table">>),
            TableId0 = erlfdb_hca:allocate(Hca, Db),
            Table0 = mk_tab_(Db, TableId0, Tab, TabBin, Props),
            ok = erlfdb:set(Db, TabKey, term_to_binary(Table0)),
            true = ets:insert(?MODULE, Table0),
            ok;
        ok ->
            ok
    end.

load_(Tab) ->
    TabBin = atom_to_binary(Tab, utf8),
    Db = db_conn_(),
    TabKey = <<"tbl_", TabBin/binary, "_settings">>,
    case erlfdb:get(Db, TabKey) of
        not_found ->
            {error, not_found};
        Table0 ->
            #st{} = Table1 = binary_to_term(Table0),
            Table2 = Table1#st{db = Db},
            ok = erlfdb:set(Db, TabKey, term_to_binary(Table2)),
            true = ets:insert(?MODULE, Table2),
            ok
    end.
