%%%-------------------------------------------------------------------
%%% @doc
%%% Periodically reap expired records from a table
%%% @end
%%%-------------------------------------------------------------------
-module(mfdb_reaper).

-behaviour(gen_server).

-export([start_link/4]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("mfdb.hrl").

-define(SERVER, ?MODULE).
-define(REAP_POLL_INTERVAL, 500).

-record(state, {conn, table_name, table_id, ttl, timer = poll_timer(undefined)}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(ReaperName, TableName, TableId, TTL) ->
    gen_server:start_link({local, ReaperName}, ?MODULE, [TableName, TableId, TTL], []).

init([TableName, TableId, TTL]) ->
    Conn = mfdb_manager:db_conn_(),
    {ok, #state{conn = Conn, table_name = TableName, table_id = TableId, ttl = TTL}}.

handle_call(stop, _From, #state{} = State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, #state{} = State) ->
    {reply, ok, State}.

handle_cast(_Request, #state{} = State) ->
    {noreply, State}.

handle_info(poll, #state{conn = Conn, table_name = TableName, table_id = TableId, ttl = TTL, timer = Timer} = State) ->
    ok = reap_expired(Conn, TableName, TableId, TTL),
    {noreply, State#state{timer = poll_timer(Timer)}};
handle_info(_Info, #state{} = State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, #state{} = State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
poll_timer(undefined) ->
    erlang:send_after(?REAP_POLL_INTERVAL, self(), poll);
poll_timer(TRef) when is_reference(TRef) ->
    erlang:cancel_timer(TRef),
    erlang:send_after(?REAP_POLL_INTERVAL, self(), poll).

reap_expired(Conn, TableName, TableId, TTL) ->
    RangeStart = mfdb_lib:encode_prefix(TableId, {<<"ttl-t2k">>, 0, ?FDB_WC}),
    End = mfdb_lib:unixtime() - TTL,
    RangeEnd = mfdb_lib:encode_prefix(TableId, {<<"ttl-t2k">>, End, ?FDB_END}),
    reap_expired_(Conn, TableName, TableId, RangeStart, RangeEnd).

reap_expired_(Conn, TableName, TableId, RangeStart, RangeEnd) ->
    case erlfdb:get_range(Conn, RangeStart, erlfdb_key:strinc(RangeEnd), [{limit, 1000}]) of
        [] ->
            ok;
        KVs ->
            LastKey = lists:foldl(
                        fun({EncKey, <<>>}, _) ->
                                RKey = mfdb_lib:decode_key(TableId, EncKey),
                                ?dbg("Delete ~p from ~p", [RKey, TableName]),
                                ok = mnesia:dirty_delete(TableName, RKey),
                                %% Key2Ttl have to be removed individually
                                TtlK2T = mfdb_lib:encode_key(TableId, {<<"ttl-k2t">>, RKey}),
                                ok = erlfdb:clear(Conn, TtlK2T),
                                EncKey
                        end, ok, KVs),
            ok = erlfdb:clear_range(Conn, RangeStart, erlfdb_key:strinc(LastKey)),
            %%reap_expired_(Conn, TableName, TableId, RangeStart, RangeEnd)
            ok
    end.
