-module(mfdb_reaper_sup).

-behaviour(supervisor).

-export([add_reaper/3]).

-export([start_link/0,
         init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, { {one_for_one, 5, 10}, []} }.

add_reaper(TableName, TableId, TTL) ->
    ReaperName = list_to_atom("mfdb_reaper_" ++ atom_to_list(TableName)),
    Spec = {ReaperName,
            {mfdb_reaper, start_link, [ReaperName, TableName, TableId, TTL]},
            transient, 1000, worker, [mfdb_reaper]},
    try supervisor:start_child(?MODULE, Spec) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, already_present} ->
            supervisor:terminate_child(?MODULE, ReaperName),
            add_reaper(TableName, TableId, TTL);
        OtherError ->
            OtherError
    catch
        E:M ->
            {error, {E, M}}
    end.
