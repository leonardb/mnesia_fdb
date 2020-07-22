-module(mfdb_reaper_sup).

-behaviour(supervisor).

-export([add_reaper/3]).

-export([start_link/0,
         init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Spec = {mfdb_reaper,
            {mfdb_reaper, start_link, []},
            temporary, 1000, worker, [mfdb_reaper]},
    {ok, { {simple_one_for_one, 5, 10}, [Spec]} }.

add_reaper(TableName, TableId, TTL) ->
    ReaperName = list_to_atom("mfdb_reaper_" ++ atom_to_list(TableName)),
    try supervisor:start_child(?MODULE, [ReaperName, TableName, TableId, TTL]) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, already_present} ->
            Pid = whereis(ReaperName),
            supervisor:terminate_child(?MODULE, Pid),
            add_reaper(TableName, TableId, TTL);
        OtherError ->
            OtherError
    catch
        E:M ->
            {error, {E, M}}
    end.
