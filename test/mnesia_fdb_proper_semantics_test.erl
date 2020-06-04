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

%% @doc Verify dirty vs transaction semantics against rocksdb mnesia backend
%% @author Ulf Wiger <ulf.wiger@feuerlabs.com>
%% @doc Verify dirty vs transaction semantics against fdb mnesia backend
%% @author Leonard Boyce <leonard.boyce@lucidlayer.com>

-module(mnesia_fdb_proper_semantics_test).

%% This module uses the proper_statem pattern to generate random
%% sequences of commands, mixing dirty and transaction operations
%% (including dirty ops from within transactions). Each sequence is run
%% against a disc_copies table and a fdb_copies table, after
%% which the result of each operation in the sequence is compared between
%% the two runs. The postcondition is that every command in every sequence
%% should yield the same value against both backends.

-export([test/1,
         prop_seq/0]).

%% statem callbacks
-export([initial_state/0,
         command/1,
         precondition/2,
         postcondition/3,
         next_state/3]).

%% command callbacks
-export([activity/2]).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(st, {}).
-define(KEYS, [a,b,c]).

basic_test_() ->
    {timeout, 60000, [fun() -> test(100) end]}.

test(N) ->
    setup_mnesia(),
    true = proper:quickcheck(?MODULE:prop_seq(), N),
    ok.

prop_seq() ->
    ?FORALL(Cmds, proper_statem:commands(?MODULE),
            begin
                setup(),
                {H, S, Res} =
                    proper_statem:run_commands(?MODULE, Cmds),
                cleanup(),
                ?WHENFAIL(
                   io:fwrite("History: ~w~n"
                             "State  : ~w~n"
                             "Result : ~p~n", [H, S, Res]),
                   proper:aggregate(
                     proper_statem:command_names(Cmds), Res =:= ok))
            end).

%% Note that this requires the fdb application to be in the path,
%% and obviously an OTP patched with the backend plugin behavior.
setup_mnesia() ->
    stopped = mnesia:stop(),
    ok = mnesia:delete_schema([node()]),
    ok = mnesia:create_schema([node()]),
    ok = mnesia:start(),
    application:ensure_all_started(mnesia_fdb),
    ?debugFmt("App Env: ~p", [application:get_all_env()]),
    {ok, fdb_copies} = mnesia_fdb:register().

setup() ->
    {atomic,ok} = mnesia:create_table(d, [{disc_copies, [node()]},
                                          {type, ordered_set},
                                          {record_name, x}]),
    {atomic,ok} = mnesia:create_table(l, [{fdb_copies, [node()]},
                                          {type, ordered_set},
                                          {record_name, x}]),
    {atomic,ok} = mnesia:clear_table(d),
    {atomic,ok} = mnesia:clear_table(l),
    %% mnesia:set_debug_level(debug),
    ok = mnesia:wait_for_tables([d, l], 30000),
    ok.

cleanup() ->
    %% timer:sleep(5000),
    {atomic, ok} = mnesia:delete_table(d),
    {atomic, ok} = mnesia:delete_table(l),
    ok.

initial_state() ->
    #st{}.

command(#st{}) ->
    ?LET(Type, type(),
         {call, ?MODULE, activity, [Type, sequence()]}).

type() ->
    proper_types:oneof([async_dirty, transaction]).

precondition(_, _) ->
    true.

postcondition(O, {call,?MODULE,activity,_} = T, {A, B}) ->
    ?debugFmt("O: ~p T: ~p A: ~p B: ~p", [O, T, A,B]),
    A == B;
postcondition(_O, _T, _AB) ->
    ?debugFmt("OO: ~p T: ~p AB: ~p", [_O, _T, _AB]),
    false.

next_state(St, _, _) ->
    St.

sequence() ->
    proper_types:list(db_cmd()).

db_cmd() ->
    ?LET(Type, type(),
         proper_types:oneof([{Type, read, key()},
                             {Type, write, key(), value()},
                             {Type, delete, key()},
                             {Type, first},
                             {Type, next, key()},
                             {Type, prev, key()},
                             {Type, last}])).

key() ->
    proper_types:oneof([a,b,c]).

value() ->
    proper_types:oneof([1,2,3]).

activity(Type, Seq) ->
    DFuns = mk_seq(Type, d, Seq),
    LFuns = mk_seq(Type, l, Seq),
    {mnesia:activity(Type, fun() ->
                                   ?debugFmt("Applying Seq ~p to ~p~n~p~n~n", [Type, d, Seq]),
                                   %%apply_seq(Type, d, Seq)
                                   [begin
                                        R = erlang:apply(mnesia, F, A),
                                        ?debugFmt("mnesia:~p(~p) -> ~p~n", [F,A,R]),
                                        R
                                    end || {F,A} <-  DFuns]
                           end),
     mnesia:activity(Type, fun() ->
                                   ?debugFmt("Applying Seq ~p to ~p~n~p~n~n", [Type, l, Seq]),
                                   %% apply_seq(Type, l, Seq)
                                   [begin
                                        R = erlang:apply(mnesia, F, A),
                                        ?debugFmt("mnesia:~p(~p) -> ~p~n", [F,A,R]),
                                        R
                                    end || {F,A} <- LFuns]
                           end)}.

mk_seq(Type, Tab, Seq) ->
    mk_seq(Type, Tab, Seq, []).


mk_seq(transaction=X, Tab, [H|T], Acc) ->
    Res = case H of
              {X,read, K}   -> {read, [Tab, K, read]};
              {_,read, K}   -> {dirty_read, [Tab,K]};
              {X,write,K,V} -> {write, [Tab, {x, K, V}, write]};
              {_,write,K,V} -> {dirty_write, [Tab, {x,K,V}]};
              {X,delete,K}  -> {delete, [Tab, K, write]};
              {_,delete,K}  -> {dirty_delete, [Tab,K]};
              {X,first}     -> {first, [Tab]};
              {_,first}     -> {dirty_first, [Tab]};
              {X,next,K}    -> {next, [Tab, K]};
              {_,next,K}    -> {dirty_next, [Tab, K]};
              {X,prev,K}    -> {prev, [Tab, K]};
              {_,prev,K}    -> {dirty_prev, [Tab, K]};
              {X,last}      -> {last, [Tab]};
              {_,last}      -> {dirty_last, [Tab]}
          end,
    ?debugFmt("~p: transaction: ~p H ~p => ~p", [Tab, X, H, Res]),
    mk_seq(X, Tab, T, [Res|Acc]);
mk_seq(X, Tab, [H|T], Acc) ->
    ?debugFmt("~p: ~p H ~p", [Tab, X, H]),
    Res = case H of
              {_,read, K}   -> {read, [Tab, K, read]};
              {_,write,K,V} -> {write, [Tab, {x, K, V}, write]};
              {_,delete,K}  -> {delete, [Tab, K, write]};
              {_,first}     -> {first, [Tab]};
              {_,next,K}    -> {next, [Tab, K]};
              {_,prev,K}    -> {prev, [Tab, K]};
              {_,last}      -> {last, [Tab]}
          end,
    mk_seq(X, Tab, T, [Res|Acc]);
mk_seq(_, _, [], Acc) ->
    lists:reverse(Acc).


apply_seq(Type, Tab, Seq) ->
    apply_seq(Type, Tab, Seq, []).

apply_seq(transaction=X, Tab, [H|T], Acc) ->
    Res = case H of
              {X,read, K}   -> mnesia:read(Tab, K, read);
              {_,read, K}   -> mnesia:dirty_read(Tab,K);
              {X,write,K,V} -> mnesia:write(Tab, {x, K, V}, write);
              {_,write,K,V} -> mnesia:dirty_write(Tab, {x,K,V});
              {X,delete,K}  -> mnesia:delete(Tab, K, write);
              {_,delete,K}  -> mnesia:dirty_delete(Tab,K);
              {X,first}     -> mnesia:first(Tab);
              {_,first}     -> mnesia:dirty_first(Tab);
              {X,next,K}    -> mnesia:next(Tab, K);
              {_,next,K}    -> mnesia:dirty_next(Tab, K);
              {X,prev,K}    -> mnesia:prev(Tab, K);
              {_,prev,K}    -> mnesia:dirty_prev(Tab, K);
              {X,last}      -> mnesia:last(Tab);
              {_,last}      -> mnesia:dirty_last(Tab)
          end,
    ?debugFmt("~p: transaction: ~p H ~p => ~p", [Tab, X, H, Res]),
    apply_seq(X, Tab, T, [Res|Acc]);
apply_seq(X, Tab, [H|T], Acc) ->
    ?debugFmt("~p: ~p H ~p", [Tab, X, H]),
    Res = case H of
              {_,read, K}   -> mnesia:read(Tab, K, read);
              {_,write,K,V} -> mnesia:write(Tab, {x, K, V}, write);
              {_,delete,K}  -> mnesia:delete(Tab, K, write);
              {_,first}     -> mnesia:first(Tab);
              {_,next,K}    -> mnesia:next(Tab, K);
              {_,prev,K}    -> mnesia:prev(Tab, K);
              {_,last}      -> mnesia:last(Tab)
          end,
    apply_seq(X, Tab, T, [Res|Acc]);
apply_seq(_, _, [], Acc) ->
    lists:reverse(Acc).
