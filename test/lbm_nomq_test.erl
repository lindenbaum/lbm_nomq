%%%=============================================================================
%%%
%%%               |  o __   _|  _  __  |_   _       _ _   (TM)
%%%               |_ | | | (_| (/_ | | |_) (_| |_| | | |
%%%
%%% @author Sven Heyll <sven.heyll@lindenbaum.eu>
%%% @author Timo Koepke <timo.koepke@lindenbaum.eu>
%%% @author Tobias Schlager <tobias.schlager@lindenbaum.eu>
%%% @copyright (C) 2014, Lindenbaum GmbH
%%%
%%%=============================================================================

-module(lbm_nomq_test).

-include_lib("eunit/include/eunit.hrl").

-export([call/2]).

-define(TOPIC, topic).

-define(DBG(Fmt, Args), io:format(standard_error, Fmt, Args)).

%%%=============================================================================
%%% TESTS
%%%=============================================================================

all_test_() ->
    {setup,
     fun() ->
             {ok, _} = application:ensure_all_started(lbm_nomq)
     end,
     fun({ok, Apps}) ->
             [application:stop(App) || App <- Apps]
     end,
     [
      fun basic_subscribe/0,
      fun no_subscribers/0,
      fun basic_push/0,
      fun multiple_subscribers/0,
      fun late_subscribe/0,
      {timeout, 60, fun concurrency/0},
      {timeout, 60, fun concurrency_subscriber_exits/0}
     ]}.

basic_subscribe() ->
    {S, _} = spawn_monitor(subscriber(?TOPIC, [])),
    receive {subscribed, S} -> ok end,
    receive {'DOWN', _, process, S, normal} -> ok end.

no_subscribers() ->
    try lbm_nomq:push(?TOPIC, msg, 100) of
        _ -> throw(test_failed)
    catch
        exit:{timeout, {lbm_nomq, push, [?TOPIC, msg, 100]}} -> ok
    end.

basic_push() ->
    Messages = [msg1, msg2, msg3],

    {S, _} = spawn_monitor(subscriber(?TOPIC, Messages)),
    receive {subscribed, S} -> ok end,

    {P, _} = spawn_monitor(pusher(?TOPIC, Messages)),

    receive {'DOWN', _, process, S, normal} -> ok end,
    receive {'DOWN', _, process, P, normal} -> ok end,

    {'EXIT', {timeout, _}} = (catch lbm_nomq:push(?TOPIC, msg, 100)).

multiple_subscribers() ->
    {S1, _} = spawn_monitor(subscriber(?TOPIC, [stop])),
    receive {subscribed, S1} -> ok end,

    {S2, _} = spawn_monitor(subscriber(?TOPIC, [stop])),
    receive {subscribed, S2} -> ok end,

    {S3, _} = spawn_monitor(subscriber(?TOPIC, [stop])),
    receive {subscribed, S3} -> ok end,

    {P, _} = spawn_monitor(pusher(?TOPIC, [stop, stop, stop])),

    receive {'DOWN', _, process, S1, normal} -> ok end,
    receive {'DOWN', _, process, S2, normal} -> ok end,
    receive {'DOWN', _, process, S3, normal} -> ok end,
    receive {'DOWN', _, process, P, normal} -> ok end,

    {'EXIT', {timeout, _}} = (catch lbm_nomq:push(?TOPIC, msg, 100)).

late_subscribe() ->
    Messages = [msg1, msg2, msg3],

    {P, _} = spawn_monitor(pusher(?TOPIC, Messages)),

    timer:sleep(1000),

    {S, _} = spawn_monitor(subscriber(?TOPIC, Messages)),
    receive {subscribed, S} -> ok end,

    receive {'DOWN', _, process, P, normal} -> ok end,
    receive {'DOWN', _, process, S, normal} -> ok end,

    {'EXIT', {timeout, _}} = (catch lbm_nomq:push(?TOPIC, msg, 100)).

concurrency() ->
    Messages = lists:seq(1, 20000),
    F = fun() ->
                {S1, _} = spawn_monitor(subscriber(?TOPIC, Messages)),
                Ps = [spawn_monitor(pusher(?TOPIC, [M])) || M <- Messages],

                [receive {'DOWN', _, _, P, normal} -> ok end || {P, _} <- Ps],
                receive {'DOWN', _, process, S1, normal} -> ok end
        end,
    Time = element(1, timer:tc(F)),
    ?DBG("~p MESSAGES TOOK ~pms~n", [length(Messages), Time / 1000]).

concurrency_subscriber_exits() ->
    Messages = lists:seq(1, 20000),
    Num = length(Messages) div 2,
    F = fun() ->
                {S1, _} = spawn_monitor(subscriber(?TOPIC, Messages, Num)),
                Ps = [spawn_monitor(pusher(?TOPIC, [M])) || M <- Messages],

                receive {'DOWN', _, process, S1, {rest, Rest}} -> ok end,
                {S2, _} = spawn_monitor(subscriber(?TOPIC, Rest)),

                [receive {'DOWN', _, _, P, normal} -> ok end || {P, _} <- Ps],
                receive {'DOWN', _, process, S2, normal} -> ok end
        end,
    Time = element(1, timer:tc(F)),
    ?DBG("~p MESSAGES TOOK ~pms~n", [length(Messages), Time / 1000]).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% push all terms in a tight loop
%%------------------------------------------------------------------------------
pusher(Topic, Terms) ->
    fun() ->
            [ok = lbm_nomq:push(Topic, Term) || Term <- Terms]
    end.

%%------------------------------------------------------------------------------
%% @private
%% subscribe, receive all terms from the given list and exit normally
%%------------------------------------------------------------------------------
subscriber(Topic, Terms) -> subscriber(Topic, Terms, length(Terms)).

%%------------------------------------------------------------------------------
%% @private
%% subscribe, receive `NumTerms' terms from the given list and exit with reason
%% set to a list with rest terms. If rest terms is empty the exit reason is
%% normal.
%%------------------------------------------------------------------------------
subscriber(Topic, Terms, NumTerms) ->
    Parent = self(),
    fun() ->
            MFA = {?MODULE, call, [self()]},
            ok = lbm_nomq:subscribe(Topic, MFA),
            Parent ! {subscribed, self()},
            receive_loop(Terms, NumTerms)
    end.

%%------------------------------------------------------------------------------
%% @private
%% poor man's gen_server:call/2, meant to work with receive_loop/1
%%------------------------------------------------------------------------------
call(Pid, Term) ->
    Ref = erlang:monitor(process, Pid),
    Pid ! {self(), Term},
    receive
        {handled, Pid, Term} ->
            erlang:demonitor(Ref),
            ok;
        {'DOWN', Ref, _, Pid, _} ->
            exit({error, no_receiver})
    end.

%%------------------------------------------------------------------------------
%% @private
%% handles the 'protocol' expected by call/2
%%------------------------------------------------------------------------------
receive_loop([], 0) ->
    ok;
receive_loop(Terms, 0) ->
    exit({rest, Terms});
receive_loop(Terms, NumTerms) ->
    receive
        {Pid, Term} ->
            case lists:member(Term, Terms) of
                true ->
                    Pid ! {handled, self(), Term},
                    receive_loop(Terms -- [Term], NumTerms - 1);
                false ->
                    exit({unexpected_term, Term})
            end;
        Term ->
            exit({unexpected_term, Term})
    end.
