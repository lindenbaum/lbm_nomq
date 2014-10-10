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

-export([call/3]).

-define(TOPIC, topic).

-define(MESSAGE(X), {message, X}).

-define(DBG(Fmt, Args), io:format(standard_error, Fmt, Args)).

%%%=============================================================================
%%% TESTS
%%%=============================================================================

all_test_() ->
    {setup, setup(), teardown(),
     [
      fun basic_subscribe/0,
      fun no_subscribers/0,
      fun no_subscribers_no_wait/0,
      fun basic_push/0,
      fun multiple_subscribers/0,
      fun late_subscribe/0,
      {timeout, 60, fun concurrency/0},
      {timeout, 60, fun concurrency_with_exits/0},
      {timeout, 60, fun concurrency_distributed/0}
     ]}.

basic_subscribe() ->
    {S, _} = spawn_monitor(subscriber(?TOPIC, 0)),
    receive {subscribed, S} -> ok end,
    receive {'DOWN', _, process, S, normal} -> ok end.

no_subscribers() ->
    try lbm_nomq:push(?TOPIC, msg, 100) of
        _ -> throw(test_failed)
    catch
        exit:{timeout, {lbm_nomq, push, [?TOPIC, msg, 100]}} -> ok
    end.

no_subscribers_no_wait() ->
    try lbm_nomq:push(?TOPIC, msg, 100, [no_wait]) of
        _ -> throw(test_failed)
    catch
        exit:{no_subscribers, {lbm_nomq, push, [?TOPIC, msg, 100]}} -> ok
    end.

basic_push() ->
    Messages = [msg1, msg2, msg3],

    {S, _} = spawn_monitor(subscriber(?TOPIC, length(Messages))),
    receive {subscribed, S} -> ok end,

    {P, _} = spawn_monitor(pusher(?TOPIC, Messages)),

    receive {'DOWN', _, process, S, normal} -> ok end,
    receive {'DOWN', _, process, P, normal} -> ok end,

    {'EXIT', {timeout, _}} = (catch lbm_nomq:push(?TOPIC, msg, 100)).

multiple_subscribers() ->
    {S1, _} = spawn_monitor(subscriber(?TOPIC, 1)),
    receive {subscribed, S1} -> ok end,

    {S2, _} = spawn_monitor(subscriber(?TOPIC, 1)),
    receive {subscribed, S2} -> ok end,

    {S3, _} = spawn_monitor(subscriber(?TOPIC, 1)),
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

    timer:sleep(500),

    {S, _} = spawn_monitor(subscriber(?TOPIC, length(Messages))),
    receive {subscribed, S} -> ok end,

    receive {'DOWN', _, process, P, normal} -> ok end,
    receive {'DOWN', _, process, S, normal} -> ok end,

    {'EXIT', {timeout, _}} = (catch lbm_nomq:push(?TOPIC, msg, 100)).

concurrency() ->
    Messages = 20000,
    Spawn = fun(M) -> spawn_monitor(pusher(?TOPIC, [M])) end,
    Down = fun({P, PR}) -> receive {'DOWN', PR, _, P, normal} -> ok end end,
    F = fun() ->
                {S, SR} = spawn_monitor(subscriber(?TOPIC, Messages)),
                ok = lists:foreach(Down, for(Messages, Spawn)),
                receive {'DOWN', SR, process, S, normal} -> ok end
        end,
    Time = element(1, timer:tc(F)),
    ?DBG("~p MESSAGES on 1 receiver took ~pms~n", [Messages, Time / 1000]).

concurrency_with_exits() ->
    Messages = 20000,
    NumTerms1 = NumTerms2 = Messages div 3,
    NumTerms3 = Messages - 2*(NumTerms1),

    Spawn = fun(M) -> spawn_monitor(pusher(?TOPIC, [M])) end,
    Down = fun({P, PR}) -> receive {'DOWN', PR, _, P, normal} -> ok end end,
    F = fun() ->
                {S1, SR1} = spawn_monitor(subscriber(?TOPIC, NumTerms1)),
                Ps = for(Messages, Spawn),

                receive {'DOWN', SR1, process, S1, normal} -> ok end,
                {S2, SR2} = spawn_monitor(subscriber(?TOPIC, NumTerms2)),
                {S3, SR3} = spawn_monitor(subscriber(?TOPIC, NumTerms3)),

                ok = lists:foreach(Down, Ps),
                receive {'DOWN', SR2, process, S2, normal} -> ok end,
                receive {'DOWN', SR3, process, S3, normal} -> ok end
        end,
    Time = element(1, timer:tc(F)),
    ?DBG("~p MESSAGES on 3 receivers took ~pms~n", [Messages, Time / 1000]).

concurrency_distributed() ->
    process_flag(trap_exit, true),

    Messages = 20000,
    NumTerms1 = NumTerms2 = NumTerms3 = Messages div 4,
    NumTerms4 = Messages - 3*(NumTerms1),

    {ok, Slave1} = slave_setup(slave1),
    {ok, Slave2} = slave_setup(slave2),
    {ok, Slave3} = slave_setup(slave3),

    Spawn = fun(M, N) -> spawn_link(N, pusher(?TOPIC, [M])) end,
    Exit = fun(P) -> receive {'EXIT', P, normal} -> ok end end,
    F = fun() ->
                S1 = spawn_link(node(), subscriber(?TOPIC, NumTerms1)),
                Ps4 = for(NumTerms1, Spawn, [Slave3]),
                Ps3 = for(NumTerms2, Spawn, [Slave2]),
                Ps2 = for(NumTerms3, Spawn, [Slave1]),
                Ps1 = for(NumTerms4, Spawn, [node()]),

                receive {'EXIT', S1, normal} -> ok end,
                S2 = spawn_link(Slave1, subscriber(?TOPIC, NumTerms2)),
                S3 = spawn_link(Slave2, subscriber(?TOPIC, NumTerms3)),
                S4 = spawn_link(Slave3, subscriber(?TOPIC, NumTerms4)),

                ok = lists:foreach(Exit, Ps4),
                ok = lists:foreach(Exit, Ps3),
                ok = lists:foreach(Exit, Ps2),
                ok = lists:foreach(Exit, Ps1),
                receive {'EXIT', S2, normal} -> ok end,
                receive {'EXIT', S3, normal} -> ok end,
                receive {'EXIT', S4, normal} -> ok end
        end,
    Time = element(1, timer:tc(F)),
    ?DBG("~p MESSAGES on 4 nodes/receivers took ~pms~n", [Messages, Time / 1000]).

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
%% Subscribe, receive the given number of terms and exit with reason normal.
%%------------------------------------------------------------------------------
subscriber(Topic, NumTerms) ->
    Parent = self(),
    fun() ->
            MFA = {?MODULE, call, [self()]},
            ok = lbm_nomq:subscribe(Topic, MFA),
            Parent ! {subscribed, self()},
            receive_loop(NumTerms)
    end.

%%------------------------------------------------------------------------------
%% @private
%% poor man's gen_server:call/2, meant to work with receive_loop/1
%%------------------------------------------------------------------------------
call(Pid, Term, Timeout) ->
    Ref = erlang:monitor(process, Pid),
    Pid ! ?MESSAGE({Ref, self(), Term}),
    receive
        {handled, Ref, Pid} ->
            erlang:demonitor(Ref),
            ok;
        {'DOWN', Ref, process, Pid, _} ->
            exit({error, no_receiver})
    after Timeout ->
            exit({error, timeout})
    end.

%%------------------------------------------------------------------------------
%% @private
%% handles the 'protocol' expected by call/2
%%------------------------------------------------------------------------------
receive_loop(0) ->
    ok;
receive_loop(NumTerms) ->
    receive
        ?MESSAGE({Ref, Pid, _Term}) when is_reference(Ref) ->
            Pid ! {handled, Ref, self()},
            receive_loop(NumTerms - 1);
        Term ->
            exit({unexpected_term, Term})
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
for(Num, Fun)           -> for(Num, Fun, []).
for(Num, Fun, Args)     -> for_loop(Num, Fun, Args, []).
for_loop(0, _,  _, Acc) -> lists:reverse(Acc);
for_loop(I, F, As, Acc) -> for_loop(I - 1, F, As, [apply(F, [I | As]) | Acc]).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
setup() ->
    fun() ->
            ok = distribute(master),
            {ok, Apps} = application:ensure_all_started(lbm_nomq),
            error_logger:tty(false),
            Apps
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
teardown() ->
    fun(Apps) ->
            [application:stop(App) || App <- Apps]
    end.

%%------------------------------------------------------------------------------
%% @private
%% Make this node a distributed node.
%%------------------------------------------------------------------------------
distribute(Name) ->
    os:cmd("epmd -daemon"),
    case net_kernel:start([Name]) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok;
        Error                         -> Error
    end.

%%------------------------------------------------------------------------------
%% @private
%% Start a slave node and setup its environment (code path, applications, ...).
%%------------------------------------------------------------------------------
slave_setup(Name) ->
    {ok, Node} = slave:start_link(hostname(), Name),
    true = lists:member(Node, nodes()),
    slave_setup_env(Node),
    {ok, Node}.

%%------------------------------------------------------------------------------
%% @private
%% Setup the slave node environment (code path, applications, ...).
%%------------------------------------------------------------------------------
slave_setup_env(Node) ->
    Paths = code:get_path(),
    PathFun = fun() -> [code:add_patha(P)|| P <- Paths] end,
    ok = slave_execute(Node, PathFun),
    AppFun = fun() -> {ok, _} = application:ensure_all_started(lbm_nomq) end,
    ok = slave_execute(Node, AppFun).

%%------------------------------------------------------------------------------
%% @private
%% Execute `Fun' on the given node.
%%------------------------------------------------------------------------------
slave_execute(Node, Fun) ->
    Pid = spawn_link(Node, Fun),
    receive
        {'EXIT', Pid, normal} -> ok;
        {'DOWN', Pid, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
hostname() -> list_to_atom(element(2, inet:gethostname())).
