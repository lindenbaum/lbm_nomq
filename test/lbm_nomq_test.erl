%%%=============================================================================
%%%
%%%               |  o __   _|  _  __  |_   _       _ _   (TM)
%%%               |_ | | | (_| (/_ | | |_) (_| |_| | | |
%%%
%%% @copyright (C) 2014, Lindenbaum GmbH
%%%
%%% Permission to use, copy, modify, and/or distribute this software for any
%%% purpose with or without fee is hereby granted, provided that the above
%%% copyright notice and this permission notice appear in all copies.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
%%%
%%%=============================================================================

-module(lbm_nomq_test).

-include_lib("eunit/include/eunit.hrl").

-export([call/3]).

-define(TOPIC, topic).

-define(MESSAGE(X), {message, X}).

-define(DBG(Fmt, Args), io:format(standard_error, Fmt, Args)).

-define(DOWN(Ref, Pid), receive {'DOWN', Ref, process, Pid, normal} -> ok end).
-define(DOWN_FUN, fun(_) -> ?DOWN(_, _) end).

-define(EXIT(Pid), receive {'EXIT', Pid, normal} -> ok end).
-define(EXIT_FUN, fun(_) -> ?EXIT(_) end).

-define(TIMEOUT, 30).

%%%=============================================================================
%%% TESTS
%%%=============================================================================

all_test_() ->
    {foreach, setup(), teardown(),
     [
      fun report_header/0,
      fun mfa_undef/0,
      fun basic_subscribe/0,
      fun no_subscribers/0,
      fun no_subscribers_no_wait/0,
      fun subscribe_with_monitor_option/0,
      fun basic_push/0,
      fun multiple_subscribers/0,
      fun late_subscribe/0,
      {spawn, fun one_topic_many_concurrent_pushers/0},
      {spawn, fun one_topic_many_concurrent_pushers_subscribers_exit/0},
      {timeout, ?TIMEOUT, {spawn, fun one_topic_many_concurrent_pushers_distributed_setup/0}},
      {timeout, ?TIMEOUT, {spawn, fun many_topics_many_concurrent_pushers/0}},
      {timeout, ?TIMEOUT, {spawn, fun many_topics_many_concurrent_pushers_distributed_setup/0}}
     ]}.

mfa_undef() ->
    ?assertEqual(
       {error, nofile},
       lbm_nomq:subscribe(?TOPIC, {module, function, []})),
    ?assertEqual(
       {error, {undef, lala}},
       lbm_nomq:subscribe(?TOPIC, {gen_server, lala, []})).

basic_subscribe() ->
    {S, SR} = spawn_monitor(subscriber(?TOPIC, 0)),
    receive {subscribed, S} -> ok end,
    ?assertEqual(1, length(lbm_nomq:subscribers(?TOPIC))),
    ?DOWN(SR, S).

no_subscribers() ->
    ?assertEqual(0, length(lbm_nomq:subscribers(?TOPIC))),
    try lbm_nomq:push(?TOPIC, msg, 100) of
        _ -> throw(test_failed)
    catch
        exit:{timeout, {lbm_nomq, push, [?TOPIC, msg, 100, []]}} ->
            ok
    end.

no_subscribers_no_wait() ->
    ?assertEqual(0, length(lbm_nomq:subscribers(?TOPIC))),
    try lbm_nomq:push(?TOPIC, msg, 100, [no_wait]) of
        _ -> throw(test_failed)
    catch
        exit:{no_subscribers, {lbm_nomq, push, [?TOPIC, msg, 100, [no_wait]]}} ->
            ok
    end.

subscribe_with_monitor_option() ->
    {S1, SR1} = spawn_monitor(subscriber(?TOPIC, 1, true)),
    receive {subscribed, S1} -> ok end,
    ?assertEqual(1, length(lbm_nomq:subscribers(?TOPIC))),
    exit(S1, kill),
    receive {'DOWN', SR1, process, S1, killed} -> ok end,

    {S2, SR2} = spawn_monitor(subscriber(?TOPIC, 1, true)),
    receive {subscribed, S2} -> ok end,
    ?assertEqual(1, length(lbm_nomq:subscribers(?TOPIC))),
    exit(S2, kill),
    receive {'DOWN', SR2, process, S2, killed} -> ok end.

basic_push() ->
    Messages = [msg1, msg2, msg3],

    {S, SR} = spawn_monitor(subscriber(?TOPIC, length(Messages))),
    receive {subscribed, S} -> ok end,
    ?assertEqual(1, length(lbm_nomq:subscribers(?TOPIC))),

    ok = lbm_nomq:info(),

    {P, PR} = spawn_monitor(pusher(?TOPIC, Messages)),

    ?DOWN(SR, S),
    ?DOWN(PR, P),

    {'EXIT', {timeout, _}} = (catch lbm_nomq:push(?TOPIC, msg, 100)).

multiple_subscribers() ->
    {S1, S1R} = spawn_monitor(subscriber(?TOPIC, 1)),
    receive {subscribed, S1} -> ok end,
    ?assertEqual(1, length(lbm_nomq:subscribers(?TOPIC))),

    {S2, S2R} = spawn_monitor(subscriber(?TOPIC, 1)),
    receive {subscribed, S2} -> ok end,
    ?assertEqual(2, length(lbm_nomq:subscribers(?TOPIC))),

    {S3, S3R} = spawn_monitor(subscriber(?TOPIC, 1)),
    receive {subscribed, S3} -> ok end,
    ?assertEqual(3, length(lbm_nomq:subscribers(?TOPIC))),

    {P, PR} = spawn_monitor(pusher(?TOPIC, [stop, stop, stop])),

    ?DOWN(S1R, S1),
    ?DOWN(S2R, S2),
    ?DOWN(S3R, S3),
    ?DOWN(PR, P),

    {'EXIT', {timeout, _}} = (catch lbm_nomq:push(?TOPIC, msg, 100)).

late_subscribe() ->
    Messages = [msg1, msg2, msg3],

    {P, PR} = spawn_monitor(pusher(?TOPIC, Messages)),

    timer:sleep(500),

    {S, SR} = spawn_monitor(subscriber(?TOPIC, length(Messages))),
    receive {subscribed, S} -> ok end,
    ?assertEqual(1, length(lbm_nomq:subscribers(?TOPIC))),

    ?DOWN(PR, P),
    ?DOWN(SR, S),

    {'EXIT', {timeout, _}} = (catch lbm_nomq:push(?TOPIC, msg, 100)).

one_topic_many_concurrent_pushers() ->
    Messages = 100000,
    Pusher = fun(Message) ->
                     spawn_monitor(pusher(?TOPIC, [Message]))
             end,
    Test = fun() ->
                   spawn_monitor(subscriber(?TOPIC, Messages)),
                   for(Messages, Pusher),
                   for(Messages + 1, ?DOWN_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, 1, 1, 1, Time).

one_topic_many_concurrent_pushers_subscribers_exit() ->
    Subscribers = 4,
    Messages = 100000,
    NumTermsPerSubscriber = Messages div Subscribers,

    Pusher = fun(Message) ->
                     spawn_monitor(pusher(?TOPIC, [Message]))
             end,
    Subscriber = fun() ->
                         spawn_monitor(subscriber(?TOPIC, NumTermsPerSubscriber))
                 end,
    Test = fun() ->
                   {S1, SR1} = Subscriber(),
                   for(Messages, Pusher),

                   ?DOWN(SR1, S1),
                   [Subscriber() || _ <- lists:seq(1, Subscribers - 1)],

                   for(Messages + (Subscribers - 1), ?DOWN_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, 1, Subscribers, 1, Time).

one_topic_many_concurrent_pushers_distributed_setup() ->
    process_flag(trap_exit, true),

    {ok, Slave1} = slave_setup(slave1),
    {ok, Slave2} = slave_setup(slave2),
    {ok, Slave3} = slave_setup(slave3),

    Nodes = [Slave3, Slave2, Slave1, node()],
    NumNodes = length(Nodes),
    Messages = 100000,
    NumPushesPerNode = Messages div NumNodes,

    Pusher = fun(Message, Node) ->
                     spawn_link(Node, pusher(?TOPIC, [Message]))
             end,
    Subscriber = fun(Node) ->
                         spawn_link(Node, subscriber(?TOPIC, NumPushesPerNode))
                 end,
    Test = fun() ->
                   S0 = Subscriber(node()),
                   foreach(
                     fun(N) ->
                             for(NumPushesPerNode, Pusher, [N])
                     end, Nodes),

                   ?EXIT(S0),
                   foreach(Subscriber, Nodes -- [node()]),
                   for(Messages + (NumNodes - 1), ?EXIT_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, 1, NumNodes, NumNodes, Time).

many_topics_many_concurrent_pushers() ->
    Topics = 5000,
    Messages = 100000,
    MessagesPerTopic = Messages div Topics,
    Pusher = fun(Topic) ->
                     fun(Message) ->
                             spawn_monitor(pusher(Topic, [Message]))
                     end
             end,
    Subscriber = fun(Topic) ->
                         spawn_monitor(subscriber(Topic, MessagesPerTopic))
                 end,
    Test = fun() ->
                   for(Topics, Subscriber),
                   foreach(
                     fun(P) ->
                             for(MessagesPerTopic, P)
                     end, for(Topics, Pusher)),
                   for(Topics + Messages, ?DOWN_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, Topics, Topics, 1, Time).

many_topics_many_concurrent_pushers_distributed_setup() ->
    process_flag(trap_exit, true),

    {ok, Slave1} = slave_setup(slave1),
    {ok, Slave2} = slave_setup(slave2),
    {ok, Slave3} = slave_setup(slave3),

    Nodes = [Slave3, Slave2, Slave1, node()],
    NumNodes = length(Nodes),
    Topics = 5000,
    TopicsPerNode = Topics div NumNodes,

    Messages = 100000,
    MessagesPerTopic = Messages div Topics,
    MessagesPerTopicPerNode = MessagesPerTopic div NumNodes,

    Pusher = fun(Topic) ->
                     fun(Message, Node) ->
                             spawn_link(Node, pusher(Topic, [Message]))
                     end
             end,
    Subscriber = fun(Topic) ->
                         fun(Node) ->
                                 spawn_link(Node, subscriber(Topic, MessagesPerTopic))
                         end
                 end,
    Test = fun() ->
                   Ps = for(Topics, Pusher),
                   {SsList , []} = lists:mapfoldr(
                                     fun(_, SsIn) ->
                                             lists:split(TopicsPerNode, SsIn)
                                     end, for(Topics, Subscriber), Nodes),

                   foreach(
                     fun(N) ->
                             foreach(
                               fun(P) ->
                                       for(MessagesPerTopicPerNode, P, [N])
                               end, Ps)
                     end, Nodes),
                   foreach(
                     fun({N, Ss}) ->
                             foreach(fun(S) -> S(N) end, Ss)
                     end, lists:zip(Nodes, SsList)),
                   for(Messages + Topics, ?EXIT_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, Topics, Topics, NumNodes, Time).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% push all terms in a tight loop
%%------------------------------------------------------------------------------
pusher(Topic, Terms) ->
    fun() ->
            [ok = lbm_nomq:push(Topic, Term, ?TIMEOUT * 1000) || Term <- Terms]
    end.

%%------------------------------------------------------------------------------
%% @private
%% Subscribe, receive the given number of terms and exit with reason normal.
%%------------------------------------------------------------------------------
subscriber(Topic, NumTerms) ->
    subscriber(Topic, NumTerms, false).
subscriber(Topic, NumTerms, WithMonitoring) ->
    Parent = self(),
    fun() ->
            case WithMonitoring of
                true  -> Options = [{monitor, self()}];
                false -> Options = []
            end,
            MFA = {?MODULE, call, [self()]},
            ok = lbm_nomq:subscribe(Topic, MFA, Options),
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
foreach(F, L) -> ok = lists:foreach(F, L).

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
            setup_apps()
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-ifdef(DEBUG).
setup_apps() ->
    application:load(sasl),
    {ok, Apps} = application:ensure_all_started(lbm_nomq),
    Apps.
-else.
setup_apps() ->
    application:load(sasl),
    error_logger:tty(false),
    ok = application:set_env(sasl, sasl_error_logger, false),
    {ok, Apps} = application:ensure_all_started(lbm_nomq),
    Apps.
-endif.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
teardown() ->
    fun(Apps) ->
            [application:stop(App) || App <- Apps],
            error_logger:tty(true)
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
    ok = slave_execute(Node, fun() -> [code:add_patha(P)|| P <- Paths] end),
    ok = slave_execute(Node, fun() -> setup_apps() end).

%%------------------------------------------------------------------------------
%% @private
%% Execute `Fun' on the given node.
%%------------------------------------------------------------------------------
slave_execute(Node, Fun) ->
    Pid = spawn_link(Node, Fun),
    receive
        {'EXIT', Pid, normal} -> ok;
        {'EXIT', Pid, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
report_header() ->
    ?DBG("~n", []),
    ?DBG("MESSAGES | TOPICS | RECEIVERS | NODES | MILLISECONDS~n", []),
    ?DBG("---------+--------+-----------+-------+-------------~n", []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
report(Messages, Topics, Receivers, Nodes, MicroSeconds) ->
    ?DBG("~8s | ~6s | ~9s | ~5s | ~w~n",
         [io_lib:write(Messages),
          io_lib:write(Topics),
          io_lib:write(Receivers),
          io_lib:write(Nodes),
          MicroSeconds / 1000]).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
hostname() -> list_to_atom(element(2, inet:gethostname())).
