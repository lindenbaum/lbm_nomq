%%%=============================================================================
%%%
%%%               |  o __   _|  _  __  |_   _       _ _   (TM)
%%%               |_ | | | (_| (/_ | | |_) (_| |_| | | |
%%%
%%% @author Sven Heyll <sven.heyll@lindenbaum.eu>
%%% @author Tobias Schlager <tobias.schlager@lindenbaum.eu>
%%% @author Timo Koepke <timo.koepke@lindenbaum.eu>
%%% @copyright (C) 2014, Lindenbaum GmbH
%%%
%%% @doc
%%% A simple implementation of a reliable message queue mechanism.
%%%
%%% It is based on the principle, that messages are safest when they reside in
%%% the originator until they have been processed by a subscriber. This is
%%% achieved by a mechanism similar to a blocking queue. The originator will be
%%% blocked until the message has been received *and* handled by exactly one
%%% subscriber. Thus, this mechanism is well-suited for applications with many
%%% concurrent producers that produce a moderate amount of messages each.
%%%
%%% In a nutshell the application provides distributed `gen:call` to one
%%% subscriber of a group, with failover/redundancy and grouping of subscribers.
%%% @end
%%%=============================================================================

-module(lbm_nomq).

-behaviour(application).
-behaviour(supervisor).

%% API
-export([subscribe_server/1,
         subscribe_fsm/1,
         subscribe/2,
         push/2,
         push/3]).

%% Application callbacks
-export([start/2, stop/1]).

%% supervisor callbacks
-export([init/1]).

-type topic()  :: any().
-type mfargs() :: {module(), atom(), [term()]}.

-export_type([topic/0, mfargs/0]).

-include("lbm_nomq.hrl").

-define(SUBSCRIBER(M, F, As), #lbm_nomq_subscr{m = M, f = F, as = As}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link subscribe/2} with `MFA' set to
%% `{gen_server, call, [self()]}'. The subscribed `gen_server' will receive the
%% pushed messages in its `handle_call/3' function.
%% @end
%%------------------------------------------------------------------------------
-spec subscribe_server(topic()) -> ok | {error, term()}.
subscribe_server(Topic) -> subscribe(Topic, {gen_server, call, [self()]}).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link subscribe/2} with `MFA' set to
%% `{gen_fsm, sync_send_event, [self()]}'.  The subscribed `gen_fsm' will
%% receive the pushed messages in its `StateName/3' function.
%% @end
%%------------------------------------------------------------------------------
-spec subscribe_fsm(topic()) -> ok | {error, term()}.
subscribe_fsm(Topic) -> subscribe(Topic, {gen_fsm, sync_send_event, [self()]}).

%%------------------------------------------------------------------------------
%% @doc
%% Subscribes `MFA' as listener for a certain topic. Messages will be delivered
%% to the process using `erlang:appy(M, F, As ++ Message)'. The function must
%% adhere to the `gen:call/4' protocol. This means that if the function returns
%% without exiting, the message is considered to be consumed successfully.
%%
%% It is possible to have multiple subscribers for a topic. However, a message
%% will be pushed to at most one subscriber. The subscriber for a message will
%% be chosen randomly.
%%
%% It is recommended to have a limited amount of subscribers for a topic, e.g.
%% let's say a maximum of 3-5. Subscribers will be unsubscribed automatically
%% when message delivery fails, e.g. when a subscriber process exits.
%% @end
%%------------------------------------------------------------------------------
-spec subscribe(topic(), mfargs()) -> ok | {error, term()}.
subscribe(Topic, {M, F, As}) when is_list(As) ->
    case code:ensure_loaded(M) of
        {module, M} ->
            case erlang:function_exported(M, F, length(As) + 1) of
                true ->
                    add_subscriber(Topic, ?SUBSCRIBER(M, F, As));
                false when M =:= erlang ->
                    add_subscriber(Topic, ?SUBSCRIBER(M, F, As));
                false ->
                    {error, {undef, {F, length(As) + 1}}}
            end;
        Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link push/3} with `Timeout' set to `infinity'.
%% @end
%%------------------------------------------------------------------------------
-spec push(topic(), term()) -> any().
push(Topic, Message) -> push(Topic, Message, infinity).

%%------------------------------------------------------------------------------
%% @doc
%% Send a message for a specific topic. This will block the calling process
%% until the message has successfully consumed by exactly one subscriber. If
%% there are no subscribers available for `Topic' the process will wait for
%% subscribers up to `Timeout' millis.
%%
%% If a push finally fails, the caller will be exited with
%% `exit({timeout, {lbm_nomq, push, [Topic, Msg, Timeout]}})'.
%% @end
%%------------------------------------------------------------------------------
-spec push(topic(), term(), integer() | infinity) -> any().
push(Topic, Message, Timeout) ->
    push_loop(get_subscribers(Topic), [], Topic, Message, Timeout).

%%%=============================================================================
%%% Application callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
start(_StartType, _StartArgs) -> supervisor:start_link(?MODULE, []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
stop(_State) -> ok.

%%%=============================================================================
%%% supervisor callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) -> {ok, {{one_for_one, 0, 1}, [lbm_nomq_dist:spec()]}}.

%%%=============================================================================
%%% internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
add_subscriber(Topic, Subscriber) ->
    lbm_nomq_dist:add_subscriber(Topic, Subscriber).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
get_subscribers(Topic) -> shuffle(lbm_nomq_dist:get_subscribers(Topic)).

%%------------------------------------------------------------------------------
%% @private
%% Try to push a message reliably to exactly one subscriber. If no good
%% subscribers can be found, the loop will block the caller until either new
%% subscribers register and handle the message or the given timeout expires.
%%------------------------------------------------------------------------------
push_loop([], BadSs, Topic, Msg, Timeout) ->
    {ok, PushRef} = lbm_nomq_dist:add_waiting(Topic, BadSs),
    case wait(Topic, PushRef, Timeout, send_after(Timeout, Topic, PushRef)) of
        {ok, {Subscribers, Time}} ->
            push_loop(Subscribers, [], Topic, Msg, Time);
        {error, timeout} ->
            exit({timeout, {?MODULE, push, [Topic, Msg, Timeout]}})
    end;
push_loop([S = ?SUBSCRIBER(M, F, As) | Ss], BadSs, Topic, Msg, Timeout) ->
    try erlang:apply(M, F, As ++ [Msg]) of
        Result ->
            ok = lbm_nomq_dist:del_subscribers(Topic, BadSs),
            Result
    catch
        exit:{timeout, _} ->
            %% subscriber is not dead, only overloaded
            push_loop(Ss, BadSs, Topic, Msg, Timeout);
        exit:_  ->
            push_loop(Ss, [S | BadSs], Topic, Msg, Timeout);
        error:badarg when M =:= gen_fsm, is_atom(hd(As)) ->
            %% for gen_fsm, when sending to invalid (local) registered name
            push_loop(Ss, [S | BadSs], Topic, Msg, Timeout)
    end.

%%------------------------------------------------------------------------------
%% @private
%% Wait for either a timeout message (created with {@link send_after/3}) or a
%% subscriber update from {@link lbm_nomq_dist}. This will also try to leave the
%% callers message queue in a consistent state avoiding ghost messages beeing
%% send to the calling process.
%%------------------------------------------------------------------------------
wait(Topic, PushRef, Timeout, TimerRef) ->
    receive
        ?UPDATE_MSG(PushRef, Topic, timeout) ->
            ok = lbm_nomq_dist:del_waiting(Topic, PushRef),
            ok = flush_updates(Topic, PushRef),
            {error, timeout};
        ?UPDATE_MSG(PushRef, Topic, Subscribers) ->
            Time = cancel_timer(TimerRef, Timeout, Topic, PushRef),
            {ok, {Subscribers, Time}}
    end.

%%------------------------------------------------------------------------------
%% @private
%% Flush all pending update messages from the callers message queue.
%%------------------------------------------------------------------------------
flush_updates(Topic, PushRef) ->
    receive
        ?UPDATE_MSG(PushRef, Topic, _) ->
            flush_updates(Topic, PushRef)
    after
        50 -> ok
    end.

%%------------------------------------------------------------------------------
%% @private
%% Start a timer for the calling process, does nothing if `Timeout' is set to
%% infinity.
%%------------------------------------------------------------------------------
send_after(Timeout, Topic, Ref) when is_integer(Timeout) ->
    erlang:send_after(Timeout, self(), ?UPDATE_MSG(Ref, Topic, timeout));
send_after(infinity, _Topic, _Ref) ->
    erlang:make_ref().

%%------------------------------------------------------------------------------
%% @private
%% Cancel a timer created with {@link send_after/3}. And reports the remaining
%% time. If the timer already expired the function tries to remove the timeout
%% message from the process message queue.
%%------------------------------------------------------------------------------
cancel_timer(TimerRef, Timeout, Topic, Ref) ->
    case erlang:cancel_timer(TimerRef) of
        false when is_integer(Timeout) ->
            %% cleanup the message queue in case timer was already delivered
            receive ?UPDATE_MSG(Ref, Topic, _) -> 0 after 0 -> 0 end;
        false when Timeout =:= infinity ->
            Timeout;
        Time when is_integer(Time) ->
            Time
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
shuffle(L) when is_list(L) ->
    shuffle(L, length(L)).
shuffle(L, Len) ->
    [E || {_, E} <- lists:sort([{crypto:rand_uniform(0, Len), E} || E <- L])].
