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
%%% @doc
%%% A simple, distributed, message distribution framework with message queue
%%% semantics (publish/subscribe), that is actually not really a message queue.
%%% It uses Erlang terms and distributed Erlang.
%%%
%%% `lbm_nomq' is based on the principle, that messages are safest when they
%%% reside in the originator until they have been delivered to/processed by a
%%% subscriber. This is achieved by a mechanism similar to a blocking queue. The
%%% originator will be blocked until the message has been received (and
%%% eventually handled) by exactly one subscriber. Thus, this mechanism is
%%% well-suited for applications with many concurrent producers that produce a
%%% moderate amount of messages each.
%%%
%%% In a nutshell `lbm_nomq' allows sending terms over logical, topic-based
%%% channels to subscribed MFAs. In the case, the subscribed MFAs adheres to
%%% `gen:call/4' semantics, message distribution is guaranteed to be reliable.
%%%
%%% It is possible to have multiple subscribers for a topic, however, `lbm_nomq'
%%% will deliver a message to exactly one of the subscribed MFAs (randomly
%%% chosen). This is useful to support active/active redundant subscribers
%%% sharing a common state.
%%% @end
%%%=============================================================================

-module(lbm_nomq).

-behaviour(application).
-behaviour(supervisor).

%% API
-export([subscribe_server/1,
         subscribe_fsm/1,
         subscribe/2,
         subscribe/3,
         subscribers/1,
         push/2,
         push/3,
         push/4,
         info/0]).

%% Application callbacks
-export([start/2, stop/1]).

%% supervisor callbacks
-export([init/1]).

-type topic()            :: any().
-type mfargs()           :: {module(), atom(), [term()]}.
-type subscribe_option() :: {monitor, PidOrLocalName :: pid() | atom()}.
-type push_option()      :: no_wait.

-export_type([topic/0, mfargs/0, subscribe_option/0, push_option/0]).

-include("lbm_nomq.hrl").

-define(S(M, F, As), #lbm_nomq_subscr{m = M, f = F, as = As}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link subscribe/2} with `MFAs' set to
%% `{gen_server, call, [self()]}'. The subscribed `gen_server' will receive the
%% pushed messages in its `handle_call/3' function.
%% @end
%%------------------------------------------------------------------------------
-spec subscribe_server(topic()) -> ok | {error, term()}.
subscribe_server(Topic) -> subscribe(Topic, {gen_server, call, [self()]}).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link subscribe/2} with `MFAs' set to
%% `{gen_fsm, sync_send_event, [self()]}'. The subscribed `gen_fsm' will
%% receive the pushed messages in its `StateName/3' function.
%% @end
%%------------------------------------------------------------------------------
-spec subscribe_fsm(topic()) -> ok | {error, term()}.
subscribe_fsm(Topic) -> subscribe(Topic, {gen_fsm, sync_send_event, [self()]}).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link subscribe/3} with `Options' set to `[]'.
%% @end
%%------------------------------------------------------------------------------
-spec subscribe(topic(), mfargs()) -> ok | {error, term()}.
subscribe(Topic, MFAs) -> subscribe(Topic, MFAs, []).

%%------------------------------------------------------------------------------
%% @doc
%% Subscribes `MFAs' as listener for a certain topic. Messages will be delivered
%% to the process using either `erlang:apply(M, F, As ++ [Message, Timeout])' or
%% `erlang:apply(M, F, As ++ [Message])' (whatever is available, where the
%% timeout version is preferred). If the function adheres to the `gen:call/4'
%% protocol message delivery will be reliable. This means that if the function
%% returns without exiting, the message is considered to be consumed
%% successfully.
%%
%% It is possible to have multiple subscribers for a topic. However, a message
%% will be pushed to exactly one subscriber. The subscriber for a message will
%% be chosen randomly.
%%
%% It is recommended to have a limited amount of subscribers for a topic, e.g.
%% let's say a maximum of 3-5. There's no explicit `unsubscribe' in `lbm_nomq'.
%% Subscriptions will be discarded automatically, when the applied `MFAs' raises
%% an exception, error or exit. For functions that always succeed `lbm_nomq'
%% offers the possibility to provide an owner process id that is associated with
%% the subscription, see the `Options' section below for more details.
%%
%% The only option currently supported, is the `{monitor, PidOrLocalName}'
%% option. This should be used when subscribing `MFAs' that do not adhere the
%% `gen:call/4' protocol, since `lbm_nomq' can only remove subscriptions when
%% a push attempt raises an exception, error or exit. Providing a process id or
%% local name associates this process with the given `MFAs' and will lead to
%% unsubscription when the process exits, e.g. to subscribe `gen_server:cast/2'
%% use someting like this:
%% `lbm_nomq:subscribe(Topic, {gen_server, cast, [Pid]}, [{monitor, Pid}])'
%% @end
%%------------------------------------------------------------------------------
-spec subscribe(topic(), mfargs(), [subscribe_option()]) -> ok | {error, term()}.
subscribe(Topic, {M, F, As}, Options) when is_list(As) ->
    case code:ensure_loaded(M) of
        {module, erlang} ->
            %% cannot check for exported functions in the erlang module
            Subscriber = new_subscriber(M, F, As, Options),
            lbm_nomq_dist:add_subscriber(Topic, Subscriber);
        {module, M} ->
            case exported(M, F, As) of
                true ->
                    Subscriber = new_subscriber(M, F, As, Options),
                    lbm_nomq_dist:add_subscriber(Topic, Subscriber);
                false ->
                    {error, {undef, F}}
            end;
        Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Return the subscribers currently registered for a certain topic. The returned
%% list may contain subscribers that are already dead and will be sorted out
%% when pushing the next time.
%% @end
%%------------------------------------------------------------------------------
-spec subscribers(topic()) -> [#lbm_nomq_subscr{}].
subscribers(Topic) -> lbm_nomq_dist:get_subscribers(Topic).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link push/3} with `Timeout' set to `5000'.
%% @end
%%------------------------------------------------------------------------------
-spec push(topic(), term()) -> any().
push(Topic, Message) -> push(Topic, Message, 5000).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link push/4} with `Options' set to `[]'.
%% @end
%%------------------------------------------------------------------------------
-spec push(topic(), term(), timeout()) -> any().
push(Topic, Message, Timeout) -> push(Topic, Message, Timeout, []).

%%------------------------------------------------------------------------------
%% @doc
%% Send a message for a specific topic. This will block the calling process
%% until either the message has been successfully consumed by exactly one
%% subscriber or the `Timeout' millis elapsed. If there are no subscribers
%% available for `Topic' the process will wait for new subscribers (until
%% `Timeout' expires).
%%
%% If a push finally fails, the caller will be exited with
%% `exit({timeout, {lbm_nomq, push, [Topic, Msg, Timeout, Options]}})'. If the
%% calling process decides to catch this error and a subscriber is just late
%% with the reply, it may arrive at any time later into the caller's message
%% queue. The caller must in this case be prepared for this and discard any such
%% garbage messages.
%%
%% The only option currently supported, is the `no_wait' option. If this flag
%% is given, the caller will not wait for subscribers (e.g. only bad or no
%% subscribers for `Topic' could be found) and will be exited immediately with
%% `exit({no_subscribers, {lbm_nomq, push, [Topic, Msg, Timeout, Options]}})'
%% instead.
%% @end
%%------------------------------------------------------------------------------
-spec push(topic(), term(), timeout(), [push_option()]) -> any().
push(Topic, Message, Timeout, Options) ->
    Args = {Topic, Message, Timeout, Options},
    push_loop(get_subscribers(Topic), [], Args, Timeout).

%%------------------------------------------------------------------------------
%% @doc
%% Print topic and subscriber info to stdout.
%% @end
%%------------------------------------------------------------------------------
-spec info() -> ok.
info() -> lbm_nomq_dist:info().

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
new_subscriber(M, F, As, [{monitor, Name}]) when is_atom(Name) ->
    #lbm_nomq_subscr{m = M, f = F, as = As, mon = erlang:whereis(Name)};
new_subscriber(M, F, As, [{monitor, Pid}]) when is_pid(Pid) ->
    #lbm_nomq_subscr{m = M, f = F, as = As, mon = Pid};
new_subscriber(M, F, As, _Opts) ->
    #lbm_nomq_subscr{m = M, f = F, as = As}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
get_subscribers(Topic) -> shuffle(subscribers(Topic)).

%%------------------------------------------------------------------------------
%% @private
%% Try to push a message to exactly one subscriber. If no good subscribers can
%% be found, the loop will block the caller until either new subscribers
%% register and handle the message or the given timeout expires (unless the
%% `no_wait' option is specified).
%%------------------------------------------------------------------------------
push_loop([], BadSs, Args = {Topic, _, _, Opts}, Timeout) ->
    ok = exit_if_no_wait(Opts, Args),
    {ok, PushRef} = lbm_nomq_dist:add_waiting(Topic, BadSs),
    case wait(Topic, PushRef, Timeout, send_after(Timeout, Topic, PushRef)) of
        {ok, {Subscribers, NewTimeout}} ->
            push_loop(Subscribers, [], Args, NewTimeout);
        {error, timeout} ->
            exit({timeout, {?MODULE, push, tuple_to_list(Args)}})
    end;
push_loop([S = ?S(M, F, As) | Ss], BadSs, Args = {Topic, Msg, _, _}, Timeout) ->
    StartTimestamp = os:timestamp(),
    try apply_mfas(M, F, As, Msg, Timeout) of
        Result ->
            ok = lbm_nomq_dist:del_subscribers(Topic, BadSs),
            Result
    catch
        exit:{timeout, _} ->
            %% subscriber is not dead, only overloaded... anyway Timeout is over
            exit({timeout, {?MODULE, push, tuple_to_list(Args)}});
        _:_  ->
            NewTimeout = remaining_millis(Timeout, StartTimestamp),
            push_loop(Ss, [S | BadSs], Args, NewTimeout)
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

%%------------------------------------------------------------------------------
%% @private
%% Exits the calling process, if the `no_wait' option is specified.
%%------------------------------------------------------------------------------
exit_if_no_wait(Opts, Args) ->
    case lists:member(no_wait, Opts) of
        true ->
            exit({no_subscribers, {?MODULE, push, tuple_to_list(Args)}});
        false ->
            ok
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
apply_mfas(M, F, As, Msg, Timeout) ->
    try
        erlang:apply(M, F, As ++ [Msg, Timeout])
    catch
        error:undef ->
            erlang:apply(M, F, As ++ [Msg])
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
exported(M, F, As) ->
    MinArity = length(As) + 1,
    erlang:function_exported(M, F, MinArity)
        orelse erlang:function_exported(M, F, MinArity + 1).

%%------------------------------------------------------------------------------
%% @private
%% Calculate the remaining value for `Timeout' given a start timestamp.
%%------------------------------------------------------------------------------
remaining_millis(Timeout, StartTimestamp) ->
    Timeout - (to_millis(os:timestamp()) - to_millis(StartTimestamp)).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
to_millis({MegaSecs, Secs, MicroSecs}) ->
    MegaSecs * 1000000000 + Secs * 1000 + MicroSecs div 1000.
