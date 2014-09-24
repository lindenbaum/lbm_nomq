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
%%% blocked until the message has been received *and* handled by (at most) one
%%% subscriber. Thus, this mechanism is well-suited for applications with many
%%% concurrent producers that produce a moderate amount of messages each.
%%%
%%% Subscriptions are propagated and persisted using distributed, RAM-only
%%% Mnesia tables.
%%% @end
%%%=============================================================================

-module(lbm_nomq).

-behaviour(application).
-behaviour(supervisor).
-behaviour(lbm_kv).

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

%% lbm_kv callbacks
-export([resolve_conflict/1]).

-type topic()  :: term().
-type mfargs() :: {module(), function(), [term()]}.

-export_type([topic/0, mfargs/0]).

-include("lbm_nomq.hrl").

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link subscribe/2} with `MFA' set to
%% `{gen_server, call, [self()]}'.
%% @end
%%------------------------------------------------------------------------------
-spec subscribe_server(topic()) -> ok | {error, term()}.
subscribe_server(Topic) -> subscribe(Topic, {gen_server, call, [self()]}).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link subscribe/2} with `MFA' set to
%% `{gen_fsm, sync_send_event, [self()]}'.
%% @end
%%------------------------------------------------------------------------------
-spec subscribe_fsm(topic()) -> ok | {error, term()}.
subscribe_fsm(Topic) -> subscribe(Topic, {gen_fsm, sync_send_event, [self()]}).

%%------------------------------------------------------------------------------
%% @doc
%% Subscribes `MFA' as listener for a certain topic. Messages will be delivered
%% to the process using `erlang:appy(M, F, As ++ Message)'.
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
-spec push(topic(), term()) -> ok | {error, term()}.
push(Topic, Message) -> push(Topic, Message, infinity).

%%------------------------------------------------------------------------------
%% @doc
%% Send a message for a specific topic. This will block the calling process
%% until the message has successfully consumed by exactly one subscriber. If
%% there are no subscribers available for `Topic' the process will wait for
%% subscribers up to `Timeout' millis.
%% @end
%%------------------------------------------------------------------------------
-spec push(topic(), term(), integer() | infinity) -> ok | {error, term()}.
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
init([]) ->
    ok = lbm_kv:create(?MODULE),
    {ok, {{one_for_one, 0, 1}, [spec(lbm_nomq_mon, [])]}}.

%%%=============================================================================
%%% lbm_kv callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
resolve_conflict(_Node) -> ok.

%%%=============================================================================
%%% internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
spec(M, As) -> {M, {M, start_link, As}, permanent, 1000, worker, [M]}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
add_subscriber(Topic, S = ?SUBSCRIBER(_, _, _)) ->
    Fun = fun([Ss]) -> [[S | Ss]]; (_) -> [[S]] end,
    case lbm_kv:update(?MODULE, Topic, Fun) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% @private
%% Only dirty reads can handle 10000+ concurrent reads.
%%------------------------------------------------------------------------------
get_subscribers(Topic) ->
    SsList = lbm_kv:get(?MODULE, Topic, dirty),
    lists:append([shuffle(Ss) || Ss <- SsList, is_list(Ss)]).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
shuffle(L) when is_list(L) ->
    shuffle(L, length(L)).
shuffle(L, Len) ->
    [E || {_, E} <- lists:sort([{crypto:rand_uniform(0, Len), E} || E <- L])].

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
push_loop([], BadSs, Topic, Msg, Timeout) ->
    {ok, PushRef} = lbm_nomq_mon:add_waiting(Topic, BadSs),
    case wait(Topic, PushRef, Timeout, send_after(Timeout, Topic, PushRef)) of
        {ok, {Subscribers, Time}} ->
            push_loop(Subscribers, [], Topic, Msg, Time);
        Error ->
            Error
    end;
push_loop([S = ?SUBSCRIBER(M, F, As) | Ss], BadSs, Topic, Msg, Timeout) ->
    try erlang:apply(M, F, As ++ [Msg]) of
        Result ->
            ok = lbm_nomq_mon:del_subscribers(Topic, BadSs),
            Result
    catch
        exit:_  -> push_loop(Ss, [S | BadSs], Topic, Msg, Timeout);
        _:_     -> push_loop(Ss, BadSs, Topic, Msg, Timeout)
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
wait(Topic, PushRef, Timeout, TimerRef) ->
    receive
        ?UPDATE_MSG(PushRef, Topic, timeout) ->
            lbm_nomq_mon:del_waiting(Topic, PushRef),
            {error, timeout};
        ?UPDATE_MSG(PushRef, Topic, Subscribers) ->
            Time = cancel_timer(TimerRef, Timeout, Topic, PushRef),
            {ok, {Subscribers, Time}}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
send_after(Timeout, Topic, Ref) when is_integer(Timeout) ->
    erlang:send_after(Timeout, self(), ?UPDATE_MSG(Ref, Topic, timeout));
send_after(infinity, _Topic, _Ref) ->
    erlang:make_ref().

%%------------------------------------------------------------------------------
%% @private
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
