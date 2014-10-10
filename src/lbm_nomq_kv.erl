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
%%% An implementation of the {@link lbm_nomq_dist} behaviour based on a the
%%% `lbm_kv' application (distributed Mnesia).
%%%
%%% The table ?MODULE contains the following terms:
%%% `{?MODULE, Subscriber, Subscriber}' with
%%%    Subscriber :: `{subscriber, Topic, #lbm_nomq_subscr{}}'
%%% @end
%%%=============================================================================

-module(lbm_nomq_kv).

-behaviour(gen_server).
-behaviour(lbm_nomq_dist).
-behaviour(lbm_kv).

%% Internal API
-export([start_link/0]).

%% lbm_nomq_dist callbacks
-export([spec/1,
         add_subscriber/2,
         del_subscribers/2,
         get_subscribers/1,
         add_waiting/2,
         del_waiting/2]).

%% lbm_kv callbacks
-export([resolve_conflict/1]).

%% gen_server callbacks
-export([init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         code_change/3,
         terminate/2]).

-include("lbm_nomq.hrl").

-define(SUBSCR(Topic, Subscriber), {subscriber, Topic, Subscriber}).

-type subscription() :: ?SUBSCR(lbm_nomq:topic(), #lbm_nomq_subscr{}).

%%%=============================================================================
%%% Internal API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% Simply start the server (registered).
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() -> gen_server:start_link({local, ?BACKEND_NAME}, ?MODULE, [], []).

%%%=============================================================================
%%% lbm_nomq_dist callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec spec(term()) -> supervisor:child_spec().
spec(Id) -> {Id, {?MODULE, start_link, []}, permanent, 1000, worker, [?MODULE]}.

%%------------------------------------------------------------------------------
%% @private
%% NOTE: Do not call this function from a massive amount of processes, since
%% Mnesia is not good handling thousands of concurrent transactions.
%%------------------------------------------------------------------------------
-spec add_subscriber(lbm_nomq:topic(), #lbm_nomq_subscr{}) -> ok.
add_subscriber(Topic, Subscriber = #lbm_nomq_subscr{}) ->
    KeyAndValue = ?SUBSCR(Topic, Subscriber),
    true = is_list(lbm_kv:put(?MODULE, KeyAndValue, KeyAndValue)),
    ok.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec del_subscribers(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> ok.
del_subscribers(_Topic, []) ->
    ok;
del_subscribers(Topic, BadSs) ->
    gen_server:cast(?BACKEND_NAME, {del_subscribers, Topic, BadSs}).

%%------------------------------------------------------------------------------
%% @private
%% NOTE: Only dirty reads can handle 10000+ concurrent reads.
%%------------------------------------------------------------------------------
-spec get_subscribers(lbm_nomq:topic()) -> [#lbm_nomq_subscr{}].
get_subscribers(Topic) ->
    [S || {?SUBSCR(_, S), _} <- lbm_kv:get(?MODULE, ?SUBSCR(Topic, '_'), dirty)].

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec add_waiting(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> {ok, reference()}.
add_waiting(Topic, BadSs) ->
    Reference = make_ref(),
    Request = {add_waiting, Topic, {self(), Reference}, BadSs},
    {gen_server:cast(?BACKEND_NAME, Request), Reference}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec del_waiting(lbm_nomq:topic(), reference()) -> ok.
del_waiting(Topic, Reference) ->
    gen_server:cast(?BACKEND_NAME, {del_waiting, Topic, {self(), Reference}}).

%%%=============================================================================
%%% lbm_kv callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% Basically ignore conflicting `lbm_kv' tables, since bad subscribers will be
%% sorted out on the fly and updates will propagate new subscribers
%% automatically.
%%------------------------------------------------------------------------------
resolve_conflict(_Node) -> ok.

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

-record(state, {
          s :: [subscription()],
          w :: #{lbm_nomq:topic() => [{pid(), reference()}]}}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    ok = lbm_kv:create(?MODULE),
    {ok, _} = mnesia:subscribe({table, ?MODULE, simple}),
    {ok, #state{s = [K || {K, _} <- lbm_kv:get_all(?MODULE)], w = maps:new()}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call(_Request, _From, State) -> {reply, undef, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast({add_waiting, Topic, Who, BadSs}, State) ->
    NewState = state_del_subscribers(Topic, BadSs, State),
    {noreply, state_add_waiting(Topic, Who, NewState)};
handle_cast({del_waiting, Topic, Who}, State) ->
    {noreply, state_del_waiting(Topic, Who, State)};
handle_cast({del_subscribers, Topic, BadSs}, State) ->
    {noreply, state_del_subscribers(Topic, BadSs, State)};
handle_cast(_Request, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({mnesia_table_event, Event}, State) ->
    {noreply, handle_mnesia(Event, State)};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, _State) -> ok.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% Handles Mnesia table events, updates subscriptions in state.
%%------------------------------------------------------------------------------
handle_mnesia({write, {?MODULE, Subscription, _}, _}, State) ->
    state_add_subscription(Subscription, State);
handle_mnesia({delete_object, {?MODULE, Subscription, _}, _}, State) ->
    state_rm_subscription(Subscription, State);
handle_mnesia({delete, {?MODULE, Subscription}, _}, State) ->
    state_rm_subscription(Subscription, State);
handle_mnesia(_, State) ->
    State.

%%------------------------------------------------------------------------------
%% @private
%% Add a subscription for a topic into state. If this is a new ubscription,
%% waiting processes will be notified (and removed).
%%------------------------------------------------------------------------------
state_add_subscription(Subscription = ?SUBSCR(Topic, _), State) ->
    case lists:member(Subscription, State#state.s) of
        true ->
            State; %% no update, no notification
        false ->
            %% actual update, notify waiting processes
            NewState = State#state{s = [Subscription | State#state.s]},
            notify_on_subscribe(Topic, NewState)
    end.

%%------------------------------------------------------------------------------
%% @private
%% Remove a subscription for a topic from the state. The function returns
%% whether the state of subscriptions actually changed.
%%------------------------------------------------------------------------------
state_rm_subscription(Subscription, State = #state{s = S}) ->
    State#state{s = S -- [Subscription]}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
state_get_subscribers(Topic, #state{s = S}) ->
    [Subscriber || ?SUBSCR(T, Subscriber) <- S, T =:= Topic].

%%------------------------------------------------------------------------------
%% @private
%% Delete subscribers detected as bad. If the process state indicates a change
%% in subscribers, try to update lbm_kv subscriptions.
%%------------------------------------------------------------------------------
state_del_subscribers(Topic, Subscribers, State = #state{s = S}) ->
    BadS = [?SUBSCR(Topic, Subscriber) || Subscriber <- Subscribers],
    case S -- BadS of
        S ->
            %% no change, no write attempt
            State;
        NewS ->
            true = is_list(lbm_kv:del(?MODULE, BadS)),
            State#state{s = NewS}
    end.

%%------------------------------------------------------------------------------
%% @private
%% Adds a process to the processes waiting for subscriptions for a certain
%% topic. If valid subscriptions for the topic are available, the process will
%% not be added and notified instead.
%%------------------------------------------------------------------------------
state_add_waiting(Topic, Who, State = #state{w = W}) ->
    case state_get_subscribers(Topic, State) of
        [] ->
            %% no subscribers, need to wait for new subscriptions
            State#state{w = maps:put(Topic, [Who | maps:get(Topic, W, [])], W)};
        Subscribers ->
            %% still subscribers available, notify immediatelly
            notify([Who], Topic, Subscribers),
            State
    end.

%%------------------------------------------------------------------------------
%% @private
%% Remove and return all waiting processes for a certain topic.
%%------------------------------------------------------------------------------
state_del_waiting(Topic, State = #state{w = W}) ->
    Whos = maps:get(Topic, W, []),
    {Whos, State#state{w = maps:remove(Topic, W)}}.

%%------------------------------------------------------------------------------
%% @private
%% Remove a waiting process for a certain topic.
%%------------------------------------------------------------------------------
state_del_waiting(Topic, Who, State = #state{w = W}) ->
    state_del_waiting(Topic, Who, maps:get(Topic, W, []), State).

%%------------------------------------------------------------------------------
%% @private
%% Remove a waiting process for a topic. Deletes the topic entry, if no more
%% waiting processes are available.
%%------------------------------------------------------------------------------
state_del_waiting(_Topic, _Who, [], State) ->
    State;
state_del_waiting(Topic, Who, [Who], State = #state{w = W}) ->
    State#state{w = maps:remove(Topic, W)};
state_del_waiting(Topic, Who, Waiting, State = #state{w = W}) ->
    State#state{w = maps:put(Topic, Waiting -- [Who], W)}.

%%------------------------------------------------------------------------------
%% @private
%% Notify all waiting processes for a certain topic.
%%------------------------------------------------------------------------------
notify_on_subscribe(Topic, State) ->
    case state_del_waiting(Topic, State) of
        {[], NewState} ->
            NewState;
        {Whos, NewState} ->
            notify(Whos, Topic, state_get_subscribers(Topic, State)),
            NewState
    end.

%%------------------------------------------------------------------------------
%% @private
%% Notify waiting processes, by ordinary message.
%%------------------------------------------------------------------------------
notify(Whos, Topic, Subscribers) ->
    [Pid ! ?UPDATE_MSG(Ref, Topic, Subscribers) || {Pid, Ref} <- Whos].
