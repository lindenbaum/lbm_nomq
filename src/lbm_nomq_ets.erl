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
%%% An implementation of the {@link lbm_nomq_dist} behaviour based on a local
%%% `ETS' table using global locks to distribute subscriptions. This is quite
%%% similar to how `pg2' distributes its internal state.
%%%
%%% The table ?MODULE contains the following terms:
%%% `{{topic, Topic}, Counter}': a topic and its current subscriber counter
%%% `{{subscriber, Topic, #lbm_nomq_subscr{}}}': a subscriber
%%% `{{waiting, Topic, {pid(), reference()}}}': a waiting pusher
%%% @end
%%%=============================================================================

-module(lbm_nomq_ets).

-behaviour(gen_server).
-behaviour(lbm_nomq_dist).

%% Internal API
-export([start_link/0]).

%% lbm_nomq_dist callbacks
-export([spec/1,
         add_subscriber/2,
         del_subscribers/2,
         get_subscribers/1,
         add_waiting/2,
         del_waiting/2]).

%% gen_server callbacks
-export([init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         code_change/3,
         terminate/2]).

-include("lbm_nomq.hrl").

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
%%------------------------------------------------------------------------------
-spec add_subscriber(lbm_nomq:topic(), #lbm_nomq_subscr{}) -> ok.
add_subscriber(Topic, Subscriber = #lbm_nomq_subscr{}) ->
    multi_call(Topic, {add, Topic, Subscriber}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec del_subscribers(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> ok.
del_subscribers(_Topic, []) ->
    ok;
del_subscribers(Topic, BadSs) ->
    case ets:member(?MODULE, {topic, Topic}) of
        false ->
            ok;
        true ->
            multi_cast(Topic, {delete, Topic, BadSs})
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec get_subscribers(lbm_nomq:topic()) -> [#lbm_nomq_subscr{}].
get_subscribers(Topic) ->
    case ets:member(?MODULE, {topic, Topic}) of
        true ->
            subscribers(Topic);
        false ->
            []
    end.

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
%%% gen_server callbacks
%%%=============================================================================

-record(state, {}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    Nodes = nodes(),
    ok = net_kernel:monitor_nodes(true),
    lists:foreach(
      fun(Node) ->
              {?BACKEND_NAME, Node} ! {new, ?MODULE, node()},
              self() ! {nodeup, Node}
      end, Nodes),
    ?MODULE = ets:new(?MODULE, [ordered_set, protected, named_table]),
    {ok, #state{}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call({add, Topic, Subscriber}, _From, State) ->
    NewSubscribers = not empty(subscribe(Topic, Subscriber)),
    notify_on_subscribe(NewSubscribers, Topic),
    {reply, ok, State};
handle_call({delete, Topic, Subscribers}, _From, State) ->
    unsubscribe(Topic, Subscribers),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, undef, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast({add_waiting, Topic, Who, BadSs}, State) ->
    SubscribersLeft = empty(unsubscribe(Topic, BadSs)),
    add_waiting(SubscribersLeft, Topic, Who),
    {noreply, State};
handle_cast({del_waiting, Topic, Who}, State) ->
    true = ets:delete(?MODULE, {waiting, Topic, Who}),
    {noreply, State};
handle_cast({merge, Subscriptions}, State) ->
    merge_subscriptions(Subscriptions),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({nodeup, Node}, State) ->
    gen_server:cast({?BACKEND_NAME, Node}, {merge, all_subscribers()}),
    {noreply, State};
handle_info({new, ?MODULE, Node}, State) ->
    gen_server:cast({?BACKEND_NAME, Node}, {merge, all_subscribers()}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, _State) ->
    true = ets:delete(?MODULE),
    ok.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% Register a (new) subscriber and return the topics with new subscribers. In
%% this case this is either an empty topic list or a list containing `Topic'.
%% The topic entry will be created if not yet existing.
%%------------------------------------------------------------------------------
subscribe(Topic, Subscriber) ->
    true = ensure_entry({topic, Topic}, 0),
    case ets:insert_new(?MODULE, {{subscriber, Topic, Subscriber}}) of
        true ->
            ets:update_counter(?MODULE, {topic, Topic}, {2, +1}),
            [Topic];
        false ->
            []
    end.

%%------------------------------------------------------------------------------
%% @private
%% Remove one or more subscriptions and return a list of deleted topics. In this
%% case this is either an empty topic list or a list containing `Topic'. The
%% topic entry will be removed if no more subscribers exist.
%%------------------------------------------------------------------------------
unsubscribe(Topic, Subscribers) when is_list(Subscribers) ->
    lists:usort(lists:append([unsubscribe(Topic, S) || S <- Subscribers]));
unsubscribe(Topic, Subscriber) ->
    Deleted = delete_subscriber(Topic, Subscriber),
    case ets:member(?MODULE, {topic, Topic}) of
        true when Deleted > 0 ->
            Decr = Deleted * -1,
            case ets:update_counter(?MODULE, {topic, Topic}, {2, Decr, 0, 0}) of
                0 ->
                    true = ets:delete(?MODULE, {topic, Topic}),
                    [Topic];
                _ ->
                    []
            end;
        _ ->
            []
    end.

%%------------------------------------------------------------------------------
%% @private
%% Adds a process to the processes waiting for subscribers of a certain topic.
%% If valid subscribers for the topic are still available, the process will not
%% be added and notified instead.
%%------------------------------------------------------------------------------
add_waiting(true, Topic, Who) ->
    case subscribers(Topic) of
        [] ->
            add_waiting(false, Topic, Who);
        Subscribers ->
            %% still subscribers available, notify immediatelly
            notify([Who], Topic, Subscribers)
    end;
add_waiting(false, Topic, Who) ->
    %% no subscribers, need to wait for new subscriptions
    ets:insert_new(?MODULE, {{waiting, Topic, Who}}).

%%------------------------------------------------------------------------------
%% @private
%% Notify waiting processes, if new subscriptions available
%%------------------------------------------------------------------------------
notify_on_subscribe(false, _Topic) ->
    ok;
notify_on_subscribe(true, Topic) ->
    notify_waiting(subscribers(Topic), Topic).

%%------------------------------------------------------------------------------
%% @private
%% Notify waiting processes, if any.
%%------------------------------------------------------------------------------
notify_waiting([], _Topic) ->
    ok;
notify_waiting(Subscribers, Topic) ->
    case waiting(Topic) of
        [] ->
            ok;
        Waiting ->
            true = ets:match_delete(?MODULE, {{waiting, Topic, '_'}}),
            notify(Waiting, Topic, Subscribers)
    end.

%%------------------------------------------------------------------------------
%% @private
%% Notify waiting processes, by ordinary message.
%%------------------------------------------------------------------------------
notify(Whos, Topic, Subscribers) ->
    [Pid ! ?UPDATE_MSG(Ref, Topic, Subscribers) || {Pid, Ref} <- Whos].

%%------------------------------------------------------------------------------
%% @private
%% Return all currently waiting processes for a topic.
%%------------------------------------------------------------------------------
waiting(Topic) -> [W || [W] <- ets:match(?MODULE, {{waiting, Topic, '$1'}})].

%%------------------------------------------------------------------------------
%% @private
%% Return all current subscribers for a topic.
%%------------------------------------------------------------------------------
subscribers(Topic) ->
    [S || [S] <- ets:match(?MODULE, {{subscriber, Topic, '$1'}})].

%%------------------------------------------------------------------------------
%% @private
%% Deletes all entries with a certain key and returns the number if elements
%% removed.
%%------------------------------------------------------------------------------
delete_subscriber(Topic, Subscriber) ->
    Key = {subscriber, Topic, Subscriber},
    ets:select_delete(?MODULE, [{{Key}, [], [true]}]).

%%------------------------------------------------------------------------------
%% @private
%% Return all current subscriptions.
%%------------------------------------------------------------------------------
all_subscribers() -> [[T, subscribers(T)] || T <- all_topics()].

%%------------------------------------------------------------------------------
%% @private
%% Return all currently known topics.
%%------------------------------------------------------------------------------
all_topics() -> [T || [T] <- ets:match(?MODULE, {{topic, '$1'}, '_'})].

%%------------------------------------------------------------------------------
%% @private
%% Merge subscriptions sent over by another node. Not yet existing topics will
%% be created. This function also notifies all waiting processes if a topic gets
%% new subscribers through the merge.
%%------------------------------------------------------------------------------
merge_subscriptions(Subscriptions) ->
    [notify_on_subscribe(true, Topic)
     || Topic <- lists:usort(
                   lists:append(
                     [subscribe(Topic, Subscriber)
                      || [Topic, Subscribers] <- Subscriptions,
                         Subscriber <- Subscribers -- subscribers(Topic)]))].

%%------------------------------------------------------------------------------
%% @private
%% Ensures the ETS entry for a certain key. If the entry does not yet exist it
%% will be created with the initial value `Initial'.
%%------------------------------------------------------------------------------
ensure_entry(Key, Initial) ->
    ets:member(?MODULE, Key) orelse true == ets:insert(?MODULE, {Key, Initial}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
multi_call(Topic, Message) ->
    Fun = fun() -> gen_server:multi_call(?BACKEND_NAME, Message) end,
    global:trans({{?MODULE, Topic}, self()}, Fun),
    ok.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
multi_cast(Topic, Message) ->
    spawn(fun() -> multi_call(Topic, Message) end),
    ok.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
empty([]) -> true;
empty(_)  -> false.
