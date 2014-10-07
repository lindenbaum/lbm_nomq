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
%%% An implementation of the {@link lbm_nomq_dist} behaviour based on local
%%% `ETS' tables using global locks to distribute subscriptions.
%%% The table contains the following terms:
%%% `{{topic, Topic}, Counter}': A topic and the current subscriber counter.
%%% `{{subscriber, Topic, Subscriber}}': The subscribers for a certain topic.
%%% @end
%%%=============================================================================

-module(lbm_nomq_ets).

-compile({no_auto_import, [get/1]}).

%% Internal API
-export([start_link/0]).

%% lbm_nomq_dist callbacks
-export([init/0,
         add/2,
         get/1,
         del/2,
         handle_info/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
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
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%=============================================================================
%%% lbm_nomq_dist callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec init() -> [{lbm_nomq:topic(), #lbm_nomq_subscr{}}].
init() -> gen_server:cast(?MODULE, {events_to, self()}), [].

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec add(lbm_nomq:topic(), #lbm_nomq_subscr{}) -> ok.
add(Topic, Subscriber = #lbm_nomq_subscr{}) ->
    multi_call(Topic, {subscribe, Topic, Subscriber}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec del(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> [#lbm_nomq_subscr{}].
del(Topic, Subscribers) ->
    case ets:member(?MODULE, {topic, Topic}) of
        false ->
            [];
        true ->
            multi_call(Topic, {unsubscribe, Topic, Subscribers}),
            get(Topic)
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec get(lbm_nomq:topic()) -> [#lbm_nomq_subscr{}].
get(Topic) ->
    case ets:member(?MODULE, {topic, Topic}) of
        true ->
            subscribers(Topic);
        false ->
            []
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec handle_info(term()) ->
                         {put, lbm_nomq:topic(), [#lbm_nomq_subscr{}]} |
                         {delete, lbm_nomq:topic()} | ignore.
handle_info({?MODULE, delete, Topic})           -> {delete, Topic};
handle_info({?MODULE, put, Topic, Subscribers}) -> {put, Topic, Subscribers};
handle_info(_Event)                             -> ignore.

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

-record(state, {receiver :: pid()}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    Nodes = nodes(),
    ok = net_kernel:monitor_nodes(true),
    lists:foreach(
      fun(Node) ->
              {?MODULE, Node} ! {new, ?MODULE, node()},
              self() ! {nodeup, Node}
      end, Nodes),
    ?MODULE = ets:new(?MODULE, [ordered_set, protected, named_table]),
    {ok, #state{}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call({subscribe, Topic, Subscriber}, _From, State) ->
    {reply, ok, notify(subscribe(Topic, Subscriber), State)};
handle_call({unsubscribe, Topic, Subscribers}, _From, State) ->
    {reply, ok, notify(unsubscribe(Topic, Subscribers), State)};
handle_call(_Request, _From, State) ->
    {reply, undef, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast({events_to, Pid}, State) ->
    {noreply, State#state{receiver = Pid}};
handle_cast({merge, Subscriptions}, State) ->
    {noreply, notify(merge(Subscriptions), State)};
handle_cast(_Request, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({nodeup, Node}, State) ->
    gen_server:cast({?MODULE, Node}, {merge, all_subscribers()}),
    {noreply, State};
handle_info({new, ?MODULE, Node}, State) ->
    gen_server:cast({?MODULE, Node}, {merge, all_subscribers()}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

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
%% Merge subscriptions sent over by another node. This function returns a list
%% with uniquely sorted topics with new subscribers. Not yet existing topics
%% will be created.
%%------------------------------------------------------------------------------
merge(Subscriptions) ->
    lists:usort(
      lists:append(
        [subscribe(Topic, Subscriber)
         || [Topic, Subscribers] <- Subscriptions,
            Subscriber <- Subscribers -- subscribers(Topic)])).

%%------------------------------------------------------------------------------
%% @private
%% Send update notifications for a list of topics to a receiver. If there are no
%% more subscribers for a topic the update is a `delete', in all other cases,
%% the update is a `put'.
%%------------------------------------------------------------------------------
notify(Topics, State = #state{receiver = Pid}) ->
    [notify(Pid, Topic, get(Topic)) || Topic <- Topics],
    State.
notify(Pid, Topic, []) ->
    catch Pid ! {?MODULE, delete, Topic};
notify(Pid, Topic, Subscribers) ->
    catch Pid ! {?MODULE, put, Topic, Subscribers}.

%%------------------------------------------------------------------------------
%% @private
%% Remove one or more subscriptions and return a list of topics with removed
%% subscribers. In this case this is either an empty topic list or a list
%% containing `Topic'. The topic entry will be removed if no more subscribers
%% exist.
%%------------------------------------------------------------------------------
unsubscribe(Topic, Subscribers) when is_list(Subscribers) ->
    lists:usort(lists:append([unsubscribe(Topic, S) || S <- Subscribers]));
unsubscribe(Topic, Subscriber) ->
    Deleted = delete_entry({subscriber, Topic, Subscriber}),
    case ets:member(?MODULE, {topic, Topic}) of
        true when Deleted > 0 ->
            Decr = Deleted * -1,
            case ets:update_counter(?MODULE, {topic, Topic}, {2, Decr, 0, 0}) of
                0 ->
                    true = ets:delete(?MODULE, {topic, Topic}),
                    [Topic];
                _ ->
                    [Topic]
            end;
        _ ->
            []
    end.

%%------------------------------------------------------------------------------
%% @private
%% Return all current subscriptions.
%%------------------------------------------------------------------------------
all_subscribers() -> [[T, subscribers(T)] || T <- all_topics()].

%%------------------------------------------------------------------------------
%% @private
%% Return all current subscribers for a topic.
%%------------------------------------------------------------------------------
subscribers(Topic) ->
    [S || [S] <- ets:match(?MODULE, {{subscriber, Topic, '$1'}})].

%%------------------------------------------------------------------------------
%% @private
%% Return all currently known topics.
%%------------------------------------------------------------------------------
all_topics() -> [T || [T] <- ets:match(?MODULE, {{topic, '$1'}, '_'})].

%%------------------------------------------------------------------------------
%% @private
%% Ensures the ETS entry for a certain key. If the entry does not yet exist it
%% will be created with the initial value `Initial'.
%%------------------------------------------------------------------------------
ensure_entry(Key, Initial) ->
    ets:member(?MODULE, Key) orelse true == ets:insert(?MODULE, {Key, Initial}).

%%------------------------------------------------------------------------------
%% @private
%% Deletes all entries with a certain key and returns the number of elements
%% removed.
%%------------------------------------------------------------------------------
delete_entry(Key) ->
    Entries = length(ets:lookup(?MODULE, Key)),
    true = ets:delete(?MODULE, Key),
    Entries.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
multi_call(Topic, Message) ->
    Fun = fun() -> gen_server:multi_call(?MODULE, Message) end,
    global:trans({{?MODULE, Topic}, self()}, Fun),
    ok.
