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
%%% A monitoring server that mirrors the distributed subscriber state. This is
%%% done to optimise writes to the distributed background storage.
%%%
%%% If a process does not find subscribers for its topic it registeres at this
%%% server to get a notification as soon as new subscribers for its topic are
%%% available.
%%% @end
%%%=============================================================================

-module(lbm_nomq_mon).

-behaviour(gen_server).

%% Internal API
-export([start_link/0,
         del_subscribers/2,
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
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @private
%% Report bad subscribers.
%%------------------------------------------------------------------------------
-spec del_subscribers(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> ok.
del_subscribers(_Topic, []) ->
    ok;
del_subscribers(Topic, BadSs) ->
    gen_server:cast(?MODULE, {del_subscribers, Topic, BadSs}).

%%------------------------------------------------------------------------------
%% @private
%% Add a process to the waiting processes for subscribers of a specific topic.
%% The process may report bad subscribers along with the registration. The
%% process will receive a message of the form
%% `?UPDATE_MSG(reference(), lbm_nomq:topic(), [#lbm_nomq_subscr{}])'
%% when new subscribers are available. The reference contained in the message
%% will be returned from this function call.
%%------------------------------------------------------------------------------
-spec add_waiting(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> {ok, reference()}.
add_waiting(Topic, BadSs) ->
    Reference = make_ref(),
    Request = {add_waiting, Topic, {self(), Reference}, BadSs},
    {gen_server:cast(?MODULE, Request), Reference}.

%%------------------------------------------------------------------------------
%% @private
%% Removes a process from the list of waiting processes for topic. Calling this
%% function is only necessary if the process gives up waiting for subscribers.
%% The wait entry will be removed automatically, when a subscriber update is
%% sent from this server.
%%------------------------------------------------------------------------------
-spec del_waiting(lbm_nomq:topic(), reference()) -> ok.
del_waiting(Topic, Reference) ->
    ok = gen_server:cast(?MODULE, {del_waiting, Topic, {self(), Reference}}),
    del_waiting_loop(Topic, Reference).
del_waiting_loop(Topic, Reference) ->
    receive
        ?UPDATE_MSG(Reference, Topic, _) ->
            del_waiting_loop(Topic, Reference)
    after
        50 -> ok
    end.

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

-record(state, {
          s :: #{lbm_nomq:topic() => [#lbm_nomq_subscr{}]},     %% Subscriptions
          w :: #{lbm_nomq:topic() => [{pid(), reference()}]}}). %% Waiting

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    {ok, Subscriptions} = ?BACKEND:init(),
    {ok, #state{s = maps:from_list(Subscriptions), w = maps:new()}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call(_Request, _From, State) -> {reply, undef, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast({add_waiting, Topic, Who, BadSs}, State) ->
    {noreply, add_waiting(Topic, Who, del_subscribers(Topic, BadSs, State))};
handle_cast({del_waiting, Topic, Who}, State) ->
    {noreply, del_waiting(Topic, Who, State)};
handle_cast({del_subscribers, Topic, BadSs}, State) ->
    {noreply, del_subscribers(Topic, BadSs, State)};
handle_cast(_Request, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info(Info, State) ->
    {noreply,
     case ?BACKEND:handle_info(Info) of
         {put, Topic, Subscribers} -> put_topic(Topic, Subscribers, State);
         {delete, Topic}           -> del_topic(Topic, State);
         ignore                    -> State
     end}.

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
%% Put subscribers for a topic into state. If new subscribers were add waiting
%% processes will be notified (and removed).
%%------------------------------------------------------------------------------
put_topic(Topic, Subscribers, State = #state{s = S, w = W}) ->
    case maps:get(Topic, S, []) of
        Subscribers ->
            State; %% no update, no notification
        _ ->
            %% actual update, notify waiting processes
            notify_waiting(maps:get(Topic, W, []), Topic, Subscribers),
            State#state{s = maps:put(Topic, Subscribers, S),
                        w = maps:remove(Topic, W)}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
del_topic(Topic, State = #state{s = S}) ->
    State#state{s = maps:without([Topic], S)}.

%%------------------------------------------------------------------------------
%% @private
%% Delete subscribers detected as bad. If the process state indicates a change
%% in subscribers, try to update the distributed backend storage.
%%------------------------------------------------------------------------------
del_subscribers(Topic, BadSs, State = #state{s = S}) ->
    Subscribers = maps:get(Topic, S, []),
    case Subscribers -- BadSs of
        Subscribers ->
            %% no change, no write attempt
            State;
        _ ->
            case ?BACKEND:del(Topic, BadSs) of
                {ok, []} ->
                    State#state{s = maps:without([Topic], S)};
                {ok, [NewSs]} ->
                    State#state{s = maps:put(Topic, NewSs, S)}
            end
    end.

%%------------------------------------------------------------------------------
%% @private
%% Adds a process to the processes waiting for subscribers of a certain topic.
%% If valid subscribers for the topic are available, the process will not be
%% added and notified instead.
%%------------------------------------------------------------------------------
add_waiting(Topic, Who, State = #state{s = S, w = W}) ->
    case maps:get(Topic, S, []) of
        [] ->
            %% no subscribers, need to wait for new subscriptions
            State#state{w = maps:put(Topic, [Who | maps:get(Topic, W, [])], W)};
        Subscribers ->
            %% still subscribers available, notify immediatelly
            notify_waiting([Who], Topic, Subscribers),
            State
    end.

%%------------------------------------------------------------------------------
%% @private
%% Remove a waiting process for a topic.
%%------------------------------------------------------------------------------
del_waiting(Topic, Who, State = #state{w = W}) ->
    del_waiting(Topic, Who, maps:get(Topic, W, []), State).

%%------------------------------------------------------------------------------
%% @private
%% Remove a waiting process for a topic. Deletes the topic entry, if no more
%% waiting processes are available.
%%------------------------------------------------------------------------------
del_waiting(_Topic, _Who, [], State) ->
    State;
del_waiting(Topic, Who, [Who], State = #state{w = W}) ->
    State#state{w = maps:remove(Topic, W)};
del_waiting(Topic, Who, Waiting, State = #state{w = W}) ->
    State#state{w = maps:put(Topic, Waiting -- [Who], W)}.

%%------------------------------------------------------------------------------
%% @private
%% Notify a waiting process, by ordinary message.
%%------------------------------------------------------------------------------
notify_waiting(Whos, Topic, Subscribers) ->
    [Pid ! ?UPDATE_MSG(Ref, Topic, Subscribers) || {Pid, Ref} <- Whos].
