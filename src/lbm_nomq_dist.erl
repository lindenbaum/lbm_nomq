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
%%% Defines the behaviour for the distributed state backend used to manage
%%% subscribers. This module also delegates to the configured backend.
%%% @end
%%%=============================================================================

-module(lbm_nomq_dist).

-include("lbm_nomq.hrl").

%% lbm_nomq_dist callbacks
-export([spec/0,
         add_subscriber/2,
         del_subscribers/2,
         get_subscribers/1,
         add_waiting/2,
         del_waiting/2]).

-define(BACKEND, lbm_nomq_ets).

%%%=============================================================================
%%% Behaviour
%%%=============================================================================

-callback spec(atom()) -> supervisor:child_spec().
%% Return the supervisor child spec that has to be used to start the backend.

-callback add_subscriber(atom(), lbm_nomq:topic(), #lbm_nomq_subscr{}) -> ok.
%% Add a subscriber for `Topic'.

-callback del_subscribers(atom(), lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> ok.
%% Report a list of bad/down subscribers for `Topic'.

-callback get_subscribers(atom(), lbm_nomq:topic()) -> [#lbm_nomq_subscr{}].
%% Returns a list of all current subscribers for `Topic'.

-callback add_waiting(atom(), lbm_nomq:topic(), [#lbm_nomq_subscr{}]) ->
    {ok, reference()}.
%% Add a process to the waiting processes for subscribers of a specific topic.
%% The process may report bad subscribers along with the registration. The
%% process will receive a message of the form
%% `?UPDATE_MSG(reference(), lbm_nomq:topic(), [#lbm_nomq_subscr{}])'
%% when new subscribers are available. The reference contained in the message
%% will be returned from this function call.

-callback del_waiting(atom(), lbm_nomq:topic(), reference()) -> ok.
%% Removes a process from the list of waiting processes for topic. Calling this
%% function is only necessary if the process gives up waiting for subscribers.
%% The wait entry will be removed automatically, when a subscriber update is
%% sent from this server.

%%%=============================================================================
%%% lbm_nomq_dist callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec spec() -> supervisor:child_spec().
spec() -> ?BACKEND:spec(?MODULE).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec add_subscriber(lbm_nomq:topic(), #lbm_nomq_subscr{}) -> ok.
add_subscriber(Topic, Subscriber) ->
    ?BACKEND:add_subscriber(?MODULE, Topic, Subscriber).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec del_subscribers(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> ok.
del_subscribers(Topic, Subscribers) ->
    ?BACKEND:del_subscribers(?MODULE, Topic, Subscribers).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec get_subscribers(lbm_nomq:topic()) -> [#lbm_nomq_subscr{}].
get_subscribers(Topic) -> ?BACKEND:get_subscribers(?MODULE, Topic).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec add_waiting(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> {ok, reference()}.
add_waiting(Topic, BadSubscribers) ->
    ?BACKEND:add_waiting(?MODULE, Topic, BadSubscribers).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec del_waiting(lbm_nomq:topic(), reference()) -> ok.
del_waiting(Topic, Reference) ->
    ?BACKEND:del_waiting(?MODULE, Topic, Reference).
