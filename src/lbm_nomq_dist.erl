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
%%% subscribers. This module eats its own dogfood and delegates to the
%%% configured backend.
%%% @end
%%%=============================================================================

-module(lbm_nomq_dist).

-include("lbm_nomq.hrl").

%% lbm_nomq_dist callbacks
-export([init/0,
         add/2,
         get/1,
         del/2,
         handle_info/1]).

%%%=============================================================================
%%% Behaviour
%%%=============================================================================

-callback init() -> [{lbm_nomq:topic(), #lbm_nomq_subscr{}}].
%% Initialize the state backend on this node. This will be called once, before
%% any of the functions below are used. It should return a mapping of all
%% initial subscriptions (if any).

-callback add(lbm_nomq:topic(), #lbm_nomq_subscr{}) -> ok.
%% Add a subscriber for `Topic'.

-callback get(lbm_nomq:topic()) -> [#lbm_nomq_subscr{}].
%% Returns a list of all current subscribers for `Topic'.

-callback del(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> [#lbm_nomq_subscr{}].
%% Remove a given list of subscribers for `Topic'.

-callback handle_info(term()) ->
    {put, lbm_nomq:topic(), [#lbm_nomq_subscr{}]} |
    {delete, lbm_nomq:topic()} |
    ignore.
%% Handles/translates asynchronous messages from the state backend.

%%%=============================================================================
%%% lbm_nomq_dist callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec init() -> [{lbm_nomq:topic(), #lbm_nomq_subscr{}}].
init() -> ?BACKEND:init().

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec add(lbm_nomq:topic(), #lbm_nomq_subscr{}) -> ok.
add(Topic, Subscriber) -> ?BACKEND:add(Topic, Subscriber).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec get(lbm_nomq:topic()) -> [#lbm_nomq_subscr{}].
get(Topic) -> ?BACKEND:get(Topic).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec del(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> [#lbm_nomq_subscr{}].
del(Topic, Subscribers) -> ?BACKEND:del(Topic, Subscribers).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec handle_info(term()) ->
                         {put, lbm_nomq:topic(), [#lbm_nomq_subscr{}]} |
                         {delete, lbm_nomq:topic()} | ignore.
handle_info(Event) -> ?BACKEND:handle_info(Event).
