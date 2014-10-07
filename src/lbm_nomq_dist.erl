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
%%% subscribers.
%%% @end
%%%=============================================================================

-module(lbm_nomq_dist).

-include("lbm_nomq.hrl").

%%%=============================================================================
%%% Behaviour
%%%=============================================================================

-callback init() -> {ok, [{lbm_nomq:topic(), #lbm_nomq_subscr{}}]}.
%% Initialize the state backend on this node. This will be called once, before
%% any of the functions below are used. It should return a mapping of all
%% initial subscriptions (if any).

-callback add(lbm_nomq:topic(), #lbm_nomq_subscr{}) -> ok | {error, term()}.
%% Add a subscriber for `Topic'.

-callback get(lbm_nomq:topic()) -> [#lbm_nomq_subscr{}].
%% Returns a list of all current subscribers for `Topic'.

-callback del(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) ->
    {ok, [#lbm_nomq_subscr{}]} | {error, term()}.
%% Remove a given list of subscribers for `Topic'.

-callback handle_info(term()) ->
    {put, lbm_nomq:topic(), [#lbm_nomq_subscr{}]} |
    {delete, lbm_nomq:topic()} |
    ignore.
%% Handles/translates asynchronous messages from the state backend.
