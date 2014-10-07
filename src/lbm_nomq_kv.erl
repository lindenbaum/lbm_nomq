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
%%% An implementation of the {@link lbm_nomq_dist} behaviour based on the
%%% `lbm_kv' application which in turn uses distributed Mnesia.
%%% @end
%%%=============================================================================

-module(lbm_nomq_kv).

-behaviour(lbm_nomq_dist).
-behaviour(lbm_kv).

%% lbm_nomq_dist callbacks
-export([init/0,
         add/2,
         get/1,
         del/2,
         handle_info/1]).

%% lbm_kv callbacks
-export([resolve_conflict/1]).

-include("lbm_nomq.hrl").

%%%=============================================================================
%%% lbm_nomq_dist callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec init() -> [{lbm_nomq:topic(), #lbm_nomq_subscr{}}].
init() ->
    ok = lbm_kv:create(?MODULE),
    ok = lbm_kv:subscribe(?MODULE),
    lbm_kv:get_all(?MODULE).

%%------------------------------------------------------------------------------
%% @private
%% NOTE: Do not call this function from a massive amount of processes, since
%% Mnesia is not good handling thousands of concurrent transactions.
%%------------------------------------------------------------------------------
-spec add(lbm_nomq:topic(), #lbm_nomq_subscr{}) -> ok.
add(Topic, Subscriber = #lbm_nomq_subscr{}) ->
    Fun = fun([Ss]) -> [[Subscriber | Ss]]; (_) -> [[Subscriber]] end,
    {ok, _} = lbm_kv:update(?MODULE, Topic, Fun),
    ok.

%%------------------------------------------------------------------------------
%% @private
%% NOTE: Only dirty reads can handle 10000+ concurrent reads.
%%------------------------------------------------------------------------------
-spec get(lbm_nomq:topic()) -> [#lbm_nomq_subscr{}].
get(Topic) -> lists:append(lbm_kv:get(?MODULE, Topic, dirty)).

%%------------------------------------------------------------------------------
%% @private
%% NOTE: Do not call this function from a massive amount of processes, since
%% Mnesia is not good handling thousands of concurrent transactions.
%%------------------------------------------------------------------------------
-spec del(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) -> [#lbm_nomq_subscr{}].
del(Topic, Subscribers) ->
    {ok, SsList} = lbm_kv:update(
                     ?MODULE,
                     Topic,
                     fun([]) ->
                             [];
                        ([Ss]) ->
                             case Ss -- Subscribers of
                                 []    -> [];
                                 NewSs -> [NewSs]
                             end
                     end),
    lists:append(SsList).

%%------------------------------------------------------------------------------
%% @private
%% Handles/translates Mnesia table events.
%%------------------------------------------------------------------------------
-spec handle_info(term()) ->
                         {put, lbm_nomq:topic(), [#lbm_nomq_subscr{}]} |
                         {delete, lbm_nomq:topic()} | ignore.
handle_info({mnesia_table_event, Event}) ->
    handle_mnesia(Event);
handle_info(_Event) ->
    ignore.

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
%%% internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_mnesia({write, {?MODULE, Topic, []}, _})        -> {delete, Topic};
handle_mnesia({write, {?MODULE, Topic, Ss}, _})        -> {put, Topic, Ss};
handle_mnesia({delete_object, {?MODULE, Topic, _}, _}) -> {delete, Topic};
handle_mnesia({delete, {?MODULE, Topic}, _})           -> {delete, Topic};
handle_mnesia(_)                                       -> ignore.
