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
%%% A backend for the persistent subscriber storage based on `lbm_kv' and thus
%%% Mnesia.
%%% @end
%%%=============================================================================

-module(lbm_nomq_kv).

-behaviour(lbm_kv).

%% API
-export([init/0,
         destroy/0,
         add/2,
         get/1,
         del/2,
         handle_info/1]).

%% lbm_kv callbacks
-export([resolve_conflict/1]).

-include("lbm_nomq.hrl").

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Initialize the storage backend. This must be called once, before using the
%% functions below. It returns a mapping of all initial subscriptions.
%% @end
%%------------------------------------------------------------------------------
-spec init() -> {ok, [{lbm_nomq:topic(), #lbm_nomq_subscr{}}]}.
init() ->
    ok = lbm_kv:create(?MODULE),
    ok = lbm_kv:subscribe(?MODULE),
    {ok, lbm_kv:get_all(?MODULE)}.

%%------------------------------------------------------------------------------
%% @doc
%% Initialize the storage backend. This must be called once, before using the
%% functions below.
%% @end
%%------------------------------------------------------------------------------
-spec destroy() -> ok.
destroy() -> lbm_kv:unsubscribe(?MODULE).

%%------------------------------------------------------------------------------
%% @doc
%% Add a subscriber for `Topic'. Do not call this function from a massive amount
%% of processes, since Mnesia is not good handling thousands of concurrent
%% transactions.
%% @end
%%------------------------------------------------------------------------------
-spec add(lbm_nomq:topic(), #lbm_nomq_subscr{}) -> ok | {error, term()}.
add(Topic, Subscriber = #lbm_nomq_subscr{}) ->
    Fun = fun([Ss]) -> [[Subscriber | Ss]]; (_) -> [[Subscriber]] end,
    case lbm_kv:update(?MODULE, Topic, Fun) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Returns a shuffled list of all current subscribers for `Topic'.
%% NOTE: Only dirty reads can handle 10000+ concurrent reads.
%% @end
%%------------------------------------------------------------------------------
-spec get(lbm_nomq:topic()) -> [#lbm_nomq_subscr{}].
get(Topic) ->
    SsList = lbm_kv:get(?MODULE, Topic, dirty),
    lists:append([shuffle(Ss) || Ss <- SsList, is_list(Ss)]).

%%------------------------------------------------------------------------------
%% @doc
%% Remove a given list of subscribers for `Topic'. Do not call this function
%% from a massive amount of processes, since Mnesia is not good handling
%% thousands of concurrent transactions.
%% @end
%%------------------------------------------------------------------------------
-spec del(lbm_nomq:topic(), [#lbm_nomq_subscr{}]) ->
                 {ok, [#lbm_nomq_subscr{}]} | {error, term()}.
del(Topic, Subscribers) ->
    lbm_kv:update(
      ?MODULE,
      Topic,
      fun([]) ->
              [];
         ([Ss]) ->
              case Ss -- Subscribers of
                  []    -> [];
                  NewSs -> [NewSs]
              end
      end).

%%------------------------------------------------------------------------------
%% @doc
%% Handles/translates asynchronous messages from the backend, in this case
%% Mnesia table events.
%% @end
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
shuffle(L) when is_list(L) ->
    shuffle(L, length(L)).
shuffle(L, Len) ->
    [E || {_, E} <- lists:sort([{crypto:rand_uniform(0, Len), E} || E <- L])].

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_mnesia({write, {?MODULE, Topic, []}, _})        -> {delete, Topic};
handle_mnesia({write, {?MODULE, Topic, Ss}, _})        -> {put, Topic, Ss};
handle_mnesia({delete_object, {?MODULE, Topic, _}, _}) -> {delete, Topic};
handle_mnesia({delete, {?MODULE, Topic}, _})           -> {delete, Topic};
handle_mnesia(_)                                       -> ignore.
