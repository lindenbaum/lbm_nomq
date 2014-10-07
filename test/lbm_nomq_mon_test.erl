%%%=============================================================================
%%%
%%%               |  o __   _|  _  __  |_   _       _ _   (TM)
%%%               |_ | | | (_| (/_ | | |_) (_| |_| | | |
%%%
%%% @author Sven Heyll <sven.heyll@lindenbaum.eu>
%%% @author Timo Koepke <timo.koepke@lindenbaum.eu>
%%% @author Tobias Schlager <tobias.schlager@lindenbaum.eu>
%%% @copyright (C) 2014, Lindenbaum GmbH
%%%
%%%=============================================================================

-module(lbm_nomq_mon_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("lbm_nomq/include/lbm_nomq.hrl").

-define(TOPIC, topic).

-define(DBG(Fmt, Args), io:format(standard_error, Fmt, Args)).

%%%=============================================================================
%%% TESTS
%%%=============================================================================

all_test_() ->
    {setup, setup(), teardown(),
     [
      fun waiting_gets_notified/0,
      fun waiting_unsuccessful/0
     ]}.

waiting_gets_notified() ->
    {ok, Ref} = lbm_nomq_mon:add_waiting(?TOPIC, []),

    Ss = [waiting_gets_notified],
    Write = {write, {?BACKEND, ?TOPIC, Ss}, ignored},
    lbm_nomq_mon ! {mnesia_table_event, Write},
    lbm_nomq_mon ! {lbm_nomq_ets, put, ?TOPIC, Ss},

    receive ?UPDATE_MSG(Ref, ?TOPIC, Ss) -> ok end.

waiting_unsuccessful() ->
    {ok, Ref} = lbm_nomq_mon:add_waiting(?TOPIC, []),

    Ss = [waiting_unsuccessful],
    Write = {write, {?BACKEND, ?TOPIC, Ss}, ignored},
    Fun = fun() ->
                  lbm_nomq_mon ! {mnesia_table_event, Write},
                  lbm_nomq_mon ! {lbm_nomq_ets, put, ?TOPIC, Ss}
          end,
    {P, _} = spawn_monitor(Fun),

    ok = lbm_nomq_mon:del_waiting(?TOPIC, Ref),

    receive {'DOWN', _, _, P, normal} -> ok end,
    receive
        M = ?UPDATE_MSG(Ref, ?TOPIC, Ss) -> throw({unexpected_msg, M})
    after
        0 -> ok
    end.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
setup() ->
    fun() ->
            {ok, Apps} = application:ensure_all_started(lbm_nomq),
            Apps
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
teardown() ->
    fun(Apps) ->
            [application:stop(App) || App <- Apps]
    end.
