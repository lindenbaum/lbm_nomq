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
%%%=============================================================================

-ifndef(lbm_nomq_hrl_).
-define(lbm_nomq_hrl_, 1).

-define(TABLE, lbm_nomq).

-record(lbm_nomq_subscr, {m :: module(), f :: atom(), as :: [term()]}).

-define(SUBSCRIBER(M, F, As), #lbm_nomq_subscr{m = M, f = F, as = As}).

-define(UPDATE_MSG(Ref, Topic, Ss), {lbm_nomq_mon, Ref, Topic, Ss}).

-endif. %% lbm_nomq_hrl_
