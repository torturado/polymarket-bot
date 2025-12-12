import time

import pytest

from src.config import Config
from src.position_manager import LegInPosition, PositionManager, PositionState
from src.utils.market_utils import MarketUpdate, PriceData


def _update(
    condition_id="c1",
    yes_ask=0.1,
    no_ask=0.9,
    yes_bid=0.09,
    no_bid=0.89,
):
    return MarketUpdate(
        condition_id=condition_id,
        yes_token_id="yes1",
        no_token_id="no1",
        prices={
            "YES": PriceData(best_bid=yes_bid, best_ask=yes_ask, timestamp=time.time()),
            "NO": PriceData(best_bid=no_bid, best_ask=no_ask, timestamp=time.time()),
        },
        received_at=time.time(),
    )


def test_evaluate_entry_yes():
    cfg = Config(entry_threshold=0.2, paper_trading=True)
    pm = PositionManager(cfg)
    upd = _update(yes_ask=0.15, no_ask=0.85)
    pos = pm.evaluate_entry(upd)
    assert pos is not None
    assert pos.leg_1_side == "YES"
    assert pos.state == PositionState.ENTERING_LEG_1


def test_hold_to_close_and_merge():
    cfg = Config(target_profit_threshold=0.95, stop_loss_threshold=1.05, paper_trading=True)
    pm = PositionManager(cfg)
    pos = LegInPosition(
        condition_id="c1",
        yes_token_id="yes1",
        no_token_id="no1",
        leg_1_side="YES",
        leg_1_entry_price=0.1,
        leg_1_size=1.0,
        leg_1_filled=True,
        state=PositionState.HOLDING,
        entry_time=time.time(),
    )
    upd = _update(yes_ask=0.12, no_ask=0.83)
    st = pm.update_position(pos, upd)
    assert st == PositionState.CLOSING


def test_hold_to_unwind():
    cfg = Config(target_profit_threshold=0.95, stop_loss_threshold=1.0, paper_trading=True)
    pm = PositionManager(cfg)
    pos = LegInPosition(
        condition_id="c1",
        yes_token_id="yes1",
        no_token_id="no1",
        leg_1_side="YES",
        leg_1_entry_price=0.6,
        leg_1_size=1.0,
        leg_1_filled=True,
        state=PositionState.HOLDING,
        entry_time=time.time(),
    )
    upd = _update(yes_ask=0.61, no_ask=0.45)
    st = pm.update_position(pos, upd)
    assert st == PositionState.UNWINDING

