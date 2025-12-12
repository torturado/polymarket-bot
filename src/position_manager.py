from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

from .config import Config
from .utils.market_utils import MarketUpdate, opposite_side, theoretical_spread


class PositionState(str, Enum):
    WATCHING = "watching"
    ENTERING_LEG_1 = "entering_leg_1"
    HOLDING = "holding"
    CLOSING = "closing"
    MERGING = "merging"
    UNWINDING = "unwinding"


@dataclass
class LegInPosition:
    condition_id: str
    yes_token_id: str
    no_token_id: str
    leg_1_side: str  # "YES" or "NO"
    leg_1_entry_price: float
    leg_1_size: float
    leg_1_filled: bool
    state: PositionState
    entry_time: float
    leg_2_entry_price: Optional[float] = None
    leg_2_size: Optional[float] = None
    leg_2_filled: bool = False


class PositionManager:
    def __init__(self, config: Config):
        self.config = config

    def evaluate_entry(self, market_data: MarketUpdate) -> Optional[LegInPosition]:
        yes_ask = market_data.prices["YES"].best_ask
        no_ask = market_data.prices["NO"].best_ask

        if max(yes_ask, no_ask) > self.config.max_opposite_ask_for_entry:
            return None

        candidates = []
        if yes_ask < self.config.entry_threshold:
            candidates.append(("YES", yes_ask, no_ask))
        if no_ask < self.config.entry_threshold:
            candidates.append(("NO", no_ask, yes_ask))
        if not candidates:
            return None

        # Choose the cheaper leg to enter.
        side, entry_price, opp_ask = min(candidates, key=lambda c: c[1])

        if opp_ask > self.config.max_opposite_ask_for_entry:
            return None
        if self.config.min_opposite_ask_for_entry > 0 and opp_ask < self.config.min_opposite_ask_for_entry:
            return None

        if self.config.min_book_depth_usdc > 0:
            depth_yes = market_data.book_depth_usdc.get("YES", 0.0)
            depth_no = market_data.book_depth_usdc.get("NO", 0.0)
            if depth_yes < self.config.min_book_depth_usdc or depth_no < self.config.min_book_depth_usdc:
                return None

        entry_exit_cost = entry_price + opp_ask
        if self.config.max_entry_exit_cost is not None and entry_exit_cost > self.config.max_entry_exit_cost:
            return None
        if self.config.min_spread_basis_points > 0:
            edge_bps = (1.0 - entry_exit_cost) * 10000.0
            if edge_bps < float(self.config.min_spread_basis_points):
                return None

        # Interpret MAX_POSITION_SIZE as USDC budget per leg (not shares).
        size = self.config.max_position_size / entry_price if entry_price > 0 else 0.0

        return LegInPosition(
            condition_id=market_data.condition_id,
            yes_token_id=market_data.yes_token_id,
            no_token_id=market_data.no_token_id,
            leg_1_side=side,
            leg_1_entry_price=entry_price,
            leg_1_size=size,
            leg_1_filled=False,
            state=PositionState.ENTERING_LEG_1,
            entry_time=time.time(),
        )

    def update_position(
        self, position: LegInPosition, market_data: MarketUpdate
    ) -> PositionState:
        prices = market_data.prices
        yes_ask = prices["YES"].best_ask
        no_ask = prices["NO"].best_ask

        if position.state == PositionState.ENTERING_LEG_1:
            # ExecutionEngine will flip leg_1_filled when order completes.
            if position.leg_1_filled:
                position.state = PositionState.HOLDING
            return position.state

        if position.state == PositionState.HOLDING:
            exit_cost = self.calculate_exit_cost(position, prices)
            if exit_cost <= self.config.target_profit_threshold:
                position.state = PositionState.CLOSING
            elif exit_cost >= self.config.stop_loss_threshold:
                position.state = PositionState.UNWINDING
            return position.state

        if position.state == PositionState.CLOSING:
            if position.leg_2_filled:
                position.state = PositionState.MERGING
            return position.state

        if position.state in (PositionState.MERGING, PositionState.UNWINDING):
            return position.state

        return position.state

    def calculate_exit_cost(
        self, position: LegInPosition, current_prices: Dict[str, object]
    ) -> float:
        opp = opposite_side(position.leg_1_side)
        opp_ask = current_prices[opp].best_ask
        return theoretical_spread(position.leg_1_entry_price, opp_ask)

    def should_stop_loss(
        self, position: LegInPosition, current_prices: Dict[str, object]
    ) -> bool:
        return self.calculate_exit_cost(position, current_prices) >= self.config.stop_loss_threshold
