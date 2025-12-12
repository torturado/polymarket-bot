from __future__ import annotations

import asyncio
import csv
import logging
from pathlib import Path
from typing import Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

from .config import Config
from .position_manager import LegInPosition


class ExecutionEngine:
    def __init__(self, client: ClobClient, config: Config):
        self.client = client
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._paper_log_path = Path(self.config.paper_trades_log)

    async def execute_leg_1(self, position: LegInPosition) -> bool:
        if self.config.paper_trading:
            self._log_paper_trade("LEG1", position)
            position.leg_1_filled = True
            return True

        token_id = position.yes_token_id if position.leg_1_side == "YES" else position.no_token_id
        order_args = OrderArgs(
            token_id=token_id,
            price=position.leg_1_entry_price,
            size=position.leg_1_size,
            side="BUY",
            order_type=OrderType.FOK,
        )
        try:
            await asyncio.to_thread(self.client.create_and_post_order, order_args)
            position.leg_1_filled = True
            return True
        except Exception as e:
            self.logger.exception("Leg 1 order failed: %s", e)
            return False

    async def execute_leg_2(self, position: LegInPosition, price: float, size: float) -> bool:
        if self.config.paper_trading:
            position.leg_2_entry_price = price
            position.leg_2_size = size
            position.leg_2_filled = True
            self._log_paper_trade("LEG2", position)
            return True

        opp_token_id = (
            position.no_token_id if position.leg_1_side == "YES" else position.yes_token_id
        )
        order_args = OrderArgs(
            token_id=opp_token_id,
            price=price,
            size=size,
            side="BUY",
            order_type=OrderType.FOK,
        )
        try:
            await asyncio.to_thread(self.client.create_and_post_order, order_args)
            position.leg_2_entry_price = price
            position.leg_2_size = size
            position.leg_2_filled = True
            return True
        except Exception as e:
            self.logger.exception("Leg 2 order failed: %s", e)
            return False

    async def execute_unwind(self, position: LegInPosition, price: float, size: float) -> bool:
        if self.config.paper_trading:
            self._log_paper_trade("UNWIND", position)
            return True
        # Unwind by selling the leg 1 token back.
        token_id = position.yes_token_id if position.leg_1_side == "YES" else position.no_token_id
        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side="SELL",
            order_type=OrderType.FAK,
        )
        try:
            await asyncio.to_thread(self.client.create_and_post_order, order_args)
            return True
        except Exception as e:
            self.logger.exception("Unwind order failed: %s", e)
            return False

    async def merge_tokens(self, position: LegInPosition) -> bool:
        if self.config.paper_trading:
            self._log_paper_trade("MERGE", position)
            return True

        # TODO: Prefer API merge endpoint if available.
        self.logger.warning("merge_tokens not implemented for live trading")
        return False

    def _log_paper_trade(self, action: str, position: LegInPosition) -> None:
        is_new = not self._paper_log_path.exists()
        self._paper_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._paper_log_path.open("a", newline="") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow(
                    [
                        "action",
                        "condition_id",
                        "leg_1_side",
                        "leg_1_price",
                        "leg_1_size",
                        "leg_2_price",
                        "leg_2_size",
                    ]
                )
            w.writerow(
                [
                    action,
                    position.condition_id,
                    position.leg_1_side,
                    position.leg_1_entry_price,
                    position.leg_1_size,
                    position.leg_2_entry_price,
                    position.leg_2_size,
                ]
            )
