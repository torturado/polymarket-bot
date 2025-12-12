from __future__ import annotations

import asyncio
import contextlib
import datetime as _dt
import json
import logging
import time
from pathlib import Path
from typing import Dict, List

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds

# Allow running as a script: `python src/leg_in_bot.py`
if __package__ in (None, ""):
    import sys as _sys

    _ROOT = Path(__file__).resolve().parents[1]
    if str(_ROOT) not in _sys.path:
        _sys.path.insert(0, str(_ROOT))

from src.config import Config
from src.execution_engine import ExecutionEngine
from src.market_monitor import MarketMonitor
from src.position_manager import LegInPosition, PositionManager, PositionState
from src.utils.market_utils import MarketSpec


class LegInBot:
    def __init__(self, config: Config):
        self.config = config
        self.config.validate()

        logging.basicConfig(level=getattr(logging, self.config.log_level.upper(), logging.INFO))
        self.logger = logging.getLogger(self.__class__.__name__)

        creds = None
        if self.config.api_key and self.config.api_secret and self.config.api_passphrase:
            creds = ApiCreds(
                api_key=self.config.api_key,
                api_secret=self.config.api_secret,
                api_passphrase=self.config.api_passphrase,
            )

        self.client = ClobClient(
            host=self.config.host,
            chain_id=self.config.chain_id,
            key=self.config.private_key,
            creds=creds,
            funder=self.config.funder,
        )

        self.market_specs = self._load_market_specs()
        self.monitor = MarketMonitor(self.client, self.market_specs, self.config)
        self.position_manager = PositionManager(self.config)
        self.execution_engine = ExecutionEngine(self.client, self.config)

        self.positions: Dict[str, LegInPosition] = {}
        self._cooldown_until: Dict[str, float] = {}
        self._entry_counts: Dict[str, int] = {}
        self._last_entry_signal_at: Dict[str, float] = {}
        self._daily_pnl_usdc: float = 0.0
        self._daily_pnl_day_utc = _dt.datetime.now(_dt.timezone.utc).date()
        self._daily_loss_tripped: bool = False

        # If using MARKET_SPECS_FILE, keep the file list separate from the
        # effective list (file + pinned open positions).
        self._base_market_specs: List[MarketSpec] = list(self.market_specs)
        self._monitor_specs_key = self._specs_key(self.market_specs)

    def _load_market_specs(self) -> List[MarketSpec]:
        if self.config.market_specs_file:
            return self._read_market_specs_file(self.config.market_specs_file)
        if self.config.market_specs:
            return [MarketSpec(**m) for m in self.config.market_specs]
        raise ValueError(
            "No market specs provided. Set MARKET_SPECS env to a JSON list "
            'like [{"condition_id":"...","yes_token_id":"...","no_token_id":"..."}]'
        )

    async def run(self) -> None:
        self.logger.info("Starting LegInBot with %d markets", len(self.market_specs))

        refresh_task = None
        if self.config.market_specs_file and self.config.market_refresh_interval_s > 0:
            refresh_task = asyncio.create_task(self._refresh_markets_loop())

        try:
            async for update in self.monitor.start_monitoring():
                await self._handle_update(update)
        finally:
            if refresh_task:
                refresh_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await refresh_task

    def _read_market_specs_file(self, path: str) -> List[MarketSpec]:
        data = json.loads(Path(path).read_text())
        if not isinstance(data, list):
            raise ValueError("MARKET_SPECS_FILE must contain a JSON list")
        return [MarketSpec(**m) for m in data]

    @staticmethod
    def _specs_key(specs: List[MarketSpec]):
        return sorted((s.condition_id, s.yes_token_id, s.no_token_id) for s in specs)

    async def _refresh_markets_loop(self) -> None:
        assert self.config.market_specs_file
        interval = float(self.config.market_refresh_interval_s)
        last_base_key = self._specs_key(self._base_market_specs)
        while True:
            await asyncio.sleep(interval)
            try:
                new_base_specs = self._read_market_specs_file(self.config.market_specs_file)
            except Exception as e:
                self.logger.warning("Failed to reload market specs: %s", e)
                continue

            base_key = self._specs_key(new_base_specs)
            if base_key != last_base_key:
                self._base_market_specs = new_base_specs
                last_base_key = base_key
                self._sync_monitor_markets(log=True)

    def _effective_market_specs(self) -> List[MarketSpec]:
        merged: Dict[str, MarketSpec] = {m.condition_id: m for m in self._base_market_specs}
        for pos in self.positions.values():
            merged.setdefault(
                pos.condition_id,
                MarketSpec(
                    condition_id=pos.condition_id,
                    yes_token_id=pos.yes_token_id,
                    no_token_id=pos.no_token_id,
                ),
            )
        return list(merged.values())

    def _sync_monitor_markets(self, *, log: bool = False) -> None:
        effective = self._effective_market_specs()
        key = self._specs_key(effective)
        if key == self._monitor_specs_key:
            return
        self._monitor_specs_key = key
        self.market_specs = effective
        self.monitor.set_markets(effective)
        if log:
            self.logger.info(
                "Market specs refreshed: base=%d effective=%d",
                len(self._base_market_specs),
                len(effective),
            )

    def _entry_allowed(self, condition_id: str, now: float) -> bool:
        self._roll_daily_pnl(now)
        if self.config.max_daily_loss > 0 and self._daily_pnl_usdc <= -float(self.config.max_daily_loss):
            if not self._daily_loss_tripped:
                self._daily_loss_tripped = True
                self.logger.warning(
                    "Daily loss limit reached: pnl=%.4f <= -%.4f; disabling new entries until next UTC day",
                    self._daily_pnl_usdc,
                    float(self.config.max_daily_loss),
                )
            return False
        if self.config.max_concurrent_positions > 0 and len(self.positions) >= self.config.max_concurrent_positions:
            return False
        if now < self._cooldown_until.get(condition_id, 0.0):
            return False
        max_entries = self.config.max_entries_per_condition
        if max_entries > 0 and self._entry_counts.get(condition_id, 0) >= max_entries:
            return False
        interval_s = max(int(self.config.entry_signal_min_interval_ms), 0) / 1000.0
        last = self._last_entry_signal_at.get(condition_id)
        if last is not None and interval_s > 0 and (now - last) < interval_s:
            return False
        return True

    def _roll_daily_pnl(self, now: float) -> None:
        day = _dt.datetime.fromtimestamp(now, tz=_dt.timezone.utc).date()
        if day != self._daily_pnl_day_utc:
            self._daily_pnl_day_utc = day
            self._daily_pnl_usdc = 0.0
            self._daily_loss_tripped = False

    def _record_pnl(self, pnl_usdc: float, *, condition_id: str, action: str) -> None:
        now = time.time()
        self._roll_daily_pnl(now)
        self._daily_pnl_usdc += float(pnl_usdc)
        self.logger.info(
            "PNL condition=%s action=%s pnl=%.4f daily_pnl=%.4f",
            condition_id,
            action,
            float(pnl_usdc),
            self._daily_pnl_usdc,
        )

    def _finish_position(self, condition_id: str) -> None:
        self.positions.pop(condition_id, None)
        cooldown_s = float(self.config.reentry_cooldown_s)
        if cooldown_s > 0:
            self._cooldown_until[condition_id] = time.time() + cooldown_s
        if self.config.market_specs_file:
            self._sync_monitor_markets()

    async def _handle_update(self, update) -> None:
        cid = update.condition_id
        position = self.positions.get(cid)

        if position is None:
            now = time.time()
            if not self._entry_allowed(cid, now):
                return
            new_pos = self.position_manager.evaluate_entry(update)
            if new_pos:
                opp_side = "NO" if new_pos.leg_1_side == "YES" else "YES"
                entry_exit_cost = new_pos.leg_1_entry_price + update.prices[opp_side].best_ask
                self.logger.info(
                    "ENTRY signal condition=%s side=%s ask=%.4f opp_ask=%.4f entry_exit_cost=%.4f",
                    new_pos.condition_id,
                    new_pos.leg_1_side,
                    new_pos.leg_1_entry_price,
                    update.prices[opp_side].best_ask,
                    entry_exit_cost,
                )
                self._last_entry_signal_at[cid] = now
                self.positions[cid] = new_pos
                ok = await self.execution_engine.execute_leg_1(new_pos)
                if not ok:
                    self.logger.warning("Leg 1 failed condition=%s", cid)
                    self.positions.pop(cid, None)
                else:
                    self._entry_counts[cid] = self._entry_counts.get(cid, 0) + 1
            return

        prev_state = position.state
        state = self.position_manager.update_position(position, update)
        if state != prev_state:
            exit_cost = None
            if state in (PositionState.CLOSING, PositionState.UNWINDING):
                try:
                    exit_cost = self.position_manager.calculate_exit_cost(position, update.prices)
                except Exception:
                    exit_cost = None
            self.logger.info(
                "STATE condition=%s %s -> %s%s",
                cid,
                prev_state.value,
                state.value,
                "" if exit_cost is None else f" exit_cost={exit_cost:.4f}",
            )

        if state == PositionState.CLOSING and not position.leg_2_filled:
            opp_side = "NO" if position.leg_1_side == "YES" else "YES"
            price = update.prices[opp_side].best_ask
            size = position.leg_1_size
            self.logger.info(
                "CLOSE condition=%s buy=%s ask=%.4f size=%.4f",
                cid,
                opp_side,
                price,
                size,
            )
            await self.execution_engine.execute_leg_2(position, price=price, size=size)

        if state == PositionState.UNWINDING:
            price = update.prices[position.leg_1_side].best_bid
            self.logger.info(
                "UNWIND condition=%s sell=%s bid=%.4f size=%.4f",
                cid,
                position.leg_1_side,
                price,
                position.leg_1_size,
            )
            ok = await self.execution_engine.execute_unwind(
                position, price=price, size=position.leg_1_size
            )
            if ok:
                pnl = (price - position.leg_1_entry_price) * position.leg_1_size
                self._record_pnl(pnl, condition_id=cid, action="UNWIND")
                self._finish_position(cid)
            else:
                self.logger.warning("UNWIND failed condition=%s; keeping position open", cid)

        if state == PositionState.MERGING:
            self.logger.info("MERGE condition=%s", cid)
            ok = await self.execution_engine.merge_tokens(position)
            if ok:
                leg_2_price = position.leg_2_entry_price
                if leg_2_price is not None:
                    pnl = (1.0 - position.leg_1_entry_price - leg_2_price) * position.leg_1_size
                    self._record_pnl(pnl, condition_id=cid, action="MERGE")
                else:
                    self.logger.warning("MERGE pnl unknown (missing leg_2_entry_price) condition=%s", cid)
                self._finish_position(cid)
            else:
                self.logger.warning("MERGE failed condition=%s; keeping position open", cid)


async def main() -> None:
    config = Config.from_env()
    bot = LegInBot(config)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
