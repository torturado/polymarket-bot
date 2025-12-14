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
from src.telemetry import Telemetry
from src.utils.market_utils import MarketSpec


class LegInBot:
    def __init__(self, config: Config, *, telemetry: Telemetry | None = None):
        self.config = config
        self.config.validate()

        logging.basicConfig(level=getattr(logging, self.config.log_level.upper(), logging.INFO))
        self.logger = logging.getLogger(self.__class__.__name__)

        creds = None
        if (
            not self.config.paper_trading
            and self.config.api_key
            and self.config.api_secret
            and self.config.api_passphrase
        ):
            creds = ApiCreds(
                api_key=self.config.api_key,
                api_secret=self.config.api_secret,
                api_passphrase=self.config.api_passphrase,
            )

        key = None if self.config.paper_trading else self.config.private_key
        funder = None if self.config.paper_trading else self.config.funder

        self.client = ClobClient(
            host=self.config.host,
            chain_id=self.config.chain_id,
            key=key,
            creds=creds,
            funder=funder,
        )

        self.market_specs = self._load_market_specs()
        self.monitor = MarketMonitor(self.client, self.market_specs, self.config)
        self.position_manager = PositionManager(self.config)
        self.execution_engine = ExecutionEngine(self.client, self.config)

        self.telemetry = telemetry
        if self.telemetry:
            for spec in self.market_specs:
                self.telemetry.set_market_meta(
                    spec.condition_id,
                    label=spec.label,
                    event_slug=spec.event_slug,
                    start_epoch_utc=spec.start_epoch_utc,
                    strike_price=spec.strike_price,
                )

        self.positions: Dict[str, LegInPosition] = {}
        self._cooldown_until: Dict[str, float] = {}
        self._entry_counts: Dict[str, int] = {}
        self._last_entry_signal_at: Dict[str, float] = {}
        self._daily_pnl_usdc: float = 0.0
        self._daily_pnl_day_utc = _dt.datetime.now(_dt.timezone.utc).date()
        self._daily_loss_tripped: bool = False
        self._tasks_by_condition: Dict[str, asyncio.Task] = {}
        self._vacuum_placed: set[tuple[str, str, float]] = set()
        self._vacuum_tasks: set[asyncio.Task] = set()

        self._paper_balance: float = float(self.config.paper_initial_balance)
        self._paper_out_of_funds_logged: bool = False
        if self.config.paper_trading:
            self.logger.info("PAPER balance initialized: %.4f USDC", self._paper_balance)

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
            await self._cancel_pending_tasks()

    async def _cancel_pending_tasks(self) -> None:
        tasks = list(self._tasks_by_condition.values()) + list(self._vacuum_tasks)
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks_by_condition.clear()
        self._vacuum_tasks.clear()

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
        if self.config.paper_trading:
            if self._paper_balance <= 0:
                if not self._paper_out_of_funds_logged:
                    self._paper_out_of_funds_logged = True
                    self.logger.warning("PAPER out of funds: balance=%.4f; disabling new entries", self._paper_balance)
                    if self.telemetry:
                        self.telemetry.emit("paper_out_of_funds", balance=float(self._paper_balance))
                return False

            # Block entries if we can't cover an estimated leg-1 budget.
            required = float(self.config.max_position_size) * float(self.config.initial_entry_fraction)
            if self._paper_balance < required:
                if self.telemetry:
                    self.telemetry.emit(
                        "paper_insufficient_funds",
                        condition_id=condition_id,
                        balance=float(self._paper_balance),
                        required=float(required),
                        kind="entry",
                    )
                return False

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
        active_last = int(self.config.entry_active_last_s)
        if active_last > 0:
            try:
                secs_to_end = self._seconds_to_window_end(now)
                if secs_to_end > float(active_last):
                    return False
            except Exception:
                return False
        return True

    def _paper_debit(self, amount_usdc: float, *, condition_id: str, action: str) -> bool:
        if not self.config.paper_trading:
            return True
        amt = float(amount_usdc)
        if amt <= 0:
            return True
        if self._paper_balance < amt:
            self.logger.warning(
                "PAPER insufficient funds: action=%s condition=%s need=%.4f balance=%.4f",
                action,
                condition_id,
                amt,
                self._paper_balance,
            )
            if self.telemetry:
                self.telemetry.emit(
                    "paper_insufficient_funds",
                    condition_id=condition_id,
                    balance=float(self._paper_balance),
                    required=float(amt),
                    kind=action,
                )
            return False
        self._paper_balance -= amt
        if self.telemetry:
            self.telemetry.emit(
                "paper_balance",
                condition_id=condition_id,
                action=action,
                delta_usdc=-amt,
                balance_usdc=float(self._paper_balance),
            )
        return True

    def _paper_credit(self, amount_usdc: float, *, condition_id: str, action: str) -> None:
        if not self.config.paper_trading:
            return
        amt = float(amount_usdc)
        if amt <= 0:
            return
        self._paper_balance += amt
        if self.telemetry:
            self.telemetry.emit(
                "paper_balance",
                condition_id=condition_id,
                action=action,
                delta_usdc=amt,
                balance_usdc=float(self._paper_balance),
            )

    def _seconds_to_window_end(self, now_ts: float) -> float:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(self.config.market_timezone)
        now_local = _dt.datetime.fromtimestamp(now_ts, tz=tz)
        window_minutes = int(self.config.market_window_minutes)
        minute_bucket = (now_local.minute // window_minutes) * window_minutes
        window_start_local = now_local.replace(minute=minute_bucket, second=0, microsecond=0)
        window_end_local = window_start_local + _dt.timedelta(minutes=window_minutes)
        return float((window_end_local - now_local).total_seconds())

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
        if self.telemetry:
            self.telemetry.emit(
                "pnl",
                condition_id=condition_id,
                action=action,
                pnl_usdc=float(pnl_usdc),
                daily_pnl_usdc=float(self._daily_pnl_usdc),
            )

    def _finish_position(self, condition_id: str) -> None:
        if self.telemetry and condition_id in self.positions:
            p = self.positions.get(condition_id)
            if p is not None:
                self.telemetry.emit(
                    "position_closed",
                    condition_id=condition_id,
                    state=p.state.value,
                    side=p.leg_1_side,
                    entry_price=float(p.leg_1_entry_price),
                    size=float(p.leg_1_size),
                )
        self.positions.pop(condition_id, None)
        cooldown_s = float(self.config.reentry_cooldown_s)
        if cooldown_s > 0:
            self._cooldown_until[condition_id] = time.time() + cooldown_s
        if self.config.market_specs_file:
            self._sync_monitor_markets()

    def _set_task(self, condition_id: str, task: asyncio.Task) -> None:
        prev = self._tasks_by_condition.get(condition_id)
        if prev is not None and not prev.done():
            task.cancel()
            return

        self._tasks_by_condition[condition_id] = task

        def _cleanup(_t: asyncio.Task) -> None:
            cur = self._tasks_by_condition.get(condition_id)
            if cur is _t:
                self._tasks_by_condition.pop(condition_id, None)
            try:
                exc = _t.exception()
            except asyncio.CancelledError:
                return
            except Exception:
                return
            if exc is not None:
                self.logger.error(
                    "Background task failed condition=%s: %s",
                    condition_id,
                    exc,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )

        task.add_done_callback(_cleanup)

    def _track_vacuum_task(self, task: asyncio.Task) -> None:
        self._vacuum_tasks.add(task)

        def _cleanup(_t: asyncio.Task) -> None:
            self._vacuum_tasks.discard(_t)
            try:
                exc = _t.exception()
            except asyncio.CancelledError:
                return
            except Exception:
                return
            if exc is not None:
                self.logger.error(
                    "Vacuum task failed: %s",
                    exc,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )

        task.add_done_callback(_cleanup)

    def _schedule_vacuum_orders(self, update) -> None:
        if not self.config.vacuum_enabled or not self.config.vacuum_prices:
            return
        cid = update.condition_id
        prices = [float(p) for p in self.config.vacuum_prices]
        for side, token_id in (("YES", update.yes_token_id), ("NO", update.no_token_id)):
            for p in prices:
                key = (cid, side, p)
                if key in self._vacuum_placed:
                    continue

                if self.config.paper_trading:
                    usdc = float(self.config.vacuum_usdc_per_order)
                    if not self._paper_debit(usdc, condition_id=cid, action="VACUUM_RESERVE"):
                        return

                self._vacuum_placed.add(key)
                size = float(self.config.vacuum_usdc_per_order) / p
                task = asyncio.create_task(
                    self.execution_engine.place_limit_buy(
                        condition_id=cid,
                        side=side,
                        token_id=token_id,
                        price=p,
                        size=size,
                        action="VACUUM",
                    )
                )
                self._track_vacuum_task(task)
                if self.telemetry:
                    self.telemetry.emit(
                        "vacuum_order",
                        condition_id=cid,
                        side=side,
                        price=float(p),
                        size=float(size),
                        usdc=float(self.config.vacuum_usdc_per_order),
                    )

    async def _task_execute_leg_1(self, position: LegInPosition) -> None:
        cid = position.condition_id
        token_id = position.yes_token_id if position.leg_1_side == "YES" else position.no_token_id
        if self.telemetry:
            self.telemetry.emit(
                "leg1_start",
                condition_id=cid,
                side=position.leg_1_side,
                price=float(position.leg_1_entry_price),
                size=float(position.leg_1_size),
            )
        ok = await self.execution_engine.execute_leg_1(
            position, price_data_fn=lambda: self.monitor.get_current_price(token_id)
        )
        if not ok:
            self.logger.warning("Leg 1 failed condition=%s", cid)
            if self.positions.get(cid) is position:
                self.positions.pop(cid, None)
            return

        if self.config.paper_trading:
            cost = float(position.leg_1_entry_price) * float(position.leg_1_size)
            if not self._paper_debit(cost, condition_id=cid, action="BUY_LEG1"):
                if self.positions.get(cid) is position:
                    self.positions.pop(cid, None)
                return

        self._entry_counts[cid] = self._entry_counts.get(cid, 0) + 1
        position.state = PositionState.HOLDING
        if self.telemetry:
            self.telemetry.emit(
                "leg1_filled",
                condition_id=cid,
                side=position.leg_1_side,
                vwap=float(position.leg_1_entry_price),
                filled_size=float(position.leg_1_size),
            )

    async def _task_arb(self, position: LegInPosition, *, yes_ask: float, no_ask: float, yes_bid: float, no_bid: float) -> None:
        cid = position.condition_id
        # Buy the cheaper leg first to reduce exposure if leg2 fails.
        if yes_ask <= no_ask:
            position.leg_1_side = "YES"
            position.leg_1_entry_price = yes_ask
        else:
            position.leg_1_side = "NO"
            position.leg_1_entry_price = no_ask

        leg1_token_id = (
            position.yes_token_id if position.leg_1_side == "YES" else position.no_token_id
        )
        ok1 = await self.execution_engine.execute_leg_1(
            position, price_data_fn=lambda: self.monitor.get_current_price(leg1_token_id)
        )
        if not ok1:
            self.logger.warning("ARB leg1 failed condition=%s", cid)
            if self.positions.get(cid) is position:
                self.positions.pop(cid, None)
            return

        if self.config.paper_trading:
            cost1 = float(position.leg_1_entry_price) * float(position.leg_1_size)
            if not self._paper_debit(cost1, condition_id=cid, action="BUY_ARB_LEG1"):
                if self.positions.get(cid) is position:
                    self.positions.pop(cid, None)
                return

        self._entry_counts[cid] = self._entry_counts.get(cid, 0) + 1

        opp_side = "NO" if position.leg_1_side == "YES" else "YES"
        opp_ask = no_ask if opp_side == "NO" else yes_ask
        if self.telemetry:
            self.telemetry.emit(
                "arb_leg2_start",
                condition_id=cid,
                side=opp_side,
                price=float(opp_ask),
                size=float(position.leg_1_size),
            )
        opp_token_id = (
            position.no_token_id if position.leg_1_side == "YES" else position.yes_token_id
        )
        ok2 = await self.execution_engine.execute_leg_2(
            position,
            price=float(opp_ask),
            size=float(position.leg_1_size),
            price_data_fn=lambda: self.monitor.get_current_price(opp_token_id),
        )
        if not ok2:
            self.logger.warning("ARB leg2 failed condition=%s; attempting unwind", cid)
            position.state = PositionState.UNWINDING
            bid = yes_bid if position.leg_1_side == "YES" else no_bid
            await self._task_unwind(position, price=float(bid), size=float(position.leg_1_size), action="UNWIND")
            return

        if self.config.paper_trading:
            cost2 = float(position.leg_2_entry_price or opp_ask) * float(position.leg_2_size or position.leg_1_size)
            if not self._paper_debit(cost2, condition_id=cid, action="BUY_ARB_LEG2"):
                # Can't fund leg2: unwind leg1 immediately.
                position.state = PositionState.UNWINDING
                bid = yes_bid if position.leg_1_side == "YES" else no_bid
                await self._task_unwind(
                    position, price=float(bid), size=float(position.leg_1_size), action="UNWIND"
                )
                return

        position.state = PositionState.MERGING
        if self.telemetry:
            self.telemetry.emit("arb_merge", condition_id=cid)
        await self._task_merge(position)

    async def _task_close_and_merge(self, position: LegInPosition, *, price: float, size: float) -> None:
        cid = position.condition_id
        if self.telemetry:
            self.telemetry.emit(
                "close_start",
                condition_id=cid,
                buy_side="NO" if position.leg_1_side == "YES" else "YES",
                price=float(price),
                size=float(size),
            )
        position.leg_2_pending = True
        try:
            opp_token_id = (
                position.no_token_id if position.leg_1_side == "YES" else position.yes_token_id
            )
            ok = await self.execution_engine.execute_leg_2(
                position,
                price=price,
                size=size,
                price_data_fn=lambda: self.monitor.get_current_price(opp_token_id),
            )
        finally:
            position.leg_2_pending = False

        if not ok:
            self.logger.warning("Leg 2 failed condition=%s; keeping position open", cid)
            return

        if self.config.paper_trading:
            cost2 = float(position.leg_2_entry_price or price) * float(position.leg_2_size or size)
            if not self._paper_debit(cost2, condition_id=cid, action="BUY_LEG2"):
                self.logger.warning("PAPER cannot fund leg2 condition=%s; skipping merge", cid)
                position.leg_2_filled = False
                position.leg_2_entry_price = None
                position.leg_2_size = None
                position.state = PositionState.HOLDING
                return

        position.state = PositionState.MERGING
        if self.telemetry:
            self.telemetry.emit("merge_start", condition_id=cid)
        position.merge_pending = True
        try:
            merged = await self.execution_engine.merge_tokens(position)
        finally:
            position.merge_pending = False

        if merged:
            if self.config.paper_trading:
                amount = min(
                    float(position.leg_1_size),
                    float(position.leg_2_size if position.leg_2_size is not None else position.leg_1_size),
                )
                self._paper_credit(amount * 1.0, condition_id=cid, action="MERGE")
            leg_2_price = position.leg_2_entry_price
            if leg_2_price is not None:
                pnl = (1.0 - position.leg_1_entry_price - leg_2_price) * position.leg_1_size
                self._record_pnl(pnl, condition_id=cid, action="MERGE")
            else:
                self.logger.warning("MERGE pnl unknown (missing leg_2_entry_price) condition=%s", cid)
            self._finish_position(cid)
        else:
            self.logger.warning("MERGE failed condition=%s; keeping position open", cid)

    async def _task_merge(self, position: LegInPosition) -> None:
        cid = position.condition_id
        if self.telemetry:
            self.telemetry.emit("merge_start", condition_id=cid)
        position.merge_pending = True
        try:
            ok = await self.execution_engine.merge_tokens(position)
        finally:
            position.merge_pending = False

        if ok:
            if self.config.paper_trading:
                amount = min(
                    float(position.leg_1_size),
                    float(position.leg_2_size if position.leg_2_size is not None else position.leg_1_size),
                )
                self._paper_credit(amount * 1.0, condition_id=cid, action="MERGE")
            leg_2_price = position.leg_2_entry_price
            if leg_2_price is not None:
                pnl = (1.0 - position.leg_1_entry_price - leg_2_price) * position.leg_1_size
                self._record_pnl(pnl, condition_id=cid, action="MERGE")
            else:
                self.logger.warning("MERGE pnl unknown (missing leg_2_entry_price) condition=%s", cid)
            self._finish_position(cid)
        else:
            self.logger.warning("MERGE failed condition=%s; keeping position open", cid)

    async def _task_unwind(self, position: LegInPosition, *, price: float, size: float, action: str) -> None:
        cid = position.condition_id
        if self.telemetry:
            self.telemetry.emit(
                "sell_start",
                condition_id=cid,
                side=position.leg_1_side,
                price=float(price),
                size=float(size),
                action=action,
            )
        position.unwind_pending = True
        try:
            ok = await self.execution_engine.execute_unwind(position, price=price, size=size)
        finally:
            position.unwind_pending = False

        if ok:
            if self.config.paper_trading:
                revenue = float(price) * float(size)
                self._paper_credit(revenue, condition_id=cid, action=f"SELL_{action}")
            pnl = (price - position.leg_1_entry_price) * position.leg_1_size
            self._record_pnl(pnl, condition_id=cid, action=action)
            self._finish_position(cid)
        else:
            self.logger.warning("UNWIND failed condition=%s; keeping position open", cid)

    async def _task_scalein(self, position: LegInPosition) -> None:
        cid = position.condition_id
        position.scalein_pending = True
        try:
            add_usdc = float(position.scalein_next_usdc or 0.0)
            position.scalein_next_usdc = None
            if add_usdc <= 0:
                position.state = PositionState.HOLDING
                return

            token_id = position.yes_token_id if position.leg_1_side == "YES" else position.no_token_id
            if self.telemetry:
                self.telemetry.emit(
                    "scalein_start",
                    condition_id=cid,
                    side=position.leg_1_side,
                    add_usdc=float(add_usdc),
                )

            def _pd():
                return self.monitor.get_current_price(token_id)

            pd0 = _pd()
            ref_price = float(pd0.best_ask) if pd0 is not None else float(position.leg_1_entry_price)
            if ref_price <= 0:
                position.state = PositionState.HOLDING
                return
            add_shares = add_usdc / ref_price

            if self.config.use_burst_execution:
                filled, vwap_price, _ = await self.execution_engine.execute_burst_buy(
                    token_id=token_id,
                    total_size=add_shares,
                    reference_price=ref_price,
                    max_price=None,
                    price_fn=lambda: (p.best_ask if (p := _pd()) is not None else ref_price),
                    best_ask_size_fn=lambda: (
                        float(getattr(p, "best_ask_size", 0.0)) if (p := _pd()) is not None else 0.0
                    ),
                    stop_on_price_change=None,
                    on_slice=(
                        (lambda p, s: self.execution_engine._log_paper_slice_leg1(position, price=p, size=s))
                        if (self.config.paper_trading and self.config.burst_log_slices_in_paper)
                        else None
                    ),
                )
            else:
                ok = await self.execution_engine.place_limit_buy(
                    condition_id=cid,
                    side=position.leg_1_side,
                    token_id=token_id,
                    price=ref_price,
                    size=add_shares,
                    action="SCALEIN",
                    order_type="FOK",
                )
                if not ok:
                    filled = 0.0
                    vwap_price = float("nan")
                else:
                    filled = float(add_shares)
                    vwap_price = float(ref_price)

            if filled <= 0:
                position.state = PositionState.HOLDING
                return

            old_shares = float(position.leg_1_size)
            old_price = float(position.leg_1_entry_price)
            new_total_shares = old_shares + float(filled)
            new_avg = ((old_price * old_shares) + (float(vwap_price) * float(filled))) / new_total_shares
            position.leg_1_size = new_total_shares
            position.leg_1_entry_price = new_avg
            position.scalein_count += 1
            position.last_scalein_at = time.time()
            position.state = PositionState.HOLDING
            if self.telemetry:
                self.telemetry.emit(
                    "scalein_filled",
                    condition_id=cid,
                    side=position.leg_1_side,
                    new_avg=float(position.leg_1_entry_price),
                    new_size=float(position.leg_1_size),
                    scalein_count=int(position.scalein_count),
                )
        finally:
            position.scalein_pending = False

    async def _handle_update(self, update) -> None:
        cid = update.condition_id
        if self.telemetry:
            self.telemetry.ingest_market_update(update)
        self._schedule_vacuum_orders(update)
        position = self.positions.get(cid)

        if position is None:
            now = time.time()
            if not self._entry_allowed(cid, now):
                return

            if self.config.arb_enabled:
                yes_ask = float(update.prices["YES"].best_ask)
                no_ask = float(update.prices["NO"].best_ask)
                entry_exit_cost = yes_ask + no_ask
                if (
                    yes_ask > 0
                    and no_ask > 0
                    and entry_exit_cost > 0
                    and entry_exit_cost <= float(self.config.arb_max_entry_exit_cost)
                ):
                    if self.config.paper_trading:
                        budget = float(self.config.arb_budget_usdc)
                        if self._paper_balance < budget:
                            if self.telemetry:
                                self.telemetry.emit(
                                    "paper_insufficient_funds",
                                    condition_id=cid,
                                    balance=float(self._paper_balance),
                                    required=float(budget),
                                    kind="arb",
                                )
                            return
                    shares = float(self.config.arb_budget_usdc) / entry_exit_cost
                    self.logger.info(
                        "ARB signal condition=%s cost=%.4f shares=%.4f yes_ask=%.4f no_ask=%.4f",
                        cid,
                        entry_exit_cost,
                        shares,
                        yes_ask,
                        no_ask,
                    )
                    if self.telemetry:
                        self.telemetry.emit(
                            "arb_signal",
                            condition_id=cid,
                            yes_ask=float(yes_ask),
                            no_ask=float(no_ask),
                            entry_exit_cost=float(entry_exit_cost),
                            shares=float(shares),
                        )
                    self._last_entry_signal_at[cid] = now
                    pos = LegInPosition(
                        condition_id=cid,
                        yes_token_id=update.yes_token_id,
                        no_token_id=update.no_token_id,
                        leg_1_side="YES",
                        leg_1_entry_price=yes_ask,
                        leg_1_size=shares,
                        leg_1_filled=False,
                        state=PositionState.ENTERING_LEG_1,
                        entry_time=now,
                    )
                    self.positions[cid] = pos
                    if self.telemetry:
                        self.telemetry.emit(
                            "position_opened",
                            condition_id=cid,
                            mode="arb",
                            side=pos.leg_1_side,
                            entry_price=float(pos.leg_1_entry_price),
                            size=float(pos.leg_1_size),
                        )
                    task = asyncio.create_task(
                        self._task_arb(
                            pos,
                            yes_ask=yes_ask,
                            no_ask=no_ask,
                            yes_bid=float(update.prices["YES"].best_bid),
                            no_bid=float(update.prices["NO"].best_bid),
                        )
                    )
                    self._set_task(cid, task)
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
                if self.telemetry:
                    self.telemetry.emit(
                        "entry_signal",
                        condition_id=cid,
                        side=new_pos.leg_1_side,
                        ask=float(new_pos.leg_1_entry_price),
                        opp_ask=float(update.prices[opp_side].best_ask),
                        entry_exit_cost=float(entry_exit_cost),
                    )
                self._last_entry_signal_at[cid] = now
                self.positions[cid] = new_pos
                if self.telemetry:
                    self.telemetry.emit(
                        "position_opened",
                        condition_id=cid,
                        mode="leg_in",
                        side=new_pos.leg_1_side,
                        entry_price=float(new_pos.leg_1_entry_price),
                        size=float(new_pos.leg_1_size),
                    )
                task = asyncio.create_task(self._task_execute_leg_1(new_pos))
                self._set_task(cid, task)
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
            if self.telemetry:
                self.telemetry.emit(
                    "state_change",
                    condition_id=cid,
                    prev=prev_state.value,
                    new=state.value,
                    exit_cost=None if exit_cost is None else float(exit_cost),
                )

        if state == PositionState.CLOSING and not position.leg_2_filled and not position.leg_2_pending:
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
            if self.config.paper_trading:
                est_cost = float(price) * float(size)
                if self._paper_balance < est_cost:
                    self.logger.warning(
                        "PAPER cannot afford leg2: condition=%s need=%.4f balance=%.4f",
                        cid,
                        est_cost,
                        self._paper_balance,
                    )
                    if self.telemetry:
                        self.telemetry.emit(
                            "paper_insufficient_funds",
                            condition_id=cid,
                            balance=float(self._paper_balance),
                            required=float(est_cost),
                            kind="close_leg2",
                        )
                    position.state = PositionState.HOLDING
                    return
            task = asyncio.create_task(self._task_close_and_merge(position, price=price, size=size))
            self._set_task(cid, task)

        if state == PositionState.TAKE_PROFIT and not position.unwind_pending:
            price = update.prices[position.leg_1_side].best_bid
            self.logger.info(
                "TAKE_PROFIT condition=%s sell=%s bid=%.4f size=%.4f",
                cid,
                position.leg_1_side,
                price,
                position.leg_1_size,
            )
            task = asyncio.create_task(
                self._task_unwind(position, price=price, size=position.leg_1_size, action="CHURN")
            )
            self._set_task(cid, task)

        if state == PositionState.UNWINDING and not position.unwind_pending:
            price = update.prices[position.leg_1_side].best_bid
            self.logger.info(
                "UNWIND condition=%s sell=%s bid=%.4f size=%.4f",
                cid,
                position.leg_1_side,
                price,
                position.leg_1_size,
            )
            task = asyncio.create_task(
                self._task_unwind(position, price=price, size=position.leg_1_size, action="UNWIND")
            )
            self._set_task(cid, task)

        if state == PositionState.SCALING_IN and not position.scalein_pending:
            add_usdc = float(position.scalein_next_usdc or 0.0)
            my_ask = update.prices[position.leg_1_side].best_ask
            self.logger.info(
                "SCALEIN condition=%s side=%s ask=%.4f add_usdc=%.4f",
                cid,
                position.leg_1_side,
                my_ask,
                add_usdc,
            )
            if self.config.paper_trading and add_usdc > 0 and self._paper_balance < add_usdc:
                self.logger.warning(
                    "PAPER cannot afford scale-in: condition=%s need=%.4f balance=%.4f",
                    cid,
                    add_usdc,
                    self._paper_balance,
                )
                position.scalein_next_usdc = None
                position.state = PositionState.HOLDING
                if self.telemetry:
                    self.telemetry.emit(
                        "paper_insufficient_funds",
                        condition_id=cid,
                        balance=float(self._paper_balance),
                        required=float(add_usdc),
                        kind="scalein",
                    )
                return
            task = asyncio.create_task(self._task_scalein(position))
            self._set_task(cid, task)

        if state == PositionState.MERGING and not position.merge_pending:
            self.logger.info("MERGE condition=%s", cid)
            task = asyncio.create_task(self._task_merge(position))
            self._set_task(cid, task)


async def main() -> None:
    config = Config.from_env()
    bot = LegInBot(config)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
