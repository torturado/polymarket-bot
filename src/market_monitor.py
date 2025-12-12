from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from py_clob_client.clob_types import BookParams
from py_clob_client.client import ClobClient

from .config import Config
from .utils.market_utils import (
    MarketSpec,
    MarketUpdate,
    PriceData,
    best_prices_from_orderbook,
)


class MarketMonitor:
    def __init__(self, client: ClobClient, markets: List[MarketSpec], config: Config):
        self.client = client
        self.markets = markets
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

        self._condition_to_pair: Dict[str, Tuple[str, str]] = {}
        self._resubscribe_event = asyncio.Event()
        self.set_markets(markets, trigger_resubscribe=False)
        self._prices: Dict[str, Optional[float]] = {}
        self._price_data: Dict[str, Optional[object]] = {}
        self._ask_depth_usdc: Dict[str, float] = {}

    def set_markets(self, markets: List[MarketSpec], *, trigger_resubscribe: bool = True) -> None:
        """
        Update the list of markets being monitored. In WS mode this will trigger
        a reconnect + resubscribe on the next tick.
        """
        self.markets = markets
        self._condition_to_pair = {
            m.condition_id: (m.yes_token_id, m.no_token_id) for m in markets
        }
        if trigger_resubscribe and self.config.use_websocket:
            self._resubscribe_event.set()

    def get_current_price(self, token_id: str):
        return self._price_data.get(token_id)

    def get_market_pair(self, condition_id: str) -> Tuple[str, str]:
        return self._condition_to_pair[condition_id]

    async def start_monitoring(self) -> AsyncIterator[MarketUpdate]:
        if self.config.use_websocket:
            async for update in self._monitor_via_websocket():
                yield update
        else:
            async for update in self._monitor_via_polling():
                yield update

    async def _monitor_via_polling(self) -> AsyncIterator[MarketUpdate]:
        interval = self.config.polling_interval_ms / 1000.0
        depth_levels = max(int(self.config.book_depth_levels), 0)
        token_ids: List[str] = []
        for m in self.markets:
            token_ids.extend([m.yes_token_id, m.no_token_id])

        params = [BookParams(token_id=t) for t in token_ids]

        while True:
            try:
                order_books = await asyncio.to_thread(self.client.get_order_books, params)
                now = time.time()
                price_map: Dict[str, object] = {}
                for token_id, ob in zip(token_ids, order_books):
                    pd = best_prices_from_orderbook(ob)
                    self._price_data[token_id] = pd
                    price_map[token_id] = pd
                    if depth_levels > 0:
                        asks = getattr(ob, "asks", None) or []
                        self._ask_depth_usdc[token_id] = self._compute_ask_depth_usdc(
                            asks, depth_levels
                        )

                for m in self.markets:
                    yes_pd = price_map.get(m.yes_token_id)
                    no_pd = price_map.get(m.no_token_id)
                    if yes_pd is None or no_pd is None:
                        continue
                    update = MarketUpdate(
                        condition_id=m.condition_id,
                        yes_token_id=m.yes_token_id,
                        no_token_id=m.no_token_id,
                        prices={"YES": yes_pd, "NO": no_pd},
                        received_at=now,
                        book_depth_usdc={
                            "YES": self._ask_depth_usdc.get(m.yes_token_id, 0.0),
                            "NO": self._ask_depth_usdc.get(m.no_token_id, 0.0),
                        },
                    )
                    yield update
            except Exception as e:
                self.logger.exception("Polling error: %s", e)

            await asyncio.sleep(interval)

    async def _monitor_via_websocket(self) -> AsyncIterator[MarketUpdate]:
        """
        WS consumer for Polymarket CLOB subscriptions.
        Handles the real-time payloads shaped like:
        {"event_type":"price_change","price_changes":[{"asset_id": "...","best_bid":"...","best_ask":"..."}]}
        """
        try:
            import websockets  # type: ignore
        except Exception:
            self.logger.warning("websockets not installed; falling back to polling")
            async for u in self._monitor_via_polling():
                yield u
            return

        while True:
            current_markets = list(self.markets)
            token_to_condition: Dict[str, MarketSpec] = {}
            condition_to_spec: Dict[str, MarketSpec] = {}
            for m in current_markets:
                token_to_condition[m.yes_token_id] = m
                token_to_condition[m.no_token_id] = m
                condition_to_spec[m.condition_id] = m

            headers = self._build_ws_headers()

            # websockets>=15 uses additional_headers; keep a fallback for older versions.
            try:
                connect_cm = websockets.connect(
                    self.config.ws_url, additional_headers=headers
                )
            except TypeError:  # pragma: no cover
                connect_cm = websockets.connect(self.config.ws_url, extra_headers=headers)

            try:
                async with connect_cm as ws:
                    self.logger.info("Connected to WS %s", self.config.ws_url)

                    subscribe_message = (
                        self.config.ws_subscribe_message
                        or self._build_default_subscribe_message(current_markets)
                    )
                    if subscribe_message:
                        try:
                            await ws.send(json.dumps(subscribe_message))
                            self.logger.info(
                                "Subscribed to %d assets",
                                len(subscribe_message.get("assets_ids", [])),
                            )
                        except Exception as e:
                            self.logger.warning("Failed to send WS subscribe message: %s", e)

                    heartbeat_s = max(int(self.config.ws_heartbeat_interval_s), 0)
                    last_stats_at = time.time()
                    last_msg_at: Optional[float] = None
                    msgs = 0
                    books = 0
                    price_change_msgs = 0
                    emitted_updates = 0

                    while True:
                        if self._resubscribe_event.is_set():
                            self._resubscribe_event.clear()
                            break

                        try:
                            if heartbeat_s > 0:
                                raw = await asyncio.wait_for(ws.recv(), timeout=heartbeat_s)
                            else:
                                raw = await ws.recv()
                        except asyncio.TimeoutError:
                            age = None if last_msg_at is None else time.time() - last_msg_at
                            age_str = "n/a" if age is None else f"{age:.1f}s"
                            self.logger.info(
                                "WS heartbeat: msgs=%d books=%d price_changes=%d updates=%d last_msg_age=%s",
                                msgs,
                                books,
                                price_change_msgs,
                                emitted_updates,
                                age_str,
                            )
                            msgs = books = price_change_msgs = emitted_updates = 0
                            last_stats_at = time.time()
                            continue

                        # If markets changed while we were awaiting recv(), don't
                        # process stale messages from the old subscription.
                        if self._resubscribe_event.is_set():
                            self._resubscribe_event.clear()
                            break

                        last_msg_at = time.time()
                        msgs += 1

                        for msg in self._iter_json_messages(raw):
                            if self._resubscribe_event.is_set():
                                break
                            if not isinstance(msg, dict):
                                continue

                            # Initial orderbook snapshot payload(s)
                            if msg.get("event_type") == "book":
                                books += 1
                                asset_id = msg.get("asset_id") or msg.get("token_id")
                                self._ingest_book_snapshot(msg, token_to_condition)
                                if asset_id and asset_id in token_to_condition:
                                    spec = token_to_condition[asset_id]
                                    yes_id, no_id = spec.yes_token_id, spec.no_token_id
                                    yes_pd = self._price_data.get(yes_id)
                                    no_pd = self._price_data.get(no_id)
                                    if yes_pd and no_pd:
                                        emitted_updates += 1
                                        yield MarketUpdate(
                                            condition_id=spec.condition_id,
                                            yes_token_id=yes_id,
                                            no_token_id=no_id,
                                            prices={"YES": yes_pd, "NO": no_pd},
                                            received_at=time.time(),
                                            book_depth_usdc={
                                                "YES": self._ask_depth_usdc.get(yes_id, 0.0),
                                                "NO": self._ask_depth_usdc.get(no_id, 0.0),
                                            },
                                        )
                                continue

                            # New CLOB payload
                            changes = msg.get("price_changes")
                            if changes:
                                price_change_msgs += 1
                                updated_specs: Dict[str, MarketSpec] = {}
                                now = time.time()
                                for change in changes:
                                    asset_id = change.get("asset_id") or change.get("token_id")
                                    if not asset_id or asset_id not in token_to_condition:
                                        continue
                                    try:
                                        best_bid = float(change.get("best_bid") or 0)
                                        best_ask = float(change.get("best_ask") or 0)
                                    except Exception:
                                        continue
                                    if best_bid <= 0 or best_ask <= 0:
                                        continue
                                    pd = PriceData(
                                        best_bid=best_bid, best_ask=best_ask, timestamp=now
                                    )
                                    self._price_data[asset_id] = pd
                                    spec = token_to_condition[asset_id]
                                    updated_specs[spec.condition_id] = spec

                                for cid, spec in updated_specs.items():
                                    if self._resubscribe_event.is_set():
                                        break
                                    # If the condition dropped from the latest specs
                                    # (mid-resubscribe), just skip instead of crashing.
                                    spec = condition_to_spec.get(cid, spec)
                                    yes_id, no_id = spec.yes_token_id, spec.no_token_id
                                    yes_pd = self._price_data.get(yes_id)
                                    no_pd = self._price_data.get(no_id)
                                    if yes_pd and no_pd:
                                        emitted_updates += 1
                                        yield MarketUpdate(
                                            condition_id=cid,
                                            yes_token_id=yes_id,
                                            no_token_id=no_id,
                                            prices={"YES": yes_pd, "NO": no_pd},
                                            received_at=now,
                                            book_depth_usdc={
                                                "YES": self._ask_depth_usdc.get(yes_id, 0.0),
                                                "NO": self._ask_depth_usdc.get(no_id, 0.0),
                                            },
                                        )
                                continue

                            # Fallback single-asset payload
                            asset_id = msg.get("asset_id") or msg.get("token_id")
                            if not asset_id or asset_id not in token_to_condition:
                                continue
                            try:
                                best_bid = float(msg.get("best_bid") or msg.get("bid") or 0)
                                best_ask = float(msg.get("best_ask") or msg.get("ask") or 0)
                            except Exception:
                                continue
                            if best_bid <= 0 or best_ask <= 0:
                                continue
                            pd = PriceData(
                                best_bid=best_bid, best_ask=best_ask, timestamp=time.time()
                            )
                            self._price_data[asset_id] = pd
                            spec = token_to_condition[asset_id]
                            yes_pd = self._price_data.get(spec.yes_token_id)
                            no_pd = self._price_data.get(spec.no_token_id)
                            if yes_pd and no_pd:
                                emitted_updates += 1
                                yield MarketUpdate(
                                    condition_id=spec.condition_id,
                                    yes_token_id=spec.yes_token_id,
                                    no_token_id=spec.no_token_id,
                                    prices={"YES": yes_pd, "NO": no_pd},
                                    received_at=time.time(),
                                    book_depth_usdc={
                                        "YES": self._ask_depth_usdc.get(spec.yes_token_id, 0.0),
                                        "NO": self._ask_depth_usdc.get(spec.no_token_id, 0.0),
                                    },
                                )

                        if self._resubscribe_event.is_set():
                            self._resubscribe_event.clear()
                            break

                        if heartbeat_s > 0 and (time.time() - last_stats_at) >= heartbeat_s:
                            age = None if last_msg_at is None else time.time() - last_msg_at
                            age_str = "n/a" if age is None else f"{age:.1f}s"
                            self.logger.info(
                                "WS heartbeat: msgs=%d books=%d price_changes=%d updates=%d last_msg_age=%s",
                                msgs,
                                books,
                                price_change_msgs,
                                emitted_updates,
                                age_str,
                            )
                            msgs = books = price_change_msgs = emitted_updates = 0
                            last_stats_at = time.time()
            except Exception as e:
                self.logger.exception("WebSocket error, retrying: %s", e)

            await asyncio.sleep(1.0)

    def _build_ws_headers(self) -> Optional[Dict[str, str]]:
        headers: Dict[str, str] = {}
        if self.config.ws_headers:
            headers.update(self.config.ws_headers)
        if self.config.ws_cookies:
            cookie_header = "; ".join(f"{k}={v}" for k, v in self.config.ws_cookies.items())
            if cookie_header:
                headers.setdefault("Cookie", cookie_header)
        return headers or None

    @staticmethod
    def _build_default_subscribe_message(markets: List[MarketSpec]) -> Optional[Dict[str, Any]]:
        assets: List[str] = []
        for m in markets:
            assets.extend([m.yes_token_id, m.no_token_id])
        # unique, stable order
        unique_assets = sorted(set(assets))
        if not unique_assets:
            return None
        return {"type": "market", "assets_ids": unique_assets}

    def _ingest_book_snapshot(
        self, msg: Dict[str, Any], token_to_condition: Dict[str, MarketSpec]
    ) -> None:
        asset_id = msg.get("asset_id") or msg.get("token_id")
        if not asset_id or asset_id not in token_to_condition:
            return
        bids = msg.get("bids") or []
        asks = msg.get("asks") or []
        depth_levels = max(int(self.config.book_depth_levels), 0)
        try:
            best_bid = max(float(b["price"]) for b in bids) if bids else 0.0
            best_ask = min(float(a["price"]) for a in asks) if asks else 0.0
        except Exception:
            return
        if best_bid <= 0 or best_ask <= 0:
            return
        now = time.time()
        self._price_data[asset_id] = PriceData(
            best_bid=best_bid, best_ask=best_ask, timestamp=now
        )
        if depth_levels > 0:
            self._ask_depth_usdc[asset_id] = self._compute_ask_depth_usdc(
                asks, depth_levels
            )

    @staticmethod
    def _compute_ask_depth_usdc(levels: List[Any], max_levels: int) -> float:
        if max_levels <= 0:
            return 0.0
        entries = []
        for level in levels:
            if isinstance(level, dict):
                price = level.get("price")
                size = level.get("size")
            else:  # py-clob-client orderbook levels
                price = getattr(level, "price", None)
                size = getattr(level, "size", None)
            try:
                price_f = float(price)
                size_f = float(size)
            except Exception:
                continue
            if price_f <= 0 or size_f <= 0:
                continue
            entries.append((price_f, size_f))
        entries.sort(key=lambda t: t[0])
        return sum(price * size for price, size in entries[:max_levels])

    @staticmethod
    def _iter_json_messages(raw: str) -> List[Any]:
        messages: List[Any] = []
        for part in raw.splitlines():
            part = part.strip()
            if not part:
                continue
            try:
                parsed = json.loads(part)
                if isinstance(parsed, list):
                    messages.extend(parsed)
                else:
                    messages.append(parsed)
            except Exception:
                continue
        return messages
