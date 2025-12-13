from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import Any, Deque, Dict, List, Optional, Set

from .utils.market_utils import MarketUpdate


@dataclass(frozen=True)
class TelemetryEvent:
    id: int
    ts: float
    type: str
    data: Dict[str, Any]


class Telemetry:
    def __init__(self, *, max_events: int = 500):
        self._next_id = 1
        self._events: Deque[TelemetryEvent] = deque(maxlen=max(int(max_events), 1))
        self._subscribers: Set[asyncio.Queue] = set()
        self._markets: Dict[str, Dict[str, Any]] = {}

    def ingest_market_update(self, update: MarketUpdate) -> None:
        cid = update.condition_id
        yes = update.prices.get("YES")
        no = update.prices.get("NO")
        if yes is None or no is None:
            return
        self._markets[cid] = {
            "condition_id": cid,
            "received_at": float(update.received_at),
            "yes_token_id": update.yes_token_id,
            "no_token_id": update.no_token_id,
            "yes": {
                "best_bid": float(yes.best_bid),
                "best_ask": float(yes.best_ask),
                "best_bid_size": float(getattr(yes, "best_bid_size", 0.0) or 0.0),
                "best_ask_size": float(getattr(yes, "best_ask_size", 0.0) or 0.0),
                "ts": float(yes.timestamp),
            },
            "no": {
                "best_bid": float(no.best_bid),
                "best_ask": float(no.best_ask),
                "best_bid_size": float(getattr(no, "best_bid_size", 0.0) or 0.0),
                "best_ask_size": float(getattr(no, "best_ask_size", 0.0) or 0.0),
                "ts": float(no.timestamp),
            },
            "sum_ask": float(yes.best_ask) + float(no.best_ask),
            "sum_bid": float(yes.best_bid) + float(no.best_bid),
        }

    def get_market(self, condition_id: str) -> Optional[Dict[str, Any]]:
        return self._markets.get(condition_id)

    def get_markets(self) -> List[Dict[str, Any]]:
        items = list(self._markets.values())
        items.sort(key=lambda m: (m.get("condition_id") or ""))
        return items

    def emit(self, type: str, **data: Any) -> TelemetryEvent:
        event = TelemetryEvent(
            id=self._next_id,
            ts=time.time(),
            type=str(type),
            data=dict(data),
        )
        self._next_id += 1
        self._events.append(event)

        dead: List[asyncio.Queue] = []
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                # Slow consumer: drop events.
                continue
            except Exception:
                dead.append(q)
        for q in dead:
            self._subscribers.discard(q)

        return event

    def get_events(self, *, limit: int = 200) -> List[Dict[str, Any]]:
        lim = max(int(limit), 0)
        events = list(self._events)[-lim:] if lim else list(self._events)
        return [asdict(e) for e in events]

    def subscribe(self, *, max_queue: int = 1000) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=max(int(max_queue), 1))
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

