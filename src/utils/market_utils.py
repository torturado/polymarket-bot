from __future__ import annotations

import statistics
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class MarketSpec:
    condition_id: str
    yes_token_id: str
    no_token_id: str


@dataclass
class PriceData:
    best_bid: float
    best_ask: float
    timestamp: float
    best_bid_size: float = 0.0
    best_ask_size: float = 0.0

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2.0


@dataclass
class MarketUpdate:
    condition_id: str
    yes_token_id: str
    no_token_id: str
    prices: Dict[str, PriceData]  # keys: "YES", "NO"
    received_at: float
    # Cached depth (USDC) from the latest orderbook snapshot, per side.
    # By default this is empty; WS mode fills it from "book" snapshots.
    book_depth_usdc: Dict[str, float] = field(default_factory=dict)


def best_prices_from_orderbook(orderbook) -> Optional[PriceData]:
    bids = getattr(orderbook, "bids", None) or []
    asks = getattr(orderbook, "asks", None) or []
    if not bids or not asks:
        return None

    try:
        bid_levels = [(float(b.price), float(b.size)) for b in bids]
        ask_levels = [(float(a.price), float(a.size)) for a in asks]
    except Exception:
        return None

    if not bid_levels or not ask_levels:
        return None

    best_bid = max(p for p, _ in bid_levels)
    best_ask = min(p for p, _ in ask_levels)
    best_bid_size = sum(sz for p, sz in bid_levels if p == best_bid)
    best_ask_size = sum(sz for p, sz in ask_levels if p == best_ask)

    ts = time.time()
    return PriceData(
        best_bid=best_bid,
        best_ask=best_ask,
        timestamp=ts,
        best_bid_size=best_bid_size,
        best_ask_size=best_ask_size,
    )


def compute_volatility(prices: List[float]) -> float:
    if len(prices) < 2:
        return 0.0
    returns = [(prices[i] - prices[i - 1]) / prices[i - 1] for i in range(1, len(prices))]
    if len(returns) < 2:
        return abs(returns[0]) if returns else 0.0
    return statistics.pstdev(returns)


def theoretical_spread(yes_price: float, no_price: float) -> float:
    return yes_price + no_price


def opposite_side(side: str) -> str:
    s = side.upper()
    if s == "YES":
        return "NO"
    if s == "NO":
        return "YES"
    raise ValueError(f"Unknown side: {side}")
