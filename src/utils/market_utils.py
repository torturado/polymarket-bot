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
        best_bid = float(bids[0].price)
        best_ask = float(asks[0].price)
    except Exception:
        return None

    ts = time.time()
    return PriceData(best_bid=best_bid, best_ask=best_ask, timestamp=ts)


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
