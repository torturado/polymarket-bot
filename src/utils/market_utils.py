from __future__ import annotations

import re
import statistics
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class MarketSpec:
    condition_id: str
    yes_token_id: str
    no_token_id: str
    # Optional metadata (e.g. Gamma event fields)
    label: Optional[str] = None
    event_slug: Optional[str] = None
    start_epoch_utc: Optional[int] = None
    strike_price: Optional[float] = None


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


_PRICE_NUM_RE = re.compile(r"(?P<num>(?:\d{1,3}(?:,\d{3})+|\d{4,})(?:\.\d+)?)")
_PRICE_WITH_DOLLAR_RE = re.compile(
    r"\$\s*(?P<num>(?:\d{1,3}(?:,\d{3})+|\d{4,})(?:\.\d+)?)"
)
_KEYWORD_PATTERNS = [
    re.compile(
        r"(?:price\s*to\s*beat|strike(?:\s*price)?|reference(?:\s*price)?|ref(?:\s*price)?|closing(?:\s*price)?|close(?:\s*price)?)"
        r"[^0-9$]{0,40}\$?\s*(?P<num>(?:\d{1,3}(?:,\d{3})+|\d{4,})(?:\.\d+)?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:price\s*(?:at|as\s*of)|oracle\s*price|settlement\s*price)"
        r"[^0-9$]{0,40}\$?\s*(?P<num>(?:\d{1,3}(?:,\d{3})+|\d{4,})(?:\.\d+)?)",
        re.IGNORECASE,
    ),
]


def extract_strike_price(
    text: str,
    *,
    min_value: float = 1000.0,
    max_value: Optional[float] = None,
) -> Optional[float]:
    """
    Heurística para extraer el "price to beat"/strike desde texto del mercado (Gamma).
    Pensado para BTC/ETH (precios altos) y tolerante a comas/decimales.
    """
    if not text:
        return None
    t = str(text).strip()
    if not t:
        return None

    def _parse_num(raw: str) -> Optional[float]:
        s = raw.replace(",", "").strip()
        try:
            return float(s)
        except Exception:
            return None

    def _is_epoch_like(raw: str, val: float) -> bool:
        s = raw.replace(",", "").strip()
        if not s:
            return False
        m = re.fullmatch(r"(\d+)(?:\.0+)?", s)
        if not m:
            return False
        digits = m.group(1)
        try:
            ival = int(digits)
        except Exception:
            return False
        # Epoch seconds (9–10 digits) or epoch milliseconds (12–13 digits).
        if len(digits) in (9, 10) and 1_000_000_000 <= ival <= 4_000_000_000:
            return True
        if len(digits) in (12, 13) and 1_000_000_000_000 <= ival <= 4_000_000_000_000:
            return True
        return False

    def _accept(raw: str, val: float) -> bool:
        if val < float(min_value):
            return False
        if max_value is not None and val > float(max_value):
            return False
        if _is_epoch_like(raw, val):
            return False
        return True

    # 1) Prefer keyword-based matches
    for rx in _KEYWORD_PATTERNS:
        m = rx.search(t)
        if not m:
            continue
        raw = m.group("num")
        val = _parse_num(raw)
        if val is None:
            continue
        if _accept(raw, val):
            return val

    # 2) Dollar-prefixed numbers (safe for 4-digit ETH, avoids years)
    for m in _PRICE_WITH_DOLLAR_RE.finditer(t):
        raw = m.group("num")
        val = _parse_num(raw)
        if val is None:
            continue
        if _accept(raw, val):
            return val

    # 3) Fallback: large-looking numbers only (commas or >= 10k)
    for m in _PRICE_NUM_RE.finditer(t):
        raw = m.group("num")
        val = _parse_num(raw)
        if val is None:
            continue
        if not _accept(raw, val):
            continue
        if "," in raw or val >= 10000:
            return val

    return None
