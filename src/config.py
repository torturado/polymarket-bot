from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _get_float(name: str, default: float) -> float:
    value = _get_env(name)
    return float(value) if value is not None else default


def _get_int(name: str, default: int) -> int:
    value = _get_env(name)
    return int(value) if value is not None else default


def _get_bool(name: str, default: bool) -> bool:
    value = _get_env(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_json(name: str) -> Optional[Dict]:
    raw = _get_env(name)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _get_json_any(name: str):
    raw = _get_env(name)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _strip_wrapping_quotes(value: str) -> str:
    v = value.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in {"'", '"'}:
        return v[1:-1].strip()
    return v


def _normalize_private_key(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = _strip_wrapping_quotes(value)
    if not v:
        return None
    return v


def _get_float_list(name: str) -> Optional[List[float]]:
    raw = _get_env(name)
    if raw is None:
        return None
    parsed = _get_json_any(name)
    if isinstance(parsed, list):
        out: List[float] = []
        for v in parsed:
            try:
                out.append(float(v))
            except Exception:
                continue
        return out or None

    # Fallback: comma-separated list.
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    out2: List[float] = []
    for p in parts:
        try:
            out2.append(float(p))
        except Exception:
            continue
    return out2 or None


@dataclass
class Config:
    # API Credentials
    host: str = "https://clob.polymarket.com"
    chain_id: int = 137
    private_key: Optional[str] = None
    funder: Optional[str] = None
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    api_passphrase: Optional[str] = None

    # WebSocket Configuration
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    ws_cookies: Optional[Dict[str, str]] = None
    ws_headers: Optional[Dict[str, str]] = None
    ws_subscribe_message: Optional[object] = None
    use_websocket: bool = False
    ws_heartbeat_interval_s: int = 15

    # Trading Parameters
    entry_threshold: float = 0.15
    target_profit_threshold: float = 0.98
    stop_loss_threshold: float = 1.05
    max_position_size: float = 100.0
    initial_entry_fraction: float = 1.0
    min_market_volatility: float = 0.05
    max_opposite_ask_for_entry: float = 0.95
    max_entry_exit_cost: Optional[float] = None
    min_opposite_ask_for_entry: float = 0.0
    min_book_depth_usdc: float = 0.0
    min_spread_basis_points: int = 0
    max_entry_side_spread: float = 0.03
    book_depth_levels: int = 5
    min_safe_price: float = 0.05

    # Entry timing: only allow entries in the last N seconds of the current window.
    entry_active_last_s: int = 0
    market_timezone: str = "America/New_York"
    market_window_minutes: int = 15

    # Scale-in (limited averaging)
    scalein_enabled: bool = False
    scalein_max_adds: int = 2
    scalein_add_usdc: float = 20.0
    scalein_trigger_drop_abs: float = 0.01
    scalein_min_interval_s: float = 1.0

    # Churn (take-profit by SELL instead of merge)
    churn_enabled: bool = False
    churn_take_profit_abs: float = 0.01

    # Burst execution (order-splitting)
    use_burst_execution: bool = False
    burst_chunk_unit: str = "shares"  # shares | usdc
    burst_min_chunk_shares: float = 20.0
    burst_max_chunk_shares: float = 50.0
    burst_min_chunk_usdc: float = 20.0
    burst_max_chunk_usdc: float = 50.0
    burst_max_orders: int = 25
    burst_min_delay_ms: int = 0
    burst_max_delay_ms: int = 50
    burst_order_type: str = "FOK"  # FOK | FAK | GTC | GTD
    burst_use_batch_endpoint: bool = False
    burst_batch_size: int = 10
    burst_max_price_slippage: float = 0.0  # absolute (e.g. 0.01 allows 1c above reference)
    burst_stop_on_price_change: bool = False
    burst_price_change_tolerance: float = 0.0
    burst_use_best_ask_size: bool = True
    burst_log_slices_in_paper: bool = False
    burst_simulate_fok_by_depth: bool = False
    burst_consume_book_in_paper: bool = True
    burst_scalein_on_price_drop: bool = False
    burst_scalein_multiplier: float = 1.5

    # Synthetic arbitrage (buy YES+NO when sum is cheap)
    arb_enabled: bool = False
    arb_max_entry_exit_cost: float = 0.99
    arb_budget_usdc: float = 100.0

    # Vacuum bids (deep value passive liquidity)
    vacuum_enabled: bool = False
    vacuum_prices: Optional[List[float]] = None
    vacuum_usdc_per_order: float = 10.0
    vacuum_order_type: str = "GTC"  # GTC | GTD | FOK | FAK

    # Paper Trading
    paper_trading: bool = True
    paper_trades_log: str = "paper_trades.csv"
    paper_initial_balance: float = 100.0

    # Risk Management
    max_concurrent_positions: int = 5
    max_daily_loss: float = 500.0
    max_entries_per_condition: int = 1
    reentry_cooldown_s: float = 60.0
    entry_signal_min_interval_ms: int = 250

    # Monitoring
    polling_interval_ms: int = 250

    # Entry signal (optional EMA crash filter)
    use_ema_crash_filter: bool = False
    ema_alpha: float = 0.2
    ema_crash_ratio: float = 0.9
    ema_min_samples: int = 10

    # Markets to watch
    # Provide either MARKETS as comma-separated condition IDs with a
    # companion MARKET_SPECS JSON, or only MARKET_SPECS.
    markets: Optional[List[str]] = None
    market_specs: Optional[List[Dict[str, str]]] = None
    market_specs_file: Optional[str] = None
    market_refresh_interval_s: int = 0

    # Logging
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env_file: str = ".env") -> "Config":
        if load_dotenv is not None:
            load_dotenv(env_file, override=False)

        markets_raw = _get_env("MARKETS")
        markets = (
            [m.strip() for m in markets_raw.split(",") if m.strip()]
            if markets_raw
            else None
        )

        market_specs = None
        specs_raw = _get_env("MARKET_SPECS")
        if specs_raw:
            try:
                market_specs = json.loads(specs_raw)
            except json.JSONDecodeError:
                market_specs = None

        return cls(
            host=_get_env("POLYMARKET_HOST", cls.host),
            chain_id=_get_int("CHAIN_ID", cls.chain_id),
            private_key=_normalize_private_key(_get_env("PRIVATE_KEY")),
            funder=_get_env("FUNDER"),
            api_key=_get_env("POLYMARKET_API_KEY"),
            api_secret=_get_env("POLYMARKET_API_SECRET"),
            api_passphrase=_get_env("POLYMARKET_PASSPHRASE"),
            ws_url=_get_env("POLYMARKET_WS_URL", cls.ws_url),
            ws_cookies=_get_json("POLYMARKET_WS_COOKIES"),
            ws_headers=_get_json("POLYMARKET_WS_HEADERS"),
            ws_subscribe_message=_get_json_any("POLYMARKET_WS_SUBSCRIBE_MESSAGE"),
            use_websocket=_get_bool("USE_WEBSOCKET", cls.use_websocket),
            ws_heartbeat_interval_s=_get_int(
                "POLYMARKET_WS_HEARTBEAT_INTERVAL_S", cls.ws_heartbeat_interval_s
            ),
            entry_threshold=_get_float("ENTRY_THRESHOLD", cls.entry_threshold),
            target_profit_threshold=_get_float(
                "TARGET_PROFIT_THRESHOLD", cls.target_profit_threshold
            ),
            stop_loss_threshold=_get_float(
                "STOP_LOSS_THRESHOLD", cls.stop_loss_threshold
            ),
            max_position_size=_get_float("MAX_POSITION_SIZE", cls.max_position_size),
            initial_entry_fraction=_get_float(
                "INITIAL_ENTRY_FRACTION", cls.initial_entry_fraction
            ),
            min_market_volatility=_get_float(
                "MIN_MARKET_VOLATILITY", cls.min_market_volatility
            ),
            max_opposite_ask_for_entry=_get_float(
                "MAX_OPPOSITE_ASK_FOR_ENTRY",
                _get_float("NO_ENTRY_IF_EITHER_ASK_ABOVE", cls.max_opposite_ask_for_entry),
            ),
            max_entry_exit_cost=(
                _get_float("MAX_ENTRY_EXIT_COST", 0.0)
                if _get_env("MAX_ENTRY_EXIT_COST") is not None
                else cls.max_entry_exit_cost
            ),
            min_opposite_ask_for_entry=_get_float(
                "MIN_OPPOSITE_ASK", cls.min_opposite_ask_for_entry
            ),
            min_book_depth_usdc=_get_float("MIN_BOOK_DEPTH_USDC", cls.min_book_depth_usdc),
            min_spread_basis_points=_get_int(
                "MIN_SPREAD_BASIS_POINTS", cls.min_spread_basis_points
            ),
            max_entry_side_spread=_get_float(
                "MAX_ENTRY_SIDE_SPREAD", cls.max_entry_side_spread
            ),
            book_depth_levels=_get_int("BOOK_DEPTH_LEVELS", cls.book_depth_levels),
            entry_active_last_s=_get_int(
                "ENTRY_ACTIVE_LAST_S", cls.entry_active_last_s
            ),
            market_timezone=_get_env("MARKET_TIMEZONE", cls.market_timezone) or cls.market_timezone,
            market_window_minutes=_get_int(
                "MARKET_WINDOW_MINUTES", cls.market_window_minutes
            ),
            scalein_enabled=_get_bool("SCALEIN_ENABLED", cls.scalein_enabled),
            scalein_max_adds=_get_int("SCALEIN_MAX_ADDS", cls.scalein_max_adds),
            scalein_add_usdc=_get_float("SCALEIN_ADD_USDC", cls.scalein_add_usdc),
            scalein_trigger_drop_abs=_get_float(
                "SCALEIN_TRIGGER_DROP_ABS", cls.scalein_trigger_drop_abs
            ),
            scalein_min_interval_s=_get_float(
                "SCALEIN_MIN_INTERVAL_S", cls.scalein_min_interval_s
            ),
            churn_enabled=_get_bool("CHURN_ENABLED", cls.churn_enabled),
            churn_take_profit_abs=_get_float(
                "CHURN_TAKE_PROFIT_ABS", cls.churn_take_profit_abs
            ),
            use_burst_execution=_get_bool("USE_BURST_EXECUTION", cls.use_burst_execution),
            burst_chunk_unit=_get_env("BURST_CHUNK_UNIT", cls.burst_chunk_unit)
            or cls.burst_chunk_unit,
            burst_min_chunk_shares=_get_float("BURST_MIN_CHUNK_SHARES", cls.burst_min_chunk_shares),
            burst_max_chunk_shares=_get_float("BURST_MAX_CHUNK_SHARES", cls.burst_max_chunk_shares),
            burst_min_chunk_usdc=_get_float("BURST_MIN_CHUNK_USDC", cls.burst_min_chunk_usdc),
            burst_max_chunk_usdc=_get_float("BURST_MAX_CHUNK_USDC", cls.burst_max_chunk_usdc),
            burst_max_orders=_get_int("BURST_MAX_ORDERS", cls.burst_max_orders),
            burst_min_delay_ms=_get_int("BURST_MIN_DELAY_MS", cls.burst_min_delay_ms),
            burst_max_delay_ms=_get_int("BURST_MAX_DELAY_MS", cls.burst_max_delay_ms),
            burst_order_type=_get_env("BURST_ORDER_TYPE", cls.burst_order_type) or cls.burst_order_type,
            burst_use_batch_endpoint=_get_bool(
                "BURST_USE_BATCH_ENDPOINT", cls.burst_use_batch_endpoint
            ),
            burst_batch_size=_get_int("BURST_BATCH_SIZE", cls.burst_batch_size),
            burst_max_price_slippage=_get_float(
                "BURST_MAX_PRICE_SLIPPAGE", cls.burst_max_price_slippage
            ),
            burst_stop_on_price_change=_get_bool(
                "BURST_STOP_ON_PRICE_CHANGE", cls.burst_stop_on_price_change
            ),
            burst_price_change_tolerance=_get_float(
                "BURST_PRICE_CHANGE_TOLERANCE", cls.burst_price_change_tolerance
            ),
            burst_use_best_ask_size=_get_bool(
                "BURST_USE_BEST_ASK_SIZE", cls.burst_use_best_ask_size
            ),
            burst_log_slices_in_paper=_get_bool(
                "BURST_LOG_SLICES_IN_PAPER", cls.burst_log_slices_in_paper
            ),
            burst_simulate_fok_by_depth=_get_bool(
                "BURST_SIMULATE_FOK_BY_DEPTH", cls.burst_simulate_fok_by_depth
            ),
            burst_consume_book_in_paper=_get_bool(
                "BURST_CONSUME_BOOK_IN_PAPER", cls.burst_consume_book_in_paper
            ),
            burst_scalein_on_price_drop=_get_bool(
                "BURST_SCALEIN_ON_PRICE_DROP", cls.burst_scalein_on_price_drop
            ),
            burst_scalein_multiplier=_get_float(
                "BURST_SCALEIN_MULTIPLIER", cls.burst_scalein_multiplier
            ),
            arb_enabled=_get_bool("ARB_ENABLED", cls.arb_enabled),
            arb_max_entry_exit_cost=_get_float(
                "ARB_MAX_ENTRY_EXIT_COST", cls.arb_max_entry_exit_cost
            ),
            arb_budget_usdc=_get_float("ARB_BUDGET_USDC", cls.arb_budget_usdc),
            vacuum_enabled=_get_bool("VACUUM_ENABLED", cls.vacuum_enabled),
            vacuum_prices=_get_float_list("VACUUM_PRICES"),
            vacuum_usdc_per_order=_get_float(
                "VACUUM_USDC_PER_ORDER", cls.vacuum_usdc_per_order
            ),
            vacuum_order_type=_get_env("VACUUM_ORDER_TYPE", cls.vacuum_order_type)
            or cls.vacuum_order_type,
            paper_trading=_get_bool("PAPER_TRADING", cls.paper_trading),
            paper_trades_log=_get_env("PAPER_TRADES_LOG", cls.paper_trades_log),
            paper_initial_balance=_get_float(
                "PAPER_INITIAL_BALANCE", cls.paper_initial_balance
            ),
            max_concurrent_positions=_get_int(
                "MAX_CONCURRENT_POSITIONS", cls.max_concurrent_positions
            ),
            max_daily_loss=_get_float("MAX_DAILY_LOSS", cls.max_daily_loss),
            max_entries_per_condition=_get_int(
                "MAX_ENTRIES_PER_CONDITION", cls.max_entries_per_condition
            ),
            min_safe_price=_get_float("MIN_SAFE_PRICE", cls.min_safe_price),
            reentry_cooldown_s=_get_float(
                "REENTRY_COOLDOWN_S",
                _get_float("PER_CONDITION_LOCK_SECONDS", cls.reentry_cooldown_s),
            ),
            entry_signal_min_interval_ms=_get_int(
                "ENTRY_SIGNAL_MIN_INTERVAL_MS",
                _get_int("SIGNAL_COOLDOWN_MS", cls.entry_signal_min_interval_ms),
            ),
            polling_interval_ms=_get_int(
                "POLLING_INTERVAL_MS", cls.polling_interval_ms
            ),
            use_ema_crash_filter=_get_bool(
                "USE_EMA_CRASH_FILTER", cls.use_ema_crash_filter
            ),
            ema_alpha=_get_float("EMA_ALPHA", cls.ema_alpha),
            ema_crash_ratio=_get_float("EMA_CRASH_RATIO", cls.ema_crash_ratio),
            ema_min_samples=_get_int("EMA_MIN_SAMPLES", cls.ema_min_samples),
            markets=markets,
            market_specs=market_specs,
            market_specs_file=_get_env("MARKET_SPECS_FILE"),
            market_refresh_interval_s=_get_int("MARKET_REFRESH_INTERVAL_S", cls.market_refresh_interval_s),
            log_level=_get_env("LOG_LEVEL", cls.log_level),
        )

    def validate(self) -> None:
        if not self.paper_trading:
            missing = [
                name
                for name in ("private_key", "funder", "api_key", "api_secret", "api_passphrase")
                if getattr(self, name) in (None, "")
            ]
            if missing:
                raise ValueError(f"Missing required config fields: {missing}")

            if self.private_key:
                pk = str(self.private_key).strip()
                if pk.startswith("0x"):
                    pk = pk[2:]
                is_hex = all(c in "0123456789abcdefABCDEF" for c in pk)
                if not is_hex or len(pk) != 64:
                    raise ValueError(
                        "Invalid PRIVATE_KEY: expected 32-byte hex string (64 hex chars, optional 0x prefix)"
                    )

        if not (0.0 < float(self.initial_entry_fraction) <= 1.0):
            raise ValueError("INITIAL_ENTRY_FRACTION must be within (0, 1]")

        if self.paper_trading and float(self.paper_initial_balance) < 0:
            raise ValueError("PAPER_INITIAL_BALANCE must be >= 0")

        if int(self.entry_active_last_s) < 0:
            raise ValueError("ENTRY_ACTIVE_LAST_S must be >= 0")
        if int(self.market_window_minutes) <= 0 or 60 % int(self.market_window_minutes) != 0:
            raise ValueError("MARKET_WINDOW_MINUTES must be a divisor of 60 (e.g. 15)")

        if self.scalein_enabled:
            if int(self.scalein_max_adds) <= 0:
                raise ValueError("SCALEIN_MAX_ADDS must be > 0")
            if float(self.scalein_add_usdc) <= 0:
                raise ValueError("SCALEIN_ADD_USDC must be > 0")
            if float(self.scalein_trigger_drop_abs) <= 0:
                raise ValueError("SCALEIN_TRIGGER_DROP_ABS must be > 0")
            if float(self.scalein_min_interval_s) < 0:
                raise ValueError("SCALEIN_MIN_INTERVAL_S must be >= 0")

        if self.churn_enabled:
            if float(self.churn_take_profit_abs) <= 0:
                raise ValueError("CHURN_TAKE_PROFIT_ABS must be > 0")

        if self.use_burst_execution:
            if self.burst_min_chunk_shares <= 0:
                raise ValueError("BURST_MIN_CHUNK_SHARES must be > 0")
            if self.burst_max_chunk_shares < self.burst_min_chunk_shares:
                raise ValueError("BURST_MAX_CHUNK_SHARES must be >= BURST_MIN_CHUNK_SHARES")
            if self.burst_min_chunk_usdc <= 0:
                raise ValueError("BURST_MIN_CHUNK_USDC must be > 0")
            if self.burst_max_chunk_usdc < self.burst_min_chunk_usdc:
                raise ValueError("BURST_MAX_CHUNK_USDC must be >= BURST_MIN_CHUNK_USDC")
            if self.burst_max_orders <= 0:
                raise ValueError("BURST_MAX_ORDERS must be > 0")
            if self.burst_min_delay_ms < 0 or self.burst_max_delay_ms < 0:
                raise ValueError("BURST_MIN_DELAY_MS/BURST_MAX_DELAY_MS must be >= 0")
            if self.burst_max_delay_ms < self.burst_min_delay_ms:
                raise ValueError("BURST_MAX_DELAY_MS must be >= BURST_MIN_DELAY_MS")
            if self.burst_batch_size <= 0:
                raise ValueError("BURST_BATCH_SIZE must be > 0")
            if self.burst_max_price_slippage < 0:
                raise ValueError("BURST_MAX_PRICE_SLIPPAGE must be >= 0")
            if self.burst_price_change_tolerance < 0:
                raise ValueError("BURST_PRICE_CHANGE_TOLERANCE must be >= 0")
            if self.burst_scalein_on_price_drop and self.burst_scalein_multiplier < 1.0:
                raise ValueError("BURST_SCALEIN_MULTIPLIER must be >= 1.0")

            if self.burst_chunk_unit.strip().lower() not in {"shares", "usdc"}:
                raise ValueError("BURST_CHUNK_UNIT must be 'shares' or 'usdc'")

        if self.arb_enabled:
            if self.arb_budget_usdc <= 0:
                raise ValueError("ARB_BUDGET_USDC must be > 0")
            if not (0.0 < self.arb_max_entry_exit_cost <= 2.0):
                raise ValueError("ARB_MAX_ENTRY_EXIT_COST must be within (0, 2]")

        if self.vacuum_enabled:
            if not self.vacuum_prices:
                raise ValueError("VACUUM_PRICES must be set when VACUUM_ENABLED=true")
            if self.vacuum_usdc_per_order <= 0:
                raise ValueError("VACUUM_USDC_PER_ORDER must be > 0")
            if any(p <= 0 or p >= 1 for p in self.vacuum_prices):
                raise ValueError("VACUUM_PRICES entries must be within (0, 1)")
            if self.vacuum_order_type.strip().upper() not in {"FOK", "FAK", "GTC", "GTD"}:
                raise ValueError("VACUUM_ORDER_TYPE must be one of FOK|FAK|GTC|GTD")
