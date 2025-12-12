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
    min_market_volatility: float = 0.05
    max_opposite_ask_for_entry: float = 0.95
    max_entry_exit_cost: Optional[float] = None

    # Paper Trading
    paper_trading: bool = True
    paper_trades_log: str = "paper_trades.csv"

    # Risk Management
    max_concurrent_positions: int = 5
    max_daily_loss: float = 500.0
    max_entries_per_condition: int = 1
    reentry_cooldown_s: float = 60.0
    entry_signal_min_interval_ms: int = 250

    # Monitoring
    polling_interval_ms: int = 250

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
            private_key=_get_env("PRIVATE_KEY"),
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
            min_market_volatility=_get_float(
                "MIN_MARKET_VOLATILITY", cls.min_market_volatility
            ),
            max_opposite_ask_for_entry=_get_float(
                "MAX_OPPOSITE_ASK_FOR_ENTRY", cls.max_opposite_ask_for_entry
            ),
            max_entry_exit_cost=(
                _get_float("MAX_ENTRY_EXIT_COST", 0.0)
                if _get_env("MAX_ENTRY_EXIT_COST") is not None
                else cls.max_entry_exit_cost
            ),
            paper_trading=_get_bool("PAPER_TRADING", cls.paper_trading),
            paper_trades_log=_get_env("PAPER_TRADES_LOG", cls.paper_trades_log),
            max_concurrent_positions=_get_int(
                "MAX_CONCURRENT_POSITIONS", cls.max_concurrent_positions
            ),
            max_daily_loss=_get_float("MAX_DAILY_LOSS", cls.max_daily_loss),
            max_entries_per_condition=_get_int(
                "MAX_ENTRIES_PER_CONDITION", cls.max_entries_per_condition
            ),
            reentry_cooldown_s=_get_float("REENTRY_COOLDOWN_S", cls.reentry_cooldown_s),
            entry_signal_min_interval_ms=_get_int(
                "ENTRY_SIGNAL_MIN_INTERVAL_MS", cls.entry_signal_min_interval_ms
            ),
            polling_interval_ms=_get_int(
                "POLLING_INTERVAL_MS", cls.polling_interval_ms
            ),
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
