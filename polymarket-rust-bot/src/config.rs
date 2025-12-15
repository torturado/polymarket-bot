use anyhow::{Context, Result};
use log::warn;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Config {
    pub ws_url: String,
    pub ws_headers: HashMap<String, String>,
    pub ws_cookies: HashMap<String, String>,
    pub ws_subscribe_message: Option<Value>,
    pub market_specs_path: PathBuf,
    pub market_refresh_interval_s: u64,
    pub market_window_minutes: u64,
    pub entry_min_time_remaining_s: f64,

    pub paper_trading: bool,
    pub paper_trades_log: PathBuf,
    pub paper_initial_balance: f64,

    pub arb_enabled: bool,
    pub entry_threshold: f64,
    pub leg1_max_wait_s: f64,
    pub arb_max_entry_exit_cost: f64,
    pub arb_budget_usdc: f64,
    pub entry_signal_min_interval_ms: u64,

    pub max_concurrent_positions: usize,
    pub max_daily_loss: f64,
    pub max_entries_per_condition: u32,
    pub reentry_cooldown_s: f64,

    pub min_book_depth_usdc: f64,
    pub book_depth_levels: usize,

    pub use_burst_execution: bool,
    pub burst_chunk_unit: String, // shares | usdc
    pub burst_min_chunk_shares: f64,
    pub burst_max_chunk_shares: f64,
    pub burst_min_chunk_usdc: f64,
    pub burst_max_chunk_usdc: f64,
    pub burst_max_orders: u32,
    pub burst_min_delay_ms: u64,
    pub burst_max_delay_ms: u64,
    pub burst_order_type: String, // FOK | FAK | GTC | GTD
    pub burst_stop_on_price_change: bool,
    pub burst_price_change_tolerance: f64,
    pub burst_use_best_ask_size: bool,
    pub burst_log_slices_in_paper: bool,
    pub burst_simulate_fok_by_depth: bool,
    pub burst_consume_book_in_paper: bool,

    // Underlying oracle (external price feed)
    pub oracle_enabled: bool,
    pub oracle_ws_url: String,
    pub oracle_symbols: Vec<String>,
    pub oracle_price_ttl_s: f64,

    // Fair value model (probability vs strike + time)
    pub fair_value_enabled: bool,
    pub edge_threshold: f64,
    pub fair_value_sigma_annual: f64,
    pub strike_capture_window_s: f64,
    pub max_leg_in_entry_cost: f64,

    // Per-position risk (unhedged leg1)
    pub leg1_stop_loss_pct: f64,
    pub force_unwind_time_remaining_s: f64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let ws_url = std::env::var("POLYMARKET_WS_URL")
            .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string());

        let ws_headers = parse_json_map_env("POLYMARKET_WS_HEADERS")?;
        let ws_cookies = parse_json_map_env("POLYMARKET_WS_COOKIES")?;
        let ws_subscribe_message = parse_json_value_env("POLYMARKET_WS_SUBSCRIBE_MESSAGE")?;

        let market_specs_path =
            resolve_market_specs_path(std::env::var("MARKET_SPECS_FILE").ok().as_deref());
        let market_refresh_interval_s = parse_u64_env("MARKET_REFRESH_INTERVAL_S", 0)?;
        let market_window_minutes = parse_u64_env("MARKET_WINDOW_MINUTES", 0)?;
        let entry_min_time_remaining_s = parse_f64_env("ENTRY_MIN_TIME_REMAINING_S", 300.0)?;

        let paper_trading = parse_bool_env("PAPER_TRADING", true);
        let paper_trades_log = PathBuf::from(parse_string_env(
            "PAPER_TRADES_LOG",
            "paper_trades_rust.csv",
        ));
        let paper_initial_balance = parse_f64_env("PAPER_INITIAL_BALANCE", 100.0)?;

        let arb_enabled = parse_bool_env("ARB_ENABLED", false);
        let entry_threshold = parse_f64_env("ENTRY_THRESHOLD", 0.15)?;
        let leg1_max_wait_s = parse_f64_env("LEG1_MAX_WAIT_S", 30.0)?;
        let arb_max_entry_exit_cost = parse_f64_env("ARB_MAX_ENTRY_EXIT_COST", 0.99)?;
        let arb_budget_usdc = parse_f64_env("ARB_BUDGET_USDC", 100.0)?;
        let entry_signal_min_interval_ms = parse_u64_env("ENTRY_SIGNAL_MIN_INTERVAL_MS", 250)?;

        let max_concurrent_positions = parse_usize_env("MAX_CONCURRENT_POSITIONS", 5)
            .context("Invalid MAX_CONCURRENT_POSITIONS")?;
        let max_daily_loss = parse_f64_env("MAX_DAILY_LOSS", 500.0)?;
        let max_entries_per_condition = parse_u32_env("MAX_ENTRIES_PER_CONDITION", 1)?;
        let reentry_cooldown_s = parse_f64_env("REENTRY_COOLDOWN_S", 60.0)?;

        let min_book_depth_usdc = parse_f64_env("MIN_BOOK_DEPTH_USDC", 0.0)?;
        let book_depth_levels = parse_usize_env("BOOK_DEPTH_LEVELS", 5)?;

        let use_burst_execution = parse_bool_env("USE_BURST_EXECUTION", false);
        let burst_chunk_unit = parse_string_env("BURST_CHUNK_UNIT", "shares");
        let burst_min_chunk_shares = parse_f64_env("BURST_MIN_CHUNK_SHARES", 20.0)?;
        let burst_max_chunk_shares = parse_f64_env("BURST_MAX_CHUNK_SHARES", 50.0)?;
        let burst_min_chunk_usdc = parse_f64_env("BURST_MIN_CHUNK_USDC", 20.0)?;
        let burst_max_chunk_usdc = parse_f64_env("BURST_MAX_CHUNK_USDC", 50.0)?;
        let burst_max_orders = parse_u32_env("BURST_MAX_ORDERS", 25)?;
        let burst_min_delay_ms = parse_u64_env("BURST_MIN_DELAY_MS", 0)?;
        let burst_max_delay_ms = parse_u64_env("BURST_MAX_DELAY_MS", 50)?;
        let burst_order_type = parse_string_env("BURST_ORDER_TYPE", "FOK");
        let burst_stop_on_price_change = parse_bool_env("BURST_STOP_ON_PRICE_CHANGE", false);
        let burst_price_change_tolerance = parse_f64_env("BURST_PRICE_CHANGE_TOLERANCE", 0.0)?;
        let burst_use_best_ask_size = parse_bool_env("BURST_USE_BEST_ASK_SIZE", true);
        let burst_log_slices_in_paper = parse_bool_env("BURST_LOG_SLICES_IN_PAPER", false);
        let burst_simulate_fok_by_depth = parse_bool_env("BURST_SIMULATE_FOK_BY_DEPTH", false);
        let burst_consume_book_in_paper = parse_bool_env("BURST_CONSUME_BOOK_IN_PAPER", true);

        let oracle_enabled = parse_bool_env("ORACLE_ENABLED", false);
        let oracle_ws_url = parse_string_env("ORACLE_WS_URL", "");
        let oracle_symbols = parse_string_list_env(
            "ORACLE_SYMBOLS",
            vec!["BTC".to_string(), "ETH".to_string(), "SOL".to_string(), "XRP".to_string()],
        )?;
        let oracle_price_ttl_s = parse_f64_env("ORACLE_PRICE_TTL_S", 10.0)?;

        let fair_value_enabled = parse_bool_env("FAIR_VALUE_ENABLED", true);
        let edge_threshold = parse_f64_env("EDGE_THRESHOLD", 0.02)?;
        let fair_value_sigma_annual = parse_f64_env("FAIR_VALUE_SIGMA_ANNUAL", 0.8)?;
        let strike_capture_window_s = parse_f64_env("STRIKE_CAPTURE_WINDOW_S", 10.0)?;
        let max_leg_in_entry_cost = parse_f64_env("MAX_LEG_IN_ENTRY_COST", 1.02)?;

        let leg1_stop_loss_pct = parse_f64_env("LEG1_STOP_LOSS_PCT", 0.0)?;
        let force_unwind_time_remaining_s =
            parse_f64_env("FORCE_UNWIND_TIME_REMAINING_S", 60.0)?;

        Ok(Self {
            ws_url,
            ws_headers,
            ws_cookies,
            ws_subscribe_message,
            market_specs_path,
            market_refresh_interval_s,
            market_window_minutes,
            entry_min_time_remaining_s,
            paper_trading,
            paper_trades_log,
            paper_initial_balance,
            arb_enabled,
            entry_threshold,
            leg1_max_wait_s,
            arb_max_entry_exit_cost,
            arb_budget_usdc,
            entry_signal_min_interval_ms,
            max_concurrent_positions,
            max_daily_loss,
            max_entries_per_condition,
            reentry_cooldown_s,
            min_book_depth_usdc,
            book_depth_levels,
            use_burst_execution,
            burst_chunk_unit,
            burst_min_chunk_shares,
            burst_max_chunk_shares,
            burst_min_chunk_usdc,
            burst_max_chunk_usdc,
            burst_max_orders,
            burst_min_delay_ms,
            burst_max_delay_ms,
            burst_order_type,
            burst_stop_on_price_change,
            burst_price_change_tolerance,
            burst_use_best_ask_size,
            burst_log_slices_in_paper,
            burst_simulate_fok_by_depth,
            burst_consume_book_in_paper,
            oracle_enabled,
            oracle_ws_url,
            oracle_symbols,
            oracle_price_ttl_s,
            fair_value_enabled,
            edge_threshold,
            fair_value_sigma_annual,
            strike_capture_window_s,
            max_leg_in_entry_cost,
            leg1_stop_loss_pct,
            force_unwind_time_remaining_s,
        })
    }
}

fn parse_bool_env(name: &str, default: bool) -> bool {
    let Ok(raw) = std::env::var(name) else {
        return default;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => default,
        "1" | "true" | "yes" | "y" | "on" => true,
        "0" | "false" | "no" | "n" | "off" => false,
        _ => default,
    }
}

fn parse_u64_env(name: &str, default: u64) -> Result<u64> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(default);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(default);
    }
    raw.parse::<u64>()
        .with_context(|| format!("Invalid {name}: expected u64"))
}

fn parse_u32_env(name: &str, default: u32) -> Result<u32> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(default);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(default);
    }
    raw.parse::<u32>()
        .with_context(|| format!("Invalid {name}: expected u32"))
}

fn parse_usize_env(name: &str, default: usize) -> Result<usize> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(default);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(default);
    }
    raw.parse::<usize>()
        .with_context(|| format!("Invalid {name}: expected usize"))
}

fn parse_f64_env(name: &str, default: f64) -> Result<f64> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(default);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(default);
    }
    raw.parse::<f64>()
        .with_context(|| format!("Invalid {name}: expected f64"))
}

fn parse_string_env(name: &str, default: &str) -> String {
    let Ok(raw) = std::env::var(name) else {
        return default.to_string();
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return default.to_string();
    }
    raw.to_string()
}

fn parse_json_map_env(name: &str) -> Result<HashMap<String, String>> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(HashMap::new());
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(HashMap::new());
    }
    match serde_json::from_str::<Value>(raw) {
        Ok(v) => {
            let Some(obj) = v.as_object() else {
                warn!("{name} is not a JSON object; ignoring.");
                return Ok(HashMap::new());
            };
            let mut out = HashMap::with_capacity(obj.len());
            for (k, v) in obj {
                if let Some(s) = v.as_str() {
                    out.insert(k.clone(), s.to_string());
                } else if v.is_number() || v.is_boolean() {
                    out.insert(k.clone(), v.to_string());
                }
            }
            Ok(out)
        }
        Err(e) => {
            let map = parse_map_like_value(raw);
            if map.is_empty() {
                warn!("{name} is not valid JSON ({e}); ignoring.");
            } else {
                warn!(
                    "{name} is not valid JSON ({e}); parsed it as key/value pairs. Tip: wrap JSON in single quotes in .env."
                );
            }
            Ok(map)
        }
    }
}

fn parse_json_value_env(name: &str) -> Result<Option<Value>> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(None);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    match serde_json::from_str::<Value>(raw) {
        Ok(v) => Ok(Some(v)),
        Err(e) => {
            warn!("{name} is not valid JSON ({e}); ignoring.");
            Ok(None)
        }
    }
}

fn parse_string_list_env(name: &str, default: Vec<String>) -> Result<Vec<String>> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(default);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(default);
    }

    if raw.starts_with('[') {
        if let Ok(v) = serde_json::from_str::<Value>(raw) {
            if let Some(arr) = v.as_array() {
                let mut out = Vec::with_capacity(arr.len());
                for item in arr {
                    if let Some(s) = item.as_str() {
                        let sym = s.trim().to_ascii_uppercase();
                        if !sym.is_empty() {
                            out.push(sym);
                        }
                    } else if item.is_number() || item.is_boolean() {
                        let sym = item.to_string().trim().to_ascii_uppercase();
                        if !sym.is_empty() {
                            out.push(sym);
                        }
                    }
                }
                if !out.is_empty() {
                    return Ok(out);
                }
            }
        }
    }

    let mut out = Vec::new();
    for part in raw.split(',') {
        let sym = part.trim().to_ascii_uppercase();
        if sym.is_empty() {
            continue;
        }
        out.push(sym);
    }
    Ok(if out.is_empty() { default } else { out })
}

fn parse_map_like_value(raw: &str) -> HashMap<String, String> {
    let inner = raw
        .trim()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(raw)
        .trim();
    if inner.is_empty() {
        return HashMap::new();
    }

    let mut out = HashMap::new();
    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let (k, v) = if let Some((k, v)) = part.split_once('=') {
            (k, v)
        } else if let Some((k, v)) = part.split_once(':') {
            (k, v)
        } else {
            continue;
        };

        let k = strip_wrapping_quotes(k);
        let v = strip_wrapping_quotes(v);
        if k.is_empty() || v.is_empty() {
            continue;
        }
        out.insert(k, v);
    }
    out
}

fn strip_wrapping_quotes(value: &str) -> String {
    let v = value.trim();
    if v.len() >= 2 && v.as_bytes()[0] == v.as_bytes()[v.len() - 1] {
        let quote = v.as_bytes()[0];
        if quote == b'"' || quote == b'\'' {
            return v[1..v.len() - 1].trim().to_string();
        }
    }
    v.to_string()
}

fn resolve_market_specs_path(raw: Option<&str>) -> PathBuf {
    let raw = raw
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("market_specs.json");
    let p = PathBuf::from(raw);
    if p.exists() {
        return p;
    }
    if !p.is_absolute() {
        let alt = Path::new("..").join(&p);
        if alt.exists() {
            return alt;
        }
    }
    p
}
