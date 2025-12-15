use crate::config::Config;
use crate::types::now_s;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use log::{debug, info, warn};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone, Copy)]
pub struct OraclePrice {
    pub price: f64,
    pub ts: f64,
}

pub type OracleStore = Arc<RwLock<HashMap<String, OraclePrice>>>;

pub fn new_store() -> OracleStore {
    Arc::new(RwLock::new(HashMap::new()))
}

pub fn get_price_sync(prices: &HashMap<String, OraclePrice>, symbol: &str, ttl_s: f64) -> Option<f64> {
    let sym = symbol.trim().to_ascii_uppercase();
    let p = prices.get(&sym)?;
    if ttl_s > 0.0 && (now_s() - p.ts) > ttl_s {
        return None;
    }
    if p.price.is_finite() && p.price > 0.0 {
        Some(p.price)
    } else {
        None
    }
}

pub async fn get_price(store: &OracleStore, symbol: &str, ttl_s: f64) -> Option<f64> {
    let prices = store.read().await;
    get_price_sync(&prices, symbol, ttl_s)
}

pub fn build_binance_spot_combined_trade_url(symbols: &[String]) -> Option<String> {
    let mut streams: Vec<String> = Vec::new();
    for raw in symbols {
        let Some(pair) = normalize_binance_pair(raw) else {
            continue;
        };
        streams.push(format!("{}@trade", pair.to_ascii_lowercase()));
    }
    streams.sort();
    streams.dedup();
    if streams.is_empty() {
        return None;
    }
    Some(format!(
        "wss://stream.binance.com:9443/stream?streams={}",
        streams.join("/")
    ))
}

pub async fn run_oracle_loop(config: Arc<Config>, store: OracleStore) -> Result<()> {
    if !config.oracle_enabled {
        info!("Oracle disabled (ORACLE_ENABLED=false)");
        return Ok(());
    }

    let url = if !config.oracle_ws_url.trim().is_empty() {
        config.oracle_ws_url.trim().to_string()
    } else {
        build_binance_spot_combined_trade_url(&config.oracle_symbols).unwrap_or_else(|| {
            "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade".to_string()
        })
    };

    let mut retry_s: u64 = 1;
    loop {
        match run_oracle_session(&url, Arc::clone(&store)).await {
            Ok(()) => {
                retry_s = 1;
            }
            Err(e) => {
                warn!("Oracle session error: {e:#}. Retrying in {retry_s}s");
                tokio::time::sleep(Duration::from_secs(retry_s)).await;
                retry_s = (retry_s.saturating_mul(2)).min(30);
            }
        }
    }
}

async fn run_oracle_session(url: &str, store: OracleStore) -> Result<()> {
    info!("Oracle connecting: url={}", url);
    let (ws, _resp) = connect_async(url)
        .await
        .with_context(|| format!("Oracle WS connect failed: {url}"))?;
    let (_write, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        let msg = msg.context("Oracle WS read error")?;
        match msg {
            Message::Text(raw) => ingest_oracle_message(&raw, &store).await,
            Message::Binary(bytes) => {
                if let Ok(raw) = String::from_utf8(bytes) {
                    ingest_oracle_message(&raw, &store).await;
                }
            }
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(frame) => {
                debug!("Oracle WS close: {frame:?}");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

async fn ingest_oracle_message(raw: &str, store: &OracleStore) {
    let Ok(v) = serde_json::from_str::<Value>(raw) else {
        return;
    };

    // Combined stream: {"stream":"btcusdt@trade","data":{...}}
    let (data, stream_name) = match v.get("data") {
        Some(d) => (d, v.get("stream").and_then(|s| s.as_str())),
        None => (&v, v.get("stream").and_then(|s| s.as_str())),
    };

    let sym = data
        .get("s")
        .and_then(|s| s.as_str())
        .or_else(|| stream_name.and_then(|s| s.split('@').next()))
        .unwrap_or("")
        .trim();
    if sym.is_empty() {
        return;
    }

    let price_str = data
        .get("p")
        .and_then(|p| p.as_str())
        .or_else(|| data.get("c").and_then(|p| p.as_str()))
        .or_else(|| data.get("price").and_then(|p| p.as_str()))
        .unwrap_or("")
        .trim();
    if price_str.is_empty() {
        return;
    }

    let Ok(price) = price_str.parse::<f64>() else {
        return;
    };
    if !(price.is_finite() && price > 0.0) {
        return;
    }

    let base = normalize_asset_symbol(sym);
    if base.is_empty() {
        return;
    }

    let ts = now_s();
    let mut w = store.write().await;
    w.insert(base, OraclePrice { price, ts });
}

fn normalize_asset_symbol(raw: &str) -> String {
    let s = raw.trim().to_ascii_uppercase();
    if s.is_empty() {
        return s;
    }
    for suffix in ["USDT", "USD", "BUSD"] {
        if s.ends_with(suffix) && s.len() > suffix.len() {
            return s[..(s.len() - suffix.len())].to_string();
        }
    }
    s
}

fn normalize_binance_pair(raw: &str) -> Option<String> {
    let s = raw.trim().to_ascii_uppercase();
    if s.is_empty() {
        return None;
    }
    if s.ends_with("USDT") && s.len() > 4 {
        return Some(s);
    }
    if s.ends_with("USD") && s.len() > 3 {
        return Some(s);
    }
    // Default: map BTC -> BTCUSDT, etc.
    Some(format!("{s}USDT"))
}

