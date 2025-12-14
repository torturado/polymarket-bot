use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Deserialize)]
pub struct MarketSpec {
    pub condition_id: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub event_slug: Option<String>,
    #[serde(default)]
    pub start_epoch_utc: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct PriceData {
    pub best_bid: f64,
    pub best_ask: f64,
    pub best_bid_size: f64,
    pub best_ask_size: f64,
    pub ask_depth_usdc: f64,
    pub timestamp: f64,
}

#[derive(Debug, Clone)]
pub struct MarketUpdate {
    pub condition_id: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub yes: PriceData,
    pub no: PriceData,
    pub received_at: f64,
}

pub fn now_s() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}
