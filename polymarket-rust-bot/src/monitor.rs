use crate::config::Config;
use crate::market_specs;
use crate::position_manager::PositionManager;
use crate::types::{now_s, MarketSpec, MarketUpdate, PriceData};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

pub async fn run_ws_monitor(
    config: Arc<Config>,
    mut specs_rx: watch::Receiver<Vec<MarketSpec>>,
    tx_strategy: Sender<MarketUpdate>,
    position_manager: Arc<PositionManager>,
) -> Result<()> {
    let mut prices: HashMap<String, PriceData> = HashMap::new();

    loop {
        let specs = specs_rx.borrow().clone();
        let open_positions = position_manager.snapshot_positions();
        let (token_to_condition, condition_to_pair, unique_assets) =
            build_subscription(&specs, &open_positions);

        if unique_assets.is_empty() {
            warn!("No assets to subscribe (empty market specs)");
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        let open_conditions = open_positions.len();
        info!(
            "Rust monitor starting: conditions={} assets={} open_positions={} ws_url={}",
            condition_to_pair.len(),
            unique_assets.len(),
            open_conditions,
            config.ws_url
        );

        if let Err(e) = run_ws_session(
            &config,
            &unique_assets,
            &token_to_condition,
            &condition_to_pair,
            &mut prices,
            &tx_strategy,
            config.book_depth_levels,
            &mut specs_rx,
        )
        .await
        {
            warn!("WS session error: {e:#}. Retrying...");
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn run_market_specs_watcher(
    config: Arc<Config>,
    tx: watch::Sender<Vec<MarketSpec>>,
) -> Result<()> {
    let interval = config.market_refresh_interval_s;
    if interval == 0 {
        return Ok(());
    }

    let mut last_key = specs_key(&tx.borrow());

    loop {
        tokio::time::sleep(Duration::from_secs(interval)).await;
        let path = config.market_specs_path.clone();
        let loaded = tokio::task::spawn_blocking(move || market_specs::load_market_specs(&path))
            .await
            .context("market specs watcher join failed");

        let specs = match loaded {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                warn!("Failed to reload market specs: {e:#}");
                continue;
            }
            Err(e) => {
                warn!("Market specs watcher task error: {e:#}");
                continue;
            }
        };

        let key = specs_key(&specs);
        if key != last_key {
            last_key = key;
            info!(
                "Market specs changed: conditions={} (refresh_s={})",
                specs.len(),
                interval
            );
            if tx.send(specs).is_err() {
                return Ok(());
            }
        }
    }
}

fn build_subscription(
    specs: &[MarketSpec],
    open_positions: &[crate::position_manager::LegInPosition],
) -> (
    HashMap<String, String>,
    HashMap<String, (String, String)>,
    Vec<String>,
) {
    let mut token_to_condition: HashMap<String, String> = HashMap::new();
    let mut condition_to_pair: HashMap<String, (String, String)> = HashMap::new();
    let mut assets: Vec<String> = Vec::new();

    for m in specs {
        token_to_condition.insert(m.yes_token_id.clone(), m.condition_id.clone());
        token_to_condition.insert(m.no_token_id.clone(), m.condition_id.clone());
        condition_to_pair.insert(
            m.condition_id.clone(),
            (m.yes_token_id.clone(), m.no_token_id.clone()),
        );
        assets.push(m.yes_token_id.clone());
        assets.push(m.no_token_id.clone());
    }

    for p in open_positions {
        token_to_condition
            .entry(p.yes_token_id.clone())
            .or_insert_with(|| p.condition_id.clone());
        token_to_condition
            .entry(p.no_token_id.clone())
            .or_insert_with(|| p.condition_id.clone());
        condition_to_pair
            .entry(p.condition_id.clone())
            .or_insert_with(|| (p.yes_token_id.clone(), p.no_token_id.clone()));
        assets.push(p.yes_token_id.clone());
        assets.push(p.no_token_id.clone());
    }

    assets.sort();
    assets.dedup();

    (token_to_condition, condition_to_pair, assets)
}

fn specs_key(specs: &[MarketSpec]) -> String {
    let mut key_parts: Vec<&str> = Vec::with_capacity(specs.len() * 3);
    for m in specs {
        key_parts.push(m.condition_id.as_str());
        key_parts.push(m.yes_token_id.as_str());
        key_parts.push(m.no_token_id.as_str());
    }
    key_parts.sort();
    key_parts.join("|")
}

async fn run_ws_session(
    config: &Config,
    unique_assets: &[String],
    token_to_condition: &HashMap<String, String>,
    condition_to_pair: &HashMap<String, (String, String)>,
    prices: &mut HashMap<String, PriceData>,
    tx_strategy: &Sender<MarketUpdate>,
    book_depth_levels: usize,
    specs_rx: &mut watch::Receiver<Vec<MarketSpec>>,
) -> Result<()> {
    let mut request = config
        .ws_url
        .clone()
        .into_client_request()
        .context("Invalid POLYMARKET_WS_URL")?;

    // Merge headers + cookies (cookie header is derived unless already present).
    apply_headers(&mut request, &config.ws_headers)?;
    apply_cookies(&mut request, &config.ws_cookies)?;

    let (ws_stream, _resp) = connect_async(request).await.context("WS connect failed")?;
    info!("Connected to WS");

    let subscribe_msg = config
        .ws_subscribe_message
        .clone()
        .unwrap_or_else(|| json!({"type":"market","assets_ids": unique_assets}));
    let subscribe_txt =
        serde_json::to_string(&subscribe_msg).context("Failed to serialize subscribe message")?;

    let (mut write, mut read) = ws_stream.split();
    write
        .send(Message::Text(subscribe_txt))
        .await
        .context("Failed to send subscribe message")?;

    loop {
        tokio::select! {
            _ = specs_rx.changed() => {
                info!("Market specs updated; resubscribing WS");
                break;
            }
            msg = read.next() => {
                let Some(msg) = msg else { break; };
                let msg = msg.context("WS read error")?;
                match msg {
                    Message::Text(raw) => {
                        for v in iter_json_messages(&raw) {
                            for condition_id in ingest_message(v, token_to_condition, prices, book_depth_levels) {
                                let Some((yes_id, no_id)) = condition_to_pair.get(&condition_id) else {
                                    continue;
                                };

                                let (Some(yes), Some(no)) = (prices.get(yes_id), prices.get(no_id)) else {
                                    continue;
                                };

                                let update = MarketUpdate {
                                    condition_id: condition_id.clone(),
                                    yes_token_id: yes_id.clone(),
                                    no_token_id: no_id.clone(),
                                    yes: yes.clone(),
                                    no: no.clone(),
                                    received_at: now_s(),
                                };

                                if tx_strategy.send(update).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Message::Binary(_) => {}
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await.ok();
                    }
                    Message::Pong(_) => {}
                    Message::Close(frame) => {
                        warn!("WS closed: {frame:?}");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

fn apply_headers(
    request: &mut tokio_tungstenite::tungstenite::http::Request<()>,
    headers: &HashMap<String, String>,
) -> Result<()> {
    let req_headers = request.headers_mut();
    for (k, v) in headers {
        let name =
            tokio_tungstenite::tungstenite::http::header::HeaderName::from_bytes(k.as_bytes())
                .with_context(|| format!("Invalid WS header name: {k}"))?;
        let value = tokio_tungstenite::tungstenite::http::header::HeaderValue::from_str(v)
            .with_context(|| format!("Invalid WS header value for {k}"))?;
        req_headers.insert(name, value);
    }
    Ok(())
}

fn apply_cookies(
    request: &mut tokio_tungstenite::tungstenite::http::Request<()>,
    cookies: &HashMap<String, String>,
) -> Result<()> {
    if cookies.is_empty() {
        return Ok(());
    }
    let mut parts: Vec<String> = Vec::with_capacity(cookies.len());
    for (k, v) in cookies {
        parts.push(format!("{k}={v}"));
    }
    parts.sort();
    let cookie_header = parts.join("; ");
    let req_headers = request.headers_mut();
    if !req_headers.contains_key(tokio_tungstenite::tungstenite::http::header::COOKIE) {
        let value =
            tokio_tungstenite::tungstenite::http::header::HeaderValue::from_str(&cookie_header)
                .context("Invalid Cookie header value")?;
        req_headers.insert(tokio_tungstenite::tungstenite::http::header::COOKIE, value);
    }
    Ok(())
}

fn iter_json_messages(raw: &str) -> Vec<Value> {
    let mut out = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(parsed) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        match parsed {
            Value::Array(arr) => out.extend(arr),
            other => out.push(other),
        }
    }
    out
}

fn ingest_message(
    v: Value,
    token_to_condition: &HashMap<String, String>,
    prices: &mut HashMap<String, PriceData>,
    book_depth_levels: usize,
) -> Vec<String> {
    let Value::Object(obj) = v else {
        return Vec::new();
    };

    let mut updated_conditions: HashSet<String> = HashSet::new();

    // Book snapshot(s).
    if obj
        .get("event_type")
        .and_then(Value::as_str)
        .is_some_and(|s| s == "book")
    {
        let token_id = obj
            .get("asset_id")
            .or_else(|| obj.get("token_id"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let Some(token_id) = token_id else {
            return Vec::new();
        };
        if !token_to_condition.contains_key(&token_id) {
            return Vec::new();
        }
        let bids = obj
            .get("bids")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let asks = obj
            .get("asks")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if bids.is_empty() || asks.is_empty() {
            return Vec::new();
        }
        let (best_bid, best_bid_size) = best_bid_from_levels(bids);
        let (best_ask, best_ask_size) = best_ask_from_levels(asks);
        if best_bid <= 0.0 || best_ask <= 0.0 {
            return Vec::new();
        }
        let now = now_s();
        let ask_depth_usdc = compute_ask_depth_usdc(asks, book_depth_levels);
        prices.insert(
            token_id.clone(),
            PriceData {
                best_bid,
                best_ask,
                best_bid_size,
                best_ask_size,
                ask_depth_usdc,
                timestamp: now,
            },
        );
        if let Some(cid) = token_to_condition.get(&token_id) {
            updated_conditions.insert(cid.clone());
        }
        return updated_conditions.into_iter().collect();
    }

    // Batch price_change payload.
    if let Some(changes) = obj.get("price_changes").and_then(Value::as_array) {
        let mut seen: HashSet<String> = HashSet::new();
        for change in changes {
            let Value::Object(ch) = change else {
                continue;
            };
            let Some(token_id) = ch
                .get("asset_id")
                .or_else(|| ch.get("token_id"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            if !token_to_condition.contains_key(token_id) || !seen.insert(token_id.to_string()) {
                continue;
            }
            let Some(best_bid) = parse_f64(ch.get("best_bid")) else {
                continue;
            };
            let Some(best_ask) = parse_f64(ch.get("best_ask")) else {
                continue;
            };
            if best_bid <= 0.0 || best_ask <= 0.0 {
                continue;
            }
            let now = now_s();
            let (prev_bid_size, prev_ask_size, prev_ask_depth) = prices
                .get(token_id)
                .map(|p| (p.best_bid_size, p.best_ask_size, p.ask_depth_usdc))
                .unwrap_or((0.0, 0.0, 0.0));
            prices.insert(
                token_id.to_string(),
                PriceData {
                    best_bid,
                    best_ask,
                    best_bid_size: prev_bid_size,
                    best_ask_size: prev_ask_size,
                    ask_depth_usdc: prev_ask_depth,
                    timestamp: now,
                },
            );
            if let Some(cid) = token_to_condition.get(token_id) {
                updated_conditions.insert(cid.clone());
            }
        }
        return updated_conditions.into_iter().collect();
    }

    // Single update payload(s) (fallback).
    let Some(token_id) = obj
        .get("asset_id")
        .or_else(|| obj.get("token_id"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    if !token_to_condition.contains_key(token_id) {
        return Vec::new();
    }
    let Some(best_bid) = parse_f64(obj.get("best_bid").or_else(|| obj.get("bid"))) else {
        return Vec::new();
    };
    let Some(best_ask) = parse_f64(obj.get("best_ask").or_else(|| obj.get("ask"))) else {
        return Vec::new();
    };
    if best_bid <= 0.0 || best_ask <= 0.0 {
        return Vec::new();
    }
    let now = now_s();
    let (prev_bid_size, prev_ask_size, prev_ask_depth) = prices
        .get(token_id)
        .map(|p| (p.best_bid_size, p.best_ask_size, p.ask_depth_usdc))
        .unwrap_or((0.0, 0.0, 0.0));
    prices.insert(
        token_id.to_string(),
        PriceData {
            best_bid,
            best_ask,
            best_bid_size: prev_bid_size,
            best_ask_size: prev_ask_size,
            ask_depth_usdc: prev_ask_depth,
            timestamp: now,
        },
    );
    if let Some(cid) = token_to_condition.get(token_id) {
        updated_conditions.insert(cid.clone());
    }
    updated_conditions.into_iter().collect()
}

fn parse_f64(v: Option<&Value>) -> Option<f64> {
    match v? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn best_bid_from_levels(levels: &[Value]) -> (f64, f64) {
    let mut best_price = 0.0;
    for lvl in levels {
        let Some((p, _s)) = parse_level(lvl) else {
            continue;
        };
        if p > best_price {
            best_price = p;
        }
    }
    if best_price <= 0.0 {
        return (0.0, 0.0);
    }
    let mut total_size = 0.0;
    for lvl in levels {
        let Some((p, s)) = parse_level(lvl) else {
            continue;
        };
        if (p - best_price).abs() <= f64::EPSILON {
            total_size += s;
        }
    }
    (best_price, total_size)
}

fn best_ask_from_levels(levels: &[Value]) -> (f64, f64) {
    let mut best_price = f64::INFINITY;
    for lvl in levels {
        let Some((p, _s)) = parse_level(lvl) else {
            continue;
        };
        if p > 0.0 && p < best_price {
            best_price = p;
        }
    }
    if !best_price.is_finite() {
        return (0.0, 0.0);
    }
    let mut total_size = 0.0;
    for lvl in levels {
        let Some((p, s)) = parse_level(lvl) else {
            continue;
        };
        if (p - best_price).abs() <= f64::EPSILON {
            total_size += s;
        }
    }
    (best_price, total_size)
}

fn parse_level(v: &Value) -> Option<(f64, f64)> {
    match v {
        Value::Object(obj) => {
            let p = parse_f64(obj.get("price"))?;
            let s = parse_f64(obj.get("size"))?;
            if p <= 0.0 || s <= 0.0 {
                return None;
            }
            Some((p, s))
        }
        Value::Array(arr) if arr.len() >= 2 => {
            let p = parse_f64(arr.get(0))?;
            let s = parse_f64(arr.get(1))?;
            if p <= 0.0 || s <= 0.0 {
                return None;
            }
            Some((p, s))
        }
        _ => None,
    }
}

fn compute_ask_depth_usdc(levels: &[Value], max_levels: usize) -> f64 {
    if max_levels == 0 {
        return 0.0;
    }
    let mut entries: Vec<(f64, f64)> = levels.iter().filter_map(parse_level).collect();
    entries.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
    entries
        .into_iter()
        .take(max_levels)
        .map(|(p, s)| p * s)
        .sum()
}
