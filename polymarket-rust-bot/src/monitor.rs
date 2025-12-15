use crate::config::Config;
use crate::market_specs;
use crate::position_manager::PositionManager;
use crate::types::{now_s, MarketSpec, MarketUpdate, PriceData};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use polymarket_rs::types::{Side as WsSide, WsEvent};
use serde_json::{json, Value};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

#[derive(Debug, Default, Clone)]
struct TokenBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl TokenBook {
    fn apply_snapshot(&mut self, bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) {
        self.bids.clear();
        self.asks.clear();

        for (price, size) in bids {
            if price > Decimal::ZERO && size > Decimal::ZERO {
                *self.bids.entry(price).or_insert(Decimal::ZERO) += size;
            }
        }
        for (price, size) in asks {
            if price > Decimal::ZERO && size > Decimal::ZERO {
                *self.asks.entry(price).or_insert(Decimal::ZERO) += size;
            }
        }
    }

    fn apply_change(&mut self, side: WsSide, price: Decimal, size: Decimal) {
        if price <= Decimal::ZERO {
            return;
        }

        let book = match side {
            WsSide::Buy => &mut self.bids,
            WsSide::Sell => &mut self.asks,
        };

        if size <= Decimal::ZERO {
            book.remove(&price);
        } else {
            book.insert(price, size);
        }
    }

    fn to_price_data(&self, book_depth_levels: usize, ts: f64) -> Option<PriceData> {
        let (best_bid, best_bid_size) = self
            .bids
            .iter()
            .next_back()
            .and_then(|(p, s)| Some((p.to_f64()?, s.to_f64()?)))?;

        let (best_ask, best_ask_size) = self
            .asks
            .iter()
            .next()
            .and_then(|(p, s)| Some((p.to_f64()?, s.to_f64()?)))?;

        if !(best_bid > 0.0 && best_ask > 0.0) {
            return None;
        }

        let mut depth = Decimal::ZERO;
        if book_depth_levels > 0 {
            for (p, s) in self.asks.iter().take(book_depth_levels) {
                depth += *p * *s;
            }
        }
        let ask_depth_usdc = depth.to_f64().unwrap_or(0.0);

        Some(PriceData {
            best_bid,
            best_ask,
            best_bid_size,
            best_ask_size,
            ask_depth_usdc,
            timestamp: ts,
        })
    }
}

pub async fn run_ws_monitor(
    config: Arc<Config>,
    mut specs_rx: watch::Receiver<Vec<MarketSpec>>,
    tx_strategy: Sender<MarketUpdate>,
    position_manager: Arc<PositionManager>,
) -> Result<()> {
    let mut books: HashMap<String, TokenBook> = HashMap::new();
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
            &mut books,
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
    HashMap<String, ConditionInfo>,
    Vec<String>,
) {
    let mut token_to_condition: HashMap<String, String> = HashMap::new();
    let mut condition_to_pair: HashMap<String, ConditionInfo> = HashMap::new();
    let mut assets: Vec<String> = Vec::new();

    for m in specs {
        token_to_condition.insert(m.yes_token_id.clone(), m.condition_id.clone());
        token_to_condition.insert(m.no_token_id.clone(), m.condition_id.clone());
        condition_to_pair.insert(
            m.condition_id.clone(),
            ConditionInfo {
                yes_token_id: m.yes_token_id.clone(),
                no_token_id: m.no_token_id.clone(),
                start_epoch_utc: m.start_epoch_utc,
                strike_price: m.strike_price,
                label: m.label.clone(),
                event_slug: m.event_slug.clone(),
            },
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
            .or_insert_with(|| ConditionInfo {
                yes_token_id: p.yes_token_id.clone(),
                no_token_id: p.no_token_id.clone(),
                start_epoch_utc: None,
                strike_price: None,
                label: None,
                event_slug: None,
            });
        assets.push(p.yes_token_id.clone());
        assets.push(p.no_token_id.clone());
    }

    assets.sort();
    assets.dedup();

    (token_to_condition, condition_to_pair, assets)
}

#[derive(Debug, Clone)]
struct ConditionInfo {
    yes_token_id: String,
    no_token_id: String,
    start_epoch_utc: Option<i64>,
    strike_price: Option<f64>,
    label: Option<String>,
    event_slug: Option<String>,
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
    condition_to_pair: &HashMap<String, ConditionInfo>,
    books: &mut HashMap<String, TokenBook>,
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
                            for condition_id in
                                ingest_message(v, token_to_condition, books, prices, book_depth_levels)
                            {
                                let Some(info) = condition_to_pair.get(&condition_id) else {
                                    continue;
                                };

                                let yes_id = &info.yes_token_id;
                                let no_id = &info.no_token_id;
                                let (Some(yes), Some(no)) = (prices.get(yes_id), prices.get(no_id)) else {
                                    continue;
                                };

                                let update = MarketUpdate {
                                    condition_id: condition_id.clone(),
                                    yes_token_id: yes_id.clone(),
                                    no_token_id: no_id.clone(),
                                    start_epoch_utc: info.start_epoch_utc,
                                    strike_price: info.strike_price,
                                    label: info.label.clone(),
                                    event_slug: info.event_slug.clone(),
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
    books: &mut HashMap<String, TokenBook>,
    prices: &mut HashMap<String, PriceData>,
    book_depth_levels: usize,
) -> Vec<String> {
    let event = match serde_json::from_value::<WsEvent>(v) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };

    let mut updated_conditions: HashSet<String> = HashSet::new();
    let now = now_s();

    match event {
        WsEvent::Book(book) => {
            let token_id = book.asset_id;
            if !token_to_condition.contains_key(&token_id) {
                return Vec::new();
            }

            let bids: Vec<(Decimal, Decimal)> =
                book.bids.into_iter().map(|l| (l.price, l.size)).collect();
            let asks: Vec<(Decimal, Decimal)> =
                book.asks.into_iter().map(|l| (l.price, l.size)).collect();

            let state = books.entry(token_id.clone()).or_default();
            state.apply_snapshot(bids, asks);

            if let Some(pd) = state.to_price_data(book_depth_levels, now) {
                prices.insert(token_id.clone(), pd);
                if let Some(cid) = token_to_condition.get(&token_id) {
                    updated_conditions.insert(cid.clone());
                }
            }
        }
        WsEvent::PriceChange(delta) => {
            let mut touched: HashSet<String> = HashSet::new();
            for change in delta.price_changes {
                if !token_to_condition.contains_key(&change.asset_id) {
                    continue;
                }
                let token_id = change.asset_id.clone();
                books
                    .entry(token_id.clone())
                    .or_default()
                    .apply_change(change.side, change.price, change.size);
                touched.insert(token_id);
            }

            for token_id in touched {
                let Some(state) = books.get(&token_id) else {
                    continue;
                };
                let Some(pd) = state.to_price_data(book_depth_levels, now) else {
                    continue;
                };

                prices.insert(token_id.clone(), pd);
                if let Some(cid) = token_to_condition.get(&token_id) {
                    updated_conditions.insert(cid.clone());
                }
            }
        }
        WsEvent::LastTradePrice(_) | WsEvent::TickSizeChange(_) => {}
    }

    updated_conditions.into_iter().collect()
}
