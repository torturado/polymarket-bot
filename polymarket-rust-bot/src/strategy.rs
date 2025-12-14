use crate::config::Config;
use crate::execution::{ExecutionEngine, PriceStore};
use crate::paper::{PaperTradeLogger, PaperWallet};
use crate::position_manager::{LegInPosition, PositionManager, PositionState};
use crate::types::{now_s, MarketUpdate, PriceData};
use anyhow::Result;
use log::{info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

pub async fn run(
    mut rx: Receiver<MarketUpdate>,
    config: Arc<Config>,
    position_manager: Arc<PositionManager>,
) -> Result<()> {
    let prices: PriceStore = Arc::new(RwLock::new(HashMap::<String, PriceData>::new()));

    let wallet = Arc::new(PaperWallet::new(config.paper_initial_balance));
    let logger = Arc::new(PaperTradeLogger::new(config.paper_trades_log.clone()));
    let exec = ExecutionEngine::new(
        Arc::clone(&config),
        Arc::clone(&wallet),
        logger,
        Arc::clone(&prices),
    );

    if config.paper_trading {
        info!(
            "PAPER trading enabled: balance={:.4} log={}",
            wallet.balance().await,
            config.paper_trades_log.display()
        );
    } else {
        warn!(
            "paper_trading=false: no orders will be simulated yet (real trading not implemented)"
        );
    }

    let state = Arc::new(Mutex::new(RiskState::new(now_s())));
    let mut tasks_by_condition: HashMap<String, JoinHandle<()>> = HashMap::new();

    while let Some(update) = rx.recv().await {
        // Keep an in-memory price store for burst simulation and unwind pricing.
        {
            let mut p = prices.write().await;
            p.insert(update.yes_token_id.clone(), update.yes.clone());
            p.insert(update.no_token_id.clone(), update.no.clone());
        }

        cleanup_finished_tasks(&mut tasks_by_condition).await;

        if !config.paper_trading || !config.arb_enabled {
            continue;
        }

        let cid = update.condition_id.clone();
        if tasks_by_condition.contains_key(&cid) {
            continue;
        }

        let yes_ask = update.yes.best_ask;
        let no_ask = update.no.best_ask;
        if yes_ask <= 0.0 || no_ask <= 0.0 {
            continue;
        }

        if config.min_book_depth_usdc > 0.0 {
            if update.yes.ask_depth_usdc < config.min_book_depth_usdc
                || update.no.ask_depth_usdc < config.min_book_depth_usdc
            {
                continue;
            }
        }

        if let Some(pos) = position_manager.get_position(&cid) {
            if !pos.leg_1_filled || pos.leg_2_pending || pos.unwind_pending {
                continue;
            }

            let leg1_is_yes = pos.leg_1_side.trim().eq_ignore_ascii_case("YES");
            let (opp_ask, opp_outcome, leg2_token_id, leg2_bid_ref, leg1_bid_ref) =
                if leg1_is_yes {
                    (
                        update.no.best_ask,
                        "NO",
                        pos.no_token_id.clone(),
                        update.no.best_bid,
                        update.yes.best_bid,
                    )
                } else {
                    (
                        update.yes.best_ask,
                        "YES",
                        pos.yes_token_id.clone(),
                        update.yes.best_bid,
                        update.no.best_bid,
                    )
                };
            if opp_ask <= 0.0 {
                continue;
            }

            let target = config.arb_max_entry_exit_cost;
            let current_cost = pos.leg_1_entry_price + opp_ask;

            if target > 0.0 && current_cost <= target {
                let marked = position_manager
                    .with_position_mut(&cid, |p| {
                        if p.leg_2_pending || !p.leg_1_filled {
                            return false;
                        }
                        p.leg_2_pending = true;
                        p.state = PositionState::Closing;
                        p.leg_2_entry_price = Some(opp_ask);
                        true
                    })
                    .unwrap_or(false);
                if !marked {
                    continue;
                }

                let cid_key = cid.clone();
                let cid_for_task = cid_key.clone();
                let exec_cl = exec.clone();
                let cfg_cl = Arc::clone(&config);
                let st_cl = Arc::clone(&state);
                let pos_mgr_cl = Arc::clone(&position_manager);
                let update_cl = update.clone();
                let handle = tokio::spawn(async move {
                    if let Err(e) = run_paper_leg2_and_merge(
                        exec_cl,
                        st_cl,
                        cfg_cl,
                        pos_mgr_cl,
                        update_cl,
                        opp_outcome.to_string(),
                        leg2_token_id,
                        opp_ask,
                        leg2_bid_ref,
                        leg1_bid_ref,
                    )
                    .await
                    {
                        warn!("PAPER leg2 task failed: condition={} err={e:#}", cid_for_task);
                    }
                });
                tasks_by_condition.insert(cid_key, handle);
                continue;
            }

            let max_wait_seconds = config.leg1_max_wait_s.max(0.0);
            let elapsed = now_s() - pos.entry_time;
            if max_wait_seconds > 0.0 && elapsed.is_finite() && elapsed > max_wait_seconds {
                warn!(
                    "TIMEOUT: Position {} held for {:.1}s without leg2 opportunity (cost={:.4} > target={:.4}). Unwinding.",
                    cid, elapsed, current_cost, target
                );

                let marked_unwind = position_manager
                    .with_position_mut(&cid, |p| {
                        if p.unwind_pending || p.leg_2_pending {
                            return false;
                        }
                        p.unwind_pending = true;
                        p.state = PositionState::Unwinding;
                        true
                    })
                    .unwrap_or(false);

                if marked_unwind {
                    let cid_key = cid.clone();
                    let cid_for_task = cid_key.clone();
                    let exec_cl = exec.clone();
                    let cfg_cl = Arc::clone(&config);
                    let st_cl = Arc::clone(&state);
                    let pos_mgr_cl = Arc::clone(&position_manager);

                    let (outcome_to_sell, token_to_sell, exit_price_ref) = if leg1_is_yes {
                        ("YES".to_string(), pos.yes_token_id.clone(), update.yes.best_bid)
                    } else {
                        ("NO".to_string(), pos.no_token_id.clone(), update.no.best_bid)
                    };

                    let size_to_sell = pos.leg_1_size;

                    let handle = tokio::spawn(async move {
                        match run_paper_unwind(
                            exec_cl,
                            st_cl,
                            cfg_cl,
                            pos_mgr_cl,
                            cid_for_task.clone(),
                            outcome_to_sell,
                            token_to_sell,
                            size_to_sell,
                            exit_price_ref,
                        )
                        .await
                        {
                            Ok(_) => info!("Unwind success for {}", cid_for_task),
                            Err(e) => warn!("Unwind failed for {}: {e:#}", cid_for_task),
                        }
                    });
                    tasks_by_condition.insert(cid_key, handle);
                }
            }

            continue;
        }

        if config.max_concurrent_positions > 0
            && position_manager.get_open_positions_count() >= config.max_concurrent_positions
        {
            continue;
        }

        let entry_thr = config.entry_threshold;
        if !(entry_thr > 0.0) {
            continue;
        }

        let mut candidates: Vec<(&str, f64)> = Vec::new();
        if yes_ask <= entry_thr {
            candidates.push(("YES", yes_ask));
        }
        if no_ask <= entry_thr {
            candidates.push(("NO", no_ask));
        }
        if candidates.is_empty() {
            continue;
        }
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let (leg1_outcome, leg1_ask_ref) = candidates[0];

        let target_cost = config.arb_max_entry_exit_cost;
        let budget = config.arb_budget_usdc;
        if !(target_cost > 0.0 && budget > 0.0) {
            continue;
        }
        let shares_target = budget / target_cost;
        if !shares_target.is_finite() || shares_target <= 0.0 {
            continue;
        }

        let now = update.received_at;
        if !entry_allowed(&exec, Arc::clone(&state), &config, &cid, now).await {
            continue;
        }

        let leg1_token_id = if leg1_outcome == "YES" {
            update.yes_token_id.clone()
        } else {
            update.no_token_id.clone()
        };

        position_manager.open_position(LegInPosition {
            condition_id: cid.clone(),
            yes_token_id: update.yes_token_id.clone(),
            no_token_id: update.no_token_id.clone(),
            leg_1_side: leg1_outcome.to_string(),
            leg_1_entry_price: leg1_ask_ref,
            leg_1_size: shares_target,
            leg_1_filled: false,
            state: PositionState::EnteringLeg1,
            entry_time: now_s(),
            leg_2_entry_price: None,
            leg_2_size: None,
            leg_2_filled: false,
            leg_2_pending: false,
            merge_pending: false,
            unwind_pending: false,
            scalein_pending: false,
            scalein_count: 0,
            scalein_next_usdc: None,
            last_scalein_at: 0.0,
        });

        let cid_key = cid.clone();
        let exec_cl = exec.clone();
        let cfg_cl = Arc::clone(&config);
        let pos_mgr_cl = Arc::clone(&position_manager);
        let handle = tokio::spawn(async move {
            if let Err(e) = run_paper_leg1_entry(
                exec_cl,
                cfg_cl,
                pos_mgr_cl,
                cid_key.clone(),
                leg1_outcome.to_string(),
                leg1_token_id,
                shares_target,
                leg1_ask_ref,
            )
            .await
            {
                warn!("PAPER leg1 task failed: condition={} err={e:#}", cid_key);
            }
        });
        tasks_by_condition.insert(cid, handle);
    }
    Ok(())
}

#[derive(Debug)]
struct RiskState {
    cooldown_until: HashMap<String, f64>,
    entry_counts: HashMap<String, u32>,
    last_entry_signal_at: HashMap<String, f64>,
    daily_day_utc: i64,
    daily_pnl_usdc: f64,
    daily_loss_tripped: bool,
    paper_out_of_funds_logged: bool,
}

impl RiskState {
    fn new(now: f64) -> Self {
        Self {
            cooldown_until: HashMap::new(),
            entry_counts: HashMap::new(),
            last_entry_signal_at: HashMap::new(),
            daily_day_utc: day_utc(now),
            daily_pnl_usdc: 0.0,
            daily_loss_tripped: false,
            paper_out_of_funds_logged: false,
        }
    }

    fn roll_day(&mut self, now: f64) {
        let d = day_utc(now);
        if d != self.daily_day_utc {
            self.daily_day_utc = d;
            self.daily_pnl_usdc = 0.0;
            self.daily_loss_tripped = false;
        }
    }
}

fn day_utc(ts: f64) -> i64 {
    if !ts.is_finite() || ts <= 0.0 {
        return 0;
    }
    (ts / 86_400.0).floor() as i64
}

async fn cleanup_finished_tasks(tasks: &mut HashMap<String, JoinHandle<()>>) {
    let mut done: Vec<String> = Vec::new();
    for (k, h) in tasks.iter() {
        if h.is_finished() {
            done.push(k.clone());
        }
    }
    for k in done {
        if let Some(h) = tasks.remove(&k) {
            let _ = h.await;
        }
    }
}

async fn entry_allowed(
    exec: &ExecutionEngine,
    state: Arc<Mutex<RiskState>>,
    config: &Config,
    condition_id: &str,
    now: f64,
) -> bool {
    let balance = exec.paper_balance().await;
    if balance <= 0.0 {
        let mut st = state.lock().await;
        if !st.paper_out_of_funds_logged {
            st.paper_out_of_funds_logged = true;
            warn!("PAPER out of funds: balance={balance:.4}; disabling new entries");
        }
        return false;
    }

    if balance + 1e-12 < config.arb_budget_usdc {
        return false;
    }

    let mut st = state.lock().await;
    st.roll_day(now);

    if config.max_daily_loss > 0.0 && st.daily_pnl_usdc <= -config.max_daily_loss {
        if !st.daily_loss_tripped {
            st.daily_loss_tripped = true;
            warn!(
                "Daily loss limit reached: pnl={:.4} <= -{:.4}; disabling new entries until next UTC day",
                st.daily_pnl_usdc, config.max_daily_loss
            );
        }
        return false;
    }

    if now < st.cooldown_until.get(condition_id).copied().unwrap_or(0.0) {
        return false;
    }

    if config.max_entries_per_condition > 0 {
        let cnt = st.entry_counts.get(condition_id).copied().unwrap_or(0);
        if cnt >= config.max_entries_per_condition {
            return false;
        }
    }

    let min_interval_s = (config.entry_signal_min_interval_ms as f64).max(0.0) / 1000.0;
    if min_interval_s > 0.0 {
        let last = st.last_entry_signal_at.get(condition_id).copied();
        if let Some(last) = last {
            if (now - last) < min_interval_s {
                return false;
            }
        }
    }

    st.last_entry_signal_at
        .insert(condition_id.to_string(), now);
    st.entry_counts
        .entry(condition_id.to_string())
        .and_modify(|c| *c = c.saturating_add(1))
        .or_insert(1);

    true
}

async fn run_paper_arb_trade(
    exec: ExecutionEngine,
    state: Arc<Mutex<RiskState>>,
    config: Arc<Config>,
    update: MarketUpdate,
) -> Result<()> {
    let cid = update.condition_id.clone();

    let yes_ask = update.yes.best_ask;
    let no_ask = update.no.best_ask;
    let cost = yes_ask + no_ask;
    if yes_ask <= 0.0 || no_ask <= 0.0 || cost <= 0.0 {
        return Ok(());
    }

    let shares_target = config.arb_budget_usdc / cost;
    if !shares_target.is_finite() || shares_target <= 0.0 {
        return Ok(());
    }

    let (
        leg1_outcome,
        leg1_token_id,
        leg1_ask_ref,
        leg1_bid_ref,
        leg2_outcome,
        leg2_token_id,
        leg2_ask_ref,
        leg2_bid_ref,
    ) = if yes_ask <= no_ask {
        (
            "YES",
            update.yes_token_id.clone(),
            yes_ask,
            update.yes.best_bid,
            "NO",
            update.no_token_id.clone(),
            no_ask,
            update.no.best_bid,
        )
    } else {
        (
            "NO",
            update.no_token_id.clone(),
            no_ask,
            update.no.best_bid,
            "YES",
            update.yes_token_id.clone(),
            yes_ask,
            update.yes.best_bid,
        )
    };

    info!(
        "PAPER ARB start: condition={} cost={:.4} shares={:.4} yes_ask={:.4} no_ask={:.4}",
        cid, cost, shares_target, yes_ask, no_ask
    );

    let leg1 = exec
        .paper_buy(
            now_s(),
            &cid,
            leg1_outcome,
            &leg1_token_id,
            shares_target,
            leg1_ask_ref,
        )
        .await?;

    if leg1.filled_shares <= 0.0 {
        return Ok(());
    }

    let leg2 = exec
        .paper_buy(
            now_s(),
            &cid,
            leg2_outcome,
            &leg2_token_id,
            leg1.filled_shares,
            leg2_ask_ref,
        )
        .await?;

    if leg2.filled_shares <= 0.0 {
        // Unwind leg 1.
        let sell = exec
            .paper_sell(
                now_s(),
                &cid,
                leg1_outcome,
                &leg1_token_id,
                leg1.filled_shares,
                leg1_bid_ref,
                leg1.vwap_price,
                "UNWIND_LEG1",
            )
            .await?;

        let pnl = (sell.vwap_price - leg1.vwap_price) * sell.filled_shares;
        finish_paper_trade(Arc::clone(&state), &config, &cid, pnl, now_s()).await;
        return Ok(());
    }

    let redeem_value = 1.0;
    let merged = leg1.filled_shares.min(leg2.filled_shares);
    let mut total_pnl = 0.0;

    if merged > 0.0 {
        let pnl_merge = (redeem_value - leg1.vwap_price - leg2.vwap_price) * merged;
        total_pnl += pnl_merge;
        exec.paper_merge(now_s(), &cid, merged, redeem_value, pnl_merge, "ARB_MERGE")
            .await?;
    }

    let leftover_leg1 = (leg1.filled_shares - merged).max(0.0);
    let leftover_leg2 = (leg2.filled_shares - merged).max(0.0);

    if leftover_leg1 > 0.0 {
        let sell = exec
            .paper_sell(
                now_s(),
                &cid,
                leg1_outcome,
                &leg1_token_id,
                leftover_leg1,
                leg1_bid_ref,
                leg1.vwap_price,
                "SELL_LEFTOVER_LEG1",
            )
            .await?;
        total_pnl += (sell.vwap_price - leg1.vwap_price) * sell.filled_shares;
    }

    if leftover_leg2 > 0.0 {
        let sell = exec
            .paper_sell(
                now_s(),
                &cid,
                leg2_outcome,
                &leg2_token_id,
                leftover_leg2,
                leg2_bid_ref,
                leg2.vwap_price,
                "SELL_LEFTOVER_LEG2",
            )
            .await?;
        total_pnl += (sell.vwap_price - leg2.vwap_price) * sell.filled_shares;
    }

    finish_paper_trade(state, &config, &cid, total_pnl, now_s()).await;
    Ok(())
}

async fn finish_paper_trade(
    state: Arc<Mutex<RiskState>>,
    config: &Config,
    condition_id: &str,
    pnl_usdc: f64,
    finished_at: f64,
) {
    let mut st = state.lock().await;
    st.roll_day(finished_at);
    st.daily_pnl_usdc += pnl_usdc;

    let cd = config.reentry_cooldown_s.max(0.0);
    if cd > 0.0 {
        st.cooldown_until
            .insert(condition_id.to_string(), finished_at + cd);
    }

    info!(
        "PAPER PNL: condition={} pnl={:.4} daily_pnl={:.4}",
        condition_id, pnl_usdc, st.daily_pnl_usdc
    );
}

async fn run_paper_leg1_entry(
    exec: ExecutionEngine,
    _config: Arc<Config>,
    position_manager: Arc<PositionManager>,
    condition_id: String,
    outcome: String,
    token_id: String,
    shares_target: f64,
    reference_price: f64,
) -> Result<()> {
    let ts = now_s();
    let fill = exec
        .paper_buy(
            ts,
            &condition_id,
            &outcome,
            &token_id,
            shares_target,
            reference_price,
        )
        .await?;

    if fill.filled_shares <= 0.0 {
        position_manager.close_position(&condition_id);
        return Ok(());
    }

    let _ = position_manager.with_position_mut(&condition_id, |p| {
        p.leg_1_filled = true;
        p.leg_1_size = fill.filled_shares;
        p.leg_1_entry_price = fill.vwap_price;
        p.state = PositionState::Holding;
        p.entry_time = ts;
    });

    Ok(())
}

async fn run_paper_leg2_and_merge(
    exec: ExecutionEngine,
    state: Arc<Mutex<RiskState>>,
    config: Arc<Config>,
    position_manager: Arc<PositionManager>,
    update: MarketUpdate,
    leg2_outcome: String,
    leg2_token_id: String,
    leg2_ask_ref: f64,
    leg2_bid_ref: f64,
    leg1_bid_ref_input: f64,
) -> Result<()> {
    let cid = update.condition_id.clone();
    let Some(pos) = position_manager.get_position(&cid) else {
        return Ok(());
    };

    if !pos.leg_1_filled || pos.leg_1_size <= 0.0 || pos.leg_1_entry_price <= 0.0 {
        position_manager.close_position(&cid);
        return Ok(());
    }

    let leg1_is_yes = pos.leg_1_side.trim().eq_ignore_ascii_case("YES");
    let (leg1_outcome, leg1_token_id, leg1_bid_ref_fallback) = if leg1_is_yes {
        ("YES".to_string(), pos.yes_token_id.clone(), update.yes.best_bid)
    } else {
        ("NO".to_string(), pos.no_token_id.clone(), update.no.best_bid)
    };
    let leg1_bid_ref = if leg1_bid_ref_input > 0.0 {
        leg1_bid_ref_input
    } else {
        leg1_bid_ref_fallback
    };

    let leg2 = exec
        .paper_buy(
            now_s(),
            &cid,
            &leg2_outcome,
            &leg2_token_id,
            pos.leg_1_size,
            leg2_ask_ref,
        )
        .await?;

    if leg2.filled_shares <= 0.0 {
        let sell = exec
            .paper_sell(
                now_s(),
                &cid,
                &leg1_outcome,
                &leg1_token_id,
                pos.leg_1_size,
                leg1_bid_ref,
                pos.leg_1_entry_price,
                "UNWIND_LEG1",
            )
            .await?;

        let pnl = (sell.vwap_price - pos.leg_1_entry_price) * sell.filled_shares;
        finish_paper_trade(Arc::clone(&state), &config, &cid, pnl, now_s()).await;
        position_manager.close_position(&cid);
        return Ok(());
    }

    let redeem_value = 1.0;
    let merged = pos.leg_1_size.min(leg2.filled_shares);
    let mut total_pnl = 0.0;

    if merged > 0.0 {
        let pnl_merge = (redeem_value - pos.leg_1_entry_price - leg2.vwap_price) * merged;
        total_pnl += pnl_merge;
        exec.paper_merge(now_s(), &cid, merged, redeem_value, pnl_merge, "ARB_MERGE")
            .await?;
    }

    let leftover_leg1 = (pos.leg_1_size - merged).max(0.0);
    let leftover_leg2 = (leg2.filled_shares - merged).max(0.0);

    if leftover_leg1 > 0.0 {
        let sell = exec
            .paper_sell(
                now_s(),
                &cid,
                &leg1_outcome,
                &leg1_token_id,
                leftover_leg1,
                leg1_bid_ref,
                pos.leg_1_entry_price,
                "SELL_LEFTOVER_LEG1",
            )
            .await?;
        total_pnl += (sell.vwap_price - pos.leg_1_entry_price) * sell.filled_shares;
    }

    if leftover_leg2 > 0.0 {
        let sell = exec
            .paper_sell(
                now_s(),
                &cid,
                &leg2_outcome,
                &leg2_token_id,
                leftover_leg2,
                leg2_bid_ref,
                leg2.vwap_price,
                "SELL_LEFTOVER_LEG2",
            )
            .await?;
        total_pnl += (sell.vwap_price - leg2.vwap_price) * sell.filled_shares;
    }

    finish_paper_trade(state, &config, &cid, total_pnl, now_s()).await;
    position_manager.close_position(&cid);
    Ok(())
}

async fn run_paper_unwind(
    exec: ExecutionEngine,
    state: Arc<Mutex<RiskState>>,
    config: Arc<Config>,
    position_manager: Arc<PositionManager>,
    condition_id: String,
    outcome: String,
    token_id: String,
    size: f64,
    price_ref: f64,
) -> Result<()> {
    let entry_price = position_manager
        .get_position(&condition_id)
        .map(|p| p.leg_1_entry_price)
        .unwrap_or(0.0);

    let sell = exec
        .paper_sell(
            now_s(),
            &condition_id,
            &outcome,
            &token_id,
            size,
            price_ref,
            entry_price,
            "TIMEOUT_UNWIND",
        )
        .await?;

    if sell.filled_shares <= 0.0 {
        let _ = position_manager.with_position_mut(&condition_id, |p| {
            p.unwind_pending = false;
            p.state = PositionState::Holding;
        });
        warn!(
            "Unwind failed (no liquidity / no bid). Retrying next tick for {}",
            condition_id
        );
        return Ok(());
    }

    let pnl = (sell.vwap_price - entry_price) * sell.filled_shares;
    finish_paper_trade(state, &config, &condition_id, pnl, now_s()).await;
    position_manager.close_position(&condition_id);

    Ok(())
}
