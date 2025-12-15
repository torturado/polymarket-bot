use crate::config::Config;
use crate::paper::{PaperTradeLogger, PaperTradeRow, PaperWallet};
use crate::types::PriceData;
use anyhow::Result;
use log::info;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type PriceStore = Arc<RwLock<HashMap<String, PriceData>>>;

#[derive(Debug, Clone)]
pub struct ExecutionEngine {
    config: Arc<Config>,
    wallet: Arc<PaperWallet>,
    logger: Arc<PaperTradeLogger>,
    prices: PriceStore,
}

#[derive(Debug, Clone)]
pub struct FillResult {
    pub filled_shares: f64,
    pub vwap_price: f64,
    pub orders_sent: u32,
    pub notional_usdc: f64,
}

impl ExecutionEngine {
    pub fn new(
        config: Arc<Config>,
        wallet: Arc<PaperWallet>,
        logger: Arc<PaperTradeLogger>,
        prices: PriceStore,
    ) -> Self {
        Self {
            config,
            wallet,
            logger,
            prices,
        }
    }

    pub async fn paper_balance(&self) -> f64 {
        self.wallet.balance().await
    }

    pub async fn paper_buy(
        &self,
        ts: f64,
        condition_id: &str,
        outcome: &str,
        token_id: &str,
        total_shares: f64,
        reference_price: f64,
    ) -> Result<FillResult> {
        if total_shares <= 0.0 {
            return Ok(FillResult {
                filled_shares: 0.0,
                vwap_price: f64::NAN,
                orders_sent: 0,
                notional_usdc: 0.0,
            });
        }

        let fill = if self.config.use_burst_execution {
            self.paper_burst_buy(
                ts,
                condition_id,
                outcome,
                token_id,
                total_shares,
                reference_price,
            )
            .await?
        } else {
            self.paper_single_buy(
                ts,
                condition_id,
                outcome,
                token_id,
                total_shares,
                reference_price,
            )
            .await?
        };

        Ok(fill)
    }

    pub async fn paper_sell(
        &self,
        ts: f64,
        condition_id: &str,
        outcome: &str,
        token_id: &str,
        shares: f64,
        reference_price: f64,
        cost_basis_price: f64,
        info: &str,
    ) -> Result<FillResult> {
        if shares <= 0.0 {
            return Ok(FillResult {
                filled_shares: 0.0,
                vwap_price: f64::NAN,
                orders_sent: 0,
                notional_usdc: 0.0,
            });
        }

        let price = self
            .get_price(token_id)
            .await
            .map(|p| p.best_bid)
            .filter(|p| *p > 0.0)
            .unwrap_or(reference_price);

        if price <= 0.0 {
            return Ok(FillResult {
                filled_shares: 0.0,
                vwap_price: f64::NAN,
                orders_sent: 0,
                notional_usdc: 0.0,
            });
        }

        let revenue = price * shares;
        let pnl = (price - cost_basis_price) * shares;
        self.wallet.credit(revenue).await;
        let balance = self.wallet.balance().await;

        self.logger
            .append(PaperTradeRow {
                ts,
                action: "SELL".to_string(),
                condition_id: condition_id.to_string(),
                outcome: outcome.to_string(),
                token_id: token_id.to_string(),
                price,
                shares,
                usdc_delta: revenue,
                balance_usdc: balance,
                pnl_usdc: Some(pnl),
                info: info.to_string(),
            })
            .await?;

        info!(
            "PAPER SELL: condition={} outcome={} price={:.4} shares={:.4} pnl={:.4} balance={:.4} info={}",
            condition_id,
            outcome,
            price,
            shares,
            pnl,
            balance,
            info
        );

        Ok(FillResult {
            filled_shares: shares,
            vwap_price: price,
            orders_sent: 1,
            notional_usdc: revenue,
        })
    }

    pub async fn paper_merge(
        &self,
        ts: f64,
        condition_id: &str,
        shares: f64,
        redeem_value: f64,
        pnl_usdc: f64,
        info: &str,
    ) -> Result<()> {
        if shares <= 0.0 || redeem_value <= 0.0 {
            return Ok(());
        }
        let redeem = redeem_value * shares;
        self.wallet.credit(redeem).await;
        let balance = self.wallet.balance().await;

        self.logger
            .append(PaperTradeRow {
                ts,
                action: "MERGE".to_string(),
                condition_id: condition_id.to_string(),
                outcome: "".to_string(),
                token_id: "".to_string(),
                price: redeem_value,
                shares,
                usdc_delta: redeem,
                balance_usdc: balance,
                pnl_usdc: Some(pnl_usdc),
                info: info.to_string(),
            })
            .await?;

        info!(
            "PAPER MERGE: condition={} shares={:.4} redeem_value={:.4} pnl={:.4} balance={:.4} info={}",
            condition_id, shares, redeem_value, pnl_usdc, balance, info
        );
        Ok(())
    }

    async fn get_price(&self, token_id: &str) -> Option<PriceData> {
        self.prices.read().await.get(token_id).cloned()
    }

    async fn paper_single_buy(
        &self,
        ts: f64,
        condition_id: &str,
        outcome: &str,
        token_id: &str,
        total_shares: f64,
        reference_price: f64,
    ) -> Result<FillResult> {
        let price = self
            .get_price(token_id)
            .await
            .map(|p| p.best_ask)
            .filter(|p| *p > 0.0)
            .unwrap_or(reference_price);
        if price <= 0.0 {
            return Ok(FillResult {
                filled_shares: 0.0,
                vwap_price: f64::NAN,
                orders_sent: 0,
                notional_usdc: 0.0,
            });
        }

        let cost = price * total_shares;
        if !self.wallet.debit(cost).await {
            return Ok(FillResult {
                filled_shares: 0.0,
                vwap_price: f64::NAN,
                orders_sent: 0,
                notional_usdc: 0.0,
            });
        }

        let balance = self.wallet.balance().await;
        self.logger
            .append(PaperTradeRow {
                ts,
                action: "BUY".to_string(),
                condition_id: condition_id.to_string(),
                outcome: outcome.to_string(),
                token_id: token_id.to_string(),
                price,
                shares: total_shares,
                usdc_delta: -cost,
                balance_usdc: balance,
                pnl_usdc: None,
                info: "single".to_string(),
            })
            .await?;

        info!(
            "PAPER BUY: condition={} outcome={} price={:.4} shares={:.4} cost={:.4} balance={:.4} info=single",
            condition_id, outcome, price, total_shares, cost, balance
        );

        Ok(FillResult {
            filled_shares: total_shares,
            vwap_price: price,
            orders_sent: 1,
            notional_usdc: cost,
        })
    }

    async fn paper_burst_buy(
        &self,
        ts: f64,
        condition_id: &str,
        outcome: &str,
        token_id: &str,
        total_shares: f64,
        reference_price: f64,
    ) -> Result<FillResult> {
        let mut remaining = total_shares;
        let max_orders = self.config.burst_max_orders.max(1);

        let chunk_unit = self.config.burst_chunk_unit.trim().to_ascii_lowercase();
        let use_best_ask = self.config.burst_use_best_ask_size;
        let stop_on_move = self.config.burst_stop_on_price_change;
        let tol = self.config.burst_price_change_tolerance.max(0.0);

        let min_chunk_shares = self.config.burst_min_chunk_shares.max(0.0);
        let max_chunk_shares = self.config.burst_max_chunk_shares.max(min_chunk_shares);
        let min_chunk_usdc = self.config.burst_min_chunk_usdc.max(0.0);
        let max_chunk_usdc = self.config.burst_max_chunk_usdc.max(min_chunk_usdc);

        let min_delay_ms = self.config.burst_min_delay_ms;
        let max_delay_ms = self.config.burst_max_delay_ms.max(min_delay_ms);

        let mut filled = 0.0;
        let mut notional = 0.0;
        let mut sent = 0u32;
        let mut available_left: Option<f64> = None;
        let mut depth_left_usdc: Option<f64> = None;
        let order_type = self.config.burst_order_type.trim().to_ascii_uppercase();

        while remaining > 0.0 && sent < max_orders {
            let pd = self.get_price(token_id).await;
            let price = pd
                .as_ref()
                .map(|p| p.best_ask)
                .filter(|p| *p > 0.0)
                .unwrap_or(reference_price);
            if price <= 0.0 {
                break;
            }

            if stop_on_move && (price - reference_price).abs() > tol {
                break;
            }

            let desired_shares = if chunk_unit == "usdc" {
                let chunk_usdc = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(min_chunk_usdc..=max_chunk_usdc)
                };
                if chunk_usdc <= 0.0 {
                    break;
                }
                chunk_usdc / price
            } else {
                let mut rng = rand::thread_rng();
                rng.gen_range(min_chunk_shares..=max_chunk_shares)
            };

            let mut slice_shares = desired_shares.min(remaining);
            if slice_shares <= 0.0 {
                break;
            }

            if use_best_ask {
                let ask_sz = pd.as_ref().map(|p| p.best_ask_size).unwrap_or(0.0);
                if ask_sz > 0.0 {
                    if self.config.burst_consume_book_in_paper {
                        available_left =
                            Some(available_left.unwrap_or(ask_sz).min(ask_sz).max(0.0));
                        slice_shares = slice_shares.min(available_left.unwrap_or(0.0));
                    } else {
                        slice_shares = slice_shares.min(ask_sz);
                    }
                }
            }

            if slice_shares <= 0.0 {
                break;
            }

            if self.config.burst_simulate_fok_by_depth {
                let depth_now = pd.as_ref().map(|p| p.ask_depth_usdc).unwrap_or(0.0);
                if depth_now > 0.0 && price > 0.0 {
                    depth_left_usdc = Some(depth_left_usdc.unwrap_or(depth_now).min(depth_now));

                    let max_shares_by_depth = depth_left_usdc.unwrap() / price;
                    if max_shares_by_depth <= 0.0 {
                        break;
                    }

                    if order_type == "FOK" {
                        if slice_shares > max_shares_by_depth + 1e-12 {
                            break;
                        }
                    } else {
                        slice_shares = slice_shares.min(max_shares_by_depth);
                    }
                }
            }

            if slice_shares <= 0.0 {
                break;
            }

            // Simulate paying per slice (more realistic if price moves).
            let slice_cost = price * slice_shares;
            if !self.wallet.debit(slice_cost).await {
                break;
            }

            filled += slice_shares;
            notional += slice_cost;
            remaining -= slice_shares;
            sent += 1;

            if let Some(left) = available_left.as_mut() {
                *left = (*left - slice_shares).max(0.0);
            }
            if self.config.burst_simulate_fok_by_depth && self.config.burst_consume_book_in_paper {
                if let Some(left) = depth_left_usdc.as_mut() {
                    *left = (*left - slice_cost).max(0.0);
                }
            }

            if self.config.burst_log_slices_in_paper {
                let balance = self.wallet.balance().await;
                self.logger
                    .append(PaperTradeRow {
                        ts,
                        action: "BUY_SLICE".to_string(),
                        condition_id: condition_id.to_string(),
                        outcome: outcome.to_string(),
                        token_id: token_id.to_string(),
                        price,
                        shares: slice_shares,
                        usdc_delta: -slice_cost,
                        balance_usdc: balance,
                        pnl_usdc: None,
                        info: format!("burst order_type={}", self.config.burst_order_type),
                    })
                    .await?;
            }

            if max_delay_ms > 0 {
                let delay_ms = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(min_delay_ms..=max_delay_ms)
                };
                if delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }

        let vwap = if filled > 0.0 {
            notional / filled
        } else {
            f64::NAN
        };

        // If we didn't log slices, log the aggregate BUY once.
        if filled > 0.0 && !self.config.burst_log_slices_in_paper {
            let balance = self.wallet.balance().await;
            self.logger
                .append(PaperTradeRow {
                    ts,
                    action: "BUY".to_string(),
                    condition_id: condition_id.to_string(),
                    outcome: outcome.to_string(),
                    token_id: token_id.to_string(),
                    price: vwap,
                    shares: filled,
                    usdc_delta: -notional,
                    balance_usdc: balance,
                    pnl_usdc: None,
                    info: format!(
                        "burst unit={} orders_sent={} order_type={}",
                        chunk_unit, sent, self.config.burst_order_type
                    ),
                })
                .await?;

            info!(
                "PAPER BUY: condition={} outcome={} price={:.4} shares={:.4} cost={:.4} balance={:.4} info=burst unit={} orders_sent={} order_type={}",
                condition_id, outcome, vwap, filled, notional, balance, chunk_unit, sent, self.config.burst_order_type
            );
        }

        Ok(FillResult {
            filled_shares: filled,
            vwap_price: vwap,
            orders_sent: sent,
            notional_usdc: notional,
        })
    }
}
