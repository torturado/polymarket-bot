mod config;
mod execution;
mod market_specs;
mod monitor;
mod paper;
mod position_manager;
mod strategy;
mod types;

use anyhow::Result;
use env_logger::Env;
use log::warn;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

#[tokio::main]
async fn main() -> Result<()> {
    if dotenvy::dotenv().is_err() {
        dotenvy::from_path("../.env").ok();
    }

    let default_log = std::env::var("LOG_LEVEL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "INFO".to_string());
    let default_log = default_log.to_ascii_lowercase();
    env_logger::Builder::from_env(Env::default().default_filter_or(default_log)).init();

    let config = Arc::new(config::Config::from_env()?);
    let specs = market_specs::load_market_specs(&config.market_specs_path)?;
    let (spec_tx, spec_rx) = watch::channel(specs);

    let (tx, rx) = mpsc::channel::<types::MarketUpdate>(2048);

    let monitor_cfg = Arc::clone(&config);
    let strategy_cfg = Arc::clone(&config);
    let position_manager = Arc::new(position_manager::PositionManager::new());
    let monitor_positions = Arc::clone(&position_manager);

    let watcher_cfg = Arc::clone(&config);
    tokio::spawn(async move {
        if let Err(e) = monitor::run_market_specs_watcher(watcher_cfg, spec_tx).await {
            warn!("market specs watcher stopped: {e:#}");
        }
    });

    tokio::spawn(async move {
        if let Err(e) = monitor::run_ws_monitor(monitor_cfg, spec_rx, tx, monitor_positions).await
        {
            warn!("WS monitor stopped: {e:#}");
        }
    });
    strategy::run(rx, strategy_cfg, position_manager).await?;

    Ok(())
}
