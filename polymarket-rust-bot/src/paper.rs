use anyhow::{Context, Result};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct PaperWallet {
    balance: Mutex<f64>,
}

impl PaperWallet {
    pub fn new(initial_balance: f64) -> Self {
        Self {
            balance: Mutex::new(initial_balance.max(0.0)),
        }
    }

    pub async fn balance(&self) -> f64 {
        *self.balance.lock().await
    }

    pub async fn debit(&self, amount_usdc: f64) -> bool {
        let amt = amount_usdc.max(0.0);
        if amt <= 0.0 {
            return true;
        }
        let mut bal = self.balance.lock().await;
        if *bal + 1e-12 < amt {
            return false;
        }
        *bal -= amt;
        true
    }

    pub async fn credit(&self, amount_usdc: f64) {
        let amt = amount_usdc.max(0.0);
        if amt <= 0.0 {
            return;
        }
        let mut bal = self.balance.lock().await;
        *bal += amt;
    }
}

#[derive(Debug, Clone)]
pub struct PaperTradeRow {
    pub ts: f64,
    pub action: String,
    pub condition_id: String,
    pub outcome: String,  // YES | NO | (empty)
    pub token_id: String, // asset id (empty for merge)
    pub price: f64,
    pub shares: f64,
    pub usdc_delta: f64,
    pub balance_usdc: f64,
    pub pnl_usdc: Option<f64>,
    pub info: String,
}

impl PaperTradeRow {
    pub fn header() -> &'static str {
        "ts,action,condition_id,outcome,token_id,price,shares,usdc_delta,balance_usdc,pnl_usdc,info"
    }

    pub fn to_csv_line(&self) -> String {
        let pnl = self.pnl_usdc.map(fmt_f64).unwrap_or_else(|| "".to_string());

        format!(
            "{},{},{},{},{},{},{},{},{},{},{}",
            fmt_f64(self.ts),
            csv_escape(&self.action),
            csv_escape(&self.condition_id),
            csv_escape(&self.outcome),
            csv_escape(&self.token_id),
            fmt_f64(self.price),
            fmt_f64(self.shares),
            fmt_f64(self.usdc_delta),
            fmt_f64(self.balance_usdc),
            pnl,
            csv_escape(&self.info),
        )
    }
}

#[derive(Debug)]
pub struct PaperTradeLogger {
    path: PathBuf,
    lock: Mutex<()>,
}

impl PaperTradeLogger {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            lock: Mutex::new(()),
        }
    }

    pub async fn append(&self, row: PaperTradeRow) -> Result<()> {
        let _guard = self.lock.lock().await;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "Failed to create paper trade log dir at {}",
                        parent.display()
                    )
                })?;
            }

            let mut f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("Failed to open paper trade log at {}", path.display()))?;

            let needs_header = f.metadata().map(|m| m.len() == 0).unwrap_or(true);
            if needs_header {
                writeln!(f, "{}", PaperTradeRow::header())
                    .context("Failed to write paper trade log header")?;
            }

            writeln!(f, "{}", row.to_csv_line()).context("Failed to write paper trade row")?;
            Ok(())
        })
        .await
        .context("PaperTradeLogger task join failed")??;
        Ok(())
    }
}

fn fmt_f64(v: f64) -> String {
    if !v.is_finite() {
        return "".to_string();
    }
    format!("{:.8}", v)
}

fn csv_escape(s: &str) -> String {
    let s = s.trim();
    if s.is_empty() {
        return "".to_string();
    }
    let needs_quotes = s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r');
    if !needs_quotes {
        return s.to_string();
    }
    let escaped = s.replace('"', "\"\"");
    format!("\"{escaped}\"")
}
