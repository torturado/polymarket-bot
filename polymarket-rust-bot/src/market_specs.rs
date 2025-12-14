use crate::types::MarketSpec;
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

pub fn load_market_specs(path: &Path) -> Result<Vec<MarketSpec>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("Failed to read MARKET_SPECS_FILE at {}", path.display()))?;
    let specs: Vec<MarketSpec> = serde_json::from_str(&raw)
        .with_context(|| format!("Failed to parse market specs JSON at {}", path.display()))?;
    Ok(specs)
}
