use log::warn;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionState {
    Watching,
    EnteringLeg1,
    ScalingIn,
    Holding,
    Closing,
    Merging,
    TakeProfit,
    Unwinding,
}

#[derive(Debug, Clone)]
pub struct LegInPosition {
    pub condition_id: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub leg_1_side: String, // "YES" | "NO"
    pub leg_1_entry_price: f64,
    pub leg_1_size: f64,
    pub leg_1_filled: bool,
    pub state: PositionState,
    pub entry_time: f64,

    pub leg_2_entry_price: Option<f64>,
    pub leg_2_size: Option<f64>,
    pub leg_2_filled: bool,
    pub leg_2_pending: bool,
    pub merge_pending: bool,
    pub unwind_pending: bool,
    pub scalein_pending: bool,
    pub scalein_count: u32,
    pub scalein_next_usdc: Option<f64>,
    pub last_scalein_at: f64,
}

#[derive(Debug, Clone)]
pub struct PositionManager {
    positions: Arc<Mutex<HashMap<String, LegInPosition>>>,
}

impl PositionManager {
    pub fn new() -> Self {
        Self {
            positions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn open_position(&self, position: LegInPosition) {
        let mut positions = self.lock_positions();
        positions.entry(position.condition_id.clone()).or_insert(position);
    }

    pub fn get_position(&self, condition_id: &str) -> Option<LegInPosition> {
        self.lock_positions().get(condition_id).cloned()
    }

    pub fn close_position(&self, condition_id: &str) {
        self.lock_positions().remove(condition_id);
    }

    pub fn update_state(&self, condition_id: &str, new_state: PositionState) {
        if let Some(pos) = self.lock_positions().get_mut(condition_id) {
            pos.state = new_state;
        }
    }

    pub fn get_open_positions_count(&self) -> usize {
        self.lock_positions().len()
    }

    pub fn with_position_mut<F, R>(&self, condition_id: &str, f: F) -> Option<R>
    where
        F: FnOnce(&mut LegInPosition) -> R,
    {
        let mut positions = self.lock_positions();
        let pos = positions.get_mut(condition_id)?;
        Some(f(pos))
    }

    pub fn snapshot_positions(&self) -> Vec<LegInPosition> {
        self.lock_positions().values().cloned().collect()
    }

    fn lock_positions(&self) -> MutexGuard<'_, HashMap<String, LegInPosition>> {
        match self.positions.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!("PositionManager mutex poisoned; continuing with inner state");
                poisoned.into_inner()
            }
        }
    }
}
