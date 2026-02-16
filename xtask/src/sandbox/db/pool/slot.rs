//! Database slot management for the test pool.

use parking_lot::Mutex;
use std::sync::atomic::AtomicBool;
use time::OffsetDateTime;

/// Health snapshot from a database slot: (last_clean_time, last_clean_result, last_residuals).
pub(super) type SlotHealthSnapshot = (
    Option<OffsetDateTime>,
    Option<String>,
    Option<Vec<(String, i64)>>,
);

/// A slot in the database pool
#[derive(Debug)]
pub(super) struct DatabaseSlot {
    pub(super) name: String,
    pub(super) url: String, // Store URL instead of pool to create fresh connections
    pub(super) pool: Mutex<Option<sinex_db::DbPool>>, // Current pool if in use
    pub(super) in_use: AtomicBool,
    pub(super) quarantined: AtomicBool,
    // Track when the slot was released for cooldown
    pub(super) last_released: Mutex<Option<std::time::Instant>>,
    // Track last cleanup outcome for diagnostics
    pub(super) last_clean_time: Mutex<Option<OffsetDateTime>>,
    pub(super) last_clean_result: Mutex<Option<String>>,
    pub(super) last_residuals: Mutex<Option<Vec<(String, i64)>>>,
}

impl DatabaseSlot {
    pub(super) fn record_clean_result(
        &self,
        result: std::result::Result<(), String>,
        residuals: Option<Vec<(String, i64)>>,
    ) {
        let now = OffsetDateTime::now_utc();
        {
            let mut time_guard = self.last_clean_time.lock();
            *time_guard = Some(now);
        }
        match result {
            Ok(()) => {
                let mut res_guard = self.last_clean_result.lock();
                *res_guard = Some("ok".to_string());
                let mut residual_guard = self.last_residuals.lock();
                *residual_guard = residuals;
            }
            Err(e) => {
                let mut res_guard = self.last_clean_result.lock();
                *res_guard = Some(format!("err: {e}"));
                let mut residual_guard = self.last_residuals.lock();
                *residual_guard = residuals;
            }
        }
    }

    pub(super) fn slot_health_snapshot(&self) -> SlotHealthSnapshot {
        let time = *self.last_clean_time.lock();
        let result = self.last_clean_result.lock().clone();
        let residuals = self.last_residuals.lock().clone();
        (time, result, residuals)
    }
}
