//! Live progress reporting for paced historical/catch-up scans (sinex-2n9).
//!
//! [`ScanPacer`](crate::runtime::pacing::ScanPacer) enforces the rate budget;
//! this module is the OBSERVABILITY half — it publishes a small live snapshot
//! (rate, position, ETA, backlog) to a NATS KV bucket after each paced batch,
//! and `sinexctl ops import list` reads it back via the
//! `sources.import_progress` RPC. Entries are transient: they are cleared
//! when the scan completes and are treated as stale (filtered out on read)
//! after `STALE_AFTER` with no update, so a crashed scan does not leave a
//! phantom "in progress" entry forever.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::nats::create_or_open_kv_store;
use sinex_primitives::rpc::sources::ImportProgressEntry;
use sinex_primitives::temporal::Timestamp;

use crate::runtime::RuntimeResult;
use crate::runtime::pacing::PacingController;

/// How long a progress entry is trusted without a fresh update before
/// `list()` treats it as stale (scan process likely crashed without
/// clearing its entry) and omits it.
pub const STALE_AFTER: Duration = Duration::from_secs(60);

/// How often the scan loop should publish a fresh snapshot. Matches the
/// existing streaming-checkpoint persist cadence convention elsewhere in
/// `adapter_source.rs` (avoids a KV write on every single batch).
pub const PUBLISH_INTERVAL: Duration = Duration::from_secs(2);

/// Wire-format snapshot stored in the KV bucket. Mirrors
/// [`ImportProgressEntry`] but keeps `Timestamp` typed internally; converted
/// to the RPC-facing string-timestamp shape at the read boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanProgressSnapshot {
    pub module_name: String,
    pub started_at: Timestamp,
    pub updated_at: Timestamp,
    pub events_processed: u64,
    pub bytes_processed: u64,
    pub rate_events_per_sec: f64,
    pub rate_bytes_per_sec: f64,
    pub events_per_sec_budget: Option<f64>,
    pub bytes_per_sec_budget: Option<f64>,
    pub position: Option<Timestamp>,
    pub horizon: Option<Timestamp>,
    pub eta_seconds: Option<f64>,
    pub backlog_pending: Option<u64>,
    pub paced: bool,
}

impl ScanProgressSnapshot {
    /// Build a snapshot from a [`PacingController`]'s current counters plus
    /// a [`ScanProgressTracker`]'s position/horizon/ETA (the controller only
    /// knows event/byte counters; the tracker owns the record-timestamp
    /// anchor a `RateBudget`-agnostic ETA estimate needs).
    #[must_use]
    pub fn from_controller(
        module_name: &str,
        started_at: Timestamp,
        controller: &PacingController,
        tracker: &ScanProgressTracker,
        backlog_pending: Option<u64>,
    ) -> Self {
        let budget = controller.budget();
        Self {
            module_name: module_name.to_string(),
            started_at,
            updated_at: Timestamp::now(),
            events_processed: controller.events_total(),
            bytes_processed: controller.bytes_total(),
            rate_events_per_sec: controller.rate_events_per_sec(),
            rate_bytes_per_sec: controller.rate_bytes_per_sec(),
            events_per_sec_budget: budget.events_per_sec,
            bytes_per_sec_budget: budget.bytes_per_sec,
            position: tracker.last_position(),
            horizon: tracker.horizon(),
            eta_seconds: tracker.eta_seconds(controller.elapsed()),
            backlog_pending,
            paced: !budget.is_unlimited(),
        }
    }

    #[must_use]
    pub fn is_stale(&self, now: Timestamp) -> bool {
        let age = now - self.updated_at;
        age.as_seconds_f64() > STALE_AFTER.as_secs_f64()
    }

    #[must_use]
    pub fn into_rpc_entry(self) -> ImportProgressEntry {
        ImportProgressEntry {
            module_name: self.module_name,
            started_at: self.started_at.format_rfc3339(),
            updated_at: self.updated_at.format_rfc3339(),
            events_processed: self.events_processed,
            bytes_processed: self.bytes_processed,
            rate_events_per_sec: self.rate_events_per_sec,
            rate_bytes_per_sec: self.rate_bytes_per_sec,
            events_per_sec_budget: self.events_per_sec_budget,
            bytes_per_sec_budget: self.bytes_per_sec_budget,
            position: self.position.map(|ts| ts.format_rfc3339()),
            horizon: self.horizon.map(|ts| ts.format_rfc3339()),
            eta_seconds: self.eta_seconds,
            backlog_pending: self.backlog_pending,
            paced: self.paced,
        }
    }
}

/// Tracks position (first/last observed record timestamp) across a scan so
/// [`eta_seconds`]-style estimation has the start-position anchor it needs.
/// Owned by the scan loop (`drain_adapter`), not by [`PacingController`],
/// since only the caller knows what a "record timestamp" means for its
/// adapter.
#[derive(Debug, Default, Clone)]
pub struct ScanProgressTracker {
    started_position: Option<Timestamp>,
    last_position: Option<Timestamp>,
    horizon: Option<Timestamp>,
}

impl ScanProgressTracker {
    #[must_use]
    pub fn new(horizon: Option<Timestamp>) -> Self {
        Self {
            started_position: None,
            last_position: None,
            horizon,
        }
    }

    pub fn observe(&mut self, position: Option<Timestamp>) {
        let Some(position) = position else { return };
        if self.started_position.is_none() {
            self.started_position = Some(position);
        }
        self.last_position = Some(position);
    }

    #[must_use]
    pub fn last_position(&self) -> Option<Timestamp> {
        self.last_position
    }

    #[must_use]
    pub fn horizon(&self) -> Option<Timestamp> {
        self.horizon
    }

    /// ETA using the actual start-position anchor: historical-seconds
    /// covered (`last - started`) divided by wall-seconds elapsed, projected
    /// across the remaining historical-seconds to `horizon`.
    #[must_use]
    pub fn eta_seconds(&self, wall_elapsed: Duration) -> Option<f64> {
        let started = self.started_position?;
        let last = self.last_position?;
        let horizon = self.horizon?;
        let elapsed_wall = wall_elapsed.as_secs_f64();
        if elapsed_wall <= 0.0 {
            return None;
        }
        let covered = (last - started).as_seconds_f64();
        if covered <= 0.0 {
            return None;
        }
        let remaining = (horizon - last).as_seconds_f64();
        if remaining <= 0.0 {
            return Some(0.0);
        }
        let historical_seconds_per_wall_second = covered / elapsed_wall;
        if historical_seconds_per_wall_second <= 0.0 {
            return None;
        }
        Some(remaining / historical_seconds_per_wall_second)
    }
}

/// NATS KV-backed store for live scan progress snapshots.
pub struct ScanProgressStore {
    kv: async_nats::jetstream::kv::Store,
}

impl ScanProgressStore {
    /// Open (creating if necessary) the shared scan-progress KV bucket.
    pub async fn open(
        nats_client: &async_nats::Client,
        env: &SinexEnvironment,
        namespace: Option<&str>,
    ) -> RuntimeResult<Self> {
        let js = async_nats::jetstream::new(nats_client.clone());
        let bucket = format!(
            "KV_{}",
            env.nats_kv_bucket_with_namespace(namespace, "sinex_scan_progress")
        );
        let kv = create_or_open_kv_store(
            &js,
            async_nats::jetstream::kv::Config {
                bucket,
                max_age: STALE_AFTER.saturating_mul(4),
                ..Default::default()
            },
        )
        .await?;
        Ok(Self { kv })
    }

    fn key(module_name: &str) -> String {
        module_name
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect()
    }

    /// Publish (overwrite) the current snapshot for a module.
    pub async fn publish(&self, snapshot: &ScanProgressSnapshot) -> RuntimeResult<()> {
        let key = Self::key(&snapshot.module_name);
        let payload = serde_json::to_vec(snapshot).map_err(sinex_primitives::SinexError::from)?;
        self.kv.put(key, payload.into()).await.map_err(|error| {
            sinex_primitives::SinexError::kv("Failed to publish scan progress snapshot")
                .with_std_error(&error)
        })?;
        Ok(())
    }

    /// Remove a module's entry (called when a scan completes or errors).
    /// Best-effort: callers should not fail the scan over a delete error.
    pub async fn clear(&self, module_name: &str) -> RuntimeResult<()> {
        let key = Self::key(module_name);
        match self.kv.delete(key).await {
            Ok(()) => Ok(()),
            Err(error) => Err(sinex_primitives::SinexError::kv(
                "Failed to clear scan progress snapshot",
            )
            .with_std_error(&error)),
        }
    }

    /// List all non-stale in-flight scan progress snapshots.
    pub async fn list(&self) -> RuntimeResult<Vec<ScanProgressSnapshot>> {
        let mut keys = self.kv.keys().await.map_err(|error| {
            sinex_primitives::SinexError::kv("Failed to list scan progress keys")
                .with_std_error(&error)
        })?;

        let mut snapshots = Vec::new();
        let now = Timestamp::now();
        use futures::StreamExt;
        while let Some(key) = keys.next().await {
            let Ok(key) = key else { continue };
            let Ok(Some(entry)) = self.kv.get(&key).await else {
                continue;
            };
            let Ok(snapshot) = serde_json::from_slice::<ScanProgressSnapshot>(&entry) else {
                continue;
            };
            if !snapshot.is_stale(now) {
                snapshots.push(snapshot);
            }
        }
        Ok(snapshots)
    }
}

#[cfg(test)]
#[path = "scan_progress_test.rs"]
mod tests;
