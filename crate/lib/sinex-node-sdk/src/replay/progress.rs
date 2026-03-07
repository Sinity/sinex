use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::{Duration, Timestamp};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Callback invoked on replay progress updates.
type ProgressCallback = Arc<dyn Fn(&ReplayProgress) + Send + Sync + 'static>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayProgress {
    pub total_events: u64,
    pub processed_events: u64,
    pub failed_events: u64,
    pub skipped_events: u64,
    pub batches_processed: usize,
    pub total_batches: usize,
    pub start_time: Timestamp,
    pub last_update: Timestamp,
    pub current_rate: f64,
    pub average_rate: f64,
    pub beta: Option<Duration>,
    pub phase: ReplayPhase,
    pub memory_usage: Option<usize>,
    pub checkpoint: Option<ReplayCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayCheckpoint {
    pub last_event_id: Option<String>,
    pub offset: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReplayPhase {
    Initializing,
    Processing,
    Completed,
    Failed,
}

#[derive(Clone)]
pub struct ProgressTracker {
    state: Arc<RwLock<ReplayProgress>>,
    callback: Option<ProgressCallback>,
}

impl ProgressTracker {
    #[must_use]
    pub fn new(total_events: u64, total_batches: usize) -> Self {
        let progress = ReplayProgress {
            total_events,
            processed_events: 0,
            failed_events: 0,
            skipped_events: 0,
            batches_processed: 0,
            total_batches,
            start_time: Timestamp::now(),
            last_update: Timestamp::now(),
            current_rate: 0.0,
            average_rate: 0.0,
            beta: None,
            phase: ReplayPhase::Initializing,
            memory_usage: None,
            checkpoint: None,
        };

        Self {
            state: Arc::new(RwLock::new(progress)),
            callback: None,
        }
    }

    pub fn with_callback(
        mut self,
        callback: impl Fn(&ReplayProgress) + Send + Sync + 'static,
    ) -> Self {
        self.callback = Some(Arc::new(callback));
        self
    }

    pub async fn set_phase(&mut self, phase: ReplayPhase) {
        let mut guard = self.state.write().await;
        guard.phase = phase;
        guard.last_update = Timestamp::now();
        drop(guard);
        self.invoke_callback().await;
    }

    pub async fn update(&mut self, processed: u64) {
        let mut guard = self.state.write().await;
        guard.processed_events += processed;
        guard.batches_processed += 1;
        guard.last_update = Timestamp::now();

        let elapsed = guard.last_update - guard.start_time;
        guard.current_rate = processed as f64 / elapsed.as_seconds_f64().max(1.0);
        guard.average_rate = guard.processed_events as f64 / elapsed.as_seconds_f64().max(1.0);

        if guard.total_events > 0 {
            let remaining = guard.total_events.saturating_sub(guard.processed_events) as f64;
            let estimate = remaining / guard.average_rate.max(0.01);
            if estimate.is_finite() && estimate >= 0.0 && estimate <= i64::MAX as f64 {
                guard.beta = Some(Duration::seconds_f64(estimate));
            } else {
                guard.beta = None;
            }
        }
        drop(guard);
        self.invoke_callback().await;
    }

    pub async fn record_failure(&mut self, count: u64) {
        let mut guard = self.state.write().await;
        guard.failed_events += count;
        guard.last_update = Timestamp::now();
        drop(guard);
        self.invoke_callback().await;
    }

    pub async fn set_checkpoint(&mut self, checkpoint: ReplayCheckpoint) {
        let mut guard = self.state.write().await;
        guard.checkpoint = Some(checkpoint);
        guard.last_update = Timestamp::now();
        drop(guard);
        self.invoke_callback().await;
    }

    async fn invoke_callback(&self) {
        if let Some(callback) = &self.callback {
            let snapshot = self.state.read().await.clone();
            callback(&snapshot);
        }
    }

    pub async fn summary(&self) -> ReplaySummary {
        let guard = self.state.read().await;
        ReplaySummary {
            processed_events: guard.processed_events,
            failed_events: guard.failed_events,
            skipped_events: guard.skipped_events,
            total_batches: guard.total_batches,
            batches_processed: guard.batches_processed,
            total_events: guard.total_events,
            start_time: guard.start_time,
            end_time: guard.last_update,
            phase: guard.phase,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplaySummary {
    pub processed_events: u64,
    pub failed_events: u64,
    pub skipped_events: u64,
    pub total_batches: usize,
    pub batches_processed: usize,
    pub total_events: u64,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub phase: ReplayPhase,
}

impl ReplaySummary {
    pub fn format_report(&self) -> String {
        format!(
            "Replay summary: processed={} failed={} skipped={} batches={}/{} phase={:?}",
            self.processed_events,
            self.failed_events,
            self.skipped_events,
            self.batches_processed,
            self.total_batches,
            self.phase
        )
    }
}
