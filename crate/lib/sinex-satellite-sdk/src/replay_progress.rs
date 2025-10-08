//! Progress tracking for long-running replay operations

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Progress tracking for replay operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayProgress {
    /// Total number of events to process
    pub total_events: u64,
    /// Events processed so far
    pub processed_events: u64,
    /// Events that failed processing
    pub failed_events: u64,
    /// Events skipped (filtered out)
    pub skipped_events: u64,
    /// Number of batches processed
    pub batches_processed: usize,
    /// Total estimated batches
    pub total_batches: usize,
    /// Start time of replay
    pub start_time: DateTime<Utc>,
    /// Last update time
    pub last_update: DateTime<Utc>,
    /// Current processing rate (events/sec)
    pub current_rate: f64,
    /// Average processing rate (events/sec)
    pub average_rate: f64,
    /// Estimated time remaining
    pub eta: Option<Duration>,
    /// Current phase
    pub phase: ReplayPhase,
    /// Memory usage (bytes)
    pub memory_usage: Option<usize>,
    /// Checkpoint information
    pub checkpoint: Option<ReplayCheckpoint>,
}

/// Phases of replay operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayPhase {
    /// Initializing replay
    Initializing,
    /// Analyzing scope
    Analyzing,
    /// Processing events
    Processing,
    /// Finalizing results
    Finalizing,
    /// Completed successfully
    Completed,
    /// Failed with error
    Failed,
    /// Paused by user
    Paused,
}

/// Checkpoint for resumable operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayCheckpoint {
    /// Last successfully processed event ID
    pub last_event_id: Option<sinex_core::types::ulid::Ulid>,
    /// Offset in the query
    pub query_offset: u64,
    /// Timestamp of checkpoint
    pub timestamp: DateTime<Utc>,
    /// State data for resumption
    pub state_data: serde_json::Value,
}

/// Thread-safe progress tracker
#[derive(Clone)]
pub struct ProgressTracker {
    inner: Arc<RwLock<ReplayProgress>>,
    update_callback: Option<UpdateCallback>,
}

impl ProgressTracker {
    /// Create new progress tracker
    pub fn new(total_events: u64, total_batches: usize) -> Self {
        let now = Utc::now();
        Self {
            inner: Arc::new(RwLock::new(ReplayProgress {
                total_events,
                processed_events: 0,
                failed_events: 0,
                skipped_events: 0,
                batches_processed: 0,
                total_batches,
                start_time: now,
                last_update: now,
                current_rate: 0.0,
                average_rate: 0.0,
                eta: None,
                phase: ReplayPhase::Initializing,
                memory_usage: None,
                checkpoint: None,
            })),
            update_callback: None,
        }
    }

    /// Set callback for progress updates
    pub fn with_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(&ReplayProgress) + Send + Sync + 'static,
    {
        self.update_callback = Some(Arc::new(callback));
        self
    }

    /// Update phase
    pub async fn set_phase(&self, phase: ReplayPhase) {
        let mut progress = self.inner.write().await;
        progress.phase = phase;
        progress.last_update = Utc::now();

        if phase == ReplayPhase::Processing && progress.start_time == progress.last_update {
            // Mark actual processing start
            progress.start_time = Utc::now();
        }

        drop(progress);
        self.trigger_callback().await;
    }

    /// Update processed count
    pub async fn update_processed(&self, count: u64) {
        let mut progress = self.inner.write().await;
        let now = Utc::now();
        let time_delta = (now - progress.last_update).num_seconds() as f64;

        if time_delta > 0.0 {
            // Calculate current rate
            let events_delta = count - progress.processed_events;
            progress.current_rate = events_delta as f64 / time_delta;
        }

        progress.processed_events = count;
        progress.last_update = now;

        // Calculate average rate
        let total_time = (now - progress.start_time).num_seconds() as f64;
        if total_time > 0.0 {
            progress.average_rate = progress.processed_events as f64 / total_time;
        }

        // Calculate ETA
        if progress.average_rate > 0.0 {
            let remaining = progress.total_events - progress.processed_events;
            let seconds_remaining = remaining as f64 / progress.average_rate;
            progress.eta = Some(Duration::seconds(seconds_remaining as i64));
        }

        drop(progress);
        self.trigger_callback().await;
    }

    /// Increment processed events
    pub async fn increment_processed(&self, delta: u64) {
        let progress = self.inner.read().await;
        let new_count = progress.processed_events + delta;
        drop(progress);
        self.update_processed(new_count).await;
    }

    /// Increment failed events
    pub async fn increment_failed(&self, delta: u64) {
        let mut progress = self.inner.write().await;
        progress.failed_events += delta;
        progress.last_update = Utc::now();
        drop(progress);
        self.trigger_callback().await;
    }

    /// Increment skipped events
    pub async fn increment_skipped(&self, delta: u64) {
        let mut progress = self.inner.write().await;
        progress.skipped_events += delta;
        progress.last_update = Utc::now();
        drop(progress);
        self.trigger_callback().await;
    }

    /// Update batch count
    pub async fn complete_batch(&self) {
        let mut progress = self.inner.write().await;
        progress.batches_processed += 1;
        progress.last_update = Utc::now();

        // Log progress every 10 batches or 10%
        let should_log = progress.batches_processed.is_multiple_of(10)
            || (progress.batches_processed * 10).is_multiple_of(progress.total_batches);

        if should_log {
            let percentage =
                (progress.batches_processed as f64 / progress.total_batches as f64) * 100.0;
            info!(
                "Replay progress: {:.1}% ({}/{} batches, {} events at {:.1} events/sec)",
                percentage,
                progress.batches_processed,
                progress.total_batches,
                progress.processed_events,
                progress.current_rate
            );
        }

        drop(progress);
        self.trigger_callback().await;
    }

    /// Update memory usage
    pub async fn update_memory(&self, bytes: usize) {
        let mut progress = self.inner.write().await;
        progress.memory_usage = Some(bytes);
        drop(progress);
        self.trigger_callback().await;
    }

    /// Save checkpoint
    pub async fn save_checkpoint(
        &self,
        last_event_id: Option<sinex_core::types::ulid::Ulid>,
        offset: u64,
        state_data: serde_json::Value,
    ) {
        let mut progress = self.inner.write().await;
        progress.checkpoint = Some(ReplayCheckpoint {
            last_event_id,
            query_offset: offset,
            timestamp: Utc::now(),
            state_data,
        });
        drop(progress);
        self.trigger_callback().await;
    }

    /// Get current progress snapshot
    pub async fn get_progress(&self) -> ReplayProgress {
        self.inner.read().await.clone()
    }

    /// Get progress percentage
    pub async fn get_percentage(&self) -> f64 {
        let progress = self.inner.read().await;
        if progress.total_events == 0 {
            return 0.0;
        }
        (progress.processed_events as f64 / progress.total_events as f64) * 100.0
    }

    /// Get formatted ETA string
    pub async fn get_eta_string(&self) -> String {
        let progress = self.inner.read().await;
        match progress.eta {
            Some(duration) => {
                let hours = duration.num_hours();
                let minutes = duration.num_minutes() % 60;
                let seconds = duration.num_seconds() % 60;

                if hours > 0 {
                    format!("{}h {}m {}s", hours, minutes, seconds)
                } else if minutes > 0 {
                    format!("{}m {}s", minutes, seconds)
                } else {
                    format!("{}s", seconds)
                }
            }
            None => "calculating...".to_string(),
        }
    }

    /// Get summary statistics
    pub async fn get_summary(&self) -> ReplaySummary {
        let progress = self.inner.read().await;
        let elapsed = (progress.last_update - progress.start_time).num_seconds() as f64;

        ReplaySummary {
            total_events: progress.total_events,
            processed_events: progress.processed_events,
            failed_events: progress.failed_events,
            skipped_events: progress.skipped_events,
            success_rate: if progress.processed_events > 0 {
                ((progress.processed_events - progress.failed_events) as f64
                    / progress.processed_events as f64)
                    * 100.0
            } else {
                0.0
            },
            average_rate: progress.average_rate,
            elapsed_time: Duration::seconds(elapsed as i64),
            estimated_total_time: if progress.average_rate > 0.0 {
                Some(Duration::seconds(
                    (progress.total_events as f64 / progress.average_rate) as i64,
                ))
            } else {
                None
            },
            memory_usage_mb: progress.memory_usage.map(|b| b as f64 / 1_048_576.0),
        }
    }

    /// Trigger update callback
    async fn trigger_callback(&self) {
        if let Some(callback) = &self.update_callback {
            let progress = self.inner.read().await.clone();
            callback(&progress);
        }
    }
}

/// Summary statistics for replay operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySummary {
    pub total_events: u64,
    pub processed_events: u64,
    pub failed_events: u64,
    pub skipped_events: u64,
    pub success_rate: f64,
    pub average_rate: f64,
    pub elapsed_time: Duration,
    pub estimated_total_time: Option<Duration>,
    pub memory_usage_mb: Option<f64>,
}

impl ReplaySummary {
    /// Format as human-readable report
    pub fn format_report(&self) -> String {
        let mut report = String::new();
        report.push_str("=== Replay Operation Summary ===\n");
        report.push_str(&format!("Total Events: {}\n", self.total_events));
        report.push_str(&format!("Processed: {}\n", self.processed_events));
        report.push_str(&format!("Failed: {}\n", self.failed_events));
        report.push_str(&format!("Skipped: {}\n", self.skipped_events));
        report.push_str(&format!("Success Rate: {:.2}%\n", self.success_rate));
        report.push_str(&format!(
            "Processing Rate: {:.1} events/sec\n",
            self.average_rate
        ));
        report.push_str(&format!(
            "Elapsed Time: {}s\n",
            self.elapsed_time.num_seconds()
        ));

        if let Some(total_time) = self.estimated_total_time {
            report.push_str(&format!(
                "Estimated Total Time: {}s\n",
                total_time.num_seconds()
            ));
        }

        if let Some(mem_mb) = self.memory_usage_mb {
            report.push_str(&format!("Memory Usage: {:.2} MB\n", mem_mb));
        }

        report
    }
}

/// Progress reporter for external monitoring
pub trait ProgressReporter: Send + Sync {
    /// Report progress update
    fn report(&self, progress: &ReplayProgress);

    /// Report completion
    fn complete(&self, summary: &ReplaySummary);

    /// Report error
    fn error(&self, message: &str);
}

/// Console progress reporter
pub struct ConsoleProgressReporter;

impl ProgressReporter for ConsoleProgressReporter {
    fn report(&self, progress: &ReplayProgress) {
        debug!(
            phase = ?progress.phase,
            processed = progress.processed_events,
            total = progress.total_events,
            rate = progress.current_rate,
            "Progress update"
        );
    }

    fn complete(&self, summary: &ReplaySummary) {
        info!("{}", summary.format_report());
    }

    fn error(&self, message: &str) {
        tracing::error!("Replay error: {}", message);
    }
}
type UpdateCallback = Arc<dyn Fn(&ReplayProgress) + Send + Sync>;
