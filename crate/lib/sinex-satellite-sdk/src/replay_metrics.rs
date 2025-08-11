//! Metrics collection for replay operations

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Metrics collector for replay operations
#[derive(Clone)]
pub struct ReplayMetrics {
    /// Start time of replay
    start_time: Arc<Option<Instant>>,

    /// Total events processed
    events_processed: Arc<AtomicU64>,

    /// Total events failed
    events_failed: Arc<AtomicU64>,

    /// Total batches processed
    batches_processed: Arc<AtomicUsize>,

    /// Total bytes processed
    bytes_processed: Arc<AtomicU64>,

    /// Processing time per batch (in microseconds)
    batch_times: Arc<parking_lot::RwLock<Vec<u64>>>,

    /// Event processing rate samples (events per second)
    rate_samples: Arc<parking_lot::RwLock<Vec<f64>>>,

    /// Pause durations
    pause_durations: Arc<parking_lot::RwLock<Vec<Duration>>>,

    /// Error counts by type
    error_counts: Arc<parking_lot::RwLock<HashMap<String, usize>>>,
}

impl ReplayMetrics {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self {
            start_time: Arc::new(None),
            events_processed: Arc::new(AtomicU64::new(0)),
            events_failed: Arc::new(AtomicU64::new(0)),
            batches_processed: Arc::new(AtomicUsize::new(0)),
            bytes_processed: Arc::new(AtomicU64::new(0)),
            batch_times: Arc::new(parking_lot::RwLock::new(Vec::new())),
            rate_samples: Arc::new(parking_lot::RwLock::new(Vec::new())),
            pause_durations: Arc::new(parking_lot::RwLock::new(Vec::new())),
            error_counts: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    /// Start metrics collection
    pub fn start(&mut self) {
        self.start_time = Arc::new(Some(Instant::now()));
    }

    /// Record batch processing
    pub fn record_batch(&self, event_count: u64, batch_time: Duration, bytes: u64) {
        self.events_processed
            .fetch_add(event_count, Ordering::Relaxed);
        self.batches_processed.fetch_add(1, Ordering::Relaxed);
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);

        // Record batch time in microseconds
        self.batch_times.write().push(batch_time.as_micros() as u64);

        // Calculate and record processing rate
        if batch_time.as_secs_f64() > 0.0 {
            let rate = event_count as f64 / batch_time.as_secs_f64();
            self.rate_samples.write().push(rate);
        }
    }

    /// Record failed events
    pub fn record_failures(&self, count: u64) {
        self.events_failed.fetch_add(count, Ordering::Relaxed);
    }

    /// Record an error
    pub fn record_error(&self, error_type: &str) {
        let mut errors = self.error_counts.write();
        *errors.entry(error_type.to_string()).or_insert(0) += 1;
    }

    /// Record a pause duration
    pub fn record_pause(&self, duration: Duration) {
        self.pause_durations.write().push(duration);
    }

    /// Get current metrics snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        let elapsed = self
            .start_time
            .as_ref()
            .and_then(|t| t.as_ref().map(|start| start.elapsed()));

        let events_processed = self.events_processed.load(Ordering::Relaxed);
        let events_failed = self.events_failed.load(Ordering::Relaxed);
        let batches_processed = self.batches_processed.load(Ordering::Relaxed);
        let bytes_processed = self.bytes_processed.load(Ordering::Relaxed);

        let batch_times = self.batch_times.read();
        let rate_samples = self.rate_samples.read();
        let pause_durations = self.pause_durations.read();

        // Calculate statistics
        let avg_batch_time = if !batch_times.is_empty() {
            Some(Duration::from_micros(
                batch_times.iter().sum::<u64>() / batch_times.len() as u64,
            ))
        } else {
            None
        };

        let avg_rate = if !rate_samples.is_empty() {
            Some(rate_samples.iter().sum::<f64>() / rate_samples.len() as f64)
        } else {
            None
        };

        let max_rate = rate_samples.iter().cloned().fold(0.0, f64::max);
        let min_rate = if !rate_samples.is_empty() {
            rate_samples.iter().cloned().fold(f64::INFINITY, f64::min)
        } else {
            0.0
        };

        let total_pause_time = pause_durations.iter().sum::<Duration>();

        let overall_rate = if let Some(elapsed) = elapsed {
            let effective_time = elapsed.saturating_sub(total_pause_time);
            if effective_time.as_secs_f64() > 0.0 {
                Some(events_processed as f64 / effective_time.as_secs_f64())
            } else {
                None
            }
        } else {
            None
        };

        MetricsSnapshot {
            captured_at: Utc::now(),
            elapsed_time: elapsed,
            events_processed,
            events_failed,
            batches_processed,
            bytes_processed,
            avg_batch_time,
            avg_rate,
            max_rate,
            min_rate,
            overall_rate,
            total_pause_time,
            pause_count: pause_durations.len(),
            error_counts: self.error_counts.read().clone(),
        }
    }

    /// Reset all metrics
    pub fn reset(&mut self) {
        self.events_processed.store(0, Ordering::Relaxed);
        self.events_failed.store(0, Ordering::Relaxed);
        self.batches_processed.store(0, Ordering::Relaxed);
        self.bytes_processed.store(0, Ordering::Relaxed);
        self.batch_times.write().clear();
        self.rate_samples.write().clear();
        self.pause_durations.write().clear();
        self.error_counts.write().clear();
        self.start_time = Arc::new(None);
    }
}

impl Default for ReplayMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of replay metrics at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// When this snapshot was taken
    pub captured_at: DateTime<Utc>,

    /// Total elapsed time (excluding pauses)
    pub elapsed_time: Option<Duration>,

    /// Total events processed
    pub events_processed: u64,

    /// Total events that failed
    pub events_failed: u64,

    /// Total batches processed
    pub batches_processed: usize,

    /// Total bytes processed
    pub bytes_processed: u64,

    /// Average time per batch
    pub avg_batch_time: Option<Duration>,

    /// Average processing rate (events/sec)
    pub avg_rate: Option<f64>,

    /// Maximum processing rate observed
    pub max_rate: f64,

    /// Minimum processing rate observed
    pub min_rate: f64,

    /// Overall processing rate (total events / effective time)
    pub overall_rate: Option<f64>,

    /// Total time spent paused
    pub total_pause_time: Duration,

    /// Number of times replay was paused
    pub pause_count: usize,

    /// Error counts by type
    pub error_counts: HashMap<String, usize>,
}

impl MetricsSnapshot {
    /// Format metrics as a human-readable report
    pub fn format_report(&self) -> String {
        let mut report = String::new();

        report.push_str("=== Replay Metrics Report ===\n");
        report.push_str(&format!("Captured at: {}\n", self.captured_at));

        if let Some(elapsed) = self.elapsed_time {
            report.push_str(&format!("Elapsed time: {:.2?}\n", elapsed));
        }

        report.push_str(&format!("\nProcessing:\n"));
        report.push_str(&format!("  Events processed: {}\n", self.events_processed));
        report.push_str(&format!("  Events failed: {}\n", self.events_failed));
        report.push_str(&format!(
            "  Batches processed: {}\n",
            self.batches_processed
        ));
        report.push_str(&format!(
            "  Bytes processed: {:.2} MB\n",
            self.bytes_processed as f64 / 1_048_576.0
        ));

        if let Some(avg_batch_time) = self.avg_batch_time {
            report.push_str(&format!("\nPerformance:\n"));
            report.push_str(&format!("  Avg batch time: {:.2?}\n", avg_batch_time));
        }

        if let Some(avg_rate) = self.avg_rate {
            report.push_str(&format!("  Avg rate: {:.2} events/sec\n", avg_rate));
        }

        if self.max_rate > 0.0 {
            report.push_str(&format!("  Max rate: {:.2} events/sec\n", self.max_rate));
            report.push_str(&format!("  Min rate: {:.2} events/sec\n", self.min_rate));
        }

        if let Some(overall_rate) = self.overall_rate {
            report.push_str(&format!("  Overall rate: {:.2} events/sec\n", overall_rate));
        }

        if self.pause_count > 0 {
            report.push_str(&format!("\nPauses:\n"));
            report.push_str(&format!("  Pause count: {}\n", self.pause_count));
            report.push_str(&format!(
                "  Total pause time: {:.2?}\n",
                self.total_pause_time
            ));
        }

        if !self.error_counts.is_empty() {
            report.push_str(&format!("\nErrors:\n"));
            for (error_type, count) in &self.error_counts {
                report.push_str(&format!("  {}: {}\n", error_type, count));
            }
        }

        report
    }

    /// Calculate success rate as percentage
    pub fn success_rate(&self) -> f64 {
        let total = self.events_processed + self.events_failed;
        if total > 0 {
            (self.events_processed as f64 / total as f64) * 100.0
        } else {
            100.0
        }
    }

    /// Get throughput in MB/sec
    pub fn throughput_mbps(&self) -> Option<f64> {
        if let Some(elapsed) = self.elapsed_time {
            let effective_time = elapsed.saturating_sub(self.total_pause_time);
            if effective_time.as_secs_f64() > 0.0 {
                Some(self.bytes_processed as f64 / 1_048_576.0 / effective_time.as_secs_f64())
            } else {
                None
            }
        } else {
            None
        }
    }
}
