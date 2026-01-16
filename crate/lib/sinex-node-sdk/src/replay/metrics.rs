use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct ReplayMetrics {
    start_time: Arc<Mutex<Option<Instant>>>,
    events_processed: Arc<AtomicU64>,
    events_failed: Arc<AtomicU64>,
    batches_processed: Arc<AtomicUsize>,
    bytes_processed: Arc<AtomicU64>,
    batch_times: Arc<RwLock<Vec<u64>>>,
    rate_samples: Arc<RwLock<Vec<f64>>>,
    pause_durations: Arc<RwLock<Vec<Duration>>>,
    error_counts: Arc<RwLock<HashMap<String, usize>>>,
}

impl ReplayMetrics {
    pub fn new() -> Self {
        Self {
            start_time: Arc::new(Mutex::new(None)),
            events_processed: Arc::new(AtomicU64::new(0)),
            events_failed: Arc::new(AtomicU64::new(0)),
            batches_processed: Arc::new(AtomicUsize::new(0)),
            bytes_processed: Arc::new(AtomicU64::new(0)),
            batch_times: Arc::new(RwLock::new(Vec::new())),
            rate_samples: Arc::new(RwLock::new(Vec::new())),
            pause_durations: Arc::new(RwLock::new(Vec::new())),
            error_counts: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn start(&mut self) {
        *self.start_time.lock() = Some(Instant::now());
    }

    pub fn record_batch(&self, events: u64) {
        self.events_processed.fetch_add(events, Ordering::SeqCst);
        self.batches_processed.fetch_add(1, Ordering::SeqCst);
        self.rate_samples
            .write()
            .push(events as f64 / self.elapsed().as_secs_f64().max(1.0));
    }

    pub fn record_batch_time(&self, duration: Duration) {
        self.batch_times
            .write()
            .push(duration.as_micros().try_into().unwrap_or(u64::MAX));
    }

    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_processed.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn record_error(&self, error_type: &str) {
        let mut guard = self.error_counts.write();
        *guard.entry(error_type.to_string()).or_insert(0) += 1;
        self.events_failed.fetch_add(1, Ordering::SeqCst);
    }

    pub fn record_pause(&self, duration: Duration) {
        self.pause_durations.write().push(duration);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let batch_guard = self.batch_times.read();
        let average_batch_time_micros = if batch_guard.is_empty() {
            None
        } else {
            let sum: u128 = batch_guard.iter().map(|v| *v as u128).sum();
            Some((sum / batch_guard.len() as u128) as u64)
        };

        MetricsSnapshot {
            events_processed: self.events_processed.load(Ordering::SeqCst),
            events_failed: self.events_failed.load(Ordering::SeqCst),
            batches_processed: self.batches_processed.load(Ordering::SeqCst) as u64,
            bytes_processed: self.bytes_processed.load(Ordering::SeqCst),
            average_batch_time_micros,
            pause_durations_ms: self
                .pause_durations
                .read()
                .iter()
                .map(|d| d.as_millis() as u64)
                .collect(),
            error_counts: self.error_counts.read().clone(),
        }
    }

    fn elapsed(&self) -> Duration {
        self.start_time
            .lock()
            .as_ref()
            .map(|start| start.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0))
    }
}

impl Default for ReplayMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub events_processed: u64,
    pub events_failed: u64,
    pub batches_processed: u64,
    pub bytes_processed: u64,
    pub average_batch_time_micros: Option<u64>,
    pub pause_durations_ms: Vec<u64>,
    pub error_counts: HashMap<String, usize>,
}
