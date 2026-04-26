//! Lightweight in-process latency / throughput histograms for derived nodes.
//!
//! # Why not t-digest / HDR-histogram?
//!
//! `sinex` is a single-user system with a small number of derived nodes
//! (currently six). The percentile signal we need is "operator-facing under
//! prod-equivalent traffic" — we are not building a high-throughput observability
//! stack, and we are not the storage layer for the percentiles (Prometheus /
//! external scrape is). A 1024-sample reservoir computed by sort-on-read is
//! 8 KiB per node, runs in microseconds at p50/p99 read time, and adds zero
//! per-event allocation on the write path. That is the right operating point.
//!
//! When this becomes wrong (more nodes, higher rates, or external scrape
//! cannot meet the cardinality budget), the natural successor is
//! `tdigest`/`hdrhistogram` behind the same `LatencyWindow` API.
//!
//! # Behaviour
//!
//! - `LatencyWindow` is a fixed-capacity ring buffer of `f64` samples. New
//!   samples overwrite the oldest in FIFO order. `percentile(q)` sorts the
//!   live samples and returns the nearest-rank value; non-finite samples are
//!   silently dropped at insert time so the gauge never emits NaN.
//! - `ThroughputWindow` is a sliding 1-minute event counter. `eps()` returns
//!   events per second over the current window, treating a partial window
//!   correctly (events / elapsed seconds, not events / 60).
//! - Both structs are owned by the adapter and updated only on the dispatch
//!   thread, so no synchronization is needed.

use std::time::{Duration, Instant};

/// Default sample reservoir size. 1024 samples is enough for stable p50/p99
/// signals at typical derived-node throughput while keeping the read-side
/// sort cost negligible. Public for tests and downstream tuning; the adapter
/// constructs windows with this default.
pub const DEFAULT_LATENCY_RESERVOIR: usize = 1024;

/// Throughput window length. Bigger window = more stable EPS reading, smaller
/// window = faster reaction to load spikes. One minute matches what operators
/// expect from `sinexctl automata` at a glance.
pub const THROUGHPUT_WINDOW: Duration = Duration::from_secs(60);

/// Fixed-capacity ring buffer of latency samples for percentile queries.
#[derive(Debug, Clone)]
pub struct LatencyWindow {
    samples: Vec<f64>,
    next: usize,
    capacity: usize,
}

impl LatencyWindow {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            samples: Vec::with_capacity(capacity),
            next: 0,
            capacity,
        }
    }

    /// Record one sample. Non-finite values (NaN, Inf) are dropped so the
    /// reservoir never poisons percentile output.
    pub fn record(&mut self, sample: f64) {
        if !sample.is_finite() {
            return;
        }
        if self.samples.len() < self.capacity {
            self.samples.push(sample);
        } else {
            self.samples[self.next] = sample;
            self.next = (self.next + 1) % self.capacity;
        }
    }

    /// Number of samples currently held. Tops out at `capacity`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Nearest-rank percentile of the held samples. `q` is the quantile in
    /// `[0.0, 1.0]`; values outside that range are clamped. Returns `None`
    /// when no samples have been recorded.
    #[must_use]
    pub fn percentile(&self, q: f64) -> Option<f64> {
        if self.samples.is_empty() {
            return None;
        }
        let q = q.clamp(0.0, 1.0);
        let mut sorted = self.samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        // Nearest-rank: index = ceil(q * n) - 1, clamped to [0, n-1].
        let n = sorted.len();
        let idx = ((q * n as f64).ceil() as usize).saturating_sub(1).min(n - 1);
        Some(sorted[idx])
    }
}

/// Sliding-window throughput counter (events per second).
#[derive(Debug, Clone)]
pub struct ThroughputWindow {
    window: Duration,
    samples: std::collections::VecDeque<Instant>,
}

impl ThroughputWindow {
    #[must_use]
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            samples: std::collections::VecDeque::new(),
        }
    }

    pub fn record(&mut self, now: Instant) {
        self.evict(now);
        self.samples.push_back(now);
    }

    fn evict(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.window);
        let Some(cutoff) = cutoff else {
            return;
        };
        while let Some(&front) = self.samples.front() {
            if front < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Events per second over the live window. Returns 0.0 if no samples are
    /// in the window. The denominator is the actual span between the oldest
    /// retained sample and `now`, capped at the window length, so a fresh
    /// window doesn't claim 60-second backstop throughput.
    #[must_use]
    pub fn eps(&mut self, now: Instant) -> f64 {
        self.evict(now);
        match self.samples.front() {
            None => 0.0,
            Some(&oldest) => {
                let span = now.saturating_duration_since(oldest);
                let span = span.min(self.window);
                let secs = span.as_secs_f64().max(0.001); // avoid div-by-zero
                self.samples.len() as f64 / secs
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_window_percentile_on_uniform_distribution() {
        let mut win = LatencyWindow::new(1024);
        for i in 0..1000 {
            win.record(i as f64);
        }
        // Nearest-rank p50 of 0..999 is at index ceil(0.5 * 1000) - 1 = 499 → 499.0.
        assert_eq!(win.percentile(0.5), Some(499.0));
        // p99 nearest-rank is index 989 → 989.0.
        assert_eq!(win.percentile(0.99), Some(989.0));
        // p100 is the max.
        assert_eq!(win.percentile(1.0), Some(999.0));
    }

    #[test]
    fn latency_window_overwrites_oldest_when_full() {
        let mut win = LatencyWindow::new(4);
        for i in 0..6 {
            win.record(i as f64);
        }
        // Reservoir should hold {2,3,4,5} (in some order).
        assert_eq!(win.len(), 4);
        let mut sorted = win.samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(sorted, vec![2.0, 3.0, 4.0, 5.0]);
    }

    #[test]
    fn latency_window_drops_non_finite() {
        let mut win = LatencyWindow::new(8);
        win.record(1.0);
        win.record(f64::NAN);
        win.record(f64::INFINITY);
        win.record(2.0);
        assert_eq!(win.len(), 2);
        assert_eq!(win.percentile(0.5), Some(1.0));
    }

    #[test]
    fn latency_window_empty_returns_none() {
        let win = LatencyWindow::new(8);
        assert_eq!(win.percentile(0.5), None);
    }

    #[test]
    fn throughput_window_eps_uses_live_span_not_window_length() {
        let mut tp = ThroughputWindow::new(Duration::from_secs(60));
        let t0 = Instant::now();
        // Record 5 events spread over 100 ms — should report ~50 eps, not
        // 5 / 60 ≈ 0.083 eps.
        tp.record(t0);
        tp.record(t0 + Duration::from_millis(25));
        tp.record(t0 + Duration::from_millis(50));
        tp.record(t0 + Duration::from_millis(75));
        tp.record(t0 + Duration::from_millis(100));
        let eps = tp.eps(t0 + Duration::from_millis(100));
        assert!(
            eps > 40.0 && eps < 60.0,
            "expected ~50 eps over 100ms, got {eps}"
        );
    }

    #[test]
    fn throughput_window_evicts_stale_samples() {
        let mut tp = ThroughputWindow::new(Duration::from_secs(60));
        let t0 = Instant::now();
        tp.record(t0);
        tp.record(t0 + Duration::from_secs(1));
        // 90 seconds later — both samples should evict.
        let later = t0 + Duration::from_secs(90);
        let eps = tp.eps(later);
        assert_eq!(eps, 0.0, "stale samples must evict from the window");
    }
}
