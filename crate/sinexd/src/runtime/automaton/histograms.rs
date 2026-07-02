//! Lightweight in-process latency / throughput histograms for automatons.
//!
//! # Why not t-digest / HDR-histogram?
//!
//! `sinex` is a single-user system with a small number of automatons
//! (currently six). The percentile signal we need is "operator-facing under
//! prod-equivalent traffic" — we are not building a high-throughput observability
//! stack, and we are not the storage layer for the percentiles (Prometheus /
//! external scrape is). A 1024-sample reservoir computed by sort-on-read is
//! 8 KiB per automaton, runs in microseconds at p50/p99 read time, and adds zero
//! per-event allocation on the write path. That is the right operating point.
//!
//! When this becomes wrong (more modules, higher rates, or external scrape
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
/// signals at typical automaton throughput while keeping the read-side
/// sort cost negligible. Public for tests and downstream tuning; the adapter
/// constructs windows with this default.
pub const DEFAULT_LATENCY_RESERVOIR: usize = 1024;

/// Throughput window length. Bigger window = more stable EPS reading, smaller
/// window = faster reaction to load spikes. One minute matches what operators
/// expect from `sinexctl runtime automata` at a glance.
pub const THROUGHPUT_WINDOW: Duration = Duration::from_mins(1);

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
        let idx = ((q * n as f64).ceil() as usize)
            .saturating_sub(1)
            .min(n - 1);
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
#[path = "histograms_test.rs"]
mod tests;
