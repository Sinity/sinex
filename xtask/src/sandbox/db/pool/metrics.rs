use super::stats::PoolStats;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// Pool performance metrics for monitoring
pub struct PoolMetrics {
    acquisitions: AtomicUsize,
    total_wait_time: AtomicU64,
    cleanup_failures: AtomicUsize,
    template_recreations: AtomicUsize,
}

impl PoolMetrics {
    pub(crate) fn new() -> Self {
        Self {
            acquisitions: AtomicUsize::new(0),
            total_wait_time: AtomicU64::new(0),
            cleanup_failures: AtomicUsize::new(0),
            template_recreations: AtomicUsize::new(0),
        }
    }

    pub(crate) fn record_acquisition(&self, wait_time: Duration) {
        self.acquisitions.fetch_add(1, Ordering::Relaxed);
        self.total_wait_time.fetch_add(
            wait_time.as_millis().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_cleanup_failure(&self) {
        self.cleanup_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_template_recreation(&self) {
        self.template_recreations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn get_stats(&self) -> PoolStats {
        let acquisitions = self.acquisitions.load(Ordering::Relaxed);
        let total_wait = self.total_wait_time.load(Ordering::Relaxed);

        PoolStats {
            total_acquisitions: acquisitions,
            average_wait_time_ms: if acquisitions > 0 {
                total_wait / acquisitions as u64
            } else {
                0
            },
            cleanup_failures: self.cleanup_failures.load(Ordering::Relaxed),
            template_recreations: self.template_recreations.load(Ordering::Relaxed),
            total_connections: 0,
            idle_connections: 0,
        }
    }
}

/// Global metrics instance
pub(crate) static POOL_METRICS: std::sync::LazyLock<PoolMetrics> =
    std::sync::LazyLock::new(PoolMetrics::new);
