//! Consumer counters and periodic stats logging.

use std::sync::atomic::{AtomicU64, Ordering};
use tracing::info;

#[derive(Debug, Default)]
pub(super) struct ConsumerStats {
    pub(super) events_processed: AtomicU64,
    pub(super) events_failed: AtomicU64,
    pub(super) events_deferred: AtomicU64,
    pub(super) suspicious_future_ts_orig: AtomicU64,
    pub(super) suspicious_past_ts_orig: AtomicU64,
    pub(super) negative_anchor_byte: AtomicU64,
    pub(super) validation_failures: AtomicU64,
    pub(super) tombstoned_events_rejected: AtomicU64,
    pub(super) dlq_routed: AtomicU64,
    pub(super) confirmation_failures: AtomicU64,
    pub(super) confirmation_retries_enqueued: AtomicU64,
    pub(super) confirmation_retry_failures: AtomicU64,
    pub(super) confirmation_durability_gaps: AtomicU64,
    pub(super) dlq_publish_failures: AtomicU64,
    pub(super) nack_failures: AtomicU64,
    pub(super) nats_errors: AtomicU64,
    pub(super) telemetry_publish_failures: AtomicU64,
}

impl ConsumerStats {
    pub(super) fn log(&self) {
        info!(
            events_processed = self.events_processed.load(Ordering::Relaxed),
            events_failed = self.events_failed.load(Ordering::Relaxed),
            events_deferred = self.events_deferred.load(Ordering::Relaxed),
            suspicious_future_ts_orig = self.suspicious_future_ts_orig.load(Ordering::Relaxed),
            suspicious_past_ts_orig = self.suspicious_past_ts_orig.load(Ordering::Relaxed),
            negative_anchor_byte = self.negative_anchor_byte.load(Ordering::Relaxed),
            validation_failures = self.validation_failures.load(Ordering::Relaxed),
            tombstoned_events_rejected = self.tombstoned_events_rejected.load(Ordering::Relaxed),
            nats_errors = self.nats_errors.load(Ordering::Relaxed),
            dlq_routed = self.dlq_routed.load(Ordering::Relaxed),
            confirmation_failures = self.confirmation_failures.load(Ordering::Relaxed),
            confirmation_retries_enqueued =
                self.confirmation_retries_enqueued.load(Ordering::Relaxed),
            confirmation_retry_failures = self.confirmation_retry_failures.load(Ordering::Relaxed),
            confirmation_durability_gaps =
                self.confirmation_durability_gaps.load(Ordering::Relaxed),
            dlq_publish_failures = self.dlq_publish_failures.load(Ordering::Relaxed),
            nack_failures = self.nack_failures.load(Ordering::Relaxed),
            telemetry_publish_failures = self.telemetry_publish_failures.load(Ordering::Relaxed),
            "JetStream consumer stats"
        );
    }
}
