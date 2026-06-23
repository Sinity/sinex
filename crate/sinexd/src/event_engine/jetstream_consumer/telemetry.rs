//! Self-observer telemetry helpers for `JetStreamConsumer`.

use std::sync::atomic::Ordering;

use super::*;

impl JetStreamConsumer {
    pub(super) fn log_observer_error(
        stats: &ConsumerStats,
        metric: &'static str,
        error: &crate::runtime::SelfObservationError,
    ) {
        stats
            .telemetry_publish_failures
            .fetch_add(1, Ordering::Relaxed);
        warn!(metric, error = %error, "Failed to emit event_engine telemetry");
    }

    pub(super) async fn emit_observer_gauge(
        &self,
        metric: &'static str,
        value: f64,
        labels: Option<HashMap<String, String>>,
    ) {
        if let Some(ref observer) = self.observer
            && let Err(error) = observer.emit_gauge(metric, value, labels).await
        {
            Self::log_observer_error(&self.stats, metric, &error);
        }
    }
}
