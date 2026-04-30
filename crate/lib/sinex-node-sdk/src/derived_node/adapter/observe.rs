//! Self-observation / telemetry methods for `DerivedNodeAdapter`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use super::DerivedNodeAdapter;
#[cfg(feature = "messaging")]
use super::log_self_observation_failure;

use crate::checkpoint::CheckpointState;
use crate::derived_node::traits::DerivedNodeImpl;
use crate::runtime::stream::{Checkpoint, NodeRuntimeState};

use std::collections::HashMap;
use std::time::Instant;

impl<N> DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl,
{
    #[cfg(feature = "messaging")]
    pub(super) fn derived_metric_labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert("node".to_string(), self.node.name().to_string());
        labels.insert("node_model".to_string(), self.node.node_model().to_string());
        if let Some(node_run_id) = self
            .runtime
            .as_ref()
            .and_then(NodeRuntimeState::node_run_id)
        {
            labels.insert("node_run_id".to_string(), node_run_id.to_string());
        }
        labels
    }

    #[cfg(feature = "messaging")]
    pub(super) fn checkpoint_labels(&self, checkpoint: &Checkpoint) -> HashMap<String, String> {
        let mut labels = self.derived_metric_labels();
        let (kind, position) = match checkpoint {
            Checkpoint::None => ("none", None),
            Checkpoint::External {
                position,
                description,
            } => ("external", Some(format!("{description}:{position}"))),
            Checkpoint::Internal {
                event_id,
                message_count,
            } => ("internal", Some(format!("{event_id}:#{message_count}"))),
            Checkpoint::Stream {
                message_id,
                event_id,
            } => (
                "stream",
                Some(match event_id {
                    Some(event_id) => format!("{message_id}:{event_id}"),
                    None => message_id.clone(),
                }),
            ),
            Checkpoint::Timestamp { timestamp, .. } => {
                ("timestamp", Some(timestamp.format_rfc3339()))
            }
        };
        labels.insert("checkpoint_kind".to_string(), kind.to_string());
        if let Some(position) = position {
            labels.insert("checkpoint_position".to_string(), position);
        }
        labels
    }

    #[cfg(feature = "messaging")]
    pub(super) async fn observe_runtime_snapshot(&self) {
        let Some(obs) = self.self_observer.as_ref() else {
            return;
        };

        let labels = self.derived_metric_labels();
        if let Err(error) = obs
            .emit_gauge(
                "derived.events_processed.run",
                self.run_events_processed as f64,
                Some(labels.clone()),
            )
            .await
        {
            log_self_observation_failure(self.node.name(), "derived.events_processed.run", &error);
        }

        if let Some(reporter) = self.health_reporter.as_ref() {
            let error_rate = reporter.metrics().error_rate(300);
            if let Err(error) = obs
                .emit_gauge("derived.error_rate_5m", error_rate, Some(labels))
                .await
            {
                log_self_observation_failure(self.node.name(), "derived.error_rate_5m", &error);
            }
        }
    }

    #[cfg(not(feature = "messaging"))]
    pub(super) async fn observe_runtime_snapshot(&self) {}

    /// Emit per-event processing-latency gauges (point-in-time + percentile)
    /// so operators can see how a derived node is keeping up with its input
    /// stream. Each call records the latest sample into the in-process
    /// reservoirs and emits both the last-value gauge and the latest
    /// percentile read.
    ///
    /// Gauges:
    /// - `derived.event_lag_ms` — last lag sample (wall time between
    ///   upstream `ts_orig` and dispatch).
    /// - `derived.tick_runtime_ms` — last runtime sample.
    /// - `derived.event_lag_p50_ms`, `derived.event_lag_p99_ms` — sliding
    ///   reservoir percentiles over the last `DEFAULT_LATENCY_RESERVOIR`
    ///   samples.
    /// - `derived.tick_runtime_p99_ms` — same reservoir, runtime samples.
    /// - `derived.throughput_eps` — events per second over the live
    ///   `THROUGHPUT_WINDOW`.
    #[cfg(feature = "messaging")]
    pub(super) async fn observe_processing_latency(&mut self, lag_ms: f64, runtime_ms: f64) {
        // Feed the windows regardless of self_observer presence so unit
        // tests and feature-gated builds keep accurate state.
        if lag_ms.is_finite() {
            self.lag_window.record(lag_ms);
        }
        if runtime_ms.is_finite() {
            self.runtime_window.record(runtime_ms);
        }
        self.throughput_window.record(Instant::now());

        let Some(obs) = self.self_observer.as_ref() else {
            return;
        };
        let labels = self.derived_metric_labels();

        if lag_ms.is_finite() {
            if let Err(error) = obs
                .emit_gauge("derived.event_lag_ms", lag_ms, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(self.node.name(), "derived.event_lag_ms", &error);
            }
        }

        if runtime_ms.is_finite() {
            if let Err(error) = obs
                .emit_gauge("derived.tick_runtime_ms", runtime_ms, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(self.node.name(), "derived.tick_runtime_ms", &error);
            }
        }

        if let Some(p50) = self.lag_window.percentile(0.5) {
            if let Err(error) = obs
                .emit_gauge("derived.event_lag_p50_ms", p50, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(
                    self.node.name(),
                    "derived.event_lag_p50_ms",
                    &error,
                );
            }
        }
        if let Some(p99) = self.lag_window.percentile(0.99) {
            if let Err(error) = obs
                .emit_gauge("derived.event_lag_p99_ms", p99, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(
                    self.node.name(),
                    "derived.event_lag_p99_ms",
                    &error,
                );
            }
        }
        if let Some(p99) = self.runtime_window.percentile(0.99) {
            if let Err(error) = obs
                .emit_gauge("derived.tick_runtime_p99_ms", p99, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(
                    self.node.name(),
                    "derived.tick_runtime_p99_ms",
                    &error,
                );
            }
        }

        let eps = self.throughput_window.eps(Instant::now());
        if let Err(error) = obs
            .emit_gauge("derived.throughput_eps", eps, Some(labels))
            .await
        {
            log_self_observation_failure(self.node.name(), "derived.throughput_eps", &error);
        }
    }

    #[cfg(not(feature = "messaging"))]
    pub(super) async fn observe_processing_latency(&mut self, lag_ms: f64, runtime_ms: f64) {
        if lag_ms.is_finite() {
            self.lag_window.record(lag_ms);
        }
        if runtime_ms.is_finite() {
            self.runtime_window.record(runtime_ms);
        }
        self.throughput_window.record(Instant::now());
    }

    /// Emit telemetry for a whole-batch processing cycle (event bridge path).
    ///
    /// Emits `derived.batch_runtime_ms` rather than `derived.tick_runtime_ms`
    /// so the batch metric does not overwrite the per-event samples recorded by
    /// `observe_processing_latency`.
    #[cfg(feature = "messaging")]
    pub(super) async fn observe_batch_processing_latency(
        &mut self,
        lag_ms: f64,
        batch_runtime_ms: f64,
        batch_size: usize,
    ) {
        if lag_ms.is_finite() {
            self.lag_window.record(lag_ms);
        }
        self.throughput_window.record(Instant::now());

        let Some(obs) = self.self_observer.as_ref() else {
            return;
        };
        let mut labels = self.derived_metric_labels();
        labels.insert("batch_size".to_string(), batch_size.to_string());

        if lag_ms.is_finite() {
            if let Err(error) = obs
                .emit_gauge("derived.event_lag_ms", lag_ms, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(self.node.name(), "derived.event_lag_ms", &error);
            }
        }

        if batch_runtime_ms.is_finite() {
            if let Err(error) = obs
                .emit_gauge(
                    "derived.batch_runtime_ms",
                    batch_runtime_ms,
                    Some(labels.clone()),
                )
                .await
            {
                log_self_observation_failure(
                    self.node.name(),
                    "derived.batch_runtime_ms",
                    &error,
                );
            }
        }
    }

    #[cfg(not(feature = "messaging"))]
    pub(super) async fn observe_batch_processing_latency(
        &mut self,
        lag_ms: f64,
        _batch_runtime_ms: f64,
        _batch_size: usize,
    ) {
        if lag_ms.is_finite() {
            self.lag_window.record(lag_ms);
        }
        self.throughput_window.record(Instant::now());
    }

    #[cfg(feature = "messaging")]
    pub(super) async fn observe_checkpoint_state(&self, state: &CheckpointState) {
        let Some(obs) = self.self_observer.as_ref() else {
            return;
        };

        let labels = self.checkpoint_labels(&state.checkpoint);
        if let Err(error) = obs
            .emit_gauge(
                "derived.checkpoint.revision",
                state.revision as f64,
                Some(labels),
            )
            .await
        {
            log_self_observation_failure(self.node.name(), "derived.checkpoint.revision", &error);
        }
    }

    #[cfg(not(feature = "messaging"))]
    pub(super) async fn observe_checkpoint_state(&self, _state: &CheckpointState) {}

    #[cfg(feature = "messaging")]
    pub(super) async fn observe_pending_invalidations(&self, count: usize) {
        let Some(obs) = self.self_observer.as_ref() else {
            return;
        };

        if let Err(error) = obs
            .emit_gauge(
                "derived.invalidations.pending",
                count as f64,
                Some(self.derived_metric_labels()),
            )
            .await
        {
            log_self_observation_failure(self.node.name(), "derived.invalidations.pending", &error);
        }
    }

    #[cfg(not(feature = "messaging"))]
    pub(super) async fn observe_pending_invalidations(&self, _count: usize) {}
}
