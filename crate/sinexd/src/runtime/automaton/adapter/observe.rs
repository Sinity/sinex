//! Self-observation / telemetry methods for `AutomatonRuntime`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use super::AutomatonRuntime;
#[cfg(feature = "messaging")]
use super::log_self_observation_failure;

use crate::runtime::automaton::traits::Automaton;
use crate::runtime::checkpoint::CheckpointState;
use crate::runtime::stream::{Checkpoint, RuntimeContext};

use std::collections::HashMap;
use std::time::Instant;

impl<N> AutomatonRuntime<N>
where
    N: Automaton,
{
    #[cfg(feature = "messaging")]
    pub(super) fn derived_metric_labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert("automaton".to_string(), self.automaton.name().to_string());
        labels.insert(
            "automaton_model".to_string(),
            self.automaton.automaton_model().to_string(),
        );
        if let Some(module_run_id) = self
            .runtime
            .as_ref()
            .and_then(RuntimeContext::module_run_id)
        {
            labels.insert("module_run_id".to_string(), module_run_id.to_string());
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
            log_self_observation_failure(
                self.automaton.name(),
                "derived.events_processed.run",
                &error,
            );
        }

        if let Some(reporter) = self.health_reporter.as_ref() {
            let error_rate = reporter.metrics().error_rate(300);
            if let Err(error) = obs
                .emit_gauge("derived.error_rate_5m", error_rate, Some(labels))
                .await
            {
                log_self_observation_failure(
                    self.automaton.name(),
                    "derived.error_rate_5m",
                    &error,
                );
            }
        }
    }

    #[cfg(not(feature = "messaging"))]
    pub(super) async fn observe_runtime_snapshot(&self) {}

    /// Emit a per-event processing-latency snapshot so operators can see how a
    /// automaton is keeping up with its input stream. Each call records the
    /// latest sample into the in-process reservoirs and publishes a single
    /// `derived.latency_snapshot` event capturing the last sample plus the
    /// current reservoir/window readings.
    ///
    /// Fields on the snapshot payload:
    /// - `event_lag_ms` — last lag sample (wall time between upstream
    ///   `ts_orig` and dispatch).
    /// - `tick_runtime_ms` — last runtime sample.
    /// - `event_lag_p50_ms`, `event_lag_p99_ms` — sliding reservoir
    ///   percentiles over the last `DEFAULT_LATENCY_RESERVOIR` samples.
    /// - `tick_runtime_p99_ms` — same reservoir, runtime samples.
    /// - `throughput_eps` — events per second over the live
    ///   `THROUGHPUT_WINDOW`.
    ///
    /// Replaces the prior six separate `metric.gauge` emissions with one event;
    /// see issue #1556.
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
        let eps = self.throughput_window.eps(Instant::now());

        if let Err(error) = obs
            .emit_automaton_latency_snapshot(
                self.automaton.name(),
                lag_ms.is_finite().then_some(lag_ms),
                runtime_ms.is_finite().then_some(runtime_ms),
                self.lag_window.percentile(0.5),
                self.lag_window.percentile(0.99),
                self.runtime_window.percentile(0.99),
                eps,
                labels,
            )
            .await
        {
            log_self_observation_failure(self.automaton.name(), "derived.latency_snapshot", &error);
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

        if lag_ms.is_finite()
            && let Err(error) = obs
                .emit_gauge("derived.event_lag_ms", lag_ms, Some(labels.clone()))
                .await
        {
            log_self_observation_failure(self.automaton.name(), "derived.event_lag_ms", &error);
        }

        if batch_runtime_ms.is_finite()
            && let Err(error) = obs
                .emit_gauge(
                    "derived.batch_runtime_ms",
                    batch_runtime_ms,
                    Some(labels.clone()),
                )
                .await
        {
            log_self_observation_failure(self.automaton.name(), "derived.batch_runtime_ms", &error);
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
            log_self_observation_failure(
                self.automaton.name(),
                "derived.checkpoint.revision",
                &error,
            );
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
            log_self_observation_failure(
                self.automaton.name(),
                "derived.invalidations.pending",
                &error,
            );
        }
    }

    #[cfg(not(feature = "messaging"))]
    pub(super) async fn observe_pending_invalidations(&self, _count: usize) {}

    /// Emit a counter for events filtered out by type/provenance mismatch.
    /// High filter rates indicate a consumer subscribed to a broader subject than
    /// the automaton's declared input type.
    #[cfg(feature = "messaging")]
    pub(super) async fn observe_filtered_events(&self, filtered_count: usize) {
        if filtered_count == 0 {
            return;
        }
        tracing::debug!(
            target: "sinex_metrics",
            metric = "derived.events_filtered",
            automaton = %self.automaton.name(),
            filtered_count,
        );
        let Some(obs) = self.self_observer.as_ref() else {
            return;
        };
        if let Err(error) = obs
            .emit_counter(
                "derived.events_filtered_total",
                filtered_count as u64,
                Some(self.derived_metric_labels()),
            )
            .await
        {
            log_self_observation_failure(
                self.automaton.name(),
                "derived.events_filtered_total",
                &error,
            );
        }
    }

    #[cfg(not(feature = "messaging"))]
    pub(super) async fn observe_filtered_events(&self, _filtered_count: usize) {}
}
