//! Output validation, observation, and event-construction for `DerivedNodeAdapter`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use super::{
    DERIVED_OUTPUT_PARENT_HARD_LIMIT, DERIVED_OUTPUT_PARENT_WARN_THRESHOLD, DerivedNodeAdapter,
    derived_event_anchor,
};
#[cfg(feature = "messaging")]
use super::log_self_observation_failure;

use crate::derived_node::context::DerivedTriggerContext;
use crate::derived_node::output::DerivedOutput;
use crate::derived_node::traits::DerivedNodeImpl;
use crate::ids::deterministic_event_id;
use crate::runtime::stream::NodeRuntimeState;
use crate::{NodeResult, SinexError};

use sinex_primitives::events::Event;
use sinex_primitives::events::builder::Provenance;
use sinex_primitives::non_empty::NonEmptyVec;
use sinex_primitives::privacy;
use sinex_primitives::{EventSource, EventType, HostName, Id, JsonValue};

use tracing::{debug, warn};

impl<N> DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl,
{
    pub(super) fn validate_output_batch(
        &self,
        outputs: &[DerivedOutput<JsonValue>],
        phase: &'static str,
    ) -> NodeResult<()> {
        let mut max_parent_count = 0usize;

        for output in outputs {
            let parent_count = output.source_event_ids.len();
            max_parent_count = max_parent_count.max(parent_count);

            if parent_count > DERIVED_OUTPUT_PARENT_HARD_LIMIT {
                let mut error = SinexError::validation(
                    "derived output exceeds synthesis parent hard limit before persistence",
                )
                .with_context("node", self.node.name())
                .with_context("phase", phase)
                .with_context("output_event_type", self.node.output_event_type())
                .with_context("parent_count", parent_count.to_string())
                .with_context("hard_limit", DERIVED_OUTPUT_PARENT_HARD_LIMIT.to_string());

                if let Some(aggregation) = &output.aggregation {
                    error = error
                        .with_context("aggregation_kind", aggregation.kind.clone())
                        .with_context("rollup_level", aggregation.rollup_level.to_string())
                        .with_context(
                            "logical_input_count",
                            aggregation.total_input_count.to_string(),
                        );
                }

                return Err(error);
            }
        }

        if max_parent_count > DERIVED_OUTPUT_PARENT_WARN_THRESHOLD {
            warn!(
                node = %self.node.name(),
                phase,
                output_event_type = %self.node.output_event_type(),
                output_count = outputs.len(),
                max_parent_count,
                threshold = DERIVED_OUTPUT_PARENT_WARN_THRESHOLD,
                hard_limit = DERIVED_OUTPUT_PARENT_HARD_LIMIT,
                "Derived output batch is approaching synthesis parent limits"
            );
        }

        Ok(())
    }

    pub(super) async fn observe_output_batch(
        &self,
        outputs: &[DerivedOutput<JsonValue>],
        phase: &'static str,
    ) {
        if outputs.is_empty() {
            return;
        }

        #[cfg(feature = "messaging")]
        if let Some(obs) = self.self_observer.as_ref() {
            let mut labels = self.derived_metric_labels();
            labels.insert("phase".to_string(), phase.to_string());
            labels.insert(
                "output_event_type".to_string(),
                self.node.output_event_type().to_string(),
            );

            let count = outputs.len() as u64;
            let parent_counts: Vec<f64> = outputs
                .iter()
                .map(|output| output.source_event_ids.len() as f64)
                .collect();
            let parent_sum = parent_counts.iter().sum::<f64>();
            let parent_min = parent_counts.iter().copied().fold(f64::INFINITY, f64::min);
            let parent_max = parent_counts.iter().copied().fold(0.0, f64::max);

            if let Err(error) = obs
                .emit_counter_with_delta(
                    "derived.outputs_emitted",
                    count,
                    count,
                    Some(labels.clone()),
                )
                .await
            {
                log_self_observation_failure(self.node.name(), "derived.outputs_emitted", &error);
            }

            if let Err(error) = obs
                .emit_histogram(
                    "derived.output.parent_count",
                    count,
                    parent_sum,
                    parent_min,
                    parent_max,
                    None,
                    Some(labels.clone()),
                )
                .await
            {
                log_self_observation_failure(
                    self.node.name(),
                    "derived.output.parent_count",
                    &error,
                );
            }

            let aggregated = outputs
                .iter()
                .filter_map(|output| output.aggregation.as_ref())
                .collect::<Vec<_>>();
            if !aggregated.is_empty() {
                let logical_counts: Vec<f64> = aggregated
                    .iter()
                    .map(|aggregation| aggregation.total_input_count as f64)
                    .collect();
                let logical_sum = logical_counts.iter().sum::<f64>();
                let logical_min = logical_counts.iter().copied().fold(f64::INFINITY, f64::min);
                let logical_max = logical_counts.iter().copied().fold(0.0, f64::max);

                if let Err(error) = obs
                    .emit_histogram(
                        "derived.output.logical_input_count",
                        aggregated.len() as u64,
                        logical_sum,
                        logical_min,
                        logical_max,
                        None,
                        Some(labels.clone()),
                    )
                    .await
                {
                    log_self_observation_failure(
                        self.node.name(),
                        "derived.output.logical_input_count",
                        &error,
                    );
                }
            }
        }
    }

    pub(super) fn build_output_events(
        &self,
        outputs: Vec<DerivedOutput<JsonValue>>,
        fallback_source_id: Option<Id<Event<JsonValue>>>,
        context: &DerivedTriggerContext,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        outputs
            .into_iter()
            .enumerate()
            .map(|(output_index, output)| {
                self.build_output_event(output, output_index, fallback_source_id, context)
            })
            .collect()
    }

    /// Build an output `Event<JsonValue>` from a `DerivedOutput<JsonValue>`.
    pub(super) fn build_output_event(
        &self,
        output: DerivedOutput<JsonValue>,
        output_index: usize,
        fallback_source_id: Option<Id<Event<JsonValue>>>,
        context: &DerivedTriggerContext,
    ) -> NodeResult<Event<JsonValue>> {
        let DerivedOutput {
            payload,
            ts_orig,
            source_event_ids,
            temporal_policy,
            semantics_version,
            scope_key,
            equivalence_key,
            aggregation: _aggregation,
        } = output;

        let privacy_context = self.node.output_privacy_context();
        let filtered_payload =
            privacy::process_json(&payload, privacy_context).map_err(|error| {
                SinexError::configuration("failed to initialize privacy engine".to_string())
                    .with_context("component", "derived_output_payload")
                    .with_context("privacy_context", format!("{privacy_context:?}"))
                    .with_std_error(error)
            })?;
        if filtered_payload != payload {
            debug!(
                node = %self.node.name(),
                output_event_type = %self.node.output_event_type(),
                ?privacy_context,
                "Applied privacy filtering to derived output payload"
            );
        }

        let typed_ids: Vec<Id<Event<JsonValue>>> =
            source_event_ids.into_iter().map(Id::from_uuid).collect();
        let source_event_ids = match NonEmptyVec::from_vec(typed_ids) {
            Some(source_event_ids) => source_event_ids,
            None => {
                if let Some(fallback_source_id) = fallback_source_id {
                    NonEmptyVec::single(fallback_source_id)
                } else {
                    return Err(SinexError::validation(
                        "derived invalidation output missing source event ids",
                    )
                    .with_context("node", self.node.name())
                    .with_context("output_event_type", self.node.output_event_type())
                    .with_context("processing_mode", format!("{:?}", context.processing_mode))
                    .with_context("trigger_kind", format!("{:?}", context.trigger_kind))
                    .with_context(
                        "scope_key",
                        scope_key.clone().unwrap_or_else(|| "<none>".to_string()),
                    ));
                }
            }
        };
        let event_id_source = format!(
            "{}:{}:{}",
            self.node.name(),
            self.node.output_event_source(),
            self.node.output_event_type()
        );
        let event_id_anchor = derived_event_anchor(
            output_index,
            &source_event_ids,
            &temporal_policy,
            semantics_version.as_deref(),
            scope_key.as_deref(),
            equivalence_key.as_deref(),
        );
        let provenance = Provenance::Synthesis {
            source_event_ids,
            operation_id: context.operation_id(),
        };
        // Extract before moving provenance into the event struct.
        let created_by_operation_id = provenance.operation_uuid();

        Ok(Event {
            id: Some(Id::from_uuid(deterministic_event_id(
                event_id_source,
                event_id_anchor,
                ts_orig,
            ))),
            source: EventSource::new(self.node.output_event_source())?,
            event_type: EventType::new(self.node.output_event_type())?,
            payload: filtered_payload,
            ts_orig: Some(ts_orig),
            host: HostName::new(&self.host)?,
            node_run_id: self
                .runtime
                .as_ref()
                .and_then(NodeRuntimeState::node_run_id),
            payload_schema_id: None,
            provenance,
            associated_blob_ids: None,
            temporal_policy: Some(temporal_policy),
            semantics_version,
            scope_key,
            equivalence_key,
            created_by_operation_id,
            node_model: Some(self.node.node_model()),
        })
    }
}
