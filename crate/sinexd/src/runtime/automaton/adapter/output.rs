//! Output validation, observation, and event-construction for `AutomatonRuntime`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

#[cfg(feature = "messaging")]
use super::log_self_observation_failure;
use super::{
    AutomatonRuntime, DERIVED_OUTPUT_PARENT_HARD_LIMIT, DERIVED_OUTPUT_PARENT_WARN_THRESHOLD,
};

use crate::runtime::automaton::context::AutomatonContext;
use crate::runtime::automaton::output::DerivedOutput;
use crate::runtime::automaton::traits::Automaton;
use crate::runtime::stream::RuntimeContext;
use crate::runtime::{RuntimeResult, SinexError};

use sinex_primitives::derivation::{ClaimSupport, DerivationDeclarationId, DerivedProductClass};
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::Provenance;
use sinex_primitives::non_empty::NonEmptyVec;
use sinex_primitives::{EventSource, EventType, HostName, Id, JsonValue};

use tracing::warn;

const DERIVED_OUTPUT_PARENT_WARN_LOG_INTERVAL: Duration = Duration::from_secs(60);

static DERIVED_PARENT_WARN_LIMITER: LazyLock<Mutex<ParentLimitWarnState>> =
    LazyLock::new(|| Mutex::new(ParentLimitWarnState::default()));

#[derive(Debug, Default)]
struct ParentLimitWarnState {
    entries: HashMap<ParentLimitWarnKey, ParentLimitWarnEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ParentLimitWarnKey {
    automaton: &'static str,
    phase: &'static str,
    output_event_type: &'static str,
}

#[derive(Debug)]
struct ParentLimitWarnEntry {
    last_logged_at: Instant,
    suppressed: u64,
}

impl ParentLimitWarnState {
    fn should_log(&mut self, key: ParentLimitWarnKey, now: Instant) -> Option<u64> {
        let Some(entry) = self.entries.get_mut(&key) else {
            self.entries.insert(
                key,
                ParentLimitWarnEntry {
                    last_logged_at: now,
                    suppressed: 0,
                },
            );
            return Some(0);
        };

        if now.duration_since(entry.last_logged_at) >= DERIVED_OUTPUT_PARENT_WARN_LOG_INTERVAL {
            let suppressed = entry.suppressed;
            entry.last_logged_at = now;
            entry.suppressed = 0;
            return Some(suppressed);
        }

        entry.suppressed = entry.suppressed.saturating_add(1);
        None
    }
}

impl<N> AutomatonRuntime<N>
where
    N: Automaton,
{
    pub(super) fn validate_output_batch(
        &self,
        outputs: &[DerivedOutput<JsonValue>],
        phase: &'static str,
    ) -> RuntimeResult<()> {
        let mut max_parent_count = 0usize;

        for output in outputs {
            let parent_count = output.source_event_ids.len();
            max_parent_count = max_parent_count.max(parent_count);

            if parent_count > DERIVED_OUTPUT_PARENT_HARD_LIMIT {
                let mut error = SinexError::validation(
                    "derived output exceeds derived parent hard limit before persistence",
                )
                .with_context("automaton", self.automaton.name())
                .with_context("phase", phase)
                .with_context("output_event_type", self.automaton.output_event_type())
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
            let key = ParentLimitWarnKey {
                automaton: self.automaton.name(),
                phase,
                output_event_type: self.automaton.output_event_type(),
            };
            let suppressed_since_last_log = DERIVED_PARENT_WARN_LIMITER
                .lock()
                .map(|mut limiter| limiter.should_log(key, Instant::now()))
                .unwrap_or(Some(0));
            if let Some(suppressed_since_last_log) = suppressed_since_last_log {
                warn!(
                    automaton = %self.automaton.name(),
                    phase,
                    output_event_type = %self.automaton.output_event_type(),
                    output_count = outputs.len(),
                    max_parent_count,
                    threshold = DERIVED_OUTPUT_PARENT_WARN_THRESHOLD,
                    hard_limit = DERIVED_OUTPUT_PARENT_HARD_LIMIT,
                    suppressed_since_last_log,
                    "Derived output batch is approaching derived parent limits"
                );
            }
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
                self.automaton.output_event_type().to_string(),
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
                log_self_observation_failure(
                    self.automaton.name(),
                    "derived.outputs_emitted",
                    &error,
                );
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
                    self.automaton.name(),
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
                        self.automaton.name(),
                        "derived.output.logical_input_count",
                        &error,
                    );
                }
            }
        }
    }

    /// Enforce the derivation control-plane contract for one output
    /// (sinex-0vx.2). `declaration_id: None` (with no `product_class` set
    /// either) passes unconditionally — this is the transition-period shape
    /// every automaton emits until sinex-0vx.3 stamps its call sites.
    ///
    /// When a declaration is claimed, it must exist on
    /// `N::OUTPUT_DECLARATIONS` and agree with the output on product class
    /// and the resolved `(output_source, output_event_type)`. A claim-support
    /// vector with an adjudicated status must carry an
    /// `adjudication_event_id` — re-asserting `ClaimSupport::is_shape_valid()`
    /// at the emission boundary defends against any construction path that
    /// bypasses `ClaimSupport`'s compile-time constructors (e.g. a
    /// wire-deserialized value fed back through a replay path).
    fn validate_output_declaration(
        &self,
        declaration_id: Option<DerivationDeclarationId>,
        product_class: Option<DerivedProductClass>,
        claim_support: Option<&ClaimSupport>,
        resolved_event_type: &'static str,
    ) -> RuntimeResult<()> {
        if let Some(claim_support) = claim_support
            && !claim_support.is_shape_valid()
        {
            return Err(SinexError::validation(
                "derived output claim_support is adjudicated without an adjudication_event_id",
            )
            .with_context("automaton", self.automaton.name())
            .with_context("output_event_type", resolved_event_type));
        }

        let Some(declaration_id) = declaration_id else {
            if product_class.is_some() {
                return Err(SinexError::validation(
                    "derived output set product_class without a declaration_id",
                )
                .with_context("automaton", self.automaton.name())
                .with_context("output_event_type", resolved_event_type));
            }
            return Ok(());
        };

        let declaration = N::OUTPUT_DECLARATIONS
            .iter()
            .find(|declaration| declaration.declaration_id == declaration_id)
            .ok_or_else(|| {
                SinexError::validation("derived output claims an undeclared declaration_id")
                    .with_context("automaton", self.automaton.name())
                    .with_context("output_event_type", resolved_event_type)
                    .with_context("declaration_id", declaration_id)
            })?;

        if let Some(product_class) = product_class
            && product_class != declaration.product_class
        {
            return Err(SinexError::validation(
                "derived output product_class disagrees with its declaration",
            )
            .with_context("automaton", self.automaton.name())
            .with_context("declaration_id", declaration_id)
            .with_context("declared_product_class", declaration.product_class.as_str())
            .with_context("output_product_class", product_class.as_str()));
        }

        let source_matches = declaration
            .output_source
            .is_none_or(|source| source == self.automaton.output_event_source());
        let type_matches = declaration
            .output_event_type
            .is_none_or(|event_type| event_type == resolved_event_type);
        if !source_matches || !type_matches {
            return Err(SinexError::validation(
                "derived output (source, event_type) disagrees with its declaration",
            )
            .with_context("automaton", self.automaton.name())
            .with_context("declaration_id", declaration_id)
            .with_context(
                "declared_source",
                declaration.output_source.unwrap_or("<any>"),
            )
            .with_context(
                "declared_event_type",
                declaration.output_event_type.unwrap_or("<any>"),
            )
            .with_context("output_source", self.automaton.output_event_source())
            .with_context("output_event_type", resolved_event_type));
        }

        Ok(())
    }

    pub(super) fn build_output_events(
        &self,
        outputs: Vec<DerivedOutput<JsonValue>>,
        fallback_source_id: Option<Id<Event<JsonValue>>>,
        context: &AutomatonContext,
    ) -> RuntimeResult<Vec<Event<JsonValue>>> {
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
        _output_index: usize,
        fallback_source_id: Option<Id<Event<JsonValue>>>,
        context: &AutomatonContext,
    ) -> RuntimeResult<Event<JsonValue>> {
        let DerivedOutput {
            payload,
            ts_orig,
            source_event_ids,
            temporal_policy,
            semantics_version,
            scope_key,
            equivalence_key,
            aggregation: _aggregation,
            event_type,
            declaration_id,
            product_class,
            claim_support,
            derivation_epoch_id: _derivation_epoch_id,
            derivation_lane_id: _derivation_lane_id,
        } = output;

        let resolved_event_type = event_type.unwrap_or_else(|| self.automaton.output_event_type());

        self.validate_output_declaration(
            declaration_id,
            product_class,
            claim_support.as_ref(),
            resolved_event_type,
        )?;

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
                    .with_context("automaton", self.automaton.name())
                    .with_context("output_event_type", resolved_event_type)
                    .with_context("processing_mode", format!("{:?}", context.processing_mode))
                    .with_context("trigger_kind", format!("{:?}", context.trigger_kind))
                    .with_context(
                        "scope_key",
                        scope_key.clone().unwrap_or_else(|| "<none>".to_string()),
                    ));
                }
            }
        };
        let mut provenance = Provenance::from_derived(source_event_ids).ok_or_else(|| {
            SinexError::validation("derived invalidation output missing source event ids")
        })?;
        if let Some(operation_id) = context.operation_id() {
            provenance = provenance.with_operation(operation_id);
        }
        // Extract before moving provenance into the event struct.
        let created_by_operation_id = provenance.operation_uuid();

        Ok(Event {
            // Fresh random UUIDv7: event ID is interpretation identity, not occurrence
            // identity. Derived events re-processed on replay get new IDs by design.
            // ON CONFLICT (id) DO NOTHING dedup operates on the id minted here and
            // carried unchanged through NATS redelivery.
            id: Some(Id::new()),
            source: EventSource::new(self.automaton.output_event_source())?,
            event_type: EventType::new(resolved_event_type)?,
            payload,
            ts_orig: Some(ts_orig),
            // Derived events carry no source-material timing rung; ts_orig is the
            // synthesis-time value chosen by the temporal policy.
            ts_quality: None,
            host: HostName::new(&self.host)?,
            module_run_id: self
                .runtime
                .as_ref()
                .and_then(RuntimeContext::module_run_id),
            payload_schema_id: None,
            provenance,
            associated_blob_ids: None,
            temporal_policy: Some(temporal_policy),
            semantics_version,
            scope_key,
            equivalence_key,
            created_by_operation_id,
            automaton_model: Some(self.automaton.automaton_model()),
            anchor_payload_hash: None,
        })
    }
}

#[cfg(test)]
#[path = "output_test.rs"]
mod tests;
