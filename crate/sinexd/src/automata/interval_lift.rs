//! Interval lift automaton -- stateful transition-to-interval derivation.
//!
//! The first rule lifts Hyprland focus transitions into generic
//! `state.interval` events. Additional point/transition sources should add
//! rules here instead of creating source-specific interval silos.

use crate::runtime::automaton::{DerivedOutput, TransducerAdapter};
use crate::runtime::{AutomatonContext, AutomatonLogicError, InputProvenanceFilter, Transducer};
use serde::{Deserialize, Serialize};
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::events::payloads::{HyprlandWindowFocusedPayload, StateIntervalPayload};
use sinex_primitives::events::EventPayload;
use sinex_primitives::{Timestamp, Uuid};
use std::collections::BTreeMap;

const SEMANTICS_VERSION: &str = "1.0.0";
const FOCUS_STATE_KIND: &str = "desktop.focus";

#[derive(Debug, Clone, Default)]
pub struct IntervalLift;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IntervalLiftState {
    active_focus: Option<FocusTransition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FocusTransition {
    event_id: Uuid,
    ts_orig: Timestamp,
    window_id: Option<String>,
    window_class: Option<String>,
    window_title: Option<String>,
    workspace_id: Option<i32>,
}

impl FocusTransition {
    fn from_payload(
        input: HyprlandWindowFocusedPayload,
        context: &AutomatonContext,
    ) -> Result<Self, AutomatonLogicError> {
        Ok(Self {
            event_id: context.trigger_uuid(),
            ts_orig: context.require_ts_orig()?,
            window_id: input.window_id,
            window_class: input.window_class,
            window_title: input.window_title,
            workspace_id: input.workspace_id,
        })
    }

    fn subject_id(&self) -> Option<String> {
        self.window_id
            .clone()
            .or_else(|| self.window_class.as_ref().map(|class| format!("class:{class}")))
    }

    fn same_subject_as(&self, other: &Self) -> bool {
        let Some(subject_id) = self.subject_id() else {
            return false;
        };
        other.subject_id().as_deref() == Some(subject_id.as_str())
    }

    fn refresh_metadata_from(&mut self, other: &Self) {
        if other.window_class.is_some() {
            self.window_class.clone_from(&other.window_class);
        }
        if other.window_title.is_some() {
            self.window_title.clone_from(&other.window_title);
        }
        if other.workspace_id.is_some() {
            self.workspace_id = other.workspace_id;
        }
    }

    fn label(&self) -> Option<String> {
        match (&self.window_class, &self.window_title) {
            (Some(class), Some(title)) if !title.is_empty() => Some(format!("{class}: {title}")),
            (Some(class), _) => Some(class.clone()),
            (_, Some(title)) if !title.is_empty() => Some(title.clone()),
            _ => None,
        }
    }

    fn attributes(&self) -> BTreeMap<String, String> {
        let mut attributes = BTreeMap::new();
        if let Some(window_id) = &self.window_id {
            attributes.insert("window_id".to_string(), window_id.clone());
        }
        if let Some(window_class) = &self.window_class {
            attributes.insert("window_class".to_string(), window_class.clone());
        }
        if let Some(window_title) = &self.window_title {
            attributes.insert("window_title".to_string(), window_title.clone());
        }
        if let Some(workspace_id) = self.workspace_id {
            attributes.insert("workspace_id".to_string(), workspace_id.to_string());
        }
        attributes
    }
}

impl Transducer for IntervalLift {
    type State = IntervalLiftState;
    type Input = HyprlandWindowFocusedPayload;
    type Output = StateIntervalPayload;

    fn name(&self) -> &'static str {
        "interval-lift"
    }

    fn input_event_type(&self) -> &'static str {
        HyprlandWindowFocusedPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_type(&self) -> &'static str {
        StateIntervalPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        StateIntervalPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::MaterialOnly
    }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        let current = FocusTransition::from_payload(input, context)?;

        let Some(previous) = state.active_focus.as_mut() else {
            state.active_focus = Some(current);
            return Ok(None);
        };

        if current.ts_orig <= previous.ts_orig {
            return Ok(None);
        }

        if previous.same_subject_as(&current) {
            previous.refresh_metadata_from(&current);
            return Ok(None);
        }

        let previous = previous.clone();
        state.active_focus = Some(current.clone());

        let duration_secs = (current.ts_orig - previous.ts_orig).whole_seconds().max(0) as u64;
        let subject_part = previous
            .subject_id()
            .unwrap_or_else(|| "unknown-window".to_string());
        let interval_id = format!(
            "interval:{FOCUS_STATE_KIND}:{subject_part}:{}:{}",
            previous.event_id, current.event_id
        );

        let payload = StateIntervalPayload {
            interval_id: interval_id.clone(),
            state_kind: FOCUS_STATE_KIND.to_string(),
            subject_id: previous.subject_id(),
            label: previous.label(),
            start_time: previous.ts_orig,
            end_time: current.ts_orig,
            duration_secs,
            start_event_type: HyprlandWindowFocusedPayload::EVENT_TYPE.as_static_str().to_string(),
            end_event_type: HyprlandWindowFocusedPayload::EVENT_TYPE.as_static_str().to_string(),
            attributes: previous.attributes(),
        };

        Ok(Some(
            DerivedOutput::windowed(
                payload,
                current.ts_orig,
                vec![previous.event_id, current.event_id],
            )
            .with_temporal_policy(SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version(SEMANTICS_VERSION)
            .with_equivalence_key(interval_id),
        ))
    }
}

/// RuntimeModule type alias registered via `AutomatonSpec` in `automata::registry`.
pub type IntervalLiftRuntime = TransducerAdapter<IntervalLift>;

// --- Source descriptor ------------------------------------------------------

use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    ResourceProfile, RetentionPolicy as ContractRetentionPolicy, RunnerPack,
    RuntimeShape as ContractRuntimeShape, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

register_source_contract! {
    SourceContract {
        id: "interval-lift",
        namespace: "derived",
        event_types: &[
            ("derived.interval-lift", "state.interval"),
        ],
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(source, state_kind, subject_id, start_parent_event_id, end_parent_event_id)",
        ),
        access_scope: AccessScope::Internal,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:interval-lift"),
        "interval-lift",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("state.interval")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("interval-lift")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}

#[cfg(test)]
#[path = "interval_lift_test.rs"]
mod tests;
