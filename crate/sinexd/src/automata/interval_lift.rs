//! Interval lift automaton -- stateful transition-to-interval derivation.
//!
//! The first rule lifts Hyprland focus transitions into generic
//! `state.interval` events. Additional point/transition sources should add
//! rules here instead of creating source-specific interval silos.

use crate::runtime::automaton::{DerivedOutput, TransducerAdapter};
use crate::runtime::{AutomatonContext, AutomatonLogicError, InputProvenanceFilter, Transducer};
use serde::{Deserialize, Deserializer, Serialize};
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::events::payloads::{
    ActivityWatchAfkChangedPayload, ActivityWatchWindowActivePayload, HyprlandWindowFocusedPayload,
    StateIntervalPayload,
};
use sinex_primitives::events::EventPayload;
use sinex_primitives::temporal::Duration;
use sinex_primitives::{JsonValue, Timestamp, Uuid};
use std::collections::BTreeMap;

const SEMANTICS_VERSION: &str = "1.0.0";
const FOCUS_STATE_KIND: &str = "desktop.focus";
const ACTIVITYWATCH_WINDOW_STATE_KIND: &str = "desktop.activitywatch.window";
const ACTIVITYWATCH_AFK_STATE_KIND: &str = "desktop.activitywatch.afk";

#[derive(Debug, Clone, Default)]
pub struct IntervalLift;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IntervalLiftState {
    #[serde(default, deserialize_with = "deserialize_active_focus")]
    active_focus: Option<StateObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateObservation {
    state_kind: String,
    event_id: Uuid,
    ts_orig: Timestamp,
    subject_id: Option<String>,
    label: Option<String>,
    event_type: String,
    attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
struct LegacyFocusTransition {
    event_id: Uuid,
    ts_orig: Timestamp,
    window_id: Option<String>,
    window_class: Option<String>,
    window_title: Option<String>,
    workspace_id: Option<i32>,
}

impl StateObservation {
    fn from_focus_payload(
        input: HyprlandWindowFocusedPayload,
        context: &AutomatonContext,
    ) -> Result<Self, AutomatonLogicError> {
        let subject_id = input.window_id.clone().or_else(|| {
            input
                .window_class
                .as_ref()
                .map(|class| format!("class:{class}"))
        });
        let label = match (&input.window_class, &input.window_title) {
            (Some(class), Some(title)) if !title.is_empty() => Some(format!("{class}: {title}")),
            (Some(class), _) => Some(class.clone()),
            (_, Some(title)) if !title.is_empty() => Some(title.clone()),
            _ => None,
        };
        let mut attributes = BTreeMap::new();
        if let Some(window_id) = &input.window_id {
            attributes.insert("window_id".to_string(), window_id.clone());
        }
        if let Some(window_class) = &input.window_class {
            attributes.insert("window_class".to_string(), window_class.clone());
        }
        if let Some(window_title) = &input.window_title {
            attributes.insert("window_title".to_string(), window_title.clone());
        }
        if let Some(workspace_id) = input.workspace_id {
            attributes.insert("workspace_id".to_string(), workspace_id.to_string());
        }

        Ok(Self {
            state_kind: FOCUS_STATE_KIND.to_string(),
            event_id: context.trigger_uuid(),
            ts_orig: context.require_ts_orig()?,
            subject_id,
            label,
            event_type: HyprlandWindowFocusedPayload::EVENT_TYPE
                .as_static_str()
                .to_string(),
            attributes,
        })
    }

    fn from_activitywatch_window_payload(
        input: ActivityWatchWindowActivePayload,
        context: &AutomatonContext,
    ) -> Result<Self, AutomatonLogicError> {
        let subject_id = match (input.app.is_empty(), input.title.is_empty()) {
            (false, false) => Some(format!("app:{}|title:{}", input.app, input.title)),
            (false, true) => Some(format!("app:{}", input.app)),
            (true, false) => Some(format!("title:{}", input.title)),
            (true, true) => None,
        };
        let label = match (input.app.is_empty(), input.title.is_empty()) {
            (false, false) => Some(format!("{}: {}", input.app, input.title)),
            (false, true) => Some(input.app.clone()),
            (true, false) => Some(input.title.clone()),
            (true, true) => None,
        };
        let mut attributes = BTreeMap::new();
        attributes.insert("bucket_id".to_string(), input.bucket_id);
        attributes.insert("duration_ms".to_string(), input.duration_ms.to_string());
        if !input.app.is_empty() {
            attributes.insert("app".to_string(), input.app);
        }
        if !input.title.is_empty() {
            attributes.insert("title".to_string(), input.title);
        }

        Ok(Self {
            state_kind: ACTIVITYWATCH_WINDOW_STATE_KIND.to_string(),
            event_id: context.trigger_uuid(),
            ts_orig: context.require_ts_orig()?,
            subject_id,
            label,
            event_type: ActivityWatchWindowActivePayload::EVENT_TYPE
                .as_static_str()
                .to_string(),
            attributes,
        })
    }

    fn from_activitywatch_afk_payload(
        input: ActivityWatchAfkChangedPayload,
        context: &AutomatonContext,
    ) -> Result<Self, AutomatonLogicError> {
        let subject_id = (!input.status.is_empty()).then(|| format!("status:{}", input.status));
        let label = (!input.status.is_empty()).then(|| input.status.clone());
        let mut attributes = BTreeMap::new();
        attributes.insert("bucket_id".to_string(), input.bucket_id);
        attributes.insert("duration_ms".to_string(), input.duration_ms.to_string());
        if !input.status.is_empty() {
            attributes.insert("status".to_string(), input.status);
        }

        Ok(Self {
            state_kind: ACTIVITYWATCH_AFK_STATE_KIND.to_string(),
            event_id: context.trigger_uuid(),
            ts_orig: context.require_ts_orig()?,
            subject_id,
            label,
            event_type: ActivityWatchAfkChangedPayload::EVENT_TYPE
                .as_static_str()
                .to_string(),
            attributes,
        })
    }

    fn same_subject_as(&self, other: &Self) -> bool {
        if self.state_kind != other.state_kind {
            return false;
        }
        let Some(subject_id) = &self.subject_id else {
            return false;
        };
        other.subject_id.as_deref() == Some(subject_id.as_str())
    }

    fn refresh_metadata_from(&mut self, other: &Self) {
        if other.label.is_some() {
            self.label.clone_from(&other.label);
        }
        self.attributes.extend(other.attributes.clone());
    }

    fn close_with(&self, current: &Self) -> DerivedOutput<StateIntervalPayload> {
        let duration_secs = (current.ts_orig - self.ts_orig).whole_seconds().max(0) as u64;
        let subject_part = self
            .subject_id
            .as_deref()
            .unwrap_or("unknown-subject")
            .to_string();
        let interval_id = format!(
            "interval:{}:{subject_part}:{}:{}",
            self.state_kind, self.event_id, current.event_id
        );

        let payload = StateIntervalPayload {
            interval_id: interval_id.clone(),
            state_kind: self.state_kind.clone(),
            subject_id: self.subject_id.clone(),
            label: self.label.clone(),
            start_time: self.ts_orig,
            end_time: current.ts_orig,
            duration_secs,
            start_event_type: self.event_type.clone(),
            end_event_type: current.event_type.clone(),
            attributes: self.attributes.clone(),
        };

        DerivedOutput::windowed(
            payload,
            current.ts_orig,
            vec![self.event_id, current.event_id],
        )
        .with_temporal_policy(SyntheticTemporalPolicy::WindowBoundary)
        .with_semantics_version(SEMANTICS_VERSION)
        .with_equivalence_key(interval_id)
    }

    fn observed_duration_interval(
        &self,
        duration_ms: u64,
        max_end_time: Timestamp,
    ) -> DerivedOutput<StateIntervalPayload> {
        let duration = Duration::milliseconds(i64::try_from(duration_ms).unwrap_or(i64::MAX));
        let observed_end_time = self.ts_orig + duration;
        let end_time = observed_end_time.min(max_end_time).max(self.ts_orig);
        let subject_part = self
            .subject_id
            .as_deref()
            .unwrap_or("unknown-subject")
            .to_string();
        let interval_id = format!(
            "interval:{}:{subject_part}:{}:{}",
            self.state_kind, self.event_id, self.event_id
        );

        let payload = StateIntervalPayload {
            interval_id: interval_id.clone(),
            state_kind: self.state_kind.clone(),
            subject_id: self.subject_id.clone(),
            label: self.label.clone(),
            start_time: self.ts_orig,
            end_time,
            duration_secs: (end_time - self.ts_orig).whole_seconds().max(0) as u64,
            start_event_type: self.event_type.clone(),
            end_event_type: self.event_type.clone(),
            attributes: self.attributes.clone(),
        };

        DerivedOutput::windowed(payload, end_time, vec![self.event_id])
            .with_temporal_policy(SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version(SEMANTICS_VERSION)
            .with_equivalence_key(interval_id)
    }
}

impl From<LegacyFocusTransition> for StateObservation {
    fn from(input: LegacyFocusTransition) -> Self {
        let subject_id = input.window_id.clone().or_else(|| {
            input
                .window_class
                .as_ref()
                .map(|class| format!("class:{class}"))
        });
        let label = match (&input.window_class, &input.window_title) {
            (Some(class), Some(title)) if !title.is_empty() => Some(format!("{class}: {title}")),
            (Some(class), _) => Some(class.clone()),
            (_, Some(title)) if !title.is_empty() => Some(title.clone()),
            _ => None,
        };
        let mut attributes = BTreeMap::new();
        if let Some(window_id) = input.window_id {
            attributes.insert("window_id".to_string(), window_id);
        }
        if let Some(window_class) = input.window_class {
            attributes.insert("window_class".to_string(), window_class);
        }
        if let Some(window_title) = input.window_title {
            attributes.insert("window_title".to_string(), window_title);
        }
        if let Some(workspace_id) = input.workspace_id {
            attributes.insert("workspace_id".to_string(), workspace_id.to_string());
        }

        Self {
            state_kind: FOCUS_STATE_KIND.to_string(),
            event_id: input.event_id,
            ts_orig: input.ts_orig,
            subject_id,
            label,
            event_type: HyprlandWindowFocusedPayload::EVENT_TYPE
                .as_static_str()
                .to_string(),
            attributes,
        }
    }
}

fn deserialize_active_focus<'de, D>(
    deserializer: D,
) -> Result<Option<StateObservation>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(value) = Option::<JsonValue>::deserialize(deserializer)? else {
        return Ok(None);
    };

    match serde_json::from_value::<StateObservation>(value.clone()) {
        Ok(observation) => Ok(Some(observation)),
        Err(current_error) => serde_json::from_value::<LegacyFocusTransition>(value)
            .map(StateObservation::from)
            .map(Some)
            .map_err(|legacy_error| {
                serde::de::Error::custom(format!(
                    "failed to decode interval-lift active_focus as current ({current_error}) or legacy focus state ({legacy_error})"
                ))
            }),
    }
}

impl Transducer for IntervalLift {
    type State = IntervalLiftState;
    type Input = JsonValue;
    type Output = StateIntervalPayload;

    fn name(&self) -> &'static str {
        "interval-lift"
    }

    fn input_event_type(&self) -> &'static str {
        "*"
    }

    fn input_event_types(&self) -> Vec<&'static str> {
        vec![
            HyprlandWindowFocusedPayload::EVENT_TYPE.as_static_str(),
            ActivityWatchWindowActivePayload::EVENT_TYPE.as_static_str(),
            ActivityWatchAfkChangedPayload::EVENT_TYPE.as_static_str(),
        ]
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
        let (slot, current) =
            match (context.source.as_str(), context.event_type.as_str()) {
                ("wm.hyprland", event_type)
                    if event_type == HyprlandWindowFocusedPayload::EVENT_TYPE.as_static_str() =>
                {
                    let payload: HyprlandWindowFocusedPayload = serde_json::from_value(input)
                        .map_err(|e| {
                            AutomatonLogicError::InputParsing(format!(
                                "failed to parse Hyprland focus payload: {e}"
                            ))
                        })?;
                    (
                        &mut state.active_focus,
                        StateObservation::from_focus_payload(payload, context)?,
                    )
                }
                ("activitywatch", event_type)
                    if event_type
                        == ActivityWatchWindowActivePayload::EVENT_TYPE.as_static_str() =>
                {
                    let payload: ActivityWatchWindowActivePayload =
                        serde_json::from_value(input).map_err(|e| {
                            AutomatonLogicError::InputParsing(format!(
                                "failed to parse ActivityWatch window payload: {e}"
                            ))
                        })?;
                    let duration_ms = payload.duration_ms;
                    let observation =
                        StateObservation::from_activitywatch_window_payload(payload, context)?;
                    return Ok(Some(
                        observation.observed_duration_interval(duration_ms, Timestamp::now()),
                    ));
                }
                ("activitywatch", event_type)
                    if event_type == ActivityWatchAfkChangedPayload::EVENT_TYPE.as_static_str() =>
                {
                    let payload: ActivityWatchAfkChangedPayload =
                        serde_json::from_value(input).map_err(|e| {
                            AutomatonLogicError::InputParsing(format!(
                                "failed to parse ActivityWatch AFK payload: {e}"
                            ))
                        })?;
                    let duration_ms = payload.duration_ms;
                    let observation =
                        StateObservation::from_activitywatch_afk_payload(payload, context)?;
                    return Ok(Some(
                        observation.observed_duration_interval(duration_ms, Timestamp::now()),
                    ));
                }
                _ => return Ok(None),
            };

        let Some(previous) = slot.as_mut() else {
            *slot = Some(current);
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
        *slot = Some(current.clone());

        Ok(Some(previous.close_with(&current)))
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
