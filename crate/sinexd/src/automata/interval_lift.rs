//! Interval lift automaton -- stateful transition-to-interval derivation.
//!
//! The first rule lifts Hyprland focus transitions into generic
//! `state.interval` events. Additional point/transition sources should add
//! rules here instead of creating source-specific interval silos.

use crate::runtime::automaton::{DerivedOutput, MultiOutputTransducerAdapter};
use crate::runtime::{
    AutomatonContext, AutomatonLogicError, InputProvenanceFilter, MultiOutputTransducer,
};
use serde::{Deserialize, Deserializer, Serialize};
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::events::payloads::{
    ActivityWatchAfkChangedPayload, ActivityWatchWindowActivePayload, HyprlandWindowFocusedPayload,
    HyprlandWorkspaceSwitchedPayload, StateIntervalPayload, SystemdUnitStartedPayload,
    SystemdUnitStoppedPayload,
};
use sinex_primitives::events::EventPayload;
use sinex_primitives::temporal::Duration;
use sinex_primitives::{JsonValue, Timestamp, Uuid};
use std::collections::BTreeMap;
use tracing::warn;

/// sinex-uzc(d): cap on concurrently-open systemd unit states. A unit that never
/// emits a stop cannot grow `active_subject_states` without bound — the stalest
/// open start is evicted (with a debt warning) once this many are open.
const MAX_ACTIVE_SUBJECT_STATES: usize = 1024;

const SEMANTICS_VERSION: &str = "2.0.0";
const FOCUS_STATE_KIND: &str = "desktop.focus";
const WORKSPACE_STATE_KIND: &str = "desktop.workspace";
const ACTIVITYWATCH_WINDOW_STATE_KIND: &str = "desktop.activitywatch.window";
const ACTIVITYWATCH_AFK_STATE_KIND: &str = "desktop.activitywatch.afk";
const SYSTEMD_UNIT_STATE_KIND: &str = "system.systemd.unit";
const WINDOW_FOCUSED_EVENT_TYPE: &str = "window.focused";
const WORKSPACE_SWITCHED_EVENT_TYPE: &str = "workspace.switched";
const WINDOW_ACTIVE_EVENT_TYPE: &str = "window.active";
const AFK_CHANGED_EVENT_TYPE: &str = "afk.changed";
const UNIT_STARTED_EVENT_TYPE: &str = "unit.started";
const UNIT_STOPPED_EVENT_TYPE: &str = "unit.stopped";
const ACTIVITYWATCH_HEARTBEAT_MERGE_WINDOW_SECS: i64 = 30;
/// sinex-zs6: a heartbeat bout ends this many seconds after its LAST observed
/// heartbeat (one merge-window of tolerance), never at the next post-gap event —
/// heartbeat absence is absence of evidence, not continued activity.
const ACTIVITYWATCH_HEARTBEAT_END_SLACK_SECS: i64 = ACTIVITYWATCH_HEARTBEAT_MERGE_WINDOW_SECS;

/// sinex-5s6 cross-kind fences. A fence event ends open desktop *attention*
/// intervals at its own `ts_orig`, so an interval does not require a next
/// same-kind event to become queryable (a focus interval no longer spans an
/// overnight suspend as one 9-hour block).
const FENCE_SUSPEND_EVENT_TYPE: &str = "fence.suspend";
const FENCE_AFK_EVENT_TYPE: &str = "fence.afk";
const AFK_STATUS_AFK: &str = "afk";

/// systemd units whose *start* marks the machine suspending/hibernating — a
/// desktop-attention fence. (Boot would fence too, but no boot event payload is
/// captured yet; tracked on sinex-5s6.)
const SUSPEND_UNIT_NAMES: &[&str] = &[
    "sleep.target",
    "suspend.target",
    "systemd-suspend.service",
    "hibernate.target",
    "systemd-hibernate.service",
    "hybrid-sleep.target",
    "systemd-hybrid-sleep.service",
    "suspend-then-hibernate.target",
    "systemd-suspend-then-hibernate.service",
];

fn is_suspend_unit(unit_name: &str) -> bool {
    SUSPEND_UNIT_NAMES.contains(&unit_name)
}

/// The cross-kind fence an input represents, if any (sinex-5s6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fence {
    /// Machine suspend/hibernate: fences focus, workspace, and AW heartbeats.
    Suspend,
    /// AFK transition: fences focus and workspace (the AFK interval itself is
    /// still lifted normally).
    Afk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IntervalLiftRuleShape {
    AdjacentTransitions,
    ObservedDuration,
    StartStopPair,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct IntervalLiftRule {
    pub(crate) source: &'static str,
    pub(crate) event_types: &'static [&'static str],
    pub(crate) state_kind: &'static str,
    pub(crate) shape: IntervalLiftRuleShape,
    pub(crate) consumer_hint: &'static str,
}

const INTERVAL_LIFT_RULES: &[IntervalLiftRule] = &[
    IntervalLiftRule {
        source: "wm.hyprland",
        event_types: &[WINDOW_FOCUSED_EVENT_TYPE],
        state_kind: FOCUS_STATE_KIND,
        shape: IntervalLiftRuleShape::AdjacentTransitions,
        consumer_hint: "attention.stream/screen.grounding/machine.context",
    },
    IntervalLiftRule {
        source: "wm.hyprland",
        event_types: &[WORKSPACE_SWITCHED_EVENT_TYPE],
        state_kind: WORKSPACE_STATE_KIND,
        shape: IntervalLiftRuleShape::AdjacentTransitions,
        consumer_hint: "attention.stream/work.episode/machine.context",
    },
    IntervalLiftRule {
        source: "activitywatch",
        event_types: &[WINDOW_ACTIVE_EVENT_TYPE],
        state_kind: ACTIVITYWATCH_WINDOW_STATE_KIND,
        shape: IntervalLiftRuleShape::ObservedDuration,
        consumer_hint: "attention.stream/work.episode/project.attribution",
    },
    IntervalLiftRule {
        source: "activitywatch",
        event_types: &[AFK_CHANGED_EVENT_TYPE],
        state_kind: ACTIVITYWATCH_AFK_STATE_KIND,
        shape: IntervalLiftRuleShape::ObservedDuration,
        consumer_hint: "attention.stream/work.episode/machine.context",
    },
    IntervalLiftRule {
        source: "systemd",
        event_types: &[UNIT_STARTED_EVENT_TYPE, UNIT_STOPPED_EVENT_TYPE],
        state_kind: SYSTEMD_UNIT_STATE_KIND,
        shape: IntervalLiftRuleShape::StartStopPair,
        consumer_hint: "machine.context/change.episode/ops.forensics",
    },
];

/// Derivation control-plane declaration for `interval-lift` (sinex-0vx.1/0vx.3).
///
/// `output_event_types()` returns a single-element list (`["state.interval"]`)
/// despite the `MultiOutputTransducer` model — the model is used for its
/// one-input-to-many-outputs shape (a fence event closes several open
/// intervals at once), not for multiple distinct event *types*.
pub const INTERVAL_LIFT_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[DerivationOutputDeclaration {
        declaration_id: "interval-lift.state.interval",
        owner: "interval-lift",
        product_class: DerivedProductClass::CanonicalDerivedEvent,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("derived.interval-lift"),
        output_event_type: Some("state.interval"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: SEMANTICS_VERSION,
        input_eligibility: InputEligibility::DefaultCanonicalInput,
        default_support: ClaimSupportTemplate::new(
            SupportLevel::Direct,
            SourceCoverage::Covered,
            ClaimTemporalQuality::WindowBoundary,
        ),
        verification_command: "xtask test -p sinexd -E 'test(interval_lift)'",
    }];

#[derive(Debug, Clone, Default)]
pub struct IntervalLift;

impl IntervalLift {
    pub(crate) fn rule_catalog() -> &'static [IntervalLiftRule] {
        INTERVAL_LIFT_RULES
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IntervalLiftState {
    #[serde(default, deserialize_with = "deserialize_active_focus")]
    active_focus: Option<StateObservation>,
    #[serde(default)]
    active_workspace: Option<StateObservation>,
    /// Open intervals keyed by rule-specific state identity. Use this for
    /// transition pairs that can have multiple independent subjects open at once.
    #[serde(default)]
    active_subject_states: BTreeMap<String, StateObservation>,
    /// Open zero-duration ActivityWatch heartbeat intervals keyed by
    /// state-kind/bucket. ActivityWatch raw rows can be heartbeat markers rather
    /// than complete intervals; keep the repair in the derived interval layer.
    #[serde(default)]
    active_activitywatch_heartbeats: BTreeMap<String, StateObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateObservation {
    state_kind: String,
    /// Trigger event interpretation id — parent lineage, NOT identity (sinex-ecy).
    event_id: Uuid,
    /// Material occurrence of the trigger event — the start-anchored occurrence
    /// identity for the interval's equivalence key (sinex-ecy). `None` for legacy
    /// / synthetic triggers with no material provenance.
    #[serde(default)]
    material_id: Option<Uuid>,
    #[serde(default)]
    anchor_byte: Option<i64>,
    ts_orig: Timestamp,
    /// Last observed evidence timestamp within a heartbeat bout (sinex-zs6).
    /// Merging advances this; heartbeat gap-tolerance and bout-end are measured
    /// against it, not the bout start (`ts_orig`). `None` == not advanced (equals
    /// `ts_orig`).
    #[serde(default)]
    last_seen: Option<Timestamp>,
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
            material_id: context.trigger_material_id,
            anchor_byte: context.trigger_anchor_byte,
            last_seen: None,
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
            material_id: context.trigger_material_id,
            anchor_byte: context.trigger_anchor_byte,
            last_seen: None,
            ts_orig: context.require_ts_orig()?,
            subject_id,
            label,
            event_type: ActivityWatchWindowActivePayload::EVENT_TYPE
                .as_static_str()
                .to_string(),
            attributes,
        })
    }

    fn from_workspace_payload(
        input: HyprlandWorkspaceSwitchedPayload,
        context: &AutomatonContext,
    ) -> Result<Self, AutomatonLogicError> {
        let subject_id = format!("workspace:{}", input.to_workspace_id);
        let label = input
            .workspace_name
            .clone()
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| input.to_workspace_id.to_string());
        let mut attributes = BTreeMap::new();
        attributes.insert(
            "to_workspace_id".to_string(),
            input.to_workspace_id.to_string(),
        );
        if let Some(workspace_name) = &input.workspace_name {
            attributes.insert("workspace_name".to_string(), workspace_name.clone());
        }
        if let Some(from_workspace_id) = input.from_workspace_id {
            attributes.insert(
                "from_workspace_id".to_string(),
                from_workspace_id.to_string(),
            );
        }
        if let Some(monitor_id) = input.monitor_id {
            attributes.insert("monitor_id".to_string(), monitor_id.to_string());
        }
        if let Some(active_window_id) = &input.active_window_id {
            attributes.insert("active_window_id".to_string(), active_window_id.clone());
        }

        Ok(Self {
            state_kind: WORKSPACE_STATE_KIND.to_string(),
            event_id: context.trigger_uuid(),
            material_id: context.trigger_material_id,
            anchor_byte: context.trigger_anchor_byte,
            last_seen: None,
            ts_orig: context.require_ts_orig()?,
            subject_id: Some(subject_id),
            label: Some(label),
            event_type: HyprlandWorkspaceSwitchedPayload::EVENT_TYPE
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
            material_id: context.trigger_material_id,
            anchor_byte: context.trigger_anchor_byte,
            last_seen: None,
            ts_orig: context.require_ts_orig()?,
            subject_id,
            label,
            event_type: ActivityWatchAfkChangedPayload::EVENT_TYPE
                .as_static_str()
                .to_string(),
            attributes,
        })
    }

    fn from_systemd_started_payload(
        input: SystemdUnitStartedPayload,
        context: &AutomatonContext,
    ) -> Result<Self, AutomatonLogicError> {
        let mut attributes = BTreeMap::new();
        attributes.insert("unit_name".to_string(), input.unit_name.clone());
        attributes.insert("unit_type".to_string(), input.unit_type.to_string());
        attributes.insert("active_state".to_string(), input.active_state.to_string());
        attributes.insert("sub_state".to_string(), input.sub_state);
        if let Some(main_pid) = input.main_pid {
            attributes.insert("main_pid".to_string(), main_pid.to_string());
        }

        Ok(Self {
            state_kind: SYSTEMD_UNIT_STATE_KIND.to_string(),
            event_id: context.trigger_uuid(),
            material_id: context.trigger_material_id,
            anchor_byte: context.trigger_anchor_byte,
            last_seen: None,
            ts_orig: context.require_ts_orig()?,
            subject_id: Some(input.unit_name.clone()),
            label: Some(input.unit_name),
            event_type: SystemdUnitStartedPayload::EVENT_TYPE
                .as_static_str()
                .to_string(),
            attributes,
        })
    }

    fn from_systemd_stopped_payload(
        input: SystemdUnitStoppedPayload,
        context: &AutomatonContext,
    ) -> Result<Self, AutomatonLogicError> {
        let mut attributes = BTreeMap::new();
        attributes.insert("unit_name".to_string(), input.unit_name.clone());
        attributes.insert("unit_type".to_string(), input.unit_type.to_string());
        attributes.insert("active_state".to_string(), input.active_state.to_string());
        attributes.insert("sub_state".to_string(), input.sub_state);
        if let Some(exit_code) = input.exit_code {
            attributes.insert("exit_code".to_string(), exit_code.to_string());
        }

        Ok(Self {
            state_kind: SYSTEMD_UNIT_STATE_KIND.to_string(),
            event_id: context.trigger_uuid(),
            material_id: context.trigger_material_id,
            anchor_byte: context.trigger_anchor_byte,
            last_seen: None,
            ts_orig: context.require_ts_orig()?,
            subject_id: Some(input.unit_name.clone()),
            label: Some(input.unit_name),
            event_type: SystemdUnitStoppedPayload::EVENT_TYPE
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

    /// Last observed evidence timestamp of the current bout (defaults to the
    /// bout start when no merge has advanced it yet).
    fn last_seen(&self) -> Timestamp {
        self.last_seen.unwrap_or(self.ts_orig)
    }

    fn refresh_metadata_from(&mut self, other: &Self) {
        if other.label.is_some() {
            self.label.clone_from(&other.label);
        }
        self.attributes.extend(other.attributes.clone());
        // sinex-zs6: advance last_seen so a continuous heartbeat stream is ONE
        // bout — gap tolerance is measured from the last heartbeat, not the first,
        // and the bout ends near the last heartbeat rather than the next event.
        self.last_seen = Some(self.last_seen().max(other.ts_orig));
    }

    fn activitywatch_stream_key(&self) -> String {
        let bucket = self
            .attributes
            .get("bucket_id")
            .map(String::as_str)
            .unwrap_or("unknown-bucket");
        format!("{}:{bucket}", self.state_kind)
    }

    fn activitywatch_gap_to(&self, current: &Self) -> i64 {
        (current.ts_orig - self.last_seen()).whole_seconds()
    }

    fn close_with(&self, current: &Self) -> DerivedOutput<StateIntervalPayload> {
        let duration_secs = (current.ts_orig - self.ts_orig).whole_seconds().max(0) as u64;
        let subject_part = self
            .subject_id
            .as_deref()
            .unwrap_or("unknown-subject")
            .to_string();
        let interval_id = interval_occurrence_key(
            &self.state_kind,
            &subject_part,
            self.material_id,
            self.anchor_byte,
            self.ts_orig,
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

        let declaration = &INTERVAL_LIFT_OUTPUT_DECLARATIONS[0];
        DerivedOutput::windowed(
            payload,
            current.ts_orig,
            vec![self.event_id, current.event_id],
        )
        .with_temporal_policy(SyntheticTemporalPolicy::WindowBoundary)
        .with_semantics_version(SEMANTICS_VERSION)
        .with_equivalence_key(interval_id)
        .with_declaration_id(declaration.declaration_id)
        .with_product_class(declaration.product_class)
        .with_claim_support(declaration.default_support.instantiate(2, 0, 1, 0))
    }

    /// Close this open interval at an external cross-kind fence (sinex-5s6): a
    /// suspend or AFK-transition event ends the interval at the fence `ts_orig`,
    /// not at the next same-kind event. The fence event is a parent alongside
    /// the interval's own start. Occurrence identity stays start-anchored (ecy),
    /// so a fence-closed interval carries a stable equivalence key.
    fn close_at_fence(
        &self,
        fence_ts: Timestamp,
        fence_event_type: &str,
        fence_event_id: Uuid,
    ) -> DerivedOutput<StateIntervalPayload> {
        let duration_secs = (fence_ts - self.ts_orig).whole_seconds().max(0) as u64;
        let subject_part = self
            .subject_id
            .as_deref()
            .unwrap_or("unknown-subject")
            .to_string();
        let interval_id = interval_occurrence_key(
            &self.state_kind,
            &subject_part,
            self.material_id,
            self.anchor_byte,
            self.ts_orig,
        );

        let payload = StateIntervalPayload {
            interval_id: interval_id.clone(),
            state_kind: self.state_kind.clone(),
            subject_id: self.subject_id.clone(),
            label: self.label.clone(),
            start_time: self.ts_orig,
            end_time: fence_ts,
            duration_secs,
            start_event_type: self.event_type.clone(),
            end_event_type: fence_event_type.to_string(),
            attributes: self.attributes.clone(),
        };

        let declaration = &INTERVAL_LIFT_OUTPUT_DECLARATIONS[0];
        DerivedOutput::windowed(payload, fence_ts, vec![self.event_id, fence_event_id])
            .with_temporal_policy(SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version(SEMANTICS_VERSION)
            .with_equivalence_key(interval_id)
            .with_declaration_id(declaration.declaration_id)
            .with_product_class(declaration.product_class)
            .with_claim_support(declaration.default_support.instantiate(2, 0, 1, 0))
    }

    /// Close a heartbeat bout at the LAST observed heartbeat plus a bounded slack
    /// (sinex-zs6), NOT at the next post-gap event's ts_orig. Only the bout's own
    /// start event is a parent — the post-gap event belongs to the next bout.
    fn close_heartbeat_bout(&self, slack_secs: i64) -> DerivedOutput<StateIntervalPayload> {
        let end_time = self.last_seen() + Duration::seconds(slack_secs);
        let duration_secs = (end_time - self.ts_orig).whole_seconds().max(0) as u64;
        let subject_part = self
            .subject_id
            .as_deref()
            .unwrap_or("unknown-subject")
            .to_string();
        let interval_id = interval_occurrence_key(
            &self.state_kind,
            &subject_part,
            self.material_id,
            self.anchor_byte,
            self.ts_orig,
        );

        let payload = StateIntervalPayload {
            interval_id: interval_id.clone(),
            state_kind: self.state_kind.clone(),
            subject_id: self.subject_id.clone(),
            label: self.label.clone(),
            start_time: self.ts_orig,
            end_time,
            duration_secs,
            start_event_type: self.event_type.clone(),
            end_event_type: self.event_type.clone(),
            attributes: self.attributes.clone(),
        };

        let declaration = &INTERVAL_LIFT_OUTPUT_DECLARATIONS[0];
        DerivedOutput::windowed(payload, end_time, vec![self.event_id])
            .with_temporal_policy(SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version(SEMANTICS_VERSION)
            .with_equivalence_key(interval_id)
            .with_declaration_id(declaration.declaration_id)
            .with_product_class(declaration.product_class)
            .with_claim_support(declaration.default_support.instantiate(1, 0, 1, 0))
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
        let interval_id = interval_occurrence_key(
            &self.state_kind,
            &subject_part,
            self.material_id,
            self.anchor_byte,
            self.ts_orig,
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

        let declaration = &INTERVAL_LIFT_OUTPUT_DECLARATIONS[0];
        DerivedOutput::windowed(payload, end_time, vec![self.event_id])
            .with_temporal_policy(SyntheticTemporalPolicy::WindowBoundary)
            .with_semantics_version(SEMANTICS_VERSION)
            .with_equivalence_key(interval_id)
            .with_declaration_id(declaration.declaration_id)
            .with_product_class(declaration.product_class)
            .with_claim_support(declaration.default_support.instantiate(1, 0, 1, 0))
    }
}

/// Start-anchored occurrence identity for a `state.interval` (sinex-ecy / y8v):
/// identity is the material occurrence of the interval's START evidence. Ends move
/// (an interval can be learned longer/later); starts do not — so the end is
/// deliberately excluded from the key, and a revised interval keeps its identity
/// (superseded, not duplicated). Never the parent event interpretation ids (which
/// re-mint every replay and collide). The `:`-delimited format never collides with
/// the old `interval:...:{event_id}:{event_id}` keys, so the migration causes no
/// false suppression. Timestamp fallback keeps identity occurrence-derived (never a
/// counter) for legacy/synthetic starts lacking material coordinates.
fn interval_occurrence_key(
    state_kind: &str,
    subject_part: &str,
    material_id: Option<Uuid>,
    anchor_byte: Option<i64>,
    start: Timestamp,
) -> String {
    match (material_id, anchor_byte) {
        (Some(id), Some(anchor)) => format!("interval:{state_kind}:{subject_part}:{id}:{anchor}"),
        _ => format!("interval:{state_kind}:{subject_part}:ts:{start}"),
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
            material_id: None,
            anchor_byte: None,
            last_seen: None,
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

impl MultiOutputTransducer for IntervalLift {
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
        Self::rule_catalog()
            .iter()
            .flat_map(|rule| rule.event_types.iter().copied())
            .collect()
    }

    fn output_event_types(&self) -> &[&'static str] {
        &["state.interval"]
    }

    fn output_event_source(&self) -> &'static str {
        StateIntervalPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::MaterialOnly
    }

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] =
        INTERVAL_LIFT_OUTPUT_DECLARATIONS;

    /// Multi-output entry (sinex-5s6). Most inputs still map 1:1 through
    /// `process_single`, but a cross-kind fence (machine suspend, AFK
    /// transition) closes SEVERAL open desktop-attention intervals at once, so
    /// interval-lift is a `MultiOutputTransducer`.
    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        match Self::classify_fence(&input, context) {
            Some(Fence::Suspend) => {
                let fence_ts = context.require_ts_orig()?;
                let fence_id = context.trigger_uuid();
                // A suspend ends user-attention intervals; machine-state systemd
                // unit intervals legitimately span the suspend, so the sleep unit
                // is NOT also lifted as a subject interval here.
                Ok(Self::fence_desktop_attention(
                    state,
                    fence_ts,
                    FENCE_SUSPEND_EVENT_TYPE,
                    fence_id,
                ))
            }
            Some(Fence::Afk) => {
                let fence_ts = context.require_ts_orig()?;
                let fence_id = context.trigger_uuid();
                let mut outputs =
                    Self::fence_focus_workspace(state, fence_ts, FENCE_AFK_EVENT_TYPE, fence_id);
                // The AFK interval itself is still lifted via the normal path.
                outputs.extend(self.process_single(state, input, context).await?);
                Ok(outputs)
            }
            None => Ok(self
                .process_single(state, input, context)
                .await?
                .into_iter()
                .collect()),
        }
    }
}

impl IntervalLift {
    /// Classify an input as a cross-kind fence (sinex-5s6) by peeking the raw
    /// JSON — without consuming `input`, so the non-fence and AFK paths can
    /// still parse it in `process_single`.
    fn classify_fence(input: &JsonValue, context: &AutomatonContext) -> Option<Fence> {
        let source = context.source.as_str();
        let event_type = context.event_type.as_str();
        if source == "systemd"
            && event_type == SystemdUnitStartedPayload::EVENT_TYPE.as_static_str()
            && input
                .get("unit_name")
                .and_then(|v| v.as_str())
                .is_some_and(is_suspend_unit)
        {
            return Some(Fence::Suspend);
        }
        if source == "activitywatch"
            && event_type == ActivityWatchAfkChangedPayload::EVENT_TYPE.as_static_str()
            && input.get("status").and_then(|v| v.as_str()) == Some(AFK_STATUS_AFK)
        {
            return Some(Fence::Afk);
        }
        None
    }

    /// Close the open focus and workspace intervals at the fence (sinex-5s6). An
    /// interval that started AFTER the fence ts (out-of-order arrival) is left
    /// open rather than closed with a negative span.
    fn fence_focus_workspace(
        state: &mut IntervalLiftState,
        fence_ts: Timestamp,
        fence_event_type: &str,
        fence_event_id: Uuid,
    ) -> Vec<DerivedOutput<StateIntervalPayload>> {
        let mut outputs = Vec::new();
        for slot in [&mut state.active_focus, &mut state.active_workspace] {
            if slot.as_ref().is_some_and(|o| o.ts_orig <= fence_ts) {
                let observation = slot.take().expect("checked Some above");
                outputs.push(observation.close_at_fence(fence_ts, fence_event_type, fence_event_id));
            }
        }
        outputs
    }

    /// Close all open desktop-attention intervals — focus, workspace, and
    /// ActivityWatch heartbeat bouts — at the fence (sinex-5s6, suspend).
    fn fence_desktop_attention(
        state: &mut IntervalLiftState,
        fence_ts: Timestamp,
        fence_event_type: &str,
        fence_event_id: Uuid,
    ) -> Vec<DerivedOutput<StateIntervalPayload>> {
        let mut outputs =
            Self::fence_focus_workspace(state, fence_ts, fence_event_type, fence_event_id);
        let heartbeats = std::mem::take(&mut state.active_activitywatch_heartbeats);
        for (key, observation) in heartbeats {
            if observation.ts_orig <= fence_ts {
                outputs.push(observation.close_at_fence(
                    fence_ts,
                    fence_event_type,
                    fence_event_id,
                ));
            } else {
                // Opened after the fence (out-of-order) — keep it open.
                state.active_activitywatch_heartbeats.insert(key, observation);
            }
        }
        outputs
    }

    /// Single-transition lift (0/1 output): the original transducer logic,
    /// unchanged. The `MultiOutputTransducer::process` above wraps it, adding
    /// cross-kind fence closure.
    async fn process_single(
        &mut self,
        state: &mut IntervalLiftState,
        input: JsonValue,
        context: &AutomatonContext,
    ) -> Result<Option<DerivedOutput<StateIntervalPayload>>, AutomatonLogicError> {
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
                ("wm.hyprland", event_type)
                    if event_type
                        == HyprlandWorkspaceSwitchedPayload::EVENT_TYPE.as_static_str() =>
                {
                    let payload: HyprlandWorkspaceSwitchedPayload =
                        serde_json::from_value(input).map_err(|e| {
                            AutomatonLogicError::InputParsing(format!(
                                "failed to parse Hyprland workspace payload: {e}"
                            ))
                        })?;
                    (
                        &mut state.active_workspace,
                        StateObservation::from_workspace_payload(payload, context)?,
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
                    if duration_ms == 0 {
                        return Ok(Self::process_activitywatch_heartbeat(
                            &mut state.active_activitywatch_heartbeats,
                            observation,
                        ));
                    }
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
                    if duration_ms == 0 {
                        return Ok(Self::process_activitywatch_heartbeat(
                            &mut state.active_activitywatch_heartbeats,
                            observation,
                        ));
                    }
                    return Ok(Some(
                        observation.observed_duration_interval(duration_ms, Timestamp::now()),
                    ));
                }
                ("systemd", event_type)
                    if event_type == SystemdUnitStartedPayload::EVENT_TYPE.as_static_str() =>
                {
                    let payload: SystemdUnitStartedPayload =
                        serde_json::from_value(input).map_err(|e| {
                            AutomatonLogicError::InputParsing(format!(
                                "failed to parse systemd unit-start payload: {e}"
                            ))
                        })?;
                    let observation =
                        StateObservation::from_systemd_started_payload(payload, context)?;
                    let Some(subject_id) = observation.subject_id.clone() else {
                        return Ok(None);
                    };
                    // sinex-uzc(c): start-after-start for the same subject — the new
                    // start implies the previous instance ran until now; emit an implied
                    // close (restart fence) instead of silently discarding the open start.
                    let restart_close = state
                        .active_subject_states
                        .get(&subject_id)
                        .filter(|previous| observation.ts_orig > previous.ts_orig)
                        .map(|previous| previous.close_with(&observation));
                    // sinex-uzc(d): bound the map — evict the stalest open start (with a
                    // debt warning) before inserting a genuinely new subject at capacity.
                    if !state.active_subject_states.contains_key(&subject_id)
                        && state.active_subject_states.len() >= MAX_ACTIVE_SUBJECT_STATES
                    {
                        if let Some(stalest) = state
                            .active_subject_states
                            .iter()
                            .min_by_key(|(_, obs)| obs.ts_orig)
                            .map(|(key, _)| key.clone())
                        {
                            warn!(
                                module = "interval-lift",
                                evicted = %stalest,
                                cap = MAX_ACTIVE_SUBJECT_STATES,
                                "interval-lift evicted stalest open systemd state (durable debt, unbounded-growth guard)"
                            );
                            state.active_subject_states.remove(&stalest);
                        }
                    }
                    state.active_subject_states.insert(subject_id, observation);
                    return Ok(restart_close);
                }
                ("systemd", event_type)
                    if event_type == SystemdUnitStoppedPayload::EVENT_TYPE.as_static_str() =>
                {
                    let payload: SystemdUnitStoppedPayload =
                        serde_json::from_value(input).map_err(|e| {
                            AutomatonLogicError::InputParsing(format!(
                                "failed to parse systemd unit-stop payload: {e}"
                            ))
                        })?;
                    let current =
                        StateObservation::from_systemd_stopped_payload(payload, context)?;
                    let Some(subject_id) = current.subject_id.clone() else {
                        return Ok(None);
                    };
                    let Some(previous) = state.active_subject_states.remove(&subject_id) else {
                        // sinex-uzc: a stop with no open start — record debt, don't
                        // silently drop (missed/duplicate boundary).
                        warn!(
                            module = "interval-lift",
                            subject = %subject_id,
                            "interval-lift saw a systemd stop with no open start (durable debt)"
                        );
                        return Ok(None);
                    };
                    // sinex-uzc(b): a stop with ts <= start is a tie/out-of-order
                    // boundary; close a zero-duration interval (end clamped to start)
                    // rather than discarding the matched start+stop and losing it.
                    let stop = if current.ts_orig < previous.ts_orig {
                        StateObservation {
                            ts_orig: previous.ts_orig,
                            ..current
                        }
                    } else {
                        current
                    };
                    return Ok(Some(previous.close_with(&stop)));
                }
                _ => return Ok(None),
            };

        let Some(previous) = slot.as_mut() else {
            *slot = Some(current);
            return Ok(None);
        };

        match current.ts_orig.cmp(&previous.ts_orig) {
            std::cmp::Ordering::Greater => {}
            std::cmp::Ordering::Equal => {
                // sinex-uzc(a): tie — two transitions at the same instant. Deterministic
                // tiebreak: the later-processed observation supersedes the open state
                // in place; no zero-duration interval is emitted.
                *previous = current;
                return Ok(None);
            }
            std::cmp::Ordering::Less => {
                // sinex-uzc(a): a transition older than the open state cannot fold into a
                // forward stream without reordering — record durable debt (warn), never a
                // silent drop.
                warn!(
                    module = "interval-lift",
                    state_kind = %current.state_kind,
                    current_ts = %current.ts_orig,
                    open_ts = %previous.ts_orig,
                    "interval-lift skipped out-of-order transition (durable debt)"
                );
                return Ok(None);
            }
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

impl IntervalLift {
    fn process_activitywatch_heartbeat(
        active: &mut BTreeMap<String, StateObservation>,
        current: StateObservation,
    ) -> Option<DerivedOutput<StateIntervalPayload>> {
        let key = current.activitywatch_stream_key();
        let Some(previous) = active.get_mut(&key) else {
            active.insert(key, current);
            return None;
        };

        if current.ts_orig <= previous.ts_orig {
            if previous.same_subject_as(&current) {
                previous.refresh_metadata_from(&current);
            }
            return None;
        }

        let same_subject = previous.same_subject_as(&current);
        let gap_secs = previous.activitywatch_gap_to(&current);
        if same_subject && gap_secs <= ACTIVITYWATCH_HEARTBEAT_MERGE_WINDOW_SECS {
            previous.refresh_metadata_from(&current);
            return None;
        }

        // sinex-zs6: a same-subject gap beyond the window means the heartbeat stream
        // stopped — absence of a heartbeat is absence of evidence, so end the bout at
        // last_seen + slack, not at the next post-gap event. A DIFFERENT subject is a
        // direct switch: the transition itself is evidence the previous state ended
        // now, so end at the switch ts.
        let closed = if same_subject {
            previous.close_heartbeat_bout(ACTIVITYWATCH_HEARTBEAT_END_SLACK_SECS)
        } else {
            previous.close_with(&current)
        };
        active.insert(key, current);
        Some(closed)
    }
}

/// RuntimeModule type alias registered via `AutomatonSpec` in `automata::registry`.
pub type IntervalLiftRuntime = MultiOutputTransducerAdapter<IntervalLift>;

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
            "(state_kind, subject_id, start_material_id, start_anchor_byte)",
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
