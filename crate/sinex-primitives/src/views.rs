//! Shared human/agent view DTOs.

use crate::domain::{OperationKind, OperationStatus};
use crate::events::Event;
use crate::ids::Id;
use crate::query::{Cursor, QueryResultEvent};
use crate::rpc::dlq::{DlqListResponse, DlqMessagePeek};
use crate::rpc::lifecycle::LifecycleStatusResponse;
use crate::rpc::replay::{ReplayOperation, ReplayState};
use crate::rpc::sources::{SourceReadiness, SourceShapeDriftObservation};
use crate::sources::continuity::{SourceContinuityReport, SourcesExplainGapResponse};
use crate::temporal::Timestamp;
use crate::{JsonValue, Provenance};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const VIEW_ENVELOPE_SCHEMA_VERSION: &str = "sinex.view-envelope/v3";
pub const CONTEXT_SUMMARY_SCHEMA_VERSION: &str = "sinex.context-summary/v1";
pub const DESKTOP_CONTEXT_VIEW_SCHEMA_VERSION: &str = "sinex.desktop-context-view/v1";
pub const DESKTOP_FOCUS_SESSION_LIST_SCHEMA_VERSION: &str = "sinex.desktop-focus-session-list/v1";
pub const DESKTOP_NOTIFICATION_PRESSURE_SCHEMA_VERSION: &str =
    "sinex.desktop-notification-pressure/v1";
pub const DESKTOP_PROJECT_CONTEXT_LIST_SCHEMA_VERSION: &str =
    "sinex.desktop-project-context-list/v1";
pub const EVENT_CARD_LIST_SCHEMA_VERSION: &str = "sinex.event-card-list/v3";
pub const EVENT_ERROR_LIST_SCHEMA_VERSION: &str = "sinex.event-error-list/v1";
pub const EVENT_QUERY_LIST_SCHEMA_VERSION: &str = "sinex.event-query-list/v1";
pub const DEBT_LIST_SCHEMA_VERSION: &str = "sinex.debt-list/v1";
pub const OPERATION_JOB_LIST_SCHEMA_VERSION: &str = "sinex.operation-job-list/v1";
pub const OPERATION_CONTROL_CARD_SCHEMA_VERSION: &str = "sinex.operation-control-card/v1";
pub const OPERATION_VIEW_SCHEMA_VERSION: &str = "sinex.operation-view/v1";
pub const SOURCE_CONTINUITY_DETAIL_SCHEMA_VERSION: &str = "sinex.source-continuity-detail/v1";
pub const SOURCE_CONTINUITY_GAP_SCHEMA_VERSION: &str = "sinex.source-continuity-gap/v1";
pub const SOURCE_CONTINUITY_LIST_SCHEMA_VERSION: &str = "sinex.source-continuity-list/v1";
pub const SOURCE_COVERAGE_LIST_SCHEMA_VERSION: &str = "sinex.source-coverage-list/v1";
pub const SOURCE_DRIFT_LIST_SCHEMA_VERSION: &str = "sinex.source-drift-list/v1";
pub const SOURCE_READINESS_DETAIL_SCHEMA_VERSION: &str = "sinex.source-readiness-detail/v1";
pub const SOURCE_READINESS_LIST_SCHEMA_VERSION: &str = "sinex.source-readiness-list/v1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SinexObjectKind {
    Event,
    SourceDriver,
    SourceMaterial,
    MaterialAnchor,
    Document,
    DocumentChunk,
    Task,
    SemanticLane,
    SemanticEntity,
    SemanticRelation,
    Operation,
    Projection,
    Artifact,
    QueryRun,
    AdmissionOutcome,
    DebtRow,
    Proposal,
    Judgment,
    ExternalRef,
    Policy,
    ReplayRun,
    Snapshot,
    DlqMessage,
    ContextPack,
    MomentCandidate,
    PrivacySession,
    Caveat,
    RpcMethod,
    RuntimeModule,
    Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DesktopContextOutputKind {
    CurrentView,
    FocusSessionProjection,
    ProjectContextProjection,
    NotificationPressureProjection,
    EvidenceWindowView,
    ContextReportArtifact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DesktopContextInputState {
    Included,
    Omitted,
    Redacted,
    Stale,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DesktopContextInputEvidence {
    pub family: String,
    pub state: DesktopContextInputState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DesktopContextCandidateView {
    pub label: String,
    pub confidence: f32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal_ref: Option<SinexObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DesktopFocusSessionView {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<Timestamp>,
    pub event_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_families: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DesktopFocusSessionListView {
    pub schema_version: String,
    pub output_kind: DesktopContextOutputKind,
    pub derivation_ref: String,
    pub output_id: String,
    pub generated_at: Timestamp,
    pub since: String,
    pub session_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sessions: Vec<DesktopFocusSessionView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

impl DesktopFocusSessionListView {
    #[must_use]
    pub fn new(derivation_ref: impl Into<String>, since: impl Into<String>) -> Self {
        Self {
            schema_version: DESKTOP_FOCUS_SESSION_LIST_SCHEMA_VERSION.to_string(),
            output_kind: DesktopContextOutputKind::FocusSessionProjection,
            derivation_ref: derivation_ref.into(),
            output_id: "desktop.focus_session".to_string(),
            generated_at: Timestamp::now(),
            since: since.into(),
            session_count: 0,
            sessions: Vec::new(),
            caveats: Vec::new(),
            actions: vec![
                ActionAvailability::read(
                    "desktop.focus_session.explain",
                    "Explain",
                    ActionAvailabilityState::Enabled,
                )
                .with_command_hint(
                    "sinexctl events context --desktop --focus-sessions --format json",
                ),
            ],
        }
    }

    #[must_use]
    pub fn into_envelope(self, source_surface: impl Into<String>) -> ViewEnvelope<Self> {
        let mut envelope = ViewEnvelope::new(source_surface, self);
        envelope.caveats = envelope.payload.caveats.clone();
        envelope.actions = envelope.payload.actions.clone();
        envelope
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DesktopNotificationPressureView {
    pub schema_version: String,
    pub output_kind: DesktopContextOutputKind,
    pub derivation_ref: String,
    pub output_id: String,
    pub generated_at: Timestamp,
    pub since: String,
    pub sent_count: usize,
    pub action_count: usize,
    pub closed_count: usize,
    pub total_notification_events: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

impl DesktopNotificationPressureView {
    #[must_use]
    pub fn new(derivation_ref: impl Into<String>, since: impl Into<String>) -> Self {
        Self {
            schema_version: DESKTOP_NOTIFICATION_PRESSURE_SCHEMA_VERSION.to_string(),
            output_kind: DesktopContextOutputKind::NotificationPressureProjection,
            derivation_ref: derivation_ref.into(),
            output_id: "desktop.notification_pressure".to_string(),
            generated_at: Timestamp::now(),
            since: since.into(),
            sent_count: 0,
            action_count: 0,
            closed_count: 0,
            total_notification_events: 0,
            evidence_refs: Vec::new(),
            caveats: Vec::new(),
            actions: vec![
                ActionAvailability::read(
                    "desktop.notification_pressure.explain",
                    "Explain",
                    ActionAvailabilityState::Enabled,
                )
                .with_command_hint(
                    "sinexctl events context --desktop --notification-pressure --format json",
                ),
            ],
        }
    }

    #[must_use]
    pub fn into_envelope(self, source_surface: impl Into<String>) -> ViewEnvelope<Self> {
        let mut envelope = ViewEnvelope::new(source_surface, self);
        envelope.caveats = envelope.payload.caveats.clone();
        envelope.actions = envelope.payload.actions.clone();
        envelope
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DesktopProjectContextRowView {
    pub label: String,
    pub confidence: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focus_session_ref: Option<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_families: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal_ref: Option<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DesktopProjectContextListView {
    pub schema_version: String,
    pub output_kind: DesktopContextOutputKind,
    pub derivation_ref: String,
    pub output_id: String,
    pub generated_at: Timestamp,
    pub since: String,
    pub row_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<DesktopProjectContextRowView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

impl DesktopProjectContextListView {
    #[must_use]
    pub fn new(derivation_ref: impl Into<String>, since: impl Into<String>) -> Self {
        Self {
            schema_version: DESKTOP_PROJECT_CONTEXT_LIST_SCHEMA_VERSION.to_string(),
            output_kind: DesktopContextOutputKind::ProjectContextProjection,
            derivation_ref: derivation_ref.into(),
            output_id: "desktop.project_context".to_string(),
            generated_at: Timestamp::now(),
            since: since.into(),
            row_count: 0,
            rows: Vec::new(),
            caveats: Vec::new(),
            actions: vec![
                ActionAvailability::read(
                    "desktop.project_context.explain",
                    "Explain",
                    ActionAvailabilityState::Enabled,
                )
                .with_command_hint(
                    "sinexctl events context --desktop --project-contexts --format json",
                ),
            ],
        }
    }

    #[must_use]
    pub fn into_envelope(self, source_surface: impl Into<String>) -> ViewEnvelope<Self> {
        let mut envelope = ViewEnvelope::new(source_surface, self);
        envelope.caveats = envelope.payload.caveats.clone();
        envelope.actions = envelope.payload.actions.clone();
        envelope
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DesktopContextView {
    pub schema_version: String,
    pub output_kind: DesktopContextOutputKind,
    pub derivation_ref: String,
    pub output_id: String,
    pub generated_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focus_session_ref: Option<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_window_ref: Option<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<DesktopContextCandidateView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<DesktopContextInputEvidence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

impl DesktopContextView {
    #[must_use]
    pub fn current(
        derivation_ref: impl Into<String>,
        inputs: Vec<DesktopContextInputEvidence>,
    ) -> Self {
        Self {
            schema_version: DESKTOP_CONTEXT_VIEW_SCHEMA_VERSION.to_string(),
            output_kind: DesktopContextOutputKind::CurrentView,
            derivation_ref: derivation_ref.into(),
            output_id: "desktop.context.current_view".to_string(),
            generated_at: Timestamp::now(),
            focus_session_ref: None,
            active_workspace: None,
            active_window_ref: None,
            candidates: Vec::new(),
            inputs,
            caveats: Vec::new(),
            actions: vec![
                ActionAvailability::read(
                    "desktop.context.explain",
                    "Explain",
                    ActionAvailabilityState::Enabled,
                )
                .with_command_hint("sinexctl desktop context explain"),
                ActionAvailability::read(
                    "desktop.context.inspect",
                    "Inspect",
                    ActionAvailabilityState::Enabled,
                )
                .with_command_hint("sinexctl desktop context inspect"),
            ],
        }
    }

    #[must_use]
    pub fn with_caveat(
        mut self,
        id: impl Into<String>,
        message: impl Into<String>,
        ref_: Option<SinexObjectRef>,
    ) -> Self {
        self.caveats.push(CaveatView {
            id: id.into(),
            message: message.into(),
            ref_,
        });
        self
    }

    #[must_use]
    pub fn into_envelope(self, source_surface: impl Into<String>) -> ViewEnvelope<Self> {
        let mut envelope = ViewEnvelope::new(source_surface, self);
        envelope.caveats = envelope.payload.caveats.clone();
        envelope.actions = envelope.payload.actions.clone();
        envelope
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceCoverageReadiness {
    Ready,
    Proposed,
    MissingMaterial,
    MissingEvents,
    MissingBinding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceCoverageContinuity {
    Active,
    MaterialOnly,
    EventOnly,
    Gapped,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourcePrivacyPosture {
    pub tier: String,
    pub context: String,
    #[serde(default)]
    pub proposed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceResourceBudgetView {
    pub resource_profile: String,
    pub work_class: String,
    pub steady_memory_mib: u32,
    pub burst_memory_mib: u32,
    pub cpu_weight: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_bytes_per_sec: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_events_per_sec: Option<u32>,
    pub max_pending_material_bytes: u64,
    pub max_pending_candidates: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_unacked_transport_messages: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pressure_actions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CoverageGapView {
    pub kind: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceCoverageView {
    pub source_id: String,
    pub namespace: String,
    pub event_types: Vec<String>,
    pub readiness: SourceCoverageReadiness,
    pub continuity: SourceCoverageContinuity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_material_at: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_at: Option<Timestamp>,
    pub material_count: i64,
    pub event_count: i64,
    pub binding_count: usize,
    pub live_binding_count: usize,
    pub proposed_binding_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gaps: Vec<CoverageGapView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    pub privacy: SourcePrivacyPosture,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_budget: Option<SourceResourceBudgetView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceCoverageListView {
    pub schema_version: String,
    pub count: usize,
    pub sources: Vec<SourceCoverageView>,
}

impl SourceCoverageListView {
    #[must_use]
    pub fn new(sources: Vec<SourceCoverageView>) -> Self {
        let count = sources.len();
        Self {
            schema_version: SOURCE_COVERAGE_LIST_SCHEMA_VERSION.to_string(),
            count,
            sources,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceReadinessListView {
    pub schema_version: String,
    pub count: usize,
    pub sources: Vec<SourceReadiness>,
}

impl SourceReadinessListView {
    #[must_use]
    pub fn new(sources: Vec<SourceReadiness>) -> Self {
        let count = sources.len();
        Self {
            schema_version: SOURCE_READINESS_LIST_SCHEMA_VERSION.to_string(),
            count,
            sources,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DebtKind {
    Capture,
    Admission,
    Projection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DebtStage {
    Capturing,
    MaterialReady,
    CandidateRejected,
    CandidateQuarantined,
    CandidateDeferred,
    ProjectionStale,
    ArtifactInvalidated,
    OperationPending,
    OperationFailed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DebtOwnerView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_ref: Option<SinexObjectRef>,
}

impl DebtOwnerView {
    #[must_use]
    pub fn admission_policy(policy_ref: impl Into<String>) -> Self {
        Self {
            package_ref: None,
            mode_ref: None,
            policy_ref: Some(policy_ref.into()),
            operation_ref: None,
        }
    }

    #[must_use]
    pub fn operation(operation_ref: SinexObjectRef) -> Self {
        Self {
            package_ref: None,
            mode_ref: None,
            policy_ref: None,
            operation_ref: Some(operation_ref),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DebtRowView {
    pub id: String,
    pub kind: DebtKind,
    pub stage: DebtStage,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<SinexObjectRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<DebtOwnerView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness: Option<FreshnessView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DebtListView {
    pub schema_version: String,
    pub count: usize,
    pub rows: Vec<DebtRowView>,
}

impl DebtListView {
    #[must_use]
    pub fn new(rows: Vec<DebtRowView>) -> Self {
        let count = rows.len();
        Self {
            schema_version: DEBT_LIST_SCHEMA_VERSION.to_string(),
            count,
            rows,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceReadinessDetailView {
    pub schema_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceReadiness>,
}

impl SourceReadinessDetailView {
    #[must_use]
    pub fn new(source: Option<SourceReadiness>) -> Self {
        Self {
            schema_version: SOURCE_READINESS_DETAIL_SCHEMA_VERSION.to_string(),
            source,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceDriftListView {
    pub schema_version: String,
    pub count: usize,
    pub drifts: Vec<SourceShapeDriftObservation>,
}

impl SourceDriftListView {
    #[must_use]
    pub fn new(drifts: Vec<SourceShapeDriftObservation>) -> Self {
        let count = drifts.len();
        Self {
            schema_version: SOURCE_DRIFT_LIST_SCHEMA_VERSION.to_string(),
            count,
            drifts,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceContinuityListView {
    pub schema_version: String,
    pub count: usize,
    pub reports: Vec<SourceContinuityReport>,
}

impl SourceContinuityListView {
    #[must_use]
    pub fn new(reports: Vec<SourceContinuityReport>) -> Self {
        let count = reports.len();
        Self {
            schema_version: SOURCE_CONTINUITY_LIST_SCHEMA_VERSION.to_string(),
            count,
            reports,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceContinuityDetailView {
    pub schema_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<SourceContinuityReport>,
}

impl SourceContinuityDetailView {
    #[must_use]
    pub fn new(report: Option<SourceContinuityReport>) -> Self {
        Self {
            schema_version: SOURCE_CONTINUITY_DETAIL_SCHEMA_VERSION.to_string(),
            report,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceContinuityGapView {
    pub schema_version: String,
    pub explanation: SourcesExplainGapResponse,
}

impl SourceContinuityGapView {
    #[must_use]
    pub fn new(explanation: SourcesExplainGapResponse) -> Self {
        Self {
            schema_version: SOURCE_CONTINUITY_GAP_SCHEMA_VERSION.to_string(),
            explanation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SinexObjectRef {
    pub kind: SinexObjectKind,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_method: Option<String>,
}

impl SinexObjectRef {
    #[must_use]
    pub fn new(kind: SinexObjectKind, id: impl Into<String>) -> Self {
        Self {
            kind,
            id: id.into(),
            label: None,
            command_hint: None,
            rpc_method: None,
        }
    }

    #[must_use]
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    #[must_use]
    pub fn with_command_hint(mut self, command_hint: impl Into<String>) -> Self {
        self.command_hint = Some(command_hint.into());
        self
    }

    #[must_use]
    pub fn with_rpc_method(mut self, rpc_method: impl Into<String>) -> Self {
        self.rpc_method = Some(rpc_method.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionAvailabilityState {
    Enabled,
    Disabled,
    Target,
    Loading,
    Dangerous,
    Partial,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionSideEffect {
    Read,
    Compose,
    Write,
    Admin,
    Destructive,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActionAvailability {
    pub id: String,
    pub label: String,
    pub state: ActionAvailabilityState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_method: Option<String>,
    pub side_effect: ActionSideEffect,
    #[serde(default, skip_serializing_if = "is_false")]
    pub requires_confirmation: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub dry_run_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audit_output_ref: Option<SinexObjectRef>,
}

impl ActionAvailability {
    #[must_use]
    pub fn read(
        id: impl Into<String>,
        label: impl Into<String>,
        state: ActionAvailabilityState,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            state,
            reason: None,
            command_hint: None,
            rpc_method: None,
            side_effect: ActionSideEffect::Read,
            requires_confirmation: false,
            dry_run_available: false,
            audit_output_ref: None,
        }
    }

    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    #[must_use]
    pub fn with_command_hint(mut self, command: impl Into<String>) -> Self {
        self.command_hint = Some(command.into());
        self
    }

    #[must_use]
    pub fn with_rpc_method(mut self, rpc_method: impl Into<String>) -> Self {
        self.rpc_method = Some(rpc_method.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyStateKind {
    RawVisible,
    MetadataOnly,
    Redacted,
    Suppressed,
    PermissionDenied,
    PolicyBlocked,
    TombstonePending,
    ExportRestricted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PrivacyStateView {
    pub state: PrivacyStateKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl PrivacyStateView {
    #[must_use]
    pub fn raw_visible() -> Self {
        Self {
            state: PrivacyStateKind::RawVisible,
            reason: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CaveatView {
    pub id: String,
    pub message: String,
    #[serde(rename = "ref")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_: Option<SinexObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FreshnessView {
    pub generated_at: Timestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_after_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ViewEnvelope<T> {
    pub schema_version: String,
    pub view_id: String,
    pub generated_at: Timestamp,
    pub source_surface: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness: Option<FreshnessView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_echo: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub filters: JsonValue,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub privacy_state: Option<PrivacyStateView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
    pub payload: T,
}

impl<T> ViewEnvelope<T> {
    #[must_use]
    pub fn new(source_surface: impl Into<String>, payload: T) -> Self {
        let generated_at = Timestamp::now();
        Self {
            schema_version: VIEW_ENVELOPE_SCHEMA_VERSION.to_string(),
            view_id: Id::<ViewEnvelopeMarker>::new().to_string(),
            generated_at,
            source_surface: source_surface.into(),
            runtime_target: None,
            freshness: Some(FreshnessView {
                generated_at,
                stale_after_secs: None,
            }),
            query_echo: None,
            filters: JsonValue::Null,
            caveats: Vec::new(),
            privacy_state: None,
            actions: Vec::new(),
            payload,
        }
    }

    #[must_use]
    pub fn with_query_echo(mut self, query_echo: JsonValue) -> Self {
        self.query_echo = Some(query_echo);
        self
    }

    #[must_use]
    pub fn with_filters(mut self, filters: JsonValue) -> Self {
        self.filters = filters;
        self
    }
}

#[derive(Debug)]
pub struct ViewEnvelopeMarker;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventTimestampView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingested: Option<Timestamp>,
    pub quality: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventSourceView {
    pub family: String,
    pub raw: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<SinexObjectRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventOriginKind {
    Material,
    Derived,
    Declared,
    System,
    ExternalMirror,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventTraceRelation {
    SourceMaterial,
    MaterialAnchor,
    SourceEvent,
    QueryRun,
    Proposal,
    Judgment,
    Operation,
    ExternalRef,
    Policy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventTraceLink {
    pub relation: EventTraceRelation,
    pub target: SinexObjectRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventCardView {
    #[serde(rename = "ref")]
    pub ref_: SinexObjectRef,
    pub timestamp: EventTimestampView,
    pub source: EventSourceView,
    pub event_type: String,
    pub origin_kind: EventOriginKind,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_preview: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub material_refs: Vec<SinexObjectRef>,
    pub privacy_state: PrivacyStateView,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trace_refs: Vec<SinexObjectRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trace_links: Vec<EventTraceLink>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projection_badges: Vec<String>,
    pub actions: Vec<ActionAvailability>,
}

impl EventCardView {
    #[must_use]
    pub fn from_query_event(result: &QueryResultEvent) -> Self {
        let event = &result.event;
        let event_id = event.id.map(|id| id.to_string());
        let ref_ = event_ref(event_id.as_deref());
        let ingested = event.id.map(|id| id.timestamp());
        let quality = if event.ts_orig.is_some() {
            "original_timestamp".to_string()
        } else {
            "ingest_timestamp_fallback".to_string()
        };

        let provenance = provenance_view(&event.provenance);
        let mut caveats = Vec::new();
        if event.id.is_none() {
            caveats.push(CaveatView {
                id: "event.unpersisted".to_string(),
                message: "event has no stable persisted id".to_string(),
                ref_: None,
            });
        }
        if result.snippet.is_none() {
            caveats.push(CaveatView {
                id: "event.no_snippet".to_string(),
                message: "query result did not include a snippet".to_string(),
                ref_: event_id.as_deref().map(|id| event_ref(Some(id))),
            });
        }

        Self {
            ref_,
            timestamp: EventTimestampView {
                original: event.ts_orig,
                ingested,
                quality,
            },
            source: EventSourceView {
                family: source_family(event.source.as_str()),
                raw: event.source.to_string(),
                source_ref: Some(
                    SinexObjectRef::new(SinexObjectKind::SourceDriver, event.source.to_string())
                        .with_label(event.source.to_string()),
                ),
            },
            event_type: event.event_type.to_string(),
            origin_kind: provenance.origin_kind,
            summary: event_summary(event, result.snippet.as_deref()),
            payload_preview: payload_preview(&event.payload),
            material_refs: provenance.material_refs,
            privacy_state: PrivacyStateView::raw_visible(),
            caveats,
            trace_refs: provenance.trace_refs,
            trace_links: provenance.trace_links,
            projection_badges: projection_badges(event),
            actions: event_actions(event_id.as_deref()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventCardListView {
    pub schema_version: String,
    pub count: usize,
    pub cards: Vec<EventCardView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_estimate: Option<i64>,
}

impl EventCardListView {
    #[must_use]
    pub fn from_query_events(events: &[QueryResultEvent]) -> Self {
        Self::from_query_events_with_metadata(events, None, None)
    }

    #[must_use]
    pub fn from_query_events_with_metadata(
        events: &[QueryResultEvent],
        next_cursor: Option<Cursor>,
        total_estimate: Option<i64>,
    ) -> Self {
        Self {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: events.len(),
            cards: events.iter().map(EventCardView::from_query_event).collect(),
            next_cursor,
            total_estimate,
        }
    }

    #[must_use]
    pub fn with_query_metadata(
        mut self,
        next_cursor: Option<Cursor>,
        total_estimate: Option<i64>,
    ) -> Self {
        self.next_cursor = next_cursor;
        self.total_estimate = total_estimate;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ContextSourceView {
    pub source: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_ts: Option<Timestamp>,
    pub latest_event: EventCardView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ContextSummaryView {
    pub schema_version: String,
    pub since: String,
    pub total_events: usize,
    pub source_count: usize,
    pub sources: Vec<ContextSourceView>,
}

impl ContextSummaryView {
    #[must_use]
    pub fn new(
        since: impl Into<String>,
        total_events: usize,
        sources: Vec<ContextSourceView>,
    ) -> Self {
        Self {
            schema_version: CONTEXT_SUMMARY_SCHEMA_VERSION.to_string(),
            since: since.into(),
            total_events,
            source_count: sources.len(),
            sources,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventQueryListView {
    pub schema_version: String,
    pub count: usize,
    pub cards: Vec<EventCardView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_estimate: Option<i64>,
}

impl EventQueryListView {
    #[must_use]
    pub fn from_query_events(
        events: &[QueryResultEvent],
        next_cursor: Option<Cursor>,
        total_estimate: Option<i64>,
    ) -> Self {
        Self {
            schema_version: EVENT_QUERY_LIST_SCHEMA_VERSION.to_string(),
            count: events.len(),
            cards: events.iter().map(EventCardView::from_query_event).collect(),
            next_cursor,
            total_estimate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventErrorListView {
    pub schema_version: String,
    pub since: String,
    pub count: usize,
    pub cards: Vec<EventCardView>,
}

impl EventErrorListView {
    #[must_use]
    pub fn from_query_events(since: impl Into<String>, events: &[QueryResultEvent]) -> Self {
        Self {
            schema_version: EVENT_ERROR_LIST_SCHEMA_VERSION.to_string(),
            since: since.into(),
            count: events.len(),
            cards: events.iter().map(EventCardView::from_query_event).collect(),
        }
    }
}

fn event_ref(event_id: Option<&str>) -> SinexObjectRef {
    let id = event_id.unwrap_or("unpersisted");
    let mut ref_ = SinexObjectRef::new(SinexObjectKind::Event, id).with_label(short_id(id));
    if event_id.is_some() {
        ref_ = ref_
            .with_command_hint(format!("sinexctl events trace {id}"))
            .with_rpc_method("events.lineage");
    }
    ref_
}

struct EventProvenanceView {
    origin_kind: EventOriginKind,
    material_refs: Vec<SinexObjectRef>,
    trace_refs: Vec<SinexObjectRef>,
    trace_links: Vec<EventTraceLink>,
}

fn provenance_view(provenance: &Provenance) -> EventProvenanceView {
    match provenance {
        Provenance::Material {
            id,
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind,
        } => {
            let material = SinexObjectRef::new(SinexObjectKind::SourceMaterial, id.to_string())
                .with_label("source material");
            let anchor_label = match (offset_start, offset_end) {
                (Some(start), Some(end)) => format!("{} {start}..{end}", offset_kind.as_wire_str()),
                _ => format!("byte {anchor_byte}"),
            };
            let anchor = SinexObjectRef::new(
                SinexObjectKind::MaterialAnchor,
                format!("{id}:{anchor_byte}"),
            )
            .with_label(anchor_label);
            EventProvenanceView {
                origin_kind: EventOriginKind::Material,
                material_refs: vec![material.clone(), anchor.clone()],
                trace_refs: Vec::new(),
                trace_links: vec![
                    EventTraceLink {
                        relation: EventTraceRelation::SourceMaterial,
                        target: material,
                    },
                    EventTraceLink {
                        relation: EventTraceRelation::MaterialAnchor,
                        target: anchor,
                    },
                ],
            }
        }
        Provenance::Derived {
            source_event_ids,
            operation_id,
        } => {
            let event_refs = source_event_ids
                .iter()
                .map(|id| {
                    SinexObjectRef::new(SinexObjectKind::Event, id.to_string())
                        .with_label(short_id(&id.to_string()))
                        .with_command_hint(format!("sinexctl events trace {id}"))
                        .with_rpc_method("events.lineage")
                })
                .collect::<Vec<_>>();
            let mut trace_links = event_refs
                .iter()
                .cloned()
                .map(|target| EventTraceLink {
                    relation: EventTraceRelation::SourceEvent,
                    target,
                })
                .collect::<Vec<_>>();
            if let Some(operation_id) = operation_id {
                trace_links.push(EventTraceLink {
                    relation: EventTraceRelation::Operation,
                    target: SinexObjectRef::new(
                        SinexObjectKind::Operation,
                        operation_id.to_string(),
                    )
                    .with_label(short_id(&operation_id.to_string()))
                    .with_command_hint(format!("sinexctl ops log --operation-id {operation_id}"))
                    .with_rpc_method("ops.get"),
                });
            }
            EventProvenanceView {
                origin_kind: EventOriginKind::Derived,
                material_refs: Vec::new(),
                trace_refs: event_refs,
                trace_links,
            }
        }
    }
}

fn event_actions(event_id: Option<&str>) -> Vec<ActionAvailability> {
    match event_id {
        Some(id) => vec![
            ActionAvailability::read("event.trace", "Trace", ActionAvailabilityState::Enabled)
                .with_command_hint(format!("sinexctl events trace {id}"))
                .with_rpc_method("events.lineage"),
            ActionAvailability::read("event.inspect", "Inspect", ActionAvailabilityState::Target)
                .with_reason("multi-pane event inspector is tracked separately")
                .with_command_hint(format!("sinexctl events inspect {id}"))
                .with_rpc_method("events.query"),
        ],
        None => vec![
            ActionAvailability::read("event.trace", "Trace", ActionAvailabilityState::Unavailable)
                .with_reason("event has no stable persisted id"),
            ActionAvailability::read(
                "event.inspect",
                "Inspect",
                ActionAvailabilityState::Unavailable,
            )
            .with_reason("event has no stable persisted id"),
        ],
    }
}

fn projection_badges(event: &Event<JsonValue>) -> Vec<String> {
    let mut badges = vec![if event.is_synthesized_event() {
        "derived".to_string()
    } else {
        "material".to_string()
    }];
    if event.temporal_policy.is_some() {
        badges.push("temporal_policy".to_string());
    }
    if event.semantics_version.is_some() {
        badges.push("semantics_versioned".to_string());
    }
    badges
}

fn event_summary(event: &Event<JsonValue>, snippet: Option<&str>) -> String {
    if let Some(snippet) = snippet
        && !snippet.trim().is_empty()
    {
        return truncate_chars(snippet.trim(), 160);
    }
    match &event.payload {
        JsonValue::Object(map) => {
            for key in ["summary", "title", "command", "path", "message", "name"] {
                if let Some(JsonValue::String(value)) = map.get(key)
                    && !value.trim().is_empty()
                {
                    return truncate_chars(value.trim(), 160);
                }
            }
            format!("{} with {} payload field(s)", event.event_type, map.len())
        }
        JsonValue::String(value) => truncate_chars(value, 160),
        JsonValue::Null => event.event_type.to_string(),
        other => truncate_chars(&other.to_string(), 160),
    }
}

fn payload_preview(payload: &JsonValue) -> Option<JsonValue> {
    match payload {
        JsonValue::Null => None,
        JsonValue::Object(map) => {
            let mut preview = serde_json::Map::new();
            for (key, value) in map.iter().take(8) {
                preview.insert(key.clone(), preview_value(value));
            }
            Some(JsonValue::Object(preview))
        }
        other => Some(json!({ "value": preview_value(other) })),
    }
}

fn preview_value(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::String(s) => JsonValue::String(truncate_chars(s, 240)),
        JsonValue::Array(values) => {
            let mut preview = values.iter().take(5).map(preview_value).collect::<Vec<_>>();
            if values.len() > preview.len() {
                preview.push(json!({ "truncated_items": values.len() - preview.len() }));
            }
            JsonValue::Array(preview)
        }
        JsonValue::Object(map) => {
            let preview = map
                .iter()
                .take(5)
                .map(|(key, value)| (key.clone(), preview_value(value)))
                .collect();
            JsonValue::Object(preview)
        }
        other => other.clone(),
    }
}

fn source_family(source: &str) -> String {
    source
        .split(['.', '-', '_'])
        .next()
        .filter(|part| !part.is_empty())
        .unwrap_or(source)
        .to_string()
}

fn short_id(id: &str) -> String {
    if id.len() <= 8 {
        id.to_string()
    } else {
        id[..8].to_string()
    }
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let end = input
        .char_indices()
        .nth(keep)
        .map_or(input.len(), |(index, _)| index);
    format!("{}...", &input[..end])
}

fn is_false(value: &bool) -> bool {
    !*value
}

// ─────────────────────────────────────────────────────────────────────────────
// Operation views
// ─────────────────────────────────────────────────────────────────────────────

/// Read-only view of a single `core.operations_log` row rendered for operator
/// and agent consumption.
///
/// Wraps the raw `OperationRecord` from `sinex-db`, replacing the untyped
/// `operation_type: String` with the typed [`OperationKind`] registry and
/// surfacing stable, named fields without exposing DB-internal identifiers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OperationView {
    /// Stable hex ID of this operation (UUID, opaque to callers).
    pub id: String,
    /// Typed classification of the operation.
    pub kind: OperationKind,
    /// Actor that submitted the operation (actor_id from auth context).
    pub operator: String,
    /// Terminal result status of the operation.
    pub status: OperationStatus,
    /// Wall-clock duration in milliseconds, `null` while still running.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i32>,
    /// Human-readable result message set on completion or failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_message: Option<String>,
    /// JSONB scope payload that scoped this operation (e.g. event ID range).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<JsonValue>,
    /// Summary JSONB produced at completion, suitable for display.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview_summary: Option<JsonValue>,
    /// Quick-access action hints for operator UIs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
}

impl OperationView {
    /// Construct from the RPC `Operation` type from `sinex-primitives::rpc::ops`.
    ///
    /// Accepts the raw `operation_type` string and converts it to [`OperationKind`].
    #[must_use]
    pub fn from_rpc(
        id: String,
        operation_type: &str,
        operator: String,
        status: OperationStatus,
        duration_ms: Option<i32>,
        result_message: Option<String>,
        scope: Option<JsonValue>,
        preview_summary: Option<JsonValue>,
    ) -> Self {
        let kind = OperationKind::from(operation_type);
        let actions = operation_actions(&id, &kind, &status);
        Self {
            id,
            kind,
            operator,
            status,
            duration_ms,
            result_message,
            scope,
            preview_summary,
            actions,
        }
    }
}

fn operation_actions(
    id: &str,
    kind: &OperationKind,
    status: &OperationStatus,
) -> Vec<ActionAvailability> {
    let is_terminal = matches!(
        status,
        OperationStatus::Success | OperationStatus::Failed | OperationStatus::Cancelled
    );
    let can_cancel =
        !is_terminal && matches!(status, OperationStatus::Running | OperationStatus::Pending);

    vec![
        ActionAvailability::read("ops.show", "Show", ActionAvailabilityState::Enabled)
            .with_command_hint(format!("sinexctl ops get {id}")),
        ActionAvailability {
            id: "ops.cancel".to_string(),
            label: "Cancel".to_string(),
            state: if can_cancel {
                ActionAvailabilityState::Enabled
            } else {
                ActionAvailabilityState::Disabled
            },
            reason: if is_terminal {
                Some("operation is already in a terminal state".to_string())
            } else {
                None
            },
            command_hint: Some(format!("sinexctl ops cancel {id}")),
            rpc_method: None,
            side_effect: ActionSideEffect::Write,
            requires_confirmation: false,
            dry_run_available: false,
            audit_output_ref: None,
        },
        ActionAvailability {
            id: "ops.replay".to_string(),
            label: "Replay".to_string(),
            state: if matches!(kind, OperationKind::Replay)
                && matches!(status, OperationStatus::Failed | OperationStatus::Cancelled)
            {
                ActionAvailabilityState::Enabled
            } else {
                ActionAvailabilityState::Unavailable
            },
            reason: if !matches!(kind, OperationKind::Replay) {
                Some("replay action only available for replay operations".to_string())
            } else {
                None
            },
            command_hint: Some(format!("sinexctl ops replay submit --ref-op {id}")),
            rpc_method: None,
            side_effect: ActionSideEffect::Write,
            requires_confirmation: true,
            dry_run_available: true,
            audit_output_ref: None,
        },
    ]
}

/// Payload carried inside a [`ViewEnvelope`] for `sinexctl ops jobs list`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OperationJobListView {
    pub schema_version: String,
    pub count: usize,
    pub jobs: Vec<OperationView>,
}

/// Shared read-model card for operation-room style control panels.
///
/// This keeps TUI/MCP/CLI-facing operation panels on the same action grammar
/// even when the underlying RPC still has a domain-specific DTO.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OperationControlCardView {
    pub schema_version: String,
    pub title: String,
    pub authority: String,
    pub phase: String,
    pub progress: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub affected_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionAvailability>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audit_refs: Vec<String>,
}

impl OperationControlCardView {
    #[must_use]
    pub fn from_replay_operation(operation: &ReplayOperation) -> Self {
        let progress = format!(
            "{} / {} events, batch {}",
            operation.checkpoint.processed_events,
            operation.checkpoint.total_events,
            operation.checkpoint.batch_number
        );
        Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: format!("ops replay {}", operation.operation_id),
            authority: "write".to_string(),
            phase: format!("{:?}", operation.state).to_lowercase(),
            progress,
            affected_refs: replay_scope_refs(operation),
            caveats: replay_caveats(operation),
            actions: replay_actions(operation),
            audit_refs: vec![format!("sinexctl ops audit {}", operation.operation_id)],
        }
    }

    #[must_use]
    pub fn from_dlq_status(stats: &DlqListResponse) -> Self {
        let total = stats.total_messages;
        let bytes = stats.total_bytes;
        let mut caveats = Vec::new();
        if total > 0 {
            caveats.push(
                "requeue/purge is mutating; inspect peek output and source readiness first"
                    .to_string(),
            );
            caveats.push(format!(
                "pressure: {}, recommended action: {}",
                stats.pressure_level, stats.recommended_action
            ));
        }
        Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: "raw-ingest DLQ".to_string(),
            authority: if total > 0 { "admin" } else { "read" }.to_string(),
            phase: if total > 0 { "blocked" } else { "clear" }.to_string(),
            progress: format!("{total} message(s), {bytes} byte(s)"),
            affected_refs: vec![format!("seq {}..{}", stats.first_seq, stats.last_seq)],
            caveats,
            actions: dlq_actions(total > 0),
            audit_refs: vec!["sinexctl ops dlq list".to_string()],
        }
    }

    #[must_use]
    pub fn dlq_unavailable() -> Self {
        Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: "raw-ingest DLQ".to_string(),
            authority: "read".to_string(),
            phase: "unknown".to_string(),
            progress: "DLQ status unavailable".to_string(),
            affected_refs: Vec::new(),
            caveats: vec!["DLQ status has not loaded yet".to_string()],
            actions: vec![read_action(
                "dlq.list",
                "List",
                ActionAvailabilityState::Enabled,
                "sinexctl ops dlq list",
                "dlq.list",
            )],
            audit_refs: vec!["sinexctl ops dlq list".to_string()],
        }
    }

    #[must_use]
    pub fn from_automaton_dlq_message(message: &DlqMessagePeek) -> Option<Self> {
        if !is_automaton_material_dlq(message) {
            return None;
        }
        Some(Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: "automaton telemetry DLQ material gap".to_string(),
            authority: "admin".to_string(),
            phase: "blocked".to_string(),
            progress: format!(
                "sample seq {}, retry {}",
                message.sequence, message.retry_count
            ),
            affected_refs: vec![
                format!("subject: {}", message.subject),
                format!(
                    "original: {}",
                    message.original_subject.as_deref().unwrap_or("unknown")
                ),
                format!(
                    "failed event sample: {}",
                    truncate_chars(&message.payload_preview, 96)
                ),
            ],
            caveats: vec![
                "first-class DLQ class: likely missing source-material registration for derived telemetry".to_string(),
                "requeue will probably re-DLQ until the Source Readiness Cockpit row is fixed".to_string(),
                "downstream projections may miss automaton telemetry until repaired".to_string(),
            ],
            actions: vec![
                read_action(
                    "source.inspect",
                    "Inspect source",
                    ActionAvailabilityState::Enabled,
                    "sinexctl tui --tab sources",
                    "sources.coverage",
                ),
                read_action(
                    "dlq.peek",
                    "Peek",
                    ActionAvailabilityState::Enabled,
                    "sinexctl ops dlq peek --limit 10",
                    "dlq.peek",
                ),
                write_action(
                    "dlq.requeue.after_repair",
                    "Requeue after repair",
                    ActionAvailabilityState::Dangerous,
                    "sinexctl ops dlq requeue --all",
                    "dlq.requeue",
                    ActionSideEffect::Admin,
                )
                .with_reason("repair source-material registration before requeue"),
            ],
            audit_refs: vec!["Ref #1241 automaton telemetry DLQ verification".to_string()],
        })
    }

    #[must_use]
    pub fn from_lifecycle_status(status: &LifecycleStatusResponse) -> Self {
        Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: "ops lifecycle archive/restore/tombstone".to_string(),
            authority: "admin".to_string(),
            phase: "guarded".to_string(),
            progress: format!("{} event(s) across lifecycle tiers", status.total_events),
            affected_refs: status
                .tiers
                .iter()
                .map(|tier| {
                    format!(
                        "{:?}: {} event(s), {} source(s)",
                        tier.tier, tier.event_count, tier.distinct_sources
                    )
                })
                .collect(),
            caveats: vec![
                "archive/restore supports dry-run; tombstone is destructive and preview/approve gated"
                    .to_string(),
            ],
            actions: lifecycle_actions(),
            audit_refs: vec!["sinexctl ops lifecycle status".to_string()],
        }
    }

    #[must_use]
    pub fn lifecycle_unavailable() -> Self {
        Self {
            schema_version: OPERATION_CONTROL_CARD_SCHEMA_VERSION.to_string(),
            title: "ops lifecycle archive/restore/tombstone".to_string(),
            authority: "admin".to_string(),
            phase: "unknown".to_string(),
            progress: "lifecycle status unavailable".to_string(),
            affected_refs: Vec::new(),
            caveats: vec!["lifecycle status has not loaded yet".to_string()],
            actions: lifecycle_actions(),
            audit_refs: vec!["sinexctl ops lifecycle status".to_string()],
        }
    }
}

fn replay_scope_refs(operation: &ReplayOperation) -> Vec<String> {
    let scope = &operation.scope;
    let mut refs = vec![format!("source: {}", scope.source_name)];
    if let Some((start, end)) = &scope.time_window {
        refs.push(format!("time: {start} -> {end}"));
    }
    if let Some(materials) = &scope.material_filter {
        refs.push(format!("materials: {}", materials.len()));
    }
    if let Some(source_id) = &scope.source_id {
        refs.push(format!("source: {source_id}"));
    }
    if let Some(source_material_id) = &scope.source_material_id {
        refs.push(format!("source-material: {source_material_id}"));
    }
    if let Some(parser_id) = &scope.parser_id {
        refs.push(format!("parser: {parser_id}"));
    }
    refs
}

fn replay_caveats(operation: &ReplayOperation) -> Vec<String> {
    let mut caveats = Vec::new();
    if operation.scope.is_staged_source_scope() {
        caveats.push("staged-source replay: inspect source readiness before execute".to_string());
    }
    if !operation.state.is_terminal()
        && matches!(
            operation.state,
            ReplayState::Previewed | ReplayState::Approved | ReplayState::Executing
        )
    {
        caveats.push("mutating replay phase: confirmation/audit trail required".to_string());
    }
    if let Some(error) = &operation.error_details {
        caveats.push(format!("error: {}", truncate_chars(error, 96)));
    }
    caveats
}

fn replay_actions(operation: &ReplayOperation) -> Vec<ActionAvailability> {
    let id = &operation.operation_id;
    let mut actions = vec![
        read_action(
            "replay.watch",
            "Monitor",
            ActionAvailabilityState::Enabled,
            format!("sinexctl ops replay watch {id}"),
            "replay.status",
        ),
        read_action(
            "replay.status",
            "Status",
            ActionAvailabilityState::Enabled,
            format!("sinexctl ops replay status {id}"),
            "replay.status",
        ),
    ];
    match operation.state {
        ReplayState::Planning => actions.push(write_action(
            "replay.preview",
            "Preview",
            ActionAvailabilityState::Enabled,
            format!("sinexctl ops replay preview {id}"),
            "replay.preview",
            ActionSideEffect::Write,
        )),
        ReplayState::Previewed => actions.push(
            write_action(
                "replay.approve",
                "Confirm",
                ActionAvailabilityState::Dangerous,
                format!("sinexctl ops replay approve {id}"),
                "replay.approve",
                ActionSideEffect::Admin,
            )
            .with_reason("approval changes replay authority state"),
        ),
        ReplayState::Approved => actions.push(
            write_action(
                "replay.execute",
                "Execute",
                ActionAvailabilityState::Dangerous,
                format!("sinexctl ops replay execute {id}"),
                "replay.execute",
                ActionSideEffect::Admin,
            )
            .with_reason("execution mutates admitted events/projections"),
        ),
        ReplayState::Executing | ReplayState::Cancelling | ReplayState::Committing => actions.push(
            write_action(
                "replay.cancel",
                "Cancel",
                ActionAvailabilityState::Dangerous,
                format!("sinexctl ops replay cancel {id} --reason <reason>"),
                "replay.cancel",
                ActionSideEffect::Admin,
            )
            .with_reason("cancellation changes an active replay operation"),
        ),
        ReplayState::Completed | ReplayState::Failed | ReplayState::Cancelled => {}
    }
    actions
}

fn dlq_actions(has_messages: bool) -> Vec<ActionAvailability> {
    vec![
        read_action(
            "dlq.peek",
            "Peek",
            ActionAvailabilityState::Enabled,
            "sinexctl ops dlq peek --limit 10",
            "dlq.peek",
        ),
        write_action(
            "dlq.requeue",
            "Requeue",
            if has_messages {
                ActionAvailabilityState::Dangerous
            } else {
                ActionAvailabilityState::Disabled
            },
            "sinexctl ops dlq requeue --all",
            "dlq.requeue",
            ActionSideEffect::Admin,
        )
        .with_reason(if has_messages {
            "requeue mutates pending DLQ messages"
        } else {
            "DLQ is empty"
        }),
        write_action(
            "dlq.purge",
            "Purge",
            if has_messages {
                ActionAvailabilityState::Dangerous
            } else {
                ActionAvailabilityState::Disabled
            },
            "sinexctl ops dlq purge --confirm",
            "dlq.purge",
            ActionSideEffect::Destructive,
        )
        .with_reason(if has_messages {
            "purge deletes pending DLQ messages"
        } else {
            "DLQ is empty"
        }),
    ]
}

fn lifecycle_actions() -> Vec<ActionAvailability> {
    vec![
        write_action(
            "lifecycle.archive.dry_run",
            "Archive dry-run",
            ActionAvailabilityState::Enabled,
            "sinexctl ops lifecycle archive --limit 1000",
            "lifecycle.archive",
            ActionSideEffect::Admin,
        )
        .with_dry_run(),
        write_action(
            "lifecycle.restore.dry_run",
            "Restore dry-run",
            ActionAvailabilityState::Enabled,
            "sinexctl ops lifecycle restore <event-id>...",
            "lifecycle.restore",
            ActionSideEffect::Admin,
        )
        .with_dry_run(),
        write_action(
            "lifecycle.tombstone.preview",
            "Tombstone preview",
            ActionAvailabilityState::Dangerous,
            "sinexctl ops lifecycle tombstone preview <operation-id>",
            "lifecycle.tombstone.preview",
            ActionSideEffect::Destructive,
        )
        .with_reason("tombstone is destructive; preview before approve"),
        write_action(
            "lifecycle.tombstone.approve",
            "Tombstone approve",
            ActionAvailabilityState::Dangerous,
            "sinexctl ops lifecycle tombstone approve <operation-id>",
            "lifecycle.tombstone.approve",
            ActionSideEffect::Destructive,
        )
        .with_reason("approval commits a destructive tombstone operation"),
    ]
}

fn read_action(
    id: impl Into<String>,
    label: impl Into<String>,
    state: ActionAvailabilityState,
    command: impl Into<String>,
    rpc_method: impl Into<String>,
) -> ActionAvailability {
    ActionAvailability::read(id, label, state)
        .with_command_hint(command)
        .with_rpc_method(rpc_method)
}

fn write_action(
    id: impl Into<String>,
    label: impl Into<String>,
    state: ActionAvailabilityState,
    command: impl Into<String>,
    rpc_method: impl Into<String>,
    side_effect: ActionSideEffect,
) -> ActionAvailability {
    ActionAvailability {
        id: id.into(),
        label: label.into(),
        state,
        reason: None,
        command_hint: Some(command.into()),
        rpc_method: Some(rpc_method.into()),
        side_effect,
        requires_confirmation: matches!(
            side_effect,
            ActionSideEffect::Admin | ActionSideEffect::Destructive
        ),
        dry_run_available: false,
        audit_output_ref: None,
    }
}

trait ActionAvailabilityExt {
    fn with_dry_run(self) -> Self;
}

impl ActionAvailabilityExt for ActionAvailability {
    fn with_dry_run(mut self) -> Self {
        self.dry_run_available = true;
        self
    }
}

fn is_automaton_material_dlq(message: &DlqMessagePeek) -> bool {
    let haystack = format!(
        "{} {} {}",
        message.subject,
        message.original_subject.as_deref().unwrap_or_default(),
        message.payload_preview
    )
    .to_ascii_lowercase();
    haystack.contains("derived")
        && (haystack.contains("source_material")
            || haystack.contains("source material")
            || haystack.contains("material"))
}

impl OperationJobListView {
    #[must_use]
    pub fn new(jobs: Vec<OperationView>) -> Self {
        let count = jobs.len();
        Self {
            schema_version: OPERATION_JOB_LIST_SCHEMA_VERSION.to_string(),
            count,
            jobs,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::events::SourceMaterial;
    use crate::events::builder::{OperationMarker, Provenance};
    use crate::non_empty::NonEmptyVec;
    use crate::rpc::dlq::DlqPressureSignal;
    use crate::rpc::replay::{ReplayCheckpoint, ReplayScope};
    use crate::{EventSource, EventType, HostName};
    use std::collections::HashMap;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn event_card_preserves_refs_actions_and_payload_preview() -> xtask::TestResult<()> {
        let event_id = Id::<Event<JsonValue>>::new();
        let material_id = Id::<SourceMaterial>::new();
        let result = QueryResultEvent {
            event: Event {
                id: Some(event_id),
                source: EventSource::new("shell.atuin")?,
                event_type: EventType::new("command.executed")?,
                payload: json!({
                    "command": "xtask test -p sinex-primitives",
                    "cwd": "/realm/project/sinex",
                    "extra": [1, 2, 3, 4, 5, 6],
                }),
                ts_orig: Some(Timestamp::now()),
                ts_quality: None,
                host: HostName::new("sinnix-prime")?,
                module_run_id: None,
                payload_schema_id: None,
                provenance: Provenance::Material {
                    id: material_id,
                    anchor_byte: 42,
                    offset_start: None,
                    offset_end: None,
                    offset_kind: crate::OffsetKind::Byte,
                },
                associated_blob_ids: None,
                temporal_policy: None,
                semantics_version: None,
                scope_key: None,
                equivalence_key: None,
                created_by_operation_id: None,
                automaton_model: None,
                anchor_payload_hash: None,
            },
            relevance_score: Some(0.9),
            snippet: Some("ran a focused test".to_string()),
        };

        let card = EventCardView::from_query_event(&result);

        assert_eq!(card.ref_.kind, SinexObjectKind::Event);
        assert_eq!(card.ref_.id, event_id.to_string());
        assert_eq!(card.source.family, "shell");
        assert_eq!(card.origin_kind, EventOriginKind::Material);
        assert_eq!(card.summary, "ran a focused test");
        assert_eq!(card.material_refs.len(), 2);
        assert!(card.trace_refs.is_empty());
        assert_eq!(card.trace_links.len(), 2);
        assert_eq!(
            card.trace_links[0].relation,
            EventTraceRelation::SourceMaterial
        );
        assert_eq!(
            card.trace_links[1].relation,
            EventTraceRelation::MaterialAnchor
        );
        assert!(
            card.actions.iter().any(|action| action.id == "event.trace"
                && action.state == ActionAvailabilityState::Enabled)
        );
        assert!(
            card.actions
                .iter()
                .any(|action| action.id == "event.inspect"
                    && action.state == ActionAvailabilityState::Target
                    && action.reason.is_some())
        );
        assert!(card.payload_preview.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn operation_control_card_replay_execute_keeps_dangerous_action_reason()
    -> xtask::TestResult<()> {
        let operation = ReplayOperation {
            operation_id: "op-fixture".to_string(),
            state: ReplayState::Approved,
            scope: ReplayScope {
                source_name: "fixture.replay".to_string(),
                time_window: None,
                material_filter: None,
                filters: HashMap::new(),
                source_id: Some("source-fixture".to_string()),
                source_material_id: Some("material-fixture".to_string()),
                parser_id: Some("parser-fixture".to_string()),
                parser_version: None,
            },
            preview_summary: None,
            checkpoint: ReplayCheckpoint {
                processed_events: 42,
                total_events: 100,
                last_event_id: None,
                batch_number: 3,
                savepoint_id: None,
                updated_at: "2026-06-19T00:00:00Z".to_string(),
            },
            actor: "operator.local".to_string(),
            created_at: "2026-06-19T00:00:00Z".to_string(),
            approved_by: Some("operator.local".to_string()),
            approved_at: Some("2026-06-19T00:00:01Z".to_string()),
            executor_module: None,
            started_at: None,
            finished_at: None,
            outcome: None,
            error_details: None,
        };

        let card = OperationControlCardView::from_replay_operation(&operation);
        let execute = card
            .actions
            .iter()
            .find(|action| action.id == "replay.execute")
            .expect("approved replay exposes execute action");

        assert_eq!(card.phase, "approved");
        assert_eq!(execute.state, ActionAvailabilityState::Dangerous);
        assert_eq!(
            execute.command_hint.as_deref(),
            Some("sinexctl ops replay execute op-fixture")
        );
        assert!(
            execute
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("mutates admitted events"))
        );
        assert!(
            card.caveats
                .iter()
                .any(|caveat| caveat.contains("staged-source replay"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn operation_control_card_empty_dlq_disables_mutating_actions_with_reason()
    -> xtask::TestResult<()> {
        let card = OperationControlCardView::from_dlq_status(&DlqListResponse {
            total_messages: 0,
            total_bytes: 0,
            first_seq: 0,
            last_seq: 0,
            pressure_level: "nominal".to_string(),
            resource_pressure: DlqPressureSignal {
                pressure_level: "nominal".to_string(),
                runtime_action: "none".to_string(),
                pending_messages: 0,
                pending_bytes: 0,
                retry_batch_size: 10,
                recommended_action: "none".to_string(),
                reason: "raw-ingest DLQ is empty".to_string(),
            },
            pending_sequence_span: 0,
            recommended_action: "none".to_string(),
            action_reason: "raw-ingest DLQ is empty".to_string(),
        });

        let requeue = card
            .actions
            .iter()
            .find(|action| action.id == "dlq.requeue")
            .expect("DLQ card exposes requeue action");
        let purge = card
            .actions
            .iter()
            .find(|action| action.id == "dlq.purge")
            .expect("DLQ card exposes purge action");

        assert_eq!(card.phase, "clear");
        assert_eq!(requeue.state, ActionAvailabilityState::Disabled);
        assert_eq!(purge.state, ActionAvailabilityState::Disabled);
        assert_eq!(requeue.reason.as_deref(), Some("DLQ is empty"));
        assert_eq!(purge.reason.as_deref(), Some("DLQ is empty"));
        Ok(())
    }

    #[sinex_test]
    async fn event_card_splits_origin_kind_from_trace_links() -> xtask::TestResult<()> {
        let source_event_id = Id::<Event<JsonValue>>::new();
        let operation_id = Id::<OperationMarker>::new();
        let result = QueryResultEvent {
            event: Event {
                id: Some(Id::<Event<JsonValue>>::new()),
                source: EventSource::new("projection.context")?,
                event_type: EventType::new("context.updated")?,
                payload: json!({ "summary": "projection updated" }),
                ts_orig: None,
                ts_quality: None,
                host: HostName::new("sinnix-prime")?,
                module_run_id: None,
                payload_schema_id: None,
                provenance: Provenance::Derived {
                    source_event_ids: NonEmptyVec::single(source_event_id),
                    operation_id: Some(operation_id),
                },
                associated_blob_ids: None,
                temporal_policy: None,
                semantics_version: None,
                scope_key: None,
                equivalence_key: None,
                created_by_operation_id: None,
                automaton_model: None,
                anchor_payload_hash: None,
            },
            relevance_score: None,
            snippet: None,
        };

        let card = EventCardView::from_query_event(&result);

        assert_eq!(card.origin_kind, EventOriginKind::Derived);
        assert_eq!(card.trace_refs.len(), 1);
        assert_eq!(card.trace_refs[0].id, source_event_id.to_string());
        assert_eq!(card.trace_links.len(), 2);
        assert_eq!(
            card.trace_links[0].relation,
            EventTraceRelation::SourceEvent
        );
        assert_eq!(card.trace_links[0].target.id, source_event_id.to_string());
        assert_eq!(card.trace_links[1].relation, EventTraceRelation::Operation);
        assert_eq!(card.trace_links[1].target.id, operation_id.to_string());

        let roundtrip: EventCardView = serde_json::from_value(serde_json::to_value(&card)?)?;
        assert_eq!(roundtrip.origin_kind, EventOriginKind::Derived);
        assert_eq!(roundtrip.trace_links, card.trace_links);

        let unknown = serde_json::from_value::<EventOriginKind>(json!("mystery_origin"));
        assert!(unknown.is_err(), "unknown origin kind must fail loudly");

        Ok(())
    }

    #[sinex_test]
    async fn event_trace_relation_vocabulary_covers_issue_contract() -> xtask::TestResult<()> {
        let relations = [
            (EventTraceRelation::SourceMaterial, "source_material"),
            (EventTraceRelation::MaterialAnchor, "material_anchor"),
            (EventTraceRelation::SourceEvent, "source_event"),
            (EventTraceRelation::QueryRun, "query_run"),
            (EventTraceRelation::Proposal, "proposal"),
            (EventTraceRelation::Judgment, "judgment"),
            (EventTraceRelation::Operation, "operation"),
            (EventTraceRelation::ExternalRef, "external_ref"),
            (EventTraceRelation::Policy, "policy"),
        ];

        for (relation, wire) in relations {
            assert_eq!(serde_json::to_value(relation)?, json!(wire));
        }

        let object_kinds = [
            (SinexObjectKind::QueryRun, "query_run"),
            (SinexObjectKind::Proposal, "proposal"),
            (SinexObjectKind::Judgment, "judgment"),
            (SinexObjectKind::Operation, "operation"),
            (SinexObjectKind::ExternalRef, "external_ref"),
            (SinexObjectKind::Policy, "policy"),
        ];

        for (kind, wire) in object_kinds {
            assert_eq!(serde_json::to_value(kind)?, json!(wire));
        }

        Ok(())
    }

    #[sinex_test]
    async fn view_envelope_serializes_schema_version_and_payload() -> xtask::TestResult<()> {
        let envelope = ViewEnvelope::new(
            "sinexctl.recent",
            EventCardListView {
                schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
                count: 0,
                cards: Vec::new(),
                next_cursor: None,
                total_estimate: None,
            },
        )
        .with_query_echo(json!({ "since": "1h", "limit": 20 }));

        let value = serde_json::to_value(&envelope)?;
        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(value["source_surface"], "sinexctl.recent");
        assert_eq!(
            value["payload"]["schema_version"],
            EVENT_CARD_LIST_SCHEMA_VERSION
        );
        assert_eq!(value["payload"]["count"], 0);
        Ok(())
    }

    #[sinex_test]
    async fn source_coverage_list_view_serializes_status_shape() -> xtask::TestResult<()> {
        let view = SourceCoverageView {
            source_id: "fixture.source".to_string(),
            namespace: "fixture".to_string(),
            event_types: vec!["fixture/fixture.event".to_string()],
            readiness: SourceCoverageReadiness::Ready,
            continuity: SourceCoverageContinuity::Active,
            last_material_at: None,
            last_event_at: None,
            material_count: 2,
            event_count: 3,
            binding_count: 1,
            live_binding_count: 1,
            proposed_binding_count: 0,
            gaps: Vec::new(),
            caveats: Vec::new(),
            privacy: SourcePrivacyPosture {
                tier: "sensitive".to_string(),
                context: "command".to_string(),
                proposed: false,
            },
            resource_budget: Some(SourceResourceBudgetView {
                resource_profile: "bounded_stream".to_string(),
                work_class: "admission_hot".to_string(),
                steady_memory_mib: 256,
                burst_memory_mib: 512,
                cpu_weight: 100,
                max_input_bytes_per_sec: Some(32 * 1024 * 1024),
                max_input_events_per_sec: Some(10_000),
                max_pending_material_bytes: 128 * 1024 * 1024,
                max_pending_candidates: 25_000,
                max_unacked_transport_messages: Some(1_000),
                batch_size: Some(2_000),
                flush_interval_ms: Some(500),
                checkpoint_interval_ms: Some(2_000),
                pressure_actions: vec![
                    "throttle".to_string(),
                    "defer".to_string(),
                    "retry".to_string(),
                    "inspect".to_string(),
                ],
            }),
            actions: vec![ActionAvailability::read(
                "sources.readiness",
                "Readiness",
                ActionAvailabilityState::Enabled,
            )],
        };
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.status",
            SourceCoverageListView::new(vec![view]),
        );

        let value = serde_json::to_value(&envelope)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(
            value["payload"]["schema_version"],
            SOURCE_COVERAGE_LIST_SCHEMA_VERSION
        );
        assert_eq!(value["payload"]["sources"][0]["readiness"], "ready");
        assert_eq!(value["payload"]["sources"][0]["continuity"], "active");
        assert_eq!(
            value["payload"]["sources"][0]["resource_budget"]["work_class"],
            "admission_hot"
        );
        assert_eq!(
            value["payload"]["sources"][0]["resource_budget"]["pressure_actions"][2],
            "retry"
        );
        Ok(())
    }

    #[sinex_test]
    async fn debt_list_view_represents_admission_and_projection_debt() -> xtask::TestResult<()> {
        let admission_row = DebtRowView {
            id: "debt:admission:fixture".to_string(),
            kind: DebtKind::Admission,
            stage: DebtStage::CandidateQuarantined,
            summary: "candidate quarantined by admission policy".to_string(),
            refs: vec![
                SinexObjectRef::new(SinexObjectKind::SourceMaterial, "material:fixture"),
                SinexObjectRef::new(SinexObjectKind::AdmissionOutcome, "outcome:fixture"),
            ],
            owner: Some(DebtOwnerView::admission_policy("admission-policy:fixture")),
            age_secs: Some(42),
            freshness: None,
            caveats: vec![CaveatView {
                id: "admission.quarantined".to_string(),
                message: "operator action is required before admission can continue".to_string(),
                ref_: Some(SinexObjectRef::new(
                    SinexObjectKind::Policy,
                    "admission-policy:fixture",
                )),
            }],
            actions: vec![
                ActionAvailability::read(
                    "debt.inspect",
                    "Inspect",
                    ActionAvailabilityState::Enabled,
                )
                .with_command_hint("sinexctl ops debt inspect debt:admission:fixture"),
            ],
        };
        let projection_row = DebtRowView {
            id: "debt:projection:fixture".to_string(),
            kind: DebtKind::Projection,
            stage: DebtStage::ProjectionStale,
            summary: "projection is stale after replay".to_string(),
            refs: vec![SinexObjectRef::new(
                SinexObjectKind::Projection,
                "projection:fixture",
            )],
            owner: Some(DebtOwnerView::operation(SinexObjectRef::new(
                SinexObjectKind::Operation,
                "operation:rebuild-fixture",
            ))),
            age_secs: Some(300),
            freshness: Some(FreshnessView {
                generated_at: Timestamp::now(),
                stale_after_secs: Some(60),
            }),
            caveats: vec![CaveatView {
                id: "projection.stale".to_string(),
                message: "derived output needs rebuild".to_string(),
                ref_: Some(SinexObjectRef::new(
                    SinexObjectKind::Artifact,
                    "artifact:fixture",
                )),
            }],
            actions: vec![ActionAvailability {
                id: "projection.rebuild".to_string(),
                label: "Rebuild".to_string(),
                state: ActionAvailabilityState::Enabled,
                reason: None,
                command_hint: Some(
                    "sinexctl ops replay submit --ref projection:fixture".to_string(),
                ),
                rpc_method: None,
                side_effect: ActionSideEffect::Write,
                requires_confirmation: true,
                dry_run_available: true,
                audit_output_ref: None,
            }],
        };

        let envelope = ViewEnvelope::new(
            "sinexctl.ops.debt",
            DebtListView::new(vec![admission_row, projection_row]),
        );
        let value = serde_json::to_value(&envelope)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(value["payload"]["schema_version"], DEBT_LIST_SCHEMA_VERSION);
        assert_eq!(value["payload"]["count"], 2);
        assert_eq!(value["payload"]["rows"][0]["kind"], "admission");
        assert_eq!(
            value["payload"]["rows"][0]["stage"],
            "candidate_quarantined"
        );
        assert_eq!(
            value["payload"]["rows"][0]["refs"][1]["kind"],
            "admission_outcome"
        );
        assert_eq!(value["payload"]["rows"][1]["kind"], "projection");
        assert_eq!(value["payload"]["rows"][1]["stage"], "projection_stale");
        assert_eq!(
            value["payload"]["rows"][1]["owner"]["operation_ref"]["kind"],
            "operation"
        );
        assert_eq!(
            value["payload"]["rows"][1]["actions"][0]["side_effect"],
            "write"
        );
        Ok(())
    }

    #[sinex_test]
    async fn event_card_json_uses_contract_field_names() -> xtask::TestResult<()> {
        let result = QueryResultEvent {
            event: Event {
                id: None,
                source: EventSource::new("test.source")?,
                event_type: EventType::new("test.event")?,
                payload: json!({ "summary": "fixture summary" }),
                ts_orig: None,
                ts_quality: None,
                host: HostName::new("test-host")?,
                module_run_id: None,
                payload_schema_id: None,
                provenance: Provenance::Derived {
                    source_event_ids: NonEmptyVec::single(Id::<Event<JsonValue>>::new()),
                    operation_id: None,
                },
                associated_blob_ids: None,
                temporal_policy: None,
                semantics_version: None,
                scope_key: None,
                equivalence_key: None,
                created_by_operation_id: None,
                automaton_model: None,
                anchor_payload_hash: None,
            },
            relevance_score: None,
            snippet: None,
        };

        let value = serde_json::to_value(EventCardView::from_query_event(&result))?;
        assert!(value.get("ref").is_some());
        assert!(value.get("ref_").is_none());
        assert_eq!(value["summary"], "fixture summary");
        assert_eq!(value["actions"][0]["state"], "unavailable");
        assert!(value["actions"][0].get("reason").is_some());
        Ok(())
    }

    #[sinex_test]
    async fn desktop_context_view_carries_evidence_caveats_and_actions() -> xtask::TestResult<()> {
        let window_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:window-focused")
            .with_label("wm.hyprland · window.focused");
        let browser_coverage_ref =
            SinexObjectRef::new(SinexObjectKind::Projection, "source-coverage:browser.web")
                .with_label("browser.web coverage");
        let policy_ref = SinexObjectRef::new(
            SinexObjectKind::Policy,
            "disclosure-policy:desktop.context.view",
        );

        let view = DesktopContextView::current(
            crate::DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID,
            vec![
                DesktopContextInputEvidence {
                    family: "wm.hyprland".to_string(),
                    state: DesktopContextInputState::Included,
                    refs: vec![window_ref.clone()],
                    caveats: Vec::new(),
                    actions: Vec::new(),
                },
                DesktopContextInputEvidence {
                    family: "browser.web".to_string(),
                    state: DesktopContextInputState::Missing,
                    refs: vec![browser_coverage_ref.clone()],
                    caveats: vec![CaveatView {
                        id: "input.browser.missing".to_string(),
                        message: "browser context is unavailable for this view".to_string(),
                        ref_: Some(browser_coverage_ref.clone()),
                    }],
                    actions: vec![
                        ActionAvailability::read(
                            "sources.browser.check",
                            "Check Browser",
                            ActionAvailabilityState::Enabled,
                        )
                        .with_command_hint("sinexctl sources status --family browser"),
                    ],
                },
                DesktopContextInputEvidence {
                    family: "terminal.activity".to_string(),
                    state: DesktopContextInputState::Redacted,
                    refs: vec![policy_ref.clone()],
                    caveats: vec![CaveatView {
                        id: "input.terminal.redacted".to_string(),
                        message: "terminal command text is hidden by view disclosure policy"
                            .to_string(),
                        ref_: Some(policy_ref.clone()),
                    }],
                    actions: Vec::new(),
                },
            ],
        )
        .with_caveat(
            "context.partial",
            "desktop context is partial because one input family is unavailable",
            Some(browser_coverage_ref),
        );

        let value = serde_json::to_value(view.into_envelope("sinexctl.desktop.context.current"))?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(value["source_surface"], "sinexctl.desktop.context.current");
        assert_eq!(
            value["payload"]["schema_version"],
            DESKTOP_CONTEXT_VIEW_SCHEMA_VERSION
        );
        assert_eq!(value["payload"]["output_kind"], "current_view");
        assert_eq!(
            value["payload"]["derivation_ref"],
            crate::DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID
        );
        assert_eq!(value["payload"]["inputs"][0]["state"], "included");
        assert_eq!(value["payload"]["inputs"][1]["state"], "missing");
        assert_eq!(value["payload"]["inputs"][2]["state"], "redacted");
        assert_eq!(
            value["payload"]["inputs"][1]["actions"][0]["command_hint"],
            "sinexctl sources status --family browser"
        );
        assert_eq!(value["caveats"][0]["id"], "context.partial");
        assert_eq!(value["actions"][0]["id"], "desktop.context.explain");
        assert_eq!(window_ref.kind, SinexObjectKind::Event);
        Ok(())
    }

    #[sinex_test]
    async fn desktop_notification_pressure_view_carries_projection_contract()
    -> xtask::TestResult<()> {
        let event_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:notification-sent")
            .with_label("desktop.notification · notification.sent");
        let mut view = DesktopNotificationPressureView::new(
            crate::DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID,
            "2h",
        );
        view.sent_count = 1;
        view.total_notification_events = 1;
        view.evidence_refs.push(event_ref.clone());
        view.caveats.push(CaveatView {
            id: "notification_pressure.partial".to_string(),
            message: "fixture pressure view is partial".to_string(),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.notification_pressure",
            )),
        });

        let envelope = view
            .into_envelope("sinexctl.events.context.desktop.notification_pressure")
            .with_query_echo(json!({ "mode": "desktop_notification_pressure" }));
        let value = serde_json::to_value(&envelope)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(
            value["payload"]["schema_version"],
            DESKTOP_NOTIFICATION_PRESSURE_SCHEMA_VERSION
        );
        assert_eq!(
            value["payload"]["derivation_ref"],
            crate::DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID
        );
        assert_eq!(
            value["payload"]["output_kind"],
            "notification_pressure_projection"
        );
        assert_eq!(
            value["payload"]["output_id"],
            "desktop.notification_pressure"
        );
        assert_eq!(value["payload"]["evidence_refs"][0]["id"], event_ref.id);
        assert_eq!(value["caveats"][0]["id"], "notification_pressure.partial");
        Ok(())
    }

    #[sinex_test]
    async fn desktop_focus_session_list_carries_projection_contract() -> xtask::TestResult<()> {
        let window_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:window-focused")
            .with_label("wm.hyprland · window.focused");
        let terminal_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:command-executed")
            .with_label("shell.atuin · command.executed");
        let mut view =
            DesktopFocusSessionListView::new(crate::DESKTOP_FOCUS_SESSION_DERIVATION_ID, "2h");
        view.sessions.push(DesktopFocusSessionView {
            session_id: "desktop.focus_session:event:window-focused..event:command-executed"
                .to_string(),
            started_at: None,
            ended_at: None,
            event_count: 2,
            input_families: vec!["desktop".to_string(), "terminal".to_string()],
            evidence_refs: vec![window_ref.clone(), terminal_ref.clone()],
            caveats: vec![CaveatView {
                id: "focus_session.open_window".to_string(),
                message: "fixture focus session is still open".to_string(),
                ref_: Some(SinexObjectRef::new(
                    SinexObjectKind::Projection,
                    "desktop.focus_session",
                )),
            }],
        });
        view.session_count = view.sessions.len();

        let envelope = view
            .into_envelope("sinexctl.events.context.desktop.focus_sessions")
            .with_query_echo(json!({ "mode": "desktop_focus_sessions" }));
        let value = serde_json::to_value(&envelope)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(
            value["payload"]["schema_version"],
            DESKTOP_FOCUS_SESSION_LIST_SCHEMA_VERSION
        );
        assert_eq!(
            value["payload"]["derivation_ref"],
            crate::DESKTOP_FOCUS_SESSION_DERIVATION_ID
        );
        assert_eq!(value["payload"]["output_kind"], "focus_session_projection");
        assert_eq!(value["payload"]["output_id"], "desktop.focus_session");
        assert_eq!(value["payload"]["sessions"][0]["event_count"], 2);
        assert_eq!(
            value["payload"]["sessions"][0]["evidence_refs"][0]["id"],
            window_ref.id
        );
        assert_eq!(value["actions"][0]["id"], "desktop.focus_session.explain");
        Ok(())
    }

    #[sinex_test]
    async fn desktop_project_context_list_carries_projection_contract() -> xtask::TestResult<()> {
        let terminal_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:terminal-cwd")
            .with_label("shell.atuin · command.executed");
        let browser_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:browser-tab")
            .with_label("activitywatch · browser.tab.active");
        let mut view =
            DesktopProjectContextListView::new(crate::DESKTOP_PROJECT_CONTEXT_DERIVATION_ID, "2h");
        view.rows.push(DesktopProjectContextRowView {
            label: "sinex".to_string(),
            confidence: 0.74,
            focus_session_ref: Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.focus_session:event:terminal-cwd..event:browser-tab",
            )),
            input_families: vec!["browser".to_string(), "terminal".to_string()],
            evidence_refs: vec![terminal_ref.clone(), browser_ref.clone()],
            proposal_ref: None,
            caveats: vec![CaveatView {
                id: "project_context.ranked_view_only".to_string(),
                message: "fixture project context is a ranked projection candidate".to_string(),
                ref_: Some(SinexObjectRef::new(
                    SinexObjectKind::Projection,
                    "desktop.project_context",
                )),
            }],
        });
        view.row_count = view.rows.len();

        let envelope = view
            .into_envelope("sinexctl.events.context.desktop.project_contexts")
            .with_query_echo(json!({ "mode": "desktop_project_contexts" }));
        let value = serde_json::to_value(&envelope)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(
            value["payload"]["schema_version"],
            DESKTOP_PROJECT_CONTEXT_LIST_SCHEMA_VERSION
        );
        assert_eq!(
            value["payload"]["derivation_ref"],
            crate::DESKTOP_PROJECT_CONTEXT_DERIVATION_ID
        );
        assert_eq!(
            value["payload"]["output_kind"],
            "project_context_projection"
        );
        assert_eq!(value["payload"]["output_id"], "desktop.project_context");
        assert_eq!(value["payload"]["rows"][0]["label"], "sinex");
        assert_eq!(
            value["payload"]["rows"][0]["evidence_refs"][0]["id"],
            terminal_ref.id
        );
        assert_eq!(value["actions"][0]["id"], "desktop.project_context.explain");
        Ok(())
    }

    #[sinex_test]
    async fn desktop_context_derivations_are_not_canonical_events() -> xtask::TestResult<()> {
        let current =
            crate::find_derivation_spec(crate::DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID)
                .expect("desktop current-view derivation is registered");
        let focus = crate::find_derivation_spec(crate::DESKTOP_FOCUS_SESSION_DERIVATION_ID)
            .expect("desktop focus-session derivation is registered");
        let project = crate::find_derivation_spec(crate::DESKTOP_PROJECT_CONTEXT_DERIVATION_ID)
            .expect("desktop project-context derivation is registered");
        let notification =
            crate::find_derivation_spec(crate::DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID)
                .expect("desktop notification-pressure derivation is registered");

        assert_eq!(current.output_id, "desktop.context.current_view");
        assert_eq!(current.output_kind, crate::OutputKind::EphemeralView);
        assert_eq!(focus.output_kind, crate::OutputKind::ProjectionRow);
        assert_eq!(project.output_kind, crate::OutputKind::ProjectionRow);
        assert_eq!(notification.output_kind, crate::OutputKind::ProjectionRow);
        assert!(focus.invalidates_on(crate::InvalidationTrigger::Redaction));
        assert!(project.invalidates_on(crate::InvalidationTrigger::Replay));
        assert!(current.invalidates_on(crate::InvalidationTrigger::DisclosurePolicyChange));
        assert!(!current.output_kind.is_canonical_event());
        assert!(!focus.output_kind.is_canonical_event());
        assert!(!project.output_kind.is_canonical_event());

        assert_eq!(
            crate::declared_output_kind("desktop.context.current_view"),
            Some(crate::OutputKind::EphemeralView)
        );
        assert_eq!(
            crate::declared_output_kind("desktop.focus_session"),
            Some(crate::OutputKind::ProjectionRow)
        );
        assert_eq!(
            crate::declared_output_kind("desktop.project_context"),
            Some(crate::OutputKind::ProjectionRow)
        );
        assert_eq!(
            crate::declared_output_kind("desktop.notification_pressure"),
            Some(crate::OutputKind::ProjectionRow)
        );
        Ok(())
    }

    #[sinex_test]
    async fn desktop_context_candidate_confidence_requires_authority_ref_for_durable_label()
    -> xtask::TestResult<()> {
        let judged = DesktopContextCandidateView {
            label: "sinex".to_string(),
            confidence: 0.91,
            evidence_refs: vec![SinexObjectRef::new(SinexObjectKind::Event, "event:cwd")],
            proposal_ref: Some(SinexObjectRef::new(
                SinexObjectKind::Proposal,
                "proposal:desktop-context-sinex",
            )),
        };
        let unjudged = DesktopContextCandidateView {
            label: "unknown".to_string(),
            confidence: 0.99,
            evidence_refs: vec![SinexObjectRef::new(SinexObjectKind::Event, "event:title")],
            proposal_ref: None,
        };
        let view = DesktopContextView::current(
            crate::DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID,
            Vec::new(),
        );
        let mut value: DesktopContextView = serde_json::from_value(serde_json::to_value(view)?)?;
        value.candidates = vec![judged, unjudged];

        let durable_candidates = value
            .candidates
            .iter()
            .filter(|candidate| candidate.proposal_ref.is_some())
            .count();

        assert_eq!(durable_candidates, 1);
        assert!(
            value
                .candidates
                .iter()
                .any(|candidate| candidate.confidence > 0.95 && candidate.proposal_ref.is_none()),
            "high confidence alone remains only a ranked view candidate"
        );
        assert_eq!(
            value.candidates[0].proposal_ref.as_ref().unwrap().kind,
            SinexObjectKind::Proposal
        );
        Ok(())
    }

    #[sinex_test]
    async fn view_schema_generation_covers_card_and_envelope() -> xtask::TestResult<()> {
        let card_schema = serde_json::to_value(schemars::schema_for!(EventCardView))?;
        let context_envelope_schema =
            serde_json::to_value(schemars::schema_for!(ViewEnvelope<ContextSummaryView>))?;
        let desktop_context_envelope_schema =
            serde_json::to_value(schemars::schema_for!(ViewEnvelope<DesktopContextView>))?;
        let desktop_project_context_envelope_schema = serde_json::to_value(schemars::schema_for!(
            ViewEnvelope<DesktopProjectContextListView>
        ))?;
        let envelope_schema =
            serde_json::to_value(schemars::schema_for!(ViewEnvelope<EventCardListView>))?;
        let debt_envelope_schema =
            serde_json::to_value(schemars::schema_for!(ViewEnvelope<DebtListView>))?;
        let error_envelope_schema =
            serde_json::to_value(schemars::schema_for!(ViewEnvelope<EventErrorListView>))?;
        let query_envelope_schema =
            serde_json::to_value(schemars::schema_for!(ViewEnvelope<EventQueryListView>))?;

        assert_eq!(card_schema["title"], "EventCardView");
        assert!(
            card_schema["properties"].get("ref").is_some(),
            "card schema should expose the contract `ref` field"
        );
        assert!(
            context_envelope_schema["properties"]
                .get("payload")
                .is_some(),
            "context envelope schema should include the typed summary payload"
        );
        assert!(
            desktop_context_envelope_schema["properties"]
                .get("payload")
                .is_some(),
            "desktop-context envelope schema should include the typed view payload"
        );
        assert!(
            desktop_project_context_envelope_schema["properties"]
                .get("payload")
                .is_some(),
            "desktop project-context envelope schema should include the typed list payload"
        );
        assert!(envelope_schema["properties"].get("payload").is_some());
        assert!(debt_envelope_schema["properties"].get("payload").is_some());
        assert!(
            envelope_schema["properties"]
                .get("source_surface")
                .is_some(),
            "envelope schema should include source surface metadata"
        );
        assert!(
            query_envelope_schema["properties"].get("payload").is_some(),
            "query envelope schema should include the typed query-list payload"
        );
        assert!(
            error_envelope_schema["properties"].get("payload").is_some(),
            "error envelope schema should include the typed error-list payload"
        );
        Ok(())
    }
}
