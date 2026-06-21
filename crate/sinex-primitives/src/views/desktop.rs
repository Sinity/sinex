//! Desktop-context view DTOs.

use super::{
    ActionAvailability, ActionAvailabilityState, CaveatView, SinexObjectRef, ViewEnvelope,
};
use crate::temporal::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const DESKTOP_CONTEXT_VIEW_SCHEMA_VERSION: &str = "sinex.desktop-context-view/v1";
pub const DESKTOP_FOCUS_SESSION_LIST_SCHEMA_VERSION: &str = "sinex.desktop-focus-session-list/v1";
pub const DESKTOP_NOTIFICATION_PRESSURE_SCHEMA_VERSION: &str =
    "sinex.desktop-notification-pressure/v1";
pub const DESKTOP_PROJECT_CONTEXT_LIST_SCHEMA_VERSION: &str =
    "sinex.desktop-project-context-list/v1";
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
