use crate::JsonValue;
use crate::ids::Id;
use crate::temporal::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const VIEW_ENVELOPE_SCHEMA_VERSION: &str = "sinex.view-envelope/v3";

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

/// Standard caveat IDs for read surfaces that report incomplete readiness or
/// coverage.
///
/// These IDs are intentionally shared across CLI, API, MCP, and TUI views so an
/// empty or partial result names the same class of gap everywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ReadinessCaveatId {
    /// The expected source, producer, or evidence lane is absent.
    #[serde(rename = "source.absent")]
    SourceAbsent,
    /// A read model exists but is stale relative to the requested view.
    #[serde(rename = "readmodel.stale_by")]
    ReadmodelStaleBy,
    /// The requested time/window slice is only partially covered.
    #[serde(rename = "window.partial")]
    WindowPartial,
    /// Coverage cannot be measured exactly with the available evidence.
    #[serde(rename = "coverage.unmeasurable")]
    CoverageUnmeasurable,
    /// A derivation lane exists but has not been promoted to authoritative use.
    #[serde(rename = "derivation.lane_not_promoted")]
    DerivationLaneNotPromoted,
}

impl ReadinessCaveatId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SourceAbsent => "source.absent",
            Self::ReadmodelStaleBy => "readmodel.stale_by",
            Self::WindowPartial => "window.partial",
            Self::CoverageUnmeasurable => "coverage.unmeasurable",
            Self::DerivationLaneNotPromoted => "derivation.lane_not_promoted",
        }
    }
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

pub(crate) fn truncate_chars(input: &str, max_chars: usize) -> String {
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
