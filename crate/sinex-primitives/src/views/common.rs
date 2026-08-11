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
    /// A read model is expected but no build has ever produced it.
    #[serde(rename = "readmodel.absent")]
    ReadmodelAbsent,
    /// A read model build is currently in flight.
    #[serde(rename = "readmodel.building")]
    ReadmodelBuilding,
    /// The last read-model build attempt errored out.
    #[serde(rename = "readmodel.failed")]
    ReadmodelFailed,
    /// A read model was only built for part of its intended scope.
    #[serde(rename = "readmodel.partial")]
    ReadmodelPartial,
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
            Self::ReadmodelAbsent => "readmodel.absent",
            Self::ReadmodelBuilding => "readmodel.building",
            Self::ReadmodelFailed => "readmodel.failed",
            Self::ReadmodelPartial => "readmodel.partial",
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

/// Strip characters unsafe to hand to a terminal/table renderer verbatim.
///
/// sinex-e0qo: captured content (D-Bus notification summaries, journald
/// MESSAGE fields, and anything else that reaches this crate's view/summary
/// constructors) can carry raw ANSI/OSC escape sequences from the source it
/// was captured from -- and sinexctl's default table/text output writes cell
/// content straight to stdout with no escape awareness. An ESC byte
/// (U+001B, C0) followed by attacker-chosen bytes can trigger OSC 52
/// clipboard writes, terminal-title spoofing, or cursor manipulation on the
/// operator's terminal. Removing every C0/C1 control character (which
/// includes ESC/CSI lead-in bytes) neuters any such sequence structurally --
/// there is no escape sequence without its lead-in byte.
///
/// This also strips a small set of Unicode "format" (Cf) characters --
/// bidi-override control points (LRE/RLE/PDF/LRO/RLO, LRI/RLI/FSI/PDI) and
/// zero-width/BOM markers -- which `char::is_control()` does NOT cover
/// (Cf is a distinct Unicode general category from Cc) but which enable a
/// related attack: filename/extension spoofing via right-to-left overrides
/// (e.g. a stored "invoicegnp.exe" rendering as "invoicexe.png"), with zero
/// visible cue since these characters measure as zero display width.
///
/// Deliberately a RENDER-time primitive, not an ingest-time one: the raw
/// captured bytes remain faithfully persisted in `core.events`; only the
/// outward-facing view/summary text constructed by this module is
/// sanitized, matching this project's "presentation is a consumer, never an
/// authority over ingest" doctrine.
pub fn strip_unsafe_display_chars(input: &str) -> String {
    input
        .chars()
        .filter(|ch| {
            if ch.is_control() {
                return false;
            }
            !matches!(
                *ch,
                '\u{200B}' // ZERO WIDTH SPACE
                | '\u{200E}' // LEFT-TO-RIGHT MARK
                | '\u{200F}' // RIGHT-TO-LEFT MARK
                | '\u{202A}'..='\u{202E}' // LRE, RLE, PDF, LRO, RLO
                | '\u{2066}'..='\u{2069}' // LRI, RLI, FSI, PDI
                | '\u{FEFF}' // ZERO WIDTH NO-BREAK SPACE / BOM
            )
        })
        .collect()
}

/// Compatibility shim over [`crate::text::truncate_chars`] for existing
/// internal callers that expect an owned `String`. New code should call
/// `crate::text::truncate_chars` directly (it returns `Cow` to avoid
/// allocating when no truncation is needed).
///
/// Strips sinex-e0qo's unsafe display characters BEFORE truncating -- the
/// stripped-content invariant must hold regardless of which call site or
/// helper does the actual char-counting/slicing.
pub(crate) fn truncate_chars(input: &str, max_chars: usize) -> String {
    let input = strip_unsafe_display_chars(input);
    crate::text::truncate_chars(&input, max_chars).into_owned()
}

fn is_false(value: &bool) -> bool {
    !*value
}
