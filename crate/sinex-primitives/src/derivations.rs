//! Derivation contracts for durable derived outputs.
//!
//! This is metadata, not a generic projection runtime. It connects existing
//! projection/artifact declarations to output-kind, freshness, invalidation,
//! and operation-planning vocabulary so replay/archive/redaction work can
//! report which derived outputs are affected.

use schemars::JsonSchema;
use serde::Serialize;

use crate::output_kind::OutputKind;
use crate::task_domain::{TASK_REDUCER_DOMAIN_ID, TASK_REDUCER_INPUT_EVENT_TYPES};

pub type DerivationSpecId = &'static str;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DerivationInputScope {
    EventTypes {
        domain_id: &'static str,
        event_types: &'static [&'static str],
    },
    MaterialClass {
        material_class: &'static str,
    },
    QueryScope {
        scope: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessPolicy {
    RebuildOnInputChange,
    RefreshOnRead,
    EphemeralQuery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvalidationTrigger {
    Replay,
    Archive,
    Redaction,
    SourceMaterialChange,
    ParserSemanticsChange,
    DisclosurePolicyChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DerivationOperationHook {
    Rebuild,
    Refresh,
    Explain,
    Redact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
pub struct DerivationSpec {
    pub id: DerivationSpecId,
    pub input_scope: DerivationInputScope,
    pub output_id: &'static str,
    pub output_kind: OutputKind,
    pub freshness_policy: FreshnessPolicy,
    pub invalidates_on: &'static [InvalidationTrigger],
    pub rebuild_resource_policy_ref: Option<&'static str>,
    pub disclosure_policy_ref: Option<&'static str>,
    pub operation_hooks: &'static [DerivationOperationHook],
}

impl DerivationSpec {
    #[must_use]
    pub fn invalidates_on(&self, trigger: InvalidationTrigger) -> bool {
        self.invalidates_on.contains(&trigger)
    }
}

pub const TASK_CURRENT_OBJECTS_DERIVATION_ID: DerivationSpecId =
    "derivation:tasks.current/domain.current_objects@v1";
pub const DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID: DerivationSpecId =
    "derivation:desktop.context.current_view@v1";
pub const DESKTOP_FOCUS_SESSION_DERIVATION_ID: DerivationSpecId =
    "derivation:desktop.focus_session@v1";
pub const DESKTOP_PROJECT_CONTEXT_DERIVATION_ID: DerivationSpecId =
    "derivation:desktop.project_context@v1";
pub const DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID: DerivationSpecId =
    "derivation:desktop.notification_pressure@v1";
pub const MEDIA_AUDIO_TRANSCRIPT_ARTIFACT_DERIVATION_ID: DerivationSpecId =
    "derivation:media.audio.transcript_artifact@v1";
pub const MEDIA_SCREEN_OCR_ARTIFACT_DERIVATION_ID: DerivationSpecId =
    "derivation:media.screen.ocr_artifact@v1";
pub const MEDIA_TEXT_INDEX_PROJECTION_DERIVATION_ID: DerivationSpecId =
    "derivation:media.text_index_projection@v1";

pub const TASK_CURRENT_OBJECTS_DERIVATION: DerivationSpec = DerivationSpec {
    id: TASK_CURRENT_OBJECTS_DERIVATION_ID,
    input_scope: DerivationInputScope::EventTypes {
        domain_id: TASK_REDUCER_DOMAIN_ID,
        event_types: TASK_REDUCER_INPUT_EVENT_TYPES,
    },
    output_id: "domain.current_objects",
    output_kind: OutputKind::ProjectionRow,
    freshness_policy: FreshnessPolicy::RebuildOnInputChange,
    invalidates_on: &[
        InvalidationTrigger::Replay,
        InvalidationTrigger::Archive,
        InvalidationTrigger::Redaction,
        InvalidationTrigger::ParserSemanticsChange,
        InvalidationTrigger::DisclosurePolicyChange,
    ],
    rebuild_resource_policy_ref: Some("resource-policy:projection.rebuild.standard"),
    disclosure_policy_ref: Some("operator.default-disclosure"),
    operation_hooks: &[
        DerivationOperationHook::Rebuild,
        DerivationOperationHook::Explain,
    ],
};

pub const DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION: DerivationSpec = DerivationSpec {
    id: DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID,
    input_scope: DerivationInputScope::QueryScope {
        scope: "desktop.context.current_view.inputs",
    },
    output_id: "desktop.context.current_view",
    output_kind: OutputKind::EphemeralView,
    freshness_policy: FreshnessPolicy::RefreshOnRead,
    invalidates_on: &[
        InvalidationTrigger::Replay,
        InvalidationTrigger::Redaction,
        InvalidationTrigger::DisclosurePolicyChange,
        InvalidationTrigger::ParserSemanticsChange,
    ],
    rebuild_resource_policy_ref: Some("resource-policy:desktop.context.current-view"),
    disclosure_policy_ref: Some("disclosure-policy:desktop.context.view"),
    operation_hooks: &[
        DerivationOperationHook::Refresh,
        DerivationOperationHook::Explain,
    ],
};

pub const DESKTOP_FOCUS_SESSION_DERIVATION: DerivationSpec = DerivationSpec {
    id: DESKTOP_FOCUS_SESSION_DERIVATION_ID,
    input_scope: DerivationInputScope::QueryScope {
        scope: "desktop.focus_session.inputs",
    },
    output_id: "desktop.focus_session",
    output_kind: OutputKind::ProjectionRow,
    freshness_policy: FreshnessPolicy::RebuildOnInputChange,
    invalidates_on: &[
        InvalidationTrigger::Replay,
        InvalidationTrigger::Archive,
        InvalidationTrigger::Redaction,
        InvalidationTrigger::ParserSemanticsChange,
        InvalidationTrigger::DisclosurePolicyChange,
    ],
    rebuild_resource_policy_ref: Some("resource-policy:desktop.context.projection-rebuild"),
    disclosure_policy_ref: Some("disclosure-policy:desktop.context.projection"),
    operation_hooks: &[
        DerivationOperationHook::Rebuild,
        DerivationOperationHook::Explain,
    ],
};

pub const DESKTOP_PROJECT_CONTEXT_DERIVATION: DerivationSpec = DerivationSpec {
    id: DESKTOP_PROJECT_CONTEXT_DERIVATION_ID,
    input_scope: DerivationInputScope::QueryScope {
        scope: "desktop.project_context.inputs",
    },
    output_id: "desktop.project_context",
    output_kind: OutputKind::ProjectionRow,
    freshness_policy: FreshnessPolicy::RebuildOnInputChange,
    invalidates_on: &[
        InvalidationTrigger::Replay,
        InvalidationTrigger::Archive,
        InvalidationTrigger::Redaction,
        InvalidationTrigger::ParserSemanticsChange,
        InvalidationTrigger::DisclosurePolicyChange,
    ],
    rebuild_resource_policy_ref: Some("resource-policy:desktop.context.projection-rebuild"),
    disclosure_policy_ref: Some("disclosure-policy:desktop.context.projection"),
    operation_hooks: &[
        DerivationOperationHook::Rebuild,
        DerivationOperationHook::Explain,
    ],
};

pub const DESKTOP_NOTIFICATION_PRESSURE_DERIVATION: DerivationSpec = DerivationSpec {
    id: DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID,
    input_scope: DerivationInputScope::EventTypes {
        domain_id: "desktop.notification",
        event_types: &[
            "notification.sent",
            "notification.action_invoked",
            "notification.closed",
        ],
    },
    output_id: "desktop.notification_pressure",
    output_kind: OutputKind::ProjectionRow,
    freshness_policy: FreshnessPolicy::RebuildOnInputChange,
    invalidates_on: &[
        InvalidationTrigger::Replay,
        InvalidationTrigger::Redaction,
        InvalidationTrigger::DisclosurePolicyChange,
    ],
    rebuild_resource_policy_ref: Some("resource-policy:desktop.context.projection-rebuild"),
    disclosure_policy_ref: Some("disclosure-policy:desktop.notification-pressure"),
    operation_hooks: &[
        DerivationOperationHook::Rebuild,
        DerivationOperationHook::Explain,
    ],
};

pub const MEDIA_AUDIO_TRANSCRIPT_ARTIFACT_DERIVATION: DerivationSpec = DerivationSpec {
    id: MEDIA_AUDIO_TRANSCRIPT_ARTIFACT_DERIVATION_ID,
    input_scope: DerivationInputScope::EventTypes {
        domain_id: "media.audio",
        event_types: &[
            "media.audio.transcript_segment_observed",
            "media.audio.transcription_run_observed",
        ],
    },
    output_id: "media.audio.transcript_artifact",
    output_kind: OutputKind::Artifact,
    freshness_policy: FreshnessPolicy::RebuildOnInputChange,
    invalidates_on: &[
        InvalidationTrigger::Replay,
        InvalidationTrigger::Archive,
        InvalidationTrigger::Redaction,
        InvalidationTrigger::SourceMaterialChange,
        InvalidationTrigger::ParserSemanticsChange,
        InvalidationTrigger::DisclosurePolicyChange,
    ],
    rebuild_resource_policy_ref: Some("resource-policy:media.audio.transcript-artifact.rebuild"),
    disclosure_policy_ref: Some("operator.media.audio-transcript.default"),
    operation_hooks: &[
        DerivationOperationHook::Rebuild,
        DerivationOperationHook::Explain,
        DerivationOperationHook::Redact,
    ],
};

pub const MEDIA_SCREEN_OCR_ARTIFACT_DERIVATION: DerivationSpec = DerivationSpec {
    id: MEDIA_SCREEN_OCR_ARTIFACT_DERIVATION_ID,
    input_scope: DerivationInputScope::EventTypes {
        domain_id: "media.screen",
        event_types: &[
            "media.screen.ocr_segment_observed",
            "media.screen.ocr_run_observed",
        ],
    },
    output_id: "media.screen.ocr_artifact",
    output_kind: OutputKind::Artifact,
    freshness_policy: FreshnessPolicy::RebuildOnInputChange,
    invalidates_on: &[
        InvalidationTrigger::Replay,
        InvalidationTrigger::Archive,
        InvalidationTrigger::Redaction,
        InvalidationTrigger::SourceMaterialChange,
        InvalidationTrigger::ParserSemanticsChange,
        InvalidationTrigger::DisclosurePolicyChange,
    ],
    rebuild_resource_policy_ref: Some("resource-policy:media.screen.ocr-artifact.rebuild"),
    disclosure_policy_ref: Some("operator.media.screen-ocr.default"),
    operation_hooks: &[
        DerivationOperationHook::Rebuild,
        DerivationOperationHook::Explain,
        DerivationOperationHook::Redact,
    ],
};

pub const MEDIA_TEXT_INDEX_PROJECTION_DERIVATION: DerivationSpec = DerivationSpec {
    id: MEDIA_TEXT_INDEX_PROJECTION_DERIVATION_ID,
    input_scope: DerivationInputScope::QueryScope {
        scope: "media.text_index.inputs",
    },
    output_id: "media.text_index_projection",
    output_kind: OutputKind::ProjectionRow,
    freshness_policy: FreshnessPolicy::RebuildOnInputChange,
    invalidates_on: &[
        InvalidationTrigger::Replay,
        InvalidationTrigger::Archive,
        InvalidationTrigger::Redaction,
        InvalidationTrigger::SourceMaterialChange,
        InvalidationTrigger::ParserSemanticsChange,
        InvalidationTrigger::DisclosurePolicyChange,
    ],
    rebuild_resource_policy_ref: Some("resource-policy:media.text-index.projection-rebuild"),
    disclosure_policy_ref: Some("operator.media.text-index.default"),
    operation_hooks: &[
        DerivationOperationHook::Rebuild,
        DerivationOperationHook::Explain,
        DerivationOperationHook::Redact,
    ],
};

pub const DERIVATION_SPECS: &[DerivationSpec] = &[
    TASK_CURRENT_OBJECTS_DERIVATION,
    DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION,
    DESKTOP_FOCUS_SESSION_DERIVATION,
    DESKTOP_PROJECT_CONTEXT_DERIVATION,
    DESKTOP_NOTIFICATION_PRESSURE_DERIVATION,
    MEDIA_AUDIO_TRANSCRIPT_ARTIFACT_DERIVATION,
    MEDIA_SCREEN_OCR_ARTIFACT_DERIVATION,
    MEDIA_TEXT_INDEX_PROJECTION_DERIVATION,
];

pub fn derivation_specs() -> impl Iterator<Item = &'static DerivationSpec> {
    DERIVATION_SPECS.iter()
}

#[must_use]
pub fn find_derivation_spec(id: &str) -> Option<&'static DerivationSpec> {
    derivation_specs().find(|spec| spec.id == id)
}

pub fn derivations_for_output(output_id: &str) -> impl Iterator<Item = &'static DerivationSpec> {
    derivation_specs().filter(move |spec| spec.output_id == output_id)
}

pub fn affected_derivations(
    trigger: InvalidationTrigger,
) -> impl Iterator<Item = &'static DerivationSpec> {
    derivation_specs().filter(move |spec| spec.invalidates_on(trigger))
}
