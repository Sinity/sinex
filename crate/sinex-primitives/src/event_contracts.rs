//! EventContract registry.
//!
//! Event contracts give admitted events a contract identity that is separate
//! from `source + event_type`. The source/type pair remains the namespace that
//! an event matches; the contract id is the semantic coordinate that package
//! and admission policy code can reference.

use crate::domain::{EventSource, EventType};
use crate::events::Event;
use crate::output_kind::OutputKind;
use crate::source_contracts::OccurrenceIdentity;
use schemars::JsonSchema;
use serde::Serialize;

/// Stable identifier for an event contract.
pub type EventContractId = &'static str;

/// Payload schema coordinate used by an [`EventContract`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PayloadSchemaContract {
    /// Contract uses the payload inventory schema for `(source, event_type)`.
    PayloadInventory {
        source: &'static str,
        event_type: &'static str,
        version: &'static str,
    },
    /// Contract requires an explicit persisted payload schema id.
    ExplicitSchemaId { schema_id: &'static str },
}

/// Occurrence identity requirements for the event payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventOccurrenceContract {
    /// Occurrence is declared by the source package contract.
    SourceDeclared,
    /// Material-provenance events use the material id plus byte/row anchor.
    MaterialAnchor,
    /// Explicit field list forms the occurrence key.
    Fields { fields: &'static [&'static str] },
}

/// Temporal expectations for admitted events under this contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventTemporalContract {
    /// Parser supplies intrinsic event time when present; admission may derive
    /// from material timing when absent.
    IntrinsicOrMaterial,
    /// Event must carry intrinsic timestamp evidence.
    IntrinsicRequired,
    /// Event time is derived from source-material timing.
    MaterialDerived,
}

/// Provenance requirements for events under this contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventProvenanceRequirement {
    Material,
    Derived,
    MaterialOrDerived,
}

/// Code-coupled semantic contract for an admitted event family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
pub struct EventContract {
    /// Stable contract id. This is the semantic coordinate other contracts
    /// should reference instead of treating `source + event_type` as authority.
    pub id: EventContractId,
    /// Namespace/display event source matched by this contract.
    pub event_source: &'static str,
    /// Event type matched by this contract.
    pub event_type: &'static str,
    /// Payload schema coordinate.
    pub payload_schema: PayloadSchemaContract,
    /// Occurrence identity contract.
    pub occurrence: EventOccurrenceContract,
    /// Existing source-contract occurrence declarations this contract accepts.
    /// Multi-package contracts can cover sources with different declared
    /// occurrence strategies without collapsing them into one false identity.
    pub source_occurrences: &'static [OccurrenceIdentity],
    /// Temporal policy.
    pub temporal: EventTemporalContract,
    /// Provenance policy.
    pub provenance: EventProvenanceRequirement,
    /// Optional operator-controlled disclosure policy ref.
    pub disclosure_policy_ref: Option<&'static str>,
    /// Admission policy expected to govern this contract.
    pub admission_policy_ref: Option<&'static str>,
    /// Package/source ids currently allowed to emit this event contract.
    pub package_refs: &'static [&'static str],
    /// Output-kind classification for this contract's primary durable output.
    pub output_kind: OutputKind,
}

impl EventContract {
    #[must_use]
    pub fn matches_namespace<T>(&self, event: &Event<T>) -> bool {
        event.source.as_str() == self.event_source && event.event_type.as_str() == self.event_type
    }

    #[must_use]
    pub fn matches_pair(&self, source: &EventSource, event_type: &EventType) -> bool {
        source.as_str() == self.event_source && event_type.as_str() == self.event_type
    }

    #[must_use]
    pub const fn is_canonical_event(&self) -> bool {
        self.output_kind.is_canonical_event()
    }
}

inventory::collect!(EventContract);

/// Existing terminal-history event contract used as the first registry entry.
pub const SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID: EventContractId =
    "event-contract:shell.history/command.imported@v1";
pub const SHELL_KITTY_COMMAND_EXECUTED_CONTRACT_ID: EventContractId =
    "event-contract:shell.kitty/command.executed@v1";
pub const BROWSER_PAGE_VISITED_CONTRACT_ID: EventContractId =
    "event-contract:webhistory/page.visited@v1";
pub const BROWSER_NAVIGATION_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:browser/navigation.observed@v1";
pub const BROWSER_TAB_ACTIVATED_CONTRACT_ID: EventContractId =
    "event-contract:browser/tab.activated@v1";
pub const BROWSER_DOWNLOAD_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:browser/download.observed@v1";
pub const EMAIL_MESSAGE_RECEIVED_CONTRACT_ID: EventContractId =
    "event-contract:email/message.received@v1";
pub const EMAIL_MESSAGE_SENT_CONTRACT_ID: EventContractId = "event-contract:email/message.sent@v1";
pub const EMAIL_ATTACHMENT_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:email/attachment.observed@v1";
pub const EMAIL_THREAD_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:email/thread.observed@v1";
pub const EMAIL_SYNC_CURSOR_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:email/sync_cursor.observed@v1";
pub const EMAIL_CAPTURE_RUNTIME_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:email/capture_runtime.observed@v1";
pub const MEDIA_AUDIO_RECORDING_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:media.audio/recording.observed@v1";
pub const MEDIA_AUDIO_CAPTURE_SESSION_STARTED_CONTRACT_ID: EventContractId =
    "event-contract:media.audio/capture_session.started@v1";
pub const MEDIA_AUDIO_CAPTURE_SESSION_ENDED_CONTRACT_ID: EventContractId =
    "event-contract:media.audio/capture_session.ended@v1";
pub const MEDIA_AUDIO_TRANSCRIPT_SEGMENT_CONTRACT_ID: EventContractId =
    "event-contract:media.audio/transcript_segment.observed@v1";
pub const MEDIA_AUDIO_TRANSCRIPTION_RUN_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:media.audio/transcription_run.observed@v1";
pub const MEDIA_SCREEN_SCREENSHOT_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:media.screen/screenshot.observed@v1";
pub const MEDIA_SCREEN_OCR_SEGMENT_CONTRACT_ID: EventContractId =
    "event-contract:media.screen/ocr_segment.observed@v1";
pub const MEDIA_SCREEN_OCR_RUN_OBSERVED_CONTRACT_ID: EventContractId =
    "event-contract:media.screen/ocr_run.observed@v1";

const SHELL_HISTORY_PACKAGES: &[&str] = &[
    "terminal.bash-history",
    "terminal.zsh-history",
    "terminal.text-history",
    "terminal.fish-history",
];
const SHELL_HISTORY_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Anchor, OccurrenceIdentity::Natural];
const SHELL_KITTY_PACKAGES: &[&str] = &["terminal.kitty-osc-live"];
const SHELL_KITTY_SOURCE_OCCURRENCES: &[OccurrenceIdentity] = &[OccurrenceIdentity::Uuid5From(
    "(terminal_session, sequence, command, cwd, ts)",
)];
const BROWSER_HISTORY_PACKAGES: &[&str] = &["browser.history"];
const BROWSER_HISTORY_SOURCE_OCCURRENCES: &[OccurrenceIdentity] = &[OccurrenceIdentity::Uuid5From(
    "(source, browser_profile, visit_id)",
)];
const BROWSER_WEBEXTENSION_PACKAGES: &[&str] = &["browser.webextension-live"];
const BROWSER_NAVIGATION_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(profile_id, tab_id, url, observed_at)",
    )];
const BROWSER_TAB_ACTIVATED_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(profile_id, tab_id, window_id, observed_at)",
    )];
const BROWSER_DOWNLOAD_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(profile_id, download_id, url, observed_at)",
    )];
const EMAIL_MAILBOX_PACKAGES: &[&str] = &["email.mailbox"];
const EMAIL_MAILBOX_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From("(message_id, folder)")];
const EMAIL_ATTACHMENT_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(message_occurrence, attachment_index, filename, content_id)",
    )];
const EMAIL_THREAD_SOURCE_OCCURRENCES: &[OccurrenceIdentity] = &[OccurrenceIdentity::Uuid5From(
    "(thread_key, message_id_or_material)",
)];
const EMAIL_SYNC_CURSOR_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(provider, account_binding_ref, mailbox_scope, cursor_kind)",
    )];
const EMAIL_CAPTURE_RUNTIME_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(provider, account_binding_ref, mode_id, observed_at)",
    )];
const MEDIA_AUDIO_TRANSCRIPT_PACKAGES: &[&str] = &["media.audio-transcript"];
const MEDIA_SCREEN_OCR_PACKAGES: &[&str] = &["media.screen-ocr"];

const MEDIA_AUDIO_RECORDING_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(raw_material_id, capture_session_id, observed_at)",
    )];
const MEDIA_AUDIO_CAPTURE_STARTED_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(capture_session_id, started_at)",
    )];
const MEDIA_AUDIO_CAPTURE_ENDED_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(capture_session_id, ended_at)",
    )];
const MEDIA_AUDIO_TRANSCRIPT_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(material_id, segment_index, start_ms, end_ms)",
    )];
const MEDIA_AUDIO_TRANSCRIPTION_RUN_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(producer_run_id, model_id, input_material_ids)",
    )];
const MEDIA_SCREEN_SCREENSHOT_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(raw_material_id, capture_session_id, display_id, region)",
    )];
const MEDIA_SCREEN_OCR_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(material_id, segment_index, bbox)",
    )];
const MEDIA_SCREEN_OCR_RUN_SOURCE_OCCURRENCES: &[OccurrenceIdentity] =
    &[OccurrenceIdentity::Uuid5From(
        "(producer_run_id, engine_id, input_material_ids)",
    )];

const MEDIA_AUDIO_RECORDING_OCCURRENCE_FIELDS: &[&str] =
    &["raw_material_id", "capture_session_id", "observed_at"];
const MEDIA_AUDIO_CAPTURE_STARTED_OCCURRENCE_FIELDS: &[&str] =
    &["capture_session_id", "started_at"];
const MEDIA_AUDIO_CAPTURE_ENDED_OCCURRENCE_FIELDS: &[&str] = &["capture_session_id", "ended_at"];
const MEDIA_AUDIO_TRANSCRIPTION_RUN_OCCURRENCE_FIELDS: &[&str] =
    &["producer_run_id", "model_id", "input_material_ids"];
const MEDIA_SCREEN_SCREENSHOT_OCCURRENCE_FIELDS: &[&str] = &[
    "raw_material_id",
    "capture_session_id",
    "display_id",
    "region",
];
const MEDIA_SCREEN_OCR_RUN_OCCURRENCE_FIELDS: &[&str] =
    &["producer_run_id", "engine_id", "input_material_ids"];

inventory::submit! {
    EventContract {
        id: SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID,
        event_source: "shell.history",
        event_type: "command.imported",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "shell.history",
            event_type: "command.imported",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: SHELL_HISTORY_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.shell-history.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: SHELL_HISTORY_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: SHELL_KITTY_COMMAND_EXECUTED_CONTRACT_ID,
        event_source: "shell.kitty",
        event_type: "command.executed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "shell.kitty",
            event_type: "command.executed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: SHELL_KITTY_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.terminal-live.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: SHELL_KITTY_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: BROWSER_PAGE_VISITED_CONTRACT_ID,
        event_source: "webhistory",
        event_type: "page.visited",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "webhistory",
            event_type: "page.visited",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: BROWSER_HISTORY_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.browser-history.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: BROWSER_HISTORY_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: BROWSER_NAVIGATION_OBSERVED_CONTRACT_ID,
        event_source: "browser",
        event_type: "navigation.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "browser",
            event_type: "navigation.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &["profile_id", "tab_id", "url", "observed_at"],
        },
        source_occurrences: BROWSER_NAVIGATION_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.browser-web.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: BROWSER_WEBEXTENSION_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: BROWSER_TAB_ACTIVATED_CONTRACT_ID,
        event_source: "browser",
        event_type: "tab.activated",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "browser",
            event_type: "tab.activated",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &["profile_id", "tab_id", "window_id", "observed_at"],
        },
        source_occurrences: BROWSER_TAB_ACTIVATED_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.browser-web.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: BROWSER_WEBEXTENSION_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: BROWSER_DOWNLOAD_OBSERVED_CONTRACT_ID,
        event_source: "browser",
        event_type: "download.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "browser",
            event_type: "download.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &["profile_id", "download_id", "url", "observed_at"],
        },
        source_occurrences: BROWSER_DOWNLOAD_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.browser-web.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: BROWSER_WEBEXTENSION_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: EMAIL_MESSAGE_RECEIVED_CONTRACT_ID,
        event_source: "email",
        event_type: "email.message.received",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.message.received",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: EMAIL_MAILBOX_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: EMAIL_MESSAGE_SENT_CONTRACT_ID,
        event_source: "email",
        event_type: "email.message.sent",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.message.sent",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: EMAIL_MAILBOX_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: EMAIL_ATTACHMENT_OBSERVED_CONTRACT_ID,
        event_source: "email",
        event_type: "email.attachment.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.attachment.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: EMAIL_ATTACHMENT_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: EMAIL_THREAD_OBSERVED_CONTRACT_ID,
        event_source: "email",
        event_type: "email.thread.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.thread.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &["thread_key", "message_id"],
        },
        source_occurrences: EMAIL_THREAD_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: EMAIL_SYNC_CURSOR_OBSERVED_CONTRACT_ID,
        event_source: "email",
        event_type: "email.sync_cursor.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.sync_cursor.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &[
                "provider",
                "account_binding_ref",
                "mailbox_scope",
                "cursor_kind",
            ],
        },
        source_occurrences: EMAIL_SYNC_CURSOR_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: EMAIL_CAPTURE_RUNTIME_OBSERVED_CONTRACT_ID,
        event_source: "email",
        event_type: "email.capture_runtime.observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "email",
            event_type: "email.capture_runtime.observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: &["provider", "account_binding_ref", "mode_id", "observed_at"],
        },
        source_occurrences: EMAIL_CAPTURE_RUNTIME_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.email-mailbox.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: EMAIL_MAILBOX_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: MEDIA_AUDIO_RECORDING_OBSERVED_CONTRACT_ID,
        event_source: "media.audio",
        event_type: "media.audio.recording_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.audio",
            event_type: "media.audio.recording_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_AUDIO_RECORDING_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_AUDIO_RECORDING_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.media.audio-transcript.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_AUDIO_TRANSCRIPT_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: MEDIA_AUDIO_CAPTURE_SESSION_STARTED_CONTRACT_ID,
        event_source: "media.audio",
        event_type: "media.audio.capture_session_started",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.audio",
            event_type: "media.audio.capture_session_started",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_AUDIO_CAPTURE_STARTED_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_AUDIO_CAPTURE_STARTED_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.media.audio-transcript.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_AUDIO_TRANSCRIPT_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: MEDIA_AUDIO_CAPTURE_SESSION_ENDED_CONTRACT_ID,
        event_source: "media.audio",
        event_type: "media.audio.capture_session_ended",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.audio",
            event_type: "media.audio.capture_session_ended",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_AUDIO_CAPTURE_ENDED_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_AUDIO_CAPTURE_ENDED_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.media.audio-transcript.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_AUDIO_TRANSCRIPT_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: MEDIA_AUDIO_TRANSCRIPT_SEGMENT_CONTRACT_ID,
        event_source: "media.audio",
        event_type: "media.audio.transcript_segment_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.audio",
            event_type: "media.audio.transcript_segment_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: MEDIA_AUDIO_TRANSCRIPT_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::MaterialOrDerived,
        disclosure_policy_ref: Some("operator.media.audio-transcript.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_AUDIO_TRANSCRIPT_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: MEDIA_AUDIO_TRANSCRIPTION_RUN_OBSERVED_CONTRACT_ID,
        event_source: "media.audio",
        event_type: "media.audio.transcription_run_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.audio",
            event_type: "media.audio.transcription_run_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_AUDIO_TRANSCRIPTION_RUN_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_AUDIO_TRANSCRIPTION_RUN_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Derived,
        disclosure_policy_ref: Some("operator.media.audio-transcript.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_AUDIO_TRANSCRIPT_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: MEDIA_SCREEN_SCREENSHOT_OBSERVED_CONTRACT_ID,
        event_source: "media.screen",
        event_type: "media.screen.screenshot_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.screen",
            event_type: "media.screen.screenshot_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_SCREEN_SCREENSHOT_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_SCREEN_SCREENSHOT_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::Material,
        disclosure_policy_ref: Some("operator.media.screen-ocr.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_SCREEN_OCR_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: MEDIA_SCREEN_OCR_SEGMENT_CONTRACT_ID,
        event_source: "media.screen",
        event_type: "media.screen.ocr_segment_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.screen",
            event_type: "media.screen.ocr_segment_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::SourceDeclared,
        source_occurrences: MEDIA_SCREEN_OCR_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicOrMaterial,
        provenance: EventProvenanceRequirement::MaterialOrDerived,
        disclosure_policy_ref: Some("operator.media.screen-ocr.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_SCREEN_OCR_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

inventory::submit! {
    EventContract {
        id: MEDIA_SCREEN_OCR_RUN_OBSERVED_CONTRACT_ID,
        event_source: "media.screen",
        event_type: "media.screen.ocr_run_observed",
        payload_schema: PayloadSchemaContract::PayloadInventory {
            source: "media.screen",
            event_type: "media.screen.ocr_run_observed",
            version: "1.0.0",
        },
        occurrence: EventOccurrenceContract::Fields {
            fields: MEDIA_SCREEN_OCR_RUN_OCCURRENCE_FIELDS,
        },
        source_occurrences: MEDIA_SCREEN_OCR_RUN_SOURCE_OCCURRENCES,
        temporal: EventTemporalContract::IntrinsicRequired,
        provenance: EventProvenanceRequirement::Derived,
        disclosure_policy_ref: Some("operator.media.screen-ocr.default"),
        admission_policy_ref: Some(crate::admission_policy::STANDARD_EVENT_ADMISSION_POLICY_ID),
        package_refs: MEDIA_SCREEN_OCR_PACKAGES,
        output_kind: OutputKind::CanonicalEvent,
    }
}

pub fn event_contracts() -> impl Iterator<Item = &'static EventContract> {
    inventory::iter::<EventContract>()
}

#[must_use]
pub fn find_event_contract(id: &str) -> Option<&'static EventContract> {
    event_contracts().find(|contract| contract.id == id)
}

#[must_use]
pub fn find_event_contract_for_pair(
    source: &EventSource,
    event_type: &EventType,
) -> Option<&'static EventContract> {
    event_contracts().find(|contract| contract.matches_pair(source, event_type))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission_policy::{STANDARD_EVENT_ADMISSION_POLICY_ID, admission_policies};
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn kitty_command_contract_is_package_and_policy_addressable() -> TestResult<()> {
        let Some(contract) = find_event_contract(SHELL_KITTY_COMMAND_EXECUTED_CONTRACT_ID) else {
            panic!("missing Kitty command EventContract");
        };

        assert_eq!(contract.event_source, "shell.kitty");
        assert_eq!(contract.event_type, "command.executed");
        assert!(contract.package_refs.contains(&"terminal.kitty-osc-live"));
        assert_eq!(
            contract.admission_policy_ref,
            Some(STANDARD_EVENT_ADMISSION_POLICY_ID)
        );

        let accepted_by_standard = admission_policies().any(|policy| {
            policy.id == STANDARD_EVENT_ADMISSION_POLICY_ID
                && policy
                    .accepted_event_contracts
                    .contains(&SHELL_KITTY_COMMAND_EXECUTED_CONTRACT_ID)
        });
        assert!(accepted_by_standard);

        Ok(())
    }

    #[sinex_test]
    async fn browser_page_visit_contract_is_package_and_policy_addressable() -> TestResult<()> {
        let Some(contract) = find_event_contract(BROWSER_PAGE_VISITED_CONTRACT_ID) else {
            panic!("missing browser page visit EventContract");
        };

        assert_eq!(contract.event_source, "webhistory");
        assert_eq!(contract.event_type, "page.visited");
        assert!(contract.package_refs.contains(&"browser.history"));
        assert_eq!(
            contract.admission_policy_ref,
            Some(STANDARD_EVENT_ADMISSION_POLICY_ID)
        );

        let accepted_by_standard = admission_policies().any(|policy| {
            policy.id == STANDARD_EVENT_ADMISSION_POLICY_ID
                && policy
                    .accepted_event_contracts
                    .contains(&BROWSER_PAGE_VISITED_CONTRACT_ID)
        });
        assert!(accepted_by_standard);

        Ok(())
    }

    #[sinex_test]
    async fn email_message_contracts_are_package_and_policy_addressable() -> TestResult<()> {
        for id in [
            EMAIL_MESSAGE_RECEIVED_CONTRACT_ID,
            EMAIL_MESSAGE_SENT_CONTRACT_ID,
            EMAIL_ATTACHMENT_OBSERVED_CONTRACT_ID,
            EMAIL_THREAD_OBSERVED_CONTRACT_ID,
            EMAIL_SYNC_CURSOR_OBSERVED_CONTRACT_ID,
            EMAIL_CAPTURE_RUNTIME_OBSERVED_CONTRACT_ID,
        ] {
            let Some(contract) = find_event_contract(id) else {
                panic!("missing email EventContract {id}");
            };

            assert_eq!(contract.event_source, "email");
            assert!(contract.package_refs.contains(&"email.mailbox"));
            assert_eq!(
                contract.admission_policy_ref,
                Some(STANDARD_EVENT_ADMISSION_POLICY_ID)
            );

            let accepted_by_standard = admission_policies().any(|policy| {
                policy.id == STANDARD_EVENT_ADMISSION_POLICY_ID
                    && policy.accepted_event_contracts.contains(&id)
            });
            assert!(accepted_by_standard, "{id} must be admission-addressable");
        }

        Ok(())
    }

    #[sinex_test]
    async fn browser_live_contracts_are_package_policy_and_payload_addressable() -> TestResult<()> {
        for (contract_id, event_type) in [
            (
                BROWSER_NAVIGATION_OBSERVED_CONTRACT_ID,
                "navigation.observed",
            ),
            (BROWSER_TAB_ACTIVATED_CONTRACT_ID, "tab.activated"),
            (BROWSER_DOWNLOAD_OBSERVED_CONTRACT_ID, "download.observed"),
        ] {
            let Some(contract) = find_event_contract(contract_id) else {
                panic!("missing browser live EventContract {contract_id}");
            };

            assert_eq!(contract.event_source, "browser");
            assert_eq!(contract.event_type, event_type);
            assert!(contract.package_refs.contains(&"browser.webextension-live"));
            assert!(contract.is_canonical_event());
            assert_eq!(
                contract.admission_policy_ref,
                Some(STANDARD_EVENT_ADMISSION_POLICY_ID)
            );

            match contract.payload_schema {
                PayloadSchemaContract::PayloadInventory {
                    source,
                    event_type: schema_event_type,
                    version,
                } => {
                    assert_eq!(source, "browser");
                    assert_eq!(schema_event_type, event_type);
                    assert_eq!(version, "1.0.0");
                }
                PayloadSchemaContract::ExplicitSchemaId { schema_id } => {
                    panic!(
                        "browser live EventContract {contract_id} should use payload inventory, got {schema_id}"
                    )
                }
            }

            let accepted_by_standard = admission_policies().any(|policy| {
                policy.id == STANDARD_EVENT_ADMISSION_POLICY_ID
                    && policy.accepted_event_contracts.contains(&contract_id)
            });
            assert!(
                accepted_by_standard,
                "{contract_id} must be admission-addressable"
            );
        }

        Ok(())
    }

    #[sinex_test]
    async fn media_capture_contracts_are_package_policy_and_payload_addressable() -> TestResult<()>
    {
        for (contract_id, package_id, source, event_type) in [
            (
                MEDIA_AUDIO_RECORDING_OBSERVED_CONTRACT_ID,
                "media.audio-transcript",
                "media.audio",
                "media.audio.recording_observed",
            ),
            (
                MEDIA_AUDIO_CAPTURE_SESSION_STARTED_CONTRACT_ID,
                "media.audio-transcript",
                "media.audio",
                "media.audio.capture_session_started",
            ),
            (
                MEDIA_AUDIO_CAPTURE_SESSION_ENDED_CONTRACT_ID,
                "media.audio-transcript",
                "media.audio",
                "media.audio.capture_session_ended",
            ),
            (
                MEDIA_AUDIO_TRANSCRIPT_SEGMENT_CONTRACT_ID,
                "media.audio-transcript",
                "media.audio",
                "media.audio.transcript_segment_observed",
            ),
            (
                MEDIA_AUDIO_TRANSCRIPTION_RUN_OBSERVED_CONTRACT_ID,
                "media.audio-transcript",
                "media.audio",
                "media.audio.transcription_run_observed",
            ),
            (
                MEDIA_SCREEN_SCREENSHOT_OBSERVED_CONTRACT_ID,
                "media.screen-ocr",
                "media.screen",
                "media.screen.screenshot_observed",
            ),
            (
                MEDIA_SCREEN_OCR_SEGMENT_CONTRACT_ID,
                "media.screen-ocr",
                "media.screen",
                "media.screen.ocr_segment_observed",
            ),
            (
                MEDIA_SCREEN_OCR_RUN_OBSERVED_CONTRACT_ID,
                "media.screen-ocr",
                "media.screen",
                "media.screen.ocr_run_observed",
            ),
        ] {
            let Some(contract) = find_event_contract(contract_id) else {
                panic!("missing media EventContract {contract_id}");
            };
            assert_eq!(contract.event_source, source);
            assert_eq!(contract.event_type, event_type);
            assert!(
                contract.package_refs.contains(&package_id),
                "{contract_id} should be emitted by package {package_id}"
            );
            assert!(contract.is_canonical_event());
            assert_eq!(
                contract.admission_policy_ref,
                Some(STANDARD_EVENT_ADMISSION_POLICY_ID)
            );

            match contract.payload_schema {
                PayloadSchemaContract::PayloadInventory {
                    source: schema_source,
                    event_type: schema_event_type,
                    version,
                } => {
                    assert_eq!(schema_source, source);
                    assert_eq!(schema_event_type, event_type);
                    assert_eq!(version, "1.0.0");
                }
                PayloadSchemaContract::ExplicitSchemaId { schema_id } => {
                    panic!(
                        "media EventContract {contract_id} should use payload inventory, got {schema_id}"
                    )
                }
            }

            let accepted_by_standard = admission_policies().any(|policy| {
                policy.id == STANDARD_EVENT_ADMISSION_POLICY_ID
                    && policy.accepted_event_contracts.contains(&contract_id)
            });
            assert!(
                accepted_by_standard,
                "{contract_id} must be admission-addressable"
            );
        }

        Ok(())
    }
}
