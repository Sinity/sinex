use sinex_primitives::events::payloads::email::{
    EmailProviderKind, EmailProviderRuntime, EmailSyncCursorKind,
};
use sinex_primitives::source_contracts::source_runtime_bindings;
use sinex_primitives::{InvalidationTrigger, SinexError};

use super::{Result, optional_scope_string};

#[derive(Debug, Clone, Copy)]
pub(super) struct PackageOperationSpec {
    pub(super) operation_type: &'static str,
    pub(super) source_id: &'static str,
    pub(super) default_mode_id: Option<&'static str>,
    pub(super) accepted_mode_ids: &'static [&'static str],
    pub(super) action: &'static str,
    pub(super) surface: &'static str,
    pub(super) executor_message: &'static str,
}

pub(super) const PACKAGE_OPERATION_EXECUTOR_STATE: &str = "awaiting_runtime_executor";
pub(super) const MEDIA_WORKER_OUTPUT_EXECUTOR_STATE: &str = "worker_output_admitted";
pub(super) const MEDIA_WORKER_COMMAND_EXECUTOR_STATE: &str = "worker_command_admitted";
pub(super) const MEDIA_WORKER_COMMAND_FAILED_STATE: &str = "worker_command_failed";
pub(super) const MEDIA_WORKER_OUTPUT_MAX_BYTES: usize = 10 * 1024 * 1024;
pub(super) const MEDIA_WORKER_STDERR_MAX_BYTES: usize = 64 * 1024;
pub(super) const MEDIA_WORKER_COMMAND_DEFAULT_TIMEOUT_MS: u64 = 30_000;
pub(super) const MEDIA_WORKER_OUTPUT_KEY: &str = "worker_output";
pub(super) const MEDIA_WORKER_OUTPUT_PATH_KEY: &str = "worker_output_path";
pub(super) const MEDIA_WORKER_COMMAND_KEY: &str = "worker_command";
const MEDIA_WORKER_EXECUTOR_MESSAGE: &str =
    "media operation consumes worker_output, worker_output_path, or worker_command when supplied";
const MEDIA_SESSION_CONTROL_MESSAGE: &str = "media session-control operation records the requested runtime transition; runner evidence and SourceCoverage report observed state";
const MEDIA_MATERIAL_OPERATION_MESSAGE: &str =
    "media material operation records operator intent and lifecycle/debt requirements";
pub(super) const EMAIL_RFC822_STAGED_MODE_ID: &str = "source:email.mailbox";
pub(super) const EMAIL_MAILDIR_STAGED_MODE_ID: &str = "source:email.mailbox.maildir-staged";
pub(super) const EMAIL_MBOX_STAGED_MODE_ID: &str = "source:email.mailbox.mbox-staged";
pub(super) const EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID: &str =
    "source:email.mailbox.gmail-api-scheduled-sync";
pub(super) const EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID: &str =
    "source:email.mailbox.imap-scheduled-sync";
pub(super) const EMAIL_IMAP_IDLE_LIVE_MODE_ID: &str = "source:email.mailbox.imap-idle-live";
pub(super) const EMAIL_STAGED_SYNC_EXECUTOR_STATE: &str = "staged_email_sync_admitted";
pub(super) const EMAIL_GMAIL_SYNC_EXECUTOR_STATE: &str = "gmail_api_sync_admitted";
pub(super) const EMAIL_IMAP_SYNC_EXECUTOR_STATE: &str = "imap_sync_admitted";
pub(super) const EMAIL_GMAIL_SYNC_FAILED_EXECUTOR_STATE: &str = "gmail_api_sync_failed";
pub(super) const EMAIL_IMAP_SYNC_FAILED_EXECUTOR_STATE: &str = "imap_sync_failed";
const EMAIL_PROVIDER_CONTROL_MESSAGE: &str =
    "email provider control operation records provider account runtime intent";
const EMAIL_STAGED_REPLAY_MESSAGE: &str =
    "email staged replay operation records replay intent for staged mailbox material";
pub(super) const EMAIL_STAGED_SYNC_DEFAULT_MAX_MESSAGE_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const EMAIL_GMAIL_SYNC_DEFAULT_PAGE_SIZE: u32 = 100;
pub(super) const EMAIL_IMAP_SYNC_DEFAULT_BATCH_SIZE: u32 = 100;
pub(super) const EMAIL_IMAP_SYNC_DEFAULT_IDLE_TIMEOUT_MS: u64 = 30_000;
pub(super) const EMAIL_STAGED_MODE_IDS: &[&str] =
    &[EMAIL_MAILDIR_STAGED_MODE_ID, EMAIL_MBOX_STAGED_MODE_ID];
pub(super) const EMAIL_PROVIDER_MODE_IDS: &[&str] = &[
    EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID,
    EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID,
    EMAIL_IMAP_IDLE_LIVE_MODE_ID,
];
pub(super) const EMAIL_SYNC_MODE_IDS: &[&str] = &[
    EMAIL_MAILDIR_STAGED_MODE_ID,
    EMAIL_MBOX_STAGED_MODE_ID,
    EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID,
    EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID,
];
pub(super) const EMAIL_MATERIALIZATION_MODE_IDS: &[&str] = &[
    EMAIL_RFC822_STAGED_MODE_ID,
    EMAIL_MAILDIR_STAGED_MODE_ID,
    EMAIL_MBOX_STAGED_MODE_ID,
    EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID,
    EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID,
    EMAIL_IMAP_IDLE_LIVE_MODE_ID,
];

#[derive(Debug, Clone, Copy)]
pub(super) struct EmailProviderModeMetadata {
    pub(super) mode: EmailProviderRuntimeMode,
    pub(super) caveats: &'static [&'static str],
}

#[derive(Debug, Clone)]
pub(super) struct EmailProviderOperationScope {
    pub(super) account_binding_ref: String,
    pub(super) mailbox_scope: Option<String>,
    pub(super) cursor_value: Option<String>,
    pub(super) uidvalidity: Option<String>,
    pub(super) uid: Option<String>,
    pub(super) gmail_history_id: Option<String>,
    pub(super) page_token: Option<String>,
}

impl EmailProviderOperationScope {
    pub(super) fn from_scope(
        operation_type: &str,
        mode: EmailProviderRuntimeMode,
        scope: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Self> {
        let account_binding_ref = scope
            .get("account_binding_ref")
            .or_else(|| scope.get("account_ref"))
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                SinexError::validation(format!(
                    "package operation {operation_type} requires account_binding_ref for provider mode {}",
                    mode.mode_id()
                ))
                .with_operation("ops.start")
                .with_context("mode_id", mode.mode_id())
            })?
            .to_string();

        let parsed = Self {
            account_binding_ref,
            mailbox_scope: optional_scope_string(scope, "mailbox_scope"),
            cursor_value: optional_scope_string(scope, "cursor_value"),
            uidvalidity: optional_scope_string(scope, "uidvalidity"),
            uid: optional_scope_string(scope, "uid"),
            gmail_history_id: optional_scope_string(scope, "gmail_history_id"),
            page_token: optional_scope_string(scope, "page_token"),
        };
        parsed.validate_provider_cursor(operation_type, mode)?;
        Ok(parsed)
    }

    fn validate_provider_cursor(
        &self,
        operation_type: &str,
        mode: EmailProviderRuntimeMode,
    ) -> Result<()> {
        match mode.provider() {
            EmailProviderKind::Gmail => {
                if self.uidvalidity.is_some() || self.uid.is_some() {
                    return Err(SinexError::validation(format!(
                        "package operation {operation_type} cannot use IMAP UID cursor fields for Gmail mode {}",
                        mode.mode_id()
                    ))
                    .with_operation("ops.start")
                    .with_context("mode_id", mode.mode_id()));
                }
            }
            EmailProviderKind::Imap => {
                if self.gmail_history_id.is_some() || self.page_token.is_some() {
                    return Err(SinexError::validation(format!(
                        "package operation {operation_type} cannot use Gmail cursor fields for IMAP mode {}",
                        mode.mode_id()
                    ))
                    .with_operation("ops.start")
                    .with_context("mode_id", mode.mode_id()));
                }
            }
        }
        Ok(())
    }

    pub(super) fn cursor_value_for(&self, provider: EmailProviderKind) -> Option<String> {
        match provider {
            EmailProviderKind::Gmail => self
                .gmail_history_id
                .clone()
                .or_else(|| self.page_token.clone())
                .or_else(|| self.cursor_value.clone()),
            EmailProviderKind::Imap => match (&self.uidvalidity, &self.uid) {
                (Some(uidvalidity), Some(uid)) => Some(format!("{uidvalidity}:{uid}")),
                _ => self.cursor_value.clone(),
            },
        }
    }

    pub(super) fn to_scope_value(&self) -> serde_json::Value {
        serde_json::json!({
            "account_binding_ref": self.account_binding_ref,
            "mailbox_scope": self.mailbox_scope,
            "cursor_value": self.cursor_value,
            "uidvalidity": self.uidvalidity,
            "uid": self.uid,
            "gmail_history_id": self.gmail_history_id,
            "page_token": self.page_token,
        })
    }
}

pub(super) const fn email_provider_authorization_state_ref(
    provider: EmailProviderKind,
) -> &'static str {
    match provider {
        EmailProviderKind::Gmail => "email.mailbox.provider_authorization.gmail.oauth",
        EmailProviderKind::Imap => "email.mailbox.provider_authorization.imap.credentials",
    }
}

pub(super) const fn email_provider_sync_cursor_kind(
    provider: EmailProviderKind,
) -> EmailSyncCursorKind {
    match provider {
        EmailProviderKind::Gmail => EmailSyncCursorKind::GmailHistoryId,
        EmailProviderKind::Imap => EmailSyncCursorKind::ImapUidvalidityUid,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EmailProviderRuntimeMode {
    GmailScheduledSync,
    ImapScheduledSync,
    ImapIdleLive,
}

impl EmailProviderRuntimeMode {
    pub(super) fn from_mode_id(mode_id: &str) -> Option<Self> {
        match mode_id {
            EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID => Some(Self::GmailScheduledSync),
            EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID => Some(Self::ImapScheduledSync),
            EMAIL_IMAP_IDLE_LIVE_MODE_ID => Some(Self::ImapIdleLive),
            _ => None,
        }
    }

    pub(super) const fn provider(self) -> EmailProviderKind {
        match self {
            Self::GmailScheduledSync => EmailProviderKind::Gmail,
            Self::ImapScheduledSync | Self::ImapIdleLive => EmailProviderKind::Imap,
        }
    }

    pub(super) const fn runtime(self) -> EmailProviderRuntime {
        match self {
            Self::GmailScheduledSync | Self::ImapScheduledSync => {
                EmailProviderRuntime::ScheduledSync
            }
            Self::ImapIdleLive => EmailProviderRuntime::IdleLive,
        }
    }

    pub(super) const fn runtime_state_ref(self) -> &'static str {
        match self {
            Self::GmailScheduledSync => "email.capture_runtime.observed:gmail.scheduled_sync",
            Self::ImapScheduledSync => "email.capture_runtime.observed:imap.scheduled_sync",
            Self::ImapIdleLive => "email.capture_runtime.observed:imap.idle_live",
        }
    }

    pub(super) const fn coverage_ref(self) -> &'static str {
        match self {
            Self::GmailScheduledSync => "coverage:email.mailbox.gmail.provider_runtime",
            Self::ImapScheduledSync => "coverage:email.mailbox.imap.provider_runtime",
            Self::ImapIdleLive => "coverage:email.mailbox.imap.idle_runtime",
        }
    }

    pub(super) const fn debt_ref(self) -> &'static str {
        match self {
            Self::GmailScheduledSync => "debt:email.mailbox.gmail.provider_runtime",
            Self::ImapScheduledSync => "debt:email.mailbox.imap.provider_runtime",
            Self::ImapIdleLive => "debt:email.mailbox.imap.idle_runtime",
        }
    }

    pub(super) const fn mode_id(self) -> &'static str {
        match self {
            Self::GmailScheduledSync => EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID,
            Self::ImapScheduledSync => EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID,
            Self::ImapIdleLive => EMAIL_IMAP_IDLE_LIVE_MODE_ID,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MediaCapturePackage {
    AudioTranscript,
    ScreenOcr,
}

impl MediaCapturePackage {
    pub(super) fn from_source_id(source_id: &str) -> Option<Self> {
        match source_id {
            "media.audio-transcript" => Some(Self::AudioTranscript),
            "media.screen-ocr" => Some(Self::ScreenOcr),
            _ => None,
        }
    }

    pub(super) const fn material_class(self) -> MediaMaterialClass {
        match self {
            Self::AudioTranscript => MediaMaterialClass::AudioRecordingOrTranscript,
            Self::ScreenOcr => MediaMaterialClass::ScreenCaptureOrOcr,
        }
    }

    const fn disclosure_destinations(self) -> &'static [MediaDisclosureDestination] {
        match self {
            Self::AudioTranscript => &[
                MediaDisclosureDestination::RawMaterial,
                MediaDisclosureDestination::TranscriptText,
                MediaDisclosureDestination::ModelOutput,
                MediaDisclosureDestination::Dlq,
                MediaDisclosureDestination::Export,
                MediaDisclosureDestination::Telemetry,
            ],
            Self::ScreenOcr => &[
                MediaDisclosureDestination::RawMaterial,
                MediaDisclosureDestination::ScreenVideo,
                MediaDisclosureDestination::OcrText,
                MediaDisclosureDestination::WindowMetadata,
                MediaDisclosureDestination::ModelOutput,
                MediaDisclosureDestination::Dlq,
                MediaDisclosureDestination::Export,
                MediaDisclosureDestination::Telemetry,
            ],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MediaMaterialClass {
    AudioRecordingOrTranscript,
    ScreenCaptureOrOcr,
}

impl MediaMaterialClass {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::AudioRecordingOrTranscript => "audio_recording_or_transcript",
            Self::ScreenCaptureOrOcr => "screen_capture_or_ocr",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaDisclosureDestination {
    RawMaterial,
    TranscriptText,
    OcrText,
    ScreenVideo,
    WindowMetadata,
    ModelOutput,
    Dlq,
    Export,
    Telemetry,
}

impl MediaDisclosureDestination {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RawMaterial => "raw_material",
            Self::TranscriptText => "transcript_text",
            Self::OcrText => "ocr_text",
            Self::ScreenVideo => "screen_video",
            Self::WindowMetadata => "window_metadata",
            Self::ModelOutput => "model_output",
            Self::Dlq => "dlq",
            Self::Export => "export",
            Self::Telemetry => "telemetry",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MediaOperationAction {
    DeleteMaterial,
    ModelRun,
    Retry,
    RebuildArtifact,
    CaptureSessionControl,
    CaptureRegion,
    RecordVideo,
}

impl MediaOperationAction {
    pub(super) fn from_spec_action(action: &str) -> Option<Self> {
        match action {
            "delete_material" => Some(Self::DeleteMaterial),
            "run_model" | "run_ocr" => Some(Self::ModelRun),
            "retry" => Some(Self::Retry),
            "rebuild_artifact" => Some(Self::RebuildArtifact),
            "enable_session" | "disable_session" | "pause" | "resume" => {
                Some(Self::CaptureSessionControl)
            }
            "capture_region" => Some(Self::CaptureRegion),
            "record_video" => Some(Self::RecordVideo),
            _ => None,
        }
    }

    const fn lifecycle_requirement(self) -> &'static str {
        match self {
            Self::DeleteMaterial => "delete_redact_replay",
            Self::ModelRun | Self::Retry => "model_output",
            Self::RebuildArtifact => "artifact_rebuild",
            Self::CaptureSessionControl | Self::CaptureRegion | Self::RecordVideo => {
                "capture_session"
            }
        }
    }

    pub(super) const fn consumes_worker_output(self) -> bool {
        matches!(
            self,
            Self::ModelRun
                | Self::Retry
                | Self::RebuildArtifact
                | Self::CaptureRegion
                | Self::RecordVideo
        )
    }

    const fn invalidation_triggers(self) -> &'static [InvalidationTrigger] {
        match self {
            Self::DeleteMaterial => &[
                InvalidationTrigger::Redaction,
                InvalidationTrigger::SourceMaterialChange,
            ],
            Self::ModelRun | Self::Retry => &[
                InvalidationTrigger::ParserSemanticsChange,
                InvalidationTrigger::DisclosurePolicyChange,
            ],
            Self::RebuildArtifact => &[
                InvalidationTrigger::Redaction,
                InvalidationTrigger::SourceMaterialChange,
                InvalidationTrigger::Replay,
                InvalidationTrigger::Archive,
                InvalidationTrigger::ParserSemanticsChange,
                InvalidationTrigger::DisclosurePolicyChange,
            ],
            Self::CaptureSessionControl | Self::CaptureRegion | Self::RecordVideo => &[
                InvalidationTrigger::SourceMaterialChange,
                InvalidationTrigger::DisclosurePolicyChange,
            ],
        }
    }

    const fn producer_run_required(self) -> bool {
        matches!(self, Self::ModelRun | Self::Retry)
    }

    const fn raw_material_policy_required(self) -> bool {
        !matches!(self, Self::RebuildArtifact)
    }
}

const PACKAGE_OPERATION_SPECS: &[PackageOperationSpec] = &[
    PackageOperationSpec {
        operation_type: "media.audio-transcript.run-model",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.local-model-batch"),
        accepted_mode_ids: &["source:media.audio-transcript.local-model-batch"],
        action: "run_model",
        surface: "media_capture",
        executor_message: MEDIA_WORKER_EXECUTOR_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.retry",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.local-model-batch"),
        accepted_mode_ids: &["source:media.audio-transcript.local-model-batch"],
        action: "retry",
        surface: "media_capture",
        executor_message: MEDIA_WORKER_EXECUTOR_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.rebuild-artifact",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.local-model-batch"),
        accepted_mode_ids: &["source:media.audio-transcript.local-model-batch"],
        action: "rebuild_artifact",
        surface: "media_capture",
        executor_message: MEDIA_WORKER_EXECUTOR_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.enable-session",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.live-session"),
        accepted_mode_ids: &["source:media.audio-transcript.live-session"],
        action: "enable_session",
        surface: "media_capture",
        executor_message: MEDIA_SESSION_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.disable-session",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.live-session"),
        accepted_mode_ids: &["source:media.audio-transcript.live-session"],
        action: "disable_session",
        surface: "media_capture",
        executor_message: MEDIA_SESSION_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.pause",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.live-session"),
        accepted_mode_ids: &["source:media.audio-transcript.live-session"],
        action: "pause",
        surface: "media_capture",
        executor_message: MEDIA_SESSION_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.resume",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.live-session"),
        accepted_mode_ids: &["source:media.audio-transcript.live-session"],
        action: "resume",
        surface: "media_capture",
        executor_message: MEDIA_SESSION_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.delete-material",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.audio-bundle-staged"),
        accepted_mode_ids: &["source:media.audio-transcript.audio-bundle-staged"],
        action: "delete_material",
        surface: "media_capture",
        executor_message: MEDIA_MATERIAL_OPERATION_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.run-ocr",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.local-model-batch"),
        accepted_mode_ids: &["source:media.screen-ocr.local-model-batch"],
        action: "run_ocr",
        surface: "media_capture",
        executor_message: MEDIA_WORKER_EXECUTOR_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.retry",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.local-model-batch"),
        accepted_mode_ids: &["source:media.screen-ocr.local-model-batch"],
        action: "retry",
        surface: "media_capture",
        executor_message: MEDIA_WORKER_EXECUTOR_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.rebuild-artifact",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.local-model-batch"),
        accepted_mode_ids: &["source:media.screen-ocr.local-model-batch"],
        action: "rebuild_artifact",
        surface: "media_capture",
        executor_message: MEDIA_WORKER_EXECUTOR_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.capture-region",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.on-demand-region"),
        accepted_mode_ids: &["source:media.screen-ocr.on-demand-region"],
        action: "capture_region",
        surface: "media_capture",
        executor_message: MEDIA_WORKER_EXECUTOR_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.record-video",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.on-demand-video"),
        accepted_mode_ids: &["source:media.screen-ocr.on-demand-video"],
        action: "record_video",
        surface: "media_capture",
        executor_message: MEDIA_WORKER_EXECUTOR_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.enable-session",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.live-session"),
        accepted_mode_ids: &["source:media.screen-ocr.live-session"],
        action: "enable_session",
        surface: "media_capture",
        executor_message: MEDIA_SESSION_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.disable-session",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.live-session"),
        accepted_mode_ids: &["source:media.screen-ocr.live-session"],
        action: "disable_session",
        surface: "media_capture",
        executor_message: MEDIA_SESSION_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.pause",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.live-session"),
        accepted_mode_ids: &["source:media.screen-ocr.live-session"],
        action: "pause",
        surface: "media_capture",
        executor_message: MEDIA_SESSION_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.resume",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.live-session"),
        accepted_mode_ids: &["source:media.screen-ocr.live-session"],
        action: "resume",
        surface: "media_capture",
        executor_message: MEDIA_SESSION_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.delete-material",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.screenshot-ocr-staged"),
        accepted_mode_ids: &[
            "source:media.screen-ocr.screenshot-ocr-staged",
            "source:media.screen-ocr.video-staged",
        ],
        action: "delete_material",
        surface: "media_capture",
        executor_message: MEDIA_MATERIAL_OPERATION_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.authorize",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_PROVIDER_MODE_IDS,
        action: "authorize",
        surface: "email_capture",
        executor_message: EMAIL_PROVIDER_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.sync",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_SYNC_MODE_IDS,
        action: "sync",
        surface: "email_capture",
        executor_message: "email sync executor runs when provider or staged scope is supplied",
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.pause",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_PROVIDER_MODE_IDS,
        action: "pause",
        surface: "email_capture",
        executor_message: EMAIL_PROVIDER_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.resume",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_PROVIDER_MODE_IDS,
        action: "resume",
        surface: "email_capture",
        executor_message: EMAIL_PROVIDER_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.inspect",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_PROVIDER_MODE_IDS,
        action: "inspect",
        surface: "email_capture",
        executor_message: EMAIL_PROVIDER_CONTROL_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.replay",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_STAGED_MODE_IDS,
        action: "replay",
        surface: "email_capture",
        executor_message: EMAIL_STAGED_REPLAY_MESSAGE,
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.fetch-attachments",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_MATERIALIZATION_MODE_IDS,
        action: "fetch_attachments",
        surface: "email_capture",
        executor_message: "email attachment materialization executor pending",
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.export",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_MATERIALIZATION_MODE_IDS,
        action: "export",
        surface: "email_capture",
        executor_message: "email mailbox export executor pending",
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.rebuild-projection",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_MATERIALIZATION_MODE_IDS,
        action: "rebuild_projection",
        surface: "email_capture",
        executor_message: "email mailbox projection rebuild executor pending",
    },
];

pub(super) fn package_operation_spec(operation_type: &str) -> Option<PackageOperationSpec> {
    PACKAGE_OPERATION_SPECS
        .iter()
        .copied()
        .find(|spec| spec.operation_type == operation_type)
}

pub(super) fn package_mode_contract_metadata(mode_id: &str) -> Option<serde_json::Value> {
    let binding = source_runtime_bindings().find(|binding| binding.subject.as_str() == mode_id)?;
    Some(serde_json::json!({
        "binding": binding,
        "resource_budget": binding.resource_budget(),
    }))
}

pub(super) fn media_operation_metadata(
    spec: &PackageOperationSpec,
    mode_id: &str,
) -> Option<serde_json::Value> {
    if spec.surface != "media_capture" {
        return None;
    }

    let package = MediaCapturePackage::from_source_id(spec.source_id)?;
    let action = MediaOperationAction::from_spec_action(spec.action)?;
    let material_class = package.material_class().as_str();
    let disclosure_destinations = package
        .disclosure_destinations()
        .iter()
        .map(|destination| destination.as_str())
        .collect::<Vec<_>>();

    Some(serde_json::json!({
        "capability_issue": 1043,
        "mode_id": mode_id,
        "material_class": material_class,
        "producer_run_required": action.producer_run_required(),
        "raw_material_policy_required": action.raw_material_policy_required(),
        "material_lifecycle_requirement": action.lifecycle_requirement(),
        "disclosure_destinations": disclosure_destinations,
        "invalidation_triggers": action.invalidation_triggers(),
        "executor_contract": {
            "state": PACKAGE_OPERATION_EXECUTOR_STATE,
            "bounded_worker_required": action.producer_run_required(),
            "operator_visible_lifecycle_required": action.raw_material_policy_required(),
            "attached_executor": action.consumes_worker_output()
        }
    }))
}
