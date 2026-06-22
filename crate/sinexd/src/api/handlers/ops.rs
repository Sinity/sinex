use serde::Deserialize;
use sinex_db::DbPoolExt;
use sinex_db::repositories::state::Operation as DbOperation;
use sinex_db::repositories::state::PROJECTION_REBUILD_OPERATION_TYPE;
use sinex_primitives::Id;
use sinex_primitives::InvalidationTrigger;
use sinex_primitives::SinexError;
use sinex_primitives::domain::{
    OperationStatus, SourceMaterialFormat, SourceMaterialTimingInfoType,
};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::events::payloads::email::{
    EmailAuthorizationState, EmailCaptureRuntimeObservedPayload, EmailContinuityState,
    EmailNetworkState, EmailProviderKind, EmailProviderRuntime, EmailSyncCursorKind,
    EmailSyncCursorObservedPayload, EmailSyncState,
};
use sinex_primitives::events::{Event, SourceMaterial};
use sinex_primitives::parser::{MaterialAnchor, maybe_occurrence_key_string};
use sinex_primitives::rpc::sources::{SourceMaterialMetadataContract, SourceOrigin};
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

// Re-export shared types
pub use sinex_primitives::rpc::ops::{
    Operation, OpsCancelRequest, OpsCancelResponse, OpsGetRequest, OpsGetResponse, OpsListRequest,
    OpsListResponse, OpsStartRequest, OpsStartResponse,
};

type Result<T> = std::result::Result<T, SinexError>;

fn default_ops_limit() -> i64 {
    100
}

/// Convert a repository `OperationRecord` to the RPC Operation type.
fn record_to_operation(record: sinex_db::repositories::OperationRecord) -> Operation {
    Operation {
        id: record.id.to_string(),
        operation_type: record.operation_type,
        operator: record.operator,
        scope: record.scope,
        result_status: record.result_status,
        result_message: record.result_message,
        preview_summary: record.preview_summary,
        duration_ms: record.duration_ms,
    }
}

/// Handle POST /ops/start - start a new operation
///
/// # Authorization
///
/// Write operations are logged for audit purposes.
pub async fn handle_ops_start(
    pool: &PgPool,
    request: OpsStartRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<OpsStartResponse> {
    use tracing::info;

    let scope_jsonb = request.scope.unwrap_or(serde_json::json!({}));
    let actor = auth.actor_id();

    let record = if request.operation_type == PROJECTION_REBUILD_OPERATION_TYPE {
        start_projection_rebuild_operation(pool, actor, scope_jsonb).await?
    } else if package_operation_spec(&request.operation_type).is_some() {
        start_package_operation(pool, actor, &request.operation_type, scope_jsonb).await?
    } else {
        pool.state()
            .start_operation(&request.operation_type, actor, scope_jsonb)
            .await?
    };

    info!(
        actor = %actor,
        operation_id = %record.id,
        operation_type = %request.operation_type,
        "Operation started"
    );

    let response = OpsStartResponse {
        operation: record_to_operation(record),
    };

    Ok(response)
}

#[derive(Debug, Clone, Copy)]
struct PackageOperationSpec {
    operation_type: &'static str,
    source_id: &'static str,
    default_mode_id: Option<&'static str>,
    accepted_mode_ids: &'static [&'static str],
    action: &'static str,
    surface: &'static str,
    executor_message: &'static str,
}

const PACKAGE_OPERATION_EXECUTOR_STATE: &str = "awaiting_runtime_executor";
const MEDIA_WORKER_OUTPUT_EXECUTOR_STATE: &str = "worker_output_admitted";
const MEDIA_WORKER_COMMAND_EXECUTOR_STATE: &str = "worker_command_admitted";
const MEDIA_WORKER_COMMAND_FAILED_STATE: &str = "worker_command_failed";
const MEDIA_WORKER_OUTPUT_MAX_BYTES: usize = 10 * 1024 * 1024;
const MEDIA_WORKER_STDERR_MAX_BYTES: usize = 64 * 1024;
const MEDIA_WORKER_COMMAND_DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MEDIA_WORKER_OUTPUT_KEY: &str = "worker_output";
const MEDIA_WORKER_OUTPUT_PATH_KEY: &str = "worker_output_path";
const MEDIA_WORKER_COMMAND_KEY: &str = "worker_command";
const EMAIL_MAILDIR_STAGED_MODE_ID: &str = "source:email.mailbox.maildir-staged";
const EMAIL_MBOX_STAGED_MODE_ID: &str = "source:email.mailbox.mbox-staged";
const EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID: &str = "source:email.mailbox.gmail-api-scheduled-sync";
const EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID: &str = "source:email.mailbox.imap-scheduled-sync";
const EMAIL_IMAP_IDLE_LIVE_MODE_ID: &str = "source:email.mailbox.imap-idle-live";
const EMAIL_STAGED_MODE_IDS: &[&str] = &[EMAIL_MAILDIR_STAGED_MODE_ID, EMAIL_MBOX_STAGED_MODE_ID];
const EMAIL_PROVIDER_MODE_IDS: &[&str] = &[
    EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID,
    EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID,
    EMAIL_IMAP_IDLE_LIVE_MODE_ID,
];
const EMAIL_SYNC_MODE_IDS: &[&str] = &[
    EMAIL_MAILDIR_STAGED_MODE_ID,
    EMAIL_MBOX_STAGED_MODE_ID,
    EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID,
    EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID,
];

#[derive(Debug, Clone, Copy)]
struct EmailProviderModeMetadata {
    mode: EmailProviderRuntimeMode,
    caveats: &'static [&'static str],
}

#[derive(Debug, Clone)]
struct EmailProviderOperationScope {
    account_binding_ref: String,
    mailbox_scope: Option<String>,
    cursor_value: Option<String>,
    uidvalidity: Option<String>,
    uid: Option<String>,
    gmail_history_id: Option<String>,
    page_token: Option<String>,
}

impl EmailProviderOperationScope {
    fn from_scope(
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

    fn cursor_value_for(&self, provider: EmailProviderKind) -> Option<String> {
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

    fn to_scope_value(&self) -> serde_json::Value {
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

const fn email_provider_authorization_state_ref(provider: EmailProviderKind) -> &'static str {
    match provider {
        EmailProviderKind::Gmail => "email.mailbox.provider_authorization.gmail.oauth",
        EmailProviderKind::Imap => "email.mailbox.provider_authorization.imap.credentials",
    }
}

const fn email_provider_sync_cursor_kind(provider: EmailProviderKind) -> EmailSyncCursorKind {
    match provider {
        EmailProviderKind::Gmail => EmailSyncCursorKind::GmailHistoryId,
        EmailProviderKind::Imap => EmailSyncCursorKind::ImapUidvalidityUid,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmailProviderRuntimeMode {
    GmailScheduledSync,
    ImapScheduledSync,
    ImapIdleLive,
}

impl EmailProviderRuntimeMode {
    fn from_mode_id(mode_id: &str) -> Option<Self> {
        match mode_id {
            EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID => Some(Self::GmailScheduledSync),
            EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID => Some(Self::ImapScheduledSync),
            EMAIL_IMAP_IDLE_LIVE_MODE_ID => Some(Self::ImapIdleLive),
            _ => None,
        }
    }

    const fn provider(self) -> EmailProviderKind {
        match self {
            Self::GmailScheduledSync => EmailProviderKind::Gmail,
            Self::ImapScheduledSync | Self::ImapIdleLive => EmailProviderKind::Imap,
        }
    }

    const fn runtime(self) -> EmailProviderRuntime {
        match self {
            Self::GmailScheduledSync | Self::ImapScheduledSync => {
                EmailProviderRuntime::ScheduledSync
            }
            Self::ImapIdleLive => EmailProviderRuntime::IdleLive,
        }
    }

    const fn runtime_state_ref(self) -> &'static str {
        match self {
            Self::GmailScheduledSync => "email.capture_runtime.observed:gmail.scheduled_sync",
            Self::ImapScheduledSync => "email.capture_runtime.observed:imap.scheduled_sync",
            Self::ImapIdleLive => "email.capture_runtime.observed:imap.idle_live",
        }
    }

    const fn coverage_ref(self) -> &'static str {
        match self {
            Self::GmailScheduledSync => "coverage:email.mailbox.gmail.provider_runtime",
            Self::ImapScheduledSync => "coverage:email.mailbox.imap.provider_runtime",
            Self::ImapIdleLive => "coverage:email.mailbox.imap.idle_runtime",
        }
    }

    const fn debt_ref(self) -> &'static str {
        match self {
            Self::GmailScheduledSync => "debt:email.mailbox.gmail.provider_runtime",
            Self::ImapScheduledSync => "debt:email.mailbox.imap.provider_runtime",
            Self::ImapIdleLive => "debt:email.mailbox.imap.idle_runtime",
        }
    }

    const fn mode_id(self) -> &'static str {
        match self {
            Self::GmailScheduledSync => EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID,
            Self::ImapScheduledSync => EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID,
            Self::ImapIdleLive => EMAIL_IMAP_IDLE_LIVE_MODE_ID,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaCapturePackage {
    AudioTranscript,
    ScreenOcr,
}

impl MediaCapturePackage {
    fn from_source_id(source_id: &str) -> Option<Self> {
        match source_id {
            "media.audio-transcript" => Some(Self::AudioTranscript),
            "media.screen-ocr" => Some(Self::ScreenOcr),
            _ => None,
        }
    }

    const fn material_class(self) -> MediaMaterialClass {
        match self {
            Self::AudioTranscript => MediaMaterialClass::AudioRecordingOrTranscript,
            Self::ScreenOcr => MediaMaterialClass::ScreenshotOrOcr,
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
enum MediaMaterialClass {
    AudioRecordingOrTranscript,
    ScreenshotOrOcr,
}

impl MediaMaterialClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::AudioRecordingOrTranscript => "audio_recording_or_transcript",
            Self::ScreenshotOrOcr => "screenshot_or_ocr",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaDisclosureDestination {
    RawMaterial,
    TranscriptText,
    OcrText,
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
            Self::WindowMetadata => "window_metadata",
            Self::ModelOutput => "model_output",
            Self::Dlq => "dlq",
            Self::Export => "export",
            Self::Telemetry => "telemetry",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaOperationAction {
    DeleteMaterial,
    ModelRun,
    Retry,
    RebuildArtifact,
    CaptureSessionControl,
    CaptureRegion,
}

impl MediaOperationAction {
    fn from_spec_action(action: &str) -> Option<Self> {
        match action {
            "delete_material" => Some(Self::DeleteMaterial),
            "run_model" | "run_ocr" => Some(Self::ModelRun),
            "retry" => Some(Self::Retry),
            "rebuild_artifact" => Some(Self::RebuildArtifact),
            "enable_session" | "disable_session" | "pause" | "resume" => {
                Some(Self::CaptureSessionControl)
            }
            "capture_region" => Some(Self::CaptureRegion),
            _ => None,
        }
    }

    const fn lifecycle_requirement(self) -> &'static str {
        match self {
            Self::DeleteMaterial => "delete_redact_replay",
            Self::ModelRun | Self::Retry => "model_output",
            Self::RebuildArtifact => "artifact_rebuild",
            Self::CaptureSessionControl | Self::CaptureRegion => "capture_session",
        }
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
            Self::CaptureSessionControl | Self::CaptureRegion => &[
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
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.retry",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.local-model-batch"),
        accepted_mode_ids: &["source:media.audio-transcript.local-model-batch"],
        action: "retry",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.rebuild-artifact",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.local-model-batch"),
        accepted_mode_ids: &["source:media.audio-transcript.local-model-batch"],
        action: "rebuild_artifact",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.enable-session",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.live-session"),
        accepted_mode_ids: &["source:media.audio-transcript.live-session"],
        action: "enable_session",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.disable-session",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.live-session"),
        accepted_mode_ids: &["source:media.audio-transcript.live-session"],
        action: "disable_session",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.pause",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.live-session"),
        accepted_mode_ids: &["source:media.audio-transcript.live-session"],
        action: "pause",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.resume",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.live-session"),
        accepted_mode_ids: &["source:media.audio-transcript.live-session"],
        action: "resume",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.audio-transcript.delete-material",
        source_id: "media.audio-transcript",
        default_mode_id: Some("source:media.audio-transcript.audio-bundle-staged"),
        accepted_mode_ids: &["source:media.audio-transcript.audio-bundle-staged"],
        action: "delete_material",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.run-ocr",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.local-model-batch"),
        accepted_mode_ids: &["source:media.screen-ocr.local-model-batch"],
        action: "run_ocr",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.retry",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.local-model-batch"),
        accepted_mode_ids: &["source:media.screen-ocr.local-model-batch"],
        action: "retry",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.rebuild-artifact",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.local-model-batch"),
        accepted_mode_ids: &["source:media.screen-ocr.local-model-batch"],
        action: "rebuild_artifact",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.capture-region",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.on-demand-region"),
        accepted_mode_ids: &["source:media.screen-ocr.on-demand-region"],
        action: "capture_region",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.enable-session",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.live-session"),
        accepted_mode_ids: &["source:media.screen-ocr.live-session"],
        action: "enable_session",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.disable-session",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.live-session"),
        accepted_mode_ids: &["source:media.screen-ocr.live-session"],
        action: "disable_session",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.pause",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.live-session"),
        accepted_mode_ids: &["source:media.screen-ocr.live-session"],
        action: "pause",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.resume",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.live-session"),
        accepted_mode_ids: &["source:media.screen-ocr.live-session"],
        action: "resume",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "media.screen-ocr.delete-material",
        source_id: "media.screen-ocr",
        default_mode_id: Some("source:media.screen-ocr.screenshot-ocr-staged"),
        accepted_mode_ids: &["source:media.screen-ocr.screenshot-ocr-staged"],
        action: "delete_material",
        surface: "media_capture",
        executor_message: "media operation recorded; runtime executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.authorize",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_PROVIDER_MODE_IDS,
        action: "authorize",
        surface: "email_capture",
        executor_message: "email operation recorded; provider executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.sync",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_SYNC_MODE_IDS,
        action: "sync",
        surface: "email_capture",
        executor_message: "email operation recorded; provider or staged executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.pause",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_PROVIDER_MODE_IDS,
        action: "pause",
        surface: "email_capture",
        executor_message: "email operation recorded; provider executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.resume",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_PROVIDER_MODE_IDS,
        action: "resume",
        surface: "email_capture",
        executor_message: "email operation recorded; provider executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.inspect",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_PROVIDER_MODE_IDS,
        action: "inspect",
        surface: "email_capture",
        executor_message: "email operation recorded; provider executor is not yet attached",
    },
    PackageOperationSpec {
        operation_type: "email.mailbox.replay",
        source_id: "email.mailbox",
        default_mode_id: None,
        accepted_mode_ids: EMAIL_STAGED_MODE_IDS,
        action: "replay",
        surface: "email_capture",
        executor_message: "email operation recorded; staged replay executor is not yet attached",
    },
];

fn package_operation_spec(operation_type: &str) -> Option<PackageOperationSpec> {
    PACKAGE_OPERATION_SPECS
        .iter()
        .copied()
        .find(|spec| spec.operation_type == operation_type)
}

async fn start_package_operation(
    pool: &PgPool,
    actor: &str,
    operation_type: &str,
    scope: serde_json::Value,
) -> Result<sinex_db::repositories::OperationRecord> {
    let spec = package_operation_spec(operation_type).ok_or_else(|| {
        SinexError::validation(format!(
            "unsupported package operation type: {operation_type}"
        ))
        .with_operation("ops.start")
    })?;

    let mut scope = match scope {
        serde_json::Value::Object(scope) => scope,
        _ => {
            return Err(
                SinexError::validation("package operation scope must be a JSON object")
                    .with_operation("ops.start"),
            );
        }
    };

    if let Some(source_id) = scope.get("source_id").and_then(serde_json::Value::as_str)
        && source_id != spec.source_id
    {
        return Err(SinexError::validation(format!(
            "package operation {operation_type} requires source_id {}",
            spec.source_id
        ))
        .with_operation("ops.start")
        .with_context("source_id", source_id.to_string()));
    }

    let mode_id = match scope.get("mode_id").and_then(serde_json::Value::as_str) {
        Some(mode_id) if spec.accepted_mode_ids.contains(&mode_id) => mode_id.to_string(),
        Some(mode_id) => {
            return Err(SinexError::validation(format!(
                "package operation {operation_type} requires one of these mode_id values: {}",
                spec.accepted_mode_ids.join(", ")
            ))
            .with_operation("ops.start")
            .with_context("mode_id", mode_id.to_string()));
        }
        None => spec
            .default_mode_id
            .ok_or_else(|| {
                SinexError::validation(format!(
                    "package operation {operation_type} requires mode_id; accepted values: {}",
                    spec.accepted_mode_ids.join(", ")
                ))
                .with_operation("ops.start")
            })?
            .to_string(),
    };

    scope.insert("surface".to_string(), serde_json::json!(spec.surface));
    scope.insert("source_id".to_string(), serde_json::json!(spec.source_id));
    scope.insert("mode_id".to_string(), serde_json::json!(&mode_id));
    scope.insert("action".to_string(), serde_json::json!(spec.action));
    scope.insert(
        "executor_state".to_string(),
        serde_json::json!(PACKAGE_OPERATION_EXECUTOR_STATE),
    );
    scope.remove("provider_runtime");
    scope.remove("provider_cursor");
    let operation_metadata = media_operation_metadata(&spec, &mode_id);
    if let Some(metadata) = operation_metadata.clone() {
        scope.insert("operation_metadata".to_string(), metadata);
    }

    let mut preview_summary = serde_json::json!({
        "surface": spec.surface,
        "operation_type": operation_type,
        "source_id": spec.source_id,
        "mode_id": mode_id,
        "action": spec.action,
        "executor_state": PACKAGE_OPERATION_EXECUTOR_STATE,
        "message": spec.executor_message,
    });
    if let Some(provider_metadata) = email_provider_mode_metadata(&mode_id) {
        let provider_scope = EmailProviderOperationScope::from_scope(
            operation_type,
            provider_metadata.mode,
            &scope,
        )?;
        scope.insert(
            "account_binding_ref".to_string(),
            serde_json::json!(&provider_scope.account_binding_ref),
        );
        scope.insert(
            "provider_operation_scope".to_string(),
            provider_scope.to_scope_value(),
        );
        let provider_runtime =
            email_provider_mode_metadata_value(provider_metadata, &provider_scope);
        let provider_cursor =
            email_provider_cursor_metadata_value(provider_metadata.mode, &provider_scope);
        scope.insert("provider_runtime".to_string(), provider_runtime.clone());
        scope.insert("provider_cursor".to_string(), provider_cursor.clone());
        preview_summary
            .as_object_mut()
            .expect("package operation preview is an object")
            .insert("provider_runtime".to_string(), provider_runtime);
        preview_summary
            .as_object_mut()
            .expect("package operation preview is an object")
            .insert("provider_cursor".to_string(), provider_cursor);
    }
    if let Some(metadata) = operation_metadata {
        preview_summary
            .as_object_mut()
            .expect("package operation preview is an object")
            .insert("operation_metadata".to_string(), metadata);
    }

    if spec.surface == "media_capture"
        && let Some(media_result) =
            execute_media_worker_output(pool, &spec, &mode_id, &mut scope, &mut preview_summary)
                .await?
    {
        return pool
            .state()
            .log_operation(DbOperation {
                id: None,
                operation_type: operation_type.to_string(),
                operator: actor.to_string(),
                scope: Some(serde_json::Value::Object(scope)),
                result_status: media_result.status,
                result_message: Some(media_result.message),
                preview_summary: Some(preview_summary),
                duration_ms: media_result.duration_ms,
            })
            .await;
    }

    pool.state()
        .log_operation(DbOperation {
            id: None,
            operation_type: operation_type.to_string(),
            operator: actor.to_string(),
            scope: Some(serde_json::Value::Object(scope)),
            result_status: OperationStatus::Running,
            result_message: Some(format!("{}; executor pending", spec.surface)),
            preview_summary: Some(preview_summary),
            duration_ms: None,
        })
        .await
}

struct MediaWorkerOutputResult {
    status: OperationStatus,
    message: String,
    duration_ms: Option<i32>,
}

struct MediaWorkerOutput {
    bytes: Vec<u8>,
    source_identifier: String,
    executor_state: &'static str,
    duration_ms: Option<i32>,
}

struct MediaWorkerCommandOutcome {
    output: Option<MediaWorkerOutput>,
    summary: serde_json::Value,
    failure_message: Option<String>,
    duration_ms: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MediaWorkerCommandRequest {
    program: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    output_source_identifier: Option<String>,
}

impl MediaWorkerCommandRequest {
    fn validate(&self) -> Result<()> {
        if self.program.trim().is_empty() {
            return Err(SinexError::validation(
                "media worker command requires a non-empty program",
            )
            .with_operation("ops.start"));
        }
        if self.args.len() > 256 {
            return Err(SinexError::validation(
                "media worker command accepts at most 256 arguments",
            )
            .with_operation("ops.start")
            .with_context("argument_count", self.args.len().to_string()));
        }
        Ok(())
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(
            self.timeout_ms
                .unwrap_or(MEDIA_WORKER_COMMAND_DEFAULT_TIMEOUT_MS)
                .max(1),
        )
    }

    fn sanitized_scope(&self) -> serde_json::Value {
        serde_json::json!({
            "program": self.program,
            "args": self.args,
            "timeout_ms": self.timeout().as_millis(),
            "output_source_identifier": self.output_source_identifier,
            "stdout_max_bytes": MEDIA_WORKER_OUTPUT_MAX_BYTES,
            "stderr_max_bytes": MEDIA_WORKER_STDERR_MAX_BYTES,
        })
    }
}

async fn execute_media_worker_output(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
) -> Result<Option<MediaWorkerOutputResult>> {
    let Some(worker_output) = resolve_media_worker_output(scope, preview_summary).await? else {
        return Ok(None);
    };
    let package = MediaCapturePackage::from_source_id(spec.source_id).ok_or_else(|| {
        SinexError::validation("media worker output operation requires a media source package")
            .with_operation("ops.start")
            .with_context("source_id", spec.source_id)
    })?;
    let action = MediaOperationAction::from_spec_action(spec.action).ok_or_else(|| {
        SinexError::validation("media worker output operation requires a media action")
            .with_operation("ops.start")
            .with_context("action", spec.action)
    })?;
    if !matches!(
        action,
        MediaOperationAction::ModelRun
            | MediaOperationAction::Retry
            | MediaOperationAction::CaptureRegion
            | MediaOperationAction::RebuildArtifact
    ) {
        return Err(SinexError::validation(format!(
            "media operation {} does not consume worker output",
            spec.operation_type
        ))
        .with_operation("ops.start"));
    }

    if let Some(message) = worker_output.failure_message {
        scope.insert(
            "executor_state".to_string(),
            serde_json::json!(MEDIA_WORKER_COMMAND_FAILED_STATE),
        );
        let preview = preview_summary
            .as_object_mut()
            .expect("package operation preview is an object");
        preview.insert(
            "executor_state".to_string(),
            serde_json::json!(MEDIA_WORKER_COMMAND_FAILED_STATE),
        );
        preview.insert("worker_command".to_string(), worker_output.summary);
        return Ok(Some(MediaWorkerOutputResult {
            status: OperationStatus::Failed,
            message,
            duration_ms: worker_output.duration_ms,
        }));
    }

    let worker_output = worker_output
        .output
        .expect("successful media worker resolution should include output");
    let mut contract = SourceMaterialMetadataContract::new(
        SourceMaterialFormat::Json,
        SourceMaterialTimingInfoType::StagedAt,
    );
    contract.origin = Some(SourceOrigin {
        source_uri: Some(worker_output.source_identifier.clone()),
        binding_id: Some(mode_id.to_string()),
        ..SourceOrigin::default()
    });

    let material =
        sinex_db::repositories::SourceMaterial::blob_text(&worker_output.source_identifier)
            .with_metadata_contract(&contract)
            .with_metadata(serde_json::json!({
                "media_worker_output": {
                    "source_id": spec.source_id,
                    "mode_id": mode_id,
                    "operation_type": spec.operation_type,
                    "action": spec.action,
                    "material_class": package.material_class().as_str()
                }
            }));
    let mut material_record = pool.source_materials().register_material(material).await?;
    let total_bytes = i64::try_from(worker_output.bytes.len()).map_err(|error| {
        SinexError::validation("media worker output is too large to record")
            .with_std_error(&error)
            .with_operation("ops.start")
    })?;
    sqlx::query!(
        "UPDATE raw.source_material_registry SET total_bytes = $1 WHERE id = $2",
        total_bytes,
        material_record.id
    )
    .execute(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to persist media worker output material size")
            .with_context("material_id", material_record.id.to_string())
            .with_std_error(&error)
    })?;
    material_record.total_bytes = Some(total_bytes);

    let dispatch = crate::sources::dispatch::default_parser_dispatch();
    let outcome = dispatch(
        spec.source_id,
        &worker_output.bytes,
        Some(material_record.id),
    )
    .map_err(|error| {
        SinexError::parse("media worker output parser failed")
            .with_context("source_id", spec.source_id)
            .with_context("mode_id", mode_id)
            .with_context("parse_error", error)
            .with_operation("ops.start")
    })?;

    let mut admitted_event_ids = Vec::new();
    for intent in outcome.events {
        let event = parsed_media_intent_to_event(
            intent,
            Id::<SourceMaterial>::from_uuid(material_record.id),
        )?;
        let persisted = pool.events().insert(event).await?;
        if let Some(id) = persisted.id {
            admitted_event_ids.push(id.to_string());
        }
    }

    scope.insert(
        "executor_state".to_string(),
        serde_json::json!(worker_output.executor_state),
    );
    scope.insert(
        "worker_output_material_id".to_string(),
        serde_json::json!(material_record.id.to_string()),
    );
    scope.insert(
        "worker_output_event_ids".to_string(),
        serde_json::json!(admitted_event_ids),
    );
    scope.insert(
        "worker_output_parser".to_string(),
        serde_json::json!({
            "parser_id": outcome.parser_id,
            "parser_version": outcome.parser_version
        }),
    );

    let preview = preview_summary
        .as_object_mut()
        .expect("package operation preview is an object");
    preview.insert(
        "executor_state".to_string(),
        serde_json::json!(worker_output.executor_state),
    );
    preview.insert(
        "worker_output_material_id".to_string(),
        serde_json::json!(material_record.id.to_string()),
    );
    preview.insert(
        "admitted_event_count".to_string(),
        serde_json::json!(
            scope["worker_output_event_ids"]
                .as_array()
                .map_or(0, std::vec::Vec::len)
        ),
    );

    Ok(Some(MediaWorkerOutputResult {
        status: OperationStatus::Success,
        message: match worker_output.executor_state {
            MEDIA_WORKER_COMMAND_EXECUTOR_STATE => {
                format!("{}; media worker command output admitted", spec.surface)
            }
            _ => format!("{}; media worker output admitted", spec.surface),
        },
        duration_ms: worker_output.duration_ms,
    }))
}

async fn resolve_media_worker_output(
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
) -> Result<Option<MediaWorkerCommandOutcome>> {
    let has_direct_output = scope.contains_key(MEDIA_WORKER_OUTPUT_KEY)
        || scope.contains_key(MEDIA_WORKER_OUTPUT_PATH_KEY);
    let has_command = scope.contains_key(MEDIA_WORKER_COMMAND_KEY);
    if has_direct_output && has_command {
        return Err(SinexError::validation(
            "media operation accepts either worker_output/worker_output_path or worker_command, not both",
        )
        .with_operation("ops.start"));
    }

    if has_command {
        let value = scope
            .remove(MEDIA_WORKER_COMMAND_KEY)
            .expect("checked worker command presence");
        let request: MediaWorkerCommandRequest =
            serde_json::from_value(value).map_err(|error| {
                SinexError::validation("media worker command has invalid shape")
                    .with_std_error(&error)
                    .with_operation("ops.start")
            })?;
        request.validate()?;
        scope.insert("worker_command".to_string(), request.sanitized_scope());
        return execute_media_worker_command(request, preview_summary)
            .await
            .map(Some);
    }

    let Some(output) = read_media_worker_output(scope).await? else {
        return Ok(None);
    };
    scope.remove(MEDIA_WORKER_OUTPUT_KEY);
    scope.remove(MEDIA_WORKER_OUTPUT_PATH_KEY);
    Ok(Some(MediaWorkerCommandOutcome {
        output: Some(output),
        summary: serde_json::json!({ "kind": "direct_worker_output" }),
        failure_message: None,
        duration_ms: None,
    }))
}

async fn execute_media_worker_command(
    request: MediaWorkerCommandRequest,
    preview_summary: &mut serde_json::Value,
) -> Result<MediaWorkerCommandOutcome> {
    let started = Instant::now();
    let mut child = Command::new(&request.program)
        .args(&request.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            SinexError::io("Failed to spawn media worker command")
                .with_context("program", request.program.clone())
                .with_std_error(&error)
                .with_operation("ops.start")
        })?;

    let stdout = child.stdout.take().ok_or_else(|| {
        SinexError::io("Failed to capture media worker stdout").with_operation("ops.start")
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        SinexError::io("Failed to capture media worker stderr").with_operation("ops.start")
    })?;
    let stdout_task = tokio::spawn(read_limited(
        stdout,
        MEDIA_WORKER_OUTPUT_MAX_BYTES,
        "stdout",
    ));
    let stderr_task = tokio::spawn(read_limited(
        stderr,
        MEDIA_WORKER_STDERR_MAX_BYTES,
        "stderr",
    ));

    let wait_result = tokio::time::timeout(request.timeout(), child.wait()).await;
    let timed_out = wait_result.is_err();
    let status = match wait_result {
        Ok(result) => Some(result.map_err(|error| {
            SinexError::io("Failed waiting for media worker command")
                .with_std_error(&error)
                .with_operation("ops.start")
        })?),
        Err(_) => {
            let _ = child.kill().await;
            None
        }
    };

    let stdout = task_bytes(stdout_task, "stdout").await;
    let stderr = task_bytes(stderr_task, "stderr").await;
    let duration_ms = elapsed_millis(started);

    let stdout_bytes = stdout.as_ref().map_or(0, Vec::len);
    let stderr_bytes = stderr.as_ref().map_or(0, Vec::len);
    let mut summary = serde_json::json!({
        "program": request.program,
        "args": request.args,
        "timeout_ms": request.timeout().as_millis(),
        "duration_ms": duration_ms,
        "timed_out": timed_out,
        "stdout_bytes": stdout_bytes,
        "stderr_bytes": stderr_bytes,
    });
    if let Some(status) = status {
        summary["exit_code"] = status
            .code()
            .map_or(serde_json::Value::Null, |code| serde_json::json!(code));
        if !status.success() {
            return Ok(MediaWorkerCommandOutcome {
                output: None,
                summary,
                failure_message: Some(format!(
                    "media_capture; media worker command exited with status {status}"
                )),
                duration_ms: Some(duration_ms),
            });
        }
    }
    if timed_out {
        return Ok(MediaWorkerCommandOutcome {
            output: None,
            summary,
            failure_message: Some("media_capture; media worker command timed out".to_string()),
            duration_ms: Some(duration_ms),
        });
    }

    let stdout = stdout.map_err(|error| {
        SinexError::io("Failed to read media worker stdout")
            .with_std_error(&error)
            .with_operation("ops.start")
    })?;
    let stderr = stderr.map_err(|error| {
        SinexError::io("Failed to read media worker stderr")
            .with_std_error(&error)
            .with_operation("ops.start")
    })?;
    summary["stdout_bytes"] = serde_json::json!(stdout.len());
    summary["stderr_bytes"] = serde_json::json!(stderr.len());
    preview_summary
        .as_object_mut()
        .expect("package operation preview is an object")
        .insert("worker_command".to_string(), summary.clone());

    Ok(MediaWorkerCommandOutcome {
        output: Some(MediaWorkerOutput {
            bytes: stdout,
            source_identifier: request.output_source_identifier.unwrap_or_else(|| {
                format!("process://media-worker-command/{}", uuid::Uuid::now_v7())
            }),
            executor_state: MEDIA_WORKER_COMMAND_EXECUTOR_STATE,
            duration_ms: Some(duration_ms),
        }),
        summary,
        failure_message: None,
        duration_ms: Some(duration_ms),
    })
}

async fn read_limited<R>(
    mut reader: R,
    max_len: usize,
    stream_name: &'static str,
) -> std::io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > max_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("media worker {stream_name} exceeded {max_len} bytes"),
            ));
        }
        output.extend_from_slice(&chunk[..read]);
    }
}

async fn task_bytes(
    task: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    stream_name: &'static str,
) -> std::io::Result<Vec<u8>> {
    task.await.map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("media worker {stream_name} reader task failed: {error}"),
        )
    })?
}

fn elapsed_millis(started: Instant) -> i32 {
    i32::try_from(started.elapsed().as_millis()).unwrap_or(i32::MAX)
}

async fn read_media_worker_output(
    scope: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<MediaWorkerOutput>> {
    if let Some(path) = scope
        .get(MEDIA_WORKER_OUTPUT_PATH_KEY)
        .and_then(serde_json::Value::as_str)
    {
        let bytes = tokio::fs::read(path).await.map_err(|error| {
            SinexError::io("Failed to read media worker output file")
                .with_context(MEDIA_WORKER_OUTPUT_PATH_KEY, path)
                .with_std_error(&error)
                .with_operation("ops.start")
        })?;
        validate_media_worker_output_size(bytes.len())?;
        return Ok(Some(MediaWorkerOutput {
            bytes,
            source_identifier: path.to_string(),
            executor_state: MEDIA_WORKER_OUTPUT_EXECUTOR_STATE,
            duration_ms: None,
        }));
    }

    let Some(value) = scope.get(MEDIA_WORKER_OUTPUT_KEY) else {
        return Ok(None);
    };
    let bytes = match value {
        serde_json::Value::String(text) => text.as_bytes().to_vec(),
        other => serde_json::to_vec(other).map_err(|error| {
            SinexError::serialization("Failed to serialize media worker output JSON")
                .with_std_error(&error)
                .with_operation("ops.start")
        })?,
    };
    validate_media_worker_output_size(bytes.len())?;
    Ok(Some(MediaWorkerOutput {
        bytes,
        source_identifier: format!("memory://media-worker-output/{}", uuid::Uuid::now_v7()),
        executor_state: MEDIA_WORKER_OUTPUT_EXECUTOR_STATE,
        duration_ms: None,
    }))
}

fn validate_media_worker_output_size(byte_len: usize) -> Result<()> {
    if byte_len > MEDIA_WORKER_OUTPUT_MAX_BYTES {
        return Err(SinexError::validation(format!(
            "media worker output is limited to {MEDIA_WORKER_OUTPUT_MAX_BYTES} bytes"
        ))
        .with_context("worker_output_bytes", byte_len.to_string())
        .with_operation("ops.start"));
    }
    Ok(())
}

fn parsed_media_intent_to_event(
    intent: sinex_primitives::parser::ParsedEventIntent,
    material_id: Id<SourceMaterial>,
) -> Result<Event<serde_json::Value>> {
    let anchor_byte = match intent.anchor {
        MaterialAnchor::ByteRange { start, .. } => start.min(i64::MAX as u64) as i64,
        MaterialAnchor::Line { byte_start, .. } => byte_start.min(i64::MAX as u64) as i64,
        MaterialAnchor::StreamFrame {
            material_offset, ..
        } => material_offset.min(i64::MAX as u64) as i64,
        MaterialAnchor::SqliteRow { rowid, .. } => rowid,
        MaterialAnchor::DirectoryEntry { .. } | MaterialAnchor::GitObject { .. } => 0,
    };
    let mut builder = DynamicPayload::new(intent.event_source, intent.event_type, intent.payload)
        .from_material_at(material_id, anchor_byte);
    if let Some(quality) = intent.timing.resolved_quality() {
        builder = builder.at_time_with_quality(intent.ts_orig, quality);
    }
    let mut event = builder.build()?;
    event.equivalence_key = maybe_occurrence_key_string(intent.occurrence_key.as_ref());
    Ok(event)
}

fn optional_scope_string(
    scope: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<String> {
    scope
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
}

fn email_provider_mode_metadata(mode_id: &str) -> Option<EmailProviderModeMetadata> {
    let mode = EmailProviderRuntimeMode::from_mode_id(mode_id)?;
    match mode {
        EmailProviderRuntimeMode::GmailScheduledSync => Some(EmailProviderModeMetadata {
            mode,
            caveats: &[
                "provider executor not attached",
                "authorization state is declared but not persisted",
                "sync cursor persistence waits for Gmail history-id runtime",
            ],
        }),
        EmailProviderRuntimeMode::ImapScheduledSync => Some(EmailProviderModeMetadata {
            mode,
            caveats: &[
                "provider executor not attached",
                "authorization state is declared but not persisted",
                "sync cursor persistence waits for IMAP UIDVALIDITY/UID runtime",
            ],
        }),
        EmailProviderRuntimeMode::ImapIdleLive => Some(EmailProviderModeMetadata {
            mode,
            caveats: &[
                "provider executor not attached",
                "authorization state is declared but not persisted",
                "IDLE reconnect/backoff state waits for runtime implementation",
            ],
        }),
    }
}

fn email_provider_mode_metadata_value(
    metadata: EmailProviderModeMetadata,
    scope: &EmailProviderOperationScope,
) -> serde_json::Value {
    let provider = metadata.mode.provider();
    let cursor_kind = email_provider_sync_cursor_kind(provider);
    let runtime_payload = EmailCaptureRuntimeObservedPayload {
        provider,
        account_binding_ref: scope.account_binding_ref.clone(),
        mode_id: metadata.mode.mode_id().to_string(),
        observed_at: Timestamp::now(),
        provider_runtime: metadata.mode.runtime(),
        auth_state: EmailAuthorizationState::Unknown,
        network_state: EmailNetworkState::Unknown,
        rate_limit_state: None,
        sync_state: EmailSyncState::Idle,
        pending_messages: None,
        pending_material_bytes: None,
        caveats: metadata
            .caveats
            .iter()
            .map(|caveat| caveat.to_string())
            .collect(),
        actions: email_provider_runtime_actions(metadata.mode)
            .iter()
            .map(|action| action.to_string())
            .collect(),
    };
    serde_json::json!({
        "provider": provider.as_str(),
        "provider_runtime": metadata.mode.runtime().as_str(),
        "account_binding_ref": scope.account_binding_ref,
        "mailbox_scope": scope.mailbox_scope,
        "authorization_state_ref": email_provider_authorization_state_ref(provider),
        "sync_cursor_ref": format!("email.sync_cursor.observed:{}", cursor_kind.as_str()),
        "sync_cursor_kind": cursor_kind.as_str(),
        "runtime_state_ref": metadata.mode.runtime_state_ref(),
        "coverage_ref": metadata.mode.coverage_ref(),
        "debt_ref": metadata.mode.debt_ref(),
        "caveats": metadata.caveats,
        "runtime_observation_contract": runtime_payload,
    })
}

fn email_provider_cursor_metadata_value(
    mode: EmailProviderRuntimeMode,
    scope: &EmailProviderOperationScope,
) -> serde_json::Value {
    let provider = mode.provider();
    let cursor_kind = email_provider_sync_cursor_kind(provider);
    let cursor_payload = EmailSyncCursorObservedPayload {
        provider,
        account_binding_ref: scope.account_binding_ref.clone(),
        mailbox_scope: scope.mailbox_scope.clone(),
        cursor_kind,
        cursor_value: scope.cursor_value_for(provider),
        uidvalidity: scope.uidvalidity.clone(),
        uid: scope.uid.clone(),
        gmail_history_id: scope.gmail_history_id.clone(),
        page_token: scope.page_token.clone(),
        observed_at: Timestamp::now(),
        continuity_state: EmailContinuityState::Unknown,
        caveats: email_provider_cursor_caveats(mode)
            .iter()
            .map(|caveat| caveat.to_string())
            .collect(),
    };
    serde_json::json!({
        "provider": provider.as_str(),
        "account_binding_ref": scope.account_binding_ref,
        "mailbox_scope": scope.mailbox_scope,
        "cursor_kind": cursor_kind.as_str(),
        "cursor_value": scope.cursor_value_for(provider),
        "continuity_state": "unknown",
        "cursor_observation_contract": cursor_payload,
    })
}

fn email_provider_cursor_caveats(mode: EmailProviderRuntimeMode) -> &'static [&'static str] {
    match mode {
        EmailProviderRuntimeMode::GmailScheduledSync => &[
            "Gmail sync executor must advance history id only after material/admission checkpoint succeeds",
        ],
        EmailProviderRuntimeMode::ImapScheduledSync | EmailProviderRuntimeMode::ImapIdleLive => &[
            "IMAP sync executor must treat UIDVALIDITY changes as continuity debt, not cursor reuse",
        ],
    }
}

fn email_provider_runtime_actions(mode: EmailProviderRuntimeMode) -> &'static [&'static str] {
    match mode {
        EmailProviderRuntimeMode::GmailScheduledSync
        | EmailProviderRuntimeMode::ImapScheduledSync => &[
            "email.mailbox.sync",
            "email.mailbox.pause",
            "email.mailbox.inspect",
        ],
        EmailProviderRuntimeMode::ImapIdleLive => &[
            "email.mailbox.pause",
            "email.mailbox.resume",
            "email.mailbox.inspect",
        ],
    }
}

fn media_operation_metadata(
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
            "attached_executor": false
        }
    }))
}

async fn start_projection_rebuild_operation(
    pool: &PgPool,
    actor: &str,
    scope: serde_json::Value,
) -> Result<sinex_db::repositories::OperationRecord> {
    let replay_operation_id = scope
        .get("replay_operation_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SinexError::validation(
                "projection-rebuild scope requires replay_operation_id for replay invalidation recovery",
            )
            .with_operation("ops.start")
        })?;
    let replay_operation_id = uuid::Uuid::parse_str(replay_operation_id).map_err(|error| {
        SinexError::validation("projection-rebuild replay_operation_id must be a UUID")
            .with_std_error(&error)
            .with_operation("ops.start")
            .with_context("replay_operation_id", replay_operation_id.to_string())
    })?;

    pool.state()
        .recover_replay_scope_invalidation(actor, replay_operation_id)
        .await
}

/// Handle GET /ops - list operations with optional filtering
///
/// # Authorization
///
/// Read-only operation. Auth context accepted for audit trail consistency.
pub async fn handle_ops_list(
    pool: &PgPool,
    request: OpsListRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<OpsListResponse> {
    use tracing::debug;

    let limit = if request.limit == default_ops_limit() || request.limit > 0 {
        request.limit
    } else {
        return Err(SinexError::validation(format!(
            "ops.list limit must be positive, got {}",
            request.limit
        )));
    };

    let records = pool
        .state()
        .list_operations(request.operation_type.as_deref(), request.status, limit)
        .await?;

    debug!(
        actor = %auth.actor_id(),
        operation_type = ?request.operation_type,
        status = ?request.status,
        limit,
        "Operations list requested"
    );

    let response = OpsListResponse {
        operations: records.into_iter().map(record_to_operation).collect(),
    };

    Ok(response)
}

/// Handle GET /ops/{id} - get operation details
///
/// # Authorization
///
/// Read-only operation. Auth context accepted for audit trail consistency.
pub async fn handle_ops_get(
    pool: &PgPool,
    request: OpsGetRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<OpsGetResponse> {
    use tracing::debug;

    debug!(
        actor = %auth.actor_id(),
        operation_id = %request.operation_id,
        "Operation get requested"
    );

    let operation_id = request
        .operation_id
        .parse::<Id<DbOperation>>()
        .map_err(|e| SinexError::parse(format!("Invalid operation ID: {e}")))?;

    let record = pool
        .state()
        .get_operation(&operation_id)
        .await?
        .ok_or_else(|| SinexError::not_found(format!("Operation not found: {operation_id}")))?;

    let response = OpsGetResponse {
        operation: record_to_operation(record),
    };

    Ok(response)
}

/// Handle POST /ops/{id}/cancel - cancel a running operation
///
/// # Authorization
///
/// This is a dangerous operation that cancels a running system operation.
/// The auth context is logged for audit purposes.
pub async fn handle_ops_cancel(
    pool: &PgPool,
    request: OpsCancelRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<OpsCancelResponse> {
    use tracing::info;

    let operation_id = request
        .operation_id
        .parse::<Id<DbOperation>>()
        .map_err(|e| SinexError::parse(format!("Invalid operation ID: {e}")))?;

    // Log the reason length and a stable truncated hash for correlation rather
    // than the raw reason text, which may contain sensitive information.
    let reason_len = request.reason.as_deref().map_or(0, str::len);
    let reason_hash = request.reason.as_deref().map(|r| {
        let hash = blake3::hash(r.as_bytes());
        // First 8 bytes (16 hex chars) is sufficient for correlation purposes.
        hash.to_hex()[..16].to_string()
    });
    info!(
        actor = %auth.actor_id(),
        operation_id = %operation_id,
        reason_len = reason_len,
        reason_hash = ?reason_hash,
        "Operation cancel initiated"
    );

    let reason = request
        .reason
        .unwrap_or_else(|| format!("Cancelled by {}", auth.actor_id()));

    let record = pool
        .state()
        .cancel_operation(&operation_id, &reason)
        .await?;

    let response = OpsCancelResponse {
        operation: record_to_operation(record),
        cancelled: true,
    };

    Ok(response)
}
