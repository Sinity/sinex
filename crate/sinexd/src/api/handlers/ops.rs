use camino::Utf8PathBuf;
use futures::StreamExt as _;
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
    EmailNetworkState, EmailProviderKind, EmailProviderRuntime, EmailRateLimitState,
    EmailSyncCursorKind, EmailSyncCursorObservedPayload, EmailSyncState,
};
use sinex_primitives::events::{Event, SourceMaterial};
use sinex_primitives::parser::{
    InputShapeAdapter, MaterialAnchor, MaterialParser, ParserContext, SourceId, SourceRecord,
    maybe_occurrence_key_string,
};
use sinex_primitives::rpc::sources::{SourceMaterialMetadataContract, SourceOrigin};
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use crate::runtime::parser::{
    EmailMboxFileAdapter, EmailMboxFileConfig, GmailApiCursorAdapter, GmailApiCursorConfig,
    GmailHttpClient, ImapSyncAdapter, ImapSyncConfig, ImapSyncMode, NativeImapSyncClient,
    NativeImapSyncClientConfig, NativeImapTlsMode,
};
use crate::sources::source_contracts::email::EmailMailboxParser;

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
const EMAIL_STAGED_SYNC_EXECUTOR_STATE: &str = "staged_email_sync_admitted";
const EMAIL_GMAIL_SYNC_EXECUTOR_STATE: &str = "gmail_api_sync_admitted";
const EMAIL_IMAP_SYNC_EXECUTOR_STATE: &str = "imap_sync_admitted";
const EMAIL_GMAIL_SYNC_FAILED_EXECUTOR_STATE: &str = "gmail_api_sync_failed";
const EMAIL_IMAP_SYNC_FAILED_EXECUTOR_STATE: &str = "imap_sync_failed";
const EMAIL_STAGED_SYNC_DEFAULT_MAX_MESSAGE_BYTES: u64 = 64 * 1024 * 1024;
const EMAIL_GMAIL_SYNC_DEFAULT_PAGE_SIZE: u32 = 100;
const EMAIL_IMAP_SYNC_DEFAULT_BATCH_SIZE: u32 = 100;
const EMAIL_IMAP_SYNC_DEFAULT_IDLE_TIMEOUT_MS: u64 = 30_000;
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

    if spec.surface == "email_capture"
        && spec.operation_type == "email.mailbox.sync"
        && mode_id == EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID
        && let Some(email_result) =
            execute_gmail_provider_sync(pool, &spec, &mode_id, &mut scope, &mut preview_summary)
                .await?
    {
        return pool
            .state()
            .log_operation(DbOperation {
                id: None,
                operation_type: operation_type.to_string(),
                operator: actor.to_string(),
                scope: Some(serde_json::Value::Object(scope)),
                result_status: email_result.status,
                result_message: Some(email_result.message),
                preview_summary: Some(preview_summary),
                duration_ms: email_result.duration_ms,
            })
            .await;
    }

    if spec.surface == "email_capture"
        && spec.operation_type == "email.mailbox.sync"
        && (mode_id == EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID || mode_id == EMAIL_IMAP_IDLE_LIVE_MODE_ID)
        && let Some(email_result) =
            execute_imap_provider_sync(pool, &spec, &mode_id, &mut scope, &mut preview_summary)
                .await?
    {
        return pool
            .state()
            .log_operation(DbOperation {
                id: None,
                operation_type: operation_type.to_string(),
                operator: actor.to_string(),
                scope: Some(serde_json::Value::Object(scope)),
                result_status: email_result.status,
                result_message: Some(email_result.message),
                preview_summary: Some(preview_summary),
                duration_ms: email_result.duration_ms,
            })
            .await;
    }

    if spec.surface == "email_capture"
        && spec.operation_type == "email.mailbox.sync"
        && EMAIL_STAGED_MODE_IDS.contains(&mode_id.as_str())
        && let Some(email_result) =
            execute_staged_email_sync(pool, &spec, &mode_id, &mut scope, &mut preview_summary)
                .await?
    {
        return pool
            .state()
            .log_operation(DbOperation {
                id: None,
                operation_type: operation_type.to_string(),
                operator: actor.to_string(),
                scope: Some(serde_json::Value::Object(scope)),
                result_status: email_result.status,
                result_message: Some(email_result.message),
                preview_summary: Some(preview_summary),
                duration_ms: email_result.duration_ms,
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

struct EmailSyncExecutionResult {
    status: OperationStatus,
    message: String,
    duration_ms: Option<i32>,
}

struct EmailProviderSyncSummary {
    material_id: String,
    event_ids: Vec<String>,
    parsed_record_count: u64,
    provider_cursor: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmailGmailSyncRequest {
    token_file: Utf8PathBuf,
    #[serde(default)]
    api_base_url: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    page_size: Option<u32>,
    #[serde(default)]
    label_ids: Vec<String>,
    #[serde(default)]
    include_spam_trash: bool,
}

#[derive(Debug, Clone)]
struct EmailImapSyncRequest {
    host: String,
    port: u16,
    username: String,
    password_file: Option<Utf8PathBuf>,
    password: Option<String>,
    mailbox: String,
    tls_mode: NativeImapTlsMode,
    batch_size: u32,
    fetch_bodies: bool,
    fetch_attachments: bool,
    body_material_policy_ref: Option<String>,
    attachment_material_policy_ref: Option<String>,
    idle_timeout_ms: u64,
}

struct EmailStagedSyncRequest {
    paths: Vec<Utf8PathBuf>,
    archive_paths: Vec<Utf8PathBuf>,
    folder: Option<String>,
    max_message_bytes: u64,
}

struct EmailStagedSyncSummary {
    material_ids: Vec<String>,
    event_ids: Vec<String>,
    parsed_record_count: usize,
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
        let event = parsed_material_intent_to_event(
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

async fn execute_staged_email_sync(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
) -> Result<Option<EmailSyncExecutionResult>> {
    let Some(request) = EmailStagedSyncRequest::from_scope(mode_id, scope)? else {
        return Ok(None);
    };

    let started = Instant::now();
    scope.insert(
        "staged_sync_input".to_string(),
        request.sanitized_scope_value(),
    );

    let summary = if mode_id == EMAIL_MBOX_STAGED_MODE_ID {
        execute_mbox_staged_email_sync(pool, spec, mode_id, &request).await?
    } else {
        execute_maildir_staged_email_sync(pool, spec, mode_id, &request).await?
    };

    scope.insert(
        "executor_state".to_string(),
        serde_json::json!(EMAIL_STAGED_SYNC_EXECUTOR_STATE),
    );
    scope.insert(
        "staged_sync_material_ids".to_string(),
        serde_json::json!(summary.material_ids),
    );
    scope.insert(
        "staged_sync_event_ids".to_string(),
        serde_json::json!(summary.event_ids),
    );
    scope.insert(
        "staged_sync_parser".to_string(),
        serde_json::json!({
            "parser_id": "email-mailbox-rfc822",
            "parser_version": "1.0.0"
        }),
    );
    scope.insert(
        "staged_sync_record_count".to_string(),
        serde_json::json!(summary.parsed_record_count),
    );

    let preview = preview_summary
        .as_object_mut()
        .expect("package operation preview is an object");
    preview.insert(
        "executor_state".to_string(),
        serde_json::json!(EMAIL_STAGED_SYNC_EXECUTOR_STATE),
    );
    preview.insert(
        "staged_sync_material_count".to_string(),
        serde_json::json!(
            scope["staged_sync_material_ids"]
                .as_array()
                .map_or(0, Vec::len)
        ),
    );
    preview.insert(
        "admitted_event_count".to_string(),
        serde_json::json!(
            scope["staged_sync_event_ids"]
                .as_array()
                .map_or(0, Vec::len)
        ),
    );
    preview.insert(
        "staged_sync_record_count".to_string(),
        serde_json::json!(summary.parsed_record_count),
    );

    Ok(Some(EmailSyncExecutionResult {
        status: OperationStatus::Success,
        message: format!("{}; staged email sync admitted", spec.surface),
        duration_ms: Some(elapsed_millis(started)),
    }))
}

async fn execute_gmail_provider_sync(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
) -> Result<Option<EmailSyncExecutionResult>> {
    let Some(request) = EmailGmailSyncRequest::from_scope(scope)? else {
        return Ok(None);
    };
    let provider_scope = EmailProviderOperationScope::from_scope(
        spec.operation_type,
        EmailProviderRuntimeMode::GmailScheduledSync,
        scope,
    )?;
    scope.insert(
        "gmail_sync_input".to_string(),
        request.sanitized_scope_value(),
    );
    let started = Instant::now();
    let token = match tokio::fs::read_to_string(&request.token_file).await {
        Ok(token) => token,
        Err(error) => {
            return Ok(Some(email_provider_failed_execution(
                scope,
                preview_summary,
                EmailProviderRuntimeMode::GmailScheduledSync,
                &provider_scope,
                EMAIL_GMAIL_SYNC_FAILED_EXECUTOR_STATE,
                format!("Gmail API token file is unavailable: {error}"),
                EmailAuthorizationState::Missing,
                EmailNetworkState::Unknown,
                None,
                started,
            )));
        }
    };
    let token = token.trim().to_string();
    if token.is_empty() {
        return Ok(Some(email_provider_failed_execution(
            scope,
            preview_summary,
            EmailProviderRuntimeMode::GmailScheduledSync,
            &provider_scope,
            EMAIL_GMAIL_SYNC_FAILED_EXECUTOR_STATE,
            "Gmail API token file is empty".to_string(),
            EmailAuthorizationState::Missing,
            EmailNetworkState::Unknown,
            None,
            started,
        )));
    }

    let material_record = register_email_provider_material(
        pool,
        spec,
        mode_id,
        EmailProviderKind::Gmail,
        &provider_scope,
    )
    .await?;
    let client = GmailHttpClient::with_endpoint(
        reqwest::Client::new(),
        request
            .api_base_url
            .unwrap_or_else(|| "https://gmail.googleapis.com/gmail/v1".to_string()),
        request.user_id.unwrap_or_else(|| "me".to_string()),
        token,
    );
    let config = GmailApiCursorConfig {
        account_binding_ref: provider_scope.account_binding_ref.clone(),
        mailbox_scope: provider_scope.mailbox_scope.clone(),
        initial_page_token: provider_scope.page_token.clone(),
        initial_history_id: provider_scope.gmail_history_id.clone(),
        page_size: request
            .page_size
            .unwrap_or(EMAIL_GMAIL_SYNC_DEFAULT_PAGE_SIZE)
            .max(1),
        label_ids: request.label_ids,
        include_spam_trash: request.include_spam_trash,
    };
    let summary = match admit_gmail_adapter_records(pool, &material_record, client, config).await {
        Ok(summary) => summary,
        Err(error) => {
            let reason = error.to_string();
            let (auth_state, network_state, rate_limit_state) =
                classify_gmail_provider_failure(&reason);
            return Ok(Some(email_provider_failed_execution(
                scope,
                preview_summary,
                EmailProviderRuntimeMode::GmailScheduledSync,
                &provider_scope,
                EMAIL_GMAIL_SYNC_FAILED_EXECUTOR_STATE,
                reason,
                auth_state,
                network_state,
                rate_limit_state,
                started,
            )));
        }
    };
    let provider_cursor = summary.provider_cursor.clone().map(|cursor| {
        email_provider_cursor_payload_metadata_value(
            EmailProviderRuntimeMode::GmailScheduledSync,
            cursor,
        )
    });
    let runtime = email_provider_executed_runtime_value(
        EmailProviderRuntimeMode::GmailScheduledSync,
        &provider_scope,
    );

    scope.insert(
        "executor_state".to_string(),
        serde_json::json!(EMAIL_GMAIL_SYNC_EXECUTOR_STATE),
    );
    scope.insert(
        "provider_material_id".to_string(),
        serde_json::json!(summary.material_id),
    );
    scope.insert(
        "provider_event_ids".to_string(),
        serde_json::json!(summary.event_ids),
    );
    scope.insert(
        "provider_record_count".to_string(),
        serde_json::json!(summary.parsed_record_count),
    );
    scope.insert("provider_runtime".to_string(), runtime.clone());
    if let Some(cursor) = provider_cursor.clone() {
        scope.insert("provider_cursor".to_string(), cursor);
    }

    let preview = preview_summary
        .as_object_mut()
        .expect("package operation preview is an object");
    preview.insert(
        "executor_state".to_string(),
        serde_json::json!(EMAIL_GMAIL_SYNC_EXECUTOR_STATE),
    );
    preview.insert(
        "provider_material_id".to_string(),
        serde_json::json!(scope["provider_material_id"]),
    );
    preview.insert(
        "provider_record_count".to_string(),
        serde_json::json!(summary.parsed_record_count),
    );
    preview.insert(
        "admitted_event_count".to_string(),
        serde_json::json!(scope["provider_event_ids"].as_array().map_or(0, Vec::len)),
    );
    preview.insert("provider_runtime".to_string(), runtime);
    if let Some(cursor) = provider_cursor {
        preview.insert("provider_cursor".to_string(), cursor);
    }

    Ok(Some(EmailSyncExecutionResult {
        status: OperationStatus::Success,
        message: format!("{}; Gmail API sync admitted", spec.surface),
        duration_ms: Some(elapsed_millis(started)),
    }))
}

async fn execute_imap_provider_sync(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
) -> Result<Option<EmailSyncExecutionResult>> {
    let Some(request) = EmailImapSyncRequest::from_scope(scope)? else {
        return Ok(None);
    };
    let mode = EmailProviderRuntimeMode::from_mode_id(mode_id).ok_or_else(|| {
        SinexError::validation("IMAP provider sync received unsupported mode")
            .with_context("mode_id", mode_id)
            .with_operation("ops.start")
    })?;
    let provider_scope = EmailProviderOperationScope::from_scope(spec.operation_type, mode, scope)?;
    scope.insert(
        "imap_sync_input".to_string(),
        request.sanitized_scope_value(),
    );
    remove_imap_secret_scope_keys(scope);
    let started = Instant::now();
    let password = match request.read_password().await {
        Ok(password) => password,
        Err(error) => {
            return Ok(Some(email_provider_failed_execution(
                scope,
                preview_summary,
                mode,
                &provider_scope,
                EMAIL_IMAP_SYNC_FAILED_EXECUTOR_STATE,
                format!("IMAP credential read failed: {error}"),
                EmailAuthorizationState::Missing,
                EmailNetworkState::Unknown,
                None,
                started,
            )));
        }
    };

    let material_record =
        register_email_provider_material(pool, spec, mode_id, mode.provider(), &provider_scope)
            .await?;
    let client = NativeImapSyncClient::new(NativeImapSyncClientConfig {
        host: request.host.clone(),
        port: request.port,
        username: request.username.clone(),
        password,
        mailbox: request.mailbox.clone(),
        tls_mode: request.tls_mode,
        idle_timeout_ms: request.idle_timeout_ms,
    });
    let config = ImapSyncConfig {
        account_binding_ref: provider_scope.account_binding_ref.clone(),
        mailbox: request.mailbox,
        mode: match mode {
            EmailProviderRuntimeMode::ImapScheduledSync => ImapSyncMode::Scheduled,
            EmailProviderRuntimeMode::ImapIdleLive => ImapSyncMode::Idle,
            EmailProviderRuntimeMode::GmailScheduledSync => {
                return Err(
                    SinexError::validation("Gmail mode cannot use IMAP executor")
                        .with_operation("ops.start"),
                );
            }
        },
        initial_uid_next: provider_scope
            .uid
            .as_deref()
            .map(str::parse::<u32>)
            .transpose()
            .map_err(|error| {
                SinexError::validation("IMAP uid cursor must fit in u32")
                    .with_std_error(&error)
                    .with_operation("ops.start")
            })?,
        initial_uid_validity: provider_scope
            .uidvalidity
            .as_deref()
            .map(str::parse::<u32>)
            .transpose()
            .map_err(|error| {
                SinexError::validation("IMAP uidvalidity cursor must fit in u32")
                    .with_std_error(&error)
                    .with_operation("ops.start")
            })?,
        initial_highest_modseq: scope
            .get("highest_modseq")
            .and_then(serde_json::Value::as_str)
            .map(str::parse::<u64>)
            .transpose()
            .map_err(|error| {
                SinexError::validation("IMAP highest_modseq cursor must fit in u64")
                    .with_std_error(&error)
                    .with_operation("ops.start")
            })?,
        batch_size: request.batch_size,
        fetch_bodies: request.fetch_bodies,
        fetch_attachments: request.fetch_attachments,
        body_material_policy_ref: request.body_material_policy_ref.clone(),
        attachment_material_policy_ref: request.attachment_material_policy_ref.clone(),
    };
    let summary = match admit_imap_adapter_records(pool, &material_record, client, config).await {
        Ok(summary) => summary,
        Err(error) => {
            let reason = error.to_string();
            let (auth_state, network_state) = classify_imap_provider_failure(&reason);
            return Ok(Some(email_provider_failed_execution(
                scope,
                preview_summary,
                mode,
                &provider_scope,
                EMAIL_IMAP_SYNC_FAILED_EXECUTOR_STATE,
                reason,
                auth_state,
                network_state,
                None,
                started,
            )));
        }
    };
    let provider_cursor = summary
        .provider_cursor
        .clone()
        .map(|cursor| email_provider_cursor_payload_metadata_value(mode, cursor));
    let runtime = email_provider_executed_runtime_value(mode, &provider_scope);

    scope.insert(
        "executor_state".to_string(),
        serde_json::json!(EMAIL_IMAP_SYNC_EXECUTOR_STATE),
    );
    scope.insert(
        "provider_material_id".to_string(),
        serde_json::json!(summary.material_id),
    );
    scope.insert(
        "provider_event_ids".to_string(),
        serde_json::json!(summary.event_ids),
    );
    scope.insert(
        "provider_record_count".to_string(),
        serde_json::json!(summary.parsed_record_count),
    );
    scope.insert("provider_runtime".to_string(), runtime.clone());
    if let Some(cursor) = provider_cursor.clone() {
        scope.insert("provider_cursor".to_string(), cursor);
    }

    let preview = preview_summary
        .as_object_mut()
        .expect("package operation preview is an object");
    preview.insert(
        "executor_state".to_string(),
        serde_json::json!(EMAIL_IMAP_SYNC_EXECUTOR_STATE),
    );
    preview.insert(
        "provider_material_id".to_string(),
        serde_json::json!(scope["provider_material_id"]),
    );
    preview.insert(
        "provider_record_count".to_string(),
        serde_json::json!(summary.parsed_record_count),
    );
    preview.insert(
        "admitted_event_count".to_string(),
        serde_json::json!(scope["provider_event_ids"].as_array().map_or(0, Vec::len)),
    );
    preview.insert("provider_runtime".to_string(), runtime);
    if let Some(cursor) = provider_cursor {
        preview.insert("provider_cursor".to_string(), cursor);
    }

    Ok(Some(EmailSyncExecutionResult {
        status: OperationStatus::Success,
        message: format!("{}; IMAP sync admitted", spec.surface),
        duration_ms: Some(elapsed_millis(started)),
    }))
}

impl EmailGmailSyncRequest {
    fn from_scope(scope: &serde_json::Map<String, serde_json::Value>) -> Result<Option<Self>> {
        let Some(token_file) = optional_scope_string(scope, "gmail_token_file")
            .or_else(|| optional_scope_string(scope, "access_token_file"))
            .or_else(|| optional_scope_string(scope, "token_file"))
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            token_file: Utf8PathBuf::from(token_file),
            api_base_url: optional_scope_string(scope, "gmail_api_base_url")
                .or_else(|| optional_scope_string(scope, "api_base_url")),
            user_id: optional_scope_string(scope, "gmail_user_id")
                .or_else(|| optional_scope_string(scope, "user_id")),
            page_size: scope
                .get("page_size")
                .and_then(serde_json::Value::as_u64)
                .map(u32::try_from)
                .transpose()
                .map_err(|error| {
                    SinexError::validation("Gmail page_size must fit in u32")
                        .with_std_error(&error)
                        .with_operation("ops.start")
                })?,
            label_ids: scope_string_list(scope, &["label_id", "label_ids"])?,
            include_spam_trash: scope
                .get("include_spam_trash")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        }))
    }

    fn sanitized_scope_value(&self) -> serde_json::Value {
        serde_json::json!({
            "token_file_ref": self.token_file.to_string(),
            "api_base_url": self.api_base_url,
            "user_id": self.user_id,
            "page_size": self.page_size.unwrap_or(EMAIL_GMAIL_SYNC_DEFAULT_PAGE_SIZE),
            "label_ids": self.label_ids.clone(),
            "include_spam_trash": self.include_spam_trash,
        })
    }
}

impl EmailImapSyncRequest {
    fn from_scope(scope: &serde_json::Map<String, serde_json::Value>) -> Result<Option<Self>> {
        let Some(host) = optional_scope_string(scope, "imap_host")
            .or_else(|| optional_scope_string(scope, "host"))
        else {
            return Ok(None);
        };
        let Some(username) = optional_scope_string(scope, "imap_username")
            .or_else(|| optional_scope_string(scope, "username"))
        else {
            return Ok(None);
        };
        let password_file = optional_scope_string(scope, "imap_password_file")
            .or_else(|| optional_scope_string(scope, "password_file"))
            .map(Utf8PathBuf::from);
        let password = optional_scope_string(scope, "imap_password")
            .or_else(|| optional_scope_string(scope, "password"));
        if password_file.is_none() && password.is_none() {
            return Ok(None);
        }

        let tls_mode = match optional_scope_string(scope, "imap_tls_mode")
            .or_else(|| optional_scope_string(scope, "tls_mode"))
            .as_deref()
        {
            Some(value) => NativeImapTlsMode::from_scope_value(value).ok_or_else(|| {
                SinexError::validation("unsupported IMAP TLS mode")
                    .with_context("tls_mode", value)
                    .with_operation("ops.start")
            })?,
            None => NativeImapTlsMode::Implicit,
        };
        let fetch_bodies = scope
            .get("fetch_bodies")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let fetch_attachments = scope
            .get("fetch_attachments")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let body_material_policy_ref = optional_scope_string(scope, "body_material_policy_ref")
            .or_else(|| optional_scope_string(scope, "raw_body_material_policy_ref"));
        let attachment_material_policy_ref =
            optional_scope_string(scope, "attachment_material_policy_ref");
        if fetch_bodies && body_material_policy_ref.is_none() {
            return Err(SinexError::validation(
                "IMAP fetch_bodies requires body_material_policy_ref",
            )
            .with_operation("ops.start"));
        }
        if fetch_attachments && !fetch_bodies {
            return Err(
                SinexError::validation("IMAP fetch_attachments requires fetch_bodies")
                    .with_operation("ops.start"),
            );
        }
        if fetch_attachments && attachment_material_policy_ref.is_none() {
            return Err(SinexError::validation(
                "IMAP fetch_attachments requires attachment_material_policy_ref",
            )
            .with_operation("ops.start"));
        }

        Ok(Some(Self {
            host,
            port: scope
                .get("imap_port")
                .or_else(|| scope.get("port"))
                .and_then(serde_json::Value::as_u64)
                .map(u16::try_from)
                .transpose()
                .map_err(|error| {
                    SinexError::validation("IMAP port must fit in u16")
                        .with_std_error(&error)
                        .with_operation("ops.start")
                })?
                .unwrap_or(match tls_mode {
                    NativeImapTlsMode::Implicit => 993,
                    NativeImapTlsMode::None => 143,
                }),
            username,
            password_file,
            password,
            mailbox: optional_scope_string(scope, "mailbox")
                .or_else(|| optional_scope_string(scope, "mailbox_scope"))
                .unwrap_or_else(|| "INBOX".to_string()),
            tls_mode,
            batch_size: scope
                .get("batch_size")
                .or_else(|| scope.get("page_size"))
                .and_then(serde_json::Value::as_u64)
                .map(u32::try_from)
                .transpose()
                .map_err(|error| {
                    SinexError::validation("IMAP batch_size must fit in u32")
                        .with_std_error(&error)
                        .with_operation("ops.start")
                })?
                .unwrap_or(EMAIL_IMAP_SYNC_DEFAULT_BATCH_SIZE)
                .max(1),
            fetch_bodies,
            fetch_attachments,
            body_material_policy_ref,
            attachment_material_policy_ref,
            idle_timeout_ms: scope
                .get("idle_timeout_ms")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(EMAIL_IMAP_SYNC_DEFAULT_IDLE_TIMEOUT_MS),
        }))
    }

    async fn read_password(&self) -> Result<String> {
        let password = if let Some(password_file) = &self.password_file {
            tokio::fs::read_to_string(password_file)
                .await
                .map_err(|error| {
                    SinexError::io("Failed to read IMAP password file")
                        .with_context("password_file", password_file.to_string())
                        .with_std_error(&error)
                        .with_operation("ops.start")
                })?
        } else {
            self.password.clone().unwrap_or_default()
        };
        let password = password.trim().to_string();
        if password.is_empty() {
            return Err(
                SinexError::validation("IMAP password is empty").with_operation("ops.start")
            );
        }
        Ok(password)
    }

    fn sanitized_scope_value(&self) -> serde_json::Value {
        serde_json::json!({
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password_file_ref": self.password_file.as_ref().map(ToString::to_string),
            "password": self.password.as_ref().map(|_| "<redacted>"),
            "mailbox": self.mailbox,
            "tls_mode": self.tls_mode.as_scope_value(),
            "batch_size": self.batch_size,
            "fetch_bodies": self.fetch_bodies,
            "fetch_attachments": self.fetch_attachments,
            "body_material_policy_ref": self.body_material_policy_ref,
            "attachment_material_policy_ref": self.attachment_material_policy_ref,
            "idle_timeout_ms": self.idle_timeout_ms,
        })
    }
}

fn remove_imap_secret_scope_keys(scope: &mut serde_json::Map<String, serde_json::Value>) {
    scope.remove("imap_password");
    scope.remove("password");
}

impl EmailStagedSyncRequest {
    fn from_scope(
        mode_id: &str,
        scope: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<Self>> {
        let paths = staged_path_list(scope, &["path", "input_path", "paths", "input_paths"])?;
        let archive_paths =
            staged_path_list(scope, &["archive_path", "archive_paths", "takeout_path"])?;
        if paths.is_empty() && archive_paths.is_empty() {
            return Ok(None);
        }
        if mode_id == EMAIL_MAILDIR_STAGED_MODE_ID && !archive_paths.is_empty() {
            return Err(SinexError::validation(
                "maildir staged email sync accepts path/input_path/paths only; use mbox-staged for MBOX or Takeout archives",
            )
            .with_operation("ops.start"));
        }

        Ok(Some(Self {
            paths,
            archive_paths,
            folder: optional_scope_string(scope, "folder"),
            max_message_bytes: scope
                .get("max_message_bytes")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(EMAIL_STAGED_SYNC_DEFAULT_MAX_MESSAGE_BYTES),
        }))
    }

    fn sanitized_scope_value(&self) -> serde_json::Value {
        serde_json::json!({
            "paths": self.paths.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "archive_paths": self.archive_paths.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "folder": self.folder,
            "max_message_bytes": self.max_message_bytes,
        })
    }
}

fn staged_path_list(
    scope: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Result<Vec<Utf8PathBuf>> {
    let mut paths = Vec::new();
    for key in keys {
        let Some(value) = scope.get(*key) else {
            continue;
        };
        match value {
            serde_json::Value::String(path) => {
                paths.push(Utf8PathBuf::from(path));
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    let path = value.as_str().ok_or_else(|| {
                        SinexError::validation(format!("{key} entries must be strings"))
                            .with_operation("ops.start")
                    })?;
                    paths.push(Utf8PathBuf::from(path));
                }
            }
            _ => {
                return Err(SinexError::validation(format!(
                    "{key} must be a string or array of strings"
                ))
                .with_operation("ops.start"));
            }
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn scope_string_list(
    scope: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Result<Vec<String>> {
    let mut values = Vec::new();
    for key in keys {
        let Some(value) = scope.get(*key) else {
            continue;
        };
        match value {
            serde_json::Value::String(text) => values.push(text.to_string()),
            serde_json::Value::Array(entries) => {
                for entry in entries {
                    let text = entry.as_str().ok_or_else(|| {
                        SinexError::validation(format!("{key} entries must be strings"))
                            .with_operation("ops.start")
                    })?;
                    values.push(text.to_string());
                }
            }
            _ => {
                return Err(SinexError::validation(format!(
                    "{key} must be a string or array of strings"
                ))
                .with_operation("ops.start"));
            }
        }
    }
    values.retain(|value| !value.trim().is_empty());
    values.sort();
    values.dedup();
    Ok(values)
}

async fn register_email_provider_material(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    provider: EmailProviderKind,
    provider_scope: &EmailProviderOperationScope,
) -> Result<sinex_db::SourceMaterialRecord> {
    let sync_run_id = uuid::Uuid::now_v7().to_string();
    let source_identifier = format!(
        "provider://{}/{}/{}?sync_run={}",
        provider.as_str(),
        provider_scope.account_binding_ref,
        mode_id.trim_start_matches("source:"),
        sync_run_id
    );
    let mut contract = SourceMaterialMetadataContract::new(
        SourceMaterialFormat::Json,
        SourceMaterialTimingInfoType::StagedAt,
    );
    contract.origin = Some(SourceOrigin {
        source_uri: Some(source_identifier.clone()),
        binding_id: Some(mode_id.to_string()),
        ..SourceOrigin::default()
    });
    let material = sinex_db::repositories::SourceMaterial::blob_text(&source_identifier)
        .with_metadata_contract(&contract)
        .with_metadata(serde_json::json!({
            "email_provider_sync": {
                "source_id": spec.source_id,
                "mode_id": mode_id,
                "operation_type": spec.operation_type,
                "action": spec.action,
                "provider": provider.as_str(),
                "account_binding_ref": provider_scope.account_binding_ref.clone(),
                "mailbox_scope": provider_scope.mailbox_scope.clone(),
                "sync_run_id": sync_run_id,
            }
        }));
    pool.source_materials().register_material(material).await
}

async fn admit_gmail_adapter_records(
    pool: &PgPool,
    material_record: &sinex_db::SourceMaterialRecord,
    client: GmailHttpClient,
    config: GmailApiCursorConfig,
) -> Result<EmailProviderSyncSummary> {
    let material_id = Id::<SourceMaterial>::from_uuid(material_record.id);
    let adapter = GmailApiCursorAdapter::new(client);
    let mut stream = adapter
        .open(material_id, &config, None)
        .await
        .map_err(|error| {
            SinexError::parse("Gmail API adapter failed to open")
                .with_context("material_id", material_record.id.to_string())
                .with_context("parse_error", error.to_string())
                .with_operation("ops.start")
        })?;
    let mut parser = EmailMailboxParser;
    let mut summary = EmailProviderSyncSummary {
        material_id: material_record.id.to_string(),
        event_ids: Vec::new(),
        parsed_record_count: 0,
        provider_cursor: None,
    };
    while let Some(record) = stream.next().await {
        let record = record.map_err(|error| {
            SinexError::parse("Gmail API adapter failed to read record")
                .with_context("material_id", material_record.id.to_string())
                .with_context("parse_error", error.to_string())
                .with_operation("ops.start")
        })?;
        summary.parsed_record_count += 1;
        if let Some(cursor) =
            admit_email_provider_record(pool, &mut parser, record, material_id, &mut summary)
                .await?
        {
            summary.provider_cursor = Some(cursor);
        }
    }
    Ok(summary)
}

async fn admit_imap_adapter_records(
    pool: &PgPool,
    material_record: &sinex_db::SourceMaterialRecord,
    client: NativeImapSyncClient,
    config: ImapSyncConfig,
) -> Result<EmailProviderSyncSummary> {
    let material_id = Id::<SourceMaterial>::from_uuid(material_record.id);
    let adapter = ImapSyncAdapter::new(client);
    let mut stream = adapter
        .open(material_id, &config, None)
        .await
        .map_err(|error| {
            SinexError::parse("IMAP adapter failed to open")
                .with_context("material_id", material_record.id.to_string())
                .with_context("parse_error", error.to_string())
                .with_operation("ops.start")
        })?;
    let mut parser = EmailMailboxParser;
    let mut summary = EmailProviderSyncSummary {
        material_id: material_record.id.to_string(),
        event_ids: Vec::new(),
        parsed_record_count: 0,
        provider_cursor: None,
    };
    while let Some(record) = stream.next().await {
        let record = record.map_err(|error| {
            SinexError::parse("IMAP adapter failed to read record")
                .with_context("material_id", material_record.id.to_string())
                .with_context("parse_error", error.to_string())
                .with_operation("ops.start")
        })?;
        summary.parsed_record_count += 1;
        if let Some(cursor) =
            admit_email_provider_record(pool, &mut parser, record, material_id, &mut summary)
                .await?
        {
            summary.provider_cursor = Some(cursor);
        }
    }
    Ok(summary)
}

async fn execute_mbox_staged_email_sync(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    request: &EmailStagedSyncRequest,
) -> Result<EmailStagedSyncSummary> {
    let mut summary = EmailStagedSyncSummary {
        material_ids: Vec::new(),
        event_ids: Vec::new(),
        parsed_record_count: 0,
    };
    for path in &request.paths {
        let material_record = register_email_staged_material(
            pool,
            spec,
            mode_id,
            path,
            SourceMaterialFormat::Text,
            serde_json::json!({ "email_staged_sync": { "input_kind": "mbox-file" } }),
        )
        .await?;
        summary.material_ids.push(material_record.id.to_string());
        let config = EmailMboxFileConfig {
            paths: vec![path.clone()],
            archive_paths: Vec::new(),
            folder: request.folder.clone(),
            max_message_bytes: request.max_message_bytes,
        };
        admit_mbox_adapter_records(pool, &material_record, config, &mut summary).await?;
    }
    for archive_path in &request.archive_paths {
        let material_record = register_email_staged_material(
            pool,
            spec,
            mode_id,
            archive_path,
            SourceMaterialFormat::Archive,
            serde_json::json!({ "email_staged_sync": { "input_kind": "takeout-archive" } }),
        )
        .await?;
        summary.material_ids.push(material_record.id.to_string());
        let config = EmailMboxFileConfig {
            paths: Vec::new(),
            archive_paths: vec![archive_path.clone()],
            folder: request.folder.clone(),
            max_message_bytes: request.max_message_bytes,
        };
        admit_mbox_adapter_records(pool, &material_record, config, &mut summary).await?;
    }
    Ok(summary)
}

async fn admit_mbox_adapter_records(
    pool: &PgPool,
    material_record: &sinex_db::SourceMaterialRecord,
    config: EmailMboxFileConfig,
    summary: &mut EmailStagedSyncSummary,
) -> Result<()> {
    let adapter = EmailMboxFileAdapter;
    let material_id = Id::<SourceMaterial>::from_uuid(material_record.id);
    let mut stream = adapter
        .open(material_id, &config, None)
        .await
        .map_err(|error| {
            SinexError::parse("email MBOX adapter failed to open")
                .with_context("material_id", material_record.id.to_string())
                .with_context("parse_error", error.to_string())
                .with_operation("ops.start")
        })?;
    let mut parser = EmailMailboxParser;
    while let Some(record) = stream.next().await {
        let record = record.map_err(|error| {
            SinexError::parse("email MBOX adapter failed to read record")
                .with_context("material_id", material_record.id.to_string())
                .with_context("parse_error", error.to_string())
                .with_operation("ops.start")
        })?;
        summary.parsed_record_count += 1;
        admit_email_record(pool, &mut parser, record, material_id, summary).await?;
    }
    Ok(())
}

async fn execute_maildir_staged_email_sync(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    request: &EmailStagedSyncRequest,
) -> Result<EmailStagedSyncSummary> {
    let files = collect_maildir_input_files(&request.paths)?;
    let mut summary = EmailStagedSyncSummary {
        material_ids: Vec::new(),
        event_ids: Vec::new(),
        parsed_record_count: 0,
    };
    let mut parser = EmailMailboxParser;
    for path in files {
        let bytes = tokio::fs::read(&path).await.map_err(|error| {
            SinexError::io("Failed to read staged email file")
                .with_context("path", path.to_string())
                .with_std_error(&error)
                .with_operation("ops.start")
        })?;
        let material_record = register_email_staged_material(
            pool,
            spec,
            mode_id,
            &path,
            SourceMaterialFormat::Text,
            serde_json::json!({ "email_staged_sync": { "input_kind": "rfc822-file" } }),
        )
        .await?;
        update_material_total_bytes(pool, material_record.id, bytes.len()).await?;
        summary.material_ids.push(material_record.id.to_string());
        let material_id = Id::<SourceMaterial>::from_uuid(material_record.id);
        let record = SourceRecord {
            material_id,
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: bytes.len() as u64,
            },
            bytes,
            logical_path: Some(path),
            source_ts_hint: None,
            metadata: request
                .folder
                .as_ref()
                .map(|folder| serde_json::json!({ "folder": folder }))
                .unwrap_or(serde_json::Value::Null),
        };
        summary.parsed_record_count += 1;
        admit_email_record(pool, &mut parser, record, material_id, &mut summary).await?;
    }
    Ok(summary)
}

fn collect_maildir_input_files(paths: &[Utf8PathBuf]) -> Result<Vec<Utf8PathBuf>> {
    let mut files = Vec::new();
    for path in paths {
        if path.is_file() {
            files.push(path.clone());
            continue;
        }
        if !path.is_dir() {
            return Err(
                SinexError::validation("staged email input path does not exist")
                    .with_context("path", path.to_string())
                    .with_operation("ops.start"),
            );
        }
        collect_maildir_files_from_dir(path, &mut files)?;
    }
    files.sort();
    files.dedup();
    if files.is_empty() {
        return Err(SinexError::validation(
            "staged email sync found no RFC822/Maildir message files",
        )
        .with_operation("ops.start"));
    }
    Ok(files)
}

fn collect_maildir_files_from_dir(path: &Utf8PathBuf, files: &mut Vec<Utf8PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(path).map_err(|error| {
        SinexError::io("Failed to read staged email directory")
            .with_context("path", path.to_string())
            .with_std_error(&error)
            .with_operation("ops.start")
    })? {
        let entry = entry.map_err(|error| {
            SinexError::io("Failed to read staged email directory entry")
                .with_context("path", path.to_string())
                .with_std_error(&error)
                .with_operation("ops.start")
        })?;
        let entry_path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|_| {
            SinexError::validation("staged email path is not valid UTF-8")
                .with_context("path", entry.path().display().to_string())
                .with_operation("ops.start")
        })?;
        let file_type = entry.file_type().map_err(|error| {
            SinexError::io("Failed to inspect staged email directory entry")
                .with_context("path", entry_path.to_string())
                .with_std_error(&error)
                .with_operation("ops.start")
        })?;
        if file_type.is_dir() {
            collect_maildir_files_from_dir(&entry_path, files)?;
        } else if file_type.is_file() && maildir_entry_path(&entry_path) {
            files.push(entry_path);
        }
    }
    Ok(())
}

fn maildir_entry_path(path: &Utf8PathBuf) -> bool {
    path.components()
        .any(|component| matches!(component.as_str(), "cur" | "new"))
}

async fn register_email_staged_material(
    pool: &PgPool,
    spec: &PackageOperationSpec,
    mode_id: &str,
    path: &Utf8PathBuf,
    format: SourceMaterialFormat,
    metadata: serde_json::Value,
) -> Result<sinex_db::SourceMaterialRecord> {
    let mut contract =
        SourceMaterialMetadataContract::new(format, SourceMaterialTimingInfoType::StagedAt);
    contract.origin = Some(SourceOrigin {
        source_uri: Some(path.to_string()),
        binding_id: Some(mode_id.to_string()),
        ..SourceOrigin::default()
    });
    let material = sinex_db::repositories::SourceMaterial::file(path.to_string())
        .with_metadata_contract(&contract)
        .with_metadata(serde_json::json!({
            "email_staged_sync": {
                "source_id": spec.source_id,
                "mode_id": mode_id,
                "operation_type": spec.operation_type,
                "action": spec.action,
            }
        }))
        .with_metadata(metadata);
    let material_record = pool.source_materials().register_material(material).await?;
    if let Ok(metadata) = std::fs::metadata(path) {
        if metadata.is_file() {
            update_material_total_bytes(pool, material_record.id, metadata.len() as usize).await?;
        }
    }
    Ok(material_record)
}

async fn update_material_total_bytes(
    pool: &PgPool,
    material_id: uuid::Uuid,
    byte_len: usize,
) -> Result<()> {
    let total_bytes = i64::try_from(byte_len).map_err(|error| {
        SinexError::validation("email staged material is too large to record")
            .with_std_error(&error)
            .with_operation("ops.start")
    })?;
    sqlx::query!(
        "UPDATE raw.source_material_registry SET total_bytes = $1 WHERE id = $2",
        total_bytes,
        material_id
    )
    .execute(pool)
    .await
    .map_err(|error| {
        SinexError::database("Failed to persist staged email material size")
            .with_context("material_id", material_id.to_string())
            .with_std_error(&error)
    })?;
    Ok(())
}

async fn admit_email_record(
    pool: &PgPool,
    parser: &mut EmailMailboxParser,
    record: SourceRecord,
    material_id: Id<SourceMaterial>,
    summary: &mut EmailStagedSyncSummary,
) -> Result<()> {
    let ctx = ParserContext {
        source_id: SourceId::from_static("email.mailbox"),
        source_material_id: material_id,
        record_anchor: record.anchor.clone(),
        operation_id: uuid::Uuid::now_v7(),
        job_id: uuid::Uuid::now_v7(),
        host: std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("HOST"))
            .unwrap_or_else(|_| "unknown-host".to_string()),
        acquisition_time: Timestamp::now(),
    };
    let intents = parser.parse_record(record, &ctx).await.map_err(|error| {
        SinexError::parse("email mailbox parser failed")
            .with_context("source_id", "email.mailbox")
            .with_context("parse_error", error.to_string())
            .with_operation("ops.start")
    })?;
    for intent in intents {
        let event = parsed_material_intent_to_event(intent, material_id)?;
        let persisted = pool.events().insert(event).await?;
        if let Some(id) = persisted.id {
            summary.event_ids.push(id.to_string());
        }
    }
    Ok(())
}

async fn admit_email_provider_record(
    pool: &PgPool,
    parser: &mut EmailMailboxParser,
    record: SourceRecord,
    material_id: Id<SourceMaterial>,
    summary: &mut EmailProviderSyncSummary,
) -> Result<Option<serde_json::Value>> {
    let ctx = ParserContext {
        source_id: SourceId::from_static("email.mailbox"),
        source_material_id: material_id,
        record_anchor: record.anchor.clone(),
        operation_id: uuid::Uuid::now_v7(),
        job_id: uuid::Uuid::now_v7(),
        host: std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("HOST"))
            .unwrap_or_else(|_| "unknown-host".to_string()),
        acquisition_time: Timestamp::now(),
    };
    let intents = parser.parse_record(record, &ctx).await.map_err(|error| {
        SinexError::parse("email provider parser failed")
            .with_context("source_id", "email.mailbox")
            .with_context("parse_error", error.to_string())
            .with_operation("ops.start")
    })?;
    let mut last_cursor = None;
    for intent in intents {
        if intent.event_type.as_str() == "email.sync_cursor.observed" {
            last_cursor = Some(intent.payload.clone());
        }
        let event = parsed_material_intent_to_event(intent, material_id)?;
        let persisted = pool.events().insert(event).await?;
        if let Some(id) = persisted.id {
            summary.event_ids.push(id.to_string());
        }
    }
    Ok(last_cursor)
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

fn parsed_material_intent_to_event(
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
                "provider executor requires explicit gmail_token_file at operation start",
                "OAuth refresh remains operator/runtime-owned outside this operation",
                "provider cursor is unknown until an executable sync admits records",
            ],
        }),
        EmailProviderRuntimeMode::ImapScheduledSync => Some(EmailProviderModeMetadata {
            mode,
            caveats: &[
                "provider executor requires explicit IMAP credentials at operation start",
                "credential refresh remains operator/runtime-owned outside this operation",
                "provider cursor is unknown until an executable sync admits records",
            ],
        }),
        EmailProviderRuntimeMode::ImapIdleLive => Some(EmailProviderModeMetadata {
            mode,
            caveats: &[
                "IMAP IDLE is executable as a bounded operation and not a daemon supervisor",
                "credential refresh remains operator/runtime-owned outside this operation",
                "daemon reconnect/backoff state remains runtime-owned outside ops.start",
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

fn email_provider_cursor_payload_metadata_value(
    mode: EmailProviderRuntimeMode,
    cursor_payload: serde_json::Value,
) -> serde_json::Value {
    let provider = mode.provider();
    let fallback_cursor_kind = email_provider_sync_cursor_kind(provider);
    let cursor_kind = cursor_payload
        .get("cursor_kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| fallback_cursor_kind.as_str());
    let account_binding_ref = cursor_payload
        .get("account_binding_ref")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let mailbox_scope = cursor_payload
        .get("mailbox_scope")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let cursor_value = cursor_payload
        .get("cursor_value")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let continuity_state = cursor_payload
        .get("continuity_state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("current");

    serde_json::json!({
        "provider": provider.as_str(),
        "account_binding_ref": account_binding_ref,
        "mailbox_scope": mailbox_scope,
        "cursor_kind": cursor_kind,
        "cursor_value": cursor_value,
        "continuity_state": continuity_state,
        "cursor_observation_contract": cursor_payload,
    })
}

fn email_provider_executed_runtime_value(
    mode: EmailProviderRuntimeMode,
    scope: &EmailProviderOperationScope,
) -> serde_json::Value {
    let provider = mode.provider();
    let cursor_kind = email_provider_sync_cursor_kind(provider);
    let runtime_payload = EmailCaptureRuntimeObservedPayload {
        provider,
        account_binding_ref: scope.account_binding_ref.clone(),
        mode_id: mode.mode_id().to_string(),
        observed_at: Timestamp::now(),
        provider_runtime: mode.runtime(),
        auth_state: EmailAuthorizationState::Authorized,
        network_state: EmailNetworkState::Online,
        rate_limit_state: None,
        sync_state: EmailSyncState::Idle,
        pending_messages: None,
        pending_material_bytes: None,
        caveats: email_provider_executed_runtime_caveats(mode)
            .iter()
            .map(|caveat| (*caveat).to_string())
            .collect(),
        actions: email_provider_runtime_actions(mode)
            .iter()
            .map(|action| action.to_string())
            .collect(),
    };
    serde_json::json!({
        "provider": provider.as_str(),
        "provider_runtime": mode.runtime().as_str(),
        "account_binding_ref": scope.account_binding_ref,
        "mailbox_scope": scope.mailbox_scope,
        "authorization_state_ref": email_provider_authorization_state_ref(provider),
        "sync_cursor_ref": format!("email.sync_cursor.observed:{}", cursor_kind.as_str()),
        "sync_cursor_kind": cursor_kind.as_str(),
        "runtime_state_ref": mode.runtime_state_ref(),
        "coverage_ref": mode.coverage_ref(),
        "debt_ref": mode.debt_ref(),
        "caveats": runtime_payload.caveats.clone(),
        "runtime_observation_contract": runtime_payload,
    })
}

fn email_provider_executed_runtime_caveats(
    mode: EmailProviderRuntimeMode,
) -> &'static [&'static str] {
    match mode {
        EmailProviderRuntimeMode::GmailScheduledSync => &[
            "Gmail API sync used an operator-provided token file; OAuth refresh remains outside this executor",
            "cursor is admitted as an event after provider records are consumed",
        ],
        EmailProviderRuntimeMode::ImapScheduledSync => &[
            "IMAP sync used operator-provided credentials; durable credential refresh remains outside this executor",
            "cursor is admitted as an event after provider records are consumed",
        ],
        EmailProviderRuntimeMode::ImapIdleLive => &[
            "IMAP IDLE observation is bounded by idle_timeout_ms in ops.start; daemon reconnect/backoff remains runtime-owned",
            "cursor is admitted as an event after provider records are consumed",
        ],
    }
}

fn email_provider_failed_execution(
    scope: &mut serde_json::Map<String, serde_json::Value>,
    preview_summary: &mut serde_json::Value,
    mode: EmailProviderRuntimeMode,
    provider_scope: &EmailProviderOperationScope,
    executor_state: &'static str,
    reason: String,
    auth_state: EmailAuthorizationState,
    network_state: EmailNetworkState,
    rate_limit_state: Option<EmailRateLimitState>,
    started: Instant,
) -> EmailSyncExecutionResult {
    let runtime = email_provider_failed_runtime_value(
        mode,
        provider_scope,
        &reason,
        auth_state,
        network_state,
        rate_limit_state,
    );
    scope.insert(
        "executor_state".to_string(),
        serde_json::json!(executor_state),
    );
    scope.insert("provider_runtime".to_string(), runtime.clone());
    scope.insert(
        "provider_failure".to_string(),
        serde_json::json!({
            "reason": reason,
            "coverage_ref": mode.coverage_ref(),
            "debt_ref": mode.debt_ref(),
            "actions": email_provider_runtime_actions(mode),
        }),
    );

    let preview = preview_summary
        .as_object_mut()
        .expect("package operation preview is an object");
    preview.insert(
        "executor_state".to_string(),
        serde_json::json!(executor_state),
    );
    preview.insert("provider_runtime".to_string(), runtime);
    preview.insert(
        "provider_failure".to_string(),
        scope["provider_failure"].clone(),
    );

    EmailSyncExecutionResult {
        status: OperationStatus::Failed,
        message: format!("email_capture; provider sync failed: {reason}"),
        duration_ms: Some(elapsed_millis(started)),
    }
}

fn email_provider_failed_runtime_value(
    mode: EmailProviderRuntimeMode,
    provider_scope: &EmailProviderOperationScope,
    reason: &str,
    auth_state: EmailAuthorizationState,
    network_state: EmailNetworkState,
    rate_limit_state: Option<EmailRateLimitState>,
) -> serde_json::Value {
    let provider = mode.provider();
    let cursor_kind = email_provider_sync_cursor_kind(provider);
    let runtime_payload = EmailCaptureRuntimeObservedPayload {
        provider,
        account_binding_ref: provider_scope.account_binding_ref.clone(),
        mode_id: mode.mode_id().to_string(),
        observed_at: Timestamp::now(),
        provider_runtime: mode.runtime(),
        auth_state,
        network_state,
        rate_limit_state,
        sync_state: EmailSyncState::Failed,
        pending_messages: None,
        pending_material_bytes: None,
        caveats: vec![reason.to_string()],
        actions: email_provider_runtime_actions(mode)
            .iter()
            .map(|action| (*action).to_string())
            .collect(),
    };
    serde_json::json!({
        "provider": provider.as_str(),
        "provider_runtime": mode.runtime().as_str(),
        "account_binding_ref": provider_scope.account_binding_ref,
        "mailbox_scope": provider_scope.mailbox_scope,
        "authorization_state_ref": email_provider_authorization_state_ref(provider),
        "sync_cursor_ref": format!("email.sync_cursor.observed:{}", cursor_kind.as_str()),
        "sync_cursor_kind": cursor_kind.as_str(),
        "runtime_state_ref": mode.runtime_state_ref(),
        "coverage_ref": mode.coverage_ref(),
        "debt_ref": mode.debt_ref(),
        "caveats": [reason],
        "runtime_observation_contract": runtime_payload,
    })
}

fn classify_gmail_provider_failure(
    reason: &str,
) -> (
    EmailAuthorizationState,
    EmailNetworkState,
    Option<EmailRateLimitState>,
) {
    if reason.contains("HTTP 401") || reason.contains("HTTP 403") {
        (
            EmailAuthorizationState::Rejected,
            EmailNetworkState::Online,
            Some(EmailRateLimitState::Clear),
        )
    } else if reason.contains("HTTP 429") {
        (
            EmailAuthorizationState::Authorized,
            EmailNetworkState::RateLimited,
            Some(EmailRateLimitState::Backoff),
        )
    } else {
        (
            EmailAuthorizationState::Authorized,
            EmailNetworkState::Error,
            None,
        )
    }
}

fn classify_imap_provider_failure(reason: &str) -> (EmailAuthorizationState, EmailNetworkState) {
    if reason.contains("AUTHENTICATIONFAILED") || reason.contains("authentication") {
        (EmailAuthorizationState::Rejected, EmailNetworkState::Online)
    } else {
        (
            EmailAuthorizationState::Authorized,
            EmailNetworkState::Error,
        )
    }
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
