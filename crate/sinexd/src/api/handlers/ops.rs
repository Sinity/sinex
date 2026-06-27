use camino::Utf8PathBuf;
use futures::StreamExt as _;
use serde::Deserialize;
use sinex_db::DbPoolExt;
use sinex_db::repositories::state::Operation as DbOperation;
use sinex_db::repositories::state::PROJECTION_REBUILD_OPERATION_TYPE;
use sinex_db::repositories::{EmailMailboxProjectionEvent, EmailProviderStateUpsert};
use sinex_primitives::Id;
use sinex_primitives::SinexError;
use sinex_primitives::domain::{
    OperationStatus, SourceMaterialFormat, SourceMaterialTimingInfoType,
};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::events::payloads::email::{
    EmailAuthorizationState, EmailCaptureRuntimeObservedPayload, EmailContinuityState,
    EmailNetworkState, EmailProviderKind, EmailRateLimitState, EmailSyncCursorObservedPayload,
    EmailSyncState,
};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::{Event, SourceMaterial};
use sinex_primitives::parser::{
    InputShapeAdapter, MaterialAnchor, MaterialParser, ParserContext, SourceId, SourceRecord,
    maybe_occurrence_key_string,
};
use sinex_primitives::rpc::sources::{SourceMaterialMetadataContract, SourceOrigin};
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use std::time::Instant;

use crate::runtime::parser::{
    EmailMboxFileAdapter, EmailMboxFileConfig, GmailApiCursorAdapter, GmailApiCursorConfig,
    GmailHttpClient, GmailOAuthCredentials, GoogleOAuthClient, ImapSyncAdapter, ImapSyncConfig,
    ImapSyncMode, NativeImapSyncClient, NativeImapSyncClientConfig, NativeImapTlsMode, OAuthError,
    OAuthTokenProvider,
};
use crate::sources::source_contracts::email::EmailMailboxParser;

mod email;
mod media;
mod package;

use package::{
    EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID, EMAIL_GMAIL_SYNC_DEFAULT_PAGE_SIZE,
    EMAIL_GMAIL_SYNC_EXECUTOR_STATE, EMAIL_GMAIL_SYNC_FAILED_EXECUTOR_STATE,
    EMAIL_IMAP_IDLE_LIVE_MODE_ID, EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID,
    EMAIL_IMAP_SYNC_DEFAULT_BATCH_SIZE, EMAIL_IMAP_SYNC_DEFAULT_IDLE_TIMEOUT_MS,
    EMAIL_IMAP_SYNC_EXECUTOR_STATE, EMAIL_IMAP_SYNC_FAILED_EXECUTOR_STATE,
    EMAIL_MAILDIR_STAGED_MODE_ID, EMAIL_MBOX_STAGED_MODE_ID, EMAIL_PROVIDER_MODE_IDS,
    EMAIL_STAGED_MODE_IDS,
    EMAIL_STAGED_SYNC_DEFAULT_MAX_MESSAGE_BYTES, EMAIL_STAGED_SYNC_EXECUTOR_STATE,
    EmailProviderModeMetadata, EmailProviderOperationScope, EmailProviderRuntimeMode,
    PACKAGE_OPERATION_EXECUTOR_STATE, PackageOperationSpec, email_provider_authorization_state_ref,
    email_provider_sync_cursor_kind, media_operation_metadata, package_mode_contract_metadata,
    package_operation_spec,
};

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
    if let Some(mode_contract) = package_mode_contract_metadata(&mode_id) {
        scope.insert("mode_contract".to_string(), mode_contract);
    }
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
    if let Some(mode_contract) = scope.get("mode_contract").cloned() {
        preview_summary
            .as_object_mut()
            .expect("package operation preview is an object")
            .insert("mode_contract".to_string(), mode_contract);
    }
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
            media::execute_worker_output(pool, &spec, &mode_id, &mut scope, &mut preview_summary)
                .await?
    {
        return log_package_operation(
            pool,
            DbOperation {
                id: None,
                operation_type: operation_type.to_string(),
                operator: actor.to_string(),
                scope: Some(serde_json::Value::Object(scope)),
                result_status: media_result.status,
                result_message: Some(media_result.message),
                preview_summary: Some(preview_summary),
                duration_ms: media_result.duration_ms,
            },
        )
        .await;
    }

    // Paused-binding gate: a provider sync for a binding the operator has paused
    // is skipped (Cancelled) before any provider work runs. The pause/resume
    // operations and this gate key on the same canonical `account_binding_ref`.
    if spec.surface == "email_capture"
        && spec.operation_type == "email.mailbox.sync"
        && EMAIL_PROVIDER_MODE_IDS.contains(&mode_id.as_str())
        && let Some(account_binding_ref) = optional_scope_string(&scope, "account_binding_ref")
        && pool
            .email_provider_states()
            .list_current_by_source(spec.source_id)
            .await?
            .iter()
            .any(|state| {
                state.mode_id == mode_id
                    && state.account_binding_ref == account_binding_ref
                    && state.sync_state == "paused"
            })
    {
        let executor_state = "email_sync_skipped_paused";
        scope.insert(
            "executor_state".to_string(),
            serde_json::json!(executor_state),
        );
        preview_summary
            .as_object_mut()
            .expect("package operation preview is an object")
            .insert(
                "executor_state".to_string(),
                serde_json::json!(executor_state),
            );
        return log_package_operation(
            pool,
            DbOperation {
                id: None,
                operation_type: operation_type.to_string(),
                operator: actor.to_string(),
                scope: Some(serde_json::Value::Object(scope)),
                result_status: OperationStatus::Cancelled,
                result_message: Some(format!(
                    "email_capture; sync skipped: binding {account_binding_ref} is paused"
                )),
                preview_summary: Some(preview_summary),
                duration_ms: Some(0),
            },
        )
        .await;
    }

    if spec.surface == "email_capture"
        && spec.operation_type == "email.mailbox.sync"
        && mode_id == EMAIL_GMAIL_SCHEDULED_SYNC_MODE_ID
        && let Some(email_result) =
            execute_gmail_provider_sync(pool, &spec, &mode_id, &mut scope, &mut preview_summary)
                .await?
    {
        return log_package_operation(
            pool,
            DbOperation {
                id: None,
                operation_type: operation_type.to_string(),
                operator: actor.to_string(),
                scope: Some(serde_json::Value::Object(scope)),
                result_status: email_result.status,
                result_message: Some(email_result.message),
                preview_summary: Some(preview_summary),
                duration_ms: email_result.duration_ms,
            },
        )
        .await;
    }

    if spec.surface == "email_capture"
        && spec.operation_type == "email.mailbox.sync"
        && (mode_id == EMAIL_IMAP_SCHEDULED_SYNC_MODE_ID || mode_id == EMAIL_IMAP_IDLE_LIVE_MODE_ID)
        && let Some(email_result) =
            execute_imap_provider_sync(pool, &spec, &mode_id, &mut scope, &mut preview_summary)
                .await?
    {
        return log_package_operation(
            pool,
            DbOperation {
                id: None,
                operation_type: operation_type.to_string(),
                operator: actor.to_string(),
                scope: Some(serde_json::Value::Object(scope)),
                result_status: email_result.status,
                result_message: Some(email_result.message),
                preview_summary: Some(preview_summary),
                duration_ms: email_result.duration_ms,
            },
        )
        .await;
    }

    if spec.surface == "email_capture"
        && spec.operation_type == "email.mailbox.sync"
        && EMAIL_STAGED_MODE_IDS.contains(&mode_id.as_str())
        && let Some(email_result) =
            execute_staged_email_sync(pool, &spec, &mode_id, &mut scope, &mut preview_summary)
                .await?
    {
        return log_package_operation(
            pool,
            DbOperation {
                id: None,
                operation_type: operation_type.to_string(),
                operator: actor.to_string(),
                scope: Some(serde_json::Value::Object(scope)),
                result_status: email_result.status,
                result_message: Some(email_result.message),
                preview_summary: Some(preview_summary),
                duration_ms: email_result.duration_ms,
            },
        )
        .await;
    }

    if spec.surface == "email_capture"
        && let Some(email_result) = email::execute_materialization_operation(
            pool,
            &spec,
            &mode_id,
            actor,
            &mut scope,
            &mut preview_summary,
        )
        .await?
    {
        return log_package_operation(
            pool,
            DbOperation {
                id: None,
                operation_type: operation_type.to_string(),
                operator: actor.to_string(),
                scope: Some(serde_json::Value::Object(scope)),
                result_status: email_result.status,
                result_message: Some(email_result.message),
                preview_summary: Some(preview_summary),
                duration_ms: email_result.duration_ms,
            },
        )
        .await;
    }

    log_package_operation(
        pool,
        DbOperation {
            id: None,
            operation_type: operation_type.to_string(),
            operator: actor.to_string(),
            scope: Some(serde_json::Value::Object(scope)),
            result_status: OperationStatus::Running,
            result_message: Some(format!("{}; executor pending", spec.surface)),
            preview_summary: Some(preview_summary),
            duration_ms: None,
        },
    )
    .await
}

async fn log_package_operation(
    pool: &PgPool,
    operation: DbOperation,
) -> Result<sinex_db::repositories::OperationRecord> {
    let record = pool.state().log_operation(operation).await?;
    persist_email_provider_state_from_operation(pool, &record).await?;
    Ok(record)
}

async fn persist_email_provider_state_from_operation(
    pool: &PgPool,
    record: &sinex_db::repositories::OperationRecord,
) -> Result<()> {
    if record.operation_type != "email.mailbox.sync" {
        return Ok(());
    }
    let Some(scope) = record.scope.as_ref() else {
        return Ok(());
    };
    let Some(provider_runtime) = scope.get("provider_runtime").cloned() else {
        return Ok(());
    };

    let Some(source_id) = json_string(scope, "source_id") else {
        return Ok(());
    };
    let Some(mode_id) = json_string(scope, "mode_id") else {
        return Ok(());
    };
    let Some(provider) = json_string(&provider_runtime, "provider") else {
        return Ok(());
    };
    let Some(account_binding_ref) = json_string(&provider_runtime, "account_binding_ref")
        .or_else(|| json_string(scope, "account_binding_ref"))
    else {
        return Ok(());
    };
    let mailbox_scope = json_string(&provider_runtime, "mailbox_scope")
        .or_else(|| {
            scope
                .get("provider_operation_scope")
                .and_then(|value| json_string(value, "mailbox_scope"))
        })
        .unwrap_or_else(|| "default".to_string());

    let runtime_contract = provider_runtime
        .get("runtime_observation_contract")
        .unwrap_or(&provider_runtime);
    let auth_state =
        json_string(runtime_contract, "auth_state").unwrap_or_else(|| "unknown".to_string());
    let network_state =
        json_string(runtime_contract, "network_state").unwrap_or_else(|| "unknown".to_string());
    let sync_state =
        json_string(runtime_contract, "sync_state").unwrap_or_else(|| "unknown".to_string());
    let rate_limit_state = json_string(runtime_contract, "rate_limit_state");
    let failure_class = scope
        .get("provider_failure")
        .and_then(|failure| json_string(failure, "failure_class"));
    let required_action = scope
        .get("provider_failure")
        .and_then(|failure| json_string(failure, "required_action"));
    let retry_after_secs = scope
        .get("provider_failure")
        .and_then(|failure| failure.get("retry_after_secs"))
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok());
    let reconnect_state = scope
        .get("provider_failure")
        .and_then(|failure| json_string(failure, "reconnect_state"));
    let runtime_state_ref = json_string(&provider_runtime, "runtime_state_ref")
        .unwrap_or_else(|| format!("email.provider_runtime.{provider}"));
    let coverage_ref = json_string(&provider_runtime, "coverage_ref")
        .unwrap_or_else(|| format!("coverage:email.mailbox.{provider}.provider_runtime"));
    let debt_ref = scope
        .get("provider_failure")
        .and_then(|failure| json_string(failure, "debt_ref"))
        .or_else(|| json_string(&provider_runtime, "debt_ref"))
        .unwrap_or_else(|| format!("debt:email.mailbox.{provider}.provider_runtime"));

    let provider_cursor = scope.get("provider_cursor").cloned();
    let cursor_payload = provider_cursor
        .as_ref()
        .and_then(|cursor| cursor.get("cursor_observation_contract"))
        .or(provider_cursor.as_ref());
    let cursor_kind = provider_cursor
        .as_ref()
        .and_then(|cursor| json_string(cursor, "cursor_kind"))
        .or_else(|| cursor_payload.and_then(|payload| json_string(payload, "cursor_kind")));
    let cursor_value = provider_cursor
        .as_ref()
        .and_then(|cursor| json_string(cursor, "cursor_value"))
        .or_else(|| cursor_payload.and_then(provider_cursor_value));
    let continuity_state = provider_cursor
        .as_ref()
        .and_then(|cursor| json_string(cursor, "continuity_state"))
        .or_else(|| cursor_payload.and_then(|payload| json_string(payload, "continuity_state")));

    pool.email_provider_states()
        .upsert(EmailProviderStateUpsert {
            source_id,
            mode_id,
            provider,
            account_binding_ref,
            mailbox_scope,
            operation_id: record.id.to_uuid(),
            result_status: record.result_status,
            auth_state,
            network_state,
            sync_state,
            rate_limit_state,
            runtime_state_ref,
            coverage_ref,
            debt_ref,
            failure_class,
            required_action,
            retry_after_secs,
            reconnect_state,
            cursor_kind,
            cursor_value,
            continuity_state,
            provider_runtime,
            provider_cursor,
            provider_failure: scope.get("provider_failure").cloned(),
        })
        .await?;

    Ok(())
}

fn json_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

fn provider_cursor_value(payload: &serde_json::Value) -> Option<String> {
    json_string(payload, "cursor_value")
        .or_else(|| json_string(payload, "gmail_history_id"))
        .or_else(|| json_string(payload, "page_token"))
        .or_else(|| {
            match (
                json_string(payload, "uidvalidity"),
                json_string(payload, "uid"),
            ) {
                (Some(uidvalidity), Some(uid)) => Some(format!("{uidvalidity}:{uid}")),
                _ => None,
            }
        })
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
    /// Pre-fetched static access-token file. Operator-owned and short-lived;
    /// used when no OAuth refresh credentials are supplied.
    #[serde(default)]
    token_file: Option<Utf8PathBuf>,
    /// OAuth refresh credentials. Preferred over a static token: the runtime
    /// exchanges the refresh token for a live access token, so scheduled sync
    /// survives the ~1h access-token lifetime.
    #[serde(default)]
    oauth: Option<EmailOAuthRequest>,
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

/// Operator-owned OAuth refresh credential file references for Gmail sync.
/// Only file *paths* are held here; secret contents are read at exchange time
/// and never serialized into scope, previews, or errors.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmailOAuthRequest {
    client_id_file: Utf8PathBuf,
    client_secret_file: Utf8PathBuf,
    refresh_token_file: Utf8PathBuf,
    /// Token endpoint override (tests point this at a local server).
    #[serde(default)]
    token_url: Option<String>,
}

#[derive(Debug, Clone)]
struct EmailImapSyncRequest {
    host: String,
    port: u16,
    username: String,
    password_file: Option<Utf8PathBuf>,
    password: Option<String>,
    /// OAuth refresh credentials for SASL XOAUTH2 (Gmail/modern IMAP). When set,
    /// the executor exchanges them for an access token and authenticates with
    /// XOAUTH2 instead of a password.
    oauth: Option<EmailOAuthRequest>,
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
    let token = match request.resolve_bearer_token().await {
        Ok(token) => token,
        Err(failure) => {
            return Ok(Some(email_provider_failed_execution(
                scope,
                preview_summary,
                EmailProviderRuntimeMode::GmailScheduledSync,
                &provider_scope,
                EMAIL_GMAIL_SYNC_FAILED_EXECUTOR_STATE,
                failure.message,
                failure.auth_state,
                failure.network_state,
                None,
                started,
            )));
        }
    };

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
    let summary =
        match admit_gmail_adapter_records(pool, &material_record, mode_id, client, config).await {
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
    emit_email_capture_runtime_observed(
        pool,
        &material_record,
        email_provider_executed_runtime_payload(
            EmailProviderRuntimeMode::GmailScheduledSync,
            &provider_scope,
        ),
    )
    .await?;

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
    let (password, access_token) = if let Some(oauth) = &request.oauth {
        // SASL XOAUTH2: exchange refresh credentials for a live access token.
        match oauth.resolve_access_token().await {
            Ok(token) => (String::new(), Some(token)),
            Err(error) => {
                let failure = OAuthTokenFailure::from_oauth(error);
                return Ok(Some(email_provider_failed_execution(
                    scope,
                    preview_summary,
                    mode,
                    &provider_scope,
                    EMAIL_IMAP_SYNC_FAILED_EXECUTOR_STATE,
                    failure.message,
                    failure.auth_state,
                    failure.network_state,
                    None,
                    started,
                )));
            }
        }
    } else {
        match request.read_password().await {
            Ok(password) => (password, None),
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
        access_token,
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
    let summary =
        match admit_imap_adapter_records(pool, &material_record, mode_id, client, config).await {
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
    emit_email_capture_runtime_observed(
        pool,
        &material_record,
        email_provider_executed_runtime_payload(mode, &provider_scope),
    )
    .await?;

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
        let token_file = optional_scope_string(scope, "gmail_token_file")
            .or_else(|| optional_scope_string(scope, "access_token_file"))
            .or_else(|| optional_scope_string(scope, "token_file"))
            .map(Utf8PathBuf::from);
        let oauth = EmailOAuthRequest::from_scope(scope, "gmail_oauth_");
        // A Gmail sync request needs at least one credential source. With
        // neither, this is not a Gmail provider sync request — fall through.
        if token_file.is_none() && oauth.is_none() {
            return Ok(None);
        }
        Ok(Some(Self {
            token_file,
            oauth,
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
            "token_file_ref": self.token_file.as_ref().map(ToString::to_string),
            "oauth": self.oauth.as_ref().map(EmailOAuthRequest::sanitized_scope_value),
            "auth_source": if self.oauth.is_some() {
                "oauth_refresh"
            } else {
                "static_token_file"
            },
            "api_base_url": self.api_base_url,
            "user_id": self.user_id,
            "page_size": self.page_size.unwrap_or(EMAIL_GMAIL_SYNC_DEFAULT_PAGE_SIZE),
            "label_ids": self.label_ids.clone(),
            "include_spam_trash": self.include_spam_trash,
        })
    }

    /// Resolve a live bearer access token from OAuth refresh credentials when
    /// present, else from the static token file. OAuth/token failures map to the
    /// provider authorization/network states surfaced by coverage/debt rows.
    async fn resolve_bearer_token(&self) -> std::result::Result<String, OAuthTokenFailure> {
        if let Some(oauth) = &self.oauth {
            return oauth
                .resolve_access_token()
                .await
                .map_err(OAuthTokenFailure::from_oauth);
        }

        let Some(token_file) = &self.token_file else {
            return Err(OAuthTokenFailure {
                message: "Gmail sync requires gmail_token_file or OAuth refresh credentials"
                    .to_string(),
                auth_state: EmailAuthorizationState::Missing,
                network_state: EmailNetworkState::Unknown,
            });
        };
        let token = tokio::fs::read_to_string(token_file).await.map_err(|error| {
            OAuthTokenFailure {
                message: format!("Gmail API token file is unavailable: {error}"),
                auth_state: EmailAuthorizationState::Missing,
                network_state: EmailNetworkState::Unknown,
            }
        })?;
        let token = token.trim().to_string();
        if token.is_empty() {
            return Err(OAuthTokenFailure {
                message: "Gmail API token file is empty".to_string(),
                auth_state: EmailAuthorizationState::Missing,
                network_state: EmailNetworkState::Unknown,
            });
        }
        Ok(token)
    }
}

impl EmailOAuthRequest {
    fn from_scope(
        scope: &serde_json::Map<String, serde_json::Value>,
        prefix: &str,
    ) -> Option<Self> {
        let client_id_file = optional_scope_string(scope, &format!("{prefix}client_id_file"))?;
        let client_secret_file =
            optional_scope_string(scope, &format!("{prefix}client_secret_file"))?;
        let refresh_token_file =
            optional_scope_string(scope, &format!("{prefix}refresh_token_file"))?;
        Some(Self {
            client_id_file: Utf8PathBuf::from(client_id_file),
            client_secret_file: Utf8PathBuf::from(client_secret_file),
            refresh_token_file: Utf8PathBuf::from(refresh_token_file),
            token_url: optional_scope_string(scope, &format!("{prefix}token_url")),
        })
    }

    /// Load the refresh credentials and exchange them for a live access token.
    async fn resolve_access_token(&self) -> std::result::Result<String, OAuthError> {
        let credentials = GmailOAuthCredentials::load_from_files(
            self.client_id_file.as_str(),
            self.client_secret_file.as_str(),
            self.refresh_token_file.as_str(),
        )
        .await?;
        let exchange = match &self.token_url {
            Some(url) => GoogleOAuthClient::with_endpoint(reqwest::Client::new(), url.clone()),
            None => GoogleOAuthClient::new(),
        };
        OAuthTokenProvider::new(credentials, exchange)
            .bearer_token()
            .await
    }

    fn sanitized_scope_value(&self) -> serde_json::Value {
        // Only file-path references are surfaced; secret contents never are.
        serde_json::json!({
            "client_id_file_ref": self.client_id_file.to_string(),
            "client_secret_file_ref": self.client_secret_file.to_string(),
            "refresh_token_file_ref": self.refresh_token_file.to_string(),
            "token_url": self.token_url,
        })
    }
}

/// Token-resolution failure carrying the provider states for a failed-execution
/// runtime observation.
struct OAuthTokenFailure {
    message: String,
    auth_state: EmailAuthorizationState,
    network_state: EmailNetworkState,
}

impl OAuthTokenFailure {
    fn from_oauth(error: OAuthError) -> Self {
        let network_state = match &error {
            OAuthError::Transport(_) => EmailNetworkState::Offline,
            OAuthError::Status { .. } | OAuthError::Decode(_) | OAuthError::EmptyAccessToken => {
                EmailNetworkState::Online
            }
            OAuthError::MissingCredential { .. } => EmailNetworkState::Unknown,
        };
        Self {
            message: format!("OAuth token exchange failed: {error}"),
            auth_state: error.authorization_state(),
            network_state,
        }
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
        let oauth = EmailOAuthRequest::from_scope(scope, "imap_oauth_");
        // A request needs at least one credential source (password or OAuth).
        if password_file.is_none() && password.is_none() && oauth.is_none() {
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
            oauth,
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
            "oauth": self.oauth.as_ref().map(EmailOAuthRequest::sanitized_scope_value),
            "auth_source": if self.oauth.is_some() {
                "oauth_xoauth2"
            } else {
                "password"
            },
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
    mode_id: &str,
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
        if let Some(cursor) = admit_email_provider_record(
            pool,
            &mut parser,
            record,
            material_id,
            mode_id,
            &mut summary,
        )
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
    mode_id: &str,
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
        if let Some(cursor) = admit_email_provider_record(
            pool,
            &mut parser,
            record,
            material_id,
            mode_id,
            &mut summary,
        )
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
        admit_mbox_adapter_records(pool, &material_record, mode_id, config, &mut summary).await?;
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
        admit_mbox_adapter_records(pool, &material_record, mode_id, config, &mut summary).await?;
    }
    Ok(summary)
}

async fn admit_mbox_adapter_records(
    pool: &PgPool,
    material_record: &sinex_db::SourceMaterialRecord,
    mode_id: &str,
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
        admit_email_record(pool, &mut parser, record, material_id, mode_id, summary).await?;
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
        admit_email_record(
            pool,
            &mut parser,
            record,
            material_id,
            mode_id,
            &mut summary,
        )
        .await?;
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
    mode_id: &str,
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
        let event_type = intent.event_type.as_str().to_string();
        let payload = intent.payload.clone();
        let event = parsed_material_intent_to_event(intent, material_id)?;
        let persisted = pool.events().insert(event).await?;
        if let Some(id) = persisted.id {
            summary.event_ids.push(id.to_string());
            project_email_mailbox_event(pool, mode_id, id.to_uuid(), event_type, payload).await?;
        }
    }
    Ok(())
}

async fn admit_email_provider_record(
    pool: &PgPool,
    parser: &mut EmailMailboxParser,
    record: SourceRecord,
    material_id: Id<SourceMaterial>,
    mode_id: &str,
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
        let event_type = intent.event_type.as_str().to_string();
        let payload = intent.payload.clone();
        if intent.event_type.as_str() == "email.sync_cursor.observed" {
            last_cursor = Some(intent.payload.clone());
        }
        let event = parsed_material_intent_to_event(intent, material_id)?;
        let persisted = pool.events().insert(event).await?;
        if let Some(id) = persisted.id {
            summary.event_ids.push(id.to_string());
            project_email_mailbox_event(pool, mode_id, id.to_uuid(), event_type, payload).await?;
        }
    }
    Ok(last_cursor)
}

async fn project_email_mailbox_event(
    pool: &PgPool,
    mode_id: &str,
    event_id: uuid::Uuid,
    event_type: String,
    payload: serde_json::Value,
) -> Result<()> {
    pool.email_mailbox_projections()
        .upsert_event(EmailMailboxProjectionEvent {
            source_id: "email.mailbox".to_string(),
            mode_id: mode_id.to_string(),
            observed_event_id: event_id,
            event_type,
            payload,
        })
        .await?;
    Ok(())
}

fn elapsed_millis(started: Instant) -> i32 {
    i32::try_from(started.elapsed().as_millis()).unwrap_or(i32::MAX)
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
                "provider executor requires a gmail_token_file or OAuth refresh credentials at operation start",
                "OAuth refresh-token exchange runs in-operation when oauth credentials are supplied",
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
        required_action: None,
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

/// Build the executed (success) provider runtime observation payload. Shared by
/// the scope-JSON builder and the event emitter so the observation recorded in
/// scope and the emitted `email.capture_runtime.observed` event never diverge.
fn email_provider_executed_runtime_payload(
    mode: EmailProviderRuntimeMode,
    scope: &EmailProviderOperationScope,
) -> EmailCaptureRuntimeObservedPayload {
    EmailCaptureRuntimeObservedPayload {
        provider: mode.provider(),
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
    }
}

/// Emit the `email.capture_runtime.observed` event for a completed provider sync,
/// anchored to the provider sync material. This makes the runtime observation a
/// real, queryable event (not only a scope blob / state row), so automata and
/// `events.query` can see provider auth/sync/network observations over time.
async fn emit_email_capture_runtime_observed(
    pool: &PgPool,
    material_record: &sinex_db::SourceMaterialRecord,
    payload: EmailCaptureRuntimeObservedPayload,
) -> Result<()> {
    let material_id = Id::<SourceMaterial>::from_uuid(material_record.id);
    let event = payload
        .from_material(material_id)
        .build()
        .map_err(|error| {
            SinexError::processing(format!(
                "failed to build email.capture_runtime.observed event: {error}"
            ))
            .with_operation("ops.start")
        })?
        .to_json_event()
        .map_err(|error| {
            SinexError::serialization(format!(
                "failed to serialize email.capture_runtime.observed event: {error}"
            ))
            .with_operation("ops.start")
        })?;
    pool.events().insert(event).await?;
    Ok(())
}

fn email_provider_executed_runtime_value(
    mode: EmailProviderRuntimeMode,
    scope: &EmailProviderOperationScope,
) -> serde_json::Value {
    let provider = mode.provider();
    let cursor_kind = email_provider_sync_cursor_kind(provider);
    let runtime_payload = email_provider_executed_runtime_payload(mode, scope);
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
    let failure_class = email_provider_failure_class(auth_state, network_state, rate_limit_state);
    let required_action =
        email_provider_required_action(auth_state, network_state, rate_limit_state);
    let retry_after_secs = email_provider_retry_after_secs(rate_limit_state);
    let reconnect_state = email_provider_reconnect_state(network_state);
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
            "failure_class": failure_class,
            "required_action": required_action,
            "retry_after_secs": retry_after_secs,
            "reconnect_state": reconnect_state,
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

fn email_provider_failure_class(
    auth_state: EmailAuthorizationState,
    network_state: EmailNetworkState,
    rate_limit_state: Option<EmailRateLimitState>,
) -> &'static str {
    match (auth_state, network_state, rate_limit_state) {
        (EmailAuthorizationState::Missing, _, _) => "authorization-missing",
        (EmailAuthorizationState::Expired | EmailAuthorizationState::Rejected, _, _) => {
            "authorization-rejected"
        }
        (_, EmailNetworkState::RateLimited, Some(EmailRateLimitState::Backoff)) => {
            "rate-limited-backoff"
        }
        (_, EmailNetworkState::RateLimited, _) => "rate-limited",
        (_, EmailNetworkState::Offline | EmailNetworkState::Error, _) => "network-reconnect",
        _ => "provider-runtime-failed",
    }
}

fn email_provider_required_action(
    auth_state: EmailAuthorizationState,
    network_state: EmailNetworkState,
    rate_limit_state: Option<EmailRateLimitState>,
) -> &'static str {
    match (auth_state, network_state, rate_limit_state) {
        (EmailAuthorizationState::Missing, _, _)
        | (EmailAuthorizationState::Expired | EmailAuthorizationState::Rejected, _, _) => {
            "email.mailbox.authorize"
        }
        (_, EmailNetworkState::RateLimited, Some(EmailRateLimitState::Backoff)) => {
            "email.mailbox.wait-for-backoff"
        }
        (_, EmailNetworkState::RateLimited, _) => "email.mailbox.retry-after-rate-limit",
        (_, EmailNetworkState::Offline | EmailNetworkState::Error, _) => "email.mailbox.reconnect",
        _ => "email.mailbox.inspect-provider",
    }
}

fn email_provider_retry_after_secs(rate_limit_state: Option<EmailRateLimitState>) -> Option<i32> {
    match rate_limit_state {
        Some(EmailRateLimitState::Backoff | EmailRateLimitState::Throttled) => Some(300),
        Some(EmailRateLimitState::Exhausted) => Some(3600),
        _ => None,
    }
}

fn email_provider_reconnect_state(network_state: EmailNetworkState) -> Option<&'static str> {
    match network_state {
        EmailNetworkState::Offline | EmailNetworkState::Error => Some("reconnect-required"),
        EmailNetworkState::RateLimited => Some("backoff-active"),
        _ => None,
    }
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
        | EmailProviderRuntimeMode::ImapScheduledSync
        | EmailProviderRuntimeMode::ImapIdleLive => &[
            "email.mailbox.sync",
            "email.mailbox.pause",
            "email.mailbox.inspect",
        ],
    }
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
