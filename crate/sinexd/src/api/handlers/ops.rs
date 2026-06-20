use sinex_db::DbPoolExt;
use sinex_db::repositories::state::Operation as DbOperation;
use sinex_db::repositories::state::PROJECTION_REBUILD_OPERATION_TYPE;
use sinex_primitives::Id;
use sinex_primitives::SinexError;
use sqlx::PgPool;

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
const EMAIL_STAGED_MODE_IDS: &[&str] = &[
    "source:email.mailbox.maildir-staged",
    "source:email.mailbox.mbox-staged",
];
const EMAIL_PROVIDER_MODE_IDS: &[&str] = &[
    "source:email.mailbox.gmail-api-scheduled-sync",
    "source:email.mailbox.imap-scheduled-sync",
    "source:email.mailbox.imap-idle-live",
];
const EMAIL_SYNC_MODE_IDS: &[&str] = &[
    "source:email.mailbox.maildir-staged",
    "source:email.mailbox.mbox-staged",
    "source:email.mailbox.gmail-api-scheduled-sync",
    "source:email.mailbox.imap-scheduled-sync",
];

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

    let preview_summary = serde_json::json!({
        "surface": spec.surface,
        "operation_type": operation_type,
        "source_id": spec.source_id,
        "mode_id": mode_id,
        "action": spec.action,
        "executor_state": PACKAGE_OPERATION_EXECUTOR_STATE,
        "message": spec.executor_message,
    });

    pool.state()
        .log_operation(DbOperation {
            id: None,
            operation_type: operation_type.to_string(),
            operator: actor.to_string(),
            scope: Some(serde_json::Value::Object(scope)),
            result_status: sinex_primitives::domain::OperationStatus::Running,
            result_message: Some(format!("{}; executor pending", spec.surface)),
            preview_summary: Some(preview_summary),
            duration_ms: None,
        })
        .await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::rpc_server::RpcAuthContext;
    use sinex_primitives::domain::OperationStatus;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn ops_start_records_media_operation_as_pending_executor(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let response = handle_ops_start(
            ctx.pool(),
            OpsStartRequest {
                operation_type: "media.screen-ocr.capture-region".to_string(),
                scope: Some(serde_json::json!({
                    "source_id": "media.screen-ocr",
                    "mode_id": "source:media.screen-ocr.on-demand-region",
                    "region": [0, 0, 640, 480],
                    "reason": "operator-requested"
                })),
            },
            &RpcAuthContext::system(),
        )
        .await?;

        assert_eq!(
            response.operation.operation_type,
            "media.screen-ocr.capture-region"
        );
        assert_eq!(response.operation.result_status, OperationStatus::Running);
        assert_eq!(
            response.operation.result_message.as_deref(),
            Some("media_capture; executor pending")
        );
        let scope = response
            .operation
            .scope
            .as_ref()
            .expect("media operation scope should be recorded");
        assert_eq!(scope["surface"], "media_capture");
        assert_eq!(scope["source_id"], "media.screen-ocr");
        assert_eq!(scope["mode_id"], "source:media.screen-ocr.on-demand-region");
        assert_eq!(scope["action"], "capture_region");
        assert_eq!(scope["executor_state"], PACKAGE_OPERATION_EXECUTOR_STATE);
        let preview = response
            .operation
            .preview_summary
            .as_ref()
            .expect("media operation preview should be recorded");
        assert_eq!(preview["executor_state"], PACKAGE_OPERATION_EXECUTOR_STATE);
        assert_eq!(preview["operation_type"], "media.screen-ocr.capture-region");

        let operation_id = response.operation.id.parse::<Id<DbOperation>>()?;
        let persisted = ctx
            .pool()
            .state()
            .get_operation(&operation_id)
            .await?
            .expect("media operation should be persisted");
        assert_eq!(persisted.operation_type, "media.screen-ocr.capture-region");
        assert_eq!(persisted.result_status, OperationStatus::Running);
        Ok(())
    }

    #[sinex_test]
    async fn ops_start_rejects_media_operation_wrong_mode(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let error = handle_ops_start(
            ctx.pool(),
            OpsStartRequest {
                operation_type: "media.audio-transcript.run-model".to_string(),
                scope: Some(serde_json::json!({
                    "source_id": "media.audio-transcript",
                    "mode_id": "source:media.audio-transcript.live-session"
                })),
            },
            &RpcAuthContext::system(),
        )
        .await
        .expect_err("wrong package mode should be rejected");

        assert!(
            error
                .to_string()
                .contains("source:media.audio-transcript.local-model-batch")
        );
        Ok(())
    }

    #[sinex_test]
    async fn ops_start_records_email_sync_for_provider_mode(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let response = handle_ops_start(
            ctx.pool(),
            OpsStartRequest {
                operation_type: "email.mailbox.sync".to_string(),
                scope: Some(serde_json::json!({
                    "source_id": "email.mailbox",
                    "mode_id": "source:email.mailbox.gmail-api-scheduled-sync",
                    "account_ref": "operator-mailbox:primary",
                    "reason": "operator-requested"
                })),
            },
            &RpcAuthContext::system(),
        )
        .await?;

        assert_eq!(response.operation.operation_type, "email.mailbox.sync");
        assert_eq!(response.operation.result_status, OperationStatus::Running);
        assert_eq!(
            response.operation.result_message.as_deref(),
            Some("email_capture; executor pending")
        );
        let scope = response
            .operation
            .scope
            .as_ref()
            .expect("email operation scope should be recorded");
        assert_eq!(scope["surface"], "email_capture");
        assert_eq!(scope["source_id"], "email.mailbox");
        assert_eq!(
            scope["mode_id"],
            "source:email.mailbox.gmail-api-scheduled-sync"
        );
        assert_eq!(scope["action"], "sync");
        assert_eq!(scope["account_ref"], "operator-mailbox:primary");

        let preview = response
            .operation
            .preview_summary
            .as_ref()
            .expect("email operation preview should be recorded");
        assert_eq!(preview["surface"], "email_capture");
        assert_eq!(preview["operation_type"], "email.mailbox.sync");
        assert_eq!(
            preview["mode_id"],
            "source:email.mailbox.gmail-api-scheduled-sync"
        );
        Ok(())
    }

    #[sinex_test]
    async fn ops_start_rejects_email_operation_without_mode(
        ctx: xtask::sandbox::TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let error = handle_ops_start(
            ctx.pool(),
            OpsStartRequest {
                operation_type: "email.mailbox.authorize".to_string(),
                scope: Some(serde_json::json!({
                    "source_id": "email.mailbox",
                    "account_ref": "operator-mailbox:primary"
                })),
            },
            &RpcAuthContext::system(),
        )
        .await
        .expect_err("email provider operation should require a package mode");

        assert!(error.to_string().contains("requires mode_id"));
        assert!(
            error
                .to_string()
                .contains("source:email.mailbox.gmail-api-scheduled-sync")
        );
        Ok(())
    }
}
