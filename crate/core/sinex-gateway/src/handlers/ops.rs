use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_db::repositories::state::Operation as DbOperation;
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
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::info;

    let request: OpsStartRequest = serde_json::from_value(params)?;
    let scope_jsonb = request.scope.unwrap_or(serde_json::json!({}));
    let actor = auth.actor_id();

    let record = pool
        .state()
        .start_operation(&request.operation_type, actor, scope_jsonb)
        .await?;

    info!(
        actor = %actor,
        operation_id = %record.id,
        operation_type = %request.operation_type,
        "Operation started"
    );

    let response = OpsStartResponse {
        operation: record_to_operation(record),
    };

    Ok(serde_json::to_value(response)?)
}

/// Handle GET /ops - list operations with optional filtering
///
/// # Authorization
///
/// Read-only operation. Auth context accepted for audit trail consistency.
pub async fn handle_ops_list(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::debug;

    let request: OpsListRequest = super::parse_default_on_null(params)?;
    let limit = if request.limit == default_ops_limit() {
        request.limit
    } else if request.limit > 0 {
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

    Ok(serde_json::to_value(response)?)
}

/// Handle GET /ops/{id} - get operation details
///
/// # Authorization
///
/// Read-only operation. Auth context accepted for audit trail consistency.
pub async fn handle_ops_get(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::debug;

    let request: OpsGetRequest = serde_json::from_value(params)?;

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

    Ok(serde_json::to_value(response)?)
}

/// Handle POST /ops/{id}/cancel - cancel a running operation
///
/// # Authorization
///
/// This is a dangerous operation that cancels a running system operation.
/// The auth context is logged for audit purposes.
pub async fn handle_ops_cancel(
    pool: &PgPool,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::info;

    let request: OpsCancelRequest = serde_json::from_value(params)?;

    let operation_id = request
        .operation_id
        .parse::<Id<DbOperation>>()
        .map_err(|e| SinexError::parse(format!("Invalid operation ID: {e}")))?;

    info!(
        actor = %auth.actor_id(),
        operation_id = %operation_id,
        reason = ?request.reason,
        "Operation cancel initiated"
    );

    let reason = request
        .reason
        .unwrap_or_else(|| "Cancelled by user".to_string());

    let record = pool
        .state()
        .cancel_operation(&operation_id, &reason)
        .await?;

    let response = OpsCancelResponse {
        operation: record_to_operation(record),
        cancelled: true,
    };

    Ok(serde_json::to_value(response)?)
}
