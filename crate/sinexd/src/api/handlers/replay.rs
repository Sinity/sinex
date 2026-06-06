//! Replay-control RPC handlers (extracted from `rpc_handlers.rs` for symmetry
//! with the other domain-specific handler modules per #1172).

use crate::api::replay_control::ReplayControlClient;
use crate::api::rpc_server::RpcAuthContext;
use sinex_db::replay::state_machine::{
    ReplayOperation as DbReplayOperation, ReplayScope as DbReplayScope,
    ReplayState as DbReplayState,
};
use sinex_primitives::rpc::replay::{
    ReplayApproveRequest, ReplayApproveResponse, ReplayCancelRequest, ReplayCancelResponse,
    ReplayCheckpoint, ReplayCreateRequest, ReplayCreateResponse, ReplayExecuteRequest,
    ReplayExecuteResponse, ReplayListRequest, ReplayListResponse, ReplayOperation,
    ReplayPreviewRequest, ReplayPreviewResponse, ReplayScope as RpcReplayScope,
    ReplayState as RpcReplayState, ReplayStatusRequest, ReplayStatusResponse,
    ReplaySubmitRequest, ReplaySubmitResponse,
};
use sinex_primitives::{Result, SinexError, Uuid};

fn parse_operation_uuid(raw: &str) -> Result<Uuid> {
    raw.parse::<Uuid>().map_err(|error| {
        SinexError::validation("invalid UUIDv7 parameter")
            .with_context("parameter", "operation_id")
            .with_context("value", raw)
            .with_std_error(&error)
    })
}

pub async fn handle_replay_create_operation(
    client: &ReplayControlClient,
    req: ReplayCreateRequest,
    auth: &RpcAuthContext,
) -> Result<ReplayCreateResponse> {
    // Propagate the planner's classified error directly. The duplicate-operation
    // guard returns `SinexError::InvalidState` (JSON-RPC -32803); flattening every
    // failure into a generic service error (-32820) would erase the error code
    // clients depend on to distinguish "already active" from a real internal fault.
    let operation = client
        .plan(auth.replay_actor(), db_replay_scope(req.scope)?)
        .await?;
    Ok(ReplayCreateResponse {
        operation: into_replay_operation(operation)?,
    })
}

pub async fn handle_replay_preview_operation(
    client: &ReplayControlClient,
    req: ReplayPreviewRequest,
    _auth: &RpcAuthContext,
) -> Result<ReplayPreviewResponse> {
    let operation_id = parse_operation_uuid(&req.operation_id)?;
    let (operation, preview) = client.preview(operation_id).await.map_err(|error| {
        SinexError::service("failed to preview replay operation").with_source(error)
    })?;
    Ok(ReplayPreviewResponse {
        operation: into_replay_operation(operation)?,
        preview,
    })
}

pub async fn handle_replay_approve_operation(
    client: &ReplayControlClient,
    req: ReplayApproveRequest,
    auth: &RpcAuthContext,
) -> Result<ReplayApproveResponse> {
    let operation_id = parse_operation_uuid(&req.operation_id)?;
    let operation = client
        .approve(operation_id, auth.replay_actor())
        .await
        .map_err(|error| {
            SinexError::service("failed to approve replay operation").with_source(error)
        })?;
    Ok(ReplayApproveResponse {
        operation: into_replay_operation(operation)?,
    })
}

pub async fn handle_replay_execute_operation(
    client: &ReplayControlClient,
    req: ReplayExecuteRequest,
    auth: &RpcAuthContext,
) -> Result<ReplayExecuteResponse> {
    let operation_id = parse_operation_uuid(&req.operation_id)?;
    let operation = client
        .execute_with_overrides(
            operation_id,
            auth.replay_actor(),
            req.dry_run,
            req.gate_overrides,
        )
        .await
        .map_err(|error| {
            SinexError::service("failed to execute replay operation").with_source(error)
        })?;
    Ok(ReplayExecuteResponse {
        operation: into_replay_operation(operation)?,
    })
}

pub async fn handle_replay_submit_operation(
    client: &ReplayControlClient,
    req: ReplaySubmitRequest,
    auth: &RpcAuthContext,
) -> Result<ReplaySubmitResponse> {
    let operation_id = parse_operation_uuid(&req.operation_id)?;
    let operation = client
        .submit_with_overrides(operation_id, auth.replay_actor(), req.gate_overrides)
        .await
        .map_err(|error| {
            SinexError::service("failed to submit replay operation").with_source(error)
        })?;
    Ok(ReplaySubmitResponse {
        operation: into_replay_operation(operation)?,
    })
}

pub async fn handle_replay_cancel_operation(
    client: &ReplayControlClient,
    req: ReplayCancelRequest,
    auth: &RpcAuthContext,
) -> Result<ReplayCancelResponse> {
    let operation_id = parse_operation_uuid(&req.operation_id)?;
    let operation = client
        .cancel(operation_id, auth.replay_actor(), req.reason)
        .await
        .map_err(|error| {
            SinexError::service("failed to cancel replay operation").with_source(error)
        })?;
    Ok(ReplayCancelResponse {
        cancelled: true,
        operation: into_replay_operation(operation)?,
    })
}

pub async fn handle_replay_operation_status(
    client: &ReplayControlClient,
    req: ReplayStatusRequest,
    _auth: &RpcAuthContext,
) -> Result<ReplayStatusResponse> {
    let operation_id = parse_operation_uuid(&req.operation_id)?;
    let operation = client.status(operation_id).await.map_err(|error| {
        SinexError::service("failed to fetch replay operation status").with_source(error)
    })?;
    Ok(ReplayStatusResponse {
        operation: into_replay_operation(operation)?,
    })
}

pub async fn handle_replay_list_operations(
    client: &ReplayControlClient,
    req: ReplayListRequest,
    _auth: &RpcAuthContext,
) -> Result<ReplayListResponse> {
    let operations = client
        .list(req.state.map(db_replay_state), req.module, req.limit)
        .await
        .map_err(|error| {
            SinexError::service("failed to list replay operations").with_source(error)
        })?;
    Ok(ReplayListResponse {
        operations: operations
            .into_iter()
            .map(into_replay_operation)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn into_replay_operation(operation: DbReplayOperation) -> Result<ReplayOperation> {
    Ok(ReplayOperation {
        operation_id: operation.operation_id.to_string(),
        state: rpc_replay_state(operation.state),
        scope: rpc_replay_scope(operation.scope),
        preview_summary: operation.preview_summary,
        checkpoint: ReplayCheckpoint {
            processed_events: operation.checkpoint.processed_events,
            total_events: operation.checkpoint.total_events,
            last_event_id: operation
                .checkpoint
                .last_event_id
                .map(|event_id| event_id.to_string()),
            batch_number: operation.checkpoint.batch_number,
            savepoint_id: operation.checkpoint.savepoint_id,
            updated_at: operation.checkpoint.updated_at.format_rfc3339(),
        },
        actor: operation.actor,
        created_at: operation.created_at.format_rfc3339(),
        approved_by: operation.approved_by,
        approved_at: operation.approved_at.map(|ts| ts.format_rfc3339()),
        executor_module: operation.executor_module,
        started_at: operation.started_at.map(|ts| ts.format_rfc3339()),
        finished_at: operation.finished_at.map(|ts| ts.format_rfc3339()),
        outcome: operation.outcome,
        error_details: operation.error_details,
    })
}

fn rpc_replay_scope(scope: DbReplayScope) -> RpcReplayScope {
    RpcReplayScope {
        source_name: scope.source_name,
        time_window: scope
            .time_window
            .map(|(start, end)| (start.format_rfc3339(), end.format_rfc3339())),
        material_filter: scope
            .material_filter
            .map(|ids| ids.into_iter().map(|id| id.to_string()).collect()),
        filters: scope.filters,
        source_id: scope.source_id,
        source_material_id: scope.source_material_id.map(|id| id.to_string()),
        parser_id: None,
        parser_version: scope.source_version,
    }
}

fn rpc_replay_state(state: DbReplayState) -> RpcReplayState {
    match state {
        DbReplayState::Planning => RpcReplayState::Planning,
        DbReplayState::Previewed => RpcReplayState::Previewed,
        DbReplayState::Approved => RpcReplayState::Approved,
        DbReplayState::Executing => RpcReplayState::Executing,
        DbReplayState::Cancelling => RpcReplayState::Cancelling,
        DbReplayState::Committing => RpcReplayState::Committing,
        DbReplayState::Completed => RpcReplayState::Completed,
        DbReplayState::Failed => RpcReplayState::Failed,
        DbReplayState::Cancelled => RpcReplayState::Cancelled,
    }
}

fn db_replay_scope(scope: RpcReplayScope) -> Result<DbReplayScope> {
    Ok(DbReplayScope {
        source_name: scope.source_name,
        time_window: scope
            .time_window
            .map(|(start, end)| {
                Ok::<_, SinexError>((
                    sinex_primitives::Timestamp::parse_rfc3339(&start).map_err(|error| {
                        SinexError::validation("invalid replay scope start timestamp")
                            .with_context("value", start)
                            .with_std_error(&error)
                    })?,
                    sinex_primitives::Timestamp::parse_rfc3339(&end).map_err(|error| {
                        SinexError::validation("invalid replay scope end timestamp")
                            .with_context("value", end)
                            .with_std_error(&error)
                    })?,
                ))
            })
            .transpose()?,
        material_filter: scope
            .material_filter
            .map(|ids| {
                ids.into_iter()
                    .map(|raw| {
                        raw.parse::<Uuid>().map_err(|error| {
                            SinexError::validation("invalid replay material UUID")
                                .with_context("value", raw)
                                .with_std_error(&error)
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?,
        filters: scope.filters,
        source_id: scope.source_id.or(scope.parser_id),
        source_material_id: scope
            .source_material_id
            .map(|raw| {
                raw.parse::<Uuid>().map_err(|error| {
                    SinexError::validation("invalid replay source material UUID")
                        .with_context("value", raw)
                        .with_std_error(&error)
                })
            })
            .transpose()?,
        source_version: scope.parser_version,
    })
}

fn db_replay_state(state: RpcReplayState) -> DbReplayState {
    match state {
        RpcReplayState::Planning => DbReplayState::Planning,
        RpcReplayState::Previewed => DbReplayState::Previewed,
        RpcReplayState::Approved => DbReplayState::Approved,
        RpcReplayState::Executing => DbReplayState::Executing,
        RpcReplayState::Cancelling => DbReplayState::Cancelling,
        RpcReplayState::Committing => DbReplayState::Committing,
        RpcReplayState::Completed => DbReplayState::Completed,
        RpcReplayState::Failed => DbReplayState::Failed,
        RpcReplayState::Cancelled => DbReplayState::Cancelled,
    }
}

#[cfg(any(feature = "test-support", test))]
pub(crate) fn parse_replay_state(value: &str) -> Result<DbReplayState> {
    match value.to_lowercase().as_str() {
        "planning" => Ok(DbReplayState::Planning),
        "previewed" => Ok(DbReplayState::Previewed),
        "approved" => Ok(DbReplayState::Approved),
        "executing" => Ok(DbReplayState::Executing),
        "cancelling" => Ok(DbReplayState::Cancelling),
        "committing" => Ok(DbReplayState::Committing),
        "completed" => Ok(DbReplayState::Completed),
        "failed" => Ok(DbReplayState::Failed),
        "cancelled" => Ok(DbReplayState::Cancelled),
        other => Err(SinexError::validation("unknown replay state").with_context("state", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn parse_replay_state_accepts_known_variants() -> TestResult<()> {
        let states = [
            ("planning", DbReplayState::Planning),
            ("PREVIEWED", DbReplayState::Previewed),
            ("Approved", DbReplayState::Approved),
            ("cancelling", DbReplayState::Cancelling),
        ];
        for (input, expected) in states {
            assert_eq!(parse_replay_state(input).unwrap(), expected);
        }
        assert!(parse_replay_state("unknown").is_err());
        Ok(())
    }
}
