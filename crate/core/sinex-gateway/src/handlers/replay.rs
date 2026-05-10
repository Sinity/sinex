//! Replay-control RPC handlers (extracted from `rpc_handlers.rs` for symmetry
//! with the other domain-specific handler modules per #1172).

use crate::handlers::rpc_handlers::RpcParams;
use crate::replay_control::ReplayControlClient;
use crate::rpc_server::RpcAuthContext;
use serde_json::Value;
use sinex_db::replay::state_machine::{
    ReplayOperation as DbReplayOperation, ReplayScope, ReplayState,
};
use sinex_primitives::rpc::replay::{
    ReplayApproveResponse, ReplayCancelResponse, ReplayCreateResponse, ReplayExecuteResponse,
    ReplayListResponse, ReplayOperation, ReplayPreviewResponse, ReplayStatusResponse,
    ReplaySubmitResponse,
};
use sinex_primitives::{Result, SinexError};

pub async fn handle_replay_create_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let scope_val = params.require_value("scope")?.clone();
    let scope: ReplayScope = serde_json::from_value(scope_val).map_err(|error| {
        SinexError::serialization("Invalid replay scope payload").with_std_error(&error)
    })?;

    let operation = client
        .plan(auth.replay_actor(), scope)
        .await
        .map_err(|error| {
            SinexError::service("failed to plan replay operation").with_source(error)
        })?;
    serde_json::to_value(ReplayCreateResponse {
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.create_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_preview_operation(
    client: &ReplayControlClient,
    params: Value,
    _auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let (operation, preview) = client.preview(operation_id).await.map_err(|error| {
        SinexError::service("failed to preview replay operation").with_source(error)
    })?;
    serde_json::to_value(ReplayPreviewResponse {
        operation: into_replay_operation(operation)?,
        preview,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.preview_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_approve_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let operation = client
        .approve(operation_id, auth.replay_actor())
        .await
        .map_err(|error| {
            SinexError::service("failed to approve replay operation").with_source(error)
        })?;
    serde_json::to_value(ReplayApproveResponse {
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.approve_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_execute_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let dry_run = params.optional_bool("dry_run")?.unwrap_or(false);
    let operation = client
        .execute(operation_id, auth.replay_actor(), dry_run)
        .await
        .map_err(|error| {
            SinexError::service("failed to execute replay operation").with_source(error)
        })?;
    serde_json::to_value(ReplayExecuteResponse {
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.execute_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_submit_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let operation = client
        .submit(operation_id, auth.replay_actor())
        .await
        .map_err(|error| {
            SinexError::service("failed to submit replay operation").with_source(error)
        })?;
    serde_json::to_value(ReplaySubmitResponse {
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.submit_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_cancel_operation(
    client: &ReplayControlClient,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let reason = params
        .optional_str("reason")?
        .map(std::string::ToString::to_string);
    let operation = client
        .cancel(operation_id, auth.replay_actor(), reason)
        .await
        .map_err(|error| {
            SinexError::service("failed to cancel replay operation").with_source(error)
        })?;
    serde_json::to_value(ReplayCancelResponse {
        cancelled: true,
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.cancel_operation response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_operation_status(
    client: &ReplayControlClient,
    params: Value,
    _auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let operation_id = params.require_uuid("operation_id")?;
    let operation = client.status(operation_id).await.map_err(|error| {
        SinexError::service("failed to fetch replay operation status").with_source(error)
    })?;
    serde_json::to_value(ReplayStatusResponse {
        operation: into_replay_operation(operation)?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.operation_status response")
            .with_std_error(&error)
    })
}

pub async fn handle_replay_list_operations(
    client: &ReplayControlClient,
    params: Value,
    _auth: &RpcAuthContext,
) -> Result<Value> {
    let params = RpcParams::new(&params);
    let state = params
        .optional_str("state")?
        .map(parse_replay_state)
        .transpose()?;
    let node = params.optional_str("node")?.map(String::from);
    let limit = params.optional_i64("limit")?;
    let operations = client.list(state, node, limit).await.map_err(|error| {
        SinexError::service("failed to list replay operations").with_source(error)
    })?;
    serde_json::to_value(ReplayListResponse {
        operations: operations
            .into_iter()
            .map(into_replay_operation)
            .collect::<Result<Vec<_>>>()?,
    })
    .map_err(|error| {
        SinexError::serialization("failed to serialize replay.list_operations response")
            .with_std_error(&error)
    })
}

fn into_replay_operation(operation: DbReplayOperation) -> Result<ReplayOperation> {
    serde_json::from_value(serde_json::to_value(operation).map_err(|error| {
        SinexError::serialization("failed to serialize replay operation into wire-compatible form")
            .with_std_error(&error)
    })?)
    .map_err(|error| {
        SinexError::serialization("failed to deserialize replay operation into RPC contract")
            .with_std_error(&error)
    })
}

pub(crate) fn parse_replay_state(value: &str) -> Result<ReplayState> {
    match value.to_lowercase().as_str() {
        "planning" => Ok(ReplayState::Planning),
        "previewed" => Ok(ReplayState::Previewed),
        "approved" => Ok(ReplayState::Approved),
        "executing" => Ok(ReplayState::Executing),
        "cancelling" => Ok(ReplayState::Cancelling),
        "committing" => Ok(ReplayState::Committing),
        "completed" => Ok(ReplayState::Completed),
        "failed" => Ok(ReplayState::Failed),
        "cancelled" => Ok(ReplayState::Cancelled),
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
            ("planning", ReplayState::Planning),
            ("PREVIEWED", ReplayState::Previewed),
            ("Approved", ReplayState::Approved),
            ("cancelling", ReplayState::Cancelling),
        ];
        for (input, expected) in states {
            assert_eq!(parse_replay_state(input).unwrap(), expected);
        }
        assert!(parse_replay_state("unknown").is_err());
        Ok(())
    }
}
