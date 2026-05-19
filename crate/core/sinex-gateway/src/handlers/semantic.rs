//! Semantic epoch and shadow-lane RPC handlers.

use sinex_db::repositories::{CreateSemanticEpoch, CreateSemanticLane, DbPoolExt};
use sinex_primitives::rpc::semantic::{
    SemanticEpochCreateRequest, SemanticEpochListRequest, SemanticEpochListResponse,
    SemanticEpochRecordResponse, SemanticLaneCreateRequest, SemanticLaneDiffsListRequest,
    SemanticLaneDiffsListResponse, SemanticLaneDiscardRequest, SemanticLaneListRequest,
    SemanticLaneListResponse, SemanticLaneOutputsListRequest, SemanticLaneOutputsListResponse,
    SemanticLaneRecordResponse, SemanticLaneSetStatusRequest,
};
use sinex_primitives::{
    Result, SemanticEpochRecord, SemanticLaneRecord, SemanticLaneStatus, SinexError, Timestamp,
    Uuid,
};
use sqlx::PgPool;

use crate::rpc_server::RpcAuthContext;

pub async fn handle_semantic_epoch_create(
    pool: &PgPool,
    req: SemanticEpochCreateRequest,
    auth: &RpcAuthContext,
) -> Result<SemanticEpochRecordResponse> {
    validate_non_empty("semantic.epochs.create", "name", &req.name)?;
    validate_non_empty("semantic.epochs.create", "config_hash", &req.config_hash)?;
    validate_scope(
        "semantic.epochs.create",
        req.scope.input_ids.len(),
        &req.scope.input_set_hash,
    )?;

    let epoch = SemanticEpochRecord {
        epoch_id: req.epoch_id.unwrap_or_else(Uuid::now_v7),
        name: req.name,
        scope: req.scope,
        code_ref: req.code_ref,
        config_hash: req.config_hash,
        components: req.components,
        prompt_set_hash: req.prompt_set_hash,
        model_config_hash: req.model_config_hash,
    };
    let created = pool
        .semantic()
        .create_epoch(CreateSemanticEpoch {
            epoch,
            created_by: req
                .created_by
                .unwrap_or_else(|| auth.actor_id().to_string()),
            operation_id: req.operation_id,
            supersedes_epoch_id: req.supersedes_epoch_id,
        })
        .await?;

    Ok(SemanticEpochRecordResponse {
        epoch: serde_json::to_value(created).map_err(serialize_error)?,
    })
}

pub async fn handle_semantic_epoch_list(
    pool: &PgPool,
    req: SemanticEpochListRequest,
) -> Result<SemanticEpochListResponse> {
    let epochs = pool.semantic().list_epochs(req.limit).await?;
    Ok(SemanticEpochListResponse {
        epochs: serialize_records(epochs)?,
    })
}

pub async fn handle_semantic_lane_create(
    pool: &PgPool,
    req: SemanticLaneCreateRequest,
) -> Result<SemanticLaneRecordResponse> {
    validate_non_empty("semantic.lanes.create", "name", &req.name)?;
    validate_non_empty("semantic.lanes.create", "purpose", &req.purpose)?;
    validate_scope(
        "semantic.lanes.create",
        req.scope.input_ids.len(),
        &req.scope.input_set_hash,
    )?;

    let lane = SemanticLaneRecord {
        lane_id: req.lane_id.unwrap_or_else(Uuid::now_v7),
        name: req.name,
        kind: req.kind,
        base_epoch_id: req.base_epoch_id,
        candidate_epoch_id: req.candidate_epoch_id,
        scope: req.scope,
        status: SemanticLaneStatus::Planned,
        purpose: req.purpose,
    };
    let created = pool
        .semantic()
        .create_lane(CreateSemanticLane {
            lane,
            operation_id: req.operation_id,
            expires_at: req.expires_at,
        })
        .await?;

    Ok(SemanticLaneRecordResponse {
        lane: serde_json::to_value(created).map_err(serialize_error)?,
    })
}

pub async fn handle_semantic_lanes_list(
    pool: &PgPool,
    req: SemanticLaneListRequest,
) -> Result<SemanticLaneListResponse> {
    let lanes = pool.semantic().list_lanes(req.status, req.limit).await?;
    Ok(SemanticLaneListResponse {
        lanes: serialize_records(lanes)?,
    })
}

pub async fn handle_semantic_lane_set_status(
    pool: &PgPool,
    req: SemanticLaneSetStatusRequest,
) -> Result<SemanticLaneRecordResponse> {
    let lane = pool
        .semantic()
        .set_lane_status(req.lane_id, req.status, req.completed_at)
        .await?;
    Ok(SemanticLaneRecordResponse {
        lane: serde_json::to_value(lane).map_err(serialize_error)?,
    })
}

pub async fn handle_semantic_lane_discard(
    pool: &PgPool,
    req: SemanticLaneDiscardRequest,
) -> Result<SemanticLaneRecordResponse> {
    let completed_at = Some(Timestamp::now());
    let lane = pool
        .semantic()
        .set_lane_status(req.lane_id, SemanticLaneStatus::Discarded, completed_at)
        .await?;
    Ok(SemanticLaneRecordResponse {
        lane: serde_json::to_value(lane).map_err(serialize_error)?,
    })
}

pub async fn handle_semantic_lane_outputs_list(
    pool: &PgPool,
    req: SemanticLaneOutputsListRequest,
) -> Result<SemanticLaneOutputsListResponse> {
    let outputs = pool
        .semantic()
        .list_lane_outputs(req.lane_id, req.limit)
        .await?;
    Ok(SemanticLaneOutputsListResponse {
        lane_id: req.lane_id,
        outputs: serialize_records(outputs)?,
    })
}

pub async fn handle_semantic_lane_diffs_list(
    pool: &PgPool,
    req: SemanticLaneDiffsListRequest,
) -> Result<SemanticLaneDiffsListResponse> {
    let diffs = pool
        .semantic()
        .list_lane_diffs(req.lane_id, req.limit)
        .await?;
    Ok(SemanticLaneDiffsListResponse {
        lane_id: req.lane_id,
        diffs: serialize_records(diffs)?,
    })
}

fn validate_non_empty(method: &str, field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(SinexError::validation(format!(
            "{method}: {field} must not be empty"
        )));
    }
    Ok(())
}

fn validate_scope(method: &str, input_count: usize, input_set_hash: &str) -> Result<()> {
    if input_count == 0 {
        return Err(SinexError::validation(format!(
            "{method}: scope.input_ids must not be empty"
        )));
    }
    if input_set_hash.trim().is_empty() {
        return Err(SinexError::validation(format!(
            "{method}: scope.input_set_hash must not be empty"
        )));
    }
    Ok(())
}

fn serialize_records<T: serde::Serialize>(records: Vec<T>) -> Result<Vec<serde_json::Value>> {
    records
        .into_iter()
        .map(|record| serde_json::to_value(record).map_err(serialize_error))
        .collect()
}

fn serialize_error(error: serde_json::Error) -> SinexError {
    SinexError::serialization("semantic RPC response serialization failed").with_std_error(&error)
}
