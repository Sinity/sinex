//! Semantic epoch and shadow-lane RPC handlers.

use sinex_db::repositories::{CreateSemanticEpoch, CreateSemanticLane, DbPoolExt};
use sinex_primitives::rpc::semantic::{
    SemanticEpochCreateRequest, SemanticEpochListRequest, SemanticEpochListResponse,
    SemanticEpochRecordResponse, SemanticLaneCreateRequest,
    SemanticLaneDiffRecordEntityRelationRequest, SemanticLaneDiffRecordResponse,
    SemanticLaneDiffsListRequest, SemanticLaneDiffsListResponse, SemanticLaneDiscardRequest,
    SemanticLaneDiscardResponse, SemanticLaneListRequest, SemanticLaneListResponse,
    SemanticLaneOutputsListRequest, SemanticLaneOutputsListResponse,
    SemanticLaneOutputsSeedCanonicalGraphRequest, SemanticLaneOutputsSeedCanonicalGraphResponse,
    SemanticLaneOutputsSeedEntityEventsRequest, SemanticLaneOutputsSeedEntityEventsResponse,
    SemanticLaneOutputsWriteRequest, SemanticLaneOutputsWriteResponse, SemanticLaneRecordResponse,
    SemanticLaneSetStatusRequest,
};
use sinex_primitives::{
    Result, SemanticEpochRecord, SemanticLaneRecord, SemanticLaneStatus, SemanticScope, SinexError,
    Timestamp, Uuid, diff_entity_relation_lanes,
};
use sqlx::PgPool;

use crate::api::rpc_server::RpcAuthContext;

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
) -> Result<SemanticLaneDiscardResponse> {
    let (lane, discarded_outputs) = pool
        .semantic()
        .discard_lane_outputs(req.lane_id, Timestamp::now())
        .await?;
    Ok(SemanticLaneDiscardResponse {
        lane: serde_json::to_value(lane).map_err(serialize_error)?,
        discarded_outputs,
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

pub async fn handle_semantic_lane_outputs_write(
    pool: &PgPool,
    req: SemanticLaneOutputsWriteRequest,
) -> Result<SemanticLaneOutputsWriteResponse> {
    let written = pool
        .semantic()
        .write_entity_relation_outputs(req.lane_id, &req.outputs)
        .await?;
    Ok(SemanticLaneOutputsWriteResponse {
        lane_id: req.lane_id,
        written,
    })
}

pub async fn handle_semantic_lane_outputs_seed_canonical_graph(
    pool: &PgPool,
    req: SemanticLaneOutputsSeedCanonicalGraphRequest,
) -> Result<SemanticLaneOutputsSeedCanonicalGraphResponse> {
    let written = pool
        .semantic()
        .seed_entity_relation_outputs_from_canonical_graph(req.lane_id)
        .await?;
    Ok(SemanticLaneOutputsSeedCanonicalGraphResponse {
        lane_id: req.lane_id,
        written,
    })
}

pub async fn handle_semantic_lane_outputs_seed_entity_events(
    pool: &PgPool,
    req: SemanticLaneOutputsSeedEntityEventsRequest,
) -> Result<SemanticLaneOutputsSeedEntityEventsResponse> {
    let written = pool
        .semantic()
        .seed_entity_relation_outputs_from_event_scope(req.lane_id)
        .await?;
    Ok(SemanticLaneOutputsSeedEntityEventsResponse {
        lane_id: req.lane_id,
        written,
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

pub async fn handle_semantic_lane_diff_record_entity_relation(
    pool: &PgPool,
    req: SemanticLaneDiffRecordEntityRelationRequest,
) -> Result<SemanticLaneDiffRecordResponse> {
    let baseline_lane = pool.semantic().get_lane(req.baseline_lane_id).await?;
    let candidate_lane = pool.semantic().get_lane(req.candidate_lane_id).await?;
    let baseline_scope = parse_scope(&baseline_lane.scope)?;
    let candidate_scope = parse_scope(&candidate_lane.scope)?;
    if baseline_scope.input_set_hash != candidate_scope.input_set_hash {
        return Err(SinexError::validation(
            "semantic.lane_diffs.record_entity_relation: lane input_set_hash values differ",
        )
        .with_context("baseline_lane_id", req.baseline_lane_id.to_string())
        .with_context("candidate_lane_id", req.candidate_lane_id.to_string()));
    }

    let baseline_outputs = pool
        .semantic()
        .read_entity_relation_outputs(req.baseline_lane_id)
        .await?;
    let candidate_outputs = pool
        .semantic()
        .read_entity_relation_outputs(req.candidate_lane_id)
        .await?;
    let report = diff_entity_relation_lanes(
        baseline_lane.candidate_epoch_id,
        candidate_lane.candidate_epoch_id,
        candidate_scope.input_set_hash,
        &baseline_outputs,
        &candidate_outputs,
        req.max_examples,
    );
    let diff = pool
        .semantic()
        .record_entity_relation_diff(
            req.diff_id.unwrap_or_else(Uuid::now_v7),
            req.baseline_lane_id,
            req.candidate_lane_id,
            &report,
        )
        .await?;
    let candidate_lane = if req.mark_candidate_compared {
        Some(
            pool.semantic()
                .set_lane_status(
                    req.candidate_lane_id,
                    SemanticLaneStatus::Compared,
                    Some(Timestamp::now()),
                )
                .await?,
        )
    } else {
        None
    };

    Ok(SemanticLaneDiffRecordResponse {
        diff: serde_json::to_value(diff).map_err(serialize_error)?,
        candidate_lane: candidate_lane
            .map(serde_json::to_value)
            .transpose()
            .map_err(serialize_error)?,
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

fn parse_scope(scope: &serde_json::Value) -> Result<SemanticScope> {
    serde_json::from_value(scope.clone()).map_err(|error| {
        SinexError::serialization("deserialize semantic lane scope").with_std_error(&error)
    })
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
