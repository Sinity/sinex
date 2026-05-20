//! Semantic epoch and shadow-lane RPC contracts.

use serde::{Deserialize, Serialize};

use crate::{
    EntityRelationLaneOutputs, SemanticComponentVersion, SemanticLaneKind, SemanticLaneStatus,
    SemanticScope, Timestamp, Uuid,
};

use super::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};

pub const SEMANTIC_EPOCHS_CREATE_METHOD: RpcMethod<
    SemanticEpochCreateRequest,
    SemanticEpochRecordResponse,
> = RpcMethod::new(
    methods::SEMANTIC_EPOCHS_CREATE,
    RpcRole::Write,
    RpcDomain::Semantic,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const SEMANTIC_EPOCHS_LIST_METHOD: RpcMethod<
    SemanticEpochListRequest,
    SemanticEpochListResponse,
> = RpcMethod::new(
    methods::SEMANTIC_EPOCHS_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Semantic,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SEMANTIC_LANES_CREATE_METHOD: RpcMethod<
    SemanticLaneCreateRequest,
    SemanticLaneRecordResponse,
> = RpcMethod::new(
    methods::SEMANTIC_LANES_CREATE,
    RpcRole::Write,
    RpcDomain::Semantic,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const SEMANTIC_LANES_LIST_METHOD: RpcMethod<SemanticLaneListRequest, SemanticLaneListResponse> =
    RpcMethod::new(
        methods::SEMANTIC_LANES_LIST,
        RpcRole::ReadOnly,
        RpcDomain::Semantic,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const SEMANTIC_LANES_SET_STATUS_METHOD: RpcMethod<
    SemanticLaneSetStatusRequest,
    SemanticLaneRecordResponse,
> = RpcMethod::new(
    methods::SEMANTIC_LANES_SET_STATUS,
    RpcRole::Write,
    RpcDomain::Semantic,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const SEMANTIC_LANES_DISCARD_METHOD: RpcMethod<
    SemanticLaneDiscardRequest,
    SemanticLaneDiscardResponse,
> = RpcMethod::new(
    methods::SEMANTIC_LANES_DISCARD,
    RpcRole::Write,
    RpcDomain::Semantic,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const SEMANTIC_LANE_OUTPUTS_LIST_METHOD: RpcMethod<
    SemanticLaneOutputsListRequest,
    SemanticLaneOutputsListResponse,
> = RpcMethod::new(
    methods::SEMANTIC_LANE_OUTPUTS_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Semantic,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SEMANTIC_LANE_OUTPUTS_WRITE_METHOD: RpcMethod<
    SemanticLaneOutputsWriteRequest,
    SemanticLaneOutputsWriteResponse,
> = RpcMethod::new(
    methods::SEMANTIC_LANE_OUTPUTS_WRITE,
    RpcRole::Write,
    RpcDomain::Semantic,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const SEMANTIC_LANE_DIFFS_LIST_METHOD: RpcMethod<
    SemanticLaneDiffsListRequest,
    SemanticLaneDiffsListResponse,
> = RpcMethod::new(
    methods::SEMANTIC_LANE_DIFFS_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Semantic,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SEMANTIC_LANE_DIFFS_RECORD_ENTITY_RELATION_METHOD: RpcMethod<
    SemanticLaneDiffRecordEntityRelationRequest,
    SemanticLaneDiffRecordResponse,
> = RpcMethod::new(
    methods::SEMANTIC_LANE_DIFFS_RECORD_ENTITY_RELATION,
    RpcRole::Write,
    RpcDomain::Semantic,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticEpochCreateRequest {
    #[serde(default)]
    pub epoch_id: Option<Uuid>,
    pub name: String,
    pub scope: SemanticScope,
    #[serde(default)]
    pub code_ref: Option<String>,
    pub config_hash: String,
    #[serde(default)]
    pub components: Vec<SemanticComponentVersion>,
    #[serde(default)]
    pub prompt_set_hash: Option<String>,
    #[serde(default)]
    pub model_config_hash: Option<String>,
    #[serde(default)]
    pub created_by: Option<String>,
    #[serde(default)]
    pub operation_id: Option<Uuid>,
    #[serde(default)]
    pub supersedes_epoch_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticEpochListRequest {
    #[serde(default = "default_limit")]
    pub limit: i64,
}

impl Default for SemanticEpochListRequest {
    fn default() -> Self {
        Self {
            limit: default_limit(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneCreateRequest {
    #[serde(default)]
    pub lane_id: Option<Uuid>,
    pub name: String,
    pub kind: SemanticLaneKind,
    #[serde(default)]
    pub base_epoch_id: Option<Uuid>,
    pub candidate_epoch_id: Uuid,
    pub scope: SemanticScope,
    pub purpose: String,
    #[serde(default)]
    pub operation_id: Option<Uuid>,
    #[serde(default)]
    pub expires_at: Option<Timestamp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneListRequest {
    #[serde(default)]
    pub status: Option<SemanticLaneStatus>,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

impl Default for SemanticLaneListRequest {
    fn default() -> Self {
        Self {
            status: None,
            limit: default_limit(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneSetStatusRequest {
    pub lane_id: Uuid,
    pub status: SemanticLaneStatus,
    #[serde(default)]
    pub completed_at: Option<Timestamp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneDiscardRequest {
    pub lane_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneOutputsListRequest {
    pub lane_id: Uuid,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneDiffsListRequest {
    pub lane_id: Uuid,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneOutputsWriteRequest {
    pub lane_id: Uuid,
    pub outputs: EntityRelationLaneOutputs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneDiffRecordEntityRelationRequest {
    #[serde(default)]
    pub diff_id: Option<Uuid>,
    pub baseline_lane_id: Uuid,
    pub candidate_lane_id: Uuid,
    #[serde(default = "default_max_examples")]
    pub max_examples: usize,
    #[serde(default = "default_true")]
    pub mark_candidate_compared: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticEpochRecordResponse {
    pub epoch: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticEpochListResponse {
    pub epochs: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneRecordResponse {
    pub lane: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneDiscardResponse {
    pub lane: serde_json::Value,
    pub discarded_outputs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneListResponse {
    pub lanes: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneOutputsListResponse {
    pub lane_id: Uuid,
    pub outputs: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneOutputsWriteResponse {
    pub lane_id: Uuid,
    pub written: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneDiffsListResponse {
    pub lane_id: Uuid,
    pub diffs: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticLaneDiffRecordResponse {
    pub diff: serde_json::Value,
    pub candidate_lane: Option<serde_json::Value>,
}

const fn default_limit() -> i64 {
    100
}

const fn default_max_examples() -> usize {
    20
}

const fn default_true() -> bool {
    true
}
