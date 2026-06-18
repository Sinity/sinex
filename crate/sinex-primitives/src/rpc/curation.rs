//! Curation proposal and judgment RPC contracts.

use crate::JsonValue;
use crate::rpc::ops::Operation;
use crate::events::{
    Event, SourceMaterial,
    payloads::{CurationFinalizedPayload, CurationJudgmentPayload},
};
use crate::query::EventQueryResult;
use crate::{Id, Timestamp, Uuid};

use serde::{Deserialize, Serialize};

use super::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};

pub const CURATION_PROPOSALS_LIST_METHOD: RpcMethod<
    CurationListProposalsRequest,
    EventQueryResult,
> = RpcMethod::new(
    methods::CURATION_PROPOSALS_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Curation,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const CURATION_JUDGMENTS_RECORD_METHOD: RpcMethod<
    CurationRecordJudgmentRequest,
    CurationRecordJudgmentResponse,
> = RpcMethod::new(
    methods::CURATION_JUDGMENTS_RECORD,
    RpcRole::Write,
    RpcDomain::Curation,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const CURATION_DUPLICATE_CANDIDATES_LIST_METHOD: RpcMethod<
    CurationListDuplicateCandidatesRequest,
    CurationListDuplicateCandidatesResponse,
> = RpcMethod::new(
    methods::CURATION_DUPLICATE_CANDIDATES_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Curation,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const CURATION_DUPLICATE_JUDGMENTS_RECORD_METHOD: RpcMethod<
    CurationRecordDuplicateJudgmentRequest,
    CurationRecordDuplicateJudgmentResponse,
> = RpcMethod::new(
    methods::CURATION_DUPLICATE_JUDGMENTS_RECORD,
    RpcRole::Write,
    RpcDomain::Curation,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const CURATION_FINALIZE_METHOD: RpcMethod<CurationFinalizeRequest, CurationFinalizeResponse> =
    RpcMethod::new(
        methods::CURATION_FINALIZE,
        RpcRole::Write,
        RpcDomain::Curation,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationListProposalsRequest {
    #[serde(default = "default_proposal_status")]
    pub status: String,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

impl Default for CurationListProposalsRequest {
    fn default() -> Self {
        Self {
            status: default_proposal_status(),
            limit: default_limit(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationRecordJudgmentRequest {
    pub proposal_event_id: String,
    pub actor_kind: crate::events::payloads::CurationJudgmentActorKind,
    #[serde(default)]
    pub actor_id: Option<String>,
    pub decision: crate::events::payloads::CurationJudgmentDecision,
    #[serde(default)]
    pub corrected_payload: Option<serde_json::Value>,
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub authorization_context: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationRecordJudgmentResponse {
    pub judgment: CurationJudgmentPayload,
    pub event: Event<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationListDuplicateCandidatesRequest {
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub event_type: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default = "default_duplicate_events_per_cluster")]
    pub events_per_cluster: i64,
}

impl Default for CurationListDuplicateCandidatesRequest {
    fn default() -> Self {
        Self {
            source: None,
            event_type: None,
            limit: default_limit(),
            events_per_cluster: default_duplicate_events_per_cluster(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationListDuplicateCandidatesResponse {
    pub clusters: Vec<CurationDuplicateCandidateCluster>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationDuplicateCandidateCluster {
    /// Replay-stable key for this candidate cluster.
    pub cluster_id: String,
    pub source: String,
    pub event_type: String,
    /// Logical key value from the event `equivalence_key` column.
    pub equivalence_key: String,
    pub event_count: i64,
    pub material_count: i64,
    pub events: Vec<CurationDuplicateCandidateEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationDuplicateCandidateEvent {
    pub event_id: Id<Event>,
    pub source_material_id: Id<SourceMaterial>,
    pub ts_orig: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurationDuplicateAction {
    Merge,
    Prefer,
    Ignore,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationRecordDuplicateJudgmentRequest {
    pub source: String,
    pub event_type: String,
    pub equivalence_key: String,
    pub event_ids: Vec<Uuid>,
    pub action: CurationDuplicateAction,
    #[serde(default)]
    pub preferred_event_id: Option<Uuid>,
    pub actor_kind: crate::events::payloads::CurationJudgmentActorKind,
    #[serde(default)]
    pub actor_id: Option<String>,
    #[serde(default)]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationRecordDuplicateJudgmentResponse {
    pub proposal: crate::events::payloads::CurationProposalPayload,
    pub proposal_event: Event<JsonValue>,
    pub judgment: CurationJudgmentPayload,
    pub judgment_event: Event<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationFinalizeRequest {
    pub judgment_event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationFinalizeResponse {
    pub finalized: CurationFinalizedPayload,
    pub event: Event<JsonValue>,
    pub operation: Operation,
}

fn default_proposal_status() -> String {
    "pending".to_string()
}

const fn default_limit() -> i64 {
    100
}

const fn default_duplicate_events_per_cluster() -> i64 {
    10
}
