//! Curation proposal and judgment RPC contracts.

use crate::JsonValue;
use crate::events::{
    Event,
    payloads::{CurationFinalizedPayload, CurationJudgmentPayload},
};
use crate::query::EventQueryResult;

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
pub struct CurationFinalizeRequest {
    pub judgment_event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurationFinalizeResponse {
    pub finalized: CurationFinalizedPayload,
    pub event: Event<JsonValue>,
}

fn default_proposal_status() -> String {
    "pending".to_string()
}

const fn default_limit() -> i64 {
    100
}
