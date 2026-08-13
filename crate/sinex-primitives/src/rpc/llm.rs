//! LLM prompt/router/budget read RPC contracts.

use serde::{Deserialize, Serialize};

use crate::events::payloads::LlmBudgetLedgerPayload;
use crate::llm::{ModelTaskRequest, RoutingDecision, RoutingPolicyRecord};
use crate::query::EventQueryResult;
use crate::views::CaveatView;
use std::collections::BTreeMap;

use super::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};

pub const LLM_PROMPTS_LIST_METHOD: RpcMethod<LlmPromptsListRequest, EventQueryResult> =
    RpcMethod::new(
        methods::LLM_PROMPTS_LIST,
        RpcRole::ReadOnly,
        RpcDomain::Llm,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const LLM_ROUTE_EXPLAIN_METHOD: RpcMethod<LlmRouteExplainRequest, LlmRouteExplainResponse> =
    RpcMethod::new(
        methods::LLM_ROUTE_EXPLAIN,
        RpcRole::ReadOnly,
        RpcDomain::Llm,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const LLM_BUDGET_REPORT_METHOD: RpcMethod<LlmBudgetReportRequest, LlmBudgetReportResponse> =
    RpcMethod::new(
        methods::LLM_BUDGET_REPORT,
        RpcRole::ReadOnly,
        RpcDomain::Llm,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const LLM_EMBEDDING_ESTIMATE_METHOD: RpcMethod<
    LlmEmbeddingEstimateRequest,
    LlmEmbeddingEstimateResponse,
> = RpcMethod::new(
    methods::LLM_EMBEDDING_ESTIMATE,
    RpcRole::ReadOnly,
    RpcDomain::Llm,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmPromptsListRequest {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

impl Default for LlmPromptsListRequest {
    fn default() -> Self {
        Self {
            status: None,
            limit: default_limit(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmRouteExplainRequest {
    pub request: ModelTaskRequest,
    pub policy: RoutingPolicyRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmRouteExplainResponse {
    pub decision: RoutingDecision,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmBudgetReportRequest {
    #[serde(default = "default_limit")]
    pub limit: i64,
}

impl Default for LlmBudgetReportRequest {
    fn default() -> Self {
        Self {
            limit: default_limit(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmBudgetReportResponse {
    pub rows: Vec<LlmBudgetLedgerPayload>,
    pub total_rows: usize,
    pub success_count: usize,
    pub failure_count: usize,
    pub rejected_count: usize,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub cost_estimate_microusd: i64,
    pub runtime_ms: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<CaveatView>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LlmEmbeddingEstimateRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmEmbeddingEstimateResponse {
    pub scanned_events: u64,
    pub scanned_materials: u64,
    pub eligible_events: u64,
    pub quarantined_events: u64,
    pub skipped_events: u64,
    pub estimated_tokens: u64,
    pub estimated_cost_microusd: u64,
    pub selected_event_types: BTreeMap<String, u64>,
    pub quarantined_by_reason: BTreeMap<String, u64>,
    pub model: String,
    pub model_allowed: bool,
}

const fn default_limit() -> i64 {
    100
}
