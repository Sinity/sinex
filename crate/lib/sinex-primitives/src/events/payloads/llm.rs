//! LLM prompt/router/budget event payloads.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::llm::{
    BudgetLedgerStatus, ModelRoute, PromptPrivacyClass, PromptTemplateStatus, RoutingDecision,
};
use crate::{Timestamp, Uuid};
use sinex_macros::EventPayload;

/// Durable prompt-template registry entry.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "llm",
    event_type = "llm.prompt_template.registered",
    version = "1.0.0"
)]
pub struct LlmPromptTemplateRegisteredPayload {
    pub prompt_id: String,
    pub version: String,
    pub purpose: String,
    pub template_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_hash: Option<String>,
    pub privacy_class: PromptPrivacyClass,
    pub owner: String,
    pub status: PromptTemplateStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_storage_ref: Option<String>,
}

/// Durable routing policy registry entry.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "llm",
    event_type = "llm.routing_policy.registered",
    version = "1.0.0"
)]
pub struct LlmRoutingPolicyRegisteredPayload {
    pub policy_id: String,
    pub task_kind: String,
    pub prompt_id: String,
    pub prompt_version: String,
    pub fallback_order: Vec<ModelRoute>,
    pub replay_policy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privacy_policy_ref: Option<String>,
    pub active: bool,
}

/// Durable record of the router decision used before a model effect attempt.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "llm", event_type = "llm.routing.decision", version = "1.0.0")]
pub struct LlmRoutingDecisionPayload {
    pub routing_decision_id: Uuid,
    pub policy_id: String,
    pub task_kind: String,
    pub prompt_id: String,
    pub prompt_version: String,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experiment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_key: Option<String>,
    pub decision_reason: String,
}

impl From<RoutingDecision> for LlmRoutingDecisionPayload {
    fn from(decision: RoutingDecision) -> Self {
        Self {
            routing_decision_id: decision.routing_decision_id,
            policy_id: decision.policy_id,
            task_kind: decision.task_kind,
            prompt_id: decision.prompt_id,
            prompt_version: decision.prompt_version,
            provider: decision.provider,
            model: decision.model,
            experiment_id: decision.experiment_id,
            bucket_key: decision.bucket_key,
            decision_reason: decision.decision_reason,
        }
    }
}

/// Budget ledger row for a model-effect attempt or privacy/policy rejection.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "llm", event_type = "llm.budget.ledger", version = "1.0.0")]
pub struct LlmBudgetLedgerPayload {
    pub budget_ledger_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_decision_id: Option<Uuid>,
    pub caller: String,
    pub task_kind: String,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_estimate_microusd: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_ms: Option<i64>,
    pub status: BudgetLedgerStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<String>,
    pub recorded_at: Timestamp,
}
