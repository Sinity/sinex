//! Prompt registry, deterministic model routing, and budget-ledger primitives.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Result, SinexError, Uuid};

/// Prompt lifecycle state in the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PromptTemplateStatus {
    Draft,
    Active,
    Shadow,
    Retired,
}

/// Privacy class declared by a prompt/template owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PromptPrivacyClass {
    Public,
    Internal,
    Sensitive,
    Secret,
}

/// Privacy routing result for a model-task attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ModelPrivacyRoute {
    RemoteAllowed,
    ForceLocal,
    RedactRequired,
    Disallowed,
}

/// Immutable prompt template registry row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PromptTemplateRecord {
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

/// Provider/model candidate admitted by a routing policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ModelRoute {
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
    pub is_local: bool,
}

impl ModelRoute {
    #[must_use]
    pub fn remote(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            tier: None,
            is_local: false,
        }
    }

    #[must_use]
    pub fn local(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            tier: None,
            is_local: true,
        }
    }
}

/// Optional canary/experiment routing configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RoutingRollout {
    pub experiment_id: String,
    pub canary_percentage: u8,
    pub canary_model: ModelRoute,
}

/// Active model-routing policy for one task kind and prompt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RoutingPolicyRecord {
    pub policy_id: String,
    pub task_kind: String,
    pub prompt_id: String,
    pub prompt_version: String,
    pub fallback_order: Vec<ModelRoute>,
    pub replay_policy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privacy_policy_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollout: Option<RoutingRollout>,
    pub active: bool,
}

/// Caller request for a shared routing decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ModelTaskRequest {
    pub task_kind: String,
    pub prompt_id: String,
    pub input_hash: String,
    pub privacy_route: ModelPrivacyRoute,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_key: Option<String>,
}

/// Reproducible prompt/model/provider selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RoutingDecision {
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

/// Budget ledger outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BudgetLedgerStatus {
    Success,
    Failure,
    Rejected,
}

/// Stable BLAKE3 hash for a prompt body or schema contract.
#[must_use]
pub fn hash_prompt_material(material: &str) -> String {
    blake3::hash(material.as_bytes()).to_hex().to_string()
}

// =============================================================================
// Model-effect cache — deterministic record-and-replay for LLM calls
// =============================================================================

/// Replay policy governing whether a recorded model effect is reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReplayPolicy {
    /// Reuse a previously recorded effect when the input hashes match.
    ReuseRecorded,
    /// Require a recorded effect; fail if none exists.
    FailIfMissing,
    /// Always re-evaluate, ignoring any recorded effect.
    ExplicitReevaluate,
}

/// Identifies a specific model-effect invocation for caching and replay.
///
/// Two requests with identical hashes and the same replay policy (or
/// `ReuseRecorded` in effect) can share a recorded response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ModelEffectRequest {
    /// Provider name (e.g. `anthropic`, `openai`, `google`).
    pub provider: String,
    /// Model identifier (e.g. `claude-opus-4-7`).
    pub model: String,
    /// BLAKE3 hash of the prompt template body.
    pub prompt_hash: String,
    /// BLAKE3 hash of the JSON Schema for structured output, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_hash: Option<String>,
    /// BLAKE3 hash of the serialized input payload.
    pub input_hash: String,
}

impl ModelEffectRequest {
    /// Compute a composite key from all hash inputs for dedup lookups.
    #[must_use]
    pub fn composite_key(&self) -> String {
        let schema = self.schema_hash.as_deref().unwrap_or("");
        let material = format!(
            "{}|{}|{}|{}|{}",
            self.provider, self.model, self.prompt_hash, schema, self.input_hash
        );
        blake3::hash(material.as_bytes()).to_hex().to_string()
    }
}

/// A recorded model effect — the immutable record of a completed LLM call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ModelEffectRecord {
    /// Unique effect ID (`UUIDv7`).
    pub effect_id: String,
    /// The request that produced this effect.
    pub request: ModelEffectRequest,
    /// The recorded output (serialized).
    pub output: String,
    /// The replay policy that was active when recorded.
    pub recorded_policy: ReplayPolicy,
    /// ISO-8601 timestamp of recording.
    pub recorded_at: String,
    /// Provenance: which node/automaton recorded this.
    pub recorded_by: String,
    /// BLAKE3 hash of the output for integrity verification.
    pub output_hash: String,
}

impl ModelEffectRecord {
    /// Create a new record from a request and raw output.
    #[must_use]
    pub fn new(
        request: ModelEffectRequest,
        output: impl Into<String>,
        policy: ReplayPolicy,
        recorded_by: impl Into<String>,
    ) -> Self {
        let output: String = output.into();
        let output_hash = blake3::hash(output.as_bytes()).to_hex().to_string();
        Self {
            effect_id: Uuid::now_v7().to_string(),
            request,
            output,
            output_hash,
            recorded_policy: policy,
            recorded_at: time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            recorded_by: recorded_by.into(),
        }
    }
}

/// Hash an arbitrary input payload for use in a `ModelEffectRequest`.
#[must_use]
pub fn hash_model_input(payload: &str) -> String {
    blake3::hash(payload.as_bytes()).to_hex().to_string()
}

/// Determine whether a recorded effect can satisfy a request,
/// given the active replay policy.
#[must_use]
pub fn can_replay(
    request: &ModelEffectRequest,
    record: &ModelEffectRecord,
    policy: ReplayPolicy,
) -> bool {
    match policy {
        ReplayPolicy::ExplicitReevaluate => false,
        ReplayPolicy::ReuseRecorded | ReplayPolicy::FailIfMissing => {
            request.composite_key() == record.request.composite_key()
        }
    }
}

/// Return the deterministic rollout bucket in `[0, 99]`.
#[must_use]
pub fn routing_bucket(
    policy_id: &str,
    experiment_id: &str,
    bucket_key: &str,
    task_kind: &str,
) -> u8 {
    let material = format!("{policy_id}\0{experiment_id}\0{bucket_key}\0{task_kind}");
    let hash = blake3::hash(material.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    (u64::from_le_bytes(bytes) % 100) as u8
}

/// Decide the prompt/model/provider route from a policy and bucket key.
pub fn decide_route(
    request: &ModelTaskRequest,
    policy: &RoutingPolicyRecord,
) -> Result<RoutingDecision> {
    if !policy.active {
        return Err(SinexError::validation("routing policy is inactive")
            .with_context("policy_id", &policy.policy_id));
    }
    if request.task_kind != policy.task_kind {
        return Err(
            SinexError::validation("routing request task does not match policy")
                .with_context("request_task_kind", &request.task_kind)
                .with_context("policy_task_kind", &policy.task_kind),
        );
    }
    if request.prompt_id != policy.prompt_id {
        return Err(
            SinexError::validation("routing request prompt does not match policy")
                .with_context("request_prompt_id", &request.prompt_id)
                .with_context("policy_prompt_id", &policy.prompt_id),
        );
    }

    match request.privacy_route {
        ModelPrivacyRoute::Disallowed => {
            return Err(SinexError::validation(
                "privacy policy rejected model routing before model effect attempt",
            )
            .with_context("task_kind", &request.task_kind)
            .with_context("prompt_id", &request.prompt_id));
        }
        ModelPrivacyRoute::RedactRequired => {
            return Err(SinexError::validation(
                "privacy policy requires redacted input before model routing",
            )
            .with_context("task_kind", &request.task_kind)
            .with_context("prompt_id", &request.prompt_id));
        }
        ModelPrivacyRoute::RemoteAllowed | ModelPrivacyRoute::ForceLocal => {}
    }

    let bucket_key = request
        .bucket_key
        .clone()
        .unwrap_or_else(|| request.input_hash.clone());
    if let Some(rollout) = &policy.rollout {
        let bucket = routing_bucket(
            &policy.policy_id,
            &rollout.experiment_id,
            &bucket_key,
            &request.task_kind,
        );
        if bucket < rollout.canary_percentage
            && route_allowed_by_privacy(&rollout.canary_model, request.privacy_route)
        {
            return Ok(decision_from_route(
                request,
                policy,
                &rollout.canary_model,
                Some(rollout.experiment_id.clone()),
                Some(bucket_key),
                format!("canary bucket {bucket} < {}", rollout.canary_percentage),
            ));
        }
    }

    let (route_index, route) = policy
        .fallback_order
        .iter()
        .enumerate()
        .find(|(_, route)| route_allowed_by_privacy(route, request.privacy_route))
        .ok_or_else(|| {
            SinexError::validation("routing policy has no privacy-eligible model")
                .with_context("policy_id", &policy.policy_id)
                .with_context("privacy_route", format!("{:?}", request.privacy_route))
        })?;

    Ok(decision_from_route(
        request,
        policy,
        route,
        None,
        Some(bucket_key),
        format!("fallback_order[{route_index}] privacy-eligible route"),
    ))
}

fn route_allowed_by_privacy(route: &ModelRoute, privacy_route: ModelPrivacyRoute) -> bool {
    match privacy_route {
        ModelPrivacyRoute::RemoteAllowed => true,
        ModelPrivacyRoute::ForceLocal => route.is_local,
        ModelPrivacyRoute::RedactRequired | ModelPrivacyRoute::Disallowed => false,
    }
}

fn decision_from_route(
    request: &ModelTaskRequest,
    policy: &RoutingPolicyRecord,
    route: &ModelRoute,
    experiment_id: Option<String>,
    bucket_key: Option<String>,
    decision_reason: String,
) -> RoutingDecision {
    RoutingDecision {
        routing_decision_id: Uuid::now_v7(),
        policy_id: policy.policy_id.clone(),
        task_kind: request.task_kind.clone(),
        prompt_id: policy.prompt_id.clone(),
        prompt_version: policy.prompt_version.clone(),
        provider: route.provider.clone(),
        model: route.model.clone(),
        experiment_id,
        bucket_key,
        decision_reason,
    }
}
