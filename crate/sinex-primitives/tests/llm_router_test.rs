use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{LlmBudgetLedgerPayload, LlmRoutingDecisionPayload};
use sinex_primitives::llm::{
    BudgetLedgerStatus, ModelPrivacyRoute, ModelRoute, ModelTaskRequest, RoutingPolicyRecord,
    RoutingRollout, decide_route, hash_prompt_material, routing_bucket,
};
use sinex_primitives::{Timestamp, Uuid};
use xtask::sandbox::prelude::*;

fn request(privacy_route: ModelPrivacyRoute) -> ModelTaskRequest {
    ModelTaskRequest {
        task_kind: "entity-extraction".to_string(),
        prompt_id: "extract-entities".to_string(),
        input_hash: "input-hash-1".to_string(),
        privacy_route,
        bucket_key: Some("stable-object-key".to_string()),
    }
}

fn policy() -> RoutingPolicyRecord {
    RoutingPolicyRecord {
        policy_id: "entity-extraction-v1".to_string(),
        task_kind: "entity-extraction".to_string(),
        prompt_id: "extract-entities".to_string(),
        prompt_version: "2026-05-18".to_string(),
        fallback_order: vec![
            ModelRoute::remote("remote-provider", "remote-model"),
            ModelRoute::local("local-provider", "local-model"),
        ],
        replay_policy: "recorded_effect_required".to_string(),
        privacy_policy_ref: Some("privacy.llm.default".to_string()),
        rollout: None,
        active: true,
    }
}

#[sinex_test]
async fn prompt_material_hash_is_stable() -> TestResult<()> {
    let first = hash_prompt_material("extract entities from {{context}}");
    let second = hash_prompt_material("extract entities from {{context}}");

    assert_eq!(first, second);
    assert_eq!(first.len(), 64);
    Ok(())
}

#[sinex_test]
async fn routing_bucket_is_reproducible() -> TestResult<()> {
    let first = routing_bucket("policy", "experiment", "object-key", "task");
    let second = routing_bucket("policy", "experiment", "object-key", "task");

    assert_eq!(first, second);
    assert!(first < 100);
    Ok(())
}

#[sinex_test]
async fn router_uses_first_remote_route_when_privacy_allows_remote() -> TestResult<()> {
    let decision = decide_route(&request(ModelPrivacyRoute::RemoteAllowed), &policy())
        .expect("remote route should be selected");

    assert_eq!(decision.provider, "remote-provider");
    assert_eq!(decision.model, "remote-model");
    assert_eq!(decision.prompt_version, "2026-05-18");
    Ok(())
}

#[sinex_test]
async fn router_forces_local_route_when_privacy_requires_local() -> TestResult<()> {
    let decision = decide_route(&request(ModelPrivacyRoute::ForceLocal), &policy())
        .expect("local route should be selected");

    assert_eq!(decision.provider, "local-provider");
    assert_eq!(decision.model, "local-model");
    assert_eq!(
        decision.decision_reason,
        "fallback_order[1] privacy-eligible route"
    );
    Ok(())
}

#[sinex_test]
async fn router_rejects_disallowed_privacy_before_model_effect() -> TestResult<()> {
    let error = decide_route(&request(ModelPrivacyRoute::Disallowed), &policy())
        .expect_err("disallowed privacy should reject routing");

    assert!(
        error
            .to_string()
            .contains("privacy policy rejected model routing")
    );
    Ok(())
}

#[sinex_test]
async fn canary_route_is_deterministic_from_bucket_key() -> TestResult<()> {
    let mut policy = policy();
    policy.rollout = Some(RoutingRollout {
        experiment_id: "exp-a".to_string(),
        canary_percentage: 100,
        canary_model: ModelRoute::remote("canary-provider", "canary-model"),
    });

    let first = decide_route(&request(ModelPrivacyRoute::RemoteAllowed), &policy)
        .expect("canary route should be selected");
    let second = decide_route(&request(ModelPrivacyRoute::RemoteAllowed), &policy)
        .expect("canary route should be selected");

    assert_eq!(first.provider, "canary-provider");
    assert_eq!(first.model, second.model);
    assert_eq!(first.experiment_id, Some("exp-a".to_string()));
    Ok(())
}

#[sinex_test]
async fn routing_decision_payload_links_budget_ledger_records() -> TestResult<()> {
    let decision = decide_route(&request(ModelPrivacyRoute::RemoteAllowed), &policy())
        .expect("route should be selected");
    let decision_id = decision.routing_decision_id;
    let decision_payload = LlmRoutingDecisionPayload::from(decision);
    let ledger = LlmBudgetLedgerPayload {
        budget_ledger_id: Uuid::now_v7(),
        routing_decision_id: Some(decision_id),
        caller: "test-caller".to_string(),
        task_kind: "entity-extraction".to_string(),
        provider: decision_payload.provider.clone(),
        model: decision_payload.model.clone(),
        prompt_tokens: Some(12),
        completion_tokens: Some(7),
        cost_estimate_microusd: Some(42),
        runtime_ms: Some(123),
        status: BudgetLedgerStatus::Success,
        failure_class: None,
        recorded_at: Timestamp::now(),
    };

    assert_eq!(LlmRoutingDecisionPayload::SOURCE.as_str(), "llm");
    assert_eq!(
        LlmRoutingDecisionPayload::EVENT_TYPE.as_str(),
        "llm.routing.decision"
    );
    assert_eq!(ledger.routing_decision_id, Some(decision_id));
    assert_eq!(LlmBudgetLedgerPayload::SOURCE.as_str(), "llm");
    assert_eq!(
        LlmBudgetLedgerPayload::EVENT_TYPE.as_str(),
        "llm.budget.ledger"
    );
    Ok(())
}
