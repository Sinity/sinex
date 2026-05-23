use sinex_db::DbPoolExt;
use sinex_gateway::handlers::{
    handle_llm_budget_report, handle_llm_prompts_list, handle_llm_route_explain,
};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    LlmBudgetLedgerPayload, LlmPromptTemplateRegisteredPayload,
};
use sinex_primitives::llm::{
    BudgetLedgerStatus, ModelPrivacyRoute, ModelRoute, ModelTaskRequest, PromptPrivacyClass,
    PromptTemplateStatus, RoutingPolicyRecord,
};
use sinex_primitives::query::EventQueryResult;
use sinex_primitives::rpc::llm::{
    LlmBudgetReportRequest, LlmPromptsListRequest, LlmRouteExplainRequest,
};
use sinex_primitives::{Timestamp, Uuid};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn llm_prompts_list_filters_prompt_registry_events(ctx: TestContext) -> TestResult<()> {
    insert_prompt(&ctx, PromptTemplateStatus::Active).await?;
    insert_prompt(&ctx, PromptTemplateStatus::Draft).await?;

    let result = handle_llm_prompts_list(
        ctx.pool(),
        LlmPromptsListRequest {
            status: Some("active".to_string()),
            limit: 10,
        },
    )
    .await?;

    match result {
        EventQueryResult::Events { events, .. } => {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event.source.as_str(), "llm");
            assert_eq!(
                events[0].event.event_type.as_str(),
                "llm.prompt_template.registered"
            );
            assert_eq!(events[0].event.payload["status"], "active");
        }
        other => panic!("expected event listing, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
async fn llm_route_explain_uses_shared_router(ctx: TestContext) -> TestResult<()> {
    let response = handle_llm_route_explain(
        ctx.pool(),
        LlmRouteExplainRequest {
            request: route_request(ModelPrivacyRoute::ForceLocal),
            policy: routing_policy(),
        },
    )
    .await?;

    assert_eq!(response.decision.provider, "local-provider");
    assert_eq!(response.decision.model, "local-model");
    assert_eq!(response.decision.prompt_id, "extract-entities");
    Ok(())
}

#[sinex_test]
async fn llm_budget_report_aggregates_ledger_events(ctx: TestContext) -> TestResult<()> {
    insert_budget(
        &ctx,
        BudgetLedgerStatus::Success,
        Some(10),
        Some(4),
        Some(50),
        Some(100),
    )
    .await?;
    insert_budget(
        &ctx,
        BudgetLedgerStatus::Failure,
        Some(3),
        None,
        None,
        Some(25),
    )
    .await?;
    insert_budget(&ctx, BudgetLedgerStatus::Rejected, None, None, None, None).await?;

    let report = handle_llm_budget_report(ctx.pool(), LlmBudgetReportRequest { limit: 10 }).await?;

    assert_eq!(report.total_rows, 3);
    assert_eq!(report.success_count, 1);
    assert_eq!(report.failure_count, 1);
    assert_eq!(report.rejected_count, 1);
    assert_eq!(report.prompt_tokens, 13);
    assert_eq!(report.completion_tokens, 4);
    assert_eq!(report.cost_estimate_microusd, 50);
    assert_eq!(report.runtime_ms, 125);
    Ok(())
}

async fn insert_prompt(
    ctx: &TestContext,
    status: PromptTemplateStatus,
) -> TestResult<sinex_primitives::events::Event<sinex_primitives::JsonValue>> {
    let material_id = ctx.create_source_material(Some("llm-prompt-test")).await?;
    let payload = LlmPromptTemplateRegisteredPayload {
        prompt_id: "extract-entities".to_string(),
        version: format!("{status:?}"),
        purpose: "entity extraction fixture".to_string(),
        template_hash: "a".repeat(64),
        schema_hash: None,
        privacy_class: PromptPrivacyClass::Internal,
        owner: "test".to_string(),
        status,
        body_storage_ref: None,
    };
    let event = payload.from_material(material_id).build()?;
    Ok(ctx.pool().events().insert(event).await?)
}

async fn insert_budget(
    ctx: &TestContext,
    status: BudgetLedgerStatus,
    prompt_tokens: Option<i64>,
    completion_tokens: Option<i64>,
    cost_estimate_microusd: Option<i64>,
    runtime_ms: Option<i64>,
) -> TestResult<sinex_primitives::events::Event<sinex_primitives::JsonValue>> {
    let material_id = ctx.create_source_material(Some("llm-budget-test")).await?;
    let payload = LlmBudgetLedgerPayload {
        budget_ledger_id: Uuid::now_v7(),
        routing_decision_id: None,
        caller: "test".to_string(),
        task_kind: "entity-extraction".to_string(),
        provider: "provider".to_string(),
        model: "model".to_string(),
        prompt_tokens,
        completion_tokens,
        cost_estimate_microusd,
        runtime_ms,
        status,
        failure_class: None,
        recorded_at: Timestamp::now(),
    };
    let event = payload.from_material(material_id).build()?;
    Ok(ctx.pool().events().insert(event).await?)
}

fn route_request(privacy_route: ModelPrivacyRoute) -> ModelTaskRequest {
    ModelTaskRequest {
        task_kind: "entity-extraction".to_string(),
        prompt_id: "extract-entities".to_string(),
        input_hash: "input-hash".to_string(),
        privacy_route,
        bucket_key: Some("stable-key".to_string()),
    }
}

fn routing_policy() -> RoutingPolicyRecord {
    RoutingPolicyRecord {
        policy_id: "entity-extraction-v1".to_string(),
        task_kind: "entity-extraction".to_string(),
        prompt_id: "extract-entities".to_string(),
        prompt_version: "2026-05-19".to_string(),
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
