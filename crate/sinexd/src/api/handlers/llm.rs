//! LLM prompt/router/budget read RPC handlers.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::LlmBudgetLedgerPayload;
use sinex_primitives::llm::{BudgetLedgerStatus, decide_route};
use sinex_primitives::query::{EventQuery, EventQueryResult, PayloadFilter};
use sinex_primitives::rpc::llm::{
    LlmBudgetReportRequest, LlmBudgetReportResponse, LlmPromptsListRequest, LlmRouteExplainRequest,
    LlmRouteExplainResponse,
};
use sinex_primitives::views::{CaveatView, ReadinessCaveatId, SinexObjectKind, SinexObjectRef};
use sinex_primitives::{Result, SinexError};
use sqlx::PgPool;

pub async fn handle_llm_prompts_list(
    pool: &PgPool,
    req: LlmPromptsListRequest,
) -> Result<EventQueryResult> {
    let payload = req.status.map(|status| PayloadFilter::Contains {
        value: json!({ "status": status }),
    });
    let mut query = EventQuery {
        sources: vec![EventSource::from_static("llm")],
        event_types: vec![EventType::from_static("llm.prompt_template.registered")],
        payload,
        limit: req.limit,
        ..Default::default()
    };
    query.validate()?;

    pool.events().query(query).await
}

pub async fn handle_llm_route_explain(
    _pool: &PgPool,
    req: LlmRouteExplainRequest,
) -> Result<LlmRouteExplainResponse> {
    let decision = decide_route(&req.request, &req.policy)?;
    Ok(LlmRouteExplainResponse { decision })
}

pub async fn handle_llm_budget_report(
    pool: &PgPool,
    req: LlmBudgetReportRequest,
) -> Result<LlmBudgetReportResponse> {
    let mut query = EventQuery {
        sources: vec![EventSource::from_static("llm")],
        event_types: vec![EventType::from_static("llm.budget.ledger")],
        limit: req.limit,
        ..Default::default()
    };
    query.validate()?;

    let result = pool.events().query(query).await?;
    let rows = match result {
        EventQueryResult::Events { events, .. } => events
            .into_iter()
            .map(|event| {
                serde_json::from_value::<LlmBudgetLedgerPayload>(event.event.payload).map_err(
                    |error| {
                        SinexError::serialization("llm.budget.report: invalid ledger payload")
                            .with_std_error(&error)
                    },
                )
            })
            .collect::<Result<Vec<_>>>()?,
        _ => Vec::new(),
    };

    Ok(summarize_budget_rows(rows))
}

fn summarize_budget_rows(rows: Vec<LlmBudgetLedgerPayload>) -> LlmBudgetReportResponse {
    let mut response = LlmBudgetReportResponse {
        total_rows: rows.len(),
        rows,
        success_count: 0,
        failure_count: 0,
        rejected_count: 0,
        prompt_tokens: 0,
        completion_tokens: 0,
        cost_estimate_microusd: 0,
        runtime_ms: 0,
        caveats: Vec::new(),
    };

    if response.rows.is_empty() {
        response.caveats.push(llm_producer_absent_caveat(
            "llm.budget.ledger",
            "LLM budget-report has no ledger rows; no budget-ledger producer is currently contributing events.",
        ));
    }

    for row in &response.rows {
        match row.status {
            BudgetLedgerStatus::Success => response.success_count += 1,
            BudgetLedgerStatus::Failure => response.failure_count += 1,
            BudgetLedgerStatus::Rejected => response.rejected_count += 1,
        }
        response.prompt_tokens += row.prompt_tokens.unwrap_or_default();
        response.completion_tokens += row.completion_tokens.unwrap_or_default();
        response.cost_estimate_microusd += row.cost_estimate_microusd.unwrap_or_default();
        response.runtime_ms += row.runtime_ms.unwrap_or_default();
    }

    response
}

fn llm_producer_absent_caveat(event_type: &'static str, message: &'static str) -> CaveatView {
    CaveatView {
        id: ReadinessCaveatId::SourceAbsent.as_str().to_string(),
        message: message.to_string(),
        ref_: Some(SinexObjectRef::new(SinexObjectKind::Projection, event_type)),
    }
}
