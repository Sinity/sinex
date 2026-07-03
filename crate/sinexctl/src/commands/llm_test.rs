use serde_json::json;
use sinex_primitives::query::EventQueryResult;
use sinex_primitives::rpc::llm::LlmBudgetReportResponse;
use sinex_primitives::views::ReadinessCaveatId;
use xtask::sandbox::prelude::*;

use super::*;

#[sinex_test]
async fn llm_prompts_empty_envelope_names_absent_producer() -> xtask::sandbox::TestResult<()> {
    let envelope = llm_prompts_envelope(
        &EventQueryResult::Events {
            events: Vec::new(),
            next_cursor: None,
            total_estimate: Some(0),
        },
        json!({ "limit": 100 }),
    );

    assert_eq!(envelope.source_surface, "sinexctl.semantic.llm.prompts");
    let caveat = envelope
        .caveats
        .iter()
        .find(|caveat| caveat.id == ReadinessCaveatId::SourceAbsent.as_str())
        .expect("empty prompts view must expose source.absent caveat");
    assert!(caveat.message.contains("prompt-template registry rows"));
    assert_eq!(
        caveat.ref_.as_ref().map(|object_ref| object_ref.id.as_str()),
        Some("llm.prompt_template.registered")
    );
    Ok(())
}

#[sinex_test]
async fn llm_budget_report_envelope_lifts_response_caveats() -> xtask::sandbox::TestResult<()> {
    let response = LlmBudgetReportResponse {
        rows: Vec::new(),
        total_rows: 0,
        success_count: 0,
        failure_count: 0,
        rejected_count: 0,
        prompt_tokens: 0,
        completion_tokens: 0,
        cost_estimate_microusd: 0,
        runtime_ms: 0,
        caveats: vec![llm_producer_absent_caveat(
            "llm.budget.ledger",
            "LLM budget-report has no ledger rows; no budget-ledger producer is currently contributing events.",
        )],
    };

    let envelope = llm_budget_report_envelope(&response, json!({ "limit": 100 }));

    assert_eq!(
        envelope.source_surface,
        "sinexctl.semantic.llm.budget-report"
    );
    let caveat = envelope
        .caveats
        .iter()
        .find(|caveat| caveat.id == ReadinessCaveatId::SourceAbsent.as_str())
        .expect("budget report envelope must preserve response caveats");
    assert!(caveat.message.contains("budget-ledger producer"));
    assert_eq!(
        caveat.ref_.as_ref().map(|object_ref| object_ref.id.as_str()),
        Some("llm.budget.ledger")
    );
    Ok(())
}
