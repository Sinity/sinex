use super::*;
use serde_json::json;
use sinex_primitives::views::SinexObjectKind;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn query_result_table_renders_refs_and_fields() -> xtask::TestResult<()> {
    let query = parse_sinex_query("operations where status = failed limit 10")?;
    let row = SinexQueryResultRow::new(
        QueryUnitId::Operations,
        SinexObjectKind::Operation,
        "operation fixture",
        json!({"id": "op-1"}),
    )
    .with_ref(SinexObjectRef::new(SinexObjectKind::Operation, "op-1").with_label("op-1"))
    .with_summary("fixture failure")
    .with_field("status", "failed");
    let view = SinexQueryResultListView::new(query, vec![row]);

    let output = format_query_rows_table(&view);

    assert!(output.contains("operations"));
    assert!(output.contains("op-1"));
    assert!(output.contains("fixture failure"));
    Ok(())
}

#[sinex_test]
async fn event_query_rejects_non_executable_descriptor_fields_before_lowering()
-> xtask::TestResult<()> {
    let error = parse_sinex_query("events where event_contract_id = terminal.command limit 10")
        .unwrap_err()
        .to_string();

    assert!(error.contains("does not support field `event_contract_id`"));
    assert!(error.contains("source, event_type, host, scope_key, equivalence_key"));
    Ok(())
}

#[sinex_test]
async fn runtime_health_query_predicates_filter_summary_rows() -> xtask::TestResult<()> {
    let healthy_query = parse_sinex_query("runtime-health where state = healthy limit 1")?;
    let degraded_query = parse_sinex_query("runtime-health where state != healthy limit 1")?;
    let row = SinexQueryResultRow::new(
        QueryUnitId::RuntimeHealth,
        SinexObjectKind::RuntimeModule,
        "runtime health",
        json!({"active_count": 2, "inactive_count": 1}),
    )
    .with_field("module", "runtime")
    .with_field("role", "summary")
    .with_field("state", "degraded")
    .with_field("stale_after", 300);

    assert!(!row_matches_query(&healthy_query, &row));
    assert!(row_matches_query(&degraded_query, &row));
    Ok(())
}

#[sinex_test]
async fn runtime_health_query_supports_descriptor_declared_fields() -> xtask::TestResult<()> {
    let module_query = parse_sinex_query("runtime-health where module contains runtime")?;
    let role_query = parse_sinex_query("runtime-health where role starts_with sum")?;
    let stale_query = parse_sinex_query("runtime-health where stale_after >= 300")?;
    let stale_range_query = parse_sinex_query("runtime-health where stale_after <= 60")?;
    let row = SinexQueryResultRow::new(
        QueryUnitId::RuntimeHealth,
        SinexObjectKind::RuntimeModule,
        "runtime health",
        json!({}),
    )
    .with_field("module", "runtime")
    .with_field("role", "summary")
    .with_field("state", "healthy")
    .with_field("stale_after", 300);

    assert!(row_matches_query(&module_query, &row));
    assert!(row_matches_query(&role_query, &row));
    assert!(row_matches_query(&stale_query, &row));
    assert!(!row_matches_query(&stale_range_query, &row));
    Ok(())
}

#[sinex_test]
async fn query_sort_and_offset_are_applied_to_result_rows() -> xtask::TestResult<()> {
    let query = parse_sinex_query("operations sort status asc offset 1 limit 1")?;
    let rows = vec![
        SinexQueryResultRow::new(
            QueryUnitId::Operations,
            SinexObjectKind::Operation,
            "failed op",
            json!({"id": "op-failed"}),
        )
        .with_ref(SinexObjectRef::new(SinexObjectKind::Operation, "op-failed"))
        .with_field("status", "failed"),
        SinexQueryResultRow::new(
            QueryUnitId::Operations,
            SinexObjectKind::Operation,
            "completed op",
            json!({"id": "op-completed"}),
        )
        .with_ref(SinexObjectRef::new(
            SinexObjectKind::Operation,
            "op-completed",
        ))
        .with_field("status", "completed"),
        SinexQueryResultRow::new(
            QueryUnitId::Operations,
            SinexObjectKind::Operation,
            "running op",
            json!({"id": "op-running"}),
        )
        .with_ref(SinexObjectRef::new(
            SinexObjectKind::Operation,
            "op-running",
        ))
        .with_field("status", "running"),
    ];

    let rows = finalize_rows(&query, rows);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].title, "failed op");
    Ok(())
}

#[sinex_test]
async fn numeric_query_sort_does_not_use_lexicographic_ordering() -> xtask::TestResult<()> {
    let query = parse_sinex_query("debt sort age desc limit 2")?;
    let rows = vec![
        SinexQueryResultRow::new(
            QueryUnitId::Debt,
            SinexObjectKind::DebtRow,
            "age 20",
            json!({}),
        )
        .with_field("age", 20),
        SinexQueryResultRow::new(
            QueryUnitId::Debt,
            SinexObjectKind::DebtRow,
            "age 100",
            json!({}),
        )
        .with_field("age", 100),
    ];

    let rows = finalize_rows(&query, rows);

    assert_eq!(rows[0].title, "age 100");
    assert_eq!(rows[1].title, "age 20");
    Ok(())
}
