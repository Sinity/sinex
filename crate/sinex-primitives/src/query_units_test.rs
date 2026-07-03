use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn registry_contains_required_issue_1964_units() -> xtask::sandbox::TestResult<()> {
    let units = query_unit_descriptors()
        .iter()
        .map(|descriptor| descriptor.unit)
        .collect::<Vec<_>>();

    assert!(units.contains(&QueryUnitId::Events));
    assert!(units.contains(&QueryUnitId::SourceDrivers));
    assert!(units.contains(&QueryUnitId::SourceMaterials));
    assert!(units.contains(&QueryUnitId::Debt));
    assert!(units.contains(&QueryUnitId::Operations));
    assert!(units.contains(&QueryUnitId::RuntimeHealth));
    Ok(())
}

#[sinex_test]
async fn descriptor_rejects_unknown_field_with_supported_field_names()
-> xtask::sandbox::TestResult<()> {
    let descriptor = query_unit_descriptor(QueryUnitId::Events);
    let error = descriptor.field("source_id").unwrap_err();

    let rendered = error.to_string();
    assert!(rendered.contains("source_id"));
    assert!(rendered.contains("source, event_type, host, scope_key, equivalence_key"));
    assert!(!rendered.contains("event_contract_id"));
    Ok(())
}

#[sinex_test]
async fn event_descriptor_exposes_only_currently_lowerable_fields_and_operators()
-> xtask::sandbox::TestResult<()> {
    let descriptor = query_unit_descriptor(QueryUnitId::Events);
    let fields = descriptor
        .fields
        .iter()
        .map(|field| field.name)
        .collect::<Vec<_>>();

    assert_eq!(
        fields,
        vec![
            "source",
            "event_type",
            "host",
            "scope_key",
            "equivalence_key"
        ]
    );
    for field in descriptor.fields {
        assert_eq!(field.operators, &[QueryOperator::Eq]);
    }
    Ok(())
}

#[sinex_test]
async fn query_validation_rejects_operator_not_declared_by_descriptor()
-> xtask::sandbox::TestResult<()> {
    let mut query = SinexQuery::new(QueryUnitId::Debt, Some(20), None);
    query.predicate = Some(SinexQueryPredicate::Compare {
        field: "kind".to_string(),
        operator: QueryOperator::Contains,
        value: QueryValue::String("admission".to_string()),
    });

    let error = query.validate().unwrap_err();
    assert!(error.to_string().contains("does not support operator"));
    Ok(())
}

#[sinex_test]
async fn query_validation_clamps_to_unit_limit() -> xtask::sandbox::TestResult<()> {
    let query = SinexQuery::new(QueryUnitId::Operations, Some(10_000), Some(-10));

    assert_eq!(query.pagination.limit, 500);
    assert_eq!(query.pagination.offset, 0);
    Ok(())
}

#[sinex_test]
async fn parser_lowers_events_query_to_descriptor_validated_ast()
-> xtask::sandbox::TestResult<()> {
    let query = parse_sinex_query(
        "events where source = \"terminal.fish-history\" and event_type = terminal.command limit 10",
    )
    .unwrap();

    assert_eq!(query.unit, QueryUnitId::Events);
    assert_eq!(query.pagination.limit, 10);
    assert!(query.sort.is_empty());
    assert!(matches!(
        query.predicate,
        Some(SinexQueryPredicate::And { .. })
    ));
    Ok(())
}

#[sinex_test]
async fn parser_rejects_unknown_unit_before_execution() -> xtask::sandbox::TestResult<()> {
    let error = parse_sinex_query("widgets where status = active").unwrap_err();

    assert!(error.to_string().contains("unknown query unit"));
    Ok(())
}

#[sinex_test]
async fn parser_rejects_unknown_field_before_execution() -> xtask::sandbox::TestResult<()> {
    let error = parse_sinex_query("debt where widget = active").unwrap_err();

    assert!(error.to_string().contains("does not support field"));
    Ok(())
}

#[sinex_test]
async fn parser_rejects_invalid_enum_value_before_execution() -> xtask::sandbox::TestResult<()>
{
    let error = parse_sinex_query("operations where status = mystery").unwrap_err();

    assert!(error.to_string().contains("does not allow enum value"));
    Ok(())
}

#[sinex_test]
async fn parser_rejects_unsupported_sort_key_before_execution() -> xtask::sandbox::TestResult<()>
{
    let error = parse_sinex_query("operations sort widget desc").unwrap_err();

    assert!(error.to_string().contains("does not support sort key"));
    assert!(error.to_string().contains("operation_id"));
    Ok(())
}

#[sinex_test]
async fn parser_lowers_runtime_stale_after_as_numeric_seconds() -> xtask::sandbox::TestResult<()>
{
    let query = parse_sinex_query("runtime-health where stale_after >= 300").unwrap();

    assert_eq!(query.unit, QueryUnitId::RuntimeHealth);
    assert!(matches!(
        query.predicate,
        Some(SinexQueryPredicate::Compare {
            field,
            operator: QueryOperator::GreaterThanOrEq,
            value: QueryValue::Integer(300),
        }) if field == "stale_after"
    ));
    Ok(())
}

#[sinex_test]
async fn parser_lowers_single_quoted_rfc3339_event_time_bounds()
-> xtask::sandbox::TestResult<()> {
    let query = parse_sinex_query(
        "events where ts_orig >= '2026-07-02T12:00:00Z' and ts_orig < '2026-07-02T13:00:00Z' limit 25",
    )?;
    let request = event_query_from_sinex_query(&query)?;
    let range = request
        .time_range
        .expect("quoted RFC3339 bounds should lower to an event time range");

    assert_eq!(
        range.start(),
        Some(Timestamp::parse_rfc3339("2026-07-02T12:00:00Z")?)
    );
    assert_eq!(
        range.end(),
        Some(Timestamp::parse_rfc3339("2026-07-02T13:00:00Z")?)
    );
    assert_eq!(request.limit, 25);
    Ok(())
}
