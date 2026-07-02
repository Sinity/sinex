use std::cmp::Ordering;
use std::collections::BTreeMap;

use clap::Args;
use color_eyre::eyre::eyre;
use serde_json::Value;
use sinex_primitives::domain::{EventSource, EventType, HostName};
use sinex_primitives::query::{EventQuery, SortDirection};
use sinex_primitives::query_units::{
    QueryOperator, QueryUnitId, QueryValue, SinexQuery, SinexQueryPredicate,
    SinexQueryResultListView, SinexQueryResultRow, parse_sinex_query,
};
use sinex_primitives::rpc::dlq::DlqListResponse;
use sinex_primitives::rpc::sources::{
    SourceMaterialSummary, SourcesCoverageRequest, SourcesListRequest,
};
use sinex_primitives::views::{
    DebtRowView, OperationView, SinexObjectKind, SinexObjectRef, SourceCoverageView, ViewEnvelope,
};

use crate::Result;
use crate::client::GatewayClient;
use crate::commands::ops::{
    debt_rows_from_derivation_trigger, debt_rows_from_dlq, debt_rows_from_source_coverage,
    operations_to_views,
};
use crate::fmt::render_envelope;
use crate::model::OutputFormat;

/// Execute a Sinex-native query unit selection.
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    sinexctl query 'events where source = \"terminal.fish-history\" and event_type = \"terminal.command\" limit 100'
    sinexctl query 'source-drivers where readiness != \"ready\" limit 50'
    sinexctl query 'source-materials where status = \"completed\" limit 25'
    sinexctl query 'debt where kind = \"admission\" or kind = \"projection\" limit 50'
    sinexctl query 'operations where status = \"failed\" sort operation_id desc limit 25'
    sinexctl query 'runtime-health limit 1'
")]
pub struct QueryUnitsCommand {
    /// Query expression, for example: `events where source = "terminal" limit 50`.
    pub query: String,
}

impl QueryUnitsCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let query = parse_sinex_query(&self.query)?;
        let rows = execute_query_unit(client, &query).await?;
        let view = SinexQueryResultListView::new(query.clone(), rows);
        let envelope = ViewEnvelope::new("sinexctl.query", view.clone())
            .with_query_echo(serde_json::to_value(&query)?);

        if let Some(output) = render_envelope(&envelope, &view.rows, format)? {
            print_machine_output(&output);
            return Ok(());
        }

        println!("{}", format_query_rows_table(&view));
        Ok(())
    }
}

pub(crate) async fn execute_query_unit(
    client: &GatewayClient,
    query: &SinexQuery,
) -> Result<Vec<SinexQueryResultRow>> {
    match query.unit {
        QueryUnitId::Events => query_events(client, query).await,
        QueryUnitId::SourceDrivers => query_source_drivers(client, query).await,
        QueryUnitId::SourceMaterials => query_source_materials(client, query).await,
        QueryUnitId::Debt => query_debt(client, query).await,
        QueryUnitId::Operations => query_operations(client, query).await,
        QueryUnitId::RuntimeHealth => query_runtime_health(client, query).await,
    }
}

async fn query_events(
    client: &GatewayClient,
    query: &SinexQuery,
) -> Result<Vec<SinexQueryResultRow>> {
    let request = event_query_from_query(query)?;
    let result = client.event_cards(request).await?;
    let rows = result
        .cards
        .into_iter()
        .map(|card| {
            let payload = serde_json::to_value(&card).unwrap_or(Value::Null);
            SinexQueryResultRow::new(
                QueryUnitId::Events,
                SinexObjectKind::Event,
                card.summary.clone(),
                payload,
            )
            .with_ref(card.ref_.clone())
            .with_summary(format!("{} {}", card.source.raw, card.event_type))
            .with_field("source", card.source.raw)
            .with_field("event_type", card.event_type)
            .with_field("origin_kind", format!("{:?}", card.origin_kind))
            .with_caveats(card.caveats)
        })
        .collect::<Vec<_>>();
    Ok(finalize_rows(query, rows))
}

fn event_query_from_query(query: &SinexQuery) -> Result<EventQuery> {
    let mut request = EventQuery {
        limit: query.pagination.limit + query.pagination.offset,
        direction: SortDirection::Desc,
        ..Default::default()
    };

    if let Some(predicate) = &query.predicate {
        apply_event_predicate(predicate, &mut request)?;
    }

    Ok(request)
}

fn apply_event_predicate(predicate: &SinexQueryPredicate, request: &mut EventQuery) -> Result<()> {
    match predicate {
        SinexQueryPredicate::Compare {
            field,
            operator,
            value,
        } if *operator == QueryOperator::Eq => {
            let value = value_as_string(value)?;
            match field.as_str() {
                "source" => request.sources.push(EventSource::new(value)?),
                "event_type" => request.event_types.push(EventType::new(value)?),
                "host" => request.hosts.push(HostName::new(value)?),
                "scope_key" => request.scope_key = Some(value.to_string()),
                "equivalence_key" => request.equivalence_key = Some(value.to_string()),
                other => {
                    return Err(eyre!(
                        "events query field `{other}` is descriptor-valid but cannot yet lower to EventQuery without widening; use source, event_type, host, scope_key, or equivalence_key"
                    ));
                }
            }
            Ok(())
        }
        SinexQueryPredicate::And { predicates } => {
            for child in predicates {
                apply_event_predicate(child, request)?;
            }
            Ok(())
        }
        other => Err(eyre!(
            "events query predicate `{other:?}` cannot lower to EventQuery without widening"
        )),
    }
}

async fn query_source_drivers(
    client: &GatewayClient,
    query: &SinexQuery,
) -> Result<Vec<SinexQueryResultRow>> {
    let envelope = client.sources_status_view().await?;
    let rows = envelope
        .payload
        .sources
        .into_iter()
        .map(source_driver_row)
        .filter(|row| row_matches_query(query, row))
        .collect();
    Ok(finalize_rows(query, rows))
}

fn source_driver_row(source: SourceCoverageView) -> SinexQueryResultRow {
    let readiness = serde_json::to_value(source.readiness).unwrap_or(Value::Null);
    let readiness_text = readiness
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| readiness.to_string().trim_matches('"').to_string());
    let payload = serde_json::to_value(&source).unwrap_or(Value::Null);
    SinexQueryResultRow::new(
        QueryUnitId::SourceDrivers,
        SinexObjectKind::SourceDriver,
        source.source_id.clone(),
        payload,
    )
    .with_ref(
        SinexObjectRef::new(SinexObjectKind::SourceDriver, source.source_id.clone())
            .with_label(source.source_id.clone())
            .with_command_hint(format!("sinexctl sources status {}", source.source_id)),
    )
    .with_summary(format!(
        "{} events, {} materials",
        source.event_count, source.material_count
    ))
    .with_field("source_id", source.source_id)
    .with_field("family", source.namespace)
    .with_field("readiness", readiness_text)
    .with_field("enabled", source.accepted_binding_count > 0)
    .with_caveats(source.caveats)
}

async fn query_source_materials(
    client: &GatewayClient,
    query: &SinexQuery,
) -> Result<Vec<SinexQueryResultRow>> {
    let status = exact_string_filter(query.predicate.as_ref(), "status")?;
    let response = client
        .sources_list(SourcesListRequest {
            status,
            limit: None,
        })
        .await?;
    let rows = response
        .materials
        .into_iter()
        .map(source_material_row)
        .filter(|row| row_matches_query(query, row))
        .collect::<Vec<_>>();
    Ok(finalize_rows(query, rows))
}

fn source_material_row(material: SourceMaterialSummary) -> SinexQueryResultRow {
    let status = serde_json::to_value(material.status)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_string());
    let payload = serde_json::to_value(&material).unwrap_or(Value::Null);
    SinexQueryResultRow::new(
        QueryUnitId::SourceMaterials,
        SinexObjectKind::SourceMaterial,
        material.id.clone(),
        payload,
    )
    .with_ref(
        SinexObjectRef::new(SinexObjectKind::SourceMaterial, material.id.clone())
            .with_label(short_ref(&material.id))
            .with_command_hint(format!("sinexctl sources show {}", material.id)),
    )
    .with_summary(material.source_identifier.clone())
    .with_field("material_id", material.id)
    .with_field("source_identifier", material.source_identifier)
    .with_field("material_kind", material.material_kind.to_string())
    .with_field("status", status)
}

async fn query_debt(
    client: &GatewayClient,
    query: &SinexQuery,
) -> Result<Vec<SinexQueryResultRow>> {
    let dlq: DlqListResponse = client.dlq_list().await?;
    let mut debt_rows = debt_rows_from_dlq(&dlq);
    let coverage = client.sources_coverage(SourcesCoverageRequest {}).await?;
    debt_rows.extend(debt_rows_from_source_coverage(&coverage.sources));
    debt_rows.extend(debt_rows_from_derivation_trigger(
        sinex_primitives::InvalidationTrigger::Replay,
    ));
    let rows = debt_rows
        .into_iter()
        .map(debt_row)
        .filter(|row| row_matches_query(query, row))
        .collect::<Vec<_>>();
    Ok(finalize_rows(query, rows))
}

fn debt_row(row: DebtRowView) -> SinexQueryResultRow {
    let kind = serde_json::to_value(row.kind)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_string());
    let severity = debt_severity(&row);
    let source = row
        .refs
        .first()
        .map(|ref_| ref_.id.clone())
        .unwrap_or_default();
    let payload = serde_json::to_value(&row).unwrap_or(Value::Null);
    SinexQueryResultRow::new(
        QueryUnitId::Debt,
        SinexObjectKind::DebtRow,
        row.summary.clone(),
        payload,
    )
    .with_ref(SinexObjectRef::new(SinexObjectKind::DebtRow, row.id.clone()).with_label(row.id))
    .with_field("kind", kind)
    .with_field("severity", severity)
    .with_field("source", source)
    .with_field("age", row.age_secs.unwrap_or_default())
    .with_caveats(row.caveats)
}

fn debt_severity(row: &DebtRowView) -> &'static str {
    if row.actions.iter().any(|action| {
        matches!(
            action.side_effect,
            sinex_primitives::views::ActionSideEffect::Destructive
                | sinex_primitives::views::ActionSideEffect::Write
                | sinex_primitives::views::ActionSideEffect::Admin
        )
    }) {
        "warning"
    } else {
        "info"
    }
}

async fn query_operations(
    client: &GatewayClient,
    query: &SinexQuery,
) -> Result<Vec<SinexQueryResultRow>> {
    let operation_type = exact_string_filter(query.predicate.as_ref(), "operation_type")?;
    let status = exact_string_filter(query.predicate.as_ref(), "status")?;
    let operations = client.ops_list(operation_type, status, None).await?;
    let rows = operations_to_views(&operations)
        .into_iter()
        .map(operation_row)
        .filter(|row| row_matches_query(query, row))
        .collect::<Vec<_>>();
    Ok(finalize_rows(query, rows))
}

fn operation_row(view: OperationView) -> SinexQueryResultRow {
    let payload = serde_json::to_value(&view).unwrap_or(Value::Null);
    SinexQueryResultRow::new(
        QueryUnitId::Operations,
        SinexObjectKind::Operation,
        view.id.clone(),
        payload,
    )
    .with_ref(
        SinexObjectRef::new(SinexObjectKind::Operation, view.id.clone())
            .with_label(short_ref(&view.id))
            .with_command_hint(format!("sinexctl ops get {}", view.id)),
    )
    .with_summary(
        view.result_message
            .clone()
            .unwrap_or_else(|| view.kind.as_str().to_string()),
    )
    .with_field("operation_id", view.id)
    .with_field("operation_type", view.kind.as_str())
    .with_field("status", view.status.to_string())
}

async fn query_runtime_health(
    client: &GatewayClient,
    query: &SinexQuery,
) -> Result<Vec<SinexQueryResultRow>> {
    let health = client.runtime_health(300).await?;
    let state = if health.inactive_count == 0 {
        "healthy"
    } else {
        "degraded"
    };
    let payload = serde_json::to_value(&health).unwrap_or(Value::Null);
    let row = SinexQueryResultRow::new(
        QueryUnitId::RuntimeHealth,
        SinexObjectKind::RuntimeModule,
        "runtime health",
        payload,
    )
    .with_ref(SinexObjectRef::new(SinexObjectKind::RuntimeModule, "runtime").with_label("runtime"))
    .with_summary(format!(
        "{} active modules, {} inactive modules",
        health.active_count, health.inactive_count
    ))
    .with_field("module", "runtime")
    .with_field("role", "summary")
    .with_field("state", state)
    .with_field("active_count", health.active_count)
    .with_field("inactive_count", health.inactive_count)
    .with_field("stale_after", 300);
    let rows = vec![row]
        .into_iter()
        .filter(|row| row_matches_query(query, row))
        .collect::<Vec<_>>();
    Ok(finalize_rows(query, rows))
}

fn finalize_rows(
    query: &SinexQuery,
    mut rows: Vec<SinexQueryResultRow>,
) -> Vec<SinexQueryResultRow> {
    apply_query_sort(query, &mut rows);
    rows.into_iter()
        .skip(query.pagination.offset as usize)
        .take(query.pagination.limit as usize)
        .collect()
}

fn apply_query_sort(query: &SinexQuery, rows: &mut [SinexQueryResultRow]) {
    for sort in query.sort.iter().rev() {
        rows.sort_by(|left, right| compare_row_field(left, right, &sort.key, sort.descending));
    }
}

fn compare_row_field(
    left: &SinexQueryResultRow,
    right: &SinexQueryResultRow,
    key: &str,
    descending: bool,
) -> Ordering {
    let ordering = match (left.fields.get(key), right.fields.get(key)) {
        (Some(left), Some(right)) => compare_json_field(left, right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    };
    if descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn compare_json_field(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left
            .as_f64()
            .partial_cmp(&right.as_f64())
            .unwrap_or(Ordering::Equal),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        _ => value_to_string(left).cmp(&value_to_string(right)),
    }
}

fn exact_string_filter(
    predicate: Option<&SinexQueryPredicate>,
    field: &str,
) -> Result<Option<String>> {
    let Some(predicate) = predicate else {
        return Ok(None);
    };
    match predicate {
        SinexQueryPredicate::Compare {
            field: actual,
            operator,
            value,
        } if actual == field && *operator == QueryOperator::Eq => {
            Ok(Some(value_as_string(value)?.to_string()))
        }
        SinexQueryPredicate::And { predicates } => {
            for child in predicates {
                if let Some(value) = exact_string_filter(Some(child), field)? {
                    return Ok(Some(value));
                }
            }
            Ok(None)
        }
        SinexQueryPredicate::Or { .. } | SinexQueryPredicate::Not { .. } => Ok(None),
        SinexQueryPredicate::Compare { .. } | SinexQueryPredicate::Has { .. } => Ok(None),
    }
}

fn row_matches_query(query: &SinexQuery, row: &SinexQueryResultRow) -> bool {
    query
        .predicate
        .as_ref()
        .is_none_or(|predicate| row_matches_predicate(predicate, &row.fields))
}

fn row_matches_predicate(
    predicate: &SinexQueryPredicate,
    fields: &BTreeMap<String, Value>,
) -> bool {
    match predicate {
        SinexQueryPredicate::Compare {
            field,
            operator,
            value,
        } => fields
            .get(field)
            .is_some_and(|actual| compare_value(actual, *operator, value)),
        SinexQueryPredicate::Has { field } => fields.contains_key(field),
        SinexQueryPredicate::And { predicates } => predicates
            .iter()
            .all(|child| row_matches_predicate(child, fields)),
        SinexQueryPredicate::Or { predicates } => predicates
            .iter()
            .any(|child| row_matches_predicate(child, fields)),
        SinexQueryPredicate::Not { predicate } => !row_matches_predicate(predicate, fields),
    }
}

fn compare_value(actual: &Value, operator: QueryOperator, expected: &QueryValue) -> bool {
    let actual = value_to_string(actual);
    let expected = match expected {
        QueryValue::String(value) => value.as_str(),
        QueryValue::Integer(value) => return compare_i64(actual.parse().ok(), operator, *value),
        QueryValue::Boolean(value) => return compare_bool(actual.parse().ok(), operator, *value),
    };
    match operator {
        QueryOperator::Eq => actual == expected,
        QueryOperator::NotEq => actual != expected,
        QueryOperator::Contains => actual.contains(expected),
        QueryOperator::StartsWith => actual.starts_with(expected),
        QueryOperator::GreaterThan => actual.as_str() > expected,
        QueryOperator::GreaterThanOrEq => actual.as_str() >= expected,
        QueryOperator::LessThan => actual.as_str() < expected,
        QueryOperator::LessThanOrEq => actual.as_str() <= expected,
        QueryOperator::Exists => true,
    }
}

fn compare_i64(actual: Option<i64>, operator: QueryOperator, expected: i64) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    match operator {
        QueryOperator::Eq => actual == expected,
        QueryOperator::NotEq => actual != expected,
        QueryOperator::GreaterThan => actual > expected,
        QueryOperator::GreaterThanOrEq => actual >= expected,
        QueryOperator::LessThan => actual < expected,
        QueryOperator::LessThanOrEq => actual <= expected,
        QueryOperator::Contains | QueryOperator::StartsWith | QueryOperator::Exists => false,
    }
}

fn compare_bool(actual: Option<bool>, operator: QueryOperator, expected: bool) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    match operator {
        QueryOperator::Eq => actual == expected,
        QueryOperator::NotEq => actual != expected,
        QueryOperator::Contains
        | QueryOperator::StartsWith
        | QueryOperator::GreaterThan
        | QueryOperator::GreaterThanOrEq
        | QueryOperator::LessThan
        | QueryOperator::LessThanOrEq
        | QueryOperator::Exists => false,
    }
}

fn value_as_string(value: &QueryValue) -> Result<&str> {
    match value {
        QueryValue::String(value) => Ok(value),
        other => Err(eyre!("expected string query value, got {other:?}")),
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn short_ref(id: &str) -> String {
    id.chars().take(12).collect()
}

fn format_query_rows_table(view: &SinexQueryResultListView) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "Query unit: {}  Rows: {}\n",
        view.query.unit, view.count
    ));
    out.push_str("Unit            Ref/Title                    Summary\n");
    out.push_str("──────────────  ───────────────────────────  ─────────────────────────\n");
    for row in &view.rows {
        let ref_or_title = row
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.label.as_deref())
            .unwrap_or(&row.title);
        let summary = row.summary.as_deref().unwrap_or("");
        out.push_str(&format!(
            "{:<14}  {:<27}  {}\n",
            row.unit,
            truncate(ref_or_title, 27),
            truncate(summary, 72)
        ));
    }
    if view.rows.is_empty() {
        out.push_str("No rows matched.\n");
    }
    out
}

fn truncate(value: &str, width: usize) -> String {
    let mut chars = value.chars();
    let mut out = String::new();
    for _ in 0..width {
        let Some(ch) = chars.next() else {
            return out;
        };
        out.push(ch);
    }
    if chars.next().is_some() && width > 1 {
        out.pop();
        out.push('…');
    }
    out
}

fn print_machine_output(output: &str) {
    print!("{output}");
    if !output.ends_with('\n') {
        println!();
    }
}

#[cfg(test)]
#[path = "query_units_test.rs"]
mod tests;
