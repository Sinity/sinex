//! Durable import/replay outcome report for post-wipe manifests.

use crate::api::service_container::ServiceContainer;
use sinex_db::DbPoolExt;
use sinex_db::repositories::ImportReportData;
use sinex_primitives::rpc::sources::{
    ImportReportBreakdown, ImportReportExample, SourcesImportReportRequest,
    SourcesImportReportResponse,
};
use sinex_primitives::{JsonValue, Result, SinexError, Uuid};
use std::collections::{BTreeMap, BTreeSet};

pub async fn handle_sources_import_report(
    services: &ServiceContainer,
    request: SourcesImportReportRequest,
) -> Result<SourcesImportReportResponse> {
    let operation_id = request.operation_id.parse::<Uuid>().map_err(|error| {
        SinexError::validation("invalid import operation UUID")
            .with_context("operation_id", request.operation_id)
            .with_std_error(&error)
    })?;
    let Some(data) = services
        .pool()
        .import_outcomes()
        .report(operation_id)
        .await?
    else {
        return Err(SinexError::not_found("import operation not found")
            .with_context("operation_id", operation_id.to_string()));
    };

    Ok(render_report(operation_id, data))
}

#[derive(Debug, Clone, Default)]
struct Counts {
    new: u64,
    suppressed: u64,
    superseded: u64,
    failures: u64,
    dlq: u64,
}

fn render_report(operation_id: Uuid, data: ImportReportData) -> SourcesImportReportResponse {
    let superseded_new_ids: BTreeSet<Uuid> = data
        .replacements
        .iter()
        .map(|replacement| replacement.new_event_id)
        .collect();
    let mut groups: BTreeMap<(String, String, Option<String>), Counts> = BTreeMap::new();
    let mut examples = Vec::new();

    for event in &data.admitted {
        let material = event.source_material_id.map(|id| id.to_string());
        let entry = groups
            .entry((
                event.source.clone(),
                event.event_type.clone(),
                material.clone(),
            ))
            .or_default();
        if superseded_new_ids.contains(&event.id) {
            entry.superseded += 1;
            if examples.len() < 20 {
                let predecessor = data
                    .replacements
                    .iter()
                    .find(|replacement| replacement.new_event_id == event.id)
                    .map(|replacement| replacement.old_event_id.to_string());
                examples.push(ImportReportExample {
                    outcome: "superseded".to_string(),
                    event_id: event.id.to_string(),
                    source: event.source.clone(),
                    event_type: event.event_type.clone(),
                    source_material_id: material,
                    reason: None,
                    superseded_event_id: predecessor,
                });
            }
        } else {
            entry.new += 1;
            if examples.len() < 20 {
                examples.push(ImportReportExample {
                    outcome: "new".to_string(),
                    event_id: event.id.to_string(),
                    source: event.source.clone(),
                    event_type: event.event_type.clone(),
                    source_material_id: material,
                    reason: None,
                    superseded_event_id: None,
                });
            }
        }
    }

    for outcome in &data.outcomes {
        let material = outcome.source_material_id.map(|id| id.to_string());
        let entry = groups
            .entry((
                outcome.source.clone(),
                outcome.event_type.clone(),
                material.clone(),
            ))
            .or_default();
        match outcome.outcome.as_str() {
            "suppressed" => entry.suppressed += 1,
            "failed" => entry.failures += 1,
            "dlq" => entry.dlq += 1,
            _ => {}
        }
        if examples.len() < 20 {
            examples.push(ImportReportExample {
                outcome: outcome.outcome.clone(),
                event_id: outcome.candidate_event_id.to_string(),
                source: outcome.source.clone(),
                event_type: outcome.event_type.clone(),
                source_material_id: material,
                reason: Some(outcome.reason.clone()),
                superseded_event_id: outcome.existing_event_id.map(|id| id.to_string()),
            });
        }
    }

    let operation_failed = data.operation.result_status.to_string() == "failure";
    let preview_failures = data
        .operation
        .preview_summary
        .as_ref()
        .map(count_failed_targets)
        .unwrap_or(0);
    let failures = data
        .outcomes
        .iter()
        .filter(|outcome| outcome.outcome == "failed")
        .count() as u64
        + u64::from(operation_failed)
        + preview_failures;
    let new = groups.values().map(|counts| counts.new).sum();
    let suppressed = groups.values().map(|counts| counts.suppressed).sum();
    let superseded = groups.values().map(|counts| counts.superseded).sum();
    let dlq = groups.values().map(|counts| counts.dlq).sum();
    let classified = new + suppressed + superseded + failures + dlq;
    let attempted_hint = data
        .operation
        .preview_summary
        .as_ref()
        .and_then(|summary| summary.get("events_processed"))
        .and_then(JsonValue::as_u64)
        .or_else(|| {
            data.operation
                .preview_summary
                .as_ref()
                .and_then(|summary| summary.get("attempted"))
                .and_then(JsonValue::as_u64)
        })
        .unwrap_or(classified);
    let attempted = attempted_hint.max(classified);
    let unresolved = attempted.saturating_sub(classified);

    let source_material_ids = collect_material_ids(&data);
    let source = data
        .operation
        .scope
        .as_ref()
        .and_then(|scope| scope.get("source_name").or_else(|| scope.get("source")))
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .or_else(|| data.admitted.first().map(|event| event.source.clone()))
        .or_else(|| data.outcomes.first().map(|outcome| outcome.source.clone()));

    SourcesImportReportResponse {
        operation_id: operation_id.to_string(),
        operation_type: data.operation.operation_type,
        operation_status: data.operation.result_status.to_string(),
        scope: data.operation.scope.unwrap_or(JsonValue::Null),
        source,
        source_material_ids,
        attempted,
        new,
        suppressed,
        superseded,
        failures,
        dlq,
        unresolved,
        breakdown: groups
            .into_iter()
            .map(
                |((source, event_type, source_material_id), counts)| ImportReportBreakdown {
                    source,
                    event_type,
                    source_material_id,
                    new: counts.new,
                    suppressed: counts.suppressed,
                    superseded: counts.superseded,
                    failures: counts.failures,
                    dlq: counts.dlq,
                },
            )
            .collect(),
        examples,
    }
}

fn count_failed_targets(summary: &JsonValue) -> u64 {
    summary
        .get("failed_targets")
        .and_then(JsonValue::as_array)
        .map_or(0, |targets| targets.len() as u64)
}

fn collect_material_ids(data: &ImportReportData) -> Vec<String> {
    let mut ids = BTreeSet::new();
    for event in &data.admitted {
        if let Some(id) = event.source_material_id {
            ids.insert(id.to_string());
        }
    }
    for outcome in &data.outcomes {
        if let Some(id) = outcome.source_material_id {
            ids.insert(id.to_string());
        }
    }
    if let Some(scope) = &data.operation.scope {
        for key in ["source_material_id", "material_id"] {
            if let Some(id) = scope.get(key).and_then(JsonValue::as_str) {
                ids.insert(id.to_string());
            }
        }
        if let Some(materials) = scope.get("material_filter").and_then(JsonValue::as_array) {
            for id in materials.iter().filter_map(JsonValue::as_str) {
                ids.insert(id.to_string());
            }
        }
    }
    ids.into_iter().collect()
}
