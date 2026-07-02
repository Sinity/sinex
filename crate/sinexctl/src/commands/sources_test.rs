use super::*;
use crate::fmt::render_finite_envelope;
use sinex_primitives::domain::{MaterialStatus, SourceMaterialTimingInfoType};
use sinex_primitives::parser::ParserId;
use sinex_primitives::rpc::sources::{
    SourceReadinessCost, SourceReadinessStatus, SourceShapeDriftObservation,
    SourceShapeTypeChange, caveat_codes,
};
use sinex_primitives::views::{
    SOURCE_CONTINUITY_DETAIL_SCHEMA_VERSION, SOURCE_CONTINUITY_GAP_SCHEMA_VERSION,
    SOURCE_CONTINUITY_LIST_SCHEMA_VERSION, SOURCE_DRIFT_LIST_SCHEMA_VERSION,
    SOURCE_READINESS_DETAIL_SCHEMA_VERSION, SOURCE_READINESS_LIST_SCHEMA_VERSION,
    VIEW_ENVELOPE_SCHEMA_VERSION,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn drift_table_surfaces_readiness_impact() -> TestResult<()> {
    let response = SourcesDriftListResponse {
        drifts: vec![SourceShapeDriftObservation {
            checkpoint_key: "source.default.fixture".to_string(),
            source_id: SourceId::from_static("browser.history"),
            consumer_group: Some("default".to_string()),
            consumer_name: Some("fixture".to_string()),
            previous_hash: "shape-old".to_string(),
            current_hash: "shape-new".to_string(),
            format: "sqlite_schema".to_string(),
            added_keys: Vec::new(),
            removed_keys: vec!["visit_id".to_string()],
            type_changes: vec![SourceShapeTypeChange {
                key: "visit_time".to_string(),
                previous_type: "number".to_string(),
                current_type: "string".to_string(),
            }],
            required_input_keys: vec!["visit_id".to_string()],
            observed_at: "2026-05-21T07:00:00Z".to_string(),
        }],
    };

    let table = format_drift_list(&response);

    assert!(table.contains("IMPACT"));
    assert!(table.contains("blocking"));
    assert!(table.contains(caveat_codes::PARSER_FIELD_TYPE_CHANGED));
    assert!(table.contains(caveat_codes::PARSER_REQUIRED_FIELD_MISSING));
    Ok(())
}

fn fixture_material(id: &str) -> SourceMaterialSummary {
    SourceMaterialSummary {
        id: id.to_string(),
        material_kind: sinex_primitives::MaterialStorageKind::Annex,
        source_identifier: "fixture.source".to_string(),
        status: MaterialStatus::Completed,
        timing_info_type: SourceMaterialTimingInfoType::Intrinsic,
        format: Some(SourceMaterialFormat::Jsonl),
        contract_version: Some(1),
        staged_at: Some("2026-06-01T00:00:00Z".to_string()),
        staged_by: Some("test".to_string()),
        size_bytes: Some(128),
        event_count: Some(7),
        mime_type: Some("application/jsonl".to_string()),
    }
}

fn fixture_coverage(source_identifier: &str) -> SourceCoverageEntry {
    SourceCoverageEntry {
        source_identifier: source_identifier.to_string(),
        material_kind: sinex_primitives::MaterialStorageKind::Annex,
        earliest_ts: Some("2026-06-01T00:00:00Z".to_string()),
        latest_ts: Some("2026-06-01T01:00:00Z".to_string()),
        event_count: Some(7),
        material_count: Some(2),
        completed_material_count: Some(2),
        failed_material_count: Some(0),
        recovered_partial_material_count: Some(1),
        sensing_material_count: Some(0),
        cancelled_material_count: Some(0),
        total_bytes: Some(256),
    }
}

fn fixture_readiness(source_identifier: &str) -> SourceReadiness {
    SourceReadiness {
        binding_id: None,
        source_family: "fixture".to_string(),
        source_id: Some(SourceId::from_static("fixture.source")),
        parser_id: Some(ParserId::from_static("fixture.parser")),
        source_identifier: source_identifier.to_string(),
        status: SourceReadinessStatus::Available,
        cost: SourceReadinessCost::LocalFast,
        freshness_seconds: Some(12),
        material_count: 2,
        parsed_event_count: Some(7),
        last_success_at: Some("2026-06-01T00:00:00Z".to_string()),
        caveats: Vec::new(),
        evidence: serde_json::json!({"fixture": true}),
    }
}

#[sinex_test]
async fn stage_request_preserves_package_mode_binding() -> TestResult<()> {
    let command = StageCommand {
        file: "/realm/data/captures/audio/session.json".to_string(),
        reason: Some("operator import".to_string()),
        material_format: Some(SourceMaterialFormat::Json),
        binding: Some("source:media.audio-transcript.audio-bundle-staged".to_string()),
        tags: vec!["media".to_string()],
    };

    let request = command.request();

    assert_eq!(request.file_path, "/realm/data/captures/audio/session.json");
    assert_eq!(request.format, Some(SourceMaterialFormat::Json));
    assert_eq!(request.reason.as_deref(), Some("operator import"));
    assert_eq!(
        request.binding_name.as_deref(),
        Some("source:media.audio-transcript.audio-bundle-staged")
    );
    assert_eq!(request.tags, vec!["media"]);
    assert!(request.with_bytes);
    Ok(())
}

#[sinex_test]
async fn source_material_list_envelope_renders_finite_json_document() -> TestResult<()> {
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.list",
        SourceMaterialListView::new(vec![fixture_material("material-1")]),
    )
    .with_query_echo(serde_json::json!({
        "status": "completed",
        "source": "fixture.source",
        "limit": 1,
    }));

    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let value: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.sources.list");
    assert_eq!(
        value["payload"]["schema_version"],
        SOURCE_MATERIAL_LIST_SCHEMA_VERSION
    );
    assert_eq!(value["payload"]["count"], 1);
    assert_eq!(value["payload"]["materials"][0]["id"], "material-1");
    assert_eq!(value["payload"]["materials"][0]["event_count"], 7);
    assert_eq!(value["query_echo"]["status"], "completed");
    assert_eq!(value["query_echo"]["source"], "fixture.source");
    Ok(())
}

#[sinex_test]
async fn source_coverage_envelope_renders_finite_json_document() -> TestResult<()> {
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.coverage",
        SourceCoverageListView::new(vec![fixture_coverage("fixture.source")]),
    )
    .with_query_echo(serde_json::json!({
        "limit": 100,
    }));

    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let value: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.sources.coverage");
    assert_eq!(
        value["payload"]["schema_version"],
        SOURCE_COVERAGE_LIST_SCHEMA_VERSION
    );
    assert_eq!(value["payload"]["count"], 1);
    assert_eq!(
        value["payload"]["sources"][0]["recovered_partial_material_count"],
        1
    );
    assert_eq!(
        value["payload"]["sources"][0]["source_identifier"],
        "fixture.source"
    );
    Ok(())
}

#[sinex_test]
async fn source_readiness_list_envelope_renders_finite_json_document() -> TestResult<()> {
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.readiness",
        SourceReadinessListView::new(vec![fixture_readiness("fixture.source")]),
    )
    .with_query_echo(serde_json::json!({
        "family": "fixture",
        "stale_after_seconds": 60,
    }));

    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let value: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.sources.readiness");
    assert_eq!(
        value["payload"]["schema_version"],
        SOURCE_READINESS_LIST_SCHEMA_VERSION
    );
    assert_eq!(value["payload"]["count"], 1);
    assert_eq!(
        value["payload"]["sources"][0]["source_identifier"],
        "fixture.source"
    );
    assert_eq!(value["payload"]["sources"][0]["status"], "available");
    assert_eq!(value["query_echo"]["family"], "fixture");
    Ok(())
}

#[sinex_test]
async fn source_readiness_detail_envelope_renders_finite_json_document() -> TestResult<()> {
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.readiness",
        SourceReadinessDetailView::new(Some(fixture_readiness("fixture.source"))),
    )
    .with_query_echo(serde_json::json!({
        "source": "fixture.source",
    }));

    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let value: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(
        value["payload"]["schema_version"],
        SOURCE_READINESS_DETAIL_SCHEMA_VERSION
    );
    assert_eq!(
        value["payload"]["source"]["source_identifier"],
        "fixture.source"
    );
    assert_eq!(value["query_echo"]["source"], "fixture.source");
    Ok(())
}

#[sinex_test]
async fn source_drift_envelope_renders_finite_json_document() -> TestResult<()> {
    let drift = SourceShapeDriftObservation {
        checkpoint_key: "source.default.fixture".to_string(),
        source_id: SourceId::from_static("browser.history"),
        consumer_group: Some("default".to_string()),
        consumer_name: Some("fixture".to_string()),
        previous_hash: "shape-old".to_string(),
        current_hash: "shape-new".to_string(),
        format: "sqlite_schema".to_string(),
        added_keys: vec!["title".to_string()],
        removed_keys: Vec::new(),
        type_changes: Vec::new(),
        required_input_keys: Vec::new(),
        observed_at: "2026-05-21T07:00:00Z".to_string(),
    };
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.drift",
        SourceDriftListView::new(vec![drift]),
    )
    .with_query_echo(serde_json::json!({
        "source": "browser.history",
        "limit": 1,
    }));

    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let value: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.sources.drift");
    assert_eq!(
        value["payload"]["schema_version"],
        SOURCE_DRIFT_LIST_SCHEMA_VERSION
    );
    assert_eq!(value["payload"]["count"], 1);
    assert_eq!(
        value["payload"]["drifts"][0]["source_id"],
        "browser.history"
    );
    Ok(())
}

#[sinex_test]
async fn source_continuity_empty_views_render_finite_json_documents() -> TestResult<()> {
    let list = ViewEnvelope::new(
        "sinexctl.sources.continuity",
        SourceContinuityListView::new(Vec::new()),
    );
    let detail = ViewEnvelope::new(
        "sinexctl.sources.continuity",
        SourceContinuityDetailView::new(None),
    );

    let list_json = render_finite_envelope(&list, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let list_value: serde_json::Value = serde_json::from_str(&list_json)?;
    assert_eq!(
        list_value["payload"]["schema_version"],
        SOURCE_CONTINUITY_LIST_SCHEMA_VERSION
    );
    assert_eq!(list_value["payload"]["count"], 0);

    let detail_json = render_finite_envelope(&detail, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let detail_value: serde_json::Value = serde_json::from_str(&detail_json)?;
    assert_eq!(
        detail_value["payload"]["schema_version"],
        SOURCE_CONTINUITY_DETAIL_SCHEMA_VERSION
    );
    assert!(detail_value["payload"].get("report").is_none());
    Ok(())
}

#[sinex_test]
async fn source_continuity_gap_envelope_renders_finite_json_document() -> TestResult<()> {
    let response = SourcesExplainGapResponse {
        source_family: SourceFamily::new("fixture")?,
        at: parse_timestamp("2026-06-01T00:00:00Z")?,
        gap: None,
        explanation: "coverage present".to_string(),
    };
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.explain_gap",
        SourceContinuityGapView::new(response),
    );

    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let value: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(
        value["payload"]["schema_version"],
        SOURCE_CONTINUITY_GAP_SCHEMA_VERSION
    );
    assert_eq!(
        value["payload"]["explanation"]["explanation"],
        "coverage present"
    );
    Ok(())
}

#[sinex_test]
async fn source_material_table_renderer_stays_on_raw_response() -> TestResult<()> {
    let table = format_source_materials_table(&SourcesListResponse {
        materials: vec![fixture_material("abcdef123456")],
    });

    assert!(table.contains("abcdef12..."));
    assert!(table.contains("fixture.source"));
    assert!(table.contains("completed"));
    assert!(table.contains("EVENTS"));
    assert!(table.contains("7"));
    Ok(())
}

#[sinex_test]
async fn source_coverage_table_renderer_stays_on_raw_response() -> TestResult<()> {
    let table = format_coverage_table(&SourcesCoverageResponse {
        sources: vec![fixture_coverage("fixture.source")],
    });

    assert!(table.contains("fixture.source"));
    assert!(table.contains("session_document"));
    assert!(table.contains("PARTIAL"));
    assert!(table.contains("7"));
    assert!(table.contains("1"));
    Ok(())
}

#[sinex_test]
async fn finite_source_views_reject_ndjson() -> TestResult<()> {
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.readiness",
        SourceReadinessListView::new(vec![fixture_readiness("fixture.source")]),
    );

    let result = render_finite_envelope(&envelope, OutputFormat::Ndjson);

    assert!(
        result.is_err(),
        "finite source views must not render ndjson"
    );
    assert!(result.unwrap_err().to_string().contains("streaming"));
    Ok(())
}
