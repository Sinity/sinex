use super::{
    handle_telemetry_event_engine_validation, handle_telemetry_throughput, throughput_component,
};
use serde_json::json;
use sinex_db::repositories::{DbPoolExt, EventStorageLane, StreamBatchRow};
use sinex_primitives::domain::{EventSource, EventType, HostName};
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::rpc::telemetry::{
    TelemetryEventEngineValidationRequest, TelemetryThroughputRequest,
};
use sinex_primitives::{Id, Timestamp, Uuid};
use xtask::sandbox::{TestContext, TestResult, sinex_test};

fn reflection_row(
    material_id: Id<SourceMaterial>,
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
) -> color_eyre::Result<StreamBatchRow> {
    Ok(StreamBatchRow {
        id: Uuid::now_v7(),
        source: EventSource::new(source)?,
        event_type: EventType::new(event_type)?,
        ts_orig: Timestamp::now(),
        host: HostName::from_static("localhost"),
        payload,
        source_material_id: Some(material_id),
        anchor_byte: Some(0),
        offset_start: None,
        offset_end: None,
        offset_kind: None,
        source_event_ids: None,
        payload_schema_id: None,
        module_run_id: None,
        anchor_payload_hash: None,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        ts_quality: None,
    })
}

#[sinex_test]
async fn throughput_component_uses_source_role_reflection_bucket() -> xtask::TestResult<()> {
    assert_eq!(throughput_component("sinex"), "reflection");
    assert_eq!(throughput_component("sinex.metric"), "reflection");
    assert_eq!(throughput_component("sinexd.event_engine"), "reflection");
    assert_eq!(throughput_component("sinexd.api.gateway"), "gateway");
    assert_eq!(throughput_component("derived.interval-lift"), "derived");
    assert_eq!(throughput_component("terminal.atuin"), "ingestion");
    Ok(())
}

#[sinex_test]
async fn event_engine_validation_reads_reflection_events(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("telemetry-validation-reflection"))
        .await?;
    ctx.pool
        .events()
        .insert_stream_batch_into(
            EventStorageLane::Reflection,
            &[reflection_row(
                material_id,
                "sinexd.event_engine",
                "batch.stats",
                json!({
                    "batch_size": 7,
                    "fetch_to_ack_ms": 11,
                    "events_deferred": 2,
                    "events_failed": 1,
                    "had_derived": true,
                    "insert_path": "query-builder",
                    "validation_valid": 5,
                    "validation_skipped": 1,
                    "validation_no_schema": 1,
                    "validation_schema_not_found": 0,
                    "validation_invalid": 1,
                    "validation_coverage_pct": 71.5,
                    "suspicious_future_ts_orig": 3
                }),
            )?],
        )
        .await?;

    let response = handle_telemetry_event_engine_validation(
        &ctx.pool,
        TelemetryEventEngineValidationRequest {},
    )
    .await?;
    let snapshot = response
        .snapshot
        .expect("reflection batch.stats row should produce a validation snapshot");

    assert_eq!(snapshot.batch_size, 7);
    assert_eq!(snapshot.fetch_to_ack_ms, 11);
    assert_eq!(snapshot.events_deferred, 2);
    assert_eq!(snapshot.events_failed, 1);
    assert!(snapshot.had_derived);
    assert_eq!(snapshot.insert_path, "query-builder");
    assert_eq!(snapshot.validation_valid, 5);
    assert_eq!(snapshot.validation_coverage_pct, 71.5);
    assert_eq!(snapshot.suspicious_future_ts_orig, 3);
    Ok(())
}

#[sinex_test]
async fn throughput_reads_reflection_events(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("telemetry-throughput-reflection"))
        .await?;
    ctx.pool
        .events()
        .insert_stream_batch_into(
            EventStorageLane::Reflection,
            &[reflection_row(
                material_id,
                "sinexd.event_engine",
                "batch.stats",
                json!({
                    "batch_size": 1,
                    "fetch_to_ack_ms": 1,
                    "events_deferred": 0,
                    "events_failed": 0,
                    "had_derived": false,
                    "insert_path": "query-builder",
                    "validation_valid": 1,
                    "validation_skipped": 0,
                    "validation_no_schema": 0,
                    "validation_schema_not_found": 0,
                    "validation_invalid": 0,
                    "validation_coverage_pct": 100.0
                }),
            )?],
        )
        .await?;

    let response = handle_telemetry_throughput(&ctx.pool, TelemetryThroughputRequest {}).await?;

    let source = response
        .per_source
        .iter()
        .find(|entry| entry.source == "sinexd.event_engine")
        .expect("reflection source should appear in throughput source rows");
    assert!(source.events_last_1h >= 1);
    let reflection = response
        .per_component
        .iter()
        .find(|entry| entry.component == "reflection")
        .expect("reflection component should include reflection.events rows");
    assert!(reflection.eps_1h > 0.0);
    Ok(())
}
