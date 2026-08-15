use crate::repositories::DbPoolExt;
use crate::repositories::Operation;
use crate::repositories::events::{EventStorageLane, StreamBatchRow};
use serde_json::json;
use sinex_primitives::Id;
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{EventSource, EventType, HostName, OperationStatus};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn source_status_breakdown_keeps_source_event_type_groups_distinct(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let operation = ctx
        .pool()
        .state()
        .log_operation(Operation {
            id: None,
            operation_type: "import".to_string(),
            operator: "fixture".to_string(),
            scope: None,
            result_status: OperationStatus::Success,
            result_message: None,
            preview_summary: None,
            duration_ms: None,
        })
        .await?;
    let material = ctx.create_source_material(Some("dedup-breakdown")).await?;
    let event_id = uuid::Uuid::now_v7();
    ctx.pool()
        .events()
        .insert_stream_batch_into(
            EventStorageLane::Activity,
            &[StreamBatchRow {
                id: event_id,
                source: EventSource::new("fixture.source")?,
                event_type: EventType::new("fixture.created")?,
                ts_orig: Timestamp::now(),
                ts_quality: None,
                host: HostName::from_static("localhost"),
                payload: json!({"fixture": true}),
                source_material_id: Some(Id::from_uuid(material.to_uuid())),
                anchor_byte: Some(0),
                offset_start: None,
                offset_end: None,
                offset_kind: None,
                source_event_ids: None,
                payload_schema_id: None,
                module_run_id: None,
                associated_blob_ids: None,
                anchor_payload_hash: None,
                temporal_policy: None,
                semantics_version: None,
                scope_key: None,
                equivalence_key: None,
                created_by_operation_id: Some(operation.id.to_uuid()),
                automaton_model: None,
                product_class: None,
                claim_support: None,
                derivation_declaration_id: None,
                derivation_epoch_id: None,
                derivation_lane_id: None,
                adjudication_event_id: None,
                content_hash: None,
            }],
        )
        .await?;
    ctx.pool()
        .import_outcomes()
        .record_suppressed(
            Some(operation.id.to_uuid()),
            uuid::Uuid::now_v7(),
            Some(material.to_uuid()),
            "fixture.source",
            "fixture.deleted",
            "same occurrence",
            Some(event_id),
        )
        .await?;

    let rows = ctx
        .pool()
        .import_outcomes()
        .source_status_breakdown(
            &[
                ("fixture.source".to_string(), "fixture.created".to_string()),
                ("fixture.source".to_string(), "fixture.deleted".to_string()),
            ],
            3,
        )
        .await?;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].event_type, "fixture.created");
    assert_eq!(rows[0].admitted, 1);
    assert_eq!(rows[0].suppressed, 0);
    assert_eq!(rows[0].examples[0].outcome, "admitted");
    assert_eq!(rows[1].event_type, "fixture.deleted");
    assert_eq!(rows[1].admitted, 0);
    assert_eq!(rows[1].suppressed, 1);
    assert_eq!(rows[1].examples[0].outcome, "suppressed");
    assert_eq!(rows[1].examples[0].existing_event_id, Some(event_id));
    Ok(())
}
