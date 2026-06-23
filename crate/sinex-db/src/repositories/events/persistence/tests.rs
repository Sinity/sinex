
use super::*;
use serde_json::json;
use sinex_primitives::Result;
use sinex_primitives::domain::{EventType, HostName};
use sinex_primitives::events::EventId;
use xtask::sandbox::sinex_test;

fn base_stream_batch_row() -> Result<StreamBatchRow> {
    Ok(StreamBatchRow {
        id: Uuid::now_v7(),
        source: EventSource::new("test.source")?,
        event_type: EventType::new("test.event")?,
        ts_orig: Timestamp::now(),
        host: HostName::from_static("localhost"),
        payload: json!({"ok": true}),
        source_material_id: None,
        anchor_byte: None,
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
        created_by_operation_id: None,
        automaton_model: None,
        ts_quality: None,
    })
}

fn base_record() -> EventRecord {
    let ts = Timestamp::now();
    let subnano = ts.nanosecond() as i32;
    EventRecord {
        id: uuid::Uuid::now_v7(),
        source: "test.source".to_string(),
        event_type: "test.event".to_string(),
        host: "localhost".to_string(),
        payload: json!({"ok": true}),
        ts_orig: ts,
        ts_orig_subnano: Some(subnano),
        ts_coided: Timestamp::now(),
        ts_persisted: Timestamp::now(),
        source_material_id: None,
        anchor_byte: None,
        offset_start: None,
        offset_end: None,
        offset_kind: None,
        source_event_ids: None,
        anchor_payload_hash: None,
        associated_blob_ids: None,
        payload_schema_id: None,
        module_run_id: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        ts_quality: None,
    }
}

#[sinex_test]
async fn missing_provenance_is_rejected() -> Result<()> {
    let record = base_record();
    let err = record.try_to_event().expect_err("should fail");
    assert!(format!("{err}").contains("missing provenance"));
    Ok(())
}

#[sinex_test]
async fn material_provenance_requires_anchor() -> Result<()> {
    let mut record = base_record();
    record.source_material_id = Some(uuid::Uuid::now_v7());
    let err = record.try_to_event().expect_err("should fail");
    assert!(format!("{err}").contains("anchor"));
    Ok(())
}

#[sinex_test]
async fn valid_material_provenance_passes() -> Result<()> {
    let mut record = base_record();
    record.source_material_id = Some(uuid::Uuid::now_v7());
    record.anchor_byte = Some(42);
    assert!(record.try_to_event().is_ok());
    Ok(())
}

#[sinex_test]
async fn invalid_material_offset_kind_is_rejected() -> Result<()> {
    let mut record = base_record();
    record.source_material_id = Some(uuid::Uuid::now_v7());
    record.anchor_byte = Some(42);
    record.offset_kind = Some("mystery".to_string());
    let err = record.try_to_event().expect_err("should fail");
    assert!(format!("{err}").contains("invalid offset kind"));
    Ok(())
}

#[sinex_test]
async fn synthesis_provenance_requires_non_empty_sources() -> Result<()> {
    let mut record = base_record();
    record.source_event_ids = Some(vec![]);
    let err = record.try_to_event().expect_err("should fail");
    assert!(format!("{err}").contains("source_event_ids"));
    Ok(())
}

#[sinex_test]
async fn synthesis_operation_lineage_round_trips_from_record() -> Result<()> {
    let mut record = base_record();
    let parent_id = uuid::Uuid::now_v7();
    let operation_id = uuid::Uuid::now_v7();
    record.source_event_ids = Some(vec![parent_id]);
    record.created_by_operation_id = Some(operation_id);

    let event = record.try_to_event()?;

    match &event.provenance {
        crate::models::Provenance::Derived {
            source_event_ids,
            operation_id: provenance_operation_id,
        } => {
            assert_eq!(
                source_event_ids.as_slice(),
                &[sinex_primitives::events::EventId::from_uuid(parent_id)]
            );
            assert_eq!(
                provenance_operation_id.as_ref().map(Id::to_uuid),
                Some(operation_id)
            );
        }
        other => panic!("expected derived provenance, got {other:?}"),
    }
    assert_eq!(event.created_by_operation_id, Some(operation_id));

    Ok(())
}

#[sinex_test]
async fn mismatched_operation_lineage_is_rejected() -> Result<()> {
    let parent_id = Id::<Event<JsonValue>>::new();
    let provenance_operation_id = Id::<sinex_primitives::events::builder::OperationMarker>::new();
    let event = Event {
        id: Some(Id::new()),
        source: EventSource::new("test.source")?,
        event_type: EventType::new("test.event")?,
        host: HostName::from_static("localhost"),
        payload: json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        ts_quality: None,
        module_run_id: None,
        payload_schema_id: None,
        provenance: crate::models::Provenance::from_derived([
            sinex_primitives::events::EventId::from_uuid(parent_id.to_uuid()),
        ])
        .expect("single parent should produce derived provenance")
        .with_operation(provenance_operation_id),
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: Some(uuid::Uuid::now_v7()),
        automaton_model: None,
        anchor_payload_hash: None,
    };

    let err = resolved_created_by_operation_id(&event).expect_err("should fail");
    assert!(format!("{err}").contains("operation lineage mismatch"));

    Ok(())
}

#[sinex_test]
async fn stream_batch_insert_strategy_prefers_query_builder_for_small_material_batches()
-> Result<()> {
    let batch = vec![base_stream_batch_row()?];
    assert_eq!(
        EventRepository::stream_batch_insert_strategy(&batch),
        Some(StreamBatchInsertStrategy::QueryBuilder)
    );
    Ok(())
}

#[sinex_test]
async fn stream_batch_insert_strategy_prefers_copy_for_large_material_batches() -> Result<()> {
    let batch = (0..COPY_BATCH_THRESHOLD)
        .map(|_| base_stream_batch_row())
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        EventRepository::stream_batch_insert_strategy(&batch),
        Some(StreamBatchInsertStrategy::Copy)
    );
    Ok(())
}

#[sinex_test]
async fn stream_batch_insert_strategy_prefers_synthesis_for_parent_batches() -> Result<()> {
    let mut row = base_stream_batch_row()?;
    row.source_event_ids = Some(vec![EventId::from_uuid(Uuid::now_v7())]);
    let batch = vec![row];
    assert_eq!(
        EventRepository::stream_batch_insert_strategy(&batch),
        Some(StreamBatchInsertStrategy::Derived)
    );
    Ok(())
}

#[sinex_test]
async fn derived_insert_rejects_non_live_parent(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let missing_parent = Id::<Event<JsonValue>>::new();
    let provenance =
        crate::models::Provenance::from_derived([EventId::from_uuid(missing_parent.to_uuid())])
            .expect("expected derived provenance");
    let event = Event {
        id: Some(Id::new()),
        source: EventSource::new("test.source")?,
        event_type: EventType::new("test.derived")?,
        host: HostName::from_static("localhost"),
        payload: json!({"derived": true}),
        ts_orig: Some(Timestamp::now()),
        ts_quality: None,
        module_run_id: None,
        payload_schema_id: None,
        provenance,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        anchor_payload_hash: None,
    };

    let error = match EventRepository::new(ctx.pool()).insert(event).await {
        Ok(_) => {
            panic!("derived insert unexpectedly accepted a missing parent");
        }
        Err(error) => error,
    };

    assert!(
        format!("{error}").contains("non-live source_event_ids"),
        "unexpected error: {error}"
    );

    Ok(())
}

#[sinex_test]
async fn copy_staging_probe_avoids_repeated_create_notice() -> Result<()> {
    let probe_sql = EventRepository::copy_staging_exists_sql();
    assert!(
        probe_sql.contains("pg_temp.sinex_batch_staging"),
        "staging-table probe must be scoped to the current connection's temp schema"
    );

    let create_sql = EventRepository::copy_staging_create_sql("id UUID");
    assert!(
        !create_sql.contains("IF NOT EXISTS"),
        "COPY staging setup must not use CREATE IF NOT EXISTS; PostgreSQL emits a NOTICE \
             on every reused temp table and sqlx forwards it to journald"
    );
    assert!(
        create_sql.contains("CREATE TEMP TABLE sinex_batch_staging"),
        "COPY staging table should still be a connection-local temporary table"
    );

    Ok(())
}

/// Drift guard: the column set used in the `query_as!` single-insert sites
/// must equal the COPY-contract column set (`EVENT_COPY_COLUMNS`). The
/// `query_as!` macro verifies individual column types at compile time but
/// does NOT verify that every column in the COPY contract appears in the
/// VALUES list, so set-drift can go undetected until runtime. This test
/// catches that by asserting the same names appear in both. (#1575)
#[sinex_test]
async fn query_as_insert_columns_match_copy_contract() -> Result<()> {
    // These are the 24 columns listed in both `insert` and `insert_with_tx`
    // `query_as!` sites. If those sites ever gain or lose a column, this
    // constant must be updated — which forces an explicit review of whether
    // EVENT_COPY_COLUMNS was updated too.
    let query_as_columns: std::collections::BTreeSet<String> = [
        "id",
        "source",
        "event_type",
        "host",
        "payload",
        "ts_orig",
        "ts_orig_subnano",
        "ts_quality",
        "module_run_id",
        "payload_schema_id",
        "source_event_ids",
        "source_material_id",
        "offset_start",
        "offset_end",
        "offset_kind",
        "anchor_byte",
        "associated_blob_ids",
        "temporal_policy",
        "semantics_version",
        "scope_key",
        "equivalence_key",
        "created_by_operation_id",
        "automaton_model",
        "anchor_payload_hash",
    ]
    .iter()
    .map(std::string::ToString::to_string)
    .collect();

    // Parse the SSOT column list from EVENT_COPY_COLUMNS.
    let copy_sql = event_copy_column_list_sql();
    let copy_columns: std::collections::BTreeSet<String> =
        copy_sql.split(", ").map(|s| s.trim().to_string()).collect();

    // BTreeSet comparison gives a sorted diff in the assertion message.
    assert_eq!(
        query_as_columns, copy_columns,
        "query_as! single-insert column set diverges from EVENT_COPY_COLUMNS (the SSOT). \
             Add the missing column to both the query_as! INSERT and EVENT_COPY_COLUMNS, \
             or remove it from both."
    );
    Ok(())
}
