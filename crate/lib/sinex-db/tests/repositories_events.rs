use serde_json::json;
use sinex_db::repositories::{
    COPY_BATCH_THRESHOLD, DbPoolExt, ReplacementKind, ReplacementRecord, StreamBatchRow,
};
use sinex_db::{Event, Provenance};
use sinex_primitives::Id;
use sinex_primitives::Pagination;
use sinex_primitives::Timestamp;
use sinex_primitives::Uuid;
use sinex_primitives::domain::{
    DerivedNodeModel, EventSource, EventType, HostName, RecordedPath, SyntheticTemporalPolicy,
};
use sinex_primitives::events::payloads::{FileCreatedPayload, KittyCommandExecutedPayload};
use sinex_primitives::events::{DynamicPayload, EventId, SourceMaterial};
use xtask::sandbox::sinex_test;

fn stream_batch_material_row(
    material_id: Id<SourceMaterial>,
    anchor_byte: i64,
) -> color_eyre::Result<StreamBatchRow> {
    Ok(StreamBatchRow {
        id: Uuid::now_v7(),
        source: EventSource::new("test.source")?,
        event_type: EventType::new("test.batch.material")?,
        ts_orig: Timestamp::now(),
        host: HostName::from_static("localhost"),
        payload: json!({ "anchor": anchor_byte }),
        source_material_id: Some(material_id),
        anchor_byte: Some(anchor_byte),
        offset_start: None,
        offset_end: None,
        offset_kind: None,
        source_event_ids: None,
        payload_schema_id: None,
        node_run_id: None,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        node_model: None,
    })
}

#[sinex_test]
async fn events_repository_inserts_typed_events(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("test-event-source-material"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    let mut payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/tmp/repo-insert.txt")
            .map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    payload.size = 512;
    let event = Event::new(
        payload,
        Provenance::from_material(material_id, 0, None, None),
    );
    let expected_host = event.host.clone();
    let inserted = ctx.pool.events().insert(event).await?;
    assert_eq!(inserted.source.as_str(), "fs-watcher");
    assert_eq!(inserted.event_type.as_str(), "file.created");
    assert_eq!(inserted.host, expected_host);
    assert_eq!(inserted.payload["path"], json!("/tmp/repo-insert.txt"));
    assert_eq!(inserted.payload["size"], json!(512));
    assert!(inserted.id.is_some());
    Ok(())
}

#[sinex_test]
async fn events_repository_preserves_provenance(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("test-source-material"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    let source_payload = KittyCommandExecutedPayload::test_default("echo provenance");
    let source_event = Event::new(
        source_payload,
        Provenance::from_material(material_id, 0, None, None),
    );

    let source = ctx.pool.events().insert(source_event).await?;
    let source_id = source.id.unwrap();

    let derived_payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/tmp/derived.txt").map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let derived_event = Event::builder(derived_payload)
        .from_parents(vec![source_id])?
        .build()?;

    let inserted = ctx.pool.events().insert(derived_event).await?;
    match inserted.provenance() {
        Provenance::Synthesis {
            source_event_ids, ..
        } => {
            assert_eq!(source_event_ids.len(), 1);
            assert_eq!(source_event_ids[0], source_id);
        }
        other => unreachable!("Expected synthesis provenance, got: {other:?}"),
    }
    Ok(())
}

#[sinex_test]
async fn events_repository_rejects_unknown_node_run_id(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("test-node-run-integrity-material"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    let payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/tmp/node-run-integrity.txt")
            .map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let event = Event::new(
        payload,
        Provenance::from_material(material_id, 0, None, None),
    )
    .with_node_run_id(Uuid::now_v7());

    let error = ctx
        .pool
        .events()
        .insert(event)
        .await
        .expect_err("unknown node_run_id must be rejected");
    let message = error.to_string();
    let normalized_message = message.to_lowercase();
    assert!(
        normalized_message.contains("node_run")
            || normalized_message.contains("foreign key")
            || normalized_message.contains("constraint violation"),
        "unexpected error message: {message}"
    );

    Ok(())
}

#[sinex_test]
async fn stream_batch_insert_accepts_large_material_batches(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("stream-batch-large-material"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<SourceMaterial>::from_uuid(material_record.id);

    let batch = (0..COPY_BATCH_THRESHOLD)
        .map(|index| stream_batch_material_row(material_id, index as i64))
        .collect::<color_eyre::Result<Vec<_>>>()?;

    let result = ctx.pool.events().insert_stream_batch(&batch).await?;
    assert_eq!(result.inserted_count, COPY_BATCH_THRESHOLD);
    assert_eq!(
        result.inserted_ids.as_ref().map(std::vec::Vec::len),
        Some(COPY_BATCH_THRESHOLD)
    );

    let stored = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::new("test.source")?,
            sinex_primitives::Pagination::new(None, None),
        )
        .await?;
    assert_eq!(
        stored
            .iter()
            .filter(|event| event.event_type.as_str() == "test.batch.material")
            .count(),
        COPY_BATCH_THRESHOLD
    );
    Ok(())
}

#[sinex_test]
async fn lifecycle_id_queries_order_same_timestamp_rows_by_id(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("lifecycle-id-ordering-material"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<SourceMaterial>::from_uuid(material_record.id);
    let source = EventSource::new("test.lifecycle.order")?;
    let ts_orig = Timestamp::parse_rfc3339("2026-01-01T00:00:00Z")?;

    let first_id = Uuid::now_v7();
    let second_id = Uuid::now_v7();
    let (lower_id, higher_id) = if first_id.as_u128() <= second_id.as_u128() {
        (first_id, second_id)
    } else {
        (second_id, first_id)
    };

    let mut higher_row = stream_batch_material_row(material_id, 1)?;
    higher_row.id = higher_id;
    higher_row.source = source.clone();
    higher_row.ts_orig = ts_orig;

    let mut lower_row = stream_batch_material_row(material_id, 0)?;
    lower_row.id = lower_id;
    lower_row.source = source.clone();
    lower_row.ts_orig = ts_orig;

    ctx.pool()
        .events()
        .insert_stream_batch(&[higher_row, lower_row])
        .await?;

    let live_ids = ctx
        .pool()
        .events()
        .get_live_event_ids(Some(&source), None, 10)
        .await?;
    assert_eq!(live_ids, vec![lower_id, higher_id]);

    let archive_operation_id = Uuid::now_v7().to_string();
    ctx.pool()
        .events()
        .execute_cascade_archive(
            &[higher_id, lower_id],
            "archive lifecycle ordering regression",
            &archive_operation_id,
            "test",
        )
        .await?;

    let archived_ids = ctx
        .pool()
        .events()
        .get_archived_event_ids(Some(&source), None, 10)
        .await?;
    assert_eq!(archived_ids, vec![lower_id, higher_id]);
    Ok(())
}

#[sinex_test]
async fn delete_by_source_archives_events(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("delete-by-source-material"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<SourceMaterial>::from_uuid(material_record.id);
    let source = EventSource::new("test.repo.delete.by_source")?;

    let event = DynamicPayload::new(
        source.as_str(),
        "test.repo.delete.by_source",
        json!({ "deleted": true }),
    )
    .from_material(material_id)
    .build()?;

    let inserted = ctx.pool.events().insert(event).await?;
    let inserted_id = inserted.id.expect("inserted event must have an id");

    let deleted = ctx.pool.events().delete_by_source(&source).await?;
    assert_eq!(
        deleted, 1,
        "delete_by_source should delete the matching event"
    );

    let live_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
            .bind(inserted_id.to_uuid())
            .fetch_one(ctx.pool())
            .await?;
    assert_eq!(
        live_count.0, 0,
        "deleted event should not remain in core.events"
    );

    let archived_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid")
            .bind(inserted_id.to_uuid())
            .fetch_one(ctx.pool())
            .await?;
    assert_eq!(
        archived_count.0, 1,
        "deleted event should be archived by the trigger"
    );

    Ok(())
}

#[sinex_test]
async fn get_material_root_events_in_range_excludes_synthesis_rows(
    ctx: TestContext,
) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("material-root-range-filter"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<SourceMaterial>::from_uuid(material_record.id);
    let source = EventSource::new("test.repo.range.filter")?;
    let start = Timestamp::now() - time::Duration::seconds(1);

    let material_event = DynamicPayload::new(
        source.as_str(),
        "test.repo.range.material",
        json!({ "kind": "material" }),
    )
    .from_material(material_id)
    .build()?;
    let inserted_material = ctx.pool.events().insert(material_event).await?;
    let material_event_id = inserted_material
        .id
        .expect("material event must have an id");

    let derived_event = DynamicPayload::new(
        source.as_str(),
        "test.repo.range.derived",
        json!({ "kind": "derived" }),
    )
    .from_parents(vec![material_event_id])?
    .build()?;
    ctx.pool.events().insert(derived_event).await?;

    let end = Timestamp::now() + time::Duration::seconds(1);
    let stored = ctx
        .pool
        .events()
        .get_material_root_events_in_range(&source, start, end, Pagination::new(Some(10), None))
        .await?;

    assert_eq!(
        stored.len(),
        1,
        "only material-provenance rows should be returned"
    );
    assert_eq!(stored[0].event_type.as_str(), "test.repo.range.material");
    assert!(
        matches!(stored[0].provenance(), Provenance::Material { .. }),
        "material-root query must not return synthesis rows"
    );

    Ok(())
}

#[sinex_test]
async fn stream_batch_insert_rejects_self_referential_synthesis_rows(
    ctx: TestContext,
) -> TestResult<()> {
    let _ = ctx;
    let event_id = Uuid::now_v7();
    let batch = vec![StreamBatchRow {
        id: event_id,
        source: EventSource::new("test.source")?,
        event_type: EventType::new("test.batch.synthesis")?,
        ts_orig: Timestamp::now(),
        host: HostName::from_static("localhost"),
        payload: json!({ "self_ref": true }),
        source_material_id: None,
        anchor_byte: None,
        offset_start: None,
        offset_end: None,
        offset_kind: None,
        source_event_ids: Some(vec![EventId::from_uuid(event_id)]),
        payload_schema_id: None,
        node_run_id: None,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        node_model: None,
    }];

    let error = ctx
        .pool
        .events()
        .insert_stream_batch(&batch)
        .await
        .expect_err("self-referential synthesis batch should be rejected");
    assert!(
        error
            .to_string()
            .contains("cycle detected in synthesis provenance"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn stream_batch_insert_rejects_intra_batch_synthesis_cycles(
    ctx: TestContext,
) -> TestResult<()> {
    let _ = ctx;
    let first_id = Uuid::now_v7();
    let second_id = Uuid::now_v7();
    let batch = vec![
        StreamBatchRow {
            id: first_id,
            source: EventSource::new("test.source")?,
            event_type: EventType::new("test.batch.synthesis")?,
            ts_orig: Timestamp::now(),
            host: HostName::from_static("localhost"),
            payload: json!({ "cycle": "first" }),
            source_material_id: None,
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: Some(vec![EventId::from_uuid(second_id)]),
            payload_schema_id: None,
            node_run_id: None,
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        },
        StreamBatchRow {
            id: second_id,
            source: EventSource::new("test.source")?,
            event_type: EventType::new("test.batch.synthesis")?,
            ts_orig: Timestamp::now(),
            host: HostName::from_static("localhost"),
            payload: json!({ "cycle": "second" }),
            source_material_id: None,
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: Some(vec![EventId::from_uuid(first_id)]),
            payload_schema_id: None,
            node_run_id: None,
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        },
    ];

    let error = ctx
        .pool
        .events()
        .insert_stream_batch(&batch)
        .await
        .expect_err("intra-batch synthesis cycle should be rejected");
    assert!(
        error
            .to_string()
            .contains("cycle detected in synthesis provenance within batch"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn register_external_in_flight_uses_provided_id(ctx: TestContext) -> TestResult<()> {
    let forced_id = uuid::Uuid::now_v7();
    let identifier = format!("test-material-{forced_id}");
    let record = ctx
        .pool
        .source_materials()
        .register_external_in_flight(
            forced_id,
            sinex_db::repositories::source_materials::material_types::FILE,
            Some(&identifier),
            json!({"note": "external registration"}),
            Timestamp::now(),
        )
        .await?;

    assert_eq!(record.id, forced_id);
    assert_eq!(record.source_identifier, identifier);
    Ok(())
}

#[sinex_test]
async fn register_external_in_flight_resets_terminal_status_to_sensing(
    ctx: TestContext,
) -> TestResult<()> {
    let forced_id = uuid::Uuid::now_v7();
    let identifier = format!("test-material-restart-{forced_id}");
    let started_at = Timestamp::now();
    let record = ctx
        .pool
        .source_materials()
        .register_external_in_flight(
            forced_id,
            sinex_db::repositories::source_materials::material_types::FILE,
            Some(&identifier),
            json!({"note": "first registration"}),
            started_at,
        )
        .await?;
    ctx.pool
        .source_materials()
        .mark_as_failed(
            Id::<sinex_db::SourceMaterialRecord>::from_uuid(record.id),
            "synthetic failure",
        )
        .await?;

    let restarted = ctx
        .pool
        .source_materials()
        .register_external_in_flight(
            forced_id,
            sinex_db::repositories::source_materials::material_types::FILE,
            Some(&identifier),
            json!({"note": "restart"}),
            Timestamp::now(),
        )
        .await?;

    assert_eq!(restarted.status, "sensing");
    assert!(restarted.end_time.is_none());
    Ok(())
}

#[sinex_test]
async fn register_external_in_flight_rejects_source_identifier_aliasing(
    ctx: TestContext,
) -> TestResult<()> {
    let original_id = uuid::Uuid::now_v7();
    let conflicting_id = uuid::Uuid::now_v7();
    let identifier = format!("test-material-conflict-{original_id}");

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            original_id,
            sinex_db::repositories::source_materials::material_types::FILE,
            Some(&identifier),
            json!({"note": "first registration"}),
            Timestamp::now(),
        )
        .await?;

    let error = ctx
        .pool
        .source_materials()
        .register_external_in_flight(
            conflicting_id,
            sinex_db::repositories::source_materials::material_types::FILE,
            Some(&identifier),
            json!({"note": "conflicting registration"}),
            Timestamp::now(),
        )
        .await
        .expect_err("different explicit material ids must not alias through source_identifier");

    assert!(error.to_string().contains("source_identifier"));

    let persisted = ctx
        .pool
        .source_materials()
        .get_by_id(Id::<sinex_db::SourceMaterialRecord>::from_uuid(original_id))
        .await?
        .expect("original explicit material id must remain intact");
    assert_eq!(persisted.id, original_id);
    Ok(())
}

// =============================================================================
// SYNTHETIC METADATA ROUNDTRIP TESTS (Slice 3)
// =============================================================================

/// Verifies that all 6 synthetic metadata fields survive insert → load roundtrip.
#[sinex_test]
async fn synthetic_metadata_roundtrips_through_insert(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("synth-meta-roundtrip"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    // Create a source event first (needed as parent for synthesis)
    let source_payload = KittyCommandExecutedPayload::test_default("echo roundtrip");
    let source_event = Event::new(
        source_payload,
        Provenance::from_material(material_id, 0, None, None),
    );
    let source = ctx.pool.events().insert(source_event).await?;
    let source_id = source.id.unwrap();

    // Build derived event with all synthetic metadata populated
    let operation_id = Uuid::now_v7();
    let derived_payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/tmp/synth-meta.txt")
            .map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let mut derived = Event::builder(derived_payload)
        .from_parents(vec![source_id])?
        .build()?;

    derived.temporal_policy = Some(SyntheticTemporalPolicy::LatestInput);
    derived.semantics_version = Some("v2.3.1".to_string());
    derived.scope_key = Some("analytics:daily:2026-03-14".to_string());
    derived.equivalence_key = Some("analytics:daily:2026-03-14:host-a".to_string());
    derived.created_by_operation_id = Some(operation_id);
    derived.node_model = Some(DerivedNodeModel::Windowed);

    let inserted = ctx.pool.events().insert(derived).await?;
    let event_id = inserted.id.unwrap();

    // Verify fields survived insert
    assert_eq!(
        inserted.temporal_policy,
        Some(SyntheticTemporalPolicy::LatestInput)
    );
    assert_eq!(inserted.semantics_version.as_deref(), Some("v2.3.1"));
    assert_eq!(
        inserted.scope_key.as_deref(),
        Some("analytics:daily:2026-03-14")
    );
    assert_eq!(
        inserted.equivalence_key.as_deref(),
        Some("analytics:daily:2026-03-14:host-a")
    );
    assert_eq!(inserted.created_by_operation_id, Some(operation_id));
    assert_eq!(inserted.node_model, Some(DerivedNodeModel::Windowed));

    // Verify fields survive load (get_by_id reads from DB)
    let loaded = ctx.pool.events().get_by_id(event_id).await?.unwrap();
    assert_eq!(
        loaded.temporal_policy,
        Some(SyntheticTemporalPolicy::LatestInput)
    );
    assert_eq!(loaded.semantics_version.as_deref(), Some("v2.3.1"));
    assert_eq!(
        loaded.scope_key.as_deref(),
        Some("analytics:daily:2026-03-14")
    );
    assert_eq!(
        loaded.equivalence_key.as_deref(),
        Some("analytics:daily:2026-03-14:host-a")
    );
    assert_eq!(loaded.created_by_operation_id, Some(operation_id));
    assert_eq!(loaded.node_model, Some(DerivedNodeModel::Windowed));

    Ok(())
}

/// Material events leave all synthetic metadata fields as None.
#[sinex_test]
async fn material_events_have_null_synthetic_metadata(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("null-synth-meta"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    let payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/tmp/plain-material.txt")
            .map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let event = Event::new(
        payload,
        Provenance::from_material(material_id, 0, None, None),
    );

    let inserted = ctx.pool.events().insert(event).await?;
    let event_id = inserted.id.unwrap();

    let loaded = ctx.pool.events().get_by_id(event_id).await?.unwrap();
    assert!(loaded.temporal_policy.is_none());
    assert!(loaded.semantics_version.is_none());
    assert!(loaded.scope_key.is_none());
    assert!(loaded.equivalence_key.is_none());
    assert!(loaded.created_by_operation_id.is_none());
    assert!(loaded.node_model.is_none());

    Ok(())
}

/// All temporal policy enum variants roundtrip correctly.
#[sinex_test]
async fn all_temporal_policy_variants_roundtrip(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("temporal-policy-variants"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    let policies = [
        SyntheticTemporalPolicy::InheritParent,
        SyntheticTemporalPolicy::LatestInput,
        SyntheticTemporalPolicy::WindowBoundary,
        SyntheticTemporalPolicy::DeclaredEffective,
    ];

    // Create parent event
    let source_payload = KittyCommandExecutedPayload::test_default("echo variants");
    let source_event = Event::new(
        source_payload,
        Provenance::from_material(material_id, 0, None, None),
    );
    let source = ctx.pool.events().insert(source_event).await?;
    let source_id = source.id.unwrap();

    for policy in policies {
        let payload = FileCreatedPayload::test_default(
            RecordedPath::from_observed(format!("/tmp/policy-{policy}.txt"))
                .map_err(|e| color_eyre::eyre::eyre!(e))?,
        );
        let mut event = Event::builder(payload)
            .from_parents(vec![source_id])?
            .build()?;
        event.temporal_policy = Some(policy);

        let inserted = ctx.pool.events().insert(event).await?;
        let loaded = ctx
            .pool
            .events()
            .get_by_id(inserted.id.unwrap())
            .await?
            .unwrap();
        assert_eq!(
            loaded.temporal_policy,
            Some(policy),
            "policy {policy} should roundtrip"
        );
    }

    Ok(())
}

/// All node model enum variants roundtrip correctly.
#[sinex_test]
async fn all_node_model_variants_roundtrip(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("node-model-variants"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    let models = [
        DerivedNodeModel::Transducer,
        DerivedNodeModel::Windowed,
        DerivedNodeModel::ScopeReconciler,
    ];

    let source_payload = KittyCommandExecutedPayload::test_default("echo models");
    let source_event = Event::new(
        source_payload,
        Provenance::from_material(material_id, 0, None, None),
    );
    let source = ctx.pool.events().insert(source_event).await?;
    let source_id = source.id.unwrap();

    for model in models {
        let payload = FileCreatedPayload::test_default(
            RecordedPath::from_observed(format!("/tmp/model-{model}.txt"))
                .map_err(|e| color_eyre::eyre::eyre!(e))?,
        );
        let mut event = Event::builder(payload)
            .from_parents(vec![source_id])?
            .build()?;
        event.node_model = Some(model);

        let inserted = ctx.pool.events().insert(event).await?;
        let loaded = ctx
            .pool
            .events()
            .get_by_id(inserted.id.unwrap())
            .await?
            .unwrap();
        assert_eq!(
            loaded.node_model,
            Some(model),
            "model {model} should roundtrip"
        );
    }

    Ok(())
}

/// Synthetic metadata survives batch insert (the COPY/unnest path).
#[sinex_test]
async fn synthetic_metadata_survives_batch_insert(ctx: TestContext) -> TestResult<()> {
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("batch-synth-meta"),
            json!({ "test": true }),
        )
        .await?;
    let material_id = Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    // Create parent events
    let source_payload = KittyCommandExecutedPayload::test_default("echo batch");
    let source_event = Event::new(
        source_payload,
        Provenance::from_material(material_id, 0, None, None),
    );
    let source = ctx.pool.events().insert(source_event).await?;
    let source_id = source.id.unwrap();

    let operation_id = Uuid::now_v7();

    // Build a batch of events with varying metadata
    let mut events = Vec::new();
    for i in 0..5 {
        let payload = FileCreatedPayload::test_default(
            RecordedPath::from_observed(format!("/tmp/batch-{i}.txt"))
                .map_err(|e| color_eyre::eyre::eyre!(e))?,
        );
        let mut event = Event::builder(payload)
            .from_parents(vec![source_id])?
            .build()?;
        event.temporal_policy = Some(SyntheticTemporalPolicy::LatestInput);
        event.semantics_version = Some(format!("v1.{i}"));
        event.scope_key = Some(format!("batch-scope:{i}"));
        event.equivalence_key = Some(format!("batch-equiv:{i}"));
        event.created_by_operation_id = Some(operation_id);
        event.node_model = Some(DerivedNodeModel::Transducer);
        events.push(event);
    }

    let inserted = ctx.pool.events().insert_batch(events).await?;
    assert_eq!(inserted.len(), 5);

    for (i, ev) in inserted.iter().enumerate() {
        let loaded = ctx.pool.events().get_by_id(ev.id.unwrap()).await?.unwrap();
        assert_eq!(
            loaded.temporal_policy,
            Some(SyntheticTemporalPolicy::LatestInput)
        );
        assert_eq!(
            loaded.semantics_version.as_deref(),
            Some(format!("v1.{i}").as_str())
        );
        assert_eq!(
            loaded.scope_key.as_deref(),
            Some(format!("batch-scope:{i}").as_str())
        );
        assert_eq!(
            loaded.equivalence_key.as_deref(),
            Some(format!("batch-equiv:{i}").as_str())
        );
        assert_eq!(loaded.created_by_operation_id, Some(operation_id));
        assert_eq!(loaded.node_model, Some(DerivedNodeModel::Transducer));
    }

    Ok(())
}

#[sinex_test]
async fn batch_insert_rolls_back_all_chunks_on_late_failure(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("batch-atomicity-material"))
        .await?;
    let source = EventSource::new("batch-atomicity-test")?;
    let event_type = EventType::new("batch.atomicity")?;
    let duplicate_id = Some(Id::<Event<serde_json::Value>>::new());

    let mut events = Vec::new();
    for index in 0..=COPY_BATCH_THRESHOLD {
        let mut event = DynamicPayload::new(
            source.clone(),
            event_type.clone(),
            json!({ "index": index }),
        )
        .from_material(material_id)
        .build()?;

        if index == 0 || index == COPY_BATCH_THRESHOLD {
            event.id = duplicate_id;
        }

        events.push(event);
    }

    let error = ctx
        .pool
        .events()
        .insert_batch(events)
        .await
        .expect_err("late invalid row should fail the whole batch");
    assert!(
        !format!("{error}").is_empty(),
        "batch failure should preserve useful error context"
    );

    let stored = ctx
        .pool
        .events()
        .get_by_source(&source, Pagination::new(Some(200), None))
        .await?;
    assert!(
        stored.is_empty(),
        "no chunk from the failed batch should remain committed"
    );

    Ok(())
}

#[sinex_test]
async fn batch_insert_rejects_intra_batch_synthesis_cycles(ctx: TestContext) -> TestResult<()> {
    let first_id = Id::<Event<serde_json::Value>>::new();
    let second_id = Id::<Event<serde_json::Value>>::new();
    let source = EventSource::new("batch-cycle-test")?;
    let event_type = EventType::new("batch.cycle")?;

    let mut first = DynamicPayload::new(
        source.clone(),
        event_type.clone(),
        json!({ "cycle": "first" }),
    )
    .from_parents(vec![EventId::from_uuid(*second_id.as_uuid())])?
    .build()?;
    first.id = Some(first_id);

    let mut second = DynamicPayload::new(source, event_type, json!({ "cycle": "second" }))
        .from_parents(vec![EventId::from_uuid(*first_id.as_uuid())])?
        .build()?;
    second.id = Some(second_id);

    let error = ctx
        .pool
        .events()
        .insert_batch(vec![first, second])
        .await
        .expect_err("intra-batch synthesis cycle should be rejected");
    assert!(
        error
            .to_string()
            .contains("cycle detected in synthesis provenance within batch"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn batch_insert_rejects_cross_chunk_intra_batch_synthesis_cycles(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("batch-cross-chunk-cycle-material"))
        .await?;
    let source = EventSource::new("batch-cross-chunk-cycle-test")?;
    let event_type = EventType::new("batch.cross_chunk_cycle")?;
    let first_id = Id::<Event<serde_json::Value>>::new();
    let second_id = Id::<Event<serde_json::Value>>::new();

    let mut events = Vec::new();
    for index in 0..49 {
        let event = DynamicPayload::new(
            source.clone(),
            event_type.clone(),
            json!({ "filler": index }),
        )
        .from_material(material_id)
        .build()?;
        events.push(event);
    }

    let mut first = DynamicPayload::new(
        source.clone(),
        event_type.clone(),
        json!({ "cycle": "first" }),
    )
    .from_parents(vec![EventId::from_uuid(*second_id.as_uuid())])?
    .build()?;
    first.id = Some(first_id);
    events.push(first);

    let mut second = DynamicPayload::new(
        source.clone(),
        event_type.clone(),
        json!({ "cycle": "second" }),
    )
    .from_parents(vec![EventId::from_uuid(*first_id.as_uuid())])?
    .build()?;
    second.id = Some(second_id);
    events.push(second);

    assert_eq!(events.len(), 51, "test must span the 50-row chunk boundary");

    let error = ctx
        .pool
        .events()
        .insert_batch(events)
        .await
        .expect_err("cross-chunk intra-batch synthesis cycle should be rejected");
    assert!(
        error
            .to_string()
            .contains("cycle detected in synthesis provenance within batch"),
        "unexpected error: {error}"
    );

    let stored = ctx
        .pool
        .events()
        .get_by_source(&source, Pagination::new(Some(100), None))
        .await?;
    assert!(
        stored.is_empty(),
        "failed cross-chunk synthesis batch must not partially commit"
    );

    Ok(())
}

#[sinex_test]
async fn event_replacements_record_and_query(ctx: TestContext) -> TestResult<()> {
    let operation_id = Uuid::now_v7();
    let old_event_1 = Uuid::now_v7();
    let old_event_2 = Uuid::now_v7();
    let new_event_1 = Uuid::now_v7();
    let new_event_2 = Uuid::now_v7();

    let replacements = vec![
        ReplacementRecord {
            old_event_id: old_event_1,
            new_event_id: new_event_1,
            relation_kind: ReplacementKind::Superseded,
            scope_key: Some("scope:fs".to_string()),
            equivalence_key: Some("eq:file1".to_string()),
        },
        ReplacementRecord {
            old_event_id: old_event_2,
            new_event_id: new_event_2,
            relation_kind: ReplacementKind::Recomputed,
            scope_key: None,
            equivalence_key: None,
        },
        ReplacementRecord {
            old_event_id: old_event_1,
            new_event_id: new_event_2,
            relation_kind: ReplacementKind::Split,
            scope_key: Some("scope:fs".to_string()),
            equivalence_key: None,
        },
    ];

    let count = ctx
        .pool
        .events()
        .record_replacements(operation_id, &replacements)
        .await?;
    assert_eq!(count, 3, "should insert all 3 replacement records");

    // Query by operation
    let by_op = ctx
        .pool
        .events()
        .get_replacements_by_operation(operation_id)
        .await?;
    assert_eq!(by_op.len(), 3);

    // Verify the first record
    let superseded = by_op.iter().find(|r| r.2 == "superseded").unwrap();
    assert_eq!(superseded.0, old_event_1);
    assert_eq!(superseded.1, new_event_1);

    // Query by old event — returns (new_event_id, relation_kind, operation_id)
    let for_event = ctx
        .pool
        .events()
        .get_replacements_for_event(old_event_1)
        .await?;
    assert_eq!(
        for_event.len(),
        2,
        "old_event_1 has two replacement records"
    );

    // Query for old_event_2
    let for_event_2 = ctx
        .pool
        .events()
        .get_replacements_for_event(old_event_2)
        .await?;
    assert_eq!(for_event_2.len(), 1);
    assert_eq!(for_event_2[0].1, "recomputed");
    assert_eq!(for_event_2[0].2, operation_id);

    Ok(())
}
