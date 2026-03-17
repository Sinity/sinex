use serde_json::json;
use sinex_db::repositories::{DbPoolExt, ReplacementKind, ReplacementRecord};
use sinex_db::{Event, Provenance};
use sinex_primitives::Id;
use sinex_primitives::Timestamp;
use sinex_primitives::Uuid;
use sinex_primitives::domain::{DerivedNodeModel, RecordedPath, SyntheticTemporalPolicy};
use sinex_primitives::events::payloads::{FileCreatedPayload, KittyCommandExecutedPayload};
use xtask::sandbox::sinex_test;

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
