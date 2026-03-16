//! Tests for the `core.event_temporal_facts` view (Slice 4.1).
//!
//! The view provides a unified queryable surface for "why does this event have this time?"
//! by projecting material events through `raw.temporal_ledger` and synthetic events
//! through their inline metadata.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::source_materials::TemporalLedgerEntry;
use sinex_db::{Event, Provenance};
use sinex_primitives::domain::{DerivedNodeModel, RecordedPath, SyntheticTemporalPolicy};
use sinex_primitives::events::payloads::{FileCreatedPayload, KittyCommandExecutedPayload};
use sinex_primitives::{Id, Timestamp, Uuid};
use xtask::sandbox::prelude::*;

/// Helper: register a source material and return its typed ID.
async fn register_test_material(
    ctx: &TestContext,
    label: &str,
) -> TestResult<Id<sinex_db::models::SourceMaterial>> {
    let record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some(label),
            json!({ "test": true }),
        )
        .await?;
    Ok(Id::from_uuid(record.id))
}

#[sinex_test]
async fn material_event_projected_through_ledger(ctx: TestContext) -> TestResult<()> {
    let material_id = register_test_material(&ctx, "temporal-facts-material").await?;
    let capture_ts = Timestamp::now();

    // Insert temporal ledger entry covering byte range [0, 1024)
    ctx.pool
        .source_materials()
        .append_temporal_ledger(TemporalLedgerEntry::realtime_capture(
            *material_id.as_uuid(),
            1024,
            capture_ts,
        ))
        .await?;

    // Insert material event at anchor_byte=0 (within the ledger range)
    let payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/tmp/temporal-facts.txt")
            .map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let event = Event::new(
        payload,
        Provenance::from_material(material_id, 0, None, None),
    );
    let inserted = ctx.pool.events().insert(event).await?;
    let event_id = inserted.id.unwrap();

    // Query the view
    let row = sqlx::query!(
        r#"SELECT
            event_id,
            provenance_kind,
            source,
            event_type,
            ts_capture,
            temporal_source_type,
            temporal_policy,
            semantics_version,
            scope_key
        FROM core.event_temporal_facts
        WHERE event_id = $1"#,
        event_id.as_uuid()
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(row.provenance_kind.as_deref(), Some("material"));
    assert_eq!(row.source.as_deref(), Some("fs-watcher"));
    assert_eq!(row.event_type.as_deref(), Some("file.created"));
    assert!(
        row.ts_capture.is_some(),
        "material events should have ts_capture from ledger"
    );
    assert_eq!(
        row.temporal_source_type.as_deref(),
        Some("realtime_capture")
    );
    // Material events should have NULL synthetic columns
    assert!(row.temporal_policy.is_none());
    assert!(row.semantics_version.is_none());
    assert!(row.scope_key.is_none());

    Ok(())
}

#[sinex_test]
async fn synthetic_event_projected_inline(ctx: TestContext) -> TestResult<()> {
    let material_id = register_test_material(&ctx, "temporal-facts-synth-parent").await?;

    // Create a source event (needed as parent for synthesis)
    let source_payload = KittyCommandExecutedPayload::test_default("echo synth-parent");
    let source_event = Event::new(
        source_payload,
        Provenance::from_material(material_id, 0, None, None),
    );
    let source = ctx.pool.events().insert(source_event).await?;
    let source_id = source.id.unwrap();

    // Build synthetic/derived event with inline metadata
    let operation_id = Uuid::now_v7();
    let derived_payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/tmp/synth-facts.txt")
            .map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let mut derived = Event::builder(derived_payload)
        .from_parents(vec![source_id])?
        .build()?;

    derived.temporal_policy = Some(SyntheticTemporalPolicy::LatestInput);
    derived.semantics_version = Some("v1.0.0".to_string());
    derived.scope_key = Some("test-scope".to_string());
    derived.equivalence_key = Some("test-equiv".to_string());
    derived.created_by_operation_id = Some(operation_id);
    derived.node_model = Some(DerivedNodeModel::Windowed);

    let inserted = ctx.pool.events().insert(derived).await?;
    let event_id = inserted.id.unwrap();

    // Query the view
    let row = sqlx::query!(
        r#"SELECT
            event_id,
            provenance_kind,
            source,
            event_type,
            ts_capture,
            temporal_policy,
            semantics_version,
            scope_key,
            equivalence_key,
            created_by_operation_id,
            node_model
        FROM core.event_temporal_facts
        WHERE event_id = $1"#,
        event_id.as_uuid()
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(row.provenance_kind.as_deref(), Some("synthetic"));
    assert_eq!(row.source.as_deref(), Some("fs-watcher"));
    // Synthetic events should NOT have ts_capture (from ledger)
    assert!(row.ts_capture.is_none());
    // Inline columns should be populated
    // serde(rename_all = "snake_case") on SyntheticTemporalPolicy → "latest_input" in DB
    assert_eq!(row.temporal_policy.as_deref(), Some("latest_input"));
    assert_eq!(row.semantics_version.as_deref(), Some("v1.0.0"));
    assert_eq!(row.scope_key.as_deref(), Some("test-scope"));
    assert_eq!(row.equivalence_key.as_deref(), Some("test-equiv"));
    assert_eq!(row.created_by_operation_id, Some(operation_id));
    // DerivedNodeModel uses default PascalCase serde (no rename_all)
    assert_eq!(row.node_model.as_deref(), Some("Windowed"));

    Ok(())
}

#[sinex_test]
async fn mixed_projection_no_cross_contamination(ctx: TestContext) -> TestResult<()> {
    let material_id = register_test_material(&ctx, "temporal-facts-mixed").await?;
    let capture_ts = Timestamp::now();

    // Set up temporal ledger for the material event
    ctx.pool
        .source_materials()
        .append_temporal_ledger(TemporalLedgerEntry::realtime_capture(
            *material_id.as_uuid(),
            2048,
            capture_ts,
        ))
        .await?;

    // Insert material event
    let mat_payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/tmp/mixed-material.txt")
            .map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let mat_event = Event::new(
        mat_payload,
        Provenance::from_material(material_id, 0, None, None),
    );
    let mat_inserted = ctx.pool.events().insert(mat_event).await?;
    let mat_id = mat_inserted.id.unwrap();

    // Insert synthetic event derived from the material event
    let synth_payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/tmp/mixed-synthetic.txt")
            .map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let mut synth = Event::builder(synth_payload)
        .from_parents(vec![mat_id])?
        .build()?;
    synth.temporal_policy = Some(SyntheticTemporalPolicy::InheritParent);
    synth.scope_key = Some("mixed-scope".to_string());

    let synth_inserted = ctx.pool.events().insert(synth).await?;
    let synth_id = synth_inserted.id.unwrap();

    // Query view for both events
    let ids = vec![*mat_id.as_uuid(), *synth_id.as_uuid()];
    let rows = sqlx::query!(
        r#"SELECT event_id, provenance_kind
        FROM core.event_temporal_facts
        WHERE event_id = ANY($1::uuid[])"#,
        &ids
    )
    .fetch_all(&ctx.pool)
    .await?;

    assert_eq!(rows.len(), 2, "both events should appear in the view");

    let mat_row = rows.iter().find(|r| r.event_id == Some(*mat_id.as_uuid()));
    let synth_row = rows
        .iter()
        .find(|r| r.event_id == Some(*synth_id.as_uuid()));

    assert!(mat_row.is_some(), "material event should be in view");
    assert!(synth_row.is_some(), "synthetic event should be in view");

    assert_eq!(
        mat_row.unwrap().provenance_kind.as_deref(),
        Some("material")
    );
    assert_eq!(
        synth_row.unwrap().provenance_kind.as_deref(),
        Some("synthetic")
    );

    Ok(())
}
