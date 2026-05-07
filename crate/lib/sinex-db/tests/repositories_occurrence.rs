//! Integration tests for occurrence and material interpretation repositories.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::events::AnchorKind;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn ensure_occurrence_is_idempotent(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("test-occurrence-material"))
        .await?
        .to_uuid();

    let anchor_data = json!({"start": 0, "len": 100});

    // First registration
    let occ1 = ctx.pool.occurrences().ensure_occurrence(
        "test.source_unit",
        material_id,
        AnchorKind::ByteOffset,
        anchor_data.clone(),
        None,
    ).await?;

    // Second registration with same identity — should return the same record
    let occ2 = ctx.pool.occurrences().ensure_occurrence(
        "test.source_unit",
        material_id,
        AnchorKind::ByteOffset,
        anchor_data,
        None,
    ).await?;

    assert_eq!(occ1.id, occ2.id, "idempotent registration should return same occurrence");
    assert_eq!(occ1.source_unit_id, "test.source_unit");
    assert_eq!(occ1.source_material_id, material_id);
    assert_eq!(occ1.anchor_kind, AnchorKind::ByteOffset.as_str());

    Ok(())
}

#[sinex_test]
async fn find_occurrences_by_material(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("test-occ-by-material"))
        .await?
        .to_uuid();

    let _occ1 = ctx.pool.occurrences().ensure_occurrence(
        "su.one",
        material_id,
        AnchorKind::ByteOffset,
        json!({"start": 0, "len": 50}),
        None,
    ).await?;

    let _occ2 = ctx.pool.occurrences().ensure_occurrence(
        "su.two",
        material_id,
        AnchorKind::LineNumber,
        json!({"byte_start": 0, "line": 1}),
        None,
    ).await?;

    let found = ctx.pool.occurrences().find_by_material(material_id).await?;
    assert_eq!(found.len(), 2);
    assert!(found.iter().any(|o| o.source_unit_id == "su.one"));
    assert!(found.iter().any(|o| o.source_unit_id == "su.two"));

    Ok(())
}

#[sinex_test]
async fn anchor_kind_variants_are_persisted(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("test-anchor-kinds"))
        .await?
        .to_uuid();

    for kind in [
        AnchorKind::ByteOffset,
        AnchorKind::SqliteRow,
        AnchorKind::LineNumber,
        AnchorKind::SequenceNumber,
        AnchorKind::GitOid,
        AnchorKind::StreamFrame,
    ] {
        let anchor_data = match kind {
            AnchorKind::ByteOffset => json!({"start": 0, "len": 10}),
            AnchorKind::SqliteRow => json!({"table": "t", "rowid": 1}),
            AnchorKind::LineNumber => json!({"byte_start": 0, "line": 1}),
            AnchorKind::SequenceNumber => json!({"seq": 1}),
            AnchorKind::GitOid => json!({"oid": "a".repeat(40)}),
            AnchorKind::StreamFrame => json!({"material_offset": 0, "frame_index": 1}),
            _ => json!({"key": "test"}),
        };

        let occ = ctx.pool.occurrences().ensure_occurrence(
            "test.anchor_kinds",
            material_id,
            kind,
            anchor_data,
            None,
        ).await?;

        assert_eq!(occ.anchor_kind, kind.as_str());
    }

    Ok(())
}

#[sinex_test]
async fn record_interpretation_and_mark_previous_not_current(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("test-interp-lifecycle"))
        .await?
        .to_uuid();

    // Create an occurrence
    let occ = ctx.pool.occurrences().ensure_occurrence(
        "test.interp",
        material_id,
        AnchorKind::ByteOffset,
        json!({"start": 0, "len": 100}),
        None,
    ).await?;

    // Record first interpretation (parser v1.0.0)
    let event_id_1 = uuid::Uuid::now_v7();
    let interp1 = ctx.pool.material_interpretations().record_interpretation(
        occ.id,
        "test-parser",
        "1.0.0",
        "test.interp",
        event_id_1,
    ).await?;

    assert!(interp1.is_current);

    // Record second interpretation (parser v1.0.0, same occurrence — replaces)
    let event_id_2 = uuid::Uuid::now_v7();
    let interp2 = ctx.pool.material_interpretations().record_interpretation(
        occ.id,
        "test-parser",
        "1.0.0",
        "test.interp",
        event_id_2,
    ).await?;

    assert!(interp2.is_current);
    assert_ne!(interp1.id, interp2.id);

    // The first interpretation should no longer be current
    let current = ctx
        .pool
        .material_interpretations()
        .find_current_by_occurrence(occ.id)
        .await?
        .expect("should have a current interpretation");

    assert_eq!(current.id, interp2.id);
    assert!(current.is_current);

    Ok(())
}
