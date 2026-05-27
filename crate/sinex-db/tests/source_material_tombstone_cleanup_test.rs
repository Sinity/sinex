//! Tests for the delete-on-tombstone repository methods on
//! `SourceMaterialRepository` (#987 partial — repository-level scope).
//!
//! The full delete-on-tombstone path runs in
//! `crate/core/sinex-gateway/src/handlers/lifecycle.rs::handle_tombstone_approve`,
//! which the existing test fixtures cannot exercise without a `ServiceContainer`
//! (see disabled tests in `lifecycle_handlers_test.rs`). These tests cover the
//! repository-level building blocks the handler composes.

use sinex_db::repositories::DbPoolExt;
use sinex_primitives::Id;
use uuid::Uuid;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn material_ids_for_archived_events_returns_empty_for_empty_input(
    ctx: TestContext,
) -> TestResult<()> {
    let repo = ctx.pool.source_materials();
    let ids = repo.material_ids_for_archived_events(&[]).await?;
    assert!(ids.is_empty());
    Ok(())
}

#[sinex_test]
async fn find_orphan_materials_returns_empty_for_empty_input(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.source_materials();
    let ids = repo.find_orphan_materials(&[]).await?;
    assert!(ids.is_empty());
    Ok(())
}

#[sinex_test]
async fn find_orphan_materials_returns_unreferenced_ids(ctx: TestContext) -> TestResult<()> {
    // Create a material with no event references — it's orphan by construction.
    let material_id = ctx
        .create_source_material(Some("orphan-detection-test"))
        .await?
        .to_uuid();

    let repo = ctx.pool.source_materials();
    let orphans = repo.find_orphan_materials(&[material_id]).await?;

    assert_eq!(
        orphans.len(),
        1,
        "newly-created material with no events should be orphan"
    );
    assert_eq!(orphans[0], material_id);
    Ok(())
}

#[sinex_test]
async fn find_orphan_materials_excludes_referenced_materials(ctx: TestContext) -> TestResult<()> {
    // Material referenced by a live event is NOT orphan.
    let material_id = ctx
        .create_source_material(Some("referenced-material-test"))
        .await?
        .to_uuid();

    // Insert a live event that references this material.
    sqlx::query!(
        r#"
        INSERT INTO core.events (
            id, source, event_type, host, payload, ts_orig, ts_orig_subnano,
            source_material_id, anchor_byte
        )
        VALUES ($1, 'test', 'fixture.event', 'test-host', '{}'::jsonb,
                NOW(), 0, $2, 0)
        "#,
        Uuid::now_v7(),
        material_id,
    )
    .execute(ctx.pool())
    .await?;

    let repo = ctx.pool.source_materials();
    let orphans = repo.find_orphan_materials(&[material_id]).await?;

    assert!(
        orphans.is_empty(),
        "material referenced by live event must not be reported as orphan"
    );
    Ok(())
}

#[sinex_test]
async fn delete_material_removes_registry_row(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("delete-target-test"))
        .await?
        .to_uuid();

    let repo = ctx.pool.source_materials();
    let typed_id = Id::from_uuid(material_id);

    // Sanity check: the material exists.
    let before = repo.get_by_id(typed_id).await?;
    assert!(before.is_some(), "material should exist before delete");

    // Delete it.
    let deleted = repo.delete_material(typed_id).await?;
    assert!(deleted, "delete_material should report row deletion");

    // Confirm gone.
    let after = repo.get_by_id(typed_id).await?;
    assert!(after.is_none(), "material should be absent after delete");

    // Idempotent: second delete returns false.
    let second = repo.delete_material(typed_id).await?;
    assert!(
        !second,
        "second delete on missing row should report no rows affected"
    );
    Ok(())
}
