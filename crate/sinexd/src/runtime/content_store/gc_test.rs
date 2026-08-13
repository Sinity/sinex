use super::sweep_stale_material_registry;
use crate::runtime::content_store::{ContentStoreConfig, MaterialContentStore};
use camino::Utf8PathBuf;
use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::{Id, MaterialStatus, Timestamp, Uuid};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn stale_unreferenced_registry_material_is_removed_by_periodic_gc(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path,
        ..Default::default()
    })?;

    let material_id = Uuid::now_v7();
    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            "test",
            Some(&format!("test://registry-gc/{material_id}")),
            json!({}),
            Timestamp::now() - time::Duration::days(30),
        )
        .await?;

    let report = sweep_stale_material_registry(ctx.pool(), &content_store, true).await?;

    assert!(report.registry_rows_deleted >= 1);
    assert!(
        ctx.pool
            .source_materials()
            .get_by_id(Id::from_uuid(material_id))
            .await?
            .is_none(),
        "anti-vacuity: removing delete_stale_unreferenced_materials from the periodic GC route leaves this aged, eventless registry row behind forever"
    );
    Ok(())
}

#[sinex_test]
async fn periodic_gc_retains_stale_material_with_manifest_replay_authority(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path,
        ..Default::default()
    })?;
    let repo = ctx.pool.source_materials();
    let material_id = Uuid::now_v7();
    repo.register_external_in_flight(
        material_id,
        "test",
        Some(&format!("test://registry-gc/manifest/{material_id}")),
        json!({}),
        Timestamp::now() - time::Duration::days(30),
    )
    .await?;
    repo.finalize_in_flight_as(
        ctx.pool(),
        Id::from_uuid(material_id),
        MaterialStatus::Failed,
        None,
        None,
        None,
        None,
    )
    .await?;
    repo.update_metadata(
        Id::from_uuid(material_id),
        json!({
            "material_manifest": {
                "manifest_type": sinex_primitives::MATERIAL_MANIFEST_V1,
                "content_key": "local-cas-s1--aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            }
        }),
    )
    .await?;
    sqlx::query("UPDATE raw.source_material_registry SET end_time = $2 WHERE id = $1")
        .bind(material_id)
        .bind(Timestamp::now() - time::Duration::days(30))
        .execute(ctx.pool())
        .await?;

    let report = sweep_stale_material_registry(ctx.pool(), &content_store, true).await?;

    assert_eq!(report.registry_rows_deleted, 0);
    assert!(
        repo.get_by_id(Id::from_uuid(material_id)).await?.is_some(),
        "manifest-backed materials are replay roots and must survive registry GC"
    );
    Ok(())
}

#[sinex_test]
async fn periodic_gc_removes_disposable_terminal_rows_but_retains_completed_materials(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let store_dir = tempfile::tempdir()?;
    let root_path = Utf8PathBuf::from_path_buf(store_dir.path().to_path_buf())
        .expect("temporary content-store path must be UTF-8");
    let content_store = MaterialContentStore::new(ContentStoreConfig {
        root_path,
        ..Default::default()
    })?;
    let repo = ctx.pool.source_materials();
    let recovered_id = Uuid::now_v7();
    let cancelled_id = Uuid::now_v7();
    let completed_id = Uuid::now_v7();
    for (id, label) in [
        (recovered_id, "recovered"),
        (cancelled_id, "cancelled"),
        (completed_id, "completed"),
    ] {
        repo.register_external_in_flight(
            id,
            "test",
            Some(&format!("test://registry-gc/{label}/{id}")),
            json!({}),
            Timestamp::now() - time::Duration::days(30),
        )
        .await?;
    }
    repo.mark_as_recovered_partial(Id::from_uuid(recovered_id), "test-recovery", json!({}))
        .await?;
    repo.finalize_in_flight_as(
        ctx.pool(),
        Id::from_uuid(cancelled_id),
        MaterialStatus::Cancelled,
        None,
        None,
        None,
        None,
    )
    .await?;
    repo.finalize_in_flight(Id::from_uuid(completed_id), None, None, None, None)
        .await?;
    sqlx::query(
        "UPDATE raw.source_material_registry SET end_time = $2 WHERE id = ANY($1::uuid[])",
    )
    .bind([recovered_id, cancelled_id, completed_id].as_slice())
    .bind(Timestamp::now() - time::Duration::days(30))
    .execute(ctx.pool())
    .await?;

    let report = sweep_stale_material_registry(ctx.pool(), &content_store, true).await?;
    assert!(report.registry_rows_deleted >= 2);
    assert!(repo.get_by_id(Id::from_uuid(recovered_id)).await?.is_none());
    assert!(repo.get_by_id(Id::from_uuid(cancelled_id)).await?.is_none());
    assert!(
        repo.get_by_id(Id::from_uuid(completed_id))
            .await?
            .is_some(),
        "successful source materials are retention roots and must not be GC'd"
    );
    Ok(())
}
