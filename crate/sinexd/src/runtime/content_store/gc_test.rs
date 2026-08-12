use super::sweep_stale_material_registry;
use crate::runtime::content_store::{ContentStoreConfig, MaterialContentStore};
use camino::Utf8PathBuf;
use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::{Id, Timestamp, Uuid};
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
