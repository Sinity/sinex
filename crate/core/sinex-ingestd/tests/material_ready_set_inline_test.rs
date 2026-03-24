use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_db::repositories::source_materials::material_types;
use sinex_ingestd::MaterialReadySet;
use sinex_primitives::Timestamp;
use uuid::Uuid;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn mark_ready_makes_material_visible() -> TestResult<()> {
    let set = MaterialReadySet::new();
    let id = Uuid::now_v7();

    assert!(!set.is_ready(&id));
    set.mark_ready(id);
    assert!(set.is_ready(&id));
    assert_eq!(set.len(), 1);
    Ok(())
}

#[sinex_test]
async fn clone_shares_state() -> TestResult<()> {
    let set = MaterialReadySet::new();
    let clone = set.clone();
    let id = Uuid::now_v7();

    set.mark_ready(id);
    assert!(clone.is_ready(&id));
    Ok(())
}

#[sinex_test]
async fn unknown_material_is_not_ready() -> TestResult<()> {
    let set = MaterialReadySet::new();
    let id = Uuid::now_v7();
    assert!(!set.is_ready(&id));
    Ok(())
}

#[sinex_test]
async fn default_creates_empty_set() -> TestResult<()> {
    let set = MaterialReadySet::default();
    assert!(set.is_empty());
    assert_eq!(set.len(), 0);
    Ok(())
}

#[sinex_test]
async fn ensure_ready_reconciles_database_material() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let set = MaterialReadySet::new();
    let material_id = Uuid::now_v7();
    let source_uri = format!("memory://material-ready-set/{material_id}");

    ctx.pool
        .source_materials()
        .register_external_in_flight(
            material_id,
            material_types::BLOB,
            Some(&source_uri),
            json!({ "origin": "material-ready-set-test" }),
            Timestamp::now(),
        )
        .await?;

    assert!(set.ensure_ready(&ctx.pool, material_id).await?);
    assert!(set.is_ready(&material_id));
    Ok(())
}
