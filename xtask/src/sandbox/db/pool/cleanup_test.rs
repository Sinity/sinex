use super::*;
use crate::sandbox::db::pool::config::PoolConfig;
use crate::sandbox::db::pool::provisioning::{
    advisory_lock_key, connect_admin_with_retry, drop_database_if_exists_admin,
    recreate_pool_database, url_with_db_name, wait_for_database_absence_admin,
};
use crate::sandbox::sinex_test;
use parking_lot::Mutex;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::AtomicBool;

fn make_slot(name: String, url: String) -> Arc<DatabaseSlot> {
    Arc::new(DatabaseSlot {
        name,
        url,
        pool: Mutex::new(None),
        in_use: AtomicBool::new(false),
        quarantined: AtomicBool::new(true),
        schema_verified: AtomicBool::new(false),
        last_released: Mutex::new(None),
        last_clean_time: Mutex::new(None),
        last_clean_result: Mutex::new(None),
        last_residuals: Mutex::new(None),
    })
}

#[sinex_test]
async fn process_cleanup_task_restores_recreated_pool() -> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_cleanup_recreated_pool_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
    recreate_pool_database(&db_name, &slot_url).await?;

    let closed_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&slot_url)
        .await?;
    closed_pool.close().await;

    let slot = make_slot(db_name.clone(), slot_url.clone());
    let task = CleanupTask {
        lock_id: advisory_lock_key(&db_name),
        pool: closed_pool,
        slot_name: db_name.clone(),
        slot_url: slot_url.clone(),
        slot: slot.clone(),
        lock_conn: None,
    };

    CleanupManager::process_cleanup_task(task).await;

    assert!(
        !slot.quarantined.load(Ordering::SeqCst),
        "successful cleanup should clear quarantine"
    );

    let restored_pool = slot
        .pool
        .lock()
        .take()
        .expect("cleanup should restore a usable pool after recreation");
    assert!(
        !restored_pool.is_closed(),
        "restored slot pool must stay open"
    );
    restored_pool.close().await;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
    Ok(())
}
