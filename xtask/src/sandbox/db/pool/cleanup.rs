//! Cleanup manager for returning database slots to the pool.

use crate::sandbox::prelude::*;
use crate::sandbox::slog::{Level, slog};
use sinex_db::DbPool;
use sinex_primitives::temporal::Timestamp;
use sqlx::Postgres;
use sqlx::pool::PoolConnection;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::provisioning::{load_pool_meta, store_pool_meta_checked};
use super::slot::DatabaseSlot;
use super::test_database::TestDatabase;

/// Cleanup task for background processing
#[derive(Debug)]
pub(super) struct CleanupTask {
    pub(super) lock_id: i64,
    pub(super) pool: DbPool,
    pub(super) slot_name: String,
    pub(super) slot_url: String,
    pub(super) slot: Arc<DatabaseSlot>,
    pub(super) lock_conn: Option<PoolConnection<Postgres>>,
}

/// Background cleanup manager to handle resource cleanup safely
pub(super) struct CleanupManager {
    sender: tokio::sync::mpsc::UnboundedSender<CleanupTask>,
}

impl CleanupManager {
    pub(super) fn new() -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<CleanupTask>();

        std::thread::Builder::new()
            .name("sinex-cleanup".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .enable_all()
                    .build()
                    .expect("failed to build cleanup runtime");
                rt.block_on(async move {
                    let semaphore = Arc::new(tokio::sync::Semaphore::new(8));
                    while let Some(task) = receiver.recv().await {
                        let permit = semaphore.clone().acquire_owned().await;
                        tokio::spawn(async move {
                            Self::process_cleanup_task(task).await;
                            drop(permit);
                        });
                    }
                });
            })
            .expect("failed to spawn cleanup manager thread");

        Self { sender }
    }

    pub(super) fn schedule_cleanup(&self, task: CleanupTask) {
        match self.sender.send(task) {
            Ok(()) => {}
            Err(err) => {
                let task = err.0;
                slog!(Level::Warn, "cleanup_channel_closed");
                std::thread::spawn(|| {
                    if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        rt.block_on(CleanupManager::process_cleanup_task(task));
                    } else {
                        futures::executor::block_on(CleanupManager::process_cleanup_task(task));
                    }
                });
            }
        }
    }

    async fn process_cleanup_task(task: CleanupTask) {
        // Clean while holding the advisory lock so other processes never observe a dirty slot.
        let mut lock_conn = task.lock_conn;
        let cleanup_pool = if task.pool.is_closed() {
            slog!(Level::Warn, "cleanup_pool_closed", slot = task.slot_name);
            match super::slot_pool_options(
                super::config::SLOT_MAX_CONNECTIONS,
                Duration::from_secs(5),
            )
            .connect(&task.slot_url)
            .await
            {
                Ok(pool) => pool,
                Err(error) => {
                    let err = format!(
                        "cleanup failed because slot pool was already closed and reconnect failed: {error}"
                    );
                    task.slot.record_clean_result(Err(err.clone()), None);
                    task.slot.quarantined.store(true, Ordering::SeqCst);
                    task.slot.schema_verified.store(false, Ordering::SeqCst);
                    if let Some(conn) = lock_conn.as_mut() {
                        persist_cleanup_meta(conn, &task.slot_name, true, Some(err)).await;
                    }
                    return;
                }
            }
        } else {
            task.pool.clone()
        };
        let clean_result = tokio::time::timeout(
            Duration::from_secs(10),
            super::reset::clean_database(
                &task.slot,
                &cleanup_pool,
                &task.slot_name,
                &task.slot_url,
            ),
        )
        .await;

        match clean_result {
            Ok(Ok(clean_result)) => {
                if clean_result.recreated {
                    let mut pool_guard = task.slot.pool.lock();
                    *pool_guard = Some(clean_result.pool);
                } else if task.pool.is_closed() {
                    let mut pool_guard = task.slot.pool.lock();
                    *pool_guard = Some(cleanup_pool);
                }
                if let Some(conn) = lock_conn.as_mut() {
                    persist_cleanup_meta(conn, &task.slot_name, false, None).await;
                }
                task.slot.record_clean_result(Ok(()), None);
                task.slot.quarantined.store(false, Ordering::SeqCst);
            }
            Ok(Err(e)) => {
                if let Some(conn) = lock_conn.as_mut() {
                    persist_cleanup_meta(conn, &task.slot_name, true, Some(e.to_string())).await;
                }
            }
            Err(_) => {
                slog!(Level::Warn, "cleanup_timeout", slot = task.slot_name);
                task.slot
                    .record_clean_result(Err("cleanup timed out after 10s".to_string()), None);
                task.slot.quarantined.store(true, Ordering::SeqCst);
                task.slot.schema_verified.store(false, Ordering::SeqCst);
                if let Some(conn) = lock_conn.as_mut() {
                    persist_cleanup_meta(
                        conn,
                        &task.slot_name,
                        true,
                        Some("cleanup timed out after 10s".to_string()),
                    )
                    .await;
                }
            }
        }

        // Advisory locks are per-session; we must unlock on the same connection that acquired it.
        if let Some(mut lock_conn) = lock_conn {
            match tokio::time::timeout(
                Duration::from_secs(5),
                sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(task.lock_id)
                    .execute(lock_conn.as_mut()),
            )
            .await
            {
                Ok(Ok(_)) => {
                    slog!(
                        Level::Debug,
                        "lock_released",
                        slot = task.slot_name,
                        lock_id = task.lock_id
                    );
                }
                Ok(Err(e)) => {
                    slog!(
                        Level::Warn,
                        "lock_release_failed",
                        slot = task.slot_name,
                        lock_id = task.lock_id,
                        error = e
                    );
                }
                Err(_) => {
                    slog!(
                        Level::Warn,
                        "lock_release_timeout",
                        slot = task.slot_name,
                        lock_id = task.lock_id
                    );
                }
            }
        } else {
            slog!(
                Level::Warn,
                "lock_conn_missing",
                slot = task.slot_name,
                lock_id = task.lock_id
            );
        }

        // Close the pool with a timeout
        let close_future = task.pool.close();
        if tokio::time::timeout(Duration::from_secs(2), close_future)
            .await
            .is_err()
        {
            slog!(Level::Warn, "pool_close_timeout", slot = task.slot_name);
        }

        // Success clears quarantine in the branch above; failures stay quarantined so the
        // slot cannot be reused until the owning cleanup logic repairs or recreates it.
    }
}

async fn persist_cleanup_meta(
    conn: &mut PoolConnection<Postgres>,
    slot_name: &str,
    dirty: bool,
    last_error: Option<String>,
) {
    match load_pool_meta(conn.as_mut(), slot_name).await {
        Ok(Some(mut meta)) => {
            meta.dirty = dirty;
            meta.last_error = last_error;
            meta.updated_at_rfc3339 = Timestamp::now().format_rfc3339();
            if let Err(error) = store_pool_meta_checked(conn.as_mut(), slot_name, &meta).await {
                slog!(
                    Level::Warn,
                    "cleanup_meta_persist_failed",
                    slot = slot_name,
                    error = error.to_string()
                );
            }
        }
        Ok(None) => {}
        Err(error) => {
            slog!(
                Level::Warn,
                "cleanup_meta_load_failed",
                slot = slot_name,
                error = error.to_string()
            );
        }
    }
}

/// Global cleanup manager
pub(super) static CLEANUP_MANAGER: std::sync::LazyLock<CleanupManager> =
    std::sync::LazyLock::new(CleanupManager::new);

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Safe, non-blocking cleanup that doesn't create runtimes
        let lock_id = self.lock_id;
        let held_ms = self.acquired_at.elapsed().as_millis();
        slog!(
            Level::Debug,
            "slot_releasing",
            slot = self.name,
            lock_id = lock_id,
            held_ms = held_ms,
            pid = self.acquisition_process_id
        );

        let task = CleanupTask {
            lock_id,
            pool: self.pool.clone(),
            slot_name: self.name.clone(),
            slot_url: self.slot.url.clone(),
            slot: self.slot.clone(),
            lock_conn: self.lock_conn.take(),
        };

        task.slot.quarantined.store(true, Ordering::SeqCst);
        CLEANUP_MANAGER.schedule_cleanup(task);

        // Clear the pool reference immediately
        let mut pool_opt = self.slot.pool.lock();
        *pool_opt = None;

        // Record when this slot was released
        {
            let mut last_released = self.slot.last_released.lock();
            *last_released = Some(std::time::Instant::now());
        }

        // Mark as not in use (for intra-process coordination)
        self.slot.in_use.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::db::pool::acquire_pool_test_guard;
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
        let _guard = acquire_pool_test_guard().await;
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
}
