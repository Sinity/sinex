//! Cleanup manager for returning database slots to the pool.

use crate::sandbox::prelude::*;
use crate::sandbox::slog::{Level, slog};
use sinex_db::DbPool;
use sinex_primitives::temporal::Timestamp;
use sqlx::Postgres;
use sqlx::pool::PoolConnection;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::provisioning::{load_pool_meta, store_pool_meta};
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
        let clean_result = tokio::time::timeout(
            Duration::from_secs(10),
            super::reset::clean_database(&task.slot, &task.pool, &task.slot_name, &task.slot_url),
        )
        .await;

        match clean_result {
            Ok(Ok(())) => {
                if let Some(conn) = lock_conn.as_mut()
                    && let Ok(Some(mut meta)) = load_pool_meta(conn.as_mut(), &task.slot_name).await
                {
                    meta.dirty = false;
                    meta.last_error = None;
                    meta.updated_at_rfc3339 = Timestamp::now().format_rfc3339();
                    let _ = store_pool_meta(conn.as_mut(), &task.slot_name, &meta).await;
                }
            }
            Ok(Err(e)) => {
                if let Some(conn) = lock_conn.as_mut()
                    && let Ok(Some(mut meta)) = load_pool_meta(conn.as_mut(), &task.slot_name).await
                {
                    meta.dirty = true;
                    meta.last_error = Some(e.to_string());
                    meta.updated_at_rfc3339 = Timestamp::now().format_rfc3339();
                    let _ = store_pool_meta(conn.as_mut(), &task.slot_name, &meta).await;
                }
            }
            Err(_) => {
                slog!(Level::Warn, "cleanup_timeout", slot = task.slot_name);
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

        // Un-quarantine the slot so it can be picked up by the next test.
        task.slot.quarantined.store(false, Ordering::SeqCst);
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
