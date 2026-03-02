//! PostgreSQL advisory locks for single-process coordination.

use crate::DbPool;
use blake3::Hasher;
use sinex_primitives::error::Result as CoreResult;
use sinex_primitives::error::SinexError;
use sinex_primitives::utils::ResourceGuard;
use sqlx::{Postgres, pool::PoolConnection};
use std::time::Duration;
use tracing::instrument;

/// PostgreSQL advisory lock implementation.
pub struct AdvisoryLock {
    lock_id: i64,
    connection: Option<PoolConnection<Postgres>>,
}

impl std::fmt::Debug for AdvisoryLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdvisoryLock")
            .field("lock_id", &self.lock_id)
            .finish()
    }
}

impl AdvisoryLock {
    /// Try to acquire an advisory lock immediately (non-blocking).
    #[instrument(skip(pool), fields(key = key))]
    pub async fn try_acquire(pool: &DbPool, key: &str) -> CoreResult<Option<ResourceGuard<Self>>> {
        let lock_id = hash_key_to_i64(key);
        let mut connection = pool.acquire().await.map_err(SinexError::from)?;

        let acquired: bool = sqlx::query_scalar!("SELECT pg_try_advisory_lock($1)", lock_id)
            .fetch_one(&mut *connection)
            .await?
            .unwrap_or(false);

        if !acquired {
            return Ok(None);
        }

        let lock = AdvisoryLock {
            lock_id,
            connection: Some(connection),
        };

        let cleanup = |mut lock: AdvisoryLock| async move {
            if let Some(mut connection) = lock.connection.take() {
                let _ = sqlx::query_scalar!("SELECT pg_advisory_unlock($1)", lock.lock_id)
                    .fetch_one(&mut *connection)
                    .await;
            }
        };

        Ok(Some(ResourceGuard::new(lock, cleanup)))
    }

    /// Acquire an advisory lock, blocking until available or timeout.
    #[instrument(skip(pool), fields(key = key, timeout_secs = timeout.as_secs()))]
    pub async fn acquire_or_wait(
        pool: &DbPool,
        key: &str,
        timeout: Duration,
    ) -> CoreResult<ResourceGuard<Self>> {
        let timeout_future = tokio::time::timeout(timeout, async {
            loop {
                if let Some(lock) = Self::try_acquire(pool, key).await? {
                    return Ok(lock);
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        match timeout_future {
            Ok(result) => result,
            Err(_) => Err(SinexError::timeout(format!(
                "Advisory lock timeout for key: {key}"
            ))),
        }
    }

    /// Check if a lock is currently held by any session.
    #[instrument(skip(pool), fields(key = key))]
    pub async fn is_locked(pool: &DbPool, key: &str) -> CoreResult<bool> {
        let lock_id = hash_key_to_i64(key);
        let mut connection = pool.acquire().await.map_err(SinexError::from)?;

        let acquired: bool = sqlx::query_scalar!("SELECT pg_try_advisory_lock($1)", lock_id)
            .fetch_one(&mut *connection)
            .await?
            .unwrap_or(false);

        if acquired {
            let _ = sqlx::query_scalar!("SELECT pg_advisory_unlock($1)", lock_id)
                .fetch_one(&mut *connection)
                .await;
            Ok(false)
        } else {
            Ok(true)
        }
    }
}

fn hash_key_to_i64(key: &str) -> i64 {
    let mut hasher = Hasher::new();
    hasher.update(key.as_bytes());
    let bytes = hasher.finalize();
    let mut id_bytes = [0u8; 8];
    id_bytes.copy_from_slice(&bytes.as_bytes()[0..8]);
    i64::from_be_bytes(id_bytes)
}
