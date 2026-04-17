//! `PostgreSQL` advisory locks for single-process coordination.

use crate::DbPool;
use blake3::Hasher;
use sinex_primitives::error::Result as CoreResult;
use sinex_primitives::error::SinexError;
use sinex_primitives::utils::ResourceGuard;
use sqlx::{Postgres, pool::PoolConnection};
use std::time::Duration;
use tracing::instrument;

/// `PostgreSQL` advisory lock implementation.
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
    fn guard_from_connection(
        lock_id: i64,
        connection: PoolConnection<Postgres>,
    ) -> ResourceGuard<Self> {
        let lock = AdvisoryLock {
            lock_id,
            connection: Some(connection),
        };

        let cleanup = |mut lock: AdvisoryLock| async move {
            if let Some(mut connection) = lock.connection.take()
                && let Err(error) =
                    unlock_or_close_connection(&mut connection, lock.lock_id, "guard cleanup").await
            {
                tracing::warn!(
                    lock_id = lock.lock_id,
                    error = %error,
                    "Advisory lock cleanup failed; closing pooled connection to avoid lock leakage"
                );

                if let Err(close_error) = connection.close().await {
                    tracing::warn!(
                        lock_id = lock.lock_id,
                        error = %close_error,
                        "Failed to close pooled connection after advisory lock cleanup failure"
                    );
                }
            }
        };

        ResourceGuard::new(lock, cleanup)
    }

    async fn connection_holds_any_advisory_lock(
        connection: &mut PoolConnection<Postgres>,
    ) -> CoreResult<bool> {
        let sql = r"
            SELECT EXISTS (
                SELECT 1
                FROM pg_locks
                WHERE pid = pg_backend_pid()
                  AND locktype = 'advisory'
            )
        ";

        sqlx::query_scalar::<_, bool>(sql)
            .fetch_one(&mut **connection)
            .await
            .map_err(SinexError::from)
    }

    async fn any_session_holds_lock(
        connection: &mut PoolConnection<Postgres>,
        lock_id: i64,
    ) -> CoreResult<bool> {
        let sql = r"
            SELECT EXISTS (
                SELECT 1
                FROM pg_locks
                WHERE locktype = 'advisory'
                  AND classid = (($1::bigint >> 32) & 4294967295::bigint)::oid
                  AND objid = ($1::bigint & 4294967295::bigint)::oid
            )
        ";

        sqlx::query_scalar::<_, bool>(sql)
            .bind(lock_id)
            .fetch_one(&mut **connection)
            .await
            .map_err(SinexError::from)
    }

    /// Try to acquire an advisory lock immediately (non-blocking).
    #[instrument(skip(pool), fields(key = key))]
    pub async fn try_acquire(pool: &DbPool, key: &str) -> CoreResult<Option<ResourceGuard<Self>>> {
        let lock_id = hash_key_to_i64(key);
        let mut connection = pool.acquire().await.map_err(SinexError::from)?;

        if Self::connection_holds_any_advisory_lock(&mut connection).await? {
            if let Err(close_error) = connection.close().await {
                tracing::warn!(
                    lock_id,
                    error = %close_error,
                    "Failed to close polluted pooled connection after advisory lock reuse was detected"
                );
            }

            return Err(SinexError::invalid_state(format!(
                "Connection already holds an advisory lock; refusing to reuse polluted session for key {key}"
            )));
        }

        let acquired: bool = sqlx::query_scalar!("SELECT pg_try_advisory_lock($1)", lock_id)
            .fetch_one(&mut *connection)
            .await?
            .unwrap_or(false);

        if !acquired {
            return Ok(None);
        }

        Ok(Some(Self::guard_from_connection(lock_id, connection)))
    }

    /// Acquire an advisory lock, blocking until available or timeout.
    #[instrument(skip(pool), fields(key = key, timeout_secs = timeout.as_secs()))]
    pub async fn acquire_or_wait(
        pool: &DbPool,
        key: &str,
        timeout: Duration,
    ) -> CoreResult<ResourceGuard<Self>> {
        let lock_id = hash_key_to_i64(key);
        let mut connection = pool.acquire().await.map_err(SinexError::from)?;

        let timeout_future = tokio::time::timeout(timeout, async {
            loop {
                if Self::connection_holds_any_advisory_lock(&mut connection).await? {
                    return Err(SinexError::invalid_state(format!(
                        "Connection already holds an advisory lock; refusing to reuse polluted session for key {key}"
                    )));
                }

                let acquired: bool = sqlx::query_scalar!("SELECT pg_try_advisory_lock($1)", lock_id)
                    .fetch_one(&mut *connection)
                    .await?
                    .unwrap_or(false);

                if acquired {
                    return Ok(());
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        match timeout_future {
            Ok(Ok(())) => Ok(Self::guard_from_connection(lock_id, connection)),
            Ok(Err(error)) => {
                if let Err(close_error) = connection.close().await {
                    tracing::warn!(
                        lock_id,
                        error = %close_error,
                        "Failed to close polluted pooled connection after advisory lock acquisition error"
                    );
                }
                Err(error)
            }
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

        if Self::connection_holds_any_advisory_lock(&mut connection).await? {
            if let Err(close_error) = connection.close().await {
                tracing::warn!(
                    lock_id,
                    error = %close_error,
                    "Failed to close polluted pooled connection after advisory lock probe detected an advisory lock"
                );
            }

            return Err(SinexError::invalid_state(format!(
                "Connection already holds an advisory lock; refusing to probe from polluted session for key {key}"
            )));
        }

        Self::any_session_holds_lock(&mut connection, lock_id).await
    }
}

async fn unlock_or_close_connection(
    connection: &mut PoolConnection<Postgres>,
    lock_id: i64,
    context: &'static str,
) -> CoreResult<()> {
    let unlocked: bool = sqlx::query_scalar!("SELECT pg_advisory_unlock($1)", lock_id)
        .fetch_one(&mut **connection)
        .await?
        .unwrap_or(false);

    if unlocked {
        Ok(())
    } else {
        Err(SinexError::invalid_state(format!(
            "Advisory lock {lock_id} was not held during {context}"
        )))
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
