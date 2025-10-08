//! PostgreSQL advisory locks for distributed coordination
//!
//! This module provides distributed locking using PostgreSQL's built-in advisory lock
//! functionality. Advisory locks are perfect for leader election, singleton job processing,
//! and resource coordination across multiple processes/instances.

use crate::types::error::SinexError;
use crate::types::utils::ResourceGuard;
use crate::types::Result as CoreResult;
use crate::DbPool;
use blake3::Hasher;
use sqlx::{pool::PoolConnection, Postgres};
use std::time::Duration;
use tracing::instrument;
use uuid::Uuid;
// (no direct Row usage when using sqlx macros)

/// PostgreSQL advisory lock implementation
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
    /// Try to acquire an advisory lock immediately (non-blocking)
    #[instrument(skip(pool), fields(key = key))]
    pub async fn try_acquire(pool: &DbPool, key: &str) -> CoreResult<Option<ResourceGuard<Self>>> {
        let lock_id = hash_key_to_i64(key);
        let mut connection = pool.acquire().await.map_err(SinexError::from)?;

        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(lock_id)
            .fetch_one(&mut *connection)
            .await?;

        if !acquired {
            return Ok(None);
        }

        let lock = AdvisoryLock {
            lock_id,
            connection: Some(connection),
        };

        let cleanup = |mut lock: AdvisoryLock| async move {
            if let Some(mut connection) = lock.connection.take() {
                let _ = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
                    .bind(lock.lock_id)
                    .fetch_one(&mut *connection)
                    .await;
            }
        };

        Ok(Some(ResourceGuard::new(lock, cleanup)))
    }

    /// Acquire an advisory lock, blocking until available or timeout
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

    /// Check if a lock is currently held by any session
    ///
    /// Implementation note: To avoid relying on `pg_locks` internals and OID casting,
    /// we probe using `pg_try_advisory_lock()` and immediately unlock if we acquire it.
    #[instrument(skip(pool), fields(key = key))]
    pub async fn is_locked(pool: &DbPool, key: &str) -> CoreResult<bool> {
        let lock_id = hash_key_to_i64(key);
        let mut connection = pool.acquire().await.map_err(SinexError::from)?;

        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(lock_id)
            .fetch_one(&mut *connection)
            .await?;

        if acquired {
            let _ = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
                .bind(lock_id)
                .fetch_one(&mut *connection)
                .await;
            Ok(false)
        } else {
            Ok(true)
        }
    }

    /// Force release a lock (use with caution - should only be used for cleanup)
    #[instrument(skip(pool), fields(key = key))]
    pub async fn force_release(pool: &DbPool, key: &str) -> CoreResult<bool> {
        let lock_id = hash_key_to_i64(key);
        let mut connection = pool.acquire().await.map_err(SinexError::from)?;

        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(lock_id)
            .fetch_one(&mut *connection)
            .await?;

        Ok(released)
    }

    /// Get lock information
    pub fn lock_id(&self) -> i64 {
        self.lock_id
    }

    pub fn is_acquired(&self) -> bool {
        self.connection.is_some()
    }
}

/// Convert a string key to a 64-bit integer for PostgreSQL advisory locks
fn hash_key_to_i64(key: &str) -> i64 {
    let mut hasher = Hasher::new();
    hasher.update(key.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    i64::from_be_bytes(bytes)
}

/// High-level coordination patterns using advisory locks
pub struct DistributedCoordination {
    pool: DbPool,
}

impl DistributedCoordination {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Leader election pattern - try to become leader for a service
    #[instrument(skip(self), fields(service = service_name))]
    pub async fn try_become_leader(
        &self,
        service_name: &str,
    ) -> CoreResult<Option<ResourceGuard<AdvisoryLock>>> {
        let leadership_key = format!("leader:{service_name}");
        AdvisoryLock::try_acquire(&self.pool, &leadership_key).await
    }

    /// Singleton job pattern - acquire exclusive access to process a job
    #[instrument(skip(self), fields(job_id = job_id))]
    pub async fn acquire_job_lock(
        &self,
        job_id: &str,
    ) -> CoreResult<Option<ResourceGuard<AdvisoryLock>>> {
        let job_key = format!("job:{job_id}");
        AdvisoryLock::try_acquire(&self.pool, &job_key).await
    }

    /// Resource coordination - acquire exclusive access to a resource
    pub async fn acquire_resource_lock(
        &self,
        resource_name: &str,
        timeout: Duration,
    ) -> CoreResult<ResourceGuard<AdvisoryLock>> {
        let resource_key = format!("resource:{resource_name}");
        AdvisoryLock::acquire_or_wait(&self.pool, &resource_key, timeout).await
    }

    /// Check if a service has a current leader
    pub async fn has_leader(&self, service_name: &str) -> CoreResult<bool> {
        let leadership_key = format!("leader:{service_name}");
        AdvisoryLock::is_locked(&self.pool, &leadership_key).await
    }

    /// Check if a job is currently being processed
    pub async fn is_job_locked(&self, job_id: &str) -> CoreResult<bool> {
        let job_key = format!("job:{job_id}");
        AdvisoryLock::is_locked(&self.pool, &job_key).await
    }
}

/// Leadership guard that provides additional leadership-specific functionality
pub struct LeadershipGuard {
    #[allow(dead_code)]
    lock_guard: ResourceGuard<AdvisoryLock>,
    service_name: String,
    instance_id: Uuid,
}

impl LeadershipGuard {
    pub fn new(
        lock_guard: ResourceGuard<AdvisoryLock>,
        service_name: String,
        instance_id: Uuid,
    ) -> Self {
        Self {
            lock_guard,
            service_name,
            instance_id,
        }
    }

    /// Record leadership in database for monitoring/debugging
    #[instrument(skip(self, pool), fields(service = %self.service_name, instance = %self.instance_id))]
    pub async fn record_leadership(&self, pool: &DbPool) -> CoreResult<()> {
        // Start transaction to ensure atomicity of event emission and state change
        let mut tx = pool.begin().await.map_err(SinexError::from)?;

        // Check if there's an existing leader for this service
        let existing_leader = sqlx::query!(
            "SELECT instance_id, acquired_at FROM core.service_leadership WHERE service_name = $1",
            &self.service_name
        )
        .fetch_optional(&mut *tx)
        .await?;

        let operation_type = if existing_leader.is_some() {
            "leadership_transfer"
        } else {
            "leadership_acquisition"
        };

        // Log leadership acquisition intent (replaced direct event insertion to fix architectural violation)
        tracing::info!(
            service_name = %self.service_name,
            new_leader_instance_id = %self.instance_id,
            operation_type = %operation_type,
            previous_leader = ?existing_leader.as_ref().map(|l| &l.instance_id),
            previous_leader_acquired_at = ?existing_leader.as_ref().map(|l| l.acquired_at),
            "Leadership acquisition intent"
        );

        // Perform the leadership record update
        sqlx::query(
            "INSERT INTO core.service_leadership (service_name, instance_id, acquired_at, last_heartbeat, version)
             VALUES ($1, $2, NOW(), NOW(), 'unknown')
             ON CONFLICT (service_name) 
             DO UPDATE SET instance_id = $2, acquired_at = NOW(), last_heartbeat = NOW()"
        )
        .bind(&self.service_name)
        .bind(self.instance_id)
        .execute(&mut *tx)
        .await?;

        // Log leadership acquired confirmation (replaced direct event insertion to fix architectural violation)
        tracing::info!(
            service_name = %self.service_name,
            leader_instance_id = %self.instance_id,
            operation_type = %operation_type,
            previous_leader = ?existing_leader.as_ref().map(|l| &l.instance_id),
            "Leadership acquired"
        );

        tx.commit().await.map_err(SinexError::from)?;

        Ok(())
    }

    /// Update leadership heartbeat
    #[instrument(skip(self, pool), fields(service = %self.service_name))]
    pub async fn heartbeat(&self, pool: &DbPool) -> CoreResult<()> {
        // Start transaction to ensure atomicity of event emission and state change
        let mut tx = pool.begin().await.map_err(SinexError::from)?;

        // Get current heartbeat details for event emission
        let current_heartbeat = sqlx::query!(
            "SELECT last_heartbeat FROM core.service_leadership WHERE service_name = $1 AND instance_id = $2",
            &self.service_name,
            &self.instance_id.to_string()
        )
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(heartbeat_info) = current_heartbeat {
            // Log heartbeat intent (replaced direct event insertion to fix architectural violation)
            tracing::debug!(
                service_name = %self.service_name,
                leader_instance_id = %self.instance_id,
                previous_heartbeat = ?heartbeat_info.last_heartbeat,
                "Leadership heartbeat intent"
            );

            // Perform the heartbeat update
            let result = sqlx::query!(
                "UPDATE core.service_leadership SET last_heartbeat = NOW() WHERE service_name = $1 AND instance_id = $2",
                &self.service_name,
                &self.instance_id.to_string()
            )
            .execute(&mut *tx)
            .await?;

            if result.rows_affected() > 0 {
                // Log heartbeat updated confirmation (replaced direct event insertion to fix architectural violation)
                tracing::debug!(
                    service_name = %self.service_name,
                    leader_instance_id = %self.instance_id,
                    previous_heartbeat = ?heartbeat_info.last_heartbeat,
                    "Leadership heartbeat updated"
                );
            }

            tx.commit().await.map_err(SinexError::from)?;
        } else {
            tx.rollback().await.ok();
        }

        Ok(())
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn instance_id(&self) -> Uuid {
        self.instance_id
    }
}
