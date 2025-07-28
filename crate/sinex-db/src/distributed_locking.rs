//! PostgreSQL advisory locks for distributed coordination
//!
//! This module provides distributed locking using PostgreSQL's built-in advisory lock
//! functionality. Advisory locks are perfect for leader election, singleton job processing,
//! and resource coordination across multiple processes/instances.

use crate::DbPool;
use sinex_core_types::CoreError;
use sinex_core_types::Result as CoreResult;
use sinex_core_utils::ResourceGuard;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// PostgreSQL advisory lock implementation
#[derive(Debug)]
pub struct AdvisoryLock {
    pool: DbPool,
    lock_id: i64,
    acquired: bool,
}

impl AdvisoryLock {
    /// Try to acquire an advisory lock immediately (non-blocking)
    pub async fn try_acquire(pool: &DbPool, key: &str) -> CoreResult<Option<ResourceGuard<Self>>> {
        let lock_id = hash_key_to_i64(key);

        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(lock_id)
            .fetch_one(pool)
            .await?;

        if acquired {
            let lock = AdvisoryLock {
                pool: pool.clone(),
                lock_id,
                acquired: true,
            };

            let cleanup = |lock: AdvisoryLock| async move {
                if lock.acquired {
                    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                        .bind(lock.lock_id)
                        .execute(&lock.pool)
                        .await;
                    tracing::debug!("Released advisory lock {}", lock.lock_id);
                }
            };

            Ok(Some(ResourceGuard::new(lock, cleanup)))
        } else {
            Ok(None)
        }
    }

    /// Acquire an advisory lock, blocking until available or timeout
    pub async fn acquire_or_wait(
        pool: &DbPool,
        key: &str,
        timeout: Duration,
    ) -> CoreResult<ResourceGuard<Self>> {
        let lock_id = hash_key_to_i64(key);

        // Use tokio timeout for the blocking call
        let _acquired = tokio::time::timeout(timeout, async {
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(lock_id)
                .execute(pool)
                .await
        })
        .await
        .map_err(|_| CoreError::Timeout(format!("Advisory lock timeout for key: {}", key)))?
        .map_err(|e| CoreError::Database(format!("Failed to acquire advisory lock: {}", e)))?;

        let lock = AdvisoryLock {
            pool: pool.clone(),
            lock_id,
            acquired: true,
        };

        let cleanup = |lock: AdvisoryLock| async move {
            if lock.acquired {
                let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(lock.lock_id)
                    .execute(&lock.pool)
                    .await;
                tracing::debug!("Released advisory lock {}", lock.lock_id);
            }
        };

        Ok(ResourceGuard::new(lock, cleanup))
    }

    /// Check if a lock is currently held by any session
    pub async fn is_locked(pool: &DbPool, key: &str) -> CoreResult<bool> {
        let lock_id = hash_key_to_i64(key);

        // Query pg_locks system view to check if lock exists
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND classid = 0 AND objid = $1"
        )
        .bind(lock_id)
        .fetch_one(pool)
        .await?;

        Ok(count > 0)
    }

    /// Force release a lock (use with caution - should only be used for cleanup)
    pub async fn force_release(pool: &DbPool, key: &str) -> CoreResult<bool> {
        let lock_id = hash_key_to_i64(key);

        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(lock_id)
            .fetch_one(pool)
            .await?;

        Ok(released)
    }

    /// Get lock information
    pub fn lock_id(&self) -> i64 {
        self.lock_id
    }

    pub fn is_acquired(&self) -> bool {
        self.acquired
    }
}

/// Convert a string key to a 64-bit integer for PostgreSQL advisory locks
fn hash_key_to_i64(key: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as i64
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
    pub async fn try_become_leader(
        &self,
        service_name: &str,
    ) -> CoreResult<Option<ResourceGuard<AdvisoryLock>>> {
        let leadership_key = format!("leader:{}", service_name);
        AdvisoryLock::try_acquire(&self.pool, &leadership_key).await
    }

    /// Singleton job pattern - acquire exclusive access to process a job
    pub async fn acquire_job_lock(
        &self,
        job_id: &str,
    ) -> CoreResult<Option<ResourceGuard<AdvisoryLock>>> {
        let job_key = format!("job:{}", job_id);
        AdvisoryLock::try_acquire(&self.pool, &job_key).await
    }

    /// Resource coordination - acquire exclusive access to a resource
    pub async fn acquire_resource_lock(
        &self,
        resource_name: &str,
        timeout: Duration,
    ) -> CoreResult<ResourceGuard<AdvisoryLock>> {
        let resource_key = format!("resource:{}", resource_name);
        AdvisoryLock::acquire_or_wait(&self.pool, &resource_key, timeout).await
    }

    /// Check if a service has a current leader
    pub async fn has_leader(&self, service_name: &str) -> CoreResult<bool> {
        let leadership_key = format!("leader:{}", service_name);
        AdvisoryLock::is_locked(&self.pool, &leadership_key).await
    }

    /// Check if a job is currently being processed
    pub async fn is_job_locked(&self, job_id: &str) -> CoreResult<bool> {
        let job_key = format!("job:{}", job_id);
        AdvisoryLock::is_locked(&self.pool, &job_key).await
    }
}

/// Leadership guard that provides additional leadership-specific functionality
pub struct LeadershipGuard {
    #[allow(dead_code)]
    lock_guard: ResourceGuard<AdvisoryLock>,
    service_name: String,
    instance_id: String,
}

impl LeadershipGuard {
    pub fn new(
        lock_guard: ResourceGuard<AdvisoryLock>,
        service_name: String,
        instance_id: String,
    ) -> Self {
        Self {
            lock_guard,
            service_name,
            instance_id,
        }
    }

    /// Record leadership in database for monitoring/debugging
    pub async fn record_leadership(&self, pool: &DbPool) -> CoreResult<()> {
        sqlx::query(
            "INSERT INTO core.service_leadership (service_name, instance_id, acquired_at, last_heartbeat, version)
             VALUES ($1, $2, NOW(), NOW(), 'unknown')
             ON CONFLICT (service_name) 
             DO UPDATE SET instance_id = $2, acquired_at = NOW(), last_heartbeat = NOW()"
        )
        .bind(&self.service_name)
        .bind(&self.instance_id)
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Update leadership heartbeat
    pub async fn heartbeat(&self, pool: &DbPool) -> CoreResult<()> {
        sqlx::query(
            "UPDATE core.service_leadership SET last_heartbeat = NOW() WHERE service_name = $1",
        )
        .bind(&self.service_name)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::prelude::*;
    use std::time::Duration;

    #[sinex_test]
    async fn test_advisory_lock_try_acquire(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();

        // First acquisition should succeed
        let lock1 = AdvisoryLock::try_acquire(&pool, "test_key").await?;
        assert!(lock1.is_some());

        // Second acquisition should fail (lock held)
        let lock2 = AdvisoryLock::try_acquire(&pool, "test_key").await?;
        assert!(lock2.is_none());

        // After dropping first lock, should be able to acquire again
        drop(lock1);
        tokio::time::sleep(Duration::from_millis(100)).await; // Allow cleanup

        let lock3 = AdvisoryLock::try_acquire(&pool, "test_key").await?;
        assert!(lock3.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn test_leadership_pattern(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        let coordination = DistributedCoordination::new(pool.clone());

        // Should be able to become leader
        let leadership = coordination.try_become_leader("test_service").await?;
        assert!(leadership.is_some());

        // Second instance should not be able to become leader
        let leadership2 = coordination.try_become_leader("test_service").await?;
        assert!(leadership2.is_none());

        // Should report that service has leader
        let has_leader = coordination.has_leader("test_service").await?;
        assert!(has_leader);
        Ok(())
    }
}
