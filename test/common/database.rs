//! Unified database access for tests
//!
//! This module provides THE single way to access databases in tests,
//! with automatic cleanup and resource management.
//!
//! # Key Features
//! - Automatic transaction rollback for test isolation
//! - Configurable cleanup strategies
//! - Shared database pool optimization
//! - Extension trait for common test operations

use crate::common::prelude::*;
use std::ops::{Deref, DerefMut};
use sqlx::{Transaction, Postgres};

/// The standard way to access database in tests
pub struct TestPool {
    inner: PgPool,
    strategy: CleanupStrategy,
    _cleanup_handle: Option<CleanupHandle>,
}

/// Cleanup strategy for test databases
#[derive(Debug, Clone, Copy)]
pub enum CleanupStrategy {
    /// Use transaction that auto-rolls back (default, recommended)
    Transaction,
    /// Truncate tables after test (for tests that need commits)
    Truncate,
    /// No cleanup (for read-only tests)
    None,
}

impl Default for CleanupStrategy {
    fn default() -> Self {
        CleanupStrategy::Transaction
    }
}

/// Handle for cleanup operations
struct CleanupHandle {
    pool: PgPool,
    strategy: CleanupStrategy,
}

impl Drop for CleanupHandle {
    fn drop(&mut self) {
        match self.strategy {
            CleanupStrategy::Truncate => {
                // Spawn a task to clean up
                let pool = self.pool.clone();
                tokio::task::spawn(async move {
                    let _ = cleanup_test_data(&pool).await;
                });
            }
            _ => {} // Transaction rolls back automatically, None does nothing
        }
    }
}

impl TestPool {
    /// Create a new test pool with default transaction isolation
    pub async fn new() -> Result<Self> {
        Self::with_strategy(CleanupStrategy::default()).await
    }

    /// Create a test pool with specific cleanup strategy
    pub async fn with_strategy(strategy: CleanupStrategy) -> Result<Self> {
        let pool = database_helpers::get_shared_test_pool().await?;
        
        let cleanup_handle = match strategy {
            CleanupStrategy::Transaction | CleanupStrategy::Truncate => {
                Some(CleanupHandle {
                    pool: pool.clone(),
                    strategy,
                })
            }
            CleanupStrategy::None => None,
        };

        Ok(TestPool {
            inner: pool,
            strategy,
            _cleanup_handle: cleanup_handle,
        })
    }

    /// Get the underlying pool
    pub fn pool(&self) -> &PgPool {
        &self.inner
    }

    /// Check if this pool uses transaction isolation
    pub fn is_transactional(&self) -> bool {
        matches!(self.strategy, CleanupStrategy::Transaction)
    }

    /// Begin a transaction (for tests that manage transactions manually)
    pub async fn begin(&self) -> Result<Transaction<'_, Postgres>> {
        Ok(self.inner.begin().await?)
    }
}

// Allow TestPool to be used where PgPool is expected
impl Deref for TestPool {
    type Target = PgPool;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for TestPool {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Clean up test data from database
async fn cleanup_test_data(pool: &PgPool) -> Result<()> {
    // Clean up in reverse dependency order to respect foreign key constraints
    sqlx::query!("DELETE FROM sinex_schemas.work_queue WHERE target_agent_name LIKE 'test_%'")
        .execute(pool)
        .await?;
    
    sqlx::query!("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name LIKE 'test_%'")
        .execute(pool)
        .await?;
        
    sqlx::query!("DELETE FROM raw.events WHERE source LIKE 'test%' OR source = 'test'")
        .execute(pool)
        .await?;
        
    Ok(())
}

/// Clean up all test data (more aggressive cleanup)
pub async fn cleanup_all_test_data(pool: &PgPool) -> Result<()> {
    cleanup_test_data(pool).await
}

/// Extension trait for TestPool convenience methods
pub trait TestPoolExt {
    /// Insert a test event and track it
    async fn insert_test_event(&self, event: &RawEvent) -> Result<Ulid>;
    
    /// Get count of all events
    async fn event_count(&self) -> Result<i64>;
    
    /// Clear all test data
    async fn clear_test_data(&self) -> Result<()>;
}

impl TestPoolExt for TestPool {
    async fn insert_test_event(&self, event: &RawEvent) -> Result<Ulid> {
        let inserted_event = queries::insert_event(&self.inner, event).await?;
        Ok(inserted_event.id)
    }

    async fn event_count(&self) -> Result<i64> {
        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events")
            .fetch_one(&self.inner)
            .await?;
        Ok(count.unwrap_or(0))
    }

    async fn clear_test_data(&self) -> Result<()> {
        cleanup_test_data(&self.inner).await
    }
}

/// Additional helper methods for TestPool
impl TestPool {
    /// Get event count for specific source
    pub async fn event_count_by_source(&self, source: &str) -> Result<i64> {
        let count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM raw.events WHERE source = $1",
            source
        )
        .fetch_one(&self.inner)
        .await?;
        Ok(count.unwrap_or(0))
    }
    
    /// Check if database is accessible
    pub async fn check_health(&self) -> Result<bool> {
        match sqlx::query!("SELECT 1 as test").fetch_one(&self.inner).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}