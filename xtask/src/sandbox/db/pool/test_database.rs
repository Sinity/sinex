//! TestDatabase — the primary handle for test database access.

use crate::sandbox::prelude::*;
use sinex_db::DbPool;
use sqlx::pool::PoolConnection;
use sqlx::Postgres;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::reset::clean_database;
use super::slot::DatabaseSlot;
use super::stats::{CleanupDiagnostics, DatabaseStats};
use super::template_db_name;

/// A test database handle that automatically returns to pool on Drop
/// This is the primary interface for test database access
pub struct TestDatabase {
    pub(super) name: String,
    pub(super) pool: DbPool,
    pub(super) slot: Arc<DatabaseSlot>,
    pub(super) lock_id: i64, // Store advisory lock ID for cleanup
    pub(super) lock_conn: Option<PoolConnection<Postgres>>,
    pub(super) acquired_at: Instant,
    pub(super) acquisition_process_id: u32,
}

impl std::fmt::Debug for TestDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestDatabase")
            .field("name", &self.name)
            .field("lock_id", &self.lock_id)
            .field("acquisition_process_id", &self.acquisition_process_id)
            .finish()
    }
}

impl TestDatabase {
    /// Get the database name
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the database pool for operations
    #[must_use]
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Connection URL for opening ad-hoc connections
    #[must_use]
    pub fn url(&self) -> &str {
        &self.slot.url
    }

    /// Advisory lock identifier associated with this database slot
    #[must_use]
    pub fn lock_id(&self) -> i64 {
        self.lock_id
    }

    /// Get acquisition timestamp for diagnostics
    #[must_use]
    pub fn acquired_at(&self) -> Instant {
        self.acquired_at
    }

    /// Get the process ID that acquired this database
    #[must_use]
    pub fn acquisition_process_id(&self) -> u32 {
        self.acquisition_process_id
    }

    /// Check if the database is healthy
    pub async fn check_health(&self) -> TestResult<bool> {
        match sqlx::query("SELECT 1 as health_check")
            .fetch_one(&self.pool)
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Get database statistics for debugging
    pub async fn get_stats(&self) -> TestResult<DatabaseStats> {
        let row = sqlx::query!(
            r#"
            SELECT
                (SELECT COUNT(*) FROM core.events) as event_count,
                (SELECT COUNT(*) FROM core.events WHERE source_event_ids IS NOT NULL) as synthesis_count,
                0 as checkpoint_count
            "#
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(DatabaseStats {
            event_count: row.event_count.unwrap_or(0),
            agent_count: row.synthesis_count.unwrap_or(0),
            checkpoint_count: i64::from(row.checkpoint_count.unwrap_or(0)),
        })
    }

    pub(crate) fn cleanup_diagnostics(&self) -> CleanupDiagnostics {
        let (time, result, residuals) = self.slot.slot_health_snapshot();
        CleanupDiagnostics {
            slot_name: self.name.clone(),
            template_name: template_db_name(),
            last_clean_time: time.map(|t| {
                t.format(&time::format_description::well_known::Rfc3339)
                    .expect("format timestamp as RFC3339")
            }),
            last_clean_result: result,
            residuals,
            quarantined: self.slot.quarantined.load(Ordering::SeqCst),
        }
    }

    /// Force cleanup of this database (for testing)
    pub async fn force_cleanup(&self) -> TestResult<()> {
        clean_database(&self.slot, &self.pool, &self.name, self.url()).await
    }
}
