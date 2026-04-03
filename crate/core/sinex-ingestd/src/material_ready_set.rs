//! Shared coordination set for material→event ordering.
//!
//! The `MaterialReadySet` solves a cross-stream ordering problem between two independent
//! NATS `JetStream` consumers within the same ingestd process:
//!
//! - **`MaterialAssembler`** consumes `source_material.begin` and registers materials in Postgres.
//! - **`JetStreamConsumer`** consumes `events.raw.>` and INSERTs events that reference materials via FK.
//!
//! Because these operate on separate NATS streams, events often arrive before their material's
//! BEGIN message is processed. The `MaterialReadySet` allows the assembler to signal readiness
//! so the event consumer can defer events whose materials aren't registered yet — without
//! relying on noisy FK violation retries.
//!
//! # Performance
//!
//! - `is_ready()`: ~100ns for hot entries
//! - `mark_ready()`: ~100ns + `Notify::notify_waiters()` (no heap allocation)
//! - Memory: bounded by TTL-based eviction rather than monotonic growth
//!
//! In the ingestd service path, boundedness comes from two layers:
//! opportunistic eviction on `mark_ready()`/`is_ready()` and a background
//! maintenance task that calls `purge_stale()` even when the process goes idle
//! after a burst.

use dashmap::DashMap;
use sinex_db::DbPoolExt;
use sinex_primitives::Id;
use sinex_schema::schema::records::SourceMaterialRecord;
use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info};
use uuid::Uuid;

use crate::{IngestdResult, SinexError};

/// Seed query window: load material IDs registered in the last hour.
/// This prevents FK violations for materials registered before this ingestd instance started.
const SEED_WINDOW_HOURS: f64 = 1.0;
/// Retain ready material IDs for long enough to cover cross-stream lag and short restarts,
/// then evict them so the coordination set does not grow forever.
const READY_RETENTION: Duration = Duration::from_secs(6 * 60 * 60);
/// Opportunistic sweep cadence. Eviction is O(n), so keep it infrequent.
const SWEEP_INTERVAL: u64 = 1024;
/// Background maintenance cadence used by ingestd to keep the ready-set bounded
/// even when the process goes quiet after a burst.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Shared set tracking which source materials have been registered in the database.
///
/// Cloning is cheap (inner `Arc`). Both the `MaterialAssembler` and `JetStreamConsumer`
/// hold a clone and operate on the same underlying set.
#[derive(Clone)]
pub struct MaterialReadySet {
    entries: Arc<DashMap<Uuid, Instant>>,
    notify: Arc<tokio::sync::Notify>,
    sweep_counter: Arc<AtomicU64>,
    retention: Duration,
    sweep_interval: u64,
}

impl MaterialReadySet {
    /// Create an empty ready set.
    #[must_use]
    pub fn new() -> Self {
        Self::with_policy(READY_RETENTION, SWEEP_INTERVAL)
    }

    fn with_policy(retention: Duration, sweep_interval: u64) -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
            notify: Arc::new(tokio::sync::Notify::new()),
            sweep_counter: Arc::new(AtomicU64::new(0)),
            retention,
            sweep_interval: sweep_interval.max(1),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_policy_for_tests(retention: Duration, sweep_interval: u64) -> Self {
        Self::with_policy(retention, sweep_interval)
    }

    /// Mark a material as registered and ready for FK references.
    ///
    /// Called by `MaterialAssembler` after a successful `register_material_record()`.
    pub fn mark_ready(&self, material_id: Uuid) {
        self.entries.insert(material_id, Instant::now());
        self.notify.notify_waiters();
        self.maybe_evict_stale();
    }

    /// Check whether a material has been registered.
    ///
    /// Returns `true` for materials that have been `mark_ready()`'d or seeded from the DB.
    #[must_use]
    pub fn is_ready(&self, material_id: &Uuid) -> bool {
        let Some(entry) = self.entries.get(material_id) else {
            return false;
        };

        let expired = entry.value().elapsed() > self.retention;
        drop(entry);

        if expired {
            self.entries.remove(material_id);
            return false;
        }

        true
    }

    /// Ensure a material is known-ready, falling back to a direct DB existence check.
    ///
    /// This closes the gap between materials registered outside the in-process
    /// assembler path (for example by gateway helpers or tests) and the in-memory
    /// coordination set used by the event consumer.
    pub async fn ensure_ready(&self, pool: &PgPool, material_id: Uuid) -> IngestdResult<bool> {
        if self.is_ready(&material_id) {
            return Ok(true);
        }

        let exists = pool
            .source_materials()
            .get_by_id(Id::<SourceMaterialRecord>::from_uuid(material_id))
            .await
            .map_err(|e| {
                SinexError::database("Failed to verify source material readiness")
                    .with_context("source_material_id", material_id.to_string())
                    .with_std_error(&e)
            })?
            .is_some();

        if exists {
            self.mark_ready(material_id);
        }

        Ok(exists)
    }

    /// Number of tracked materials (for observability / stats logging).
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Suggested background maintenance interval for periodic stale eviction.
    #[must_use]
    pub fn maintenance_interval(&self) -> Duration {
        MAINTENANCE_INTERVAL.min(self.retention)
    }

    /// Seed the set from the database on startup.
    ///
    /// Loads material IDs registered within the last [`SEED_WINDOW_HOURS`] hours so that
    /// events referencing recently-registered materials don't get unnecessarily deferred
    /// after an ingestd restart.
    pub async fn seed_from_db(&self, pool: &PgPool) -> IngestdResult<()> {
        let rows = sqlx::query_scalar!(
            r#"
            SELECT id AS "id: uuid::Uuid"
            FROM raw.source_material_registry
            WHERE staged_at > NOW() - INTERVAL '1 hour' * $1
            "#,
            SEED_WINDOW_HOURS,
        )
        .fetch_all(pool)
        .await
        .map_err(|e| {
            SinexError::database(format!(
                "Failed to seed MaterialReadySet from database: {e}"
            ))
        })?;

        let count = rows.len();
        let now = Instant::now();
        for uuid in rows {
            self.entries.insert(uuid, now);
        }

        if count > 0 {
            info!(
                seeded = count,
                window_hours = SEED_WINDOW_HOURS,
                "MaterialReadySet seeded from database"
            );
        } else {
            debug!(
                window_hours = SEED_WINDOW_HOURS,
                "MaterialReadySet seed query returned no results (clean start)"
            );
        }

        Ok(())
    }

    fn maybe_evict_stale(&self) {
        let count = self.sweep_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if count % self.sweep_interval == 0 {
            let removed = self.purge_stale();
            if removed > 0 {
                debug!(
                    removed,
                    retained = self.entries.len(),
                    "Evicted stale materials from MaterialReadySet"
                );
            }
        }
    }

    /// Remove all expired entries immediately.
    pub fn purge_stale(&self) -> usize {
        let now = Instant::now();
        let expired: Vec<Uuid> = self
            .entries
            .iter()
            .filter_map(|entry| {
                (now.saturating_duration_since(*entry.value()) > self.retention)
                    .then_some(*entry.key())
            })
            .collect();

        expired
            .into_iter()
            .filter(|material_id| self.entries.remove(material_id).is_some())
            .count()
    }
}

impl Default for MaterialReadySet {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MaterialReadySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterialReadySet")
            .field("len", &self.entries.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    // Inline because testing TTL eviction cleanly needs access to the internal policy constructor.
    use super::*;

    #[test]
    fn stale_entries_are_evicted() {
        let set = MaterialReadySet::with_policy(Duration::from_millis(1), 1);
        let material_id = Uuid::now_v7();

        set.mark_ready(material_id);
        std::thread::sleep(Duration::from_millis(5));

        assert!(!set.is_ready(&material_id));
        assert!(set.is_empty());
    }

    #[test]
    fn purge_stale_removes_idle_entries_without_lookup() {
        let set = MaterialReadySet::with_policy(Duration::from_millis(1), u64::MAX);
        let material_id = Uuid::now_v7();

        set.mark_ready(material_id);
        std::thread::sleep(Duration::from_millis(5));

        assert_eq!(set.purge_stale(), 1);
        assert!(set.is_empty());
    }
}
