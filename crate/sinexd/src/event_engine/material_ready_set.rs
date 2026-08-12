//! Shared coordination set for material→event ordering.
//!
//! The `MaterialReadySet` solves a cross-stream ordering problem between two independent
//! ingestion flows within the same event_engine process:
//!
//! - **`MaterialAssembler`** consumes material begin frames and registers materials in Postgres.
//! - **`JetStreamConsumer`** consumes `events.raw.>` and INSERTs events that reference materials via FK.
//!
//! Events and source-material frames are independent streams, so events can arrive before their
//! material's begin frame is processed. The `MaterialReadySet` allows the assembler to signal
//! readiness so the event consumer can defer events whose materials aren't registered yet —
//! without relying on noisy FK violation retries.
//!
//! # Performance
//!
//! - `is_ready()`: ~100ns for hot entries
//! - `mark_ready()`: ~100ns + `Notify::notify_waiters()` (no heap allocation)
//! - Memory: bounded by an expiration index rather than monotonic growth
//!
//! The background maintenance task calls `purge_stale()` even when the process
//! goes idle after a burst. Expiration maintenance is ordered by deadline, so
//! cleanup does not scan every ready material on each tick.

use dashmap::DashMap;
use sinex_db::DbPoolExt;
use sinex_db::schema::defs::records::SourceMaterialRecord;
use sinex_primitives::Id;
use sqlx::PgPool;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, info};
use uuid::Uuid;

use crate::event_engine::{EventEngineResult, SinexError};

/// Seed query window: load material IDs registered in the last hour.
/// This prevents FK violations for materials registered before this event_engine instance started.
const SEED_WINDOW_HOURS: f64 = 1.0;
/// Retain ready material IDs for long enough to cover cross-stream lag and short restarts,
/// then evict them so the coordination set does not grow forever.
const READY_RETENTION: Duration = Duration::from_hours(6);
/// Background maintenance cadence used by event_engine to keep the ready-set bounded
/// even when the process goes quiet after a burst.
const MAINTENANCE_INTERVAL: Duration = Duration::from_mins(5);

/// Shared set tracking which source materials have been registered in the database.
///
/// Cloning is cheap (inner `Arc`). Both the `MaterialAssembler` and `JetStreamConsumer`
/// hold a clone and operate on the same underlying set.
#[derive(Clone)]
pub struct MaterialReadySet {
    entries: Arc<DashMap<Uuid, Instant>>,
    expirations: Arc<Mutex<BTreeSet<(Instant, Instant, Uuid)>>>,
    notify: Arc<tokio::sync::Notify>,
    retention: Duration,
}

impl MaterialReadySet {
    /// Create an empty ready set.
    #[must_use]
    pub fn new() -> Self {
        Self::with_policy(READY_RETENTION, 1)
    }

    fn with_policy(retention: Duration, _sweep_interval: u64) -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
            expirations: Arc::new(Mutex::new(BTreeSet::new())),
            notify: Arc::new(tokio::sync::Notify::new()),
            retention,
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
        self.insert_ready(material_id, Instant::now());
        self.notify.notify_waiters();
    }

    /// Check whether a material has been registered.
    ///
    /// Returns `true` for materials that have been `mark_ready()`'d or seeded from the DB.
    #[must_use]
    pub fn is_ready(&self, material_id: &Uuid) -> bool {
        let Some(entry) = self.entries.get(material_id) else {
            return false;
        };

        let ready_at = *entry.value();
        let expired = ready_at.elapsed() > self.retention;
        drop(entry);

        if expired {
            if self
                .entries
                .remove_if(material_id, |_, current| *current == ready_at)
                .is_some()
            {
                self.remove_expiration(ready_at + self.retention, ready_at, *material_id);
            }
            return false;
        }

        true
    }

    /// Ensure a material is known-ready, falling back to a direct DB existence check.
    ///
    /// This closes the gap between materials registered outside the in-process
    /// assembler path (for example by gateway helpers or tests) and the in-memory
    /// coordination set used by the event consumer.
    pub async fn ensure_ready(&self, pool: &PgPool, material_id: Uuid) -> EventEngineResult<bool> {
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
    /// after an event_engine restart.
    pub async fn seed_from_db(&self, pool: &PgPool) -> EventEngineResult<()> {
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
            self.insert_ready(uuid, now);
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

    fn insert_ready(&self, material_id: Uuid, ready_at: Instant) {
        let previous = self.entries.insert(material_id, ready_at);
        let mut expirations = self.expirations.lock().expect("expiration index poisoned");
        if let Some(previous) = previous {
            expirations.remove(&(previous + self.retention, previous, material_id));
        }
        expirations.insert((ready_at + self.retention, ready_at, material_id));
    }

    fn remove_expiration(&self, expires_at: Instant, ready_at: Instant, material_id: Uuid) {
        self.expirations
            .lock()
            .expect("expiration index poisoned")
            .remove(&(expires_at, ready_at, material_id));
    }

    /// Remove all expired entries immediately.
    #[must_use]
    pub fn purge_stale(&self) -> usize {
        let now = Instant::now();
        let due: Vec<(Instant, Instant, Uuid)> = {
            let mut expirations = self.expirations.lock().expect("expiration index poisoned");
            let mut due = Vec::new();
            while expirations
                .first()
                .is_some_and(|(expires_at, _, _)| *expires_at <= now)
            {
                due.push(
                    expirations
                        .pop_first()
                        .expect("expiration index was non-empty"),
                );
            }
            due
        };

        due.into_iter()
            .filter(|(_, ready_at, material_id)| {
                self.entries
                    .remove_if(material_id, |_, current| *current == *ready_at)
                    .is_some()
            })
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
#[path = "material_ready_set_test.rs"]
mod tests;
