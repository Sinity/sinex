//! Shared coordination set for material→event ordering.
//!
//! The `MaterialReadySet` solves a cross-stream ordering problem between two independent
//! NATS JetStream consumers within the same ingestd process:
//!
//! - **MaterialAssembler** consumes `source_material.begin` and registers materials in Postgres.
//! - **JetStreamConsumer** consumes `events.raw.>` and INSERTs events that reference materials via FK.
//!
//! Because these operate on separate NATS streams, events often arrive before their material's
//! BEGIN message is processed. The `MaterialReadySet` allows the assembler to signal readiness
//! so the event consumer can defer events whose materials aren't registered yet — without
//! relying on noisy FK violation retries.
//!
//! # Performance
//!
//! - `is_ready()`: ~100ns (lock-free `DashSet::contains`)
//! - `mark_ready()`: ~100ns + `Notify::notify_waiters()` (no heap allocation)
//! - Memory: ~80 bytes per Uuid entry

use dashmap::DashSet;
use uuid::Uuid;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info};

use crate::{IngestdResult, SinexError};

/// Seed query window: load material IDs registered in the last hour.
/// This prevents FK violations for materials registered before this ingestd instance started.
const SEED_WINDOW_HOURS: f64 = 1.0;

/// Shared set tracking which source materials have been registered in the database.
///
/// Cloning is cheap (inner `Arc`). Both the `MaterialAssembler` and `JetStreamConsumer`
/// hold a clone and operate on the same underlying set.
#[derive(Clone)]
pub struct MaterialReadySet {
    set: Arc<DashSet<Uuid>>,
    notify: Arc<tokio::sync::Notify>,
}

impl MaterialReadySet {
    /// Create an empty ready set.
    pub fn new() -> Self {
        Self {
            set: Arc::new(DashSet::new()),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Mark a material as registered and ready for FK references.
    ///
    /// Called by `MaterialAssembler` after a successful `register_material_record()`.
    pub fn mark_ready(&self, material_id: Uuid) {
        self.set.insert(material_id);
        self.notify.notify_waiters();
    }

    /// Check whether a material has been registered.
    ///
    /// Returns `true` for materials that have been `mark_ready()`'d or seeded from the DB.
    pub fn is_ready(&self, material_id: &Uuid) -> bool {
        self.set.contains(material_id)
    }

    /// Number of tracked materials (for observability / stats logging).
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
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
        for uuid in rows {
            // Convert UUID back to UUIDv7 (the canonical ID format)
            let uuid = Uuid::from(uuid);
            self.set.insert(uuid);
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
}

impl Default for MaterialReadySet {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MaterialReadySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterialReadySet")
            .field("len", &self.set.len())
            .finish()
    }
}
