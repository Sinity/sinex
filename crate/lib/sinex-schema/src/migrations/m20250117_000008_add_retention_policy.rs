//! Remove automatic retention policy - embrace principled forgetting instead
//!
//! ## Historical Context
//!
//! This migration originally added a 90-day TimescaleDB retention policy to `core.events`.
//! However, this was philosophically inconsistent with Sinex's manifesto of "immutable
//! event log" and "complete history":
//!
//! **The Problem**: TimescaleDB's `drop_chunks()` is a HARD DELETE that bypasses SQL triggers.
//! The archive-on-delete trigger in `core.fn_archive_before_delete()` never fires for chunk
//! drops, meaning events were silently destroyed without any audit trail.
//!
//! ## New Philosophy: "Principled Forgetting"
//!
//! Instead of silent automatic deletion, Sinex now uses an explicit three-tier lifecycle:
//!
//! ```text
//! Live (core.events) ←→ Archive (audit.archived_events) → Tombstone (core.event_tombstones)
//! ```
//!
//! - **Live → Archive**: User-initiated, preserves full data, reversible
//! - **Archive → Tombstone**: User-initiated, preserves skeleton only, permanent
//! - **No automatic deletion**: User controls their data explicitly
//!
//! See migration `m20260203_000019_add_event_tombstones` for the implementation.
//!
//! ## Migration Behavior
//!
//! This migration now:
//! - **Up**: Removes any existing retention policy (idempotent)
//! - **Down**: No-op (we don't want to restore automatic deletion)
//!
//! If you need storage management, use `sinexctl lifecycle` commands instead.

use crate::schema::{Events, TableDef};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Remove any existing retention policy to prevent silent data destruction.
        // This is idempotent - if no policy exists, it's a no-op.
        let sql = format!(
            "SELECT remove_retention_policy('{}.{}', if_exists => true);",
            Events::schema_name(),
            Events::table_name()
        );
        manager.get_connection().execute_unprepared(&sql).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Intentionally a no-op.
        // We do NOT want to restore automatic silent deletion on rollback.
        // If someone truly needs retention policies, they can add them manually
        // with full understanding of the implications.
        let _ = manager;
        Ok(())
    }
}
