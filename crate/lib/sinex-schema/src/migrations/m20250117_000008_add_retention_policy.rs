//! Add 90-day retention policy for core.events
//!
//! **Issue 60 (HIGH)**: No TimescaleDB Retention Policy
//!
//! The 90-day retention period is documented in the schema but was never
//! enforced at the database level. Without this policy, data accumulates
//! indefinitely, leading to:
//! - Unbounded storage growth
//! - Degraded query performance over time
//! - Potential disk exhaustion
//!
//! This migration adds a TimescaleDB retention policy that automatically
//! drops chunks older than 90 days. The policy runs as a background job
//! and is managed by TimescaleDB's job scheduler.
//!
//! ## Rollback Safety
//!
//! Removing the retention policy does NOT restore deleted data. If you need
//! to preserve older data, increase the retention interval BEFORE it expires:
//! ```sql
//! SELECT add_retention_policy('core.events', INTERVAL '180 days', if_not_exists => true);
//! ```

use crate::schema::{Events, TableDef};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let sql = format!(
            "SELECT add_retention_policy('{}.{}', INTERVAL '90 days', if_not_exists => true);",
            Events::schema_name(),
            Events::table_name()
        );
        manager.get_connection().execute_unprepared(&sql).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // WARNING: Removing the retention policy does NOT restore deleted data
        // This only prevents future automatic deletions
        let sql = format!(
            "SELECT remove_retention_policy('{}.{}', if_exists => true);",
            Events::schema_name(),
            Events::table_name()
        );
        manager.get_connection().execute_unprepared(&sql).await?;
        Ok(())
    }
}
