//! Configure TimescaleDB chunk interval for core.events
//!
//! **Issue 61 (MEDIUM)**: No Chunk Size Configuration
//!
//! TimescaleDB's default chunk interval is 7 days, which may not be optimal
//! for all workloads. This migration sets an explicit 7-day chunk interval
//! to make the configuration explicit and documented.
//!
//! The 7-day interval is appropriate for typical event ingestion patterns:
//! - Balances chunk size vs. query performance
//! - Aligns with weekly reporting cycles
//! - Prevents excessive chunk proliferation
//!
//! Operators can adjust this value later based on actual query patterns and
//! data volume by running:
//! ```sql
//! SELECT set_chunk_time_interval('core.events', INTERVAL 'N days');
//! ```

use crate::schema::{Events, TableDef};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let sql = format!(
            "SELECT set_chunk_time_interval('{}.{}', INTERVAL '7 days');",
            Events::schema_name(),
            Events::table_name()
        );
        manager.get_connection().execute_unprepared(&sql).await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // Chunk interval changes are non-destructive and don't need rollback
        // Future chunks will use the default/previous interval automatically
        Ok(())
    }
}
