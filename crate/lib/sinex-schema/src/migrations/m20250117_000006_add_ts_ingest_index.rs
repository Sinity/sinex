//! Add index on ts_ingest column for core.events
//!
//! **Issue 62 (MEDIUM)**: Missing ts_ingest Index
//!
//! Most queries filter on ts_ingest (the actual ingestion timestamp) but only
//! ts_orig (the original event timestamp) was indexed. This migration adds a
//! descending index on ts_ingest to optimize queries that filter or sort by
//! ingestion time.

use crate::schema::{Events, TableDef};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let index_sql = format!(
            "CREATE INDEX IF NOT EXISTS ix_events_ts_ingest ON {}.{} (ts_ingest DESC)",
            Events::schema_name(),
            Events::table_name()
        );
        manager
            .get_connection()
            .execute_unprepared(&index_sql)
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP INDEX IF EXISTS core.ix_events_ts_ingest")
            .await?;
        Ok(())
    }
}
