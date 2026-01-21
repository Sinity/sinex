//! Drop expensive GIN indexes on core.events payload to reduce write amplification.
//!
//! Reverting migrations:
//! - m20250115_000003_add_events_payload_trgm_index
//! - m20250115_000004_add_events_payload_fts_index

use crate::schema::{Events, TableDef};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop Trigram Index
        manager
            .get_connection()
            .execute_unprepared(&format!(
                "DROP INDEX IF EXISTS {}.ix_events_payload_trgm",
                Events::schema_name()
            ))
            .await?;

        // Drop FTS Index
        manager
            .get_connection()
            .execute_unprepared(&format!(
                "DROP INDEX IF EXISTS {}.ix_events_payload_fts",
                Events::schema_name()
            ))
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Restore Trigram Index
        let trgm_sql = format!(
            "CREATE INDEX IF NOT EXISTS ix_events_payload_trgm ON {}.{} USING GIN ((payload::text) gin_trgm_ops)",
            Events::schema_name(),
            Events::table_name()
        );
        manager
            .get_connection()
            .execute_unprepared(&trgm_sql)
            .await?;

        // Restore FTS Index
        let fts_sql = format!(
            "CREATE INDEX IF NOT EXISTS ix_events_payload_fts ON {}.{} USING GIN (to_tsvector('simple', payload::text))",
            Events::schema_name(),
            Events::table_name()
        );
        manager
            .get_connection()
            .execute_unprepared(&fts_sql)
            .await?;

        Ok(())
    }
}
