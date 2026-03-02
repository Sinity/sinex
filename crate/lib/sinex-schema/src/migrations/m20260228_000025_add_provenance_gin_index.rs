//! Add GIN index on `core.events.source_event_ids` for efficient provenance
//! descendant queries. Without this index, `WHERE $id = ANY(source_event_ids)`
//! requires a full table scan. Ancestor queries already use the primary key.

use sea_orm_migration::prelude::*;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20260228_000025_add_provenance_gin_index"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // GIN index enables efficient array-contains lookups for descendant lineage.
        // Note: TimescaleDB hypertables don't support CONCURRENTLY — the index is
        // created across all chunks automatically by TimescaleDB's DDL interception.
        manager
            .get_connection()
            .execute_unprepared(
                "CREATE INDEX IF NOT EXISTS ix_events_source_event_ids
                 ON core.events USING GIN (source_event_ids);",
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP INDEX IF EXISTS core.ix_events_source_event_ids;")
            .await?;
        Ok(())
    }
}
