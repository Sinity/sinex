//! Rename `core.events.ingestor_version` → `node_version` to align with the
//! unified "node" terminology (not all event publishers are ingestors).

use sea_orm_migration::prelude::*;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20260221_000023_rename_ingestor_version_to_node_version"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Conditional: only rename if the old column still exists.
        // On fresh databases the canonical schema already creates node_version directly,
        // so there is nothing to rename.
        manager
            .get_connection()
            .execute_unprepared(
                "DO $$
                 BEGIN
                   IF EXISTS (
                     SELECT 1 FROM information_schema.columns
                     WHERE table_schema = 'core'
                       AND table_name = 'events'
                       AND column_name = 'ingestor_version'
                   ) THEN
                     ALTER TABLE core.events
                       RENAME COLUMN ingestor_version TO node_version;
                   END IF;
                 END;
                 $$;",
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE core.events
                 RENAME COLUMN node_version TO ingestor_version;",
            )
            .await?;
        Ok(())
    }
}
