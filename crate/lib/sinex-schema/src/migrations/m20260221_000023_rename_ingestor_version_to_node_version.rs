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
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE core.events
                 RENAME COLUMN ingestor_version TO node_version;",
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
