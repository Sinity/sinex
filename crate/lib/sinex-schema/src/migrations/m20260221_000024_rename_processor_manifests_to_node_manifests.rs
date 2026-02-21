//! Rename `core.processor_manifests` → `core.node_manifests` and
//! `processor_name` → `node_name` column to align with the "node" terminology.

use sea_orm_migration::prelude::*;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20260221_000024_rename_processor_manifests_to_node_manifests"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE core.processor_manifests
                 RENAME COLUMN processor_name TO node_name;
                 ALTER TABLE core.processor_manifests
                 RENAME TO node_manifests;",
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE core.node_manifests
                 RENAME TO processor_manifests;
                 ALTER TABLE core.processor_manifests
                 RENAME COLUMN node_name TO processor_name;",
            )
            .await?;
        Ok(())
    }
}
