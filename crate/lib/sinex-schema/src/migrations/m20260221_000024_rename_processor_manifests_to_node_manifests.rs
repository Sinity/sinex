//! Rename `core.processor_manifests` → `core.node_manifests` and
//! `node_name` → `node_name` column to align with the "node" terminology.

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
        // Conditional: only rename if the old table still exists.
        // On fresh databases the schema already creates node_manifests directly,
        // so there is nothing to rename.
        manager
            .get_connection()
            .execute_unprepared(
                "DO $$
                 BEGIN
                   IF EXISTS (
                     SELECT 1 FROM information_schema.tables
                     WHERE table_schema = 'core'
                       AND table_name = 'processor_manifests'
                   ) THEN
                     ALTER TABLE core.processor_manifests
                       RENAME COLUMN node_name TO node_name;
                     ALTER TABLE core.processor_manifests
                       RENAME TO node_manifests;
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
                "ALTER TABLE core.node_manifests
                 RENAME TO processor_manifests;
                 ALTER TABLE core.processor_manifests
                 RENAME COLUMN node_name TO node_name;",
            )
            .await?;
        Ok(())
    }
}
