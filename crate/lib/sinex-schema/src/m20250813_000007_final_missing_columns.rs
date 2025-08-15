use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. Add metadata column to entity_relations
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.entity_relations 
                ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb
            "#,
            )
            .await?;

        // 2. Add columns to source_material_registry
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                ADD COLUMN IF NOT EXISTS retention_policy TEXT DEFAULT 'permanent',
                ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN IF NOT EXISTS archive_time TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            "#,
            )
            .await?;

        // 3. Add indexes to help with queries
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Add indexes for better query performance
                CREATE INDEX IF NOT EXISTS idx_source_material_registry_ingestion_time 
                ON raw.source_material_registry(ingestion_time);
                
                CREATE INDEX IF NOT EXISTS idx_entity_relations_metadata 
                ON core.entity_relations USING gin(metadata);
            "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the added columns
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.entity_relations 
                DROP COLUMN IF EXISTS metadata;
                
                ALTER TABLE raw.source_material_registry 
                DROP COLUMN IF EXISTS retention_policy,
                DROP COLUMN IF EXISTS ingestion_time,
                DROP COLUMN IF EXISTS archive_time,
                DROP COLUMN IF EXISTS updated_at;
                
                DROP INDEX IF EXISTS idx_source_material_registry_ingestion_time;
                DROP INDEX IF EXISTS idx_entity_relations_metadata;
            "#,
            )
            .await?;

        Ok(())
    }
}
