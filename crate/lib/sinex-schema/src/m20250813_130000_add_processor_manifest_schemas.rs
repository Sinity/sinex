//! Add missing input_schemas and output_schemas columns to processor_manifests

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add missing schema columns to processor_manifests
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.processor_manifests 
                ADD COLUMN IF NOT EXISTS input_schemas JSONB,
                ADD COLUMN IF NOT EXISTS output_schemas JSONB
            "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.processor_manifests 
                DROP COLUMN IF EXISTS input_schemas,
                DROP COLUMN IF EXISTS output_schemas
            "#,
            )
            .await?;

        Ok(())
    }
}
