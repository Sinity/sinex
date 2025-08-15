//! Add missing runtime_requirements column to processor_manifests

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add runtime_requirements column to processor_manifests
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.processor_manifests 
                ADD COLUMN IF NOT EXISTS runtime_requirements JSONB
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
                DROP COLUMN IF EXISTS runtime_requirements
            "#,
            )
            .await?;

        Ok(())
    }
}
