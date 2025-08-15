use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add missing columns to processor_manifests
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.processor_manifests 
                ADD COLUMN IF NOT EXISTS processor_type TEXT DEFAULT 'generic',
                ADD COLUMN IF NOT EXISTS description TEXT,
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
                DROP COLUMN IF EXISTS processor_type,
                DROP COLUMN IF EXISTS description,
                DROP COLUMN IF EXISTS created_at,
                DROP COLUMN IF EXISTS updated_at
            "#,
            )
            .await?;

        Ok(())
    }
}
