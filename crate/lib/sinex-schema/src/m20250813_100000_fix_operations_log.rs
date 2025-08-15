//! Fix operations_log table - add missing checkpoint column

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add checkpoint column to operations_log
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.operations_log 
                ADD COLUMN IF NOT EXISTS checkpoint JSONB
            "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop checkpoint column
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.operations_log 
                DROP COLUMN IF EXISTS checkpoint
            "#,
            )
            .await?;

        Ok(())
    }
}
