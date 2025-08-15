//! Add missing created_at column to operations_log

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add created_at column to operations_log
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.operations_log 
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
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
                ALTER TABLE core.operations_log 
                DROP COLUMN IF EXISTS created_at
            "#,
            )
            .await?;

        Ok(())
    }
}
