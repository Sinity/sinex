//! Add missing columns to operations_log table

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add missing columns to operations_log
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.operations_log 
                ADD COLUMN IF NOT EXISTS approved_by TEXT,
                ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS executor_node TEXT
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
                DROP COLUMN IF EXISTS approved_by,
                DROP COLUMN IF EXISTS approved_at,
                DROP COLUMN IF EXISTS executor_node
            "#,
            )
            .await?;

        Ok(())
    }
}
