//! Add missing approved_by and input_schemas columns

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add approved_by column to event_payload_schemas
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE sinex_schemas.event_payload_schemas 
                ADD COLUMN IF NOT EXISTS approved_by TEXT,
                ADD COLUMN IF NOT EXISTS input_schemas JSONB
            "#,
            )
            .await?;

        // Add approved_by to any other tables that might need it
        // (check errors to see which tables need this column)

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE sinex_schemas.event_payload_schemas 
                DROP COLUMN IF EXISTS approved_by,
                DROP COLUMN IF EXISTS input_schemas
            "#,
            )
            .await?;

        Ok(())
    }
}
