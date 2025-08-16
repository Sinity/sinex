//! Add associated_blob_ids column to events table

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add associated_blob_ids column to core.events table
        manager
            .get_connection()
            .execute_unprepared(
                r#"
            ALTER TABLE core.events 
            ADD COLUMN IF NOT EXISTS associated_blob_ids ULID[];
            "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Remove associated_blob_ids column
        manager
            .get_connection()
            .execute_unprepared(
                r#"
            ALTER TABLE core.events 
            DROP COLUMN IF EXISTS associated_blob_ids;
            "#,
            )
            .await?;

        Ok(())
    }
}
