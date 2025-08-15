//! Change preview_summary column from TEXT to JSONB in operations_log

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Change preview_summary column to JSONB
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.operations_log 
                ALTER COLUMN preview_summary DROP DEFAULT;
                
                ALTER TABLE core.operations_log 
                ALTER COLUMN preview_summary TYPE JSONB USING 
                    CASE 
                        WHEN preview_summary IS NULL THEN NULL
                        ELSE preview_summary::jsonb
                    END;
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
                ALTER COLUMN preview_summary TYPE TEXT USING preview_summary::text
            "#,
            )
            .await?;

        Ok(())
    }
}
