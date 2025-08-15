//! Change scope column from TEXT to JSONB in operations_log

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Change scope column to JSONB
        // First drop the default, then change type, then set new default
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.operations_log 
                ALTER COLUMN scope DROP DEFAULT;
                
                ALTER TABLE core.operations_log 
                ALTER COLUMN scope TYPE JSONB USING 
                    CASE 
                        WHEN scope = 'unknown' THEN '{}'::jsonb
                        ELSE scope::jsonb
                    END;
                    
                ALTER TABLE core.operations_log 
                ALTER COLUMN scope SET DEFAULT '{}'::jsonb;
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
                ALTER COLUMN scope TYPE TEXT USING scope::text,
                ALTER COLUMN scope SET DEFAULT 'unknown'::text
            "#,
            )
            .await?;

        Ok(())
    }
}
