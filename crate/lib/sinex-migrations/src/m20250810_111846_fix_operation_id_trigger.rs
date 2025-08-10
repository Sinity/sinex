//! Fix the operation_id trigger to use payload instead of metadata field

use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop existing trigger and function first
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TRIGGER IF EXISTS track_operation_trigger ON core.events;
                DROP FUNCTION IF EXISTS track_operation_in_event();
                "#,
            )
            .await?;

        // Recreate function with correct payload handling
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION track_operation_in_event()
                RETURNS TRIGGER AS $$
                DECLARE
                    current_op_id TEXT;
                BEGIN
                    -- Get current operation_id from session
                    current_op_id := current_setting('sinex.operation_id', true);
                    
                    -- If operation_id is set, add it to event payload under _meta
                    IF current_op_id IS NOT NULL AND current_op_id != '' THEN
                        NEW.payload = NEW.payload || 
                                      jsonb_build_object('_meta', jsonb_build_object('operation_id', current_op_id));
                    END IF;
                    
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                CREATE TRIGGER track_operation_trigger
                BEFORE INSERT ON core.events
                FOR EACH ROW EXECUTE FUNCTION track_operation_in_event();
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the trigger and function
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TRIGGER IF EXISTS track_operation_trigger ON core.events;
                DROP FUNCTION IF EXISTS track_operation_in_event();
                "#,
            )
            .await?;

        Ok(())
    }
}
