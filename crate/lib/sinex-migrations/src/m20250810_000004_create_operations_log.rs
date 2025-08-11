//! Migration to create operations_log table for tracking all operations
//!
//! This table tracks replay/archive/restore operations per TARGET_canonical.md specification

use crate::schema::OperationsLog;
use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create core.operations_log table using schema definition
        manager
            .get_connection()
            .execute_unprepared(&OperationsLog::create_table())
            .await?;

        // Create indexes
        for index_sql in OperationsLog::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create function to set operation_id session variable (used by triggers)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION set_operation_id(op_id TEXT)
                RETURNS VOID AS $$
                BEGIN
                    PERFORM set_config('sinex.operation_id', op_id, false);
                END;
                $$ LANGUAGE plpgsql;
                
                -- Create function to track operation in event payload
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
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TRIGGER IF EXISTS track_operation_trigger ON core.events;
                DROP FUNCTION IF EXISTS track_operation_in_event();
                DROP FUNCTION IF EXISTS set_operation_id(TEXT);
                DROP TABLE IF EXISTS core.operations_log CASCADE;
                "#,
            )
            .await?;

        Ok(())
    }
}
