//! Migration to create operations_log table for tracking all operations with operation_id
//!
//! This table tracks replay/archive/restore operations per TARGET_canonical.md specification

use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create core.operations_log table per TARGET_canonical.md spec
        manager
            .create_table(
                Table::create()
                    .table((Alias::new("core"), OperationsLog::Table))
                    .if_not_exists()
                    .col(
                        ColumnDef::new(OperationsLog::OperationId)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(OperationsLog::Actor).text().not_null())
                    .col(ColumnDef::new(OperationsLog::Scope).json().not_null())
                    .col(ColumnDef::new(OperationsLog::PreviewSummary).json())
                    .col(
                        ColumnDef::new(OperationsLog::StartedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(ColumnDef::new(OperationsLog::FinishedAt).timestamp_with_time_zone())
                    .col(ColumnDef::new(OperationsLog::Outcome).text())
                    .col(ColumnDef::new(OperationsLog::ErrorDetails).text())
                    .col(
                        ColumnDef::new(OperationsLog::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        // Add CHECK constraint for outcome values
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.operations_log 
                ADD CONSTRAINT operations_log_outcome_check 
                CHECK (outcome IN ('success', 'error', 'cancelled'));
                "#,
            )
            .await?;

        // Create indexes
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_operations_log_actor_started")
                    .table((Alias::new("core"), OperationsLog::Table))
                    .col(OperationsLog::Actor)
                    .col(OperationsLog::StartedAt)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_operations_log_outcome_started")
                    .table((Alias::new("core"), OperationsLog::Table))
                    .col(OperationsLog::Outcome)
                    .col(OperationsLog::StartedAt)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_operations_log_started_at")
                    .table((Alias::new("core"), OperationsLog::Table))
                    .col(OperationsLog::StartedAt)
                    .to_owned(),
            )
            .await?;

        // Create function to set operation_id session variable
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

                CREATE OR REPLACE FUNCTION get_operation_id()
                RETURNS TEXT AS $$
                BEGIN
                    RETURN current_setting('sinex.operation_id', true);
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Create trigger to automatically track operation_id in events
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
        // Drop trigger and functions
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TRIGGER IF EXISTS track_operation_trigger ON core.events;
                DROP FUNCTION IF EXISTS track_operation_in_event();
                DROP FUNCTION IF EXISTS get_operation_id();
                DROP FUNCTION IF EXISTS set_operation_id(TEXT);
                "#,
            )
            .await?;

        // Drop indexes
        manager
            .drop_index(
                Index::drop()
                    .name("idx_operations_log_started_at")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_index(
                Index::drop()
                    .name("idx_operations_log_outcome_started")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_index(
                Index::drop()
                    .name("idx_operations_log_actor_started")
                    .to_owned(),
            )
            .await?;

        // Drop table
        manager
            .drop_table(
                Table::drop()
                    .table((Alias::new("core"), OperationsLog::Table))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum OperationsLog {
    Table,
    OperationId,
    Actor,
    Scope,
    PreviewSummary,
    StartedAt,
    FinishedAt,
    Outcome,
    ErrorDetails,
    CreatedAt,
}
