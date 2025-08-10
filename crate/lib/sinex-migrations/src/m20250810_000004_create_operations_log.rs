//! Migration to create replay_operations table for tracking replay operations with operation_id
//!
//! This table tracks replay operations with unique operation IDs

use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create core.replay_operations table
        manager
            .create_table(
                Table::create()
                    .table((Alias::new("core"), ReplayOperations::Table))
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ReplayOperations::OperationId)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(ReplayOperations::OperationType)
                            .string()
                            .not_null()
                            .default("replay"),
                    )
                    .col(
                        ColumnDef::new(ReplayOperations::Status)
                            .string()
                            .not_null()
                            .default("draft"),
                    )
                    .col(ColumnDef::new(ReplayOperations::Metadata).json())
                    .col(
                        ColumnDef::new(ReplayOperations::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(ColumnDef::new(ReplayOperations::ExecutedAt).timestamp_with_time_zone())
                    .col(ColumnDef::new(ReplayOperations::CompletedAt).timestamp_with_time_zone())
                    .col(ColumnDef::new(ReplayOperations::ErrorMessage).text())
                    .col(
                        ColumnDef::new(ReplayOperations::EventCount)
                            .big_integer()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(ReplayOperations::AffectedEventIds)
                            .array(ColumnType::custom("ULID")),
                    )
                    .col(ColumnDef::new(ReplayOperations::Provenance).json())
                    .to_owned(),
            )
            .await?;

        // Create indexes
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_replay_operations_type_status")
                    .table((Alias::new("core"), ReplayOperations::Table))
                    .col(ReplayOperations::OperationType)
                    .col(ReplayOperations::Status)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_replay_operations_created_at")
                    .table((Alias::new("core"), ReplayOperations::Table))
                    .col(ReplayOperations::CreatedAt)
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
                    
                    -- If operation_id is set, add it to event metadata
                    IF current_op_id IS NOT NULL AND current_op_id != '' THEN
                        NEW.metadata = COALESCE(NEW.metadata, '{}'::jsonb) || 
                                      jsonb_build_object('operation_id', current_op_id);
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
                    .name("idx_replay_operations_created_at")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_index(
                Index::drop()
                    .name("idx_replay_operations_type_status")
                    .to_owned(),
            )
            .await?;

        // Drop table
        manager
            .drop_table(
                Table::drop()
                    .table((Alias::new("core"), ReplayOperations::Table))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum ReplayOperations {
    Table,
    OperationId,
    OperationType,
    Status,
    Metadata,
    CreatedAt,
    ExecutedAt,
    CompletedAt,
    ErrorMessage,
    EventCount,
    AffectedEventIds,
    Provenance,
}
