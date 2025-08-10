//! Fix operations_log table to match TARGET_final.md specification
//!
//! This migration:
//! 1. Drops the incorrectly named replay_operations table if it exists
//! 2. Creates the correct core.operations_log table per spec

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop any incorrectly named table if it exists
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS core.replay_operations CASCADE;")
            .await?;

        // Drop existing operations_log table with wrong schema and recreate with correct schema
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Drop existing operations_log table (with wrong schema) if it exists
                DROP TABLE IF EXISTS core.operations_log CASCADE;
                
                -- Create the correct operations_log table per TARGET_canonical.md spec
                CREATE TABLE core.operations_log (
                    operation_id ULID PRIMARY KEY,
                    actor TEXT NOT NULL,
                    scope JSONB NOT NULL,
                    preview_summary JSONB,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    outcome TEXT CHECK (outcome IN ('success', 'error', 'cancelled')),
                    error_details TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                
                -- Create indexes
                CREATE INDEX idx_operations_log_actor_started
                ON core.operations_log (actor, started_at);
                
                CREATE INDEX idx_operations_log_outcome_started
                ON core.operations_log (outcome, started_at);
                
                CREATE INDEX idx_operations_log_started_at
                ON core.operations_log (started_at);
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the operations_log table
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TABLE IF EXISTS core.operations_log CASCADE;
                "#,
            )
            .await?;

        Ok(())
    }
}
