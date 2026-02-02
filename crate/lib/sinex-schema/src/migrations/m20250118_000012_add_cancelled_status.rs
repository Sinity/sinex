//! Add 'cancelled' to `operations_log` `result_status` constraint
//!
//! The ops.cancel handler sets `result_status` = 'cancelled' but the original
//! check constraint only allowed: success, failure, partial, running.
//!
//! This migration adds 'cancelled' as a valid status.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();

        // Drop the old constraint and add the new one with 'cancelled' included
        conn.execute_unprepared(
            "ALTER TABLE core.operations_log
             DROP CONSTRAINT IF EXISTS operations_log_result_status_check",
        )
        .await?;

        conn.execute_unprepared(
            "ALTER TABLE core.operations_log
             ADD CONSTRAINT operations_log_result_status_check
             CHECK (result_status IN ('success', 'failure', 'partial', 'running', 'cancelled'))",
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();

        // Revert to original constraint (operations with 'cancelled' status would need cleanup first)
        conn.execute_unprepared(
            "ALTER TABLE core.operations_log
             DROP CONSTRAINT IF EXISTS operations_log_result_status_check",
        )
        .await?;

        conn.execute_unprepared(
            "ALTER TABLE core.operations_log
             ADD CONSTRAINT operations_log_result_status_check
             CHECK (result_status IN ('success', 'failure', 'partial', 'running'))",
        )
        .await?;

        Ok(())
    }
}
