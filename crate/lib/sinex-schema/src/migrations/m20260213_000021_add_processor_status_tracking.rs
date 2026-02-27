//! Add status tracking columns to `core.node_manifests`
//!
//! This migration adds runtime status tracking to the processor manifests table:
//! - `status`: Current processor state (active, inactive, etc.)
//! - `last_heartbeat_at`: Timestamp of the most recent heartbeat from this processor
//!
//! These columns enable efficient queries for active processors without scanning
//! the events table for heartbeat records.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(PROCESSOR_STATUS_UP)
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(PROCESSOR_STATUS_DOWN)
            .await?;

        Ok(())
    }
}

const PROCESSOR_STATUS_UP: &str = r"
ALTER TABLE core.node_manifests ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';
ALTER TABLE core.node_manifests ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_processors_status ON core.node_manifests(status);
CREATE INDEX IF NOT EXISTS idx_processors_heartbeat ON core.node_manifests(last_heartbeat_at);
";

const PROCESSOR_STATUS_DOWN: &str = r"
DROP INDEX IF EXISTS core.idx_processors_heartbeat;
DROP INDEX IF EXISTS core.idx_processors_status;
ALTER TABLE core.node_manifests DROP COLUMN IF EXISTS last_heartbeat_at;
ALTER TABLE core.node_manifests DROP COLUMN IF EXISTS status;
";
