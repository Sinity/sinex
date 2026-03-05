//! Drop retired coordination tables
//!
//! This migration drops the `core.node_instances` and `core.service_leadership` tables
//! which have been replaced by NATS KV-based coordination (`KV_sinex_instances` and
//! `KV_sinex_leadership` buckets).
//!
//! These tables were never part of the canonical schema migration but may have been
//! created manually during development. This migration safely drops them if they exist.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop tables in reverse dependency order
        // service_leadership may have referenced node_instances
        manager
            .get_connection()
            .execute_unprepared(
                r"
                DROP TABLE IF EXISTS core.service_leadership CASCADE;
                DROP TABLE IF EXISTS core.node_instances CASCADE;
                ",
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Recreate the tables for rollback purposes.
        // This is a best-effort reconstruction from the table shape expected by this migration.
        manager
            .get_connection()
            .execute_unprepared(
                r"
                CREATE TABLE IF NOT EXISTS core.node_instances (
                    instance_id TEXT PRIMARY KEY,
                    service_name TEXT NOT NULL,
                    hostname TEXT NOT NULL,
                    version TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL,
                    last_heartbeat TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_node_instances_service
                    ON core.node_instances(service_name);

                CREATE INDEX IF NOT EXISTS idx_node_instances_heartbeat
                    ON core.node_instances(last_heartbeat);

                CREATE TABLE IF NOT EXISTS core.service_leadership (
                    service_name TEXT PRIMARY KEY,
                    leader_instance_id TEXT NOT NULL,
                    acquired_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_service_leadership_expires
                    ON core.service_leadership(expires_at);
                ",
            )
            .await?;

        Ok(())
    }
}
