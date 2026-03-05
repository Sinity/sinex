//! Remove automatic retention policy from `core.events`.
//!
//! Sinex keeps deletion explicit and auditable through the lifecycle flow:
//! `core.events` -> `audit.archived_events` -> `core.event_tombstones`.
//! Automatic chunk drops are intentionally disabled.

use crate::schema::{Events, TableDef};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Remove any existing retention policy to prevent silent data destruction.
        // This is idempotent - if no policy exists, it's a no-op.
        let sql = format!(
            "SELECT remove_retention_policy('{}.{}', if_exists => true);",
            Events::schema_name(),
            Events::table_name()
        );
        manager.get_connection().execute_unprepared(&sql).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Intentionally a no-op: automatic retention remains disabled.
        let _ = manager;
        Ok(())
    }
}
