//! Add recommended serving indexes from TARGET_final.md
//!
//! These indexes improve query performance for common access patterns.

use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add recommended serving indexes from TARGET_final.md E.1
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Recommended serving indexes for time-based queries
                CREATE INDEX IF NOT EXISTS ix_events_ts_orig 
                ON core.events (ts_orig DESC);
                
                CREATE INDEX IF NOT EXISTS ix_events_type_ts 
                ON core.events (event_type, ts_orig DESC);
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
                DROP INDEX IF EXISTS core.ix_events_ts_orig;
                DROP INDEX IF EXISTS core.ix_events_type_ts;
                "#,
            )
            .await?;

        Ok(())
    }
}
