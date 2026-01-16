//! Add trigram index for payload text search on core.events.

use crate::schema::{Events, TableDef};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(r#"CREATE EXTENSION IF NOT EXISTS "pg_trgm";"#)
            .await?;

        let index_sql = format!(
            "CREATE INDEX IF NOT EXISTS ix_events_payload_trgm ON {}.{} USING GIN ((payload::text) gin_trgm_ops)",
            Events::schema_name(),
            Events::table_name()
        );
        manager
            .get_connection()
            .execute_unprepared(&index_sql)
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP INDEX IF EXISTS core.ix_events_payload_trgm")
            .await?;
        Ok(())
    }
}
