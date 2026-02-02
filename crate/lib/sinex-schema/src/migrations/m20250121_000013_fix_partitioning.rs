//! Fixes partitioning function volatility and enforces chunk interval
//!
//! **Issue**: `ulid_to_timestamptz` relied on implicit casting which is volatile (dependant on session time zone).
//! Hypertable partitioning functions MUST be immutable.
//!
//! **Fix**:
//! 1. Replaces `ulid_to_timestamptz` with a version using explicit `AT TIME ZONE 'UTC'`.
//! 2. Explicitly sets `chunk_time_interval` to 7 days to ensure consistency.

use crate::schema::{Events, TableDef};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. Redefine ulid_to_timestamptz to be immutable
        manager
            .get_connection()
            .execute_unprepared(
                r#"
            CREATE OR REPLACE FUNCTION public.ulid_to_timestamptz(input ULID)
            RETURNS TIMESTAMPTZ
            AS 'SELECT (input::text::ulid)::timestamp AT TIME ZONE ''UTC'''
            LANGUAGE sql
            IMMUTABLE;
            "#,
            )
            .await?;

        // 2. Enforce chunk interval
        let sql = format!(
            "SELECT set_chunk_time_interval('{}.{}', INTERVAL '7 days');",
            Events::schema_name(),
            Events::table_name()
        );
        manager.get_connection().execute_unprepared(&sql).await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Revert function to previous definition (technically volatile but that was the old state)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
            CREATE OR REPLACE FUNCTION public.ulid_to_timestamptz(input ULID)
            RETURNS TIMESTAMPTZ
            AS 'SELECT input::timestamp'
            LANGUAGE sql
            IMMUTABLE;
            "#,
            )
            .await?;
        Ok(())
    }
}
