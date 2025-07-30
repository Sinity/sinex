//! Checkpoint query registry for centralized checkpoint operations
//!
//! This module provides all database queries related to processor checkpoint
//! storage, retrieval, and management. All queries automatically handle
//! ULID/UUID conversion and provide consistent error handling.

use crate::query_builder::{QueryBuilder, QueryParam};
use crate::query_helpers::{db_error, DbResult};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;
use sqlx::PgPool;

/// Checkpoint query registry with centralized checkpoint operations
pub struct CheckpointQueries;

impl CheckpointQueries {
    /// Get checkpoint by processor name and consumer group
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<CheckpointRecord>(pool)`
    pub fn get_checkpoint(
        processor_name: String,
        consumer_group: String,
        consumer_name: String,
    ) -> QueryBuilder {
        QueryBuilder::select("core.processor_checkpoints")
            .columns(&[
                "id::uuid as \"id!\"",
                "processor_name as \"processor_name!\"",
                "consumer_group as \"consumer_group!\"",
                "consumer_name as \"consumer_name!\"",
                "last_processed_id",
                "processed_count as \"processed_count!\"",
                "last_activity as \"last_activity!\"",
                "state_data",
                "checkpoint_version as \"checkpoint_version!\"",
                "checkpoint_data",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("consumer_group", QueryParam::String(consumer_group))
            .where_eq("consumer_name", QueryParam::String(consumer_name))
    }

    /// Upsert checkpoint (insert or update)
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn upsert_checkpoint(
        id: Ulid,
        processor_name: String,
        consumer_group: String,
        consumer_name: String,
        last_processed_id: Option<Ulid>,
        processed_count: i64,
        last_activity: DateTime<Utc>,
        state_data: Option<JsonValue>,
        checkpoint_version: i32,
        checkpoint_data: Option<JsonValue>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> QueryBuilder {
        // This is a complex upsert query that needs raw SQL
        // We'll use a custom implementation here
        Self::build_upsert_checkpoint_query(
            id,
            processor_name,
            consumer_group,
            consumer_name,
            last_processed_id,
            processed_count,
            last_activity,
            state_data,
            checkpoint_version,
            checkpoint_data,
            created_at,
            updated_at,
        )
    }

    /// Internal helper to build upsert checkpoint query with ON CONFLICT
    fn build_upsert_checkpoint_query(
        id: Ulid,
        processor_name: String,
        consumer_group: String,
        consumer_name: String,
        last_processed_id: Option<Ulid>,
        processed_count: i64,
        last_activity: DateTime<Utc>,
        state_data: Option<JsonValue>,
        checkpoint_version: i32,
        checkpoint_data: Option<JsonValue>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> QueryBuilder {
        // For complex UPSERT with ON CONFLICT, we need a custom query
        // This will be implemented as a raw SQL query since QueryBuilder doesn't support ON CONFLICT yet
        let mut builder = QueryBuilder::insert("core.processor_checkpoints");
        builder = builder.columns(&[
            "id",
            "processor_name",
            "consumer_group",
            "consumer_name",
            "last_processed_id",
            "processed_count",
            "last_activity",
            "state_data",
            "checkpoint_version",
            "checkpoint_data",
            "created_at",
            "updated_at",
        ]);

        builder = builder.values(&[
            QueryParam::Ulid(id),
            QueryParam::String(processor_name),
            QueryParam::String(consumer_group),
            QueryParam::String(consumer_name),
            QueryParam::OptionalUlid(last_processed_id),
            QueryParam::Integer(processed_count),
            QueryParam::Timestamp(last_activity),
            QueryParam::OptionalJson(state_data),
            QueryParam::Integer(checkpoint_version as i64),
            QueryParam::OptionalJson(checkpoint_data),
            QueryParam::Timestamp(created_at),
            QueryParam::Timestamp(updated_at),
        ]);

        // Note: This will generate a basic INSERT. For ON CONFLICT UPSERT,
        // we'll need to use a custom method until QueryBuilder supports it
        builder
    }

    /// Get checkpoint history for a processor
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<CheckpointHistoryRecord>(pool)`
    pub fn get_checkpoint_history(
        processor_name: String,
        consumer_group: String,
        consumer_name: String,
        limit: i64,
    ) -> QueryBuilder {
        QueryBuilder::select("core.processor_checkpoints")
            .columns(&[
                "id::text as \"id!\"",
                "last_processed_id",
                "processed_count as \"processed_count!\"",
                "last_activity as \"last_activity!\"",
                "checkpoint_version as \"checkpoint_version!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("consumer_group", QueryParam::String(consumer_group))
            .where_eq("consumer_name", QueryParam::String(consumer_name))
            .order_by("updated_at", "DESC")
            .limit(limit)
    }

    /// Get checkpoint statistics
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<CheckpointStatsRecord>(pool)`
    pub fn get_checkpoint_stats(
        processor_name: String,
        consumer_group: String,
        consumer_name: String,
    ) -> QueryBuilder {
        QueryBuilder::select("core.processor_checkpoints")
            .columns(&[
                "COUNT(*) as \"total_checkpoints!\"",
                "MAX(processed_count) as \"max_processed\"",
                "MAX(updated_at) as \"last_update\"",
                "MIN(created_at) as \"first_checkpoint\"",
            ])
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("consumer_group", QueryParam::String(consumer_group))
            .where_eq("consumer_name", QueryParam::String(consumer_name))
    }

    /// Delete checkpoint
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_checkpoint(
        processor_name: String,
        consumer_group: String,
        consumer_name: String,
    ) -> QueryBuilder {
        QueryBuilder::delete("core.processor_checkpoints")
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("consumer_group", QueryParam::String(consumer_group))
            .where_eq("consumer_name", QueryParam::String(consumer_name))
    }

    /// Get all checkpoints for a processor (across all consumer groups)
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<CheckpointRecord>(pool)`
    pub fn get_all_checkpoints_for_processor(processor_name: String) -> QueryBuilder {
        QueryBuilder::select("core.processor_checkpoints")
            .columns(&[
                "id::uuid as \"id!\"",
                "processor_name as \"processor_name!\"",
                "consumer_group as \"consumer_group!\"",
                "consumer_name as \"consumer_name!\"",
                "last_processed_id",
                "processed_count as \"processed_count!\"",
                "last_activity as \"last_activity!\"",
                "state_data",
                "checkpoint_version as \"checkpoint_version!\"",
                "checkpoint_data",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_eq("processor_name", QueryParam::String(processor_name))
            .order_by("updated_at", "DESC")
    }

    /// Get checkpoints by consumer group
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<CheckpointRecord>(pool)`
    pub fn get_checkpoints_by_consumer_group(consumer_group: String) -> QueryBuilder {
        QueryBuilder::select("core.processor_checkpoints")
            .columns(&[
                "id::uuid as \"id!\"",
                "processor_name as \"processor_name!\"",
                "consumer_group as \"consumer_group!\"",
                "consumer_name as \"consumer_name!\"",
                "last_processed_id",
                "processed_count as \"processed_count!\"",
                "last_activity as \"last_activity!\"",
                "state_data",
                "checkpoint_version as \"checkpoint_version!\"",
                "checkpoint_data",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_eq("consumer_group", QueryParam::String(consumer_group))
            .order_by("updated_at", "DESC")
    }

    /// Get checkpoints updated after a specific time
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<CheckpointRecord>(pool)`
    pub fn get_checkpoints_updated_after(timestamp: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::select("core.processor_checkpoints")
            .columns(&[
                "id::uuid as \"id!\"",
                "processor_name as \"processor_name!\"",
                "consumer_group as \"consumer_group!\"",
                "consumer_name as \"consumer_name!\"",
                "last_processed_id",
                "processed_count as \"processed_count!\"",
                "last_activity as \"last_activity!\"",
                "state_data",
                "checkpoint_version as \"checkpoint_version!\"",
                "checkpoint_data",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op("updated_at", ">", QueryParam::Timestamp(timestamp))
            .order_by("updated_at", "DESC")
    }

    /// Get checkpoints with version less than specified
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<CheckpointRecord>(pool)`
    pub fn get_checkpoints_with_version_less_than(version: i32) -> QueryBuilder {
        QueryBuilder::select("core.processor_checkpoints")
            .columns(&[
                "id::uuid as \"id!\"",
                "processor_name as \"processor_name!\"",
                "consumer_group as \"consumer_group!\"",
                "consumer_name as \"consumer_name!\"",
                "last_processed_id",
                "processed_count as \"processed_count!\"",
                "last_activity as \"last_activity!\"",
                "state_data",
                "checkpoint_version as \"checkpoint_version!\"",
                "checkpoint_data",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op(
                "checkpoint_version",
                "<",
                QueryParam::Integer(version as i64),
            )
            .order_by("updated_at", "DESC")
    }

    /// Count checkpoints by processor
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn count_checkpoints_by_processor(processor_name: String) -> QueryBuilder {
        QueryBuilder::select("core.processor_checkpoints")
            .columns(&["COUNT(*) as count"])
            .where_eq("processor_name", QueryParam::String(processor_name))
    }

    /// Get active checkpoints (updated within last hour)
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<CheckpointRecord>(pool)`
    pub fn get_active_checkpoints() -> QueryBuilder {
        let one_hour_ago = Utc::now() - chrono::Duration::hours(1);

        QueryBuilder::select("core.processor_checkpoints")
            .columns(&[
                "id::uuid as \"id!\"",
                "processor_name as \"processor_name!\"",
                "consumer_group as \"consumer_group!\"",
                "consumer_name as \"consumer_name!\"",
                "last_processed_id",
                "processed_count as \"processed_count!\"",
                "last_activity as \"last_activity!\"",
                "state_data",
                "checkpoint_version as \"checkpoint_version!\"",
                "checkpoint_data",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op("updated_at", ">=", QueryParam::Timestamp(one_hour_ago))
            .order_by("updated_at", "DESC")
    }

    /// Update checkpoint processed count
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_processed_count(
        processor_name: String,
        consumer_group: String,
        consumer_name: String,
        processed_count: i64,
    ) -> QueryBuilder {
        QueryBuilder::update("core.processor_checkpoints")
            .set("processed_count", QueryParam::Integer(processed_count))
            .set("updated_at", QueryParam::Timestamp(Utc::now()))
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("consumer_group", QueryParam::String(consumer_group))
            .where_eq("consumer_name", QueryParam::String(consumer_name))
    }

    /// Update checkpoint last activity
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_last_activity(
        processor_name: String,
        consumer_group: String,
        consumer_name: String,
        last_activity: DateTime<Utc>,
    ) -> QueryBuilder {
        QueryBuilder::update("core.processor_checkpoints")
            .set("last_activity", QueryParam::Timestamp(last_activity))
            .set("updated_at", QueryParam::Timestamp(Utc::now()))
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("consumer_group", QueryParam::String(consumer_group))
            .where_eq("consumer_name", QueryParam::String(consumer_name))
    }

    /// Upsert checkpoint with ON CONFLICT handling (raw SQL implementation)
    ///
    /// This is a specialized method for complex upsert operations that the QueryBuilder
    /// doesn't yet support. It uses raw SQL with proper parameter binding.
    pub async fn upsert_checkpoint_with_conflict(
        pool: &PgPool,
        id: Ulid,
        processor_name: String,
        consumer_group: String,
        consumer_name: String,
        last_processed_id: Option<Ulid>,
        processed_count: i64,
        last_activity: DateTime<Utc>,
        state_data: Option<JsonValue>,
        checkpoint_version: i32,
        checkpoint_data: Option<JsonValue>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> DbResult<sqlx::postgres::PgQueryResult> {
        let result = sqlx::query!(
            r#"
            INSERT INTO core.processor_checkpoints (
                id,
                processor_name,
                consumer_group,
                consumer_name,
                last_processed_id,
                processed_count,
                last_activity,
                state_data,
                checkpoint_version,
                checkpoint_data,
                created_at,
                updated_at
            ) VALUES (
                $1::uuid, $2, $3, $4, $5::uuid, $6, $7, $8, $9, $10, $11, $12
            )
            ON CONFLICT (processor_name, consumer_group, consumer_name) 
            DO UPDATE SET
                last_processed_id = EXCLUDED.last_processed_id,
                processed_count = EXCLUDED.processed_count,
                last_activity = EXCLUDED.last_activity,
                state_data = EXCLUDED.state_data,
                checkpoint_version = EXCLUDED.checkpoint_version,
                checkpoint_data = EXCLUDED.checkpoint_data,
                updated_at = EXCLUDED.updated_at
            "#,
            crate::query_helpers::ulid_to_uuid(id),
            processor_name,
            consumer_group,
            consumer_name,
            last_processed_id.map(|id| crate::query_helpers::ulid_to_uuid(id)),
            processed_count,
            last_activity,
            state_data,
            checkpoint_version,
            checkpoint_data,
            created_at,
            updated_at
        )
        .execute(pool)
        .await
        .map_err(|e| db_error(e, "upsert checkpoint with conflict"))?;

        Ok(result)
    }
}
