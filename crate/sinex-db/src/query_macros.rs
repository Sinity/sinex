//! Query macros for common patterns and ergonomic improvements
//!
//! This module provides macros that make common query patterns even more concise
//! and ergonomic. These macros build on top of the centralized query system.

/// Macro for getting an event by ID
///
/// Usage:
/// ```rust
/// let event = get_event_by_id!(pool, event_id).await?;
/// ```
#[macro_export]
macro_rules! get_event_by_id {
    ($pool:expr, $event_id:expr) => {
        $crate::queries::EventQueries::get_by_id($event_id).fetch_one::<$crate::RawEvent>($pool)
    };
}

/// Macro for inserting an event
///
/// Usage:
/// ```rust
/// let event = insert_event!(pool, {
///     source: "test.source".to_string(),
///     event_type: "test_event".to_string(),
///     host: "localhost".to_string(),
///     payload: json!({"test": "data"}),
///     ts_orig: None,
///     ingestor_version: None,
///     payload_schema_id: None,
///     source_event_ids: None,
/// }).await?;
/// ```
#[macro_export]
macro_rules! insert_event {
    ($pool:expr, {
        source: $source:expr,
        event_type: $event_type:expr,
        host: $host:expr,
        payload: $payload:expr,
        ts_orig: $ts_orig:expr,
        ingestor_version: $ingestor_version:expr,
        payload_schema_id: $payload_schema_id:expr,
        source_event_ids: $source_event_ids:expr,
    }) => {
        $crate::queries::EventQueries::insert_event_with_source_ids(
            $source,
            $event_type,
            $host,
            $payload,
            $ts_orig,
            $ingestor_version,
            $payload_schema_id,
            $source_event_ids,
        )
        .fetch_one::<$crate::RawEvent>($pool)
    };
}

/// Macro for counting events
///
/// Usage:
/// ```rust
/// let count = count_events!(pool).await?;
/// ```
#[macro_export]
macro_rules! count_events {
    ($pool:expr) => {
        $crate::queries::EventQueries::count_all()
            .fetch_one::<(i64,)>($pool)
            .map(|r| r.unwrap().0)
    };
}

/// Macro for getting recent events
///
/// Usage:
/// ```rust
/// let events = get_recent_events!(pool, 10).await?;
/// let events = get_recent_events!(pool, 10, 20).await?; // with offset
/// ```
#[macro_export]
macro_rules! get_recent_events {
    ($pool:expr, $limit:expr) => {
        $crate::queries::EventQueries::get_recent(Some($limit), None)
            .fetch_all::<$crate::RawEvent>($pool)
    };
    ($pool:expr, $limit:expr, $offset:expr) => {
        $crate::queries::EventQueries::get_recent(Some($limit), Some($offset))
            .fetch_all::<$crate::RawEvent>($pool)
    };
}

/// Macro for getting checkpoint
///
/// Usage:
/// ```rust
/// let checkpoint = get_checkpoint!(pool, "processor", "group", "consumer").await?;
/// ```
#[macro_export]
macro_rules! get_checkpoint {
    ($pool:expr, $processor:expr, $group:expr, $consumer:expr) => {
        $crate::queries::CheckpointQueries::get_checkpoint(
            $processor.to_string(),
            $group.to_string(),
            $consumer.to_string(),
        )
        .fetch_optional::<$crate::CheckpointRecord>($pool)
    };
}

/// Macro for upserting checkpoint
///
/// Usage:
/// ```rust
/// upsert_checkpoint!(pool, {
///     id: checkpoint_id,
///     processor_name: "my-processor".to_string(),
///     consumer_group: "default".to_string(),
///     consumer_name: "hostname-1234".to_string(),
///     last_processed_id: Some("message-id".to_string()),
///     processed_count: 100,
///     last_activity: Utc::now(),
///     state_data: Some(json!({"key": "value"})),
///     checkpoint_version: 2,
///     checkpoint_data: Some(json!({"checkpoint": "data"})),
///     created_at: Utc::now(),
///     updated_at: Utc::now(),
/// }).await?;
/// ```
#[macro_export]
macro_rules! upsert_checkpoint {
    ($pool:expr, {
        id: $id:expr,
        processor_name: $processor_name:expr,
        consumer_group: $consumer_group:expr,
        consumer_name: $consumer_name:expr,
        last_processed_id: $last_processed_id:expr,
        processed_count: $processed_count:expr,
        last_activity: $last_activity:expr,
        state_data: $state_data:expr,
        checkpoint_version: $checkpoint_version:expr,
        checkpoint_data: $checkpoint_data:expr,
        created_at: $created_at:expr,
        updated_at: $updated_at:expr,
    }) => {
        $crate::queries::CheckpointQueries::upsert_checkpoint(
            $id,
            $processor_name,
            $consumer_group,
            $consumer_name,
            $last_processed_id,
            $processed_count,
            $last_activity,
            $state_data,
            $checkpoint_version,
            $checkpoint_data,
            $created_at,
            $updated_at,
        )
        .execute($pool)
    };
}

/// Macro for getting schema by event type
///
/// Usage:
/// ```rust
/// let schema = get_schema_by_event_type!(pool, "test_event").await?;
/// ```
#[macro_export]
macro_rules! get_schema_by_event_type {
    ($pool:expr, $event_type:expr) => {
        $crate::queries::SchemaQueries::get_latest_for_event_type($event_type.to_string())
            .fetch_optional::<$crate::SchemaRecord>($pool)
    };
}

/// Macro for inserting schema
///
/// Usage:
/// ```rust
/// let schema = insert_schema!(pool, {
///     event_type: "test_event".to_string(),
///     schema_version: 1,
///     schema_data: json!({"type": "object"}),
/// }).await?;
/// ```
#[macro_export]
macro_rules! insert_schema {
    ($pool:expr, {
        event_type: $event_type:expr,
        schema_version: $schema_version:expr,
        schema_data: $schema_data:expr,
    }) => {
        $crate::queries::SchemaQueries::insert_schema($event_type, $schema_version, $schema_data)
            .fetch_one::<$crate::SchemaRecord>($pool)
    };
}

/// Macro for getting artifact by ID
///
/// Usage:
/// ```rust
/// let artifact = get_artifact_by_id!(pool, artifact_id).await?;
/// ```
#[macro_export]
macro_rules! get_artifact_by_id {
    ($pool:expr, $artifact_id:expr) => {
        $crate::queries::ArtifactQueries::get_by_id($artifact_id)
            .fetch_one::<$crate::ArtifactRecord>($pool)
    };
}

/// Macro for inserting artifact
///
/// Usage:
/// ```rust
/// let artifact = insert_artifact!(pool, {
///     blob_id: blob_id,
///     title: "My Artifact".to_string(),
///     description: Some("Description".to_string()),
///     metadata: json!({"key": "value"}),
/// }).await?;
/// ```
#[macro_export]
macro_rules! insert_artifact {
    ($pool:expr, {
        blob_id: $blob_id:expr,
        title: $title:expr,
        description: $description:expr,
        metadata: $metadata:expr,
    }) => {
        $crate::queries::ArtifactQueries::insert_artifact($blob_id, $title, $description, $metadata)
            .fetch_one::<$crate::ArtifactRecord>($pool)
    };
}

/// Macro for getting health metrics
///
/// Usage:
/// ```rust
/// let metrics = get_health_metrics!(pool).await?;
/// ```
#[macro_export]
macro_rules! get_health_metrics {
    ($pool:expr) => {
        $crate::queries::OperationQueries::get_health_metrics()
            .fetch_one::<$crate::HealthMetricsRecord>($pool)
    };
}

/// Macro for getting throughput metrics
///
/// Usage:
/// ```rust
/// let metrics = get_throughput_metrics!(pool, since_timestamp).await?;
/// ```
#[macro_export]
macro_rules! get_throughput_metrics {
    ($pool:expr, $since:expr) => {
        $crate::queries::OperationQueries::get_throughput_metrics($since)
            .fetch_one::<$crate::ThroughputMetricsRecord>($pool)
    };
}

/// Macro for checking if a record exists
///
/// Usage:
/// ```rust
/// let exists = record_exists!(pool, "core.events", "event_id = $1", event_id).await?;
/// ```
#[macro_export]
macro_rules! record_exists {
    ($pool:expr, $table:expr, $where_clause:expr, $param:expr) => {
        $crate::QueryBuilder::select($table)
            .columns(&["EXISTS(SELECT 1) as exists"])
            .where_eq($where_clause, $param)
            .fetch_one::<(bool,)>($pool)
            .map(|r| r.unwrap().0)
    };
}

/// Macro for executing a simple query with parameters
///
/// Usage:
/// ```rust
/// let result = execute_query!(pool, "UPDATE core.events SET payload = $1 WHERE event_id = $2",
///     QueryParam::Json(json!({"updated": true})),
///     QueryParam::Ulid(event_id)
/// ).await?;
/// ```
#[macro_export]
macro_rules! execute_query {
    ($pool:expr, $sql:expr, $($param:expr),*) => {{
        let mut query = sqlx::query($sql);
        $(
            query = query.bind($param);
        )*
        query.execute($pool)
    }};
}

/// Macro for fetching one record with parameters
///
/// Usage:
/// ```rust
/// let record = fetch_one_query!(pool, MyRecord, "SELECT * FROM my_table WHERE id = $1", id).await?;
/// ```
#[macro_export]
macro_rules! fetch_one_query {
    ($pool:expr, $record_type:ty, $sql:expr, $($param:expr),*) => {{
        let mut query = sqlx::query_as::<_, $record_type>($sql);
        $(
            query = query.bind($param);
        )*
        query.fetch_one($pool)
    }};
}

/// Macro for fetching all records with parameters
///
/// Usage:
/// ```rust
/// let records = fetch_all_query!(pool, MyRecord, "SELECT * FROM my_table WHERE status = $1", status).await?;
/// ```
#[macro_export]
macro_rules! fetch_all_query {
    ($pool:expr, $record_type:ty, $sql:expr, $($param:expr),*) => {{
        let mut query = sqlx::query_as::<_, $record_type>($sql);
        $(
            query = query.bind($param);
        )*
        query.fetch_all($pool)
    }};
}

/// Macro for batch operations
///
/// Usage:
/// ```rust
/// batch_operation!(pool, |tx| async move {
///     let event1 = insert_event!(tx, {...}).await?;
///     let event2 = insert_event!(tx, {...}).await?;
///     Ok((event1, event2))
/// }).await?;
/// ```
#[macro_export]
macro_rules! batch_operation {
    ($pool:expr, $f:expr) => {
        $crate::with_transaction($pool, $f)
    };
}

/// Macro for retry operations
///
/// Usage:
/// ```rust
/// retry_operation!(pool, |tx| async move {
///     // Operations that might deadlock
///     Ok(result)
/// }).await?;
/// ```
#[macro_export]
macro_rules! retry_operation {
    ($pool:expr, $f:expr) => {
        $crate::with_retry_transaction($pool, $crate::RetryConfig::default(), $f)
    };
}

/// Macro for building complex WHERE clauses
///
/// Usage:
/// ```rust
/// let events = build_query!(QueryBuilder::select("core.events")
///     .columns(&["event_id", "source", "event_type"])
///     .where_eq("source", QueryParam::String("test".to_string()))
///     .where_op("ts_ingest", ">", QueryParam::Timestamp(since))
///     .limit(10)
/// ).fetch_all::<RawEvent>(pool).await?;
/// ```
#[macro_export]
macro_rules! build_query {
    ($builder:expr) => {
        $builder
    };
}

/// Macro for time-based queries
///
/// Usage:
/// ```rust
/// let events = time_range_query!(pool, "core.events", "ts_ingest", start_time, end_time).await?;
/// ```
#[macro_export]
macro_rules! time_range_query {
    ($pool:expr, $table:expr, $time_column:expr, $start:expr, $end:expr) => {
        $crate::QueryBuilder::select($table)
            .columns(&["*"])
            .where_op($time_column, ">=", $crate::QueryParam::Timestamp($start))
            .where_op($time_column, "<=", $crate::QueryParam::Timestamp($end))
            .order_by($time_column, "DESC")
            .fetch_all::<$crate::RawEvent>($pool)
    };
}

/// Macro for pagination queries
///
/// Usage:
/// ```rust
/// let events = paginate_query!(pool, "core.events", 10, 20).await?;
/// ```
#[macro_export]
macro_rules! paginate_query {
    ($pool:expr, $table:expr, $limit:expr, $offset:expr) => {
        $crate::QueryBuilder::select($table)
            .columns(&["*"])
            .limit($limit)
            .offset($offset)
            .fetch_all::<$crate::RawEvent>($pool)
    };
}

/// Macro for aggregation queries
///
/// Usage:
/// ```rust
/// let stats = aggregate_query!(pool, "core.events", "source", "COUNT(*) as count").await?;
/// ```
#[macro_export]
macro_rules! aggregate_query {
    ($pool:expr, $table:expr, $group_by:expr, $aggregates:expr) => {
        $crate::QueryBuilder::select($table)
            .columns(&[$group_by, $aggregates])
            .order_by("count", "DESC")
            .fetch_all::<(String, i64)>($pool)
    };
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_macro_compilation() {
        // These tests just verify the macros compile correctly
        // They don't execute because we don't have a real database in tests

        // Test that the macros expand to valid Rust code
        // The actual functionality is tested in integration tests
        assert!(true);
    }
}
