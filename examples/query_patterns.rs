//! Query Builder Usage Examples
//!
//! This file demonstrates proper usage of the QueryBuilder abstraction
//! instead of raw SQL queries.

use sinex_db::{queries::*, query_builder::QueryBuilder, QueryParam};
use sinex_error::{CoreError, ResultExt};
use sinex_events::RawEvent;
use sinex_ulid::Ulid;
use sqlx::PgPool;

/// Example 1: Simple SELECT query
async fn get_event_by_id(pool: &PgPool, event_id: Ulid) -> Result<RawEvent, CoreError> {
    // ❌ WRONG: Using raw SQL
    // let event = sqlx::query_as!(
    //     Event,
    //     "SELECT * FROM core.events WHERE id = $1",
    //     event_id.to_uuid()
    // )
    // .fetch_one(pool)
    // .await?;

    // ✅ CORRECT: Using QueryBuilder
    EventQueries::get_by_id(event_id)
        .fetch_one(pool)
        .await
        .map_err(|e| CoreError::not_found(format!("event not found: {}", e)))
}

/// Example 2: INSERT with multiple parameters
async fn insert_event(pool: &PgPool, event: &RawEvent) -> Result<(), CoreError> {
    // ❌ WRONG: Raw SQL insert
    // sqlx::query!(
    //     r#"
    //     INSERT INTO core.events (id, ts_orig, source, event_type, payload)
    //     VALUES ($1, $2, $3, $4, $5)
    //     "#,
    //     event.id.to_uuid(),
    //     event.ts_orig,
    //     event.source,
    //     event.event_type,
    //     event.payload
    // )
    // .execute(pool)
    // .await?;

    // ✅ CORRECT: Using QueryBuilder
    QueryBuilder::insert("core.events")
        .columns(&["id", "ts_orig", "source", "event_type", "payload"])
        .values(&[
            QueryParam::Ulid(event.id),
            QueryParam::OptionalTimestamp(event.ts_orig),
            QueryParam::String(event.source.clone()),
            QueryParam::String(event.event_type.clone()),
            QueryParam::Json(event.payload.clone()),
        ])
        .execute(pool)
        .await
        .map_err(|e| CoreError::database(format!("insert_event: {}", e)))?;

    Ok(())
}

/// Example 3: Complex query with filters
async fn find_events_by_type_and_source(
    pool: &PgPool,
    event_type: &str,
    source: &str,
    limit: i64,
) -> Result<Vec<RawEvent>, CoreError> {
    // ❌ WRONG: Building query strings manually
    // let events = sqlx::query_as!(
    //     Event,
    //     r#"
    //     SELECT * FROM core.events
    //     WHERE event_type = $1 AND source = $2
    //     ORDER BY ts_orig DESC
    //     LIMIT $3
    //     "#,
    //     event_type,
    //     source,
    //     limit
    // )
    // .fetch_all(pool)
    // .await?;

    // ✅ CORRECT: Using QueryBuilder with fluent API
    QueryBuilder::select("core.events")
        .where_eq("event_type", QueryParam::String(event_type.to_string()))
        .where_eq("source", QueryParam::String(source.to_string()))
        .order_by("ts_orig", "DESC")
        .limit(limit)
        .fetch_all::<RawEvent>(pool)
        .await
        .map_err(|e| CoreError::database(format!("find_events_by_type_and_source: {}", e)))
}

/// Example 4: Using domain-specific query modules
async fn get_latest_checkpoint(
    pool: &PgPool,
    processor_name: &str,
) -> Result<Option<Ulid>, CoreError> {
    // ❌ WRONG: Direct table access
    // let result = sqlx::query!(
    //     r#"
    //     SELECT last_processed_id
    //     FROM core.processor_checkpoints
    //     WHERE processor_name = $1
    //     "#,
    //     processor_name
    // )
    // .fetch_optional(pool)
    // .await?;

    // ✅ CORRECT: Using dedicated query module
    // Note: This is a simplified example - actual checkpoint queries would use the proper API
    QueryBuilder::select("core.processor_checkpoints")
        .columns(&["last_processed_id"])
        .where_eq(
            "processor_name",
            QueryParam::String(processor_name.to_string()),
        )
        .fetch_optional::<(Option<Ulid>,)>(pool)
        .await
        .map(|row| row.map(|r| r.0).flatten())
        .map_err(|e| CoreError::database(format!("get_latest_checkpoint: {}", e)))
}

/// Example 5: Batch operations
async fn insert_events_batch(pool: &PgPool, events: &[RawEvent]) -> Result<(), CoreError> {
    // ❌ WRONG: Loop with individual queries
    // for event in events {
    //     sqlx::query!(
    //         "INSERT INTO core.events (id, ts_orig, source, event_type, payload) VALUES ($1, $2, $3, $4, $5)",
    //         event.id.to_uuid(),
    //         event.ts_orig,
    //         event.source,
    //         event.event_type,
    //         event.payload
    //     )
    //     .execute(pool)
    //     .await?;
    // }

    // ✅ CORRECT: Using individual inserts (transaction support requires raw SQL currently)
    // Note: For true batch inserts with transactions, use raw SQL or wait for
    // QueryBuilder transaction support
    for event in events {
        QueryBuilder::insert("core.events")
            .columns(&["id", "ts_orig", "source", "event_type", "payload"])
            .values(&[
                QueryParam::Ulid(event.id),
                QueryParam::OptionalTimestamp(event.ts_orig),
                QueryParam::String(event.source.clone()),
                QueryParam::String(event.event_type.clone()),
                QueryParam::Json(event.payload.clone()),
            ])
            .execute(pool)
            .await
            .map_err(|e| CoreError::database(format!("insert_event_in_batch: {}", e)))?;
    }

    Ok(())
}

/// Example 6: Transaction with multiple operations
async fn process_event_with_checkpoint(
    pool: &PgPool,
    event: &RawEvent,
    processor_name: &str,
) -> Result<(), CoreError> {
    // ✅ CORRECT: For transaction-based operations, use raw SQL or separate operations
    // Note: QueryBuilder currently doesn't support transactions directly

    // Insert event
    QueryBuilder::insert("core.events")
        .columns(&["id", "ts_orig", "source", "event_type", "payload"])
        .values(&[
            QueryParam::Ulid(event.id),
            QueryParam::OptionalTimestamp(event.ts_orig),
            QueryParam::String(event.source.clone()),
            QueryParam::String(event.event_type.clone()),
            QueryParam::Json(event.payload.clone()),
        ])
        .execute(pool)
        .await
        .map_err(|e| CoreError::database(format!("insert_event: {}", e)))?;

    // Update checkpoint
    QueryBuilder::update("core.processor_checkpoints")
        .set("last_processed_id", QueryParam::Ulid(event.id))
        .set("last_activity", QueryParam::Timestamp(chrono::Utc::now()))
        .where_eq(
            "processor_name",
            QueryParam::String(processor_name.to_string()),
        )
        .execute(pool)
        .await
        .map_err(|e| CoreError::database(format!("update_checkpoint: {}", e)))?;

    Ok(())
}

/// Example 7: Dynamic query building
async fn search_events(
    pool: &PgPool,
    filters: EventSearchFilters,
) -> Result<Vec<RawEvent>, CoreError> {
    // ✅ CORRECT: Building query dynamically based on filters
    let mut query = QueryBuilder::select("core.events");
    let mut param_count = 0;

    if let Some(source) = filters.source {
        query = query.where_eq("source", QueryParam::String(source));
    }

    if let Some(event_type) = filters.event_type {
        query = query.where_eq("event_type", QueryParam::String(event_type));
    }

    if let Some(after) = filters.after {
        query = query.where_op("ts_orig", ">", QueryParam::Timestamp(after));
    }

    query
        .order_by("ts_orig", "DESC")
        .limit(filters.limit.unwrap_or(100))
        .fetch_all::<RawEvent>(pool)
        .await
        .map_err(|e| CoreError::database(format!("search_events: {}", e)))
}

#[derive(Default)]
struct EventSearchFilters {
    source: Option<String>,
    event_type: Option<String>,
    after: Option<chrono::DateTime<chrono::Utc>>,
    limit: Option<i64>,
}

/// Example 8: Aggregation queries
async fn get_event_counts_by_source(
    pool: &PgPool,
    since: chrono::DateTime<chrono::Utc>,
) -> Result<Vec<(String, i64)>, CoreError> {
    // ✅ CORRECT: Using query builder for aggregations
    QueryBuilder::select("core.events")
        .columns(&["source", "COUNT(*) as count"])
        .where_op("ts_orig", ">", QueryParam::Timestamp(since))
        .group_by("source")
        .order_by("count", "DESC")
        .fetch_all::<(String, i64)>(pool)
        .await
        .map_err(|e| CoreError::database(format!("get_event_counts_by_source: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test helpers would go here
    // Note: Actual tests should use the test abstractions from test/common/
}

fn main() {
    println!("This is an example file demonstrating query patterns.");
    println!("See the individual functions for usage examples.");
}
