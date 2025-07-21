//! Query Builder Usage Examples
//! 
//! This file demonstrates proper usage of the QueryBuilder abstraction
//! instead of raw SQL queries.

use sinex_db::{queries::*, query_builder::QueryBuilder, QueryParam};
use sinex_events::RawEvent;
use sinex_error::{CoreError, ErrorContext};
use sqlx::PgPool;
use sinex_ulid::Ulid;

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
        .context(CoreError::NotFound {
            entity: "event".to_string(),
        })
}

/// Example 2: INSERT with multiple parameters
async fn insert_event(pool: &PgPool, event: &Event) -> Result<(), CoreError> {
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
    QueryBuilder::new("INSERT INTO core.events")
        .columns(&["id", "ts_orig", "source", "event_type", "payload"])
        .values(&[
            QueryParam::Ulid(event.id),
            QueryParam::DateTime(event.ts_orig),
            QueryParam::Text(event.source.clone()),
            QueryParam::Text(event.event_type.clone()),
            QueryParam::Json(event.payload.clone()),
        ])
        .execute(pool)
        .await
        .context(CoreError::Database {
            operation: "insert_event".to_string(),
        })?;

    Ok(())
}

/// Example 3: Complex query with filters
async fn find_events_by_type_and_source(
    pool: &PgPool,
    event_type: &str,
    source: &str,
    limit: i64,
) -> Result<Vec<Event>, CoreError> {
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
    QueryBuilder::new("SELECT * FROM core.events")
        .where_clause("event_type = $1", vec![QueryParam::Text(event_type.to_string())])
        .and_where("source = $2", vec![QueryParam::Text(source.to_string())])
        .order_by("ts_orig DESC")
        .limit(limit)
        .fetch_all::<Event>(pool)
        .await
        .context(CoreError::Database {
            operation: "find_events_by_type_and_source".to_string(),
        })
}

/// Example 4: Using domain-specific query modules
async fn get_latest_checkpoint(
    pool: &PgPool,
    automaton_name: &str,
) -> Result<Option<Ulid>, CoreError> {
    // ❌ WRONG: Direct table access
    // let result = sqlx::query!(
    //     r#"
    //     SELECT last_processed_id
    //     FROM core.automaton_checkpoints
    //     WHERE automaton_name = $1
    //     "#,
    //     automaton_name
    // )
    // .fetch_optional(pool)
    // .await?;

    // ✅ CORRECT: Using dedicated query module
    CheckpointQueries::get_last_processed_id(automaton_name)
        .fetch_optional(pool)
        .await
        .context(CoreError::Database {
            operation: "get_latest_checkpoint".to_string(),
        })
}

/// Example 5: Batch operations
async fn insert_events_batch(pool: &PgPool, events: &[Event]) -> Result<(), CoreError> {
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

    // ✅ CORRECT: Using batch insert builder
    let mut builder = QueryBuilder::new("INSERT INTO core.events")
        .columns(&["id", "ts_orig", "source", "event_type", "payload"]);

    for event in events {
        builder = builder.add_values(&[
            QueryParam::Ulid(event.id),
            QueryParam::DateTime(event.ts_orig),
            QueryParam::Text(event.source.clone()),
            QueryParam::Text(event.event_type.clone()),
            QueryParam::Json(event.payload.clone()),
        ]);
    }

    builder
        .execute(pool)
        .await
        .context(CoreError::Database {
            operation: "insert_events_batch".to_string(),
        })?;

    Ok(())
}

/// Example 6: Transaction with multiple operations
async fn process_event_with_checkpoint(
    pool: &PgPool,
    event: &Event,
    automaton_name: &str,
) -> Result<(), CoreError> {
    // ✅ CORRECT: Using transaction with query builders
    let mut tx = pool.begin().await.context(CoreError::Database {
        operation: "begin_transaction".to_string(),
    })?;

    // Insert event
    EventQueries::insert(event)
        .execute(&mut *tx)
        .await
        .context(CoreError::Database {
            operation: "insert_event_in_tx".to_string(),
        })?;

    // Update checkpoint
    CheckpointQueries::update_checkpoint(automaton_name, event.id)
        .execute(&mut *tx)
        .await
        .context(CoreError::Database {
            operation: "update_checkpoint_in_tx".to_string(),
        })?;

    tx.commit().await.context(CoreError::Database {
        operation: "commit_transaction".to_string(),
    })?;

    Ok(())
}

/// Example 7: Dynamic query building
async fn search_events(
    pool: &PgPool,
    filters: EventSearchFilters,
) -> Result<Vec<Event>, CoreError> {
    // ✅ CORRECT: Building query dynamically based on filters
    let mut query = QueryBuilder::new("SELECT * FROM core.events");
    let mut param_count = 0;

    if let Some(source) = filters.source {
        param_count += 1;
        query = query.where_clause(
            &format!("source = ${}", param_count),
            vec![QueryParam::Text(source)],
        );
    }

    if let Some(event_type) = filters.event_type {
        param_count += 1;
        let clause = if param_count == 1 {
            format!("event_type = ${}", param_count)
        } else {
            format!("event_type = ${}", param_count)
        };
        query = query.and_where(&clause, vec![QueryParam::Text(event_type)]);
    }

    if let Some(after) = filters.after {
        param_count += 1;
        query = query.and_where(
            &format!("ts_orig > ${}", param_count),
            vec![QueryParam::DateTime(after)],
        );
    }

    query
        .order_by("ts_orig DESC")
        .limit(filters.limit.unwrap_or(100))
        .fetch_all::<Event>(pool)
        .await
        .context(CoreError::Database {
            operation: "search_events".to_string(),
        })
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
    QueryBuilder::new("SELECT source, COUNT(*) as count FROM core.events")
        .where_clause("ts_orig > $1", vec![QueryParam::DateTime(since)])
        .group_by("source")
        .order_by("count DESC")
        .fetch_all::<(String, i64)>(pool)
        .await
        .context(CoreError::Database {
            operation: "get_event_counts_by_source".to_string(),
        })
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
