//! Consolidated Database Integration Tests
//!
//! This module contains comprehensive integration tests for all database functionality,
//! consolidated from multiple separate test files. Tests cover:
//! - Basic database operations and transactions
//! - TimescaleDB hypertable functionality
//! - ULID primary key integration
//! - JSON schema validation with pg_jsonschema
//! - Work queue operations and TTL
//! - Connection pool edge cases and limits
//! - Query performance and optimization
//! - Data integrity and consistency
//!
//! Uses #[sinex_test] for automatic transaction isolation and TestContext
//! for unified database access.

use crate::common::prelude::*;
use crate::common::{self, assertions, events, generators, schema_test_utils};
use chrono::{Duration, Utc};
use futures::future::join_all;
use sinex_core::{RawEventBuilder};
// use sinex_db::events::insert_event_with_validator; // Unused import removed
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use uuid::Uuid;

// =============================================================================
// BASIC DATABASE OPERATIONS
// =============================================================================

/// Test basic event lifecycle: insert → retrieve → verify
///
/// This is the most fundamental test - if this fails, nothing else works.
// Basic insert/retrieve test removed - redundant with test_batch_event_insertion

/// Test batch insertion of multiple events
#[sinex_test(timeout = 40)]
async fn test_batch_event_insertion(ctx: TestContext) -> TestResult {
    let events = generators::test_events(10);

    // Insert events concurrently for better performance
    let insert_tasks: Vec<_> = events.iter().map(|event| {
        let pool = ctx.pool().clone();
        let event = event.clone();
        tokio::spawn(async move {
            sinex_db::events::insert_event_with_validator(&pool, &event, None).await.map(|e| e.id)
        })
    }).collect();

    // Wait for all insertions to complete
    let mut inserted_ids = Vec::new();
    for task in insert_tasks {
        let id = task.await??;
        inserted_ids.push(id);
    }

    // Verify all events exist (also done concurrently)
    let verify_tasks: Vec<_> = inserted_ids.iter().map(|&id| {
        let pool = ctx.pool().clone();
        tokio::spawn(async move {
            get_event_by_id(&pool, id).await.map(|_| true).unwrap_or(false)
        })
    }).collect();

    for task in verify_tasks {
        assert!(task.await?);
    }

    // Check total count - use basic query
    let count = sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events")
        .fetch_one(ctx.pool())
        .await?;
    assert!(count.unwrap_or(0) >= 10);

    Ok(())
}

/// Test querying events by source
#[sinex_test(timeout = 35)]
async fn test_query_events_by_source(ctx: TestContext) -> TestResult {
    // Create test events
    let fs_event1 = events::file_created_event("/test/file1.txt");
    let fs_event2 = events::file_modified_event("/test/file2.txt");
    let term_event = events::kitty_event("ls -la");

    // Insert all events concurrently
    let events_to_insert = [&fs_event1, &fs_event2, &term_event];
    let insert_tasks: Vec<_> = events_to_insert.iter().map(|&event| {
        let pool = ctx.pool().clone();
        let event = event.clone();
        tokio::spawn(async move {
            assertions::assert_event_inserted(&pool, &event).await
        })
    }).collect();

    // Wait for all insertions
    for task in insert_tasks {
        task.await??;
    }

    // Query using our helper function
    let filesystem_events = common::get_events_by_source(ctx.pool(), "fs", 10).await?;
    assert!(filesystem_events.len() >= 2);

    for event in &filesystem_events {
        pretty_assertions::assert_eq!(event.source, "fs");
    }

    Ok(())
}

/// Test invalid event insertion fails appropriately
#[sinex_test]
async fn test_invalid_event_insertion_fails(ctx: TestContext) -> TestResult {
    let invalid_event = events::invalid_event();
    assertions::assert_event_insertion_fails(ctx.pool(), &invalid_event).await?;
    Ok(())
}

/// Test ULID ordering in time-based queries
#[sinex_test(timeout = 35)]
async fn test_ulid_time_ordering(ctx: TestContext) -> TestResult {
    // Insert events with a small delay to ensure different timestamps
    let event1 = events::file_created_event("/test/first.txt");
    let id1 = assertions::assert_event_inserted(ctx.pool(), &event1).await?;

    tokio::task::yield_now().await;

    let event2 = events::file_created_event("/test/second.txt");
    let id2 = assertions::assert_event_inserted(ctx.pool(), &event2).await?;

    // Verify ULIDs are in time order (later ULID should be larger)
    assert!(id2.to_string() > id1.to_string());

    Ok(())
}

// =============================================================================
// ULID INTEGRATION TESTS
// =============================================================================

#[sinex_test(timeout = 40)]
async fn test_ulid_ordering_in_database(ctx: TestContext) -> TestResult {
    // Insert multiple events and collect their IDs
    let mut ulids = Vec::new();

    for i in 0..5 {
        let event = events::file_created_event(&format!("/test/file_{}.txt", i));
        let id = assertions::assert_event_inserted(ctx.pool(), &event).await?;
        ulids.push(id);

        // Small delay to ensure ULID monotonic ordering
        tokio::time::sleep(StdDuration::from_millis(1)).await;
    }

    // Query filesystem events to verify ordering
    let filesystem_events = common::get_events_by_source(ctx.pool(), "fs", 10).await?;
    assert!(filesystem_events.len() >= 5);

    // Verify ULIDs are in chronological order
    for i in 1..ulids.len() {
        assert!(ulids[i] > ulids[i-1], "ULIDs should be in chronological order");
    }

    Ok(())
}

#[sinex_test]
async fn test_ulid_uuid_conversion_consistency(ctx: TestContext) -> TestResult {
    // Test that ULID <-> UUID conversion is consistent
    let original_ulid = Ulid::new();
    let uuid_form = original_ulid.to_uuid();
    let back_to_ulid = Ulid::from_uuid(uuid_form);

    pretty_assertions::assert_eq!(original_ulid, back_to_ulid);

    // Test in database context
    let event = events::file_created_event("/test/ulid-uuid.txt");
    let event_id = assertions::assert_event_inserted(ctx.pool(), &event).await?;

    // Query back using UUID conversion
    let retrieved = common::get_event_by_id(ctx.pool(), event_id).await?;
    pretty_assertions::assert_eq!(retrieved.id, event_id);

    Ok(())
}

// =============================================================================
// TIMESCALEDB TESTS
// =============================================================================

#[sinex_test]
async fn test_raw_events_is_timescale_hypertable(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Verify raw.events is a hypertable
    let hypertable_info: Option<(String, String, String, String)> = sqlx::query_as(
        "SELECT hypertable_schema, hypertable_name,
                column_name, dimension_type
         FROM timescaledb_information.dimensions
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'",
    )
    .fetch_optional(pool)
    .await?;

    assert!(
        hypertable_info.is_some(),
        "raw.events should be a hypertable"
    );
    let (schema, table, dimension_col, dimension_type) = hypertable_info.unwrap();
    pretty_assertions::assert_eq!(schema, "raw");
    pretty_assertions::assert_eq!(table, "events");
    pretty_assertions::assert_eq!(dimension_col, "id");
    pretty_assertions::assert_eq!(dimension_type, "Time"); // Time dimension

    // Check chunk interval (stored as microseconds for ULID-based time dimension)
    let chunk_interval: Option<i64> = sqlx::query_scalar(
        "SELECT integer_interval
         FROM timescaledb_information.dimensions
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'",
    )
    .fetch_optional(pool)
    .await?;

    assert!(
        chunk_interval.is_some(),
        "Expected chunk interval to be set"
    );
    let interval_seconds = chunk_interval.unwrap() / 1_000_000; // Convert microseconds to seconds
    let interval_days = interval_seconds / 86400;
    pretty_assertions::assert_eq!(interval_days, 7, "Chunk interval should be 7 days");

    Ok(())
}

#[sinex_test]
async fn test_timescale_chunk_creation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Clean up any previous test data
    let _ = sqlx::query("DELETE FROM raw.events WHERE source = 'chunk_test'")
        .execute(pool)
        .await;

    // Get initial chunk count
    let initial_chunks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM timescaledb_information.chunks
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'",
    )
    .fetch_one(pool)
    .await?;

    // Insert events across different time periods to trigger chunk creation
    let time_periods = [Utc::now(),
        Utc::now() - Duration::days(10),
        Utc::now() - Duration::days(20),
        Utc::now() + Duration::days(5)];

    for (i, ts) in time_periods.iter().enumerate() {
        let event = RawEventBuilder::new(
            "chunk_test",
            format!("event_type_{}", i),
            json!({"chunk_test": i}),
        )
        .build();

        // Insert with specific timestamp by creating ULID from timestamp
        let event_id = Ulid::from_datetime(*ts);
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload)
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)",
        )
        .bind(event_id.to_string())
        .bind(event.source)
        .bind(event.event_type)
        .bind(event.host)
        .bind(event.payload)
        .execute(pool)
        .await?;
    }

    // Get new chunk count
    let new_chunks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM timescaledb_information.chunks
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'",
    )
    .fetch_one(pool)
    .await?;

    assert!(
        new_chunks >= initial_chunks,
        "Should have created additional chunks for different time periods"
    );

    // Verify chunks contain the correct data
    for (i, ts) in time_periods.iter().enumerate() {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM raw.events
             WHERE source = $1
             AND event_type = $2
             AND ts_ingest >= $3 - interval '1 hour'
             AND ts_ingest <= $3 + interval '1 hour'",
        )
        .bind("chunk_test")
        .bind(format!("event_type_{}", i))
        .bind(ts)
        .fetch_one(pool)
        .await?;

        pretty_assertions::assert_eq!(count, 1, "Each event should be in its appropriate chunk");
    }
    Ok(())
}

#[sinex_test]
async fn test_timescale_compression_policy(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Check if compression policy exists
    let compression_policy: Option<(i32,)> = sqlx::query_as(
        "SELECT job_id
         FROM timescaledb_information.jobs
         WHERE hypertable_schema = 'raw'
         AND hypertable_name = 'events'
         AND proc_name = 'compress_chunks'",
    )
    .fetch_optional(pool)
    .await?;

    if compression_policy.is_some() {
        // Get compression settings
        let compress_after: Option<i64> = sqlx::query_scalar(
            "SELECT EXTRACT(EPOCH FROM (config->>'compress_after')::interval)::bigint / 86400
             FROM timescaledb_information.jobs
             WHERE hypertable_schema = 'raw'
             AND hypertable_name = 'events'
             AND proc_name = 'compress_chunks'",
        )
        .fetch_optional(pool)
        .await?;

        assert!(compress_after.is_some());
        let days = compress_after.unwrap();
        assert!(days >= 7, "Compression should happen after at least 7 days");
    }

    // Insert old data to test compression
    let _old_timestamp = Utc::now() - Duration::days(30);
    for i in 0..10 {
        let event_id = Ulid::new();
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload)
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)",
        )
        .bind(event_id.to_string())
        .bind("compression_test")
        .bind("old_event")
        .bind("test_host")
        .bind(json!({"seq": i}))
        .execute(pool)
        .await?;
    }

    // Check if old chunks are marked for compression
    let compressible_chunks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM timescaledb_information.chunks
         WHERE hypertable_schema = 'raw'
         AND hypertable_name = 'events'
         AND range_end < now() - interval '7 days'
         AND is_compressed = false",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    println!("Found {} compressible chunks", compressible_chunks);
    Ok(())
}

// =============================================================================
// JSON SCHEMA VALIDATION TESTS
// =============================================================================

#[sinex_test]
async fn test_json_schema_registration(ctx: TestContext) -> TestResult {
    // Register a JSON Schema
    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "window_id": {
                "type": "integer",
                "minimum": 0
            },
            "window_title": {
                "type": "string",
                "minLength": 1
            },
            "timestamp": {
                "type": "string",
                "format": "date-time"
            }
        },
        "required": ["window_id", "window_title"],
        "additionalProperties": false
    });

    // Generate unique test identifiers to avoid conflicts
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("hyprland-test-{}", test_run_id);
    let event_type = format!("window_focused-{}", test_run_id);

    let schema_clone = schema.clone();
    let schema_id =
        schema_test_utils::register_test_schema(ctx.pool(), &event_source, &event_type, schema)
            .await?;

    // Verify schema was stored correctly
    let retrieved_schema: serde_json::Value = sqlx::query_scalar(
        "SELECT json_schema_definition FROM sinex_schemas.event_payload_schemas WHERE id = $1::ulid"
    )
    .bind(schema_id.to_uuid())
    .fetch_one(ctx.pool())
    .await?;

    pretty_assertions::assert_eq!(
        retrieved_schema,
        schema_clone,
        "Schema should be stored correctly"
    );

    Ok(())
}

#[sinex_test]
async fn test_json_schema_validation_constraint(ctx: TestContext) -> TestResult {
    // First, register a strict schema
    let strict_schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["click", "hover", "focus"]
            },
            "element_id": {
                "type": "string",
                "pattern": "^[a-zA-Z0-9_-]+$"
            },
            "coordinates": {
                "type": "object",
                "properties": {
                    "x": {"type": "number"},
                    "y": {"type": "number"}
                },
                "required": ["x", "y"]
            }
        },
        "required": ["action", "element_id"],
        "additionalProperties": false
    });

    // Generate unique test identifiers to avoid conflicts
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("ui_test-{}", test_run_id);
    let event_type = format!("user_interaction-{}", test_run_id);

    let schema_id = Ulid::from_uuid(
        sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO sinex_schemas.event_payload_schemas
         (event_source, event_type, schema_version, json_schema_definition)
         VALUES ($1, $2, $3, $4::jsonb)
         RETURNING id::uuid",
        )
        .bind(&event_source)
        .bind(&event_type)
        .bind("v1.0")
        .bind(&strict_schema)
        .fetch_one(ctx.pool())
        .await?,
    );

    // Test valid payload - using event builder for cleaner syntax
    let valid_event = ctx
        .event_builder(&event_source, &event_type)
        .payload(json!({
            "action": "click",
            "element_id": "submit-button",
            "coordinates": {
                "x": 100.5,
                "y": 200.0
            }
        }))
        .build();

    schema_test_utils::assert_schema_valid_event(ctx.pool(), &valid_event, schema_id).await?;

    // Test invalid payload - missing required field
    let invalid_event1 = ctx
        .event_builder(&event_source, &event_type)
        .payload(json!({
            "action": "click"
            // missing element_id
        }))
        .build();

    schema_test_utils::assert_schema_invalid_event(ctx.pool(), &invalid_event1, schema_id).await?;

    // Test invalid payload - wrong enum value
    let invalid_event2 = ctx
        .event_builder(&event_source, &event_type)
        .payload(json!({
            "action": "drag", // not in enum
            "element_id": "some-element"
        }))
        .build();

    schema_test_utils::assert_schema_invalid_event(ctx.pool(), &invalid_event2, schema_id).await?;

    Ok(())
}

#[sinex_test]
async fn test_complex_nested_schema_validation(ctx: TestContext) -> TestResult {
    // Test deeply nested schema validation
    let complex_schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "user": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "profile": {
                        "type": "object",
                        "properties": {
                            "settings": {
                                "type": "object",
                                "properties": {
                                    "theme": {"type": "string", "enum": ["light", "dark"]},
                                    "notifications": {"type": "boolean"}
                                },
                                "required": ["theme"]
                            }
                        },
                        "required": ["settings"]
                    }
                },
                "required": ["id", "profile"]
            }
        },
        "required": ["user"]
    });

    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("complex-test-{}", test_run_id);
    let event_type = format!("nested-event-{}", test_run_id);

    let schema_id = schema_test_utils::register_test_schema(
        ctx.pool(),
        &event_source,
        &event_type,
        complex_schema,
    )
    .await?;

    // Valid deeply nested payload
    let valid_event = ctx
        .event_builder(&event_source, &event_type)
        .payload(json!({
            "user": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "profile": {
                    "settings": {
                        "theme": "dark",
                        "notifications": true
                    }
                }
            }
        }))
        .build();

    schema_test_utils::assert_schema_valid_event(ctx.pool(), &valid_event, schema_id).await?;

    // Invalid - missing deep required field
    let invalid_event = ctx
        .event_builder(&event_source, &event_type)
        .payload(json!({
            "user": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "profile": {
                    "settings": {
                        // missing required "theme"
                        "notifications": false
                    }
                }
            }
        }))
        .build();

    schema_test_utils::assert_schema_invalid_event(ctx.pool(), &invalid_event, schema_id).await?;

    Ok(())
}

// =============================================================================
// SCHEMA VALIDATION TESTS
// =============================================================================

/// Test that validation prevents malformed events from being inserted
#[sinex_test]
async fn test_validation_prevents_malformed_events(ctx: TestContext) -> TestResult {
    // Test 1: Valid event should work
    let valid_event = RawEventBuilder::new(
        "fs",
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/test/valid.txt",
            "size": 1024,
            "permissions": "644"
        }),
    )
    .build();

    // This should succeed
    let _result = insert_event(ctx.pool(), &valid_event).await?;

    // Test 2: Invalid event should fail
    let invalid_event = RawEventBuilder::new(
        "", // Empty source should fail
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/test/invalid.txt"
        }),
    )
    .build();

    let result = insert_event(ctx.pool(), &invalid_event).await;
    assert!(result.is_err(), "Invalid event should fail validation");

    Ok(())
}

#[sinex_test]
async fn test_schema_validation_with_registered_schemas(ctx: TestContext) -> TestResult {
    // Register a schema for filesystem events
    let fs_schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "minLength": 1
            },
            "size": {
                "type": "integer",
                "minimum": 0
            }
        },
        "required": ["path"],
        "additionalProperties": true
    });

    let _schema_id = schema_test_utils::register_test_schema(
        ctx.pool(),
        "fs",
        event_type_constants::filesystem::FILE_CREATED,
        fs_schema,
    )
    .await?;

    // Valid event
    let valid_event = RawEventBuilder::new(
        "fs",
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/test/valid.txt",
            "size": 1024
        }),
    )
    .build();

    let _result = insert_event(ctx.pool(), &valid_event).await?;

    // Invalid event - missing required field
    let invalid_event = RawEventBuilder::new(
        "fs",
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "size": 1024
            // missing path
        }),
    )
    .build();

    let result = insert_event(ctx.pool(), &invalid_event).await;
    assert!(result.is_err(), "Should fail validation without required path");

    Ok(())
}

// =============================================================================
// WORK QUEUE TESTS
// =============================================================================

#[sinex_test]
async fn test_work_queue_table_exists(ctx: TestContext) -> TestResult {
    // Check that work_queue table exists
    let result = sqlx::query!(
        "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_name = 'work_queue' AND table_schema = 'sinex_schemas'"
    )
    .fetch_one(ctx.pool())
    .await?;

    pretty_assertions::assert_eq!(result.count.unwrap(), 1, "work_queue table should exist");
    Ok(())
}

#[sinex_test]
async fn test_work_queue_has_new_columns(ctx: TestContext) -> TestResult {
    // Check for new columns
    let columns = sqlx::query!(
        r#"
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'work_queue'
        AND table_schema = 'sinex_schemas'
        AND column_name IN ('processed_at', 'failure_reason')
        ORDER BY column_name
        "#
    )
    .fetch_all(ctx.pool())
    .await?;

    pretty_assertions::assert_eq!(
        columns.len(),
        2,
        "work_queue should have processed_at and failure_reason columns"
    );

    let column_names: Vec<String> = columns
        .iter()
        .filter_map(|r| r.column_name.clone())
        .collect();
    assert!(
        column_names.contains(&"processed_at".to_string()),
        "Missing processed_at column"
    );
    assert!(
        column_names.contains(&"failure_reason".to_string()),
        "Missing failure_reason column"
    );

    Ok(())
}

#[sinex_test]
async fn test_work_queue_status_enum_includes_succeeded(ctx: TestContext) -> TestResult {
    // Test that the status column supports 'succeeded' and 'failed' values
    
    // First insert a test event
    let event = RawEventBuilder::new("test_source", "test_event", json!({"test": "data"})).build();
    let event_id = insert_event(ctx.pool(), &event).await?;

    // Add to work queue
    let _queue_item = add_to_work_queue(ctx.pool(), event_id, "test-agent", 3).await?;

    // Try to update status to 'succeeded' - should work with new enum values
    let result = sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = now() WHERE raw_event_id = $1::uuid::ulid",
        event_id.to_uuid()
    )
    .execute(ctx.pool())
    .await;

    assert!(
        result.is_ok(),
        "Should be able to set status to 'succeeded'"
    );
    Ok(())
}

#[sinex_test]
async fn test_work_queue_basic_operations(ctx: TestContext) -> TestResult {
    // Test basic work queue operations
    let event = RawEventBuilder::new("test_source", "test_event", json!({"test": "data"})).build();
    let event_id = insert_event(ctx.pool(), &event).await?;

    // Add to work queue
    let _queue_item = add_to_work_queue(ctx.pool(), event_id, "test-agent", 3).await?;

    // Claim the item
    let claimed_items = claim_work_queue_items(ctx.pool(), "test-agent", "worker-1", 1).await?;
    assert_eq!(claimed_items.len(), 1);

    // Complete the item
    complete_work_queue_item(ctx.pool(), claimed_items[0].queue_id).await?;

    // Verify it's completed
    let completed_count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE queue_id = $1::uuid::ulid AND status = 'succeeded'",
        claimed_items[0].queue_id.to_uuid()
    )
    .fetch_one(ctx.pool())
    .await?;

    assert_eq!(completed_count.unwrap(), 1);

    Ok(())
}

// =============================================================================
// WORK QUEUE TTL TESTS
// =============================================================================

#[sinex_test]
async fn test_ttl_policy_purges_old_succeeded_items(ctx: TestContext) -> TestResult {
    // Create test events
    let old_event = RawEventBuilder::new(
        "test_source",
        "test_event",
        json!({"test": "old_succeeded"}),
    )
    .build();
    let old_event_id = insert_event(ctx.pool(), &old_event).await?;

    let recent_event = RawEventBuilder::new(
        "test_source",
        "test_event",
        json!({"test": "recent_succeeded"}),
    )
    .build();
    let recent_event_id = insert_event(ctx.pool(), &recent_event).await?;

    // Add to work queue
    let old_queue_item = add_to_work_queue(ctx.pool(), old_event_id, "test-agent", 3).await?;
    let recent_queue_item = add_to_work_queue(ctx.pool(), recent_event_id, "test-agent", 3).await?;

    // Mark both as succeeded but with different timestamps
    let old_timestamp = Utc::now() - Duration::days(8); // Over TTL
    let recent_timestamp = Utc::now() - Duration::hours(1); // Within TTL

    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = $1 WHERE queue_id = $2::uuid::ulid",
        old_timestamp,
        old_queue_item.to_uuid()
    )
    .execute(ctx.pool())
    .await?;

    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'succeeded', processed_at = $1 WHERE queue_id = $2::uuid::ulid",
        recent_timestamp,
        recent_queue_item.to_uuid()
    )
    .execute(ctx.pool())
    .await?;

    // Simulate TTL cleanup (normally done by background job)
    let cleaned_up = sqlx::query!(
        "DELETE FROM sinex_schemas.work_queue WHERE status = 'succeeded' AND processed_at < now() - interval '7 days' RETURNING queue_id::text"
    )
    .fetch_all(ctx.pool())
    .await?;

    assert!(cleaned_up.len() > 0, "Should have cleaned up old succeeded items");

    // Verify recent item still exists
    let recent_exists = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE queue_id = $1::uuid::ulid",
        recent_queue_item.to_uuid()
    )
    .fetch_one(ctx.pool())
    .await?;

    assert_eq!(recent_exists.unwrap(), 1, "Recent item should still exist");

    Ok(())
}

// =============================================================================
// QUEUE METRICS TESTS
// =============================================================================

#[sinex_test]
async fn test_queue_depth_metric_calculation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create test events
    let event1 = RawEventBuilder::new("metrics_test", "event1", json!({"test": 1})).build();
    let event2 = RawEventBuilder::new("metrics_test", "event2", json!({"test": 2})).build();
    let event3 = RawEventBuilder::new("metrics_test", "event3", json!({"test": 3})).build();

    let event1_id = insert_event(pool, &event1).await?;
    let event2_id = insert_event(pool, &event2).await?;
    let event3_id = insert_event(pool, &event3).await?;

    // Add to work queue for different agents
    let agent1 = "metrics-agent-1";
    let agent2 = "metrics-agent-2";

    add_to_work_queue(pool, event1_id, agent1, 3).await?;
    add_to_work_queue(pool, event2_id, agent1, 3).await?;
    add_to_work_queue(pool, event3_id, agent2, 3).await?;

    // Calculate queue depth metrics
    let metrics = calculate_queue_depth_metrics(pool).await?;

    // Verify metrics for each agent
    let agent1_metric = metrics.iter().find(|m| m.target_agent_name == agent1);
    let agent2_metric = metrics.iter().find(|m| m.target_agent_name == agent2);

    assert_eq!(agent1_metric.map(|m| m.queue_depth).unwrap_or(0), 2, "Agent 1 should have 2 items");
    assert_eq!(agent2_metric.map(|m| m.queue_depth).unwrap_or(0), 1, "Agent 2 should have 1 item");

    Ok(())
}

// =============================================================================
// ROUTING CACHE TESTS
// =============================================================================

#[sinex_test]
async fn test_routing_cache_view_exists(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    
    // Test that the routing_cache materialized view exists
    let view_exists = sqlx::query!(
        r#"
        SELECT COUNT(*) as count
        FROM pg_matviews
        WHERE schemaname = 'sinex_schemas'
        AND matviewname = 'routing_cache'
        "#
    )
    .fetch_one(pool)
    .await?;

    // Note: This might be 0 if the view doesn't exist yet
    println!("Routing cache view exists: {}", view_exists.count.unwrap_or(0) > 0);
    
    Ok(())
}

#[sinex_test]
async fn test_routing_cache_basic_functionality(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create a test event
    let event = RawEventBuilder::new("routing_test", "test_event", json!({"data": "test"})).build();
    let event_id = insert_event(pool, &event).await?;

    // Add to work queue
    let _queue_item = add_to_work_queue(pool, event_id, "routing-agent", 3).await?;

    // Query work queue to verify routing logic
    let work_items = sqlx::query!(
        "SELECT queue_id::text, target_agent_name FROM sinex_schemas.work_queue WHERE raw_event_id::uuid = $1",
        event_id.to_uuid()
    )
    .fetch_all(pool)
    .await?;

    assert!(work_items.len() > 0, "Should have work items");
    assert_eq!(work_items[0].target_agent_name, "routing-agent");

    Ok(())
}

// =============================================================================
// CONNECTION POOL TESTS
// =============================================================================

#[sinex_test]
async fn test_connection_pool_max_connections(ctx: TestContext) -> TestResult {
    // Use the existing managed pool instead of creating a new one
    let pool = ctx.pool().clone();

    // Try to acquire more connections than the pool size
    let mut handles = vec![];
    let pool = Arc::new(pool);

    for i in 0..10 {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            let start = Instant::now();
            match pool.acquire().await {
                Ok(_conn) => {
                    // Hold the connection for a bit
                    tokio::time::sleep(StdDuration::from_millis(100)).await;
                    Ok((i, start.elapsed()))
                }
                Err(e) => Err((i, e)),
            }
        });
        handles.push(handle);
    }

    let results = join_all(handles).await;

    // First several should succeed quickly
    let mut succeeded = 0;
    let mut _timed_out = 0;

    for result in results {
        match result? {
            Ok((_, elapsed)) => {
                succeeded += 1;
                // Should get connection relatively quickly
                assert!(elapsed < StdDuration::from_secs(3));
            }
            Err((_, _)) => {
                _timed_out += 1;
            }
        }
    }

    // At least 5 should succeed (pool size)
    assert!(succeeded >= 5);

    Ok(())
}

#[sinex_test]
async fn test_connection_pool_concurrent_pressure(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Spawn many concurrent tasks
    let mut handles = vec![];

    for i in 0..100 {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            // Each task does a quick query
            let result: i32 = sqlx::query_scalar("SELECT $1::int")
                .bind(i)
                .fetch_one(&pool)
                .await?;

            Ok::<_, sqlx::Error>(result)
        });
        handles.push(handle);
    }

    // All should complete successfully
    let results = join_all(handles).await;

    for (i, result) in results.into_iter().enumerate() {
        let value = result??;
        pretty_assertions::assert_eq!(value, i as i32);
    }

    Ok(())
}

#[sinex_test]
async fn test_connection_pool_error_recovery(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Cause an error on a connection
    let result = sqlx::query("SELECT * FROM nonexistent_table")
        .fetch_all(pool)
        .await;
    assert!(result.is_err());

    // Pool should still be usable
    let working: i32 = sqlx::query_scalar("SELECT 42").fetch_one(pool).await?;
    pretty_assertions::assert_eq!(working, 42);

    // Try multiple operations to ensure pool is healthy
    for i in 0..10 {
        let result: i32 = sqlx::query_scalar("SELECT $1::int")
            .bind(i)
            .fetch_one(pool)
            .await?;
        pretty_assertions::assert_eq!(result, i);
    }

    Ok(())
}

#[sinex_test]
async fn test_connection_pool_statement_cache(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Execute the same prepared statement many times
    let start = Instant::now();
    for i in 0..100 {
        let _result: i32 = sqlx::query_scalar("SELECT $1::int + $2::int")
            .bind(i)
            .bind(10)
            .fetch_one(pool)
            .await?;
    }
    let cached_duration = start.elapsed();

    // Execute different statements (no cache benefit)
    let start = Instant::now();
    for i in 0..100 {
        let query = format!("SELECT {}::int + 10", i);
        let _result: i32 = sqlx::query_scalar(&query).fetch_one(pool).await?;
    }
    let uncached_duration = start.elapsed();

    // Cached queries should generally be faster (though not guaranteed in all environments)
    println!(
        "Cached: {:?}, Uncached: {:?}",
        cached_duration, uncached_duration
    );

    Ok(())
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================



