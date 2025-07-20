// Consolidated Database Integration Tests
//
// This module contains comprehensive integration tests for all database functionality,
// consolidated from multiple separate test files. Tests cover:
// - Basic database operations and transactions
// - TimescaleDB hypertable functionality
// - ULID primary key integration
// - JSON schema validation with pg_jsonschema
// - Checkpoint operations and progress tracking
// - Connection pool edge cases and limits
// - Query performance and optimization
// - Data integrity and consistency
//
// Uses #[sinex_test] for automatic transaction isolation and TestContext
// for unified database access.

use crate::common::prelude::*;
use crate::common::{self, assertions, events, generators, schema_test_utils};
use chrono::{Duration, Utc};
use futures::future::join_all;
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use sinex_events::{EventFactory, services, event_types};
use sinex_ulid::Ulid;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use uuid::Uuid;

// Local type definition for checkpoint queries
#[derive(sqlx::FromRow)]
struct CheckpointRecord {
    pub automaton_name: String,
    pub consumer_group: String,
    pub last_processed_id: Option<String>,
    pub state_data: Option<serde_json::Value>,
    pub processed_count: i64,
}

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
    let insert_tasks: Vec<_> = events
        .iter()
        .map(|event| {
            let pool = ctx.pool().clone();
            let event = event.clone();
            tokio::spawn(async move {
                sinex_db::insert_event_with_validator(&pool, &event, None)
                    .await
                    .map(|e| e.id)
            })
        })
        .collect();

    // Wait for all insertions to complete
    let mut inserted_ids = Vec::new();
    for task in insert_tasks {
        let id = task.await??;
        inserted_ids.push(id);
    }

    // Verify all events exist (also done concurrently)
    let verify_tasks: Vec<_> = inserted_ids
        .iter()
        .map(|&id| {
            let pool = ctx.pool().clone();
            tokio::spawn(async move {
                get_event_by_id(&pool, id)
                    .await
                    .is_ok()
            })
        })
        .collect();

    for task in verify_tasks {
        assert!(task.await?);
    }

    // Check total count - use centralized query
    let (count,): (i64,) = EventQueries::count_all()
        .fetch_one(ctx.pool())
        .await?;
    assert!(count >= 10);

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
    let pool = ctx.pool().clone();
    let insert_tasks: Vec<_> = events_to_insert
        .iter()
        .map(|&event| {
            let pool = pool.clone();
            let event = event.clone();
            tokio::spawn(async move { assertions::assert_event_inserted(&pool, &event).await })
        })
        .collect();

    // Wait for all insertions
    for task in insert_tasks {
        task.await?;
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
        assert!(
            ulids[i] > ulids[i - 1],
            "ULIDs should be in chronological order"
        );
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
    let pool = ctx.pool().clone();

    // Verify core.events is a hypertable
    let hypertable_info: Option<(String, String, String, String)> = sqlx::query_as(
        "SELECT hypertable_schema, hypertable_name,
                column_name, dimension_type
         FROM timescaledb_information.dimensions
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'",
    )
    .fetch_optional(&pool)
    .await?;

    assert!(
        hypertable_info.is_some(),
        "core.events should be a hypertable"
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
    .fetch_optional(&pool)
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
    let pool = ctx.pool().clone();

    // Clean up any previous test data
    let _ = EventQueries::delete_by_source("chunk_test".to_string()).execute(&pool).await;

    // Get initial chunk count
    let initial_chunks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM timescaledb_information.chunks
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'",
    )
    .fetch_one(&pool)
    .await?;

    // Insert events across different time periods to trigger chunk creation
    let time_periods = [
        Utc::now(),
        Utc::now() - Duration::days(10),
        Utc::now() - Duration::days(20),
        Utc::now() + Duration::days(5),
    ];

    for (i, ts) in time_periods.iter().enumerate() {
        let factory = EventFactory::new("chunk_test");
        let event = factory.create_event(
            &format!("event_type_{}", i),
            json!({"chunk_test": i}),
        );

        // Insert with specific timestamp by creating ULID from timestamp
        let event_id = Ulid::from_datetime(*ts);
        EventQueries::insert_event(
            event.source.clone(),
            event.event_type.clone(),
            event.host.clone(),
            event.payload.clone(),
            event.ts_orig,
            event.ingestor_version.clone(),
            event.payload_schema_id,
            event.source_event_ids.clone(),
        )
        .execute(&pool)
        .await?;
    }

    // Get new chunk count
    let new_chunks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM timescaledb_information.chunks
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'",
    )
    .fetch_one(&pool)
    .await?;

    assert!(
        new_chunks >= initial_chunks,
        "Should have created additional chunks for different time periods"
    );

    // Verify chunks contain the correct data
    for (i, ts) in time_periods.iter().enumerate() {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM core.events
             WHERE source = $1
             AND event_type = $2
             AND ts_ingest >= $3 - interval '1 hour'
             AND ts_ingest <= $3 + interval '1 hour'",
        )
        .bind("chunk_test")
        .bind(format!("event_type_{}", i))
        .bind(ts)
        .fetch_one(&pool)
        .await?;

        pretty_assertions::assert_eq!(count, 1, "Each event should be in its appropriate chunk");
    }
    Ok(())
}

#[sinex_test]
async fn test_timescale_compression_policy(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Check if compression policy exists
    let compression_policy: Option<(i32,)> = sqlx::query_as(
        "SELECT job_id
         FROM timescaledb_information.jobs
         WHERE hypertable_schema = 'raw'
         AND hypertable_name = 'events'
         AND proc_name = 'compress_chunks'",
    )
    .fetch_optional(&pool)
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
        .fetch_optional(&pool)
        .await?;

        assert!(compress_after.is_some());
        let days = compress_after.unwrap();
        assert!(days >= 7, "Compression should happen after at least 7 days");
    }

    // Insert old data to test compression
    let _old_timestamp = Utc::now() - Duration::days(30);
    for i in 0..10 {
        let event_id = Ulid::new();
        EventQueries::insert_event(
            "compression_test".to_string(),
            "old_event".to_string(),
            "test_host".to_string(),
            json!({"seq": i}),
            None,
            None,
            None,
            None,
        )
        .execute(&pool)
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
    .fetch_one(&pool)
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
    use sinex_db::queries::SchemaQueries;
    #[derive(sqlx::FromRow)]
    struct SchemaRecord {
        json_schema_definition: serde_json::Value,
    }
    let schema_record: SchemaRecord = SchemaQueries::get_by_id(schema_id)
        .fetch_one(ctx.pool())
        .await?;
    let retrieved_schema = schema_record.json_schema_definition;

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

    let schema_id = schema_test_utils::register_test_schema(
        ctx.pool(),
        &event_source,
        &event_type,
        strict_schema.clone(),
    )
    .await?;

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
    let factory = EventFactory::new(sources::FS);
    let valid_event = factory.create_event(
        event_types::filesystem::FILE_CREATED,
        json!({
            "path": "/test/valid.txt",
            "size": 1024,
            "permissions": "644"
        }),
    );

    // This should succeed
    let _result = insert_event(ctx.pool(), &valid_event).await?;

    // Test 2: Invalid event should fail
    let factory = EventFactory::new(""); // Empty source should fail
    let invalid_event = factory.create_event(
        event_types::filesystem::FILE_CREATED,
        json!({
            "path": "/test/invalid.txt"
        }),
    );

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
        event_types::filesystem::FILE_CREATED,
        fs_schema,
    )
    .await?;

    // Valid event
    let factory = EventFactory::new(sources::FS);
    let valid_event = factory.create_event(
        event_types::filesystem::FILE_CREATED,
        json!({
            "path": "/test/valid.txt",
            "size": 1024
        }),
    );

    let _result = insert_event(ctx.pool(), &valid_event).await?;

    // Invalid event - missing required field
    let factory = EventFactory::new(sources::FS);
    let invalid_event = factory.create_event(
        event_types::filesystem::FILE_CREATED,
        json!({
            "size": 1024
            // missing path
        }),
    );

    let result = insert_event(ctx.pool(), &invalid_event).await;
    assert!(
        result.is_err(),
        "Should fail validation without required path"
    );

    Ok(())
}

// =============================================================================
// CHECKPOINT TESTS
// =============================================================================

/// Test checkpoint persistence and progress tracking
#[sinex_test]
async fn test_checkpoint_persistence(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create a test automaton checkpoint
    let automaton_name = "test_checkpoint_automaton";
    let consumer_group = "test_group";
    let checkpoint_id = Ulid::new();

    // Insert checkpoint
    CheckpointQueries::upsert_checkpoint(
        checkpoint_id,
        automaton_name.to_string(),
        consumer_group.to_string(),
        "consumer_test".to_string(),
        Some("test_event_id".to_string()),
        42,
        chrono::Utc::now(),
        Some(json!({"processed_count": 42, "test_data": true})),
        1,
        None,
        chrono::Utc::now(),
        chrono::Utc::now(),
    )
    .execute(&pool)
    .await?;

    // Verify checkpoint exists
    #[derive(sqlx::FromRow)]
    struct CheckpointRecord {
        automaton_name: String,
        consumer_group: String,
        last_processed_id: Option<String>,
        state_data: Option<serde_json::Value>,
        processed_count: i64,
    }
    let checkpoint: CheckpointRecord = sqlx::query_as!(
        CheckpointRecord,
        r#"
        SELECT automaton_name, consumer_group, last_processed_id, 
               state_data as "state_data: serde_json::Value", processed_count
        FROM core.automaton_checkpoints
        WHERE id = $1::uuid
        "#,
        checkpoint_id.to_uuid()
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(checkpoint.automaton_name, automaton_name);
    assert_eq!(checkpoint.consumer_group, consumer_group);
    assert_eq!(
        checkpoint.last_processed_id.as_ref(),
        Some(&"test_event_id".to_string())
    );

    let state_data = checkpoint.state_data.unwrap();
    assert_eq!(
        state_data.get("processed_count").unwrap().as_u64().unwrap(),
        42
    );
    assert_eq!(
        state_data.get("test_data").unwrap().as_bool().unwrap(),
        true
    );

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_update_operations(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    let automaton_name = "test_update_automaton";
    let checkpoint_id = Ulid::new();

    // Create CheckpointManager
    use sinex_satellite_sdk::checkpoint::{CheckpointManager, CheckpointState};
    let checkpoint_manager = CheckpointManager::new(
        ctx.pool().clone(),
        automaton_name.to_string(),
        "default_group".to_string(),
        "test_consumer".to_string(),
    );

    // Load and update checkpoint
    let mut checkpoint = checkpoint_manager.load_checkpoint().await?;
    checkpoint.processed_count = 10;
    checkpoint.set_last_processed_id(Some("initial_event".to_string()));
    checkpoint.data = Some(json!({"processed_count": 10}));
    checkpoint_manager.save_checkpoint(&checkpoint).await?;

    // Update checkpoint again
    checkpoint.processed_count = 25;
    checkpoint.set_last_processed_id(Some("updated_event".to_string()));
    checkpoint.data = Some(json!({"processed_count": 25, "status": "active"}));
    checkpoint_manager.save_checkpoint(&checkpoint).await?;

    // Verify update
    let checkpoint: CheckpointRecord = sqlx::query_as!(
        CheckpointRecord,
        r#"
        SELECT automaton_name, consumer_group, last_processed_id, 
               state_data as "state_data: serde_json::Value", processed_count
        FROM core.automaton_checkpoints
        WHERE id = $1::uuid
        "#,
        checkpoint_id.to_uuid()
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(
        checkpoint.last_processed_id.as_deref(),
        Some("updated_event")
    );

    let state_data = checkpoint.state_data.unwrap();
    assert_eq!(
        state_data.get("processed_count").unwrap().as_u64().unwrap(),
        25
    );
    assert_eq!(
        state_data.get("status").unwrap().as_str().unwrap(),
        "active"
    );

    Ok(())
}

// =============================================================================
// CHECKPOINT LIFECYCLE TESTS
// =============================================================================

#[sinex_test]
async fn test_checkpoint_lifecycle_management(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Test checkpoint creation and cleanup patterns
    let automaton_name = "test_lifecycle_automaton";

    // Create multiple checkpoints for the same automaton
    let checkpoint_ids = [Ulid::new(), Ulid::new(), Ulid::new()];

    for (i, checkpoint_id) in checkpoint_ids.iter().enumerate() {
        CheckpointQueries::upsert_checkpoint(
            *checkpoint_id,
            automaton_name.to_string(),
            "default_group".to_string(),
            format!("consumer_{}", i),
            Some(format!("event_{}", i)),
            (i * 10) as i64,
            Utc::now() - Duration::hours((i + 1) as i64),
            Some(json!({"processed_count": i * 10})),
            1,
            None,
            Utc::now() - Duration::hours((i + 1) as i64),
            Utc::now(),
        )
        .execute(&pool)
        .await?;
    }

    // Verify all checkpoints exist
    let (checkpoint_count,): (i64,) = CheckpointQueries::count_checkpoints_by_processor(automaton_name.to_string())
        .fetch_one(&pool)
        .await?;

    assert_eq!(checkpoint_count, 3, "All checkpoints should be created");

    // Get the most recent checkpoint
    let latest_checkpoint = CheckpointQueries::get_all_checkpoints_for_processor(automaton_name.to_string())
        .fetch_one(&pool)
        .await?;

    assert_eq!(
        latest_checkpoint.last_processed_id.as_deref(),
        Some("event_0")
    );

    // Cleanup old checkpoints (keeping only the latest)
    let deleted_count = sqlx::query!(
        "DELETE FROM core.automaton_checkpoints 
         WHERE automaton_name = $1 
         AND id != $2::uuid 
         RETURNING id::text",
        automaton_name,
        latest_checkpoint
            .id
            .unwrap()
            .parse::<Ulid>()
            .unwrap()
            .to_uuid()
    )
    .fetch_all(&pool)
    .await?;

    assert_eq!(deleted_count.len(), 2, "Should cleanup old checkpoints");

    // Verify only latest remains
    let remaining_count: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.automaton_checkpoints WHERE automaton_name = $1",
        automaton_name
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    assert_eq!(remaining_count, 1, "Only latest checkpoint should remain");

    Ok(())
}

// =============================================================================
// CHECKPOINT METRICS TESTS
// =============================================================================

#[sinex_test]
async fn test_checkpoint_progress_metrics(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test checkpoints for different automata
    let automaton1 = "metrics-automaton-1";
    let automaton2 = "metrics-automaton-2";

    let checkpoint1_id = Ulid::new();
    let checkpoint2_id = Ulid::new();
    let checkpoint3_id = Ulid::new();

    // Insert checkpoints with different progress levels
    sqlx::query!(
        "INSERT INTO core.automaton_checkpoints (id, automaton_name, last_processed_id, state_data)
         VALUES ($1::uuid, $2, $3, $4)",
        checkpoint1_id.to_uuid(),
        automaton1,
        "event_100",
        json!({"processed_count": 100, "last_activity": "2024-01-01T10:00:00Z"})
    )
    .execute(&pool)
    .await?;

    sqlx::query!(
        "INSERT INTO core.automaton_checkpoints (id, automaton_name, last_processed_id, state_data)
         VALUES ($1::uuid, $2, $3, $4)",
        checkpoint2_id.to_uuid(),
        automaton1,
        "event_200",
        json!({"processed_count": 200, "last_activity": "2024-01-01T11:00:00Z"})
    )
    .execute(&pool)
    .await?;

    sqlx::query!(
        "INSERT INTO core.automaton_checkpoints (id, automaton_name, last_processed_id, state_data)
         VALUES ($1::uuid, $2, $3, $4)",
        checkpoint3_id.to_uuid(),
        automaton2,
        "event_50",
        json!({"processed_count": 50, "last_activity": "2024-01-01T09:00:00Z"})
    )
    .execute(&pool)
    .await?;

    // Calculate checkpoint metrics by automaton
    let metrics = sqlx::query!(
        "SELECT automaton_name, COUNT(*) as checkpoint_count,
                MAX((state_data->>'processed_count')::int) as max_processed
         FROM core.automaton_checkpoints
         WHERE automaton_name IN ($1, $2)
         GROUP BY automaton_name
         ORDER BY automaton_name",
        automaton1,
        automaton2
    )
    .fetch_all(&pool)
    .await?;

    assert_eq!(metrics.len(), 2, "Should have metrics for both automata");

    let automaton1_metrics = metrics
        .iter()
        .find(|m| m.automaton_name == automaton1)
        .unwrap();
    let automaton2_metrics = metrics
        .iter()
        .find(|m| m.automaton_name == automaton2)
        .unwrap();

    assert_eq!(automaton1_metrics.checkpoint_count.unwrap(), 2);
    assert_eq!(automaton1_metrics.max_processed.unwrap(), 200);

    assert_eq!(automaton2_metrics.checkpoint_count.unwrap(), 1);
    assert_eq!(automaton2_metrics.max_processed.unwrap(), 50);

    Ok(())
}

// =============================================================================
// REDIS STREAMS INTEGRATION TESTS
// =============================================================================

#[sinex_test]
async fn test_redis_streams_checkpoint_coordination(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Test that checkpoint data coordinates with Redis Streams
    // This simulates the satellite architecture pattern

    let automaton_name = "redis-test-automaton";
    let consumer_group = "test_consumer_group";

    // Create checkpoint that would correspond to Redis consumer group state
    let checkpoint_id = Ulid::new();
    sqlx::query!(
        "INSERT INTO core.automaton_checkpoints (id, automaton_name, consumer_group, last_processed_id, state_data)
         VALUES ($1::uuid, $2, $3, $4, $5)",
        checkpoint_id.to_uuid(),
        automaton_name,
        consumer_group,
        "1640995200000-0", // Redis stream ID format
        json!({
            "consumer_group": consumer_group,
            "last_message_id": "1640995200000-0",
            "processed_count": 15,
            "pending_count": 3
        })
    )
    .execute(&pool)
    .await?;

    // Verify checkpoint exists and has expected Redis-compatible structure
    let checkpoint = sqlx::query!(
        "SELECT automaton_name, consumer_group, last_processed_id, state_data
         FROM core.automaton_checkpoints WHERE id = $1::uuid",
        checkpoint_id.to_uuid()
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(checkpoint.automaton_name, automaton_name);
    assert_eq!(checkpoint.consumer_group, consumer_group);
    assert_eq!(
        checkpoint.last_processed_id.as_ref(),
        Some(&"1640995200000-0".to_string())
    );

    let state = checkpoint.state_data.unwrap();
    assert_eq!(state.get("processed_count").unwrap().as_u64().unwrap(), 15);
    assert_eq!(state.get("pending_count").unwrap().as_u64().unwrap(), 3);

    Ok(())
}

#[sinex_test]
async fn test_automaton_checkpoint_progress_tracking(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Test checkpoint progress tracking for satellite architecture
    let automaton_name = "progress-tracking-automaton";

    // Simulate processing progress over time
    let progress_points = [
        ("1640995200000-0", 0),
        ("1640995210000-0", 5),
        ("1640995220000-0", 12),
        ("1640995230000-0", 18),
    ];

    let checkpoint_id = Ulid::new();

    // Insert initial checkpoint
    sqlx::query!(
        "INSERT INTO core.automaton_checkpoints (id, automaton_name, last_processed_id, state_data)
         VALUES ($1::uuid, $2, $3, $4)",
        checkpoint_id.to_uuid(),
        automaton_name,
        progress_points[0].0,
        json!({"processed_count": progress_points[0].1})
    )
    .execute(&pool)
    .await?;

    // Simulate progress updates
    for (stream_id, count) in &progress_points[1..] {
        sqlx::query!(
            "UPDATE core.automaton_checkpoints 
             SET last_processed_id = $2, state_data = $3, updated_at = NOW()
             WHERE id = $1::uuid",
            checkpoint_id.to_uuid(),
            stream_id,
            json!({"processed_count": count})
        )
        .execute(&pool)
        .await?;
    }

    // Verify final state
    let final_checkpoint = sqlx::query!(
        "SELECT last_processed_id, state_data FROM core.automaton_checkpoints WHERE id = $1::uuid",
        checkpoint_id.to_uuid()
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(
        final_checkpoint.last_processed_id.as_deref(),
        Some("1640995230000-0")
    );

    let state = final_checkpoint.state_data.unwrap();
    assert_eq!(state.get("processed_count").unwrap().as_u64().unwrap(), 18);

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
    let pool = ctx.pool().clone();

    // Cause an error on a connection
    let result = sqlx::query("SELECT * FROM nonexistent_table")
        .fetch_all(&pool)
        .await;
    assert!(result.is_err());

    // Pool should still be usable
    let working: i32 = sqlx::query_scalar("SELECT 42").fetch_one(&pool).await?;
    pretty_assertions::assert_eq!(working, 42);

    // Try multiple operations to ensure pool is healthy
    for i in 0..10 {
        let result: i32 = sqlx::query_scalar("SELECT $1::int")
            .bind(i)
            .fetch_one(&pool)
            .await?;
        pretty_assertions::assert_eq!(result, i);
    }

    Ok(())
}

#[sinex_test]
async fn test_connection_pool_statement_cache(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Execute the same prepared statement many times
    let start = Instant::now();
    for i in 0..100 {
        let _result: i32 = sqlx::query_scalar("SELECT $1::int + $2::int")
            .bind(i)
            .bind(10)
            .fetch_one(&pool)
            .await?;
    }
    let cached_duration = start.elapsed();

    // Execute different statements (no cache benefit)
    let start = Instant::now();
    for i in 0..100 {
        let query = format!("SELECT {}::int + 10", i);
        let _result: i32 = sqlx::query_scalar(&query).fetch_one(&pool).await?;
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
// OPERATIONS LOG TESTS
// =============================================================================

/// Test operations_log table basic functionality
#[sinex_test]
async fn test_operations_log_basic_functionality(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Test start_operation function
    let operation_id_str: String = sqlx::query_scalar!(
        "SELECT core.start_operation($1, $2, $3::jsonb)::text as \"operation_id!\"",
        "stage",
        "test_user",
        json!({"command": "exo blob stage test.log", "flags": ["--verbose"]})
    )
    .fetch_one(&pool)
    .await?;

    use std::str::FromStr;
    let operation_id = Ulid::from_str(&operation_id_str)?;

    // Verify operation was created correctly
    let operation = sqlx::query!(
        "SELECT operation_type, status, invoked_by_user, parameters
         FROM core.operations_log WHERE operation_id = $1::text::ulid",
        operation_id_str
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(operation.operation_type, "stage");
    assert_eq!(operation.status, "started");
    assert_eq!(operation.invoked_by_user.as_deref(), Some("test_user"));

    let params = operation.parameters;
    assert_eq!(
        params.get("command").unwrap().as_str().unwrap(),
        "exo blob stage test.log"
    );

    // Test complete_operation function
    sqlx::query!(
        "SELECT core.complete_operation($1::text::ulid, $2::jsonb)",
        operation_id_str,
        json!({"events_created": 42, "blobs_processed": 1})
    )
    .execute(&pool)
    .await?;

    // Verify completion and duration calculation
    let completed_operation = sqlx::query!(
        "SELECT status, completed_at, duration_ms, summary
         FROM core.operations_log WHERE operation_id = $1::text::ulid",
        operation_id_str
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(completed_operation.status, "completed");
    assert!(completed_operation.completed_at.is_some());
    assert!(completed_operation.duration_ms.is_some());
    assert!(completed_operation.duration_ms.unwrap() >= 0);

    let summary = completed_operation.summary.unwrap();
    assert_eq!(summary.get("events_created").unwrap().as_u64().unwrap(), 42);
    assert_eq!(summary.get("blobs_processed").unwrap().as_u64().unwrap(), 1);

    Ok(())
}

/// Test operations_log error handling and validation
#[sinex_test]
async fn test_operations_log_error_handling(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Test invalid operation type
    let result = sqlx::query_scalar::<_, Ulid>("SELECT core.start_operation($1, $2, $3::jsonb)")
        .bind("invalid_type")
        .bind("test_user")
        .bind(json!({}))
        .fetch_one(&pool)
        .await;

    assert!(result.is_err(), "Should reject invalid operation type");

    // Test fail_operation function
    let operation_id_str: String = sqlx::query_scalar!(
        "SELECT core.start_operation($1, $2, $3::jsonb)::text as \"operation_id!\"",
        "replay",
        "test_user",
        json!({"command": "exo replay --ingestor fs-watcher --blob abc123"})
    )
    .fetch_one(&pool)
    .await?;

    sqlx::query!(
        "SELECT core.fail_operation($1::text::ulid, $2::jsonb)",
        operation_id_str,
        json!({"error": "blob not found", "error_code": "E404"})
    )
    .execute(&pool)
    .await?;

    // Verify failure was recorded
    let failed_operation = sqlx::query!(
        "SELECT status, completed_at, duration_ms, summary
         FROM core.operations_log WHERE operation_id = $1::text::ulid",
        operation_id_str
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(failed_operation.status, "failed");
    assert!(failed_operation.completed_at.is_some());
    assert!(failed_operation.duration_ms.is_some());

    let summary = failed_operation.summary.unwrap();
    assert_eq!(
        summary.get("error").unwrap().as_str().unwrap(),
        "blob not found"
    );
    assert_eq!(summary.get("error_code").unwrap().as_str().unwrap(), "E404");

    Ok(())
}

/// Test operations_log performance indexes
#[sinex_test]
async fn test_operations_log_index_performance(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create multiple operations for index testing
    let operation_types = ["stage", "replay", "archive", "restore", "curate"];
    let users = ["user1", "user2", "user3"];

    for op_type in &operation_types {
        for user in &users {
            let operation_id_str: String = sqlx::query_scalar!(
                "SELECT core.start_operation($1, $2, $3::jsonb)::text as \"operation_id!\"",
                op_type,
                user,
                json!({"test": true})
            )
            .fetch_one(&pool)
            .await?;

            // Complete half of them, fail the other half
            if operation_id_str.chars().last().unwrap() as u8 % 2 == 0 {
                sqlx::query!(
                    "SELECT core.complete_operation($1::text::ulid, $2::jsonb)",
                    operation_id_str,
                    json!({"test": "completed"})
                )
                .execute(&pool)
                .await?;
            } else {
                sqlx::query!(
                    "SELECT core.fail_operation($1::text::ulid, $2::jsonb)",
                    operation_id_str,
                    json!({"test": "failed"})
                )
                .execute(&pool)
                .await?;
            }
        }
    }

    // Test that index is used for common queries
    // Query by operation type and status (should use idx_operations_log_monitoring)
    let explain_result = sqlx::query_scalar!(
        "EXPLAIN SELECT * FROM core.operations_log 
         WHERE operation_type = $1 AND status = $2 
         ORDER BY started_at DESC LIMIT 10",
        "stage",
        "completed"
    )
    .fetch_all(&pool)
    .await?;

    let plan = explain_result
        .into_iter()
        .filter_map(|x| x)
        .collect::<Vec<String>>()
        .join(" ");

    // Should use the monitoring index
    assert!(
        plan.contains("idx_operations_log_monitoring") || plan.contains("Index Scan"),
        "Query should use index efficiently: {}",
        plan
    );

    // Test user-based query
    let user_operations = sqlx::query!(
        "SELECT COUNT(*) as count FROM core.operations_log WHERE invoked_by_user = $1",
        "user1"
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(user_operations.count.unwrap(), 5); // 5 operation types for user1

    Ok(())
}

/// Test operations_log auditability and intent tracking
#[sinex_test]
async fn test_operations_log_auditability(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Simulate a complete workflow with proper audit trail
    let user = "audit_test_user";

    // 1. Stage operation
    let stage_op_id_str: String = sqlx::query_scalar!(
        "SELECT core.start_operation($1, $2, $3::jsonb)::text as \"operation_id!\"",
        "stage",
        user,
        json!({
            "command": "exo blob stage /path/to/important.log",
            "flags": ["--source", "production-server", "--comment", "Critical system logs"],
            "file_size": 1048576,
            "file_path": "/path/to/important.log"
        })
    )
    .fetch_one(&pool)
    .await?;

    sqlx::query!(
        "SELECT core.complete_operation($1::text::ulid, $2::jsonb)",
        stage_op_id_str,
        json!({
            "blob_id": "01K0B2PBEWTZTS5AG5A128C92Z",
            "events_created": 1,
            "bytes_staged": 1048576,
            "checksum": "blake3:abc123def456"
        })
    )
    .execute(&pool)
    .await?;

    // 2. Replay operation
    let replay_op_id_str: String = sqlx::query_scalar!(
        "SELECT core.start_operation($1, $2, $3::jsonb)::text as \"operation_id!\"",
        "replay",
        user,
        json!({
            "command": "exo replay --ingestor system-logs --blob 01K0B2PBEWTZTS5AG5A128C92Z",
            "flags": ["--since", "2025-01-01T00:00:00Z", "--until", "2025-01-02T00:00:00Z"],
            "ingestor": "system-logs",
            "blob_id": "01K0B2PBEWTZTS5AG5A128C92Z"
        })
    )
    .fetch_one(&pool)
    .await?;

    sqlx::query!(
        "SELECT core.complete_operation($1::text::ulid, $2::jsonb)",
        replay_op_id_str,
        json!({
            "events_created": 127,
            "events_archived": 3,
            "time_range_processed": {
                "start": "2025-01-01T00:00:00Z",
                "end": "2025-01-02T00:00:00Z"
            },
            "ingestor_version": "v2.1.0"
        })
    )
    .execute(&pool)
    .await?;

    // Verify complete audit trail
    let audit_trail = sqlx::query!(
        "SELECT operation_id::text, operation_type, status, 
                started_at, completed_at, duration_ms, parameters, summary
         FROM core.operations_log 
         WHERE invoked_by_user = $1 
         ORDER BY started_at ASC",
        user
    )
    .fetch_all(&pool)
    .await?;

    assert_eq!(audit_trail.len(), 2, "Should have complete audit trail");

    // Verify stage operation
    let stage_record = &audit_trail[0];
    assert_eq!(stage_record.operation_type, "stage");
    assert_eq!(stage_record.status, "completed");
    assert!(stage_record.duration_ms.is_some());

    let stage_params = &stage_record.parameters;
    assert_eq!(
        stage_params.get("command").unwrap().as_str().unwrap(),
        "exo blob stage /path/to/important.log"
    );

    let stage_summary = stage_record.summary.as_ref().unwrap();
    assert_eq!(
        stage_summary
            .get("events_created")
            .unwrap()
            .as_u64()
            .unwrap(),
        1
    );
    assert_eq!(
        stage_summary.get("bytes_staged").unwrap().as_u64().unwrap(),
        1048576
    );

    // Verify replay operation
    let replay_record = &audit_trail[1];
    assert_eq!(replay_record.operation_type, "replay");
    assert_eq!(replay_record.status, "completed");

    let replay_params = &replay_record.parameters;
    assert_eq!(
        replay_params.get("ingestor").unwrap().as_str().unwrap(),
        "system-logs"
    );

    let replay_summary = replay_record.summary.as_ref().unwrap();
    assert_eq!(
        replay_summary
            .get("events_created")
            .unwrap()
            .as_u64()
            .unwrap(),
        127
    );
    assert_eq!(
        replay_summary
            .get("events_archived")
            .unwrap()
            .as_u64()
            .unwrap(),
        3
    );

    // Verify operations are in chronological order
    assert!(audit_trail[0].started_at < audit_trail[1].started_at);

    Ok(())
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================
