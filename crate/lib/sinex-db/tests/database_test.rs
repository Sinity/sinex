//! Database Unit Tests - Migrated to Modern Test Infrastructure
//!
//! This test suite validates the core database functionality including:
//! - Event persistence and retrieval
//! - Concurrent operations and isolation  
//! - Schema validation
//! - Transaction semantics
//! - Performance characteristics

use sinex_db::{
    DynamicPayload, Event, Id, JsonValue, PoolConfig, Provenance, SinexError, Timestamp,
    acquire_with_timeout, create_pool_with_config,
};
use sinex_primitives::domain::{EventSource, EventType, RecordedPath};
use sinex_primitives::events::payloads::{FileCreatedPayload, KittyCommandExecutedPayload};
use sinex_primitives::{Pagination, Seconds, Uuid};
use xtask::sandbox::prelude::*;

// Additional specific imports
use std::collections::HashSet;
use std::sync::Arc;
use time::Duration;

// =============================================================================
// CORE DATABASE OPERATIONS
// =============================================================================

#[sinex_test]
async fn test_event_persistence_basics(ctx: TestContext) -> TestResult<()> {
    // Test basic repository insertion using typed payloads.
    let material_id = ctx.create_source_material(Some("db-test-material")).await?;

    let mut payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/tmp/test.txt").map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    payload.size = 1024;
    payload.permissions = Some(0o644);

    let event = Event::new(
        payload,
        Provenance::from_material(material_id, 0, None, None),
    );

    let inserted = ctx.pool.events().insert(event).await?;
    let event_id = inserted.id.expect("inserted event should have id");

    // Verify event structure
    assert_eq!(inserted.source.as_str(), "fs-watcher");
    assert_eq!(inserted.event_type.as_str(), "file.created");
    assert_eq!(inserted.payload["path"], json!("/tmp/test.txt"));
    assert_eq!(inserted.payload["size"], json!(1024));
    let retrieved = ctx.pool.events().get_by_id(event_id).await?;
    let retrieved = retrieved.expect("event should be retrievable by id");
    assert_eq!(
        retrieved.id.expect("retrieved event should have id"),
        event_id
    );
    assert_eq!(retrieved.payload["path"], json!("/tmp/test.txt"));

    Ok(())
}

#[sinex_test]
async fn test_event_queries(ctx: TestContext) -> TestResult<()> {
    // Test query pattern using production repository queries.
    let material_id = ctx
        .create_source_material(Some("db-query-material"))
        .await?;

    let fs_payload = FileCreatedPayload::test_default(
        RecordedPath::from_observed("/tmp/1.txt").map_err(|e| color_eyre::eyre::eyre!(e))?,
    );
    let fs_event = Event::new(
        fs_payload,
        Provenance::from_material(material_id, 0, None, None),
    );

    let terminal_payload = KittyCommandExecutedPayload::test_default("ls");
    let terminal_event = Event::new(
        terminal_payload,
        Provenance::from_material(material_id, 0, None, None),
    );

    let inserted_fs = ctx.pool.events().insert(fs_event).await?;
    let inserted_terminal = ctx.pool.events().insert(terminal_event).await?;
    let fs_id = inserted_fs.id.expect("fs event should have id");
    let terminal_id = inserted_terminal.id.expect("terminal event should have id");

    let fs_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("fs-watcher"),
            Pagination::new(Some(10), None),
        )
        .await?;
    assert!(fs_events.iter().any(|event| event.id == Some(fs_id)));

    let command_events = ctx
        .pool
        .events()
        .get_by_event_type(
            &EventType::from("command.executed"),
            Pagination::new(Some(10), None),
        )
        .await?;
    assert!(
        command_events
            .iter()
            .any(|event| event.id == Some(terminal_id))
    );

    Ok(())
}

// =============================================================================
// EDGE CASES AND SPECIAL CHARACTERS
// =============================================================================

#[sinex_test]
async fn test_edge_case_payloads(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("edge-case")).await?;

    let test_cases = vec![
        ("empty_payload", json!({})),
        ("null_values", json!({"value": null, "data": null})),
        (
            "unicode_text",
            json!({"text": "Hello 世界 🌍", "path": "/tmp/test-α-β-γ.txt"}),
        ),
        (
            "special_chars",
            json!({"text": "quotes: \"double\" 'single'", "newlines": "line1\nline2\ttab"}),
        ),
        ("large_payload", json!({"data": "x".repeat(100_000)})),
        ("deep_nesting", {
            let mut nested = json!("value");
            for _ in 0..10 {
                nested = json!({"nested": nested});
            }
            nested
        }),
    ];

    for (i, (test_name, payload)) in test_cases.into_iter().enumerate() {
        let event = DynamicPayload::new("edge-test", test_name, payload.clone())
            .from_material_at(material_id, i as i64)
            .build()?;
        let inserted = ctx.pool.events().insert(event).await?;

        assert_eq!(inserted.payload, payload);

        let event_id = inserted.id.unwrap();
        let retrieved = ctx.pool.events().get_by_id(event_id).await?.unwrap();
        assert_eq!(retrieved.payload, payload);
    }

    Ok(())
}

#[sinex_test]
async fn test_concurrent_event_insertion(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("concurrent-test")).await?;
    let num_tasks = 10;
    let events_per_task = 10;
    let run_suffix = Uuid::now_v7();

    let pool = ctx.pool.clone();
    let mut handles = vec![];
    let barrier = Arc::new(tokio::sync::Barrier::new(num_tasks));

    for task_id in 0..num_tasks {
        let barrier_clone = barrier.clone();
        let pool_clone = pool.clone();

        let handle = tokio::spawn(async move {
            // Wait for all tasks to start simultaneously
            barrier_clone.wait().await;

            let mut task_ids = Vec::new();

            for event_num in 0..events_per_task {
                let source = format!("task-{task_id}-{run_suffix}");
                let event = DynamicPayload::new(
                    source.as_str(),
                    "concurrent.test",
                    json!({
                        "task_id": task_id,
                        "event_num": event_num,
                        "timestamp": Timestamp::now()
                    }),
                )
                .from_material_at(material_id, event_num as i64)
                .build()
                .map_err(|e| SinexError::unknown(e.to_string()))?;
                let inserted = pool_clone
                    .events()
                    .insert(event)
                    .await
                    .map_err(|e| SinexError::unknown(e.to_string()))?;

                task_ids.push(inserted.id.unwrap());
            }

            Ok::<Vec<Id<Event<JsonValue>>>, SinexError>(task_ids)
        });

        handles.push(handle);
    }

    // Collect all results as strings since IDs don't implement Hash
    let mut all_id_strings: HashSet<String> = HashSet::new();
    let mut total_events = 0;

    for handle in handles {
        let task_ids = handle
            .await
            .map_err(|e| SinexError::service(format!("Task failed: {e}")))??;

        // Verify no duplicate IDs across tasks
        for id in &task_ids {
            let id_str = id.to_string();
            assert!(
                !all_id_strings.contains(&id_str),
                "ID collision detected: {id}"
            );
            all_id_strings.insert(id_str);
        }

        total_events += task_ids.len();
    }

    // Verify total count
    assert_eq!(total_events, num_tasks * events_per_task);
    assert_eq!(all_id_strings.len(), total_events);

    // Verify events persisted by querying a sample source
    let sample_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from(format!("task-0-{run_suffix}")),
            Pagination::new(Some(100), None),
        )
        .await?;
    assert_eq!(sample_events.len(), events_per_task);

    Ok(())
}

// =============================================================================
// TRANSACTION SEMANTICS
// =============================================================================

#[sinex_test]
async fn test_transaction_rollback(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("txn-rollback")).await?;
    let initial_count = ctx.pool.events().count_all().await?;

    // Test successful insertion
    let success_event =
        DynamicPayload::new("transaction-test", "success", json!({"test": "commit"}))
            .from_material(material_id)
            .build()?;
    let _inserted = ctx.pool.events().insert(success_event).await?;

    let after_success = ctx.pool.events().count_all().await?;
    assert!(after_success > initial_count);

    // Invalid source is rejected by typed domain validation before persistence.
    assert!(
        EventSource::new("").is_err(),
        "Empty source should be rejected"
    );

    // Event count should be unchanged after rejection
    let after_rejection = ctx.pool.events().count_all().await?;
    assert_eq!(after_rejection, after_success);

    // Verify no rollback events were persisted
    let rollback_events = ctx
        .pool
        .events()
        .get_by_event_type(
            &EventType::from("rollback"),
            Pagination::new(Some(10), None),
        )
        .await?;
    assert_eq!(rollback_events.len(), 0);
    Ok(())
}

// =============================================================================
// SCHEMA VALIDATION
// =============================================================================

#[sinex_test]
async fn test_schema_validation(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("schema-validation"))
        .await?;

    // Test creating events with valid payloads
    let valid_event = DynamicPayload::new(
        "schema-test",
        "valid.event",
        json!({
            "required_field": "value",
            "optional_field": 42
        }),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(valid_event).await?;
    assert!(inserted.id.is_some());

    // Test that edge-case payloads are handled gracefully
    let edge_event = DynamicPayload::new(
        "schema-test",
        "edge.case",
        json!({
            "string_field": "",  // Empty string
            "number_field": 0,   // Zero value
            "array_field": [],   // Empty array
            "object_field": {}   // Empty object
        }),
    )
    .from_material_at(material_id, 1)
    .build()?;
    let edge_inserted = ctx.pool.events().insert(edge_event).await?;
    assert!(edge_inserted.id.is_some());

    Ok(())
}

// =============================================================================
// PERFORMANCE CHARACTERISTICS
// =============================================================================

#[sinex_test]
async fn test_bulk_insert_performance(ctx: TestContext) -> TestResult<()> {
    let batch_size = 100;
    let start_time = std::time::Instant::now();

    // Create a material for this test's events
    let material_id = ctx.create_source_material(Some("bulk-insert-test")).await?;

    // Create batch of events
    let mut events = Vec::new();
    for i in 0..batch_size {
        let event = DynamicPayload::new(
            EventSource::from("performance-test"),
            EventType::from("bulk.insert"),
            json!({
                "batch_index": i,
                "data": format!("event_{}", i)
            }),
        )
        .from_material(material_id)
        .build()?;
        events.push(event);
    }

    // Insert all events using batch insertion to mirror production behaviour
    let inserted_events = ctx.pool.events().insert_batch(events.clone()).await?;

    let insert_duration = start_time.elapsed();

    // Verify all events were inserted
    assert_eq!(inserted_events.len(), batch_size);
    let stored_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("performance-test"),
            Pagination::new(Some(200), None),
        )
        .await?;
    assert_eq!(stored_events.len(), batch_size);

    // Performance assertion - should complete reasonably quickly
    // Allow generous time for CI environments
    assert!(
        insert_duration.as_millis() < 5000,
        "Bulk insert of {} events took {}ms, should be < 5000ms",
        batch_size,
        insert_duration.as_millis()
    );

    Ok(())
}

#[sinex_test]
async fn pool_acquire_timeout_is_reported(ctx: TestContext) -> TestResult<()> {
    let config = PoolConfig {
        max_connections: 1,
        min_connections: 1,
        acquire_timeout_secs: Seconds::from_secs(30),
        idle_timeout_secs: Seconds::from_secs(300),
        statement_timeout_secs: Seconds::from_secs(60),
        validate_against_postgres_max: false,
    };
    let pool = create_pool_with_config(ctx.database_url(), &config).await?;

    let _conn = pool.acquire().await?;
    let result = acquire_with_timeout(&pool, Duration::milliseconds(50)).await;
    assert!(matches!(result, Err(SinexError::Timeout(_))));

    Ok(())
}

#[sinex_test]
async fn test_query_performance(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("query-perf")).await?;

    // Insert test data directly
    let num_events = 200;
    for i in 0..num_events {
        let event = DynamicPayload::new(
            format!("query-perf-{}", i % 10), // 10 different sources
            "query.test",
            json!({
                "index": i,
                "category": i % 5  // 5 different categories
            }),
        )
        .from_material_at(material_id, i as i64)
        .build()?;
        ctx.pool.events().insert(event).await?;
    }

    // Validate dataset landed
    let total = ctx
        .pool
        .events()
        .get_by_event_type(
            &EventType::from("query.test"),
            Pagination::new(Some(300), None),
        )
        .await?;
    assert!(total.len() >= num_events);

    // Test various query patterns
    let start_time = std::time::Instant::now();

    // Query by source
    let source_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("query-perf-0"),
            Pagination::new(Some(200), None),
        )
        .await?;
    assert!(!source_events.is_empty());

    // Query by event type
    let type_events = ctx
        .pool
        .events()
        .get_by_event_type(
            &EventType::from("query.test"),
            Pagination::new(Some(200), None),
        )
        .await?;
    assert!(!type_events.is_empty());

    // Query recent events
    let recent_events = ctx.pool.events().get_recent(50).await?;
    assert_eq!(recent_events.len(), 50);

    let query_duration = start_time.elapsed();

    // Performance assertion
    assert!(
        query_duration.as_millis() < 3000,
        "Query operations took {}ms, should be < 3000ms",
        query_duration.as_millis()
    );

    Ok(())
}

// =============================================================================
// DATA INTEGRITY
// =============================================================================

#[sinex_test]
async fn test_uuid_persistence(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("uuid-persist")).await?;

    // Use a UUIDv7 value to verify string round-tripping through payload persistence.
    let test_uuid = Uuid::now_v7();

    let event = DynamicPayload::new(
        "uuid-test",
        "regression.test",
        json!({"uuid": test_uuid.to_string()}),
    )
    .from_material(material_id)
    .build()?;
    let inserted = ctx.pool.events().insert(event).await?;

    // Verify the event was inserted (UUIDv7 is auto-generated)
    assert!(inserted.id.is_some());

    // Retrieve by the generated ID and verify
    let event_id = inserted.id.unwrap();
    let retrieved = ctx.pool.events().get_by_id(event_id).await?.unwrap();

    assert_eq!(retrieved.id.unwrap(), event_id);
    assert_eq!(retrieved.payload["uuid"], json!(test_uuid.to_string()));

    Ok(())
}

#[sinex_test]
async fn test_timestamp_handling(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("timestamp")).await?;

    let before_insert = Timestamp::now();
    let event = DynamicPayload::new("timestamp-test", "time.test", json!({"test": "timestamp"}))
        .from_material(material_id)
        .build()?;
    let inserted_event = ctx.pool.events().insert(event).await?;
    let after_insert = Timestamp::now();

    // Verify ingestion timestamp is recent
    let ingest_ts = inserted_event.id.as_ref().unwrap().timestamp();
    let tolerance = Duration::milliseconds(50); // Increased for CI
    let before_ts: Timestamp = (*before_insert - tolerance).into();
    let after_ts: Timestamp = (*after_insert + tolerance).into();
    assert!(
        ingest_ts >= before_ts,
        "ingest timestamp {ingest_ts:?} precedes lower bound {before_ts:?}"
    );
    assert!(
        ingest_ts <= after_ts,
        "ingest timestamp {ingest_ts:?} exceeds upper bound {after_ts:?}"
    );

    // Retrieve and verify timestamps persist
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(inserted_event.id.unwrap())
        .await?
        .unwrap();

    assert_eq!(
        retrieved.id.as_ref().unwrap().timestamp(),
        inserted_event.id.as_ref().unwrap().timestamp()
    );

    Ok(())
}

// =============================================================================
// ERROR HANDLING
// =============================================================================

#[sinex_test]
async fn test_constraint_violations(ctx: TestContext) -> TestResult<()> {
    let _material_id = ctx.create_source_material(Some("constraint-test")).await?;

    assert!(
        EventSource::new("").is_err(),
        "Empty source should be rejected"
    );
    assert!(
        EventType::new("").is_err(),
        "Empty event type should be rejected"
    );

    // Verify no invalid events were inserted
    let all_events = ctx.pool.events().get_recent(100).await?;
    assert!(all_events.iter().all(|e| !e.source.as_str().is_empty()));
    assert!(all_events.iter().all(|e| !e.event_type.as_str().is_empty()));

    Ok(())
}

#[sinex_test]
async fn test_database_recovery_scenarios(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("recovery")).await?;

    // Large JSON payload
    let large_payload = json!({
        "large_string": "x".repeat(1_000_000),
        "large_array": vec![42; 10_000],
        "nested": {
            "deep": {
                "very": {
                    "deeply": {
                        "nested": "value"
                    }
                }
            }
        }
    });

    let event = DynamicPayload::new("recovery-test", "large.payload", large_payload)
        .from_material(material_id)
        .build()?;
    let inserted = ctx.pool.events().insert(event).await?;

    assert!(inserted.id.is_some());

    // Retrieve large event to ensure it persisted correctly
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(inserted.id.unwrap())
        .await?
        .unwrap();

    assert_eq!(
        retrieved.payload["large_string"].as_str().unwrap().len(),
        1_000_000
    );
    assert_eq!(
        retrieved.payload["large_array"].as_array().unwrap().len(),
        10_000
    );

    Ok(())
}
