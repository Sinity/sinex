//! Database Unit Tests - Migrated to Modern Test Infrastructure
//!
//! This test suite validates the core database functionality including:
//! - Event persistence and retrieval
//! - Concurrent operations and isolation  
//! - Schema validation
//! - Transaction semantics
//! - Performance characteristics

use sinex_core::db::models::{Event, JsonValue};
use sinex_core::types::domain::{EventSource, EventType, SanitizedPath};
use sinex_core::types::events::payloads::{FileCreatedPayload, KittyCommandExecutedPayload};
use sinex_core::types::Seconds;
use sinex_core::{
    acquire_with_timeout, create_pool_with_config, Id, PoolConfig, Provenance, SinexError,
    SourceMaterial, Ulid,
};
use sinex_test_utils::prelude::*;

// Additional specific imports
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

// =============================================================================
// CORE DATABASE OPERATIONS
// =============================================================================

#[sinex_test]
async fn test_event_persistence_basics(ctx: TestContext) -> TestResult<()> {
    // Test basic repository insertion using typed payloads.
    let material_id = ctx.create_source_material(Some("db-test-material")).await?;

    let mut payload = FileCreatedPayload::test_default(
        SanitizedPath::from_str_validated("/tmp/test.txt")
            .map_err(|e| color_eyre::eyre::eyre!(e))?,
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
    let retrieved = ctx.pool.events().get_by_id(event_id.clone()).await?;
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
        SanitizedPath::from_str_validated("/tmp/1.txt").map_err(|e| color_eyre::eyre::eyre!(e))?,
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
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    assert!(fs_events
        .iter()
        .any(|event| event.id == Some(fs_id.clone())));

    let command_events = ctx
        .pool
        .events()
        .get_by_event_type(
            &EventType::from("command.executed"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    assert!(command_events
        .iter()
        .any(|event| event.id == Some(terminal_id.clone())));

    Ok(())
}

// =============================================================================
// EDGE CASES AND SPECIAL CHARACTERS
// =============================================================================

#[sinex_test]
async fn test_edge_case_payloads(ctx: TestContext) -> TestResult<()> {
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

    for (test_name, payload) in test_cases {
        let event = ctx
            .publish_json_event("edge-test", test_name, payload.clone())
            .await?;

        assert_eq!(event.payload, payload);

        let event_id = event.id.unwrap();
        let retrieved = ctx.pool.events().get_by_id(event_id).await?.unwrap();
        assert_eq!(retrieved.payload, payload);
    }

    Ok(())
}

#[sinex_serial_test]
async fn test_concurrent_event_insertion(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    // Test concurrent insertion from multiple tasks
    let num_tasks = 10;
    let events_per_task = 10;
    let run_suffix = Ulid::new();

    let mut handles = vec![];
    let barrier = Arc::new(tokio::sync::Barrier::new(num_tasks));

    let shared_ctx = Arc::new(ctx);
    let _pool_for_cleanup = shared_ctx.pool.clone();
    for task_id in 0..num_tasks {
        let barrier_clone = barrier.clone();
        let ctx_clone = Arc::clone(&shared_ctx);

        let handle = tokio::spawn(async move {
            // Wait for all tasks to start simultaneously
            barrier_clone.wait().await;

            let mut task_ids = Vec::new();

            // Insert events concurrently
            for event_num in 0..events_per_task {
                let inserted = ctx_clone
                    .publish_json_event(
                        &format!("task-{task_id}-{run_suffix}"),
                        "concurrent.test",
                        json!({
                            "task_id": task_id,
                            "event_num": event_num,
                            "timestamp": chrono::Utc::now()
                        }),
                    )
                    .await
                    .map_err(|e| SinexError::unknown(e.to_string()))?;

                task_ids.push(inserted.id.unwrap());
            }

            // Verify all events for this task
            tokio::time::timeout(Duration::from_secs(12), async {
                loop {
                    let events = ctx_clone
                        .pool
                        .events()
                        .get_by_source(
                            &EventSource::from(format!("task-{task_id}-{run_suffix}")),
                            sinex_core::types::Pagination::new(Some(100), None),
                        )
                        .await?;
                    if events.len() >= events_per_task {
                        break Ok::<_, SinexError>(());
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .map_err(|e| SinexError::unknown(e.to_string()))??;

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

    Ok(())
}

// =============================================================================
// TRANSACTION SEMANTICS
// =============================================================================

#[sinex_serial_test]
async fn test_transaction_rollback(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;

    let initial_count = ctx.pool.events().count_all().await?;

    // Test successful transaction
    let _success_event = ctx
        .publish_json_event("transaction-test", "success", json!({"test": "commit"}))
        .await?;

    let after_success = ctx.pool.events().count_all().await?;
    assert!(after_success >= initial_count + 1);

    // Note: Complex transaction rollback testing requires low-level database access
    // For now, we test that invalid events are properly rejected
    let invalid_result = ctx
        .publish_json_event(
            "", // Empty source should be rejected
            "rollback",
            json!({"test": "rollback"}),
        )
        .await;

    assert!(invalid_result.is_err(), "Empty source should be rejected");

    // Event count should be unchanged after rejection
    let after_rejection = ctx.pool.events().count_all().await?;
    assert_eq!(after_rejection, after_success);

    // Verify no rollback events were persisted
    let rollback_events = ctx
        .pool
        .events()
        .get_by_event_type(
            &EventType::from("rollback"),
            sinex_core::types::Pagination::new(Some(10), None),
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
    // Test creating events with valid payloads
    let valid_event = ctx
        .publish_json_event(
            "schema-test",
            "valid.event",
            json!({
                "required_field": "value",
                "optional_field": 42
            }),
        )
        .await?;

    assert!(valid_event.id.is_some());

    // Test that malformed events are handled gracefully
    // Note: The test infrastructure should handle validation internally
    // We're testing the repository layer behavior

    let edge_case_event = ctx
        .publish_json_event(
            "schema-test",
            "edge.case",
            json!({
                "string_field": "",  // Empty string
                "number_field": 0,   // Zero value
                "array_field": [],   // Empty array
                "object_field": {}   // Empty object
            }),
        )
        .await?;

    assert!(edge_case_event.id.is_some());

    Ok(())
}

// =============================================================================
// PERFORMANCE CHARACTERISTICS
// =============================================================================

#[sinex_test]
async fn test_bulk_insert_performance(ctx: TestContext) -> TestResult<()> {
    let batch_size = 100;
    let start_time = std::time::Instant::now();
    let bootstrap_material =
        Id::<SourceMaterial>::from_str("014D2PF2DBSQQZXQ5TK1V58CGG").expect("valid bootstrap id");
    ctx.ensure_source_material(bootstrap_material, Some("test-material-bootstrap"))
        .await?;

    // Create batch of events
    let mut events = Vec::new();
    for i in 0..batch_size {
        let event = Event::dynamic(
            EventSource::from("performance-test"),
            EventType::from("bulk.insert"),
            json!({
                "batch_index": i,
                "data": format!("event_{}", i)
            }),
        )
        .from_material(bootstrap_material, 0)
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
            sinex_core::types::Pagination::new(Some(200), None),
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
        validate_against_postgres_max: false,
    };
    let pool = create_pool_with_config(ctx.database_url(), &config).await?;

    let _conn = pool.acquire().await?;
    let result = acquire_with_timeout(&pool, Duration::from_millis(50)).await;
    assert!(matches!(result, Err(SinexError::Timeout(_))));

    Ok(())
}

#[sinex_serial_test]
async fn test_query_performance(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;

    // Insert test data
    let num_events = 200;
    let mut events = Vec::with_capacity(num_events);
    for i in 0..num_events {
        let source = format!("query-perf-{}", i % 10); // 10 different sources
        events.push(Event::<JsonValue>::test_event(
            EventSource::from(source),
            EventType::from("query.test"),
            json!({
                "index": i,
                "category": i % 5  // 5 different categories
            }),
        ));
    }

    ctx.pool.events().insert_batch(events).await?;

    // Validate dataset landed before running timed queries.
    let total = ctx.pool.events().count_all().await?;
    assert_eq!(total as usize, num_events);

    // Test various query patterns
    let start_time = std::time::Instant::now();

    // Query by source
    let source_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("query-perf-0"),
            sinex_core::types::Pagination::new(Some(200), None),
        )
        .await?;
    assert!(!source_events.is_empty());

    // Query by event type
    let type_events = ctx
        .pool
        .events()
        .get_by_event_type(
            &EventType::from("query.test"),
            sinex_core::types::Pagination::new(Some(200), None),
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
async fn test_ulid_persistence(ctx: TestContext) -> TestResult<()> {
    // Test specific ULID edge cases
    let test_ulid = Ulid::from_str("01ARZ3NDEKTSV4RRFFQ69G5FAV")?;

    let event = Event::<JsonValue>::test_event(
        EventSource::from("ulid-test"),
        EventType::from("regression.test"),
        json!({"ulid": test_ulid.to_string()}),
    );

    let inserted_event = ctx.pool.events().insert(event).await?;

    // Verify the event was inserted (ULID is auto-generated)
    assert!(inserted_event.id.is_some());

    // Retrieve by the generated ID and verify
    let event_id = inserted_event.id.unwrap();
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(event_id.clone())
        .await?
        .unwrap();

    assert_eq!(retrieved.id.unwrap(), event_id);
    assert_eq!(retrieved.payload["ulid"], json!(test_ulid.to_string()));

    Ok(())
}

#[sinex_test]
async fn test_timestamp_handling(ctx: TestContext) -> TestResult<()> {
    use chrono::{Duration as ChronoDuration, TimeZone, Utc};

    // Test with specific original timestamp
    let original_time = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

    let event = Event::<JsonValue>::test_event(
        EventSource::from("timestamp-test"),
        EventType::from("time.test"),
        json!({"test": "timestamp"}),
    )
    .at_time(original_time);

    let before_insert = Utc::now();
    let inserted_event = ctx.pool.events().insert(event).await?;
    let after_insert = Utc::now();

    // Verify original timestamp preserved
    assert_eq!(inserted_event.ts_orig, Some(original_time));

    // Verify ingestion timestamp is recent
    let ingest_ts = inserted_event.id.as_ref().unwrap().as_ulid().timestamp();
    let tolerance = ChronoDuration::milliseconds(5);
    assert!(
        ingest_ts >= before_insert - tolerance,
        "ingest timestamp {ingest_ts:?} precedes lower bound {lower:?}",
        lower = before_insert - tolerance
    );
    assert!(
        ingest_ts <= after_insert + tolerance,
        "ingest timestamp {ingest_ts:?} exceeds upper bound {upper:?}",
        upper = after_insert + tolerance
    );

    // Retrieve and verify timestamps persist
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(inserted_event.id.clone().unwrap())
        .await?
        .unwrap();

    assert_eq!(retrieved.ts_orig, Some(original_time));
    assert_eq!(
        retrieved.id.as_ref().unwrap().as_ulid().timestamp(),
        inserted_event.id.as_ref().unwrap().as_ulid().timestamp()
    );

    Ok(())
}

// =============================================================================
// ERROR HANDLING
// =============================================================================

#[sinex_test]
async fn test_constraint_violations(ctx: TestContext) -> TestResult<()> {
    // Test handling of constraint violations gracefully

    // Empty source should be rejected
    let empty_source_result = ctx
        .publish_json_event(
            "", // Empty source
            "test.event",
            json!({"data": "test"}),
        )
        .await;
    assert!(empty_source_result.is_err());

    // Empty event type should be rejected
    let empty_type_result = ctx
        .publish_json_event(
            "test-source",
            "", // Empty event type
            json!({"data": "test"}),
        )
        .await;
    assert!(empty_type_result.is_err());

    // Verify no invalid events were inserted
    let all_events = ctx.pool.events().get_recent(100).await?;
    assert!(all_events.iter().all(|e| !e.source.as_str().is_empty()));
    assert!(all_events.iter().all(|e| !e.event_type.as_str().is_empty()));

    Ok(())
}

#[sinex_test]
async fn test_database_recovery_scenarios(ctx: TestContext) -> TestResult<()> {
    // Test various scenarios that could cause database issues

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

    let large_event = ctx
        .publish_json_event("recovery-test", "large.payload", large_payload)
        .await?;

    assert!(large_event.id.is_some());

    // Retrieve large event to ensure it persisted correctly
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(large_event.id.unwrap())
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
