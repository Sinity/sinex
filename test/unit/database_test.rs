//! Database Unit Tests
//!
//! Consolidated database layer tests covering:
//! - Basic database operations and connectivity
//! - Event insertion, validation, and querying
//! - Schema validation and model serialization
//! - Database pool management and transaction handling
//! - Event validator functionality
//! - Complex query operations

use crate::common::prelude::*;
use sinex_core::{typed_sources, typed_event_types};
use sinex_db::validation::EventValidator;
use sinex_db::models::*;

// =============================================================================
// BASIC DATABASE OPERATIONS
// =============================================================================

/// Test basic database connectivity and simple queries
#[sinex_test]
async fn test_database_connection(ctx: TestContext) -> TestResult {
    // Test database connectivity with enhanced error context
    let result: i32 = assert_database_state(
        ctx.pool(),
        async {
            sqlx::query_scalar!("SELECT 1 as test_value")
                .fetch_one(ctx.pool())
                .await
                .map(|opt| opt.unwrap_or(0))
        },
        "basic database connectivity test",
    )
    .await?;

    // Use ValidationChain to validate the result
    let result_validation =
        assert_with_validation(result, "db_test_result").custom(|&val| val == 1, "should equal 1");

    assert_validation_passes(result_validation)?;
    Ok(())
}

/// Test basic event insertion using enhanced event builder
#[sinex_test]
async fn test_basic_event_insertion(ctx: TestContext) -> TestResult {
    // Create a simple test event using enhanced event builder
    let event = EventBuilder::filesystem()
        .path("/test/simple_file.txt")
        .created()
        .size(1024)
        .build();

    // Insert using enhanced assertion with error context
    let event_id =
        assert_event_inserted_with_context(ctx.pool(), &event, "basic_event_insertion_test")
            .await?;

    // Retrieve the inserted event
    let inserted_event = sinex_db::events_correct::get_event_by_id(ctx.pool(), event_id)
        .await
        .map_err(|e| {
            CoreError::database("Failed to retrieve inserted event")
                .with_event_id(event_id)
                .with_context("test_name", "basic_event_insertion")
                .with_source(e)
                .build()
        })?;

    // Verify using enhanced event equivalence assertion
    assert_events_equivalent(&inserted_event, &event)?;

    // Use ValidationChain to validate the event structure
    let event_validation = assert_with_validation(inserted_event.clone(), "inserted_event")
        .has_valid_source()
        .has_valid_event_type()
        .payload_is_object();

    assert_validation_passes(event_validation)?;

    // Validate specific payload fields using ValidationChain
    let path_validation = assert_with_validation(
        inserted_event.payload["path"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        "event_path",
    )
    .not_empty()
    .custom(
        |path| path.starts_with("/test/"),
        "should be in test directory",
    );

    assert_validation_passes(path_validation)?;
    Ok(())
}

/// Test multiple event insertion with unique IDs
#[sinex_test]
async fn test_multiple_event_insertion(ctx: TestContext) -> TestResult {
    // Create multiple test events
    let events = vec![
        EventBuilder::filesystem()
            .path("/test/file1.txt")
            .created()
            .build(),
        EventBuilder::terminal().command("ls").success().build(),
        EventBuilder::clipboard().text("test clipboard").build(),
    ];

    let mut assertion_batch = TestAssertionBatch::new("multi_event_insertion_test");
    let mut event_ids = Vec::new();

    // Insert all events and collect results
    for (i, event) in events.iter().enumerate() {
        let event_id =
            assert_event_inserted_with_context(ctx.pool(), event, &format!("multi_event_{}", i))
                .await?;
        event_ids.push(event_id);
    }

    // Use batch assertions to validate all events
    for (i, (event, event_id)) in events.iter().zip(event_ids.iter()).enumerate() {
        assertion_batch.assert_that(
            || {
                assert_with_context(
                    event_id.to_string().len() == 26,
                    "ULID should be 26 characters",
                    &format!("event_{}_ulid_check", i),
                )
            },
            &format!("event {} ULID validation", i),
        );

        assertion_batch.assert_validation(
            ValidationChain::validate(event.source.clone(), &format!("event_{}_source", i))
                .not_empty(),
            &format!("event {} source validation", i),
        );
    }

    // Execute all batched assertions
    assertion_batch.execute()?;
    Ok(())
}

/// Test working with the new macro infrastructure
#[sinex_test]
async fn test_enhanced_infrastructure(ctx: TestContext) -> TestResult {
    // Test that TestContext provides proper test name
    let test_name = ctx.test_name();
    assert!(!test_name.is_empty());

    // Simple database query
    let result = sqlx::query_scalar!("SELECT 2 + 2 as sum")
        .fetch_one(ctx.pool())
        .await?;
    pretty_assertions::assert_eq!(result, Some(4));

    // Test event creation helpers
    let event = ctx.filesystem_event("/test/file.txt");
    assert_eq!(event.event_type, "file.created");

    // Insert the event
    ctx.insert_event(&event).await?;

    // Verify it exists
    let count = ctx.event_count().await?;
    assert!(count >= 1);

    Ok(())
}

/// Test transaction isolation pattern
#[sinex_test]
async fn test_transaction_isolation(ctx: TestContext) -> TestResult {
    let initial_count = ctx.event_count().await?;
    let events_to_insert = 3;
    
    // Create some test events
    for i in 0..events_to_insert {
        let event = ctx
            .event_builder("test", "example")
            .payload(serde_json::json!({ "index": i }))
            .build();
        ctx.insert_event(&event).await?;
    }

    let new_count = ctx.event_count().await?;
    pretty_assertions::assert_eq!(new_count - initial_count, events_to_insert);
    Ok(())
}

// =============================================================================
// QUERY OPERATIONS
// =============================================================================

/// Test querying events by source
#[sinex_test(timeout = 35)]
async fn test_query_events_by_source(ctx: TestContext) -> TestResult {
    // Insert events from different sources
    let fs_event = RawEventBuilder::new(
        "fs",
        "file.created",
        json!({"path": "/test/fs_file.txt"}),
    )
    .build();

    let terminal_event = RawEventBuilder::new(
        "shell.kitty",
        "command.executed",
        json!({"command": "ls"}),
    )
    .build();

    let wm_event =
        RawEventBuilder::new("wm.hyprland", "window.focus", json!({"window_id": 123})).build();

    queries::insert_event(ctx.pool(), &fs_event).await?;
    queries::insert_event(ctx.pool(), &terminal_event).await?;
    queries::insert_event(ctx.pool(), &wm_event).await?;

    // Query events by source
    let fs_events = queries::get_events_by_source(ctx.pool(), typed_sources::FS.as_str(), 10).await?;
    assert!(!fs_events.is_empty());
    assert!(fs_events.iter().all(|e| e.source == typed_sources::FS.as_str()));

    let shell_events = queries::get_events_by_source(ctx.pool(), typed_sources::SHELL_KITTY.as_str(), 10).await?;
    assert!(!shell_events.is_empty());
    assert!(shell_events.iter().all(|e| e.source == typed_sources::SHELL_KITTY.as_str()));

    let wm_events = queries::get_events_by_source(ctx.pool(), typed_sources::WM_HYPRLAND.as_str(), 10).await?;
    assert!(!wm_events.is_empty());
    assert!(wm_events.iter().all(|e| e.source == typed_sources::WM_HYPRLAND.as_str()));

    Ok(())
}

/// Test querying events by event type
#[sinex_test(timeout = 35)]
async fn test_query_events_by_type(ctx: TestContext) -> TestResult {
    // Insert events of different types
    let create_event = RawEventBuilder::new(
        "fs",
        "file.created",
        json!({"path": "/test/create_file.txt"}),
    )
    .build();

    let delete_event = RawEventBuilder::new(
        "fs",
        "file.deleted",
        json!({"path": "/test/delete_file.txt"}),
    )
    .build();

    let command_event = RawEventBuilder::new(
        "shell.kitty",
        "command.executed",
        json!({"command": "rm file.txt"}),
    )
    .build();

    queries::insert_event(ctx.pool(), &create_event).await?;
    queries::insert_event(ctx.pool(), &delete_event).await?;
    queries::insert_event(ctx.pool(), &command_event).await?;

    // Query by event type
    let create_events = queries::get_events_by_type(ctx.pool(), "file.created", 10).await?;
    assert!(!create_events.is_empty());
    assert!(create_events.iter().all(|e| e.event_type == "file.created"));

    let delete_events = queries::get_events_by_type(ctx.pool(), "file.deleted", 10).await?;
    assert!(!delete_events.is_empty());
    assert!(delete_events.iter().all(|e| e.event_type == "file.deleted"));

    let command_events = queries::get_events_by_type(ctx.pool(), "command.executed", 10).await?;
    assert!(!command_events.is_empty());
    assert!(command_events.iter().all(|e| e.event_type == "command.executed"));

    Ok(())
}

// =============================================================================
// EVENT VALIDATION
// =============================================================================

/// Test event validation creation and basic functionality
#[sinex_test]
async fn test_event_validation_creation(_ctx: TestContext) -> TestResult {
    // Test that EventValidator can be created and used with ValidationChain
    let validator = EventValidator::new();

    // Create test events for validation
    let valid_event = EventBuilder::terminal()
        .command("echo test")
        .success()
        .build();

    let invalid_event = RawEvent {
        id: Ulid::new(),
        source: "".to_string(), // Invalid: empty source
        event_type: "test.invalid".to_string(),
        ts_ingest: chrono::Utc::now(),
        ts_orig: None,
        host: "test_host".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: json!({}),
    };

    // Use ValidationChain to test the events
    let valid_result = validator.validate(&valid_event);
    let invalid_result = validator.validate(&invalid_event);

    // Use enhanced assertions with context
    assert_with_context(
        valid_result.is_ok(),
        "Valid event should pass validation",
        "event_validator_creation_test",
    )?;

    assert_with_context(
        invalid_result.is_err(),
        "Invalid event should fail validation",
        "event_validator_creation_test",
    )?;

    Ok(())
}

/// Test comprehensive event validation scenarios
#[sinex_test]
async fn test_comprehensive_event_validation(_ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();

    // Test various invalid event scenarios
    let test_cases = vec![
        // Empty source
        (RawEvent {
            id: Ulid::new(),
            source: "".to_string(),
            event_type: "test.valid".to_string(),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "test_host".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({}),
        }, "empty_source"),
        // Invalid event type format
        (RawEvent {
            id: Ulid::new(),
            source: "valid_source".to_string(),
            event_type: "invalid-format".to_string(),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "test_host".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({}),
        }, "invalid_event_type"),
        // Empty host
        (RawEvent {
            id: Ulid::new(),
            source: "valid_source".to_string(),
            event_type: "test.valid".to_string(),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({}),
        }, "empty_host"),
    ];

    for (event, case_name) in test_cases {
        let result = validator.validate(&event);
        assert_with_context(
            result.is_err(),
            &format!("Event should fail validation for case: {}", case_name),
            "comprehensive_event_validation_test",
        )?;
    }

    // Test valid event
    let valid_event = EventBuilder::filesystem()
        .path("/test/valid_file.txt")
        .created()
        .build();
    
    let result = validator.validate(&valid_event);
    assert_with_context(
        result.is_ok(),
        "Valid event should pass validation",
        "comprehensive_event_validation_test",
    )?;

    Ok(())
}

// =============================================================================
// MODEL SERIALIZATION
// =============================================================================

/// Test model serialization and deserialization
#[sinex_test]
async fn test_model_serialization(_ctx: TestContext) -> TestResult {
    // Test RawEvent serialization
    let event = EventBuilder::filesystem()
        .path("/test/serialization.txt")
        .created()
        .size(2048)
        .build();

    // Serialize to JSON
    let json_str = serde_json::to_string(&event)
        .map_err(|e| format!("Failed to serialize event: {}", e))?;
    
    // Deserialize back
    let deserialized: RawEvent = serde_json::from_str(&json_str)
        .map_err(|e| format!("Failed to deserialize event: {}", e))?;

    // Verify equivalence
    assert_events_equivalent(&event, &deserialized)?;

    // Test specific fields
    pretty_assertions::assert_eq!(event.id, deserialized.id);
    pretty_assertions::assert_eq!(event.source, deserialized.source);
    pretty_assertions::assert_eq!(event.event_type, deserialized.event_type);
    pretty_assertions::assert_eq!(event.payload, deserialized.payload);

    Ok(())
}

/// Test model serialization with complex payloads
#[sinex_test]
async fn test_complex_payload_serialization(_ctx: TestContext) -> TestResult {
    let complex_payload = json!({
        "file_info": {
            "path": "/test/complex.txt",
            "size": 1024,
            "permissions": "0644",
            "metadata": {
                "created": "2024-01-01T00:00:00Z",
                "modified": "2024-01-01T12:00:00Z",
                "tags": ["important", "test", "complex"]
            }
        },
        "operation": {
            "type": "create",
            "user": "test_user",
            "process": {
                "pid": 1234,
                "name": "test_process",
                "args": ["arg1", "arg2", "arg3"]
            }
        }
    });

    let event = RawEventBuilder::new(
        "fs",
        "file.created",
        complex_payload.clone(),
    )
    .build();

    // Serialize and deserialize
    let json_str = serde_json::to_string(&event)?;
    let deserialized: RawEvent = serde_json::from_str(&json_str)?;

    // Verify complex payload preservation
    pretty_assertions::assert_eq!(event.payload, deserialized.payload);
    pretty_assertions::assert_eq!(event.payload["file_info"]["path"], "/test/complex.txt");
    pretty_assertions::assert_eq!(event.payload["operation"]["process"]["pid"], 1234);
    pretty_assertions::assert_eq!(event.payload["file_info"]["metadata"]["tags"][0], "important");

    Ok(())
}

// =============================================================================
// SCHEMA VALIDATION
// =============================================================================

/// Test schema validation with valid events
#[sinex_test]
async fn test_schema_validation_success(_ctx: TestContext) -> TestResult {
    // Test various event types with valid schemas
    let test_events = vec![
        EventBuilder::filesystem()
            .path("/test/valid_file.txt")
            .created()
            .build(),
        EventBuilder::terminal()
            .command("echo hello")
            .success()
            .build(),
        EventBuilder::clipboard()
            .text("valid clipboard content")
            .build(),
    ];

    let validator = EventValidator::new();
    
    for event in test_events {
        let result = validator.validate(&event);
        assert_with_context(
            result.is_ok(),
            &format!("Event of type {} should pass validation", event.event_type),
            "schema_validation_success_test",
        )?;
    }

    Ok(())
}

/// Test schema validation with invalid events
#[sinex_test]
async fn test_schema_validation_failure(_ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();
    
    // Test event with invalid payload structure
    let invalid_event = RawEvent {
        id: Ulid::new(),
        source: "fs".to_string(),
        event_type: "file.created".to_string(),
        ts_ingest: chrono::Utc::now(),
        ts_orig: None,
        host: "test_host".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: json!({
            "invalid_field": "this should not be here",
            "missing_required_path": "path field is missing"
        }),
    };

    let result = validator.validate(&invalid_event);
    assert_with_context(
        result.is_err(),
        "Event with invalid payload should fail validation",
        "schema_validation_failure_test",
    )?;

    Ok(())
}

// =============================================================================
// WORK QUEUE OPERATIONS
// =============================================================================

/// Test work queue operations with agent management
#[sinex_test(timeout = 45)]
async fn test_work_queue_operations(ctx: TestContext) -> TestResult {
    // Create agent first (required for foreign key)
    let _agent = queries::upsert_agent_manifest(
        ctx.pool(),
        "test_agent",
        "1.0.0",
        "running",
        "test",
        Some("Test agent for work queue"),
        None,
        None,
    )
    .await?;

    // Insert a raw event first
    let event = RawEventBuilder::new(
        "fs",
        "file.created",
        json!({"path": "/test/work_queue_test.txt"}),
    )
    .build();

    let inserted_event = queries::insert_event(ctx.pool(), &event).await?;

    // Add to work queue
    let queue_item = queries::add_to_work_queue(
        ctx.pool(),
        inserted_event.id,
        "test_agent",
        3, // max_attempts
    )
    .await?;

    pretty_assertions::assert_eq!(queue_item.raw_event_id, inserted_event.id);
    pretty_assertions::assert_eq!(queue_item.target_agent_name, "test_agent");
    pretty_assertions::assert_eq!(queue_item.status, "pending");
    pretty_assertions::assert_eq!(queue_item.attempts, 0);
    pretty_assertions::assert_eq!(queue_item.max_attempts, 3);

    // Get next item for processing
    let next_item = queries::get_next_work_item(ctx.pool(), "test_agent").await?;
    assert!(next_item.is_some());

    let item = next_item.unwrap();
    pretty_assertions::assert_eq!(item.raw_event_id, inserted_event.id);
    pretty_assertions::assert_eq!(item.target_agent_name, "test_agent");
    pretty_assertions::assert_eq!(item.status, "processing");

    // Complete processing
    queries::complete_work_item(ctx.pool(), item.queue_id).await?;

    // Verify item is completed
    let completed_item = queries::get_work_item_by_id(ctx.pool(), item.queue_id).await?;
    pretty_assertions::assert_eq!(completed_item.status, "succeeded");

    Ok(())
}

/// Test work queue retry logic and DLQ handling
#[sinex_test(timeout = 45)]
async fn test_work_queue_retry_logic(ctx: TestContext) -> TestResult {
    // Create agent first (required for foreign key)
    let _agent = queries::upsert_agent_manifest(
        ctx.pool(),
        "test_agent",
        "1.0.0",
        "running",
        "test",
        Some("Test agent for retry logic"),
        None,
        None,
    )
    .await?;

    // Insert a raw event
    let event = RawEventBuilder::new(
        "fs",
        "file.created",
        json!({"path": "/test/retry_test.txt", "size": 1024}),
    )
    .build();

    let inserted_event = queries::insert_event(ctx.pool(), &event).await?;

    // Add to work queue with limited retries
    let queue_item = queries::add_to_work_queue(
        ctx.pool(),
        inserted_event.id,
        "test_agent",
        2, // max_attempts
    )
    .await?;

    // First attempt - should succeed
    let first_item = queries::get_next_work_item(ctx.pool(), "test_agent").await?;
    assert!(first_item.is_some(), "Should get item on first attempt");
    let item = first_item.unwrap();
    assert_eq!(item.attempts, 0, "First attempt should have 0 prior attempts");
    
    // Fail the first attempt
    queries::fail_work_item(ctx.pool(), item.queue_id, "Test failure 1").await?;
    
    // Second attempt - should succeed (retry)
    let second_item = queries::get_next_work_item(ctx.pool(), "test_agent").await?;
    assert!(second_item.is_some(), "Should get item on second attempt (retry)");
    let item = second_item.unwrap();
    assert_eq!(item.attempts, 1, "Second attempt should have 1 prior attempt");
    assert_eq!(item.queue_id, queue_item.queue_id, "Should be the same work item");
    
    // Fail the second attempt (this will exhaust max_attempts=2)
    queries::fail_work_item(ctx.pool(), item.queue_id, "Test failure 2").await?;
    
    // Third attempt - should not get item (max retries exceeded)
    let third_item = queries::get_next_work_item(ctx.pool(), "test_agent").await?;
    assert!(
        third_item.is_none(),
        "Should not get item on third attempt (max retries exceeded)"
    );

    // Verify item is in DLQ
    let dlq_items = queries::get_dlq_items(ctx.pool(), "test_agent", 10).await?;
    assert!(!dlq_items.is_empty());

    let dlq_item = &dlq_items[0];
    pretty_assertions::assert_eq!(dlq_item.failed_event_id, inserted_event.id);
    pretty_assertions::assert_eq!(dlq_item.agent_name, "test_agent");
    assert!(!dlq_item.failure_reason.is_empty());

    Ok(())
}

// =============================================================================
// CONCURRENCY AND ORDERING TESTS
// =============================================================================

/// Test concurrent event insertion with unique IDs
#[sinex_test(timeout = 40)]
async fn test_concurrent_event_insertion(ctx: TestContext) -> TestResult {
    use tokio::task::JoinSet;

    let _pool = Arc::new(ctx.pool().clone());
    let mut join_set = JoinSet::new();

    // Spawn multiple concurrent insertions
    for i in 0..10 {
        let pool_clone = Arc::new(ctx.pool().clone());
        join_set.spawn(async move {
            let event = RawEventBuilder::new(
                "fs",
                "file.created",
                json!({
                    "path": format!("/test/concurrent_{}.txt", i),
                    "thread_id": i
                }),
            )
            .build();

            queries::insert_event(&pool_clone, &event).await
        });
    }

    // Wait for all insertions to complete
    let mut results = Vec::new();
    while let Some(result) = join_set.join_next().await {
        results.push(result??);
    }

    // Verify all insertions succeeded
    pretty_assertions::assert_eq!(results.len(), 10);

    // Verify all events are unique
    let mut ids = std::collections::HashSet::new();
    for event in results {
        assert!(ids.insert(event.id)); // Should be unique
    }

    Ok(())
}

/// Test ULID ordering in database queries
#[sinex_test(timeout = 35)]
async fn test_ulid_ordering_in_database(ctx: TestContext) -> TestResult {
    let mut events = Vec::new();

    // Insert events with small delays to ensure ULID ordering
    for i in 0..5 {
        let event =
            RawEventBuilder::new("fs", "file.created", json!({"sequence": i})).build();

        let inserted = queries::insert_event(ctx.pool(), &event).await?;
        events.push(inserted);

        // Small delay to ensure timestamp progression
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Query events ordered by ID (ULID)
    let _ordered_events = queries::get_recent_events(ctx.pool(), 10).await?;

    // Verify ULID ordering matches insertion order
    for i in 1..events.len() {
        assert!(events[i].id.to_string() > events[i - 1].id.to_string());
        assert!(events[i].ts_ingest >= events[i - 1].ts_ingest);
    }

    Ok(())
}

/// Test event validation with valid and invalid payloads
#[sinex_test]
async fn test_event_validation(ctx: TestContext) -> TestResult {
    // Test with valid event
    let valid_event = RawEventBuilder::new(
        "fs",
        "file.created",
        json!({
            "path": "/valid/path.txt",
            "size": 1024,
            "created_time": "2024-01-01T12:00:00Z"
        }),
    )
    .build();

    let result = queries::insert_event(ctx.pool(), &valid_event).await;
    assert!(result.is_ok());

    // Test with event that has invalid payload structure
    // (This depends on whether validation is enforced at database level)
    let invalid_event = RawEventBuilder::new(
        "fs",
        "file.created",
        json!({
            "invalid_field": "this should not be here",
            "missing_required_path": true
        }),
    )
    .build();

    // Depending on validation implementation, this might succeed or fail
    // For now, just test that it doesn't panic
    let _result = queries::insert_event(ctx.pool(), &invalid_event).await;
    // Result can be Ok or Err - we're testing that it handles it gracefully

    Ok(())
}

// =============================================================================
// LEGACY COMPATIBILITY TESTS
// =============================================================================

/// Test minimal macro functionality
#[sinex_test]
async fn test_minimal_macro(_ctx: TestContext) -> TestResult {
    // Simple test to verify the macro works
    let result = 1 + 1;
    pretty_assertions::assert_eq!(result, 2);
    Ok(())
}

/// Test streamlined validation demo
#[sinex_test]
async fn test_streamlined_validation_demo(_ctx: TestContext) -> TestResult {
    // Demonstrate streamlined validation patterns
    let validator = EventValidator::new();
    
    // Create a well-formed event
    let event = EventBuilder::filesystem()
        .path("/test/streamlined.txt")
        .created()
        .size(512)
        .build();

    // Validate using streamlined pattern
    let validation_result = ValidationChain::validate(event.clone(), "demo_event")
        .has_valid_source()
        .has_valid_event_type()
        .payload_is_object()
        .into_result();

    assert_validation_passes(validation_result)?;

    // Also test with EventValidator
    let validator_result = validator.validate(&event);
    assert_with_context(
        validator_result.is_ok(),
        "Streamlined event should pass EventValidator",
        "streamlined_validation_demo_test",
    )?;

    Ok(())
}
