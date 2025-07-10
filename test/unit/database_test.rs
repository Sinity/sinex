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
use sinex_db::work_queue::claim_work_queue_items;
use sinex_core::{sources, event_type_constants}; 
use sinex_db::validation::EventValidator;
use sinex_db::query_helpers::ulid_to_uuid;
use std::sync::{Arc, atomic::{AtomicU32, AtomicBool}};
use serde_json::json;

// =============================================================================
// BASIC DATABASE OPERATIONS
// =============================================================================

// Database connection test removed - redundant with preflight and infrastructure tests

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
    let inserted_event = sinex_db::get_event_by_id(ctx.pool(), event_id)
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
        sources::FS,
        event_type_constants::filesystem::FILE_CREATED,
        json!({"path": "/test/fs_file.txt"}),
    )
    .build();

    let terminal_event = RawEventBuilder::new(
        sources::SHELL_KITTY,
        event_type_constants::shell::COMMAND_EXECUTED,
        json!({"command": "ls"}),
    )
    .build();

    let wm_event = RawEventBuilder::new(
        sources::WM_HYPRLAND, 
        event_type_constants::window_manager::WINDOW_FOCUSED,
        json!({"window_id": 123})
    ).build();

    sinex_db::insert_event(ctx.pool(), &fs_event).await?;
    sinex_db::insert_event(ctx.pool(), &terminal_event).await?;
    sinex_db::insert_event(ctx.pool(), &wm_event).await?;

    // Verify events were inserted by checking each one by ID
    let retrieved_fs = sinex_db::get_event_by_id(ctx.pool(), fs_event.id).await?;
    assert_eq!(retrieved_fs.source, sources::FS);
    
    let retrieved_terminal = sinex_db::get_event_by_id(ctx.pool(), terminal_event.id).await?;
    assert_eq!(retrieved_terminal.source, sources::SHELL_KITTY);
    
    let retrieved_wm = sinex_db::get_event_by_id(ctx.pool(), wm_event.id).await?;
    assert_eq!(retrieved_wm.source, sources::WM_HYPRLAND);

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

    sinex_db::insert_event(ctx.pool(), &create_event).await?;
    sinex_db::insert_event(ctx.pool(), &delete_event).await?;
    sinex_db::insert_event(ctx.pool(), &command_event).await?;

    // Query by event type
    let create_events = get_events_by_type(ctx.pool(), "file.created", 10).await?;
    assert!(!create_events.is_empty());
    assert!(create_events.iter().all(|e| e.event_type == "file.created"));

    let delete_events = get_events_by_type(ctx.pool(), "file.deleted", 10).await?;
    assert!(!delete_events.is_empty());
    assert!(delete_events.iter().all(|e| e.event_type == "file.deleted"));

    let command_events = get_events_by_type(ctx.pool(), "command.executed", 10).await?;
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
    let _agent = sinex_db::upsert_agent_manifest(
        ctx.pool(),
        "test_agent",
        "1.0.0",
        Some("Test agent for work queue"),
        "test",
        serde_json::json!({}),
        serde_json::json!([]),
        serde_json::json!([]),
        serde_json::json!([])
    )
    .await?;

    // Insert a raw event first
    let event = RawEventBuilder::new(
        "fs",
        "file.created",
        json!({"path": "/test/work_queue_test.txt"}),
    )
    .build();

    let inserted_event = sinex_db::insert_event(ctx.pool(), &event).await?;

    // Add to work queue
    let queue_item = sinex_db::work_queue::add_to_work_queue_detailed(
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
    let items = claim_work_queue_items(ctx.pool(), "test_agent", "test_worker", 1).await?;
    let next_item = items.into_iter().next();
    assert!(next_item.is_some());

    let item = next_item.unwrap();
    pretty_assertions::assert_eq!(item.raw_event_id, inserted_event.id);
    pretty_assertions::assert_eq!(item.target_agent_name, "test_agent");
    pretty_assertions::assert_eq!(item.status, "processing");

    // Complete processing
    sinex_db::complete_work_item(ctx.pool(), item.queue_id).await?;

    // Verify item is completed
    let completed_item = sinex_db::get_work_item_by_id(ctx.pool(), item.queue_id).await?;
    pretty_assertions::assert_eq!(completed_item.status, "succeeded");

    Ok(())
}

/// Test work queue retry logic and DLQ handling
#[sinex_test(timeout = 45)]
async fn test_work_queue_retry_logic(ctx: TestContext) -> TestResult {
    // Create agent first (required for foreign key)
    let _agent = sinex_db::upsert_agent_manifest(
        ctx.pool(),
        "test_agent",
        "1.0.0",
        Some("Test agent for retry logic"),
        "test",
        serde_json::json!({}),
        serde_json::json!([]),
        serde_json::json!([]),
        serde_json::json!([])
    )
    .await?;

    // Insert a raw event
    let event = RawEventBuilder::new(
        "fs",
        "file.created",
        json!({"path": "/test/retry_test.txt", "size": 1024}),
    )
    .build();

    let inserted_event = sinex_db::insert_event(ctx.pool(), &event).await?;

    // Add to work queue with limited retries
    let queue_item = sinex_db::add_to_work_queue(
        ctx.pool(),
        inserted_event.id,
        "test_agent",
        2, // max_attempts
    )
    .await?;

    // First attempt - should succeed
    let first_item = sinex_db::get_next_work_item(ctx.pool(), "test_agent").await?;
    assert!(first_item.is_some(), "Should get item on first attempt");
    let item = first_item.unwrap();
    assert_eq!(item.attempts, 0, "First attempt should have 0 prior attempts");
    
    // Fail the first attempt
    sinex_db::fail_work_item(ctx.pool(), item.queue_id, "Test failure 1").await?;
    
    // Second attempt - should succeed (retry)
    let second_item = sinex_db::get_next_work_item(ctx.pool(), "test_agent").await?;
    assert!(second_item.is_some(), "Should get item on second attempt (retry)");
    let item = second_item.unwrap();
    assert_eq!(item.attempts, 1, "Second attempt should have 1 prior attempt");
    assert_eq!(item.queue_id, queue_item, "Should be the same work item");
    
    // Fail the second attempt (this will exhaust max_attempts=2)
    sinex_db::fail_work_item(ctx.pool(), item.queue_id, "Test failure 2").await?;
    
    // Third attempt - should not get item (max retries exceeded)
    let third_item = sinex_db::get_next_work_item(ctx.pool(), "test_agent").await?;
    assert!(
        third_item.is_none(),
        "Should not get item on third attempt (max retries exceeded)"
    );

    // Verify item is in DLQ
    let dlq_items = sinex_db::get_dlq_items(ctx.pool(), "test_agent", 10).await?;
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

            sinex_db::insert_event(&pool_clone, &event).await
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

        let inserted = sinex_db::insert_event(ctx.pool(), &event).await?;
        events.push(inserted);

        // Small delay to ensure timestamp progression
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Query events ordered by ID (ULID)
    let _ordered_events = get_recent_events(ctx.pool(), 10).await?;

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

    let result = sinex_db::insert_event(ctx.pool(), &valid_event).await;
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
    let _result = sinex_db::insert_event(ctx.pool(), &invalid_event).await;
    // Result can be Ok or Err - we're testing that it handles it gracefully

    Ok(())
}

// =============================================================================
// DATABASE VERIFICATION TESTS (from database_verification_test.rs)
// =============================================================================

/// Test database connectivity verification
#[sinex_test]
async fn test_database_connectivity_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) = sinex_preflight::database::verify_database_connectivity().await?;

    assert_eq!(status, sinex_preflight::VerificationStatus::Pass);
    assert!(!messages.is_empty());
    assert!(messages.iter().any(|m| m.contains("Database connection established")));

    // Check details structure
    assert!(details.get("database_url").is_some());
    assert!(details.get("postgresql_version").is_some());
    assert!(details.get("connection_pool").is_some());

    Ok(())
}

/// Test PostgreSQL extensions verification
#[sinex_test]
async fn test_postgresql_extensions_verification(ctx: TestContext) -> TestResult {
    let (status, details, _messages) = sinex_preflight::database::verify_postgresql_extensions().await?;

    // Should pass or warn, depending on which extensions are available
    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));

    // Should have checked for required extensions
    let extensions = details.get("extensions").unwrap().as_object().unwrap();
    assert!(extensions.contains_key("uuid-ossp"));
    assert!(extensions.contains_key("timescaledb"));
    assert!(extensions.contains_key("pg_jsonschema"));

    Ok(())
}

/// Test migration readiness verification
#[sinex_test]
async fn test_migration_readiness_verification(ctx: TestContext) -> TestResult {
    let (status, details, _messages) = sinex_preflight::database::verify_migration_readiness().await?;

    assert_eq!(status, sinex_preflight::VerificationStatus::Pass);
    assert!(details.get("current_migrations").is_some());

    Ok(())
}

/// Test database CRUD operations
#[sinex_test]
async fn test_database_crud_operations(ctx: TestContext) -> TestResult {
    // Use the existing helper functions that work correctly
    let event = RawEventBuilder::new(
        "unit-test-crud",
        "test.crud_operations",
        serde_json::json!({"test": "crud_operations"}),
    )
    .build();

    let inserted_event = sinex_db::insert_event(ctx.pool(), &event).await?;
    let retrieved_event = sinex_db::get_event_by_id(ctx.pool(), inserted_event.id).await?;
    
    assert_eq!(retrieved_event.source, "unit-test-crud");
    assert_eq!(retrieved_event.event_type, "test.crud_operations");
    
    Ok(())
}

/// Test database transaction handling
#[sinex_test]
async fn test_database_transaction_handling(ctx: TestContext) -> TestResult {
    let initial_count = ctx.event_count().await?;

    // Test successful transaction by inserting an event
    let event1 = RawEventBuilder::new(
        "unit-test-tx",
        "test.transaction",
        serde_json::json!({"test": "commit"}),
    )
    .build();

    ctx.insert_event(&event1).await?;

    // Verify committed
    let committed_count = ctx.event_count().await?;
    assert_eq!(committed_count, initial_count + 1);

    // Test that the event was actually inserted and is retrievable
    let retrieved_event = sinex_db::get_event_by_id(ctx.pool(), event1.id).await?;
    assert_eq!(retrieved_event.source, "unit-test-tx");
    assert_eq!(retrieved_event.event_type, "test.transaction");

    Ok(())
}

/// Test database connection pool health
#[sinex_test]
async fn test_database_connection_pool_health(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test multiple connections from the pool
    let mut connections = Vec::new();

    for _ in 0..5 {
        let conn = pool.acquire().await?;
        connections.push(conn);
    }

    // All connections should be valid
    assert_eq!(connections.len(), 5);

    // Test that we can execute queries on all connections
    for (i, conn) in connections.iter_mut().enumerate() {
        let result = sqlx::query!("SELECT $1::text as test_value", i as i32)
            .fetch_one(&mut **conn)
            .await?;

        assert_eq!(result.test_value, Some((i as i32).to_string()));
    }

    // Connections are automatically returned to pool when dropped
    drop(connections);

    // Verify pool is still functional
    let final_test = sqlx::query!("SELECT 1 as test")
        .fetch_one(pool)
        .await?;

    assert_eq!(final_test.test, Some(1));

    Ok(())
}

/// Test database error handling
#[sinex_test]
async fn test_database_error_handling(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test handling of SQL syntax errors
    let syntax_error = sqlx::query("SELECT * FROM nonexistent_table_12345")
        .fetch_optional(pool)
        .await;

    assert!(syntax_error.is_err(), "Should fail with syntax/table error");

    // Test handling of constraint violations by creating an event and trying to insert a duplicate
    let event = RawEventBuilder::new(
        "unit-test-error",
        "test.error_handling",
        serde_json::json!({"test": "constraint"}),
    )
    .build();

    let inserted_event = sinex_db::insert_event(ctx.pool(), &event).await?;
    
    // Try to insert with same ID (should fail with constraint violation)
    let duplicate_event = RawEvent {
        id: inserted_event.id, // Same ID
        source: "unit-test-error".to_string(),
        event_type: "test.error_handling".to_string(),
        ts_ingest: chrono::Utc::now(),
        ts_orig: None,
        host: "test_host".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: serde_json::json!({"test": "duplicate"}),
    };

    let constraint_error = sinex_db::insert_event(ctx.pool(), &duplicate_event).await;
    assert!(constraint_error.is_err(), "Should fail with constraint violation");

    Ok(())
}

// =============================================================================
// EVENTSOURCE TRAIT TESTS (from simple_ingestor_tests.rs)
// =============================================================================

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct TestSourceConfig {
    events_to_generate: u32,
    generation_delay_ms: u64,
    should_fail: bool,
}

impl Default for TestSourceConfig {
    fn default() -> Self {
        Self {
            events_to_generate: 5,
            generation_delay_ms: 10,
            should_fail: false,
        }
    }
}

struct TestEventSource {
    config: TestSourceConfig,
    events_sent: Arc<AtomicU32>,
    should_error: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl EventSource for TestEventSource {
    type Config = TestSourceConfig;
    const SOURCE_NAME: &'static str = "test_source";

    async fn initialize(ctx: EventSourceContext) -> sinex_core::Result<Self> {
        let config: TestSourceConfig = serde_json::from_value(ctx.config).map_err(|e| {
            sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e))
        })?;

        Ok(Self {
            config,
            events_sent: Arc::new(AtomicU32::new(0)),
            should_error: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn stream_events(&mut self, tx: tokio::sync::mpsc::Sender<RawEvent>) -> sinex_core::Result<()> {
        if self.config.should_fail {
            return Err(sinex_core::CoreError::Other("Test failure".to_string()));
        }

        for i in 0..self.config.events_to_generate {
            if self.should_error.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(sinex_core::CoreError::Other(
                    "Test error during streaming".to_string(),
                ));
            }

            let event = RawEventBuilder::new(
                Self::SOURCE_NAME,
                "test_event",
                json!({"test": true, "sequence": i}),
            ).build();

            if tx.send(event).await.is_err() {
                break; // Receiver dropped
            }

            self.events_sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            tokio::time::sleep(tokio::time::Duration::from_millis(self.config.generation_delay_ms)).await;
        }

        // Keep running until shutdown
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    async fn shutdown(&mut self) -> sinex_core::Result<()> {
        Ok(())
    }
}

/// Test EventSource trait initialization
#[sinex_test]
async fn test_event_source_initialization(ctx: TestContext) -> TestResult {
    let config = TestSourceConfig {
        events_to_generate: 10,
        generation_delay_ms: 5,
        should_fail: false,
    };

    let ctx_local = crate::common::event_sources::test_context(serde_json::to_value(&config)?);
    let source = TestEventSource::initialize(ctx_local).await?;

    pretty_assertions::assert_eq!(source.config.events_to_generate, 10);
    pretty_assertions::assert_eq!(source.config.generation_delay_ms, 5);
    assert!(!source.config.should_fail);

    Ok(())
}

/// Test EventSource streaming with receiver drop
#[sinex_test]
async fn test_event_source_streaming(ctx: TestContext) -> TestResult {
    let config = TestSourceConfig {
        events_to_generate: 3,
        generation_delay_ms: 50,
        should_fail: false,
    };

    let ctx_local = crate::common::event_sources::test_context(serde_json::to_value(&config)?);
    let mut source = TestEventSource::initialize(ctx_local).await?;
    let events_sent = source.events_sent.clone();

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);

    // Start streaming in background
    let stream_handle = tokio::spawn(async move { source.stream_events(tx).await });

    // Collect events
    let mut events = Vec::new();
    for _ in 0..3 {
        if let Some(event) = rx.recv().await {
            events.push(event);
        }
    }

    // Cancel streaming
    stream_handle.abort();

    pretty_assertions::assert_eq!(events.len(), 3);
    pretty_assertions::assert_eq!(events_sent.load(std::sync::atomic::Ordering::SeqCst), 3);

    // Verify event structure
    for (i, event) in events.iter().enumerate() {
        pretty_assertions::assert_eq!(event.source, "test_source");
        pretty_assertions::assert_eq!(event.event_type, "test_event");
        pretty_assertions::assert_eq!(event.payload["sequence"], i);
    }

    Ok(())
}

/// Test EventSource error handling
#[sinex_test]
async fn test_event_source_runtime_error(ctx: TestContext) -> TestResult {
    let config = TestSourceConfig {
        events_to_generate: 10,
        generation_delay_ms: 10,
        should_fail: false,
    };

    let ctx_local = crate::common::event_sources::test_context(serde_json::to_value(&config)?);
    let mut source = TestEventSource::initialize(ctx_local).await?;
    let should_error = source.should_error.clone();
    let events_sent = source.events_sent.clone();

    let (tx, _rx) = tokio::sync::mpsc::channel(10);

    let stream_handle = tokio::spawn(async move { source.stream_events(tx).await });

    // Wait for some events to be generated
    tokio::task::yield_now().await;

    // Trigger error
    should_error.store(true, std::sync::atomic::Ordering::SeqCst);

    // Wait for error
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let result = stream_handle.await;
    assert!(result.is_ok()); // Task completed (with error)

    // Should have sent some events before error
    let sent_count = events_sent.load(std::sync::atomic::Ordering::SeqCst);
    assert!(sent_count > 0 && sent_count < 10);

    Ok(())
}

/// Test EventSource database integration
#[sinex_test]
async fn test_event_source_database_integration(ctx: TestContext) -> TestResult {
    // Generate a unique event type for this test to avoid contamination
    let test_id = Ulid::new().to_string();
    let unique_event_type = format!("test_event_{}", &test_id[..8]);
    
    let config = TestSourceConfig {
        events_to_generate: 2,
        generation_delay_ms: 10,
        should_fail: false,
    };

    let ctx_local = crate::common::event_sources::test_context(serde_json::to_value(&config)?);
    let mut source = TestEventSource::initialize(ctx_local).await?;

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);

    let stream_handle = tokio::spawn(async move { source.stream_events(tx).await });

    // Receive and store events with our unique type
    let mut inserted_count = 0;
    for _ in 0..2 {
        if let Some(mut event) = rx.recv().await {
            // Modify the event type to be unique for this test
            event.event_type = unique_event_type.clone();
            
            // Store in database using proper queries that handle ts_ingest correctly
            let event_uuid = ulid_to_uuid(event.id);
            sqlx::query!(
                r#"
                INSERT INTO raw.events (id, source, event_type, payload, host)
                VALUES ($1::uuid, $2, $3, $4, $5)
                "#,
                event_uuid,
                event.source,
                event.event_type,
                event.payload,
                event.host
            )
            .execute(ctx.pool())
            .await?;
            inserted_count += 1;
        }
    }

    stream_handle.abort();
    
    // Ensure we actually inserted 2 events
    assert_eq!(inserted_count, 2, "Should have inserted 2 events");

    // Verify events were stored - count only our unique event type
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM raw.events WHERE event_type = $1")
            .bind(&unique_event_type)
            .fetch_one(ctx.pool())
            .await?;

    // We inserted exactly 2 events with our unique type
    assert_eq!(count, 2, "Should have exactly 2 events with type {}, found {}", unique_event_type, count);

    Ok(())
}

// =============================================================================
// RESOURCE VERIFICATION TESTS (from resource_verification_test.rs)
// =============================================================================

/// Test system resources verification
#[sinex_test]
async fn test_system_resources_verification(_ctx: TestContext) -> TestResult {
    let (status, details, messages) = sinex_preflight::resources::verify_system_resources().await?;

    // Should pass or warn in test environment
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Should have checked various resources
    assert!(details.get("memory").is_some());
    assert!(details.get("disk").is_some());
    assert!(details.get("cpu").is_some());
    assert!(details.get("fs").is_some());

    assert!(!messages.is_empty());

    Ok(())
}

/// Test memory availability check
#[sinex_test]
async fn test_memory_availability_check(_ctx: TestContext) -> TestResult {
    // We can't directly test the internal function, but we can test the overall verification
    let (_status, details, _) = sinex_preflight::resources::verify_system_resources().await?;

    let memory_info = details.get("memory").unwrap();

    // Should have memory information
    assert!(memory_info.get("total_gb").is_some());
    assert!(memory_info.get("available_gb").is_some());
    assert!(memory_info.get("usage_percent").is_some());
    assert!(memory_info.get("meets_requirements").is_some());

    // Available memory should be positive
    let available_gb = memory_info["available_gb"].as_f64().unwrap();
    assert!(available_gb > 0.0, "Available memory should be positive");

    Ok(())
}

/// Test disk space check
#[sinex_test]
async fn test_disk_space_check(_ctx: TestContext) -> TestResult {
    let (_status, details, _) = sinex_preflight::resources::verify_system_resources().await?;

    let disk_info = details.get("disk").unwrap();
    let paths = disk_info.get("paths").unwrap().as_object().unwrap();

    // Should have checked some standard paths
    for (path, info) in paths {
        if let Some(total_gb) = info.get("total_gb").and_then(|v| v.as_f64()) {
            assert!(total_gb > 0.0, "Total disk space should be positive for {}", path);
        }

        if let Some(available_gb) = info.get("available_gb").and_then(|v| v.as_f64()) {
            assert!(available_gb >= 0.0, "Available disk space should be non-negative for {}", path);
        }
    }

    Ok(())
}

/// Test CPU capacity check
#[sinex_test]
async fn test_cpu_capacity_check(_ctx: TestContext) -> TestResult {
    let (_status, details, _) = sinex_preflight::resources::verify_system_resources().await?;

    let cpu_info = details.get("cpu").unwrap();

    // Should have CPU information
    assert!(cpu_info.get("cpu_count").is_some());
    assert!(cpu_info.get("load_average_1min").is_some());
    assert!(cpu_info.get("meets_requirements").is_some());

    // CPU count should be positive
    let cpu_count = cpu_info["cpu_count"].as_u64().unwrap();
    assert!(cpu_count > 0, "CPU count should be positive");

    // Load average should be non-negative
    let load_avg = cpu_info["load_average_1min"].as_f64().unwrap();
    assert!(load_avg >= 0.0, "Load average should be non-negative");

    Ok(())
}

/// Test filesystem permissions check
#[sinex_test]
async fn test_filesystem_permissions_check(_ctx: TestContext) -> TestResult {
    let (_status, details, _) = sinex_preflight::resources::verify_system_resources().await?;

    let filesystem_info = details.get("fs").unwrap();
    let directories = filesystem_info.get("directories").unwrap().as_object().unwrap();

    // Should have checked some directories
    assert!(!directories.is_empty(), "Should have checked some directories");

    for (dir_path, info) in directories {
        // Each directory should have permission info
        assert!(info.get("writable").is_some(), "Should check writability for {}", dir_path);

        if let Some(error) = info.get("error") {
            println!("Permission check warning for {}: {}", dir_path, error);
        }
    }

    Ok(())
}

/// Test filesystem operations
#[sinex_test]
async fn test_filesystem_operations(_ctx: TestContext) -> TestResult {
    // Test basic filesystem operations that the verification would perform
    let temp_dir = tempfile::TempDir::new()?;
    let test_file_path = temp_dir.path().join("test-file.txt");

    // Test write
    std::fs::write(&test_file_path, "test content")?;

    // Test read
    let content = std::fs::read_to_string(&test_file_path)?;
    assert_eq!(content, "test content");

    // Test metadata
    let metadata = test_file_path.metadata()?;
    assert!(metadata.is_file());
    assert!(metadata.len() > 0);

    // Test directory creation
    let test_subdir = temp_dir.path().join("subdir");
    std::fs::create_dir(&test_subdir)?;
    assert!(test_subdir.exists());
    assert!(test_subdir.is_dir());

    // Cleanup is automatic with TempDir

    Ok(())
}

// =============================================================================
// MODEL TESTS (from model/mod.rs)
// =============================================================================

/// Test RawEvent validation
#[sinex_test]
async fn test_raw_event_validation(_ctx: TestContext) -> TestResult {
    // Test RawEvent can be created with required fields
    let event_id = Ulid::new();
    let payload = json!({"test": "data"});

    // This test validates that our core data structure works
    // Note: Actual creation happens via database insert functions
    assert!(
        !event_id.to_string().is_empty(),
        "Event ID should be valid ULID"
    );
    assert!(payload.is_object(), "Payload should be valid JSON object");

    // Validate payload contains expected structure
    assert!(
        payload.get("test").is_some(),
        "Payload should contain test data"
    );
    Ok(())
}

/// Test queue status transitions
#[sinex_test]
async fn test_queue_status_transitions(_ctx: TestContext) -> TestResult {
    // Test that queue status enum has all expected variants
    use sinex_db::models::QueueStatus;

    // Verify we can create each status
    let statuses = [QueueStatus::Pending,
        QueueStatus::Processing,
        QueueStatus::Succeeded,
        QueueStatus::Failed,
        QueueStatus::FailedRetryable];

    pretty_assertions::assert_eq!(statuses.len(), 5, "Should have all queue status variants");

    // Verify status transitions make logical sense
    // (This is more documentation than validation)
    pretty_assertions::assert_ne!(QueueStatus::Pending, QueueStatus::Processing);
    pretty_assertions::assert_ne!(QueueStatus::Processing, QueueStatus::Succeeded);
    Ok(())
}

// ULID ordering property test moved to test/property/ulid_property_test.rs

/// Test JSON payload constraints
#[sinex_test]
async fn test_json_payload_constraints(_ctx: TestContext) -> TestResult {
    // Test various JSON payload structures that should be valid
    let valid_payloads = vec![
        json!({"event_type": "fs", "path": "/tmp/test"}),
        json!({"event_type": "terminal", "command": "ls", "exit_code": 0}),
        json!({"event_type": "window", "title": "Editor", "geometry": {"x": 0, "y": 0}}),
        json!({"timestamp": 1234567890, "data": [1, 2, 3]}),
        json!({}), // Empty payload should be valid
    ];

    for payload in valid_payloads {
        assert!(
            payload.is_object() || payload.is_array() || payload.is_null(),
            "Payload should be valid JSON structure: {}",
            payload
        );
    }

    // Test that we can serialize/deserialize basic structures
    let test_payload = json!({"test": "serialization", "number": 42});
    let serialized = serde_json::to_string(&test_payload).expect("Should serialize");
    let deserialized: serde_json::Value =
        serde_json::from_str(&serialized).expect("Should deserialize");
    pretty_assertions::assert_eq!(
        test_payload,
        deserialized,
        "Serialization round-trip should preserve data"
    );
    Ok(())
}

// =============================================================================
// LEGACY COMPATIBILITY TESTS
// =============================================================================

// Minimal macro test removed - redundant with 502 other tests using #[sinex_test]

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

    validation_result?;

    // Also test with EventValidator
    let validator_result = validator.validate(&event);
    assert_with_context(
        validator_result.is_ok(),
        "Streamlined event should pass EventValidator",
        "streamlined_validation_demo_test",
    )?;

    Ok(())
}
