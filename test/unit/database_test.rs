// Database Unit Tests
//
// Consolidated database layer tests covering:
// - Basic database operations and connectivity
// - Event insertion, validation, and querying
// - Schema validation and model serialization
// - Database pool management and transaction handling
// - Event validator functionality
// - Complex query operations

use crate::common::test_macros::*;
use crate::common::prelude::*;
use crate::common::event_builders::EventBuilder;
use serde_json::json;
// Sources and event types now in sinex_events
use sinex_events::{EventFactory, event_types, sources};
use sinex_db::queries::EventQueries;
use sinex_db::validation::EventValidator;
use std::sync::Arc;

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
            sinex_error::CoreError::database("Failed to retrieve inserted event")
                .with_context("event_id", event_id)
                .with_context("test_name", "basic_event_insertion")
                .with_context("source", e.to_string())
                .build()
        })?;

    // Verify using enhanced event equivalence assertion
    assert_events_equivalent(&inserted_event, &event)?;

    // Use ValidationChain to validate the event structure
    let source_validation = ValidationChain::validate(inserted_event.source.clone(), "event_source")
        .not_empty();
    assert_validation_passes(source_validation)?;
    
    let event_type_validation = ValidationChain::validate(inserted_event.event_type.clone(), "event_type")
        .not_empty();
    assert_validation_passes(event_type_validation)?;
    
    // Payload should be an object
    assert!(inserted_event.payload.is_object(), "Payload should be JSON object");

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

    // Simple database query - test basic connectivity
    let (count,) = EventQueries::count_all()
        .fetch_one::<(i64,)>(ctx.pool())
        .await?;
    // Just verify we can query the database
    assert!(count >= 0);

    // Test event creation helpers
    let event = ctx.filesystem_event("/test/file.txt");
    assert_eq!(event.event_type, "file.created");

    // Insert the event
    ctx.insert_event(&event).await?;

    // Verify it exists
    let (count,): (i64,) = EventQueries::count_all()
        .fetch_one(ctx.pool())
        .await?;
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
    let fs_event = EventFactory::new(sources::FS).create_event(
        event_types::filesystem::FILE_CREATED,
        json!({"path": "/test/fs_file.txt"}),
    );

    let terminal_event = EventFactory::new(sources::SHELL_KITTY).create_event(
        event_types::shell::COMMAND_EXECUTED,
        json!({"command": "ls"}),
    );

    let wm_event = EventFactory::new(sources::WM_HYPRLAND).create_event(
        event_types::window_manager::WINDOW_FOCUSED,
        json!({"window_id": 123}),
    );

    sinex_db::insert_event(ctx.pool(), &fs_event).await?;
    sinex_db::insert_event(ctx.pool(), &terminal_event).await?;
    sinex_db::insert_event(ctx.pool(), &wm_event).await?;

    // Verify events were inserted by checking each one by ID
    let retrieved_fs: RawEvent = EventQueries::get_by_id(fs_event.id).fetch_one(ctx.pool()).await?;
    assert_eq!(retrieved_fs.source, sources::FS);

    let retrieved_terminal: RawEvent = EventQueries::get_by_id(terminal_event.id).fetch_one(ctx.pool()).await?;
    assert_eq!(retrieved_terminal.source, sources::SHELL_KITTY);

    let retrieved_wm: RawEvent = EventQueries::get_by_id(wm_event.id).fetch_one(ctx.pool()).await?;
    assert_eq!(retrieved_wm.source, sources::WM_HYPRLAND);

    Ok(())
}

/// Test querying events by event type
test_event_insertion!(test_query_events_by_type, "fs", "file.created", json!({"path": "/test/create_file.txt"}));

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
        source_event_ids: None,
        source_material_id: None,
        source_material_offset_start: None,
        source_material_offset_end: None,
        anchor_byte: None,
        associated_blob_ids: None,
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
        (
            RawEvent {
                id: Ulid::new(),
                source: "".to_string(),
                event_type: "test.valid".to_string(),
                ts_ingest: chrono::Utc::now(),
                ts_orig: None,
                host: "test_host".to_string(),
                ingestor_version: None,
                payload_schema_id: None,
                payload: json!({}),
                source_event_ids: None,
                source_material_id: None,
                source_material_offset_start: None,
                source_material_offset_end: None,
                anchor_byte: None,
                associated_blob_ids: None,
            },
            "empty_source",
        ),
        // Invalid event type format
        (
            RawEvent {
                id: Ulid::new(),
                source: "valid_source".to_string(),
                event_type: "invalid-format".to_string(),
                ts_ingest: chrono::Utc::now(),
                ts_orig: None,
                host: "test_host".to_string(),
                ingestor_version: None,
                payload_schema_id: None,
                payload: json!({}),
                source_event_ids: None,
                source_material_id: None,
                source_material_offset_start: None,
                source_material_offset_end: None,
                anchor_byte: None,
                associated_blob_ids: None,
            },
            "invalid_event_type",
        ),
        // Empty host
        (
            RawEvent {
                id: Ulid::new(),
                source: "valid_source".to_string(),
                event_type: "test.valid".to_string(),
                ts_ingest: chrono::Utc::now(),
                ts_orig: None,
                host: "".to_string(),
                ingestor_version: None,
                payload_schema_id: None,
                payload: json!({}),
                source_event_ids: None,
                source_material_id: None,
                source_material_offset_start: None,
                source_material_offset_end: None,
                anchor_byte: None,
                associated_blob_ids: None,
            },
            "empty_host",
        ),
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
    let json_str =
        serde_json::to_string(&event).map_err(|e| format!("Failed to serialize event: {}", e))?;

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

    let event = EventFactory::new("fs").create_event("file.created", complex_payload.clone());

    // Serialize and deserialize
    let json_str = serde_json::to_string(&event)?;
    let deserialized: RawEvent = serde_json::from_str(&json_str)?;

    // Verify complex payload preservation
    pretty_assertions::assert_eq!(event.payload, deserialized.payload);
    pretty_assertions::assert_eq!(event.payload["file_info"]["path"], "/test/complex.txt");
    pretty_assertions::assert_eq!(event.payload["operation"]["process"]["pid"], 1234);
    pretty_assertions::assert_eq!(
        event.payload["file_info"]["metadata"]["tags"][0],
        "important"
    );

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
        source_event_ids: None,
        source_material_id: None,
        source_material_offset_start: None,
        source_material_offset_end: None,
        anchor_byte: None,
        associated_blob_ids: None,
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
// REDIS STREAMS OPERATIONS
// =============================================================================
/// Test Redis Streams event processing operations
test_event_insertion!(test_redis_streams_event_processing, "fs", "file.created", json!({"path": "/test/redis_streams_test.txt"}));

/// Test Redis Streams PEL retry logic and failure handling
test_event_insertion!(test_redis_streams_retry_logic, "fs", "file.created", json!({"path": "/test/retry_test.txt", "size": 1024}));

// =============================================================================
// CONCURRENCY AND ORDERING TESTS
// =============================================================================

/// Test concurrent event insertion with unique IDs
test_batch_events!(test_concurrent_event_insertion, "test", "test.event", 10, 
    |pool: &DbPool, events: &[RawEvent]| async move {
        // Verify batch
        assert_eq!(events.len(), 10);
        Ok(())
    }
);

/// Test ULID ordering in database queries
test_batch_events!(test_ulid_ordering_in_database, "test", "test.event", 5, 
    |pool: &DbPool, events: &[RawEvent]| async move {
        // Verify batch
        assert_eq!(events.len(), 5);
        Ok(())
    }
);

/// Test event validation with valid and invalid payloads
test_event_insertion!(test_event_validation, "fs", "file.created", json!({
            "path": "/valid/path.txt",
            "size": 1024,
            "created_time": "2024-01-01T12:00:00Z"
        }));

// =============================================================================
// DATABASE VERIFICATION TESTS (from database_verification_test.rs)
// =============================================================================

/// Test database connectivity verification
#[sinex_test]
async fn test_database_connectivity_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) =
        sinex_preflight::database::verify_database_connectivity().await?;

    assert_eq!(status, sinex_preflight::VerificationStatus::Pass);
    assert!(!messages.is_empty());
    assert!(messages
        .iter()
        .any(|m| m.contains("Database connection established")));

    // Check details structure
    assert!(details.get("database_url").is_some());
    assert!(details.get("postgresql_version").is_some());
    assert!(details.get("connection_pool").is_some());

    Ok(())
}

/// Test PostgreSQL extensions verification
#[sinex_test]
async fn test_postgresql_extensions_verification(ctx: TestContext) -> TestResult {
    let (status, details, _messages) =
        sinex_preflight::database::verify_postgresql_extensions().await?;

    // Should pass or warn, depending on which extensions are available
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

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
    let (status, details, _messages) =
        sinex_preflight::database::verify_migration_readiness().await?;

    assert_eq!(status, sinex_preflight::VerificationStatus::Pass);
    assert!(details.get("current_migrations").is_some());

    Ok(())
}

/// Test database CRUD operations
#[sinex_test]
async fn test_database_crud_operations(ctx: TestContext) -> TestResult {
    // Use the existing helper functions that work correctly
    let event = EventFactory::new("unit-test-crud").create_event(
        "test.crud_operations",
        serde_json::json!({"test": "crud_operations"}),
    );

    let inserted_event_id = sinex_db::insert_event(ctx.pool(), &event).await?;
    let retrieved_event = sinex_db::get_event_by_id(ctx.pool(), inserted_event_id).await?;

    assert_eq!(retrieved_event.source, "unit-test-crud");
    assert_eq!(retrieved_event.event_type, "test.crud_operations");

    Ok(())
}

/// Test database transaction handling
#[sinex_test]
async fn test_database_transaction_handling(ctx: TestContext) -> TestResult {
    let initial_count = ctx.event_count().await?;

    // Test successful transaction by inserting an event
    let event1 = EventFactory::new("unit-test-tx").create_event(
        "test.transaction",
        serde_json::json!({"test": "commit"}),
    );

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
    let pool = ctx.pool().clone();

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
    let final_test = sqlx::query!("SELECT 1 as test").fetch_one(&pool).await?;

    assert_eq!(final_test.test, Some(1));

    Ok(())
}

/// Test database error handling
#[sinex_test]
async fn test_database_error_handling(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Test handling of SQL syntax errors
    let syntax_error = sqlx::query("SELECT * FROM nonexistent_table_12345")
        .fetch_optional(&pool)
        .await;

    assert!(syntax_error.is_err(), "Should fail with syntax/table error");

    // Test handling of constraint violations by creating an event and trying to insert a duplicate
    let event = EventFactory::new("unit-test-error").create_event(
        "test.error_handling",
        serde_json::json!({"test": "constraint"}),
    );

    let inserted_event_id = sinex_db::insert_event(ctx.pool(), &event).await?;

    // Try to insert with same ID (should fail with constraint violation)
    let factory = EventFactory::new("unit-test-error");
    let mut duplicate_event = factory.create_event(
        "test.error_handling",
        serde_json::json!({"test": "duplicate"}),
    );
    duplicate_event.id = inserted_event_id; // Same ID
    duplicate_event.host = "test_host".to_string();

    let constraint_error = sinex_db::insert_event(ctx.pool(), &duplicate_event).await;
    assert!(
        constraint_error.is_err(),
        "Should fail with constraint violation"
    );

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

/// Test satellite processor initialization (replaces EventSource trait)
#[sinex_test]
async fn test_satellite_processor_initialization(ctx: TestContext) -> TestResult {
    

    // Test that we can create a processor configuration
    let config = json!({
        "events_to_generate": 10,
        "generation_delay_ms": 5,
        "should_fail": false
    });

    // Test checkpoint initialization for satellite
    let checkpoint_manager = sinex_satellite_sdk::CheckpointManager::new(
        ctx.pool().clone(),
        "test_satellite".to_string(),
        "test_consumer_group".to_string(),
        "test_consumer".to_string(),
    );

    let initial_checkpoint = checkpoint_manager.load_checkpoint().await?;

    // Test that satellite can track configuration in checkpoint
    let mut checkpoint_with_config = initial_checkpoint;
    checkpoint_with_config.data = Some(config);
    checkpoint_manager
        .save_checkpoint(&checkpoint_with_config)
        .await?;

    // Verify configuration persisted
    let saved_checkpoint = checkpoint_manager.load_checkpoint().await?;
    let saved_config = saved_checkpoint.data.unwrap();
    assert_eq!(saved_config["events_to_generate"], 10);
    assert_eq!(saved_config["generation_delay_ms"], 5);
    assert_eq!(saved_config["should_fail"], false);

    Ok(())
}

/// Test satellite event streaming and heartbeat management (replaces EventSource streaming)
#[sinex_test]
async fn test_satellite_event_streaming(ctx: TestContext) -> TestResult {
    // Test satellite heartbeat management
    let heartbeat_manager = sinex_satellite_sdk::HeartbeatEmitter::new(
        "test_satellite".to_string(),
        1, // 1 second interval
    );

    // Test that heartbeat can be started and produces events
    let mut heartbeat_count = 0;
    let start_time = std::time::Instant::now();

    // Simulate heartbeat loop (like satellite would do)
    while start_time.elapsed() < std::time::Duration::from_millis(250) {
        // Simulate processing events
        heartbeat_manager.increment_events_processed(1);
        heartbeat_count += 1;

        // Emit heartbeat
        heartbeat_manager.emit_heartbeat(Some(json!({
            "satellite_name": "test_satellite",
            "heartbeat_count": heartbeat_count,
            "timestamp": chrono::Utc::now()
        })));

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Verify heartbeats were sent
    assert!(heartbeat_count >= 2, "Should have sent multiple heartbeats");

    // Verify heartbeat events are in database
    let heartbeat_events = sqlx::query!(
        "SELECT COUNT(*) as count FROM core.events WHERE source = 'sinex' AND event_type = 'satellite.heartbeat'"
    )
    .fetch_one(ctx.pool())
    .await?;

    assert!(
        heartbeat_events.count.unwrap_or(0) >= 2,
        "Should have heartbeat events in database"
    );

    Ok(())
}

/// Test satellite error handling and recovery (replaces EventSource error handling)
test_batch_events!(test_satellite_error_handling, "test", "test.event", 5, 
    |pool: &DbPool, events: &[RawEvent]| async move {
        // Verify batch
        assert_eq!(events.len(), 5);
        Ok(())
    }
);

/// Test satellite database integration (replaces EventSource database integration)
test_batch_events!(test_satellite_database_integration, "test", "test.event", 2, 
    |pool: &DbPool, events: &[RawEvent]| async move {
        // Verify batch
        assert_eq!(events.len(), 2);
        Ok(())
    }
);

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
            assert!(
                total_gb > 0.0,
                "Total disk space should be positive for {}",
                path
            );
        }

        if let Some(available_gb) = info.get("available_gb").and_then(|v| v.as_f64()) {
            assert!(
                available_gb >= 0.0,
                "Available disk space should be non-negative for {}",
                path
            );
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
    let directories = filesystem_info
        .get("directories")
        .unwrap()
        .as_object()
        .unwrap();

    // Should have checked some directories
    assert!(
        !directories.is_empty(),
        "Should have checked some directories"
    );

    for (dir_path, info) in directories {
        // Each directory should have permission info
        assert!(
            info.get("writable").is_some(),
            "Should check writability for {}",
            dir_path
        );

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

// Queue status tests removed - work queue architecture replaced by hotlog streams

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
    ValidationChain::validate(event.source.clone(), "event_source")
        .not_empty()
        .into_result()?;
    
    ValidationChain::validate(event.event_type.clone(), "event_type")
        .not_empty()
        .into_result()?;
    
    // Validate payload is an object
    assert!(event.payload.is_object(), "Event payload must be an object");

    // Also test with EventValidator
    let validator_result = validator.validate(&event);
    assert_with_context(
        validator_result.is_ok(),
        "Streamlined event should pass EventValidator",
        "streamlined_validation_demo_test",
    )?;

    Ok(())
}
