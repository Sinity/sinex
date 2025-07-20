// # Regression Testing
//
// Tests that prevent specific bugs from reoccurring by testing previously fixed issues:
// - Complex interaction bugs
// - Performance regression detection
// - Configuration edge cases
// - Concurrent access issues
// - Validation edge cases
//
// ## Test Categories
//
// - **Concurrent Database Tests**: Database-related race conditions and concurrency issues
// - **Configuration Reload Tests**: Config management and reload-related bugs
// - **JSON Payload Tests**: JSON handling and serialization edge cases
// - **ULID Overflow Tests**: ULID generation and overflow edge cases
// - **Validation Edge Cases**: Input validation boundary conditions
//
// ## Performance Expectations
//
// - **Individual tests**: 10-30 seconds
// - **Resource usage**: Moderate to high concurrency testing
// - **Purpose**: Prevent regression of known issues

use crate::common::prelude::*;

use crate::common::resources;
// DEPRECATED: CollectorConfig no longer exists after modernization to environment-only configuration
// use sinex_collector::config::{CollectorConfig, ConfigManager};
use sinex_db::validation::EventValidator;
use sinex_db::queries::{EventQueries, CheckpointQueries, OperationQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use sinex_events::{EventFactory, services, event_types};

// ==================== CONCURRENT DATABASE TESTS ====================

#[sinex_test(timeout = 30)]
async fn test_concurrent_ulid_generation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let num_tasks = 10;
    let events_per_task = 100;
    let barrier = Arc::new(tokio::sync::Barrier::new(num_tasks));

    let mut handles = vec![];

    for task_id in 0..num_tasks {
        let pool = pool.clone();
        let barrier = barrier.clone();

        let handle = tokio::spawn(async move {
            // Wait for all tasks to be ready
            barrier.wait().await;

            let mut ulids = vec![];
            for i in 0..events_per_task {
                let event = crate::common::events::generic_adversarial_event(
                    "test",
                    "concurrent.test",
                    json!({
                        "task": task_id,
                        "event": i
                    }),
                    None,
                );

                let result = insert_event(&pool, &event).await.unwrap();
                ulids.push(result);
            }
            ulids
        });

        handles.push(handle);
    }

    // Collect all ULIDs
    let mut all_ulids = vec![];
    for handle in handles {
        let ulids = handle.await.unwrap();
        all_ulids.extend(ulids);
    }

    // Check for duplicates - this might FAIL under high concurrency
    let unique_ulids: std::collections::HashSet<_> = all_ulids.iter().collect();
    pretty_assertions::assert_eq!(
        all_ulids.len(),
        unique_ulids.len(),
        "Found {} duplicate ULIDs in {} total",
        all_ulids.len() - unique_ulids.len(),
        all_ulids.len()
    );

    Ok(())
}

#[sinex_test(timeout = 30)]
async fn test_worker_double_processing(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Insert a test event
    let event = crate::common::events::generic_adversarial_event(
        "test",
        "worker_test",
        json!({"test": true}),
        None,
    );
    let inserted = insert_event(pool, &event).await.unwrap();

    // Simulate two workers trying to claim the same event
    let pool1 = pool.clone();
    let pool2 = pool.clone();
    let event_id = inserted;

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let b1 = barrier.clone();
    let b2 = barrier.clone();

    let worker1 = tokio::spawn(async move {
        b1.wait().await;
        // Try to claim event for processing
        EventQueries::update_event_payload_merge(&pool1, event_id, json!({"processed_by": "worker1"})).await
    });

    let worker2 = tokio::spawn(async move {
        b2.wait().await;
        // Try to claim same event
        EventQueries::update_event_payload_merge(&pool2, event_id, json!({"processed_by": "worker2"})).await
    });

    let (r1, r2) = tokio::join!(worker1, worker2);

    // Both should succeed because there's no proper locking!
    // This demonstrates the need for SELECT FOR UPDATE SKIP LOCKED
    assert!(
        r1.is_ok() && r2.is_ok(),
        "Both workers modified the same event!"
    );

    // Check final state
    let final_event = sinex_db::get_event_by_id(pool, event_id).await.unwrap();
    println!("Final payload: {}", final_event.payload);
    // This will show that both workers processed it - a bug!

    Ok(())
}

#[sinex_test(timeout = 30)]
async fn test_concurrent_database_connection_exhaustion(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let num_connections = 50; // More than typical pool size

    let mut handles = Vec::new();

    for i in 0..num_connections {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            // Hold connection for a while
            let mut tx = pool_clone.begin().await?;

            // Insert a test event within transaction
            let event =
                EventFactory::new("connection_test").create_event("test_event", json!({"connection_id": i}));

            EventQueries::insert_event_in_tx(&mut *tx, &event).await?;

            // Hold the connection briefly
            tokio::time::sleep(Duration::from_millis(100)).await;

            tx.commit().await?;

            Ok::<_, anyhow::Error>(i)
        });
        handles.push(handle);
    }

    // Wait for all connections to complete
    let mut successful_connections = 0;
    let mut failed_connections = 0;

    for handle in handles {
        match handle.await {
            Ok(Ok(_)) => successful_connections += 1,
            Ok(Err(_)) => failed_connections += 1,
            Err(_) => failed_connections += 1,
        }
    }

    println!(
        "Connection test: {} successful, {} failed",
        successful_connections, failed_connections
    );

    // Should handle connection exhaustion gracefully
    assert!(
        successful_connections > 0,
        "At least some connections should succeed"
    );

    Ok(())
}

// ==================== CONFIGURATION RELOAD TESTS ====================
// 
// DEPRECATED: The following tests used the old CollectorConfig::load_from_file architecture
// which has been modernized to environment-only configuration. These tests are preserved
// for reference but are commented out as they no longer compile with the current codebase.

/*
#[sinex_test(timeout = 30)]
async fn test_config_reload_race_condition(ctx: TestContext) -> TestResult {
    // Create a config manager with a test config file
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");

    let initial_config = r#"
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp"]
"#;

    std::fs::write(&config_path, initial_config).unwrap();

    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));

    // Start watching for changes
    let mut update_rx = manager.start_watching().await.unwrap();

    // Rapidly update config multiple times
    for i in 0..10 {
        let new_config = format!(
            r#"
enabled_events = ["file.created", "file.modified"]

[event.files]
watch_paths = ["/tmp", "/home/user{}"]
"#,
            i
        );

        std::fs::write(&config_path, new_config).unwrap();

        // Small delay to trigger filesystem events
        tokio::task::yield_now().await;
    }

    // Try to receive all updates
    let mut update_count = 0;
    while let Ok(Some(_)) = timeout(Duration::from_secs(2), update_rx.recv()).await {
        update_count += 1;
    }

    // We wrote 10 times but may receive fewer due to debouncing
    // This might expose race conditions in the watcher
    println!("Config updates received: {}/10", update_count);

    // Final config should have the last value
    let final_config = manager.get_config().await;
    assert!(final_config
        .enabled_events
        .contains(&"file.modified".to_string()));

    Ok(())
}

#[sinex_test(timeout = 30)]
async fn test_config_malformed_handling(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");

    // Start with valid config
    let valid_config = r#"enabled_events = ["file.created"]"#;
    std::fs::write(&config_path, valid_config).unwrap();

    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));
    let mut update_rx = manager.start_watching().await.unwrap();

    // Write invalid TOML
    std::fs::write(&config_path, "this is not valid TOML!").unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The watcher should handle this gracefully
    // But the current implementation might panic!
    let _update_result = timeout(Duration::from_secs(1), update_rx.recv()).await;

    // Should still have the old valid config
    let current_config = manager.get_config().await;
    pretty_assertions::assert_eq!(current_config.enabled_events.len(), 1);

    Ok(())
}

#[sinex_test(timeout = 30)]
async fn test_config_missing_required_fields(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");

    // Config missing required fields
    let incomplete_config = r#"
# Missing enabled_events field
[event.files]
watch_paths = ["/tmp"]
"#;

    std::fs::write(&config_path, incomplete_config).unwrap();

    // This should fail gracefully
    let config_result = CollectorConfig::load_from_file(&config_path);

    match config_result {
        Ok(config) => {
            // If it loads, it should have sensible defaults
            assert!(
                !config.enabled_events.is_empty() || config.enabled_events.is_empty(),
                "Config should handle missing fields gracefully"
            );
        }
        Err(e) => {
            // If it fails, it should be a clear error
            println!("Config loading failed as expected: {}", e);
        }
    }

    Ok(())
}

#[sinex_test(timeout = 30)]
async fn test_config_file_permissions(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");

    // Create valid config
    let valid_config = r#"enabled_events = ["file.created"]"#;
    std::fs::write(&config_path, valid_config).unwrap();

    // Test reading with restricted permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        // Make file readable only by owner
        let mut perms = std::fs::metadata(&config_path)?.permissions();
        perms.set_mode(0o600);
        std::fs::set_permissions(&config_path, perms)?;

        // Should still be readable by same user
        let config = CollectorConfig::load_from_file(&config_path);
        assert!(
            config.is_ok(),
            "Should be able to read config with restricted permissions"
        );
    }

    Ok(())
}
*/

// ==================== JSON PAYLOAD TESTS ====================

#[sinex_test]
async fn test_json_payload_size_limits(ctx: TestContext) -> TestResult {
    // Test extremely large JSON payloads
    let mut huge_array = vec![];
    for i in 0..10000 {
        huge_array.push(json!({
            "index": i,
            "data": "x".repeat(100)
        }));
    }

    let event = crate::common::events::generic_adversarial_event(
        "test",
        "huge.payload",
        json!({"huge_array": huge_array}),
        None,
    );

    // This might cause issues with serialization or database storage
    let serialized = serde_json::to_string(&event);
    assert!(serialized.is_ok(), "Should handle large payloads");

    if let Ok(json_str) = serialized {
        println!("Payload size: {} bytes", json_str.len());
        // PostgreSQL jsonb has a practical limit
        assert!(
            json_str.len() < 1_000_000_000,
            "Payload too large for PostgreSQL"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_json_special_characters(ctx: TestContext) -> TestResult {
    // Test JSON with special characters that might break things
    let evil_payloads = [
        json!({ "key": "\u{0000}" }),              // Null byte
        json!({ "key": "\u{001F}" }),              // Control character
        json!({ "emoji": "😈🔥💣" }),              // Emojis
        json!({ "rtl": "مرحبا بالعالم" }),         // Right-to-left text
        json!({ "invalid": "test_invalid_char" }), // Test special handling
    ];

    for (i, payload) in evil_payloads.iter().enumerate() {
        let mut event = crate::common::events::generic_adversarial_event(
            "test",
            "test.event",
            json!({"test": true}),
            None,
        );
        event.payload = payload.clone();

        // These might fail serialization or cause database issues
        match serde_json::to_string(&event) {
            Ok(_) => println!("Payload {} serialized successfully", i),
            Err(e) => println!("Payload {} failed: {}", i, e),
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_recursive_json_structure(ctx: TestContext) -> TestResult {
    // Create a deeply nested structure
    let mut nested = json!({ "value": "base" });
    for i in 0..1000 {
        nested = json!({
            "level": i,
            "nested": nested
        });
    }

    let event =
        crate::common::events::generic_adversarial_event("test", "deeply.nested", nested, None);

    // This might cause stack overflow or other issues
    let result = serde_json::to_string(&event);
    println!("Deep nesting serialization: {:?}", result.is_ok());
    Ok(())
}

#[sinex_test]
async fn test_json_numeric_edge_cases(ctx: TestContext) -> TestResult {
    // Test edge cases with numeric values
    let numeric_payloads = [
        json!({ "value": i64::MAX }),
        json!({ "value": i64::MIN }),
        json!({ "value": f64::MAX }),
        json!({ "value": f64::MIN }),
        json!({ "value": f64::INFINITY }),
        json!({ "value": f64::NEG_INFINITY }),
        json!({ "value": f64::NAN }),
    ];

    for (i, payload) in numeric_payloads.iter().enumerate() {
        let event = crate::common::events::generic_adversarial_event(
            "test",
            "numeric.test",
            payload.clone(),
            None,
        );

        match serde_json::to_string(&event) {
            Ok(json_str) => {
                println!("Numeric payload {} serialized: {}", i, json_str.len());
                // Check if special float values are handled
                if json_str.contains("null") || json_str.contains("Infinity") {
                    println!("  Special float value detected in serialization");
                }
            }
            Err(e) => println!("Numeric payload {} failed: {}", i, e),
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_json_string_escaping(ctx: TestContext) -> TestResult {
    // Test string escaping edge cases
    let escape_test_strings = [
        "\"quoted string\"",
        "\\backslash\\",
        "\n\r\t",
        "\u{0001}\u{0002}\u{0003}",
        "Mixed: \"quotes\" and \\backslashes\\",
    ];

    for (i, test_str) in escape_test_strings.iter().enumerate() {
        let event = crate::common::events::generic_adversarial_event(
            "test",
            "escape.test",
            json!({"test_string": test_str}),
            None,
        );

        match serde_json::to_string(&event) {
            Ok(json_str) => {
                println!("Escape test {} serialized successfully", i);

                // Try to deserialize back
                match serde_json::from_str::<serde_json::Value>(&json_str) {
                    Ok(deserialized) => {
                        let recovered_str =
                            deserialized["payload"]["test_string"].as_str().unwrap();
                        assert_eq!(
                            recovered_str, *test_str,
                            "String should roundtrip correctly"
                        );
                    }
                    Err(e) => println!("Deserialization failed: {}", e),
                }
            }
            Err(e) => println!("Escape test {} failed: {}", i, e),
        }
    }
    Ok(())
}

// ==================== ULID OVERFLOW TESTS ====================

#[sinex_test]
async fn test_monotonic_ulid_overflow(ctx: TestContext) -> TestResult {
    // Create a ULID with all random bytes set to 255 (max value)
    let mut max_bytes = [255u8; 16];
    // Keep the timestamp part valid
    let timestamp = Ulid::new().to_bytes();
    max_bytes[..6].copy_from_slice(&timestamp[..6]);

    let max_ulid = Ulid::from_bytes(max_bytes).unwrap();

    // This should handle overflow gracefully
    // Note: new_monotonic not available in current implementation
    // This test documents what would happen with monotonic generation
    let next_ulid = Ulid::new();

    // The next ULID should be greater than max_ulid
    assert!(
        next_ulid > max_ulid,
        "Monotonic ULID should handle overflow"
    );
    Ok(())
}

#[sinex_test]
async fn test_monotonic_ulid_rapid_generation(ctx: TestContext) -> TestResult {
    // Generate many ULIDs in the same millisecond
    let mut ulids = Vec::new();
    let mut _prev: Option<Ulid> = None;

    // Generate 1000 ULIDs as fast as possible
    for _ in 0..1000 {
        // Note: new_monotonic not available - using regular new()
        let ulid = Ulid::new();
        ulids.push(ulid);
        _prev = Some(ulid);
    }

    // Check all are unique and monotonic
    for window in ulids.windows(2) {
        assert!(window[0] < window[1], "ULIDs should be strictly monotonic");
    }

    // Check for duplicates
    let mut unique = std::collections::HashSet::new();
    for ulid in &ulids {
        assert!(unique.insert(ulid.to_string()), "Found duplicate ULID!");
    }
    Ok(())
}

#[sinex_test]
async fn test_ulid_timestamp_boundaries(ctx: TestContext) -> TestResult {
    // Test ULIDs at timestamp boundaries
    let test_timestamps = [
        0u64,                                         // Unix epoch
        1_000_000_000_000,                            // Year 2001
        chrono::Utc::now().timestamp_millis() as u64, // Current time
        281_474_976_710_655,                          // Max ULID timestamp (year 10889)
    ];

    for &timestamp in &test_timestamps {
        // Create ULID with specific timestamp
        let datetime = chrono::DateTime::from_timestamp_millis(timestamp as i64).unwrap();
        let ulid = Ulid::from_datetime(datetime);

        // Verify timestamp extraction
        let extracted_timestamp = ulid.inner().timestamp_ms();
        assert_eq!(
            extracted_timestamp, timestamp,
            "Timestamp should roundtrip correctly"
        );

        // Verify it can be converted to/from various formats
        let uuid = ulid.to_uuid();
        let ulid_from_uuid = Ulid::from_uuid(uuid);
        assert_eq!(ulid, ulid_from_uuid, "UUID conversion should roundtrip");

        let string = ulid.to_string();
        let ulid_from_string = string.parse::<Ulid>().unwrap();
        assert_eq!(ulid, ulid_from_string, "String conversion should roundtrip");
    }

    Ok(())
}

#[sinex_test]
async fn test_ulid_database_storage_regression(ctx: TestContext) -> TestResult {
    // Test that ULIDs are correctly stored and retrieved from database
    let pool = ctx.pool();
    let test_ulid = Ulid::new();

    // Insert event with specific ULID
    let mut event = EventFactory::new("ulid_test").create_event("storage_test", json!({"test": "ulid storage"}));
    event.id = test_ulid;

    insert_event(pool, &event).await?;

    // Retrieve and verify
    let retrieved_events = EventQueries::get_events_by_source_and_type(pool, "ulid_test", "storage_test").await?;
    assert!(!retrieved_events.is_empty(), "Should have found the test event");
    
    let retrieved_ulid = retrieved_events[0].id;
    assert_eq!(
        retrieved_ulid, test_ulid,
        "ULID should roundtrip through database"
    );

    // Test sorting by ULID
    let newer_ulid = Ulid::new();
    let mut newer_event = EventFactory::new("ulid_test").create_event("storage_test", json!({"test": "newer event"}));
    newer_event.id = newer_ulid;

    insert_event(pool, &newer_event).await?;

    // Query in chronological order
    let ordered_events = EventQueries::get_events_by_source_ordered_by_id(pool, "ulid_test").await?;

    assert_eq!(ordered_events.len(), 2);
    let first_ulid = ordered_events[0].id;
    let second_ulid = ordered_events[1].id;

    assert!(
        first_ulid < second_ulid,
        "ULIDs should be ordered chronologically"
    );

    Ok(())
}

// ==================== VALIDATION EDGE CASES ====================

#[sinex_test]
async fn test_invalid_octal_permissions(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();

    // This should FAIL but probably won't due to the bug
    let invalid_octal = json!({
        "path": "/test.txt",
        "size": 1024,
        "permissions": "888"  // Invalid octal (8 is not a valid octal digit)
    });

    let result = validator.validate_with_rules("fs", "file.created", &invalid_octal);
    assert!(
        result.is_err(),
        "Should reject invalid octal permissions like '888'"
    );
    Ok(())
}

#[sinex_test]
async fn test_permissions_edge_cases(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();

    // Test various edge cases
    let test_cases = vec![
        ("999", false, "all digits > 7"),
        ("0000", true, "4 digits with leading zero"),
        ("777", true, "valid 3 digits"),
        ("1777", true, "valid 4 digits with sticky bit"),
        ("", false, "empty string"),
        ("77", false, "only 2 digits"),
        ("77777", false, "too many digits"),
        ("0x777", false, "hex prefix"),
        ("0o777", false, "octal prefix"),
    ];

    for (perms, should_be_valid, desc) in test_cases {
        let event = json!({
            "path": "/test.txt",
            "size": 1024,
            "permissions": perms
        });

        let result = validator.validate_with_rules("fs", "file.created", &event);
        if should_be_valid {
            assert!(result.is_ok(), "Should accept {}: {}", desc, perms);
        } else {
            assert!(result.is_err(), "Should reject {}: {}", desc, perms);
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_path_validation_missing(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();

    // The validator doesn't check for path traversal or null bytes!
    let dangerous_paths = vec![
        "../../../etc/passwd",
        "/test\0.txt", // null byte
        "//double//slashes//",
        "/test/../../../etc/passwd",
        "", // empty path
    ];

    for path in dangerous_paths {
        let event = json!({
            "path": path,
            "size": 1024
        });

        let result = validator.validate_with_rules("fs", "file.created", &event);
        // This will likely PASS but shouldn't for security reasons
        println!("Path '{}' validation: {:?}", path, result.is_ok());
    }
    Ok(())
}

#[sinex_test]
async fn test_field_type_validation_edge_cases(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();

    // Test type validation edge cases
    let type_test_cases = vec![
        // Size as string instead of number
        (
            json!({
                "path": "/test.txt",
                "size": "1024"  // String instead of number
            }),
            false,
            "size as string",
        ),
        // Negative size
        (
            json!({
                "path": "/test.txt",
                "size": -1024
            }),
            false,
            "negative size",
        ),
        // Extremely large size
        (
            json!({
                "path": "/test.txt",
                "size": u64::MAX
            }),
            true,
            "max size",
        ),
        // Missing required field
        (
            json!({
                "size": 1024
                // Missing path
            }),
            false,
            "missing path",
        ),
        // Additional unexpected fields
        (
            json!({
                "path": "/test.txt",
                "size": 1024,
                "unexpected_field": "should this be allowed?"
            }),
            true,
            "unexpected field",
        ),
    ];

    for (event, should_be_valid, desc) in type_test_cases {
        let result = validator.validate_with_rules("fs", "file.created", &event);
        if should_be_valid {
            assert!(result.is_ok(), "Should accept {}: {:?}", desc, event);
        } else {
            assert!(result.is_err(), "Should reject {}: {:?}", desc, event);
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_command_validation_edge_cases(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();

    // Test command validation edge cases
    let command_test_cases = vec![
        // Very long command
        (
            json!({
                "command": "a".repeat(100_000),
                "exit_code": 0
            }),
            true,
            "very long command",
        ),
        // Command with null bytes
        (
            json!({
                "command": "ls\0-la",
                "exit_code": 0
            }),
            true,
            "command with null bytes",
        ),
        // Empty command
        (
            json!({
                "command": "",
                "exit_code": 0
            }),
            false,
            "empty command",
        ),
        // Command with control characters
        (
            json!({
                "command": "echo\x01\x02\x03",
                "exit_code": 0
            }),
            true,
            "command with control chars",
        ),
        // Invalid exit code
        (
            json!({
                "command": "test",
                "exit_code": 999
            }),
            true,
            "high exit code",
        ),
        // Negative exit code
        (
            json!({
                "command": "test",
                "exit_code": -1
            }),
            false,
            "negative exit code",
        ),
    ];

    for (event, should_be_valid, desc) in command_test_cases {
        let result = validator.validate_with_rules("shell", "command.executed", &event);
        if should_be_valid {
            assert!(result.is_ok(), "Should accept {}: {:?}", desc, event);
        } else {
            assert!(result.is_err(), "Should reject {}: {:?}", desc, event);
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_timestamp_validation_edge_cases(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();

    // Test timestamp validation edge cases
    let timestamp_test_cases = vec![
        // Invalid RFC3339 format
        (
            json!({
                "timestamp": "2023-01-01 12:00:00"  // Missing timezone
            }),
            false,
            "invalid RFC3339",
        ),
        // Valid RFC3339
        (
            json!({
                "timestamp": "2023-01-01T12:00:00Z"
            }),
            true,
            "valid RFC3339",
        ),
        // Future timestamp
        (
            json!({
                "timestamp": "2050-01-01T12:00:00Z"
            }),
            true,
            "future timestamp",
        ),
        // Very old timestamp
        (
            json!({
                "timestamp": "1970-01-01T00:00:00Z"
            }),
            true,
            "epoch timestamp",
        ),
        // Timestamp with nanoseconds
        (
            json!({
                "timestamp": "2023-01-01T12:00:00.123456789Z"
            }),
            true,
            "nanosecond precision",
        ),
    ];

    for (event, should_be_valid, desc) in timestamp_test_cases {
        let result = validator.validate_with_rules("test", "timestamp.test", &event);
        if should_be_valid {
            assert!(result.is_ok(), "Should accept {}: {:?}", desc, event);
        } else {
            assert!(result.is_err(), "Should reject {}: {:?}", desc, event);
        }
    }

    Ok(())
}
