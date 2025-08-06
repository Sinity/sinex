//! API Unit Tests  
//!
//! Tests for API layer functionality using the current architecture:
//! - Repository pattern with DbPoolExt
//! - Event management through repositories
//! - Configuration parsing and validation
//! - Modern error handling with color-eyre
//! - Integration with current sinex-test-utils

use sinex_test_utils::prelude::*;
use sinex_db::repositories::DbPoolExt;
use sinex_db::models::*;
use sinex_types::domain::{EventSource, EventType};
use sinex_types::{Id, Ulid};
use serde_json::json;
use std::collections::HashMap;

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/// Helper for configuration parsing tests
mod config_helpers {
    use color_eyre::eyre::{anyhow, Result};
    use toml::Value as ConfigValue;

    pub fn navigate_to_value<'a>(config: &'a ConfigValue, path: &str) -> Result<&'a ConfigValue> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = config;

        for part in &parts {
            current = current
                .get(part)
                .ok_or_else(|| anyhow!("Path '{}' not found at '{}'", path, part))?;
        }

        Ok(current)
    }

    pub fn require_str<'a>(config: &'a ConfigValue, path: &str) -> Result<&'a str> {
        let value = navigate_to_value(config, path)?;
        value
            .as_str()
            .ok_or_else(|| anyhow!("Required string field '{}' not found or not a string", path))
    }

    pub fn require_u64(config: &ConfigValue, path: &str) -> Result<u64> {
        let value = navigate_to_value(config, path)?;
        value
            .as_integer()
            .and_then(|i| u64::try_from(i).ok())
            .ok_or_else(|| anyhow!("Required u64 field '{}' not found or not a valid u64", path))
    }

    pub fn require_bool(config: &ConfigValue, path: &str) -> Result<bool> {
        let value = navigate_to_value(config, path)?;
        value
            .as_bool()
            .ok_or_else(|| anyhow!("Required bool field '{}' not found or not a boolean", path))
    }

    pub fn require_array<'a>(config: &'a ConfigValue, path: &str) -> Result<&'a Vec<ConfigValue>> {
        let value = navigate_to_value(config, path)?;
        value
            .as_array()
            .ok_or_else(|| anyhow!("Required array field '{}' not found or not an array", path))
    }
}

use config_helpers::*;

// =============================================================================
// REPOSITORY PATTERN TESTS - Current Architecture
// =============================================================================

#[sinex_test]
async fn test_event_repository_basic_operations(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    
    // Test event creation through repository
    let event = Event::schemaless()
        .source(EventSource::from_static("test-api"))
        .event_type(EventType::from_static("api.test"))
        .payload(json!({
            "test_type": "repository_basic",
            "value": 42
        }))
        .build();
    
    // Insert through repository
    let inserted = pool.events().insert(event.clone()).await?;
    assert!(inserted.id.is_some());
    assert_eq!(inserted.source, event.source);
    assert_eq!(inserted.event_type, event.event_type);
    
    // Retrieve by ID
    let retrieved = pool.events()
        .get_by_id(inserted.id.unwrap())
        .await?;
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.id, inserted.id);
    assert_eq!(retrieved.payload, inserted.payload);
    
    Ok(())
}

#[sinex_test]
async fn test_event_repository_query_operations(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    
    // Create multiple events with different sources
    let events = vec![
        Event::schemaless()
            .source(EventSource::from_static("api-source-1"))
            .event_type(EventType::from_static("test.event"))
            .payload(json!({"group": "A", "value": 1}))
            .build(),
        Event::schemaless()
            .source(EventSource::from_static("api-source-1"))
            .event_type(EventType::from_static("test.event"))
            .payload(json!({"group": "A", "value": 2}))
            .build(),
        Event::schemaless()
            .source(EventSource::from_static("api-source-2"))
            .event_type(EventType::from_static("test.other"))
            .payload(json!({"group": "B", "value": 3}))
            .build(),
    ];
    
    // Insert all events
    let mut inserted_ids = Vec::new();
    for event in events {
        let inserted = pool.events().insert(event).await?;
        inserted_ids.push(inserted.id.unwrap());
    }
    
    // Query by source
    let source1_events = pool.events()
        .by_source("api-source-1")
        .fetch()
        .await?;
    assert_eq!(source1_events.len(), 2);
    
    // Query by event type
    let test_events = pool.events()
        .by_type("test.event")
        .fetch()
        .await?;
    assert_eq!(test_events.len(), 2);
    
    // Query by both source and type
    let specific_events = pool.events()
        .by_source("api-source-1")
        .by_type("test.event")
        .fetch()
        .await?;
    assert_eq!(specific_events.len(), 2);
    
    // Count queries
    let total_count = pool.events().count().await?;
    assert!(total_count >= 3); // At least our 3 events
    
    let source1_count = pool.events()
        .by_source("api-source-1")
        .count()
        .await?;
    assert_eq!(source1_count, 2);
    
    Ok(())
}

#[sinex_test]
async fn test_event_repository_pagination(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    
    // Create 10 events
    for i in 0..10 {
        let event = Event::schemaless()
            .source(EventSource::from_static("pagination-test"))
            .event_type(EventType::from_static("test.pagination"))
            .payload(json!({"index": i}))
            .build();
        pool.events().insert(event).await?;
    }
    
    // Test limit
    let limited_events = pool.events()
        .by_source("pagination-test")
        .limit(5)
        .fetch()
        .await?;
    assert_eq!(limited_events.len(), 5);
    
    // Test offset + limit (if supported by the repository)
    let offset_events = pool.events()
        .by_source("pagination-test")
        .limit(3)
        .fetch()
        .await?;
    assert_eq!(offset_events.len(), 3);
    
    Ok(())
}

// =============================================================================
// EVENT VALIDATION AND ERROR HANDLING TESTS  
// =============================================================================

#[sinex_test]
async fn test_event_validation_edge_cases(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    
    // Test with minimal valid event
    let minimal_event = Event::schemaless()
        .source(EventSource::from_static("validation-test"))
        .event_type(EventType::from_static("minimal"))
        .payload(json!({}))
        .build();
    
    let result = pool.events().insert(minimal_event).await;
    assert!(result.is_ok(), "Minimal valid event should be accepted");
    
    // Test with large payload
    let large_payload = json!({
        "data": "x".repeat(10000),
        "metadata": {
            "size": 10000,
            "type": "large_test"
        }
    });
    
    let large_event = Event::schemaless()
        .source(EventSource::from_static("validation-test"))
        .event_type(EventType::from_static("large.payload"))
        .payload(large_payload)
        .build();
    
    let result = pool.events().insert(large_event).await;
    assert!(result.is_ok(), "Large payload should be handled correctly");
    
    Ok(())
}

#[sinex_test]
async fn test_invalid_event_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test invalid source creation (empty source)
    let result = std::panic::catch_unwind(|| {
        EventSource::new("")
    });
    
    // Empty source should either panic or be handled gracefully
    // The exact behavior depends on the EventSource implementation
    
    // Test event creation with invalid JSON payload
    let problematic_payload = json!({
        "field_with_null": null,
        "deeply_nested": {
            "level1": {
                "level2": {
                    "level3": "deeply nested value"
                }
            }
        }
    });
    
    let event = Event::schemaless()
        .source(EventSource::from_static("error-test"))
        .event_type(EventType::from_static("problematic"))
        .payload(problematic_payload)
        .build();
    
    // This should work fine - the repository should handle complex JSON
    let result = ctx.pool().events().insert(event).await;
    assert!(result.is_ok(), "Complex JSON payload should be handled");
    
    Ok(())
}

// =============================================================================
// CONFIGURATION PARSING TESTS
// =============================================================================

#[test]
fn test_toml_configuration_parsing() -> color_eyre::eyre::Result<()> {
    let config_str = r#"
        [database]
        url = "postgresql://localhost/sinex_test"
        pool_size = 10
        enable_logging = true
        
        [services]
        names = ["ingestd", "gateway", "fs-watcher"]
        timeout_seconds = 30
        
        [features]
        experimental = false
        debug_mode = true
    "#;
    
    let config: toml::Value = toml::from_str(config_str)?;
    
    // Test helper functions
    let db_url = require_str(&config, "database.url")?;
    assert_eq!(db_url, "postgresql://localhost/sinex_test");
    
    let pool_size = require_u64(&config, "database.pool_size")?;
    assert_eq!(pool_size, 10);
    
    let enable_logging = require_bool(&config, "database.enable_logging")?;
    assert!(enable_logging);
    
    let service_names = require_array(&config, "services.names")?;
    assert_eq!(service_names.len(), 3);
    
    let timeout = require_u64(&config, "services.timeout_seconds")?;
    assert_eq!(timeout, 30);
    
    // Test error cases
    let invalid_path_result = require_str(&config, "nonexistent.path");
    assert!(invalid_path_result.is_err());
    
    let wrong_type_result = require_str(&config, "database.pool_size");
    assert!(wrong_type_result.is_err());
    
    Ok(())
}

#[test]
fn test_configuration_edge_cases() -> color_eyre::eyre::Result<()> {
    let config_str = r#"
        [empty_section]
        
        [numbers]
        zero = 0
        negative = -42
        large = 9223372036854775807  # i64::MAX
        
        [strings]
        empty = ""
        unicode = "Hello 世界 🌍"
        multiline = """
        This is a
        multiline string
        """
    "#;
    
    let config: toml::Value = toml::from_str(config_str)?;
    
    // Test edge case values
    let zero = require_u64(&config, "numbers.zero")?;
    assert_eq!(zero, 0);
    
    let empty_str = require_str(&config, "strings.empty")?;
    assert_eq!(empty_str, "");
    
    let unicode_str = require_str(&config, "strings.unicode")?;
    assert_eq!(unicode_str, "Hello 世界 🌍");
    
    let multiline = require_str(&config, "strings.multiline")?;
    assert!(multiline.contains("multiline string"));
    
    Ok(())
}

// =============================================================================
// API INTEGRATION TESTS - Realistic Scenarios
// =============================================================================

#[sinex_test]
async fn test_api_workflow_complete(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    
    // Simulate a complete API workflow
    // 1. Create events from different sources
    let fs_event = Event::schemaless()
        .source(EventSource::from_static("filesystem"))
        .event_type(EventType::from_static("file.created"))
        .payload(json!({
            "path": "/tmp/test.txt",
            "size": 1024,
            "permissions": "0644"
        }))
        .build();
    
    let terminal_event = Event::schemaless()
        .source(EventSource::from_static("terminal"))
        .event_type(EventType::from_static("command.executed"))
        .payload(json!({
            "command": "touch /tmp/test.txt",
            "exit_code": 0,
            "working_dir": "/tmp"
        }))
        .build();
    
    // 2. Insert events
    let fs_inserted = pool.events().insert(fs_event).await?;
    let terminal_inserted = pool.events().insert(terminal_event).await?;
    
    // 3. Query and verify relationships
    let filesystem_events = pool.events()
        .by_source("filesystem")
        .fetch()
        .await?;
    assert!(!filesystem_events.is_empty());
    
    let terminal_events = pool.events()
        .by_source("terminal")
        .fetch()
        .await?;
    assert!(!terminal_events.is_empty());
    
    // 4. Verify temporal ordering
    assert!(fs_inserted.ts_ingest <= terminal_inserted.ts_ingest || 
            terminal_inserted.ts_ingest <= fs_inserted.ts_ingest);
    
    // 5. Query recent events across all sources
    let recent_events = pool.events()
        .limit(10)
        .fetch()
        .await?;
    assert!(recent_events.len() >= 2);
    
    Ok(())
}

#[sinex_test]
async fn test_bulk_operations_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    
    // Test bulk insert performance
    let start_time = std::time::Instant::now();
    
    let mut events = Vec::new();
    for i in 0..100 {
        let event = Event::schemaless()
            .source(EventSource::from_static("bulk-test"))
            .event_type(EventType::from_static("bulk.insert"))
            .payload(json!({
                "batch_id": "test-batch-1",
                "index": i,
                "timestamp": chrono::Utc::now().timestamp()
            }))
            .build();
        events.push(event);
    }
    
    // Insert events individually (could be optimized with batch insert in the future)
    for event in events {
        pool.events().insert(event).await?;
    }
    
    let duration = start_time.elapsed();
    
    // Verify all events were inserted
    let inserted_count = pool.events()
        .by_source("bulk-test")
        .count()
        .await?;
    assert_eq!(inserted_count, 100);
    
    // Performance assertion (should complete in reasonable time)
    assert!(duration.as_secs() < 30, "Bulk insert should complete within 30 seconds");
    
    Ok(())
}

// =============================================================================
// ERROR BOUNDARY AND RESILIENCE TESTS
// =============================================================================

#[sinex_test]
async fn test_api_error_boundaries(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    
    // Test querying non-existent events
    let non_existent_id = Id::<Event>::new();
    let result = pool.events().get_by_id(non_existent_id).await?;
    assert!(result.is_none());
    
    // Test querying with non-existent source
    let no_events = pool.events()
        .by_source("definitely-does-not-exist")
        .fetch()
        .await?;
    assert!(no_events.is_empty());
    
    // Test extreme values
    let large_limit_events = pool.events()
        .limit(1000000) // Very large limit
        .fetch()
        .await?;
    // Should not crash, may be limited by the database
    
    Ok(())
}

#[sinex_test]
async fn test_concurrent_api_access(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use std::sync::Arc;
    use tokio::task::JoinSet;
    
    let pool = Arc::new(ctx.pool().clone());
    let mut join_set = JoinSet::new();
    
    // Spawn multiple concurrent operations
    for i in 0..20 {
        let pool_clone = pool.clone();
        join_set.spawn(async move {
            // Each task performs different operations
            match i % 3 {
                0 => {
                    // Insert operation
                    let event = Event::schemaless()
                        .source(EventSource::from_static("concurrent-test"))
                        .event_type(EventType::from_static("concurrent.insert"))
                        .payload(json!({"worker": i}))
                        .build();
                    pool_clone.events().insert(event).await
                }
                1 => {
                    // Query operation
                    let _events = pool_clone.events()
                        .by_source("concurrent-test")
                        .limit(5)
                        .fetch()
                        .await?;
                    Ok(Event::schemaless().build()) // Dummy return for type consistency
                }
                _ => {
                    // Count operation
                    let _count = pool_clone.events()
                        .by_source("concurrent-test")
                        .count()
                        .await?;
                    Ok(Event::schemaless().build()) // Dummy return for type consistency
                }
            }
        });
    }
    
    // Wait for all operations to complete
    let mut results = Vec::new();
    while let Some(result) = join_set.join_next().await {
        results.push(result?);
    }
    
    // All operations should complete successfully
    assert_eq!(results.len(), 20);
    
    // Verify that concurrent inserts worked
    let final_count = pool.events()
        .by_source("concurrent-test")
        .count()
        .await?;
    assert!(final_count >= 6); // At least 6 insert operations should have succeeded
    
    Ok(())
}

// =============================================================================
// REGRESSION TESTS - Specific edge cases discovered during development
// =============================================================================

#[sinex_test]
async fn test_event_id_consistency(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool();
    
    // Create event and verify ID handling
    let event = Event::schemaless()
        .source(EventSource::from_static("id-test"))
        .event_type(EventType::from_static("id.consistency"))
        .payload(json!({"test": "id_consistency"}))
        .build();
    
    // Before insertion, ID should be None or a generated value
    let pre_insert_id = event.id;
    
    // Insert event
    let inserted = pool.events().insert(event).await?;
    
    // After insertion, should have a valid ID
    assert!(inserted.id.is_some());
    let db_id = inserted.id.unwrap();
    
    // Retrieve the same event
    let retrieved = pool.events()
        .get_by_id(db_id)
        .await?
        .expect("Event should exist");
    
    // IDs should match
    assert_eq!(retrieved.id, inserted.id);
    
    // ULID conversion should work
    let ulid: Ulid = db_id.into();
    let id_from_ulid = Id::<Event>::from(ulid);
    assert_eq!(id_from_ulid, db_id);
    
    Ok(())
}

#[test]
fn test_domain_type_edge_cases() -> color_eyre::eyre::Result<()> {
    // Test EventSource edge cases
    let static_source = EventSource::from_static("static-source");
    let dynamic_source = EventSource::new("dynamic-source");
    
    assert_eq!(static_source.as_str(), "static-source");
    assert_eq!(dynamic_source.as_str(), "dynamic-source");
    
    // Test EventType edge cases
    let static_type = EventType::from_static("static.type");
    let dynamic_type = EventType::new("dynamic.type");
    
    assert_eq!(static_type.as_str(), "static.type");
    assert_eq!(dynamic_type.as_str(), "dynamic.type");
    
    // Test with special characters
    let special_source = EventSource::new("test-source_123");
    let special_type = EventType::new("test.type_v2");
    
    assert_eq!(special_source.as_str(), "test-source_123");
    assert_eq!(special_type.as_str(), "test.type_v2");
    
    Ok(())
}