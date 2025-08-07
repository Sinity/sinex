//! Integration test for the enhanced sinex_test macro with rstest support

use sinex_test_utils::prelude::*;

// Test 1: Basic rstest integration - sinex_test detects #[case] attributes
#[sinex_test]
#[case("fs-watcher", "file.created")]
#[case("terminal", "command.executed")]
#[case("desktop", "window.focused")]
#[case("system", "service.started")]
async fn test_event_creation_with_rstest(
    ctx: TestContext,  // Created automatically for each case
    #[case] source: &str,
    #[case] event_type: &str,
) -> Result<()> {
    // Each test case runs with its own TestContext from the pool
    let event = ctx.create_test_event(
        source,
        event_type, 
        json!({"test_case": true})
    ).await?;
    
    // Verify the event was created correctly
    assert_eq!(event.source.as_str(), source);
    assert_eq!(event.event_type.as_str(), event_type);
    assert_eq!(event.payload["test_case"], json!(true));
    
    // Query it back to ensure it's in the database
    let events = ctx.pool.events()
        .by_source(source)
        .by_type(event_type)
        .fetch()
        .await?;
    
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].id, event.id);
    
    Ok(())
}

// Test 2: Complex parameterized test with multiple case parameters
#[sinex_test]
#[case("short", 10, true)]
#[case("medium", 1000, true)]
#[case("long", 10000, true)]
#[case("too_long", 1000000, false)]  // This one should fail validation
async fn test_payload_size_validation(
    ctx: TestContext,
    #[case] name: &str,
    #[case] size: usize,
    #[case] should_succeed: bool,
) -> Result<()> {
    let large_data = "x".repeat(size);
    
    let payload = json!({
        "name": name,
        "data": large_data,
        "size_bytes": size
    });
    
    let result = ctx.create_test_event("test", "payload.size", payload.clone()).await;
    
    if should_succeed {
        let event = result?;
        assert_eq!(event.payload["name"], json!(name));
        assert_eq!(event.payload["size_bytes"], json!(size));
        
        // Verify we can query it back
        let found = ctx.pool.events()
            .get_by_id(event.id.unwrap())
            .await?
            .expect("Event should exist");
        assert_eq!(found.id, event.id);
    } else {
        // Should fail for payloads that are too large
        assert!(result.is_err());
    }
    
    Ok(())
}

// Test 3: Using rstest with fixtures
#[sinex_test]
#[case("created")]
#[case("modified")]
#[case("deleted")]
async fn test_filesystem_events_with_fixture(
    ctx: TestContext,
    test_paths: Vec<Utf8PathBuf>,  // This is a fixture from test-utils
    #[case] operation: &str,
) -> Result<()> {
    let event_type = format!("file.{}", operation);
    
    // Create events for each path
    for path in &test_paths {
        ctx.create_test_event(
            "fs-watcher",
            &event_type,
            json!({"path": path.as_str()})
        ).await?;
    }
    
    // Verify all were created
    let events = ctx.pool.events()
        .by_source("fs-watcher")
        .by_type(&event_type)
        .fetch()
        .await?;
    
    assert_eq!(events.len(), test_paths.len());
    
    // Each should have a different path
    let paths: Vec<_> = events
        .iter()
        .map(|e| e.payload["path"].as_str().unwrap())
        .collect();
    
    for test_path in &test_paths {
        assert!(paths.contains(&test_path.as_str()));
    }
    
    Ok(())
}

// Test 4: Demonstrating snapshot testing with rstest cases
#[sinex_test]
#[case("terminal", json!({"command": "ls -la", "exit_code": 0}))]
#[case("filesystem", json!({"path": "/tmp/test.txt", "size": 1024}))]
#[case("desktop", json!({"window_id": "0x12345", "title": "Editor"}))]
async fn test_snapshots_per_case(
    ctx: TestContext,
    #[case] source: &str,
    #[case] payload: Value,
) -> Result<()> {
    let event = ctx.create_test_event(
        source,
        "snapshot.test",
        payload
    ).await?;
    
    // Each test case gets its own snapshot
    // The snapshot path includes both test name and case identifier
    ctx.snapshot_event(&event, Some(source));
    
    Ok(())
}

// Test 5: Demonstrating that regular sinex_test still works without rstest
#[sinex_test]
async fn test_regular_sinex_test_still_works(ctx: TestContext) -> Result<()> {
    // This is a regular test without rstest - should work as before
    let event = ctx.create_test_event(
        "test",
        "regular.test",
        json!({})
    ).await?;
    
    assert_eq!(event.source.as_str(), "test");
    assert_eq!(event.event_type.as_str(), "regular.test");
    
    Ok(())
}

// Test 6: Edge case - no TestContext parameter
#[sinex_test]
#[case(1, 2, 3)]
#[case(10, 20, 30)]
#[case(100, 200, 300)]
async fn test_without_context(
    #[case] a: i32,
    #[case] b: i32,
    #[case] expected: i32,
) -> Result<()> {
    // This test doesn't use TestContext at all
    assert_eq!(a + b, expected);
    Ok(())
}

#[cfg(test)]
mod verification {
    use super::*;
    
    // Verify that the macro expansion works correctly
    #[test]
    fn verify_macro_expansion_compiles() {
        // The fact that this module compiles proves the macro works
    }
}