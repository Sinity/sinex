//! Integration test demonstrating modern test infrastructure
//!
//! This test shows how to use rstest, insta, and tracing-test together
//! with the Sinex test infrastructure.

use sinex_test_utils::prelude::*;
use rstest::*;
use tracing_test::traced_test;

/// Test parameterized event creation with rstest
#[rstest]
#[case("fs-watcher", "file.created")]
#[case("terminal", "command.executed")]
#[case("desktop", "window.focused")]
#[sinex_test]
#[traced_test]
async fn test_parameterized_event_creation(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
) -> Result<()> {
    // Log something for tracing-test
    tracing::info!("Creating event: {} -> {}", source, event_type);
    
    // Create event
    let event = ctx.event()
        .source(source)
        .type_(event_type)
        .field("test", true)
        .insert()
        .await?;
    
    // Verify
    ctx.assert("event creation")
        .eq(&event.source.as_str(), &source)?
        .eq(&event.event_type.as_str(), &event_type)?;
    
    // Use modern test context for snapshot
    ctx.snapshot_event(&event, Some(&format!("{}_{}", source, event_type)));
    
    Ok(())
}

/// Test with fixtures
#[rstest]
#[sinex_test]
async fn test_multiple_sources(
    ctx: TestContext,
    test_sources: Vec<&'static str>,
) -> Result<()> {
    // Create events from fixture sources
    for source in test_sources.iter().take(3) {
        let event = ctx.event()
            .source(*source)
            .type_("test.fixture")
            .insert()
            .await?;
            
        ctx.assert(&format!("event from {}", source))
            .eq(&event.source.as_str(), source)?;
    }
    
    // Query all events
    let events = ctx.events().fetch().await?;
    ctx.assert("event count")
        .that(events.len() >= 3, "should have at least 3 events")?;
    
    Ok(())
}

/// Test snapshot functionality
#[sinex_test]
async fn test_snapshot_complex_event(ctx: TestContext) -> Result<()> {
    // Create a complex filesystem event
    let event = ctx.event()
        .filesystem()
        .path("/data/important/document.pdf")
        .size(2 * 1024 * 1024) // 2MB
        .permissions(0o600)
        .owner("admin")
        .group("staff")
        .modified()
        .insert()
        .await?;
    
    // Snapshot with redactions
    ctx.snapshot(&event, Some("complex_filesystem_event"));
    
    // Query and verify
    let found = ctx.events()
        .by_id(event.id.unwrap())
        .fetch_one()
        .await?;
    
    // Use similar_assert for better diffs
    ctx.assert_similar(&found.payload, &event.payload, "payloads should match");
    
    Ok(())
}

/// Test combining rstest cases with complex assertions
#[rstest]
#[case::success(0, true)]
#[case::failure(1, false)]
#[case::error(127, false)]
#[sinex_test]
#[traced_test]
async fn test_terminal_command_outcomes(
    ctx: TestContext,
    #[case] exit_code: i32,
    #[case] expected_success: bool,
) -> Result<()> {
    tracing::debug!("Testing exit code: {}", exit_code);
    
    // Create terminal command event
    let event = ctx.event()
        .terminal()
        .command("test-command")
        .exit_code(exit_code)
        .duration_ms(100)
        .working_dir("/tmp")
        .insert()
        .await?;
    
    // Verify payload
    let payload = &event.payload;
    ctx.assert("command outcome")
        .eq(&payload["exit_code"].as_i64().unwrap(), &(exit_code as i64))?
        .eq(&payload["success"].as_bool().unwrap(), &expected_success)?;
    
    // Snapshot each case
    ctx.snapshot_event(&event, Some(&format!("terminal_exit_{}", exit_code)));
    
    Ok(())
}

/// Test batch operations with modern infrastructure
#[sinex_test]
async fn test_batch_event_creation(ctx: TestContext) -> Result<()> {
    // Create multiple events
    let events = vec![
        ("file1.txt", 100),
        ("file2.txt", 200),
        ("file3.txt", 300),
    ];
    
    let mut created_events = Vec::new();
    for (path, size) in events {
        let event = ctx.event()
            .filesystem()
            .path(&format!("/data/{}", path))
            .size(size)
            .created()
            .insert()
            .await?;
        created_events.push(event);
    }
    
    // Snapshot the batch
    ctx.snapshot(&created_events, Some("batch_filesystem_events"));
    
    // Query and verify count
    let fs_events = ctx.events()
        .by_source("fs-watcher")
        .fetch()
        .await?;
    
    ctx.assert("batch creation")
        .that(fs_events.len() >= 3, "should have created at least 3 events")?;
    
    Ok(())
}