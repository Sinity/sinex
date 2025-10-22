//! Example of using the modern test infrastructure with rstest, insta, and tracing-test

use color_eyre::eyre::Result;
use insta::assert_json_snapshot;
use rstest::*;
use sinex_test_utils::prelude::*;
use tracing_test::traced_test;

/// Example of parameterized tests with rstest
#[rstest]
#[case("fs-watcher", "file.created", "/tmp/test.txt")]
#[case("fs-watcher", "file.modified", "/tmp/test.txt")]
#[case("fs-watcher", "file.deleted", "/tmp/test.txt")]
#[case("terminal", "command.executed", "ls -la")]
#[case("desktop", "window.focused", "Firefox")]
#[sinex_test]
#[traced_test]
async fn test_event_creation_parameterized(
    #[case] source: &str,
    #[case] event_type: &str,
    #[case] test_data: &str,
) {
    // Create test context
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Log something that we can verify with traced_test
    tracing::info!("Creating event with source={}, type={}", source, event_type);

    // Create event using the test context
    let event = ctx
        .create_test_event(
            source,
            event_type,
            json!({
                "data": test_data,
                "test": true
            }),
        )
        .await
        .expect("Failed to create event");

    // Basic assertions
    assert_eq!(event.source.as_str(), source);
    assert_eq!(event.event_type.as_str(), event_type);

    // Use the modern test context extension for snapshot testing
    ctx.snapshot_event(
        &event,
        Some(&format!("{}_{}", source, event_type.replace('.', "_"))),
    );

    // Query the event back
    let events = ctx
        .pool
        .events()
        .by_source(source)
        .by_type(event_type)
        .fetch()
        .await
        .expect("Failed to query events");

    assert_eq!(events.len(), 1);

    // Use similar_asserts for better diffs
    ctx.assert_similar(&events[0].payload, &event.payload, "Payload should match");
}

/// Example of fixture-based testing
#[rstest]
#[sinex_test]
async fn test_with_fixtures(
    test_sources: Vec<&'static str>,
    test_event_types: Vec<(&'static str, &'static str)>,
) {
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Create events for all test sources
    for source in test_sources.iter().take(2) {
        let event = ctx
            .create_test_event(*source, "test.event", json!({}))
            .await
            .expect("Failed to create event");

        assert_eq!(event.source.as_str(), *source);
    }

    // Verify event types fixture
    assert!(test_event_types.len() >= 5);
    for (source, event_type) in test_event_types.iter().take(2) {
        println!("Testing {} -> {}", source, event_type);
    }
}

/// Example of snapshot testing with insta
#[sinex_test]
async fn test_complex_event_snapshot() {
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Create a complex event
    let event = ctx
        .create_test_event(
            "fs-watcher",
            "file.modified",
            json!({
                "path": "/home/user/documents/report.pdf",
                "size": 1024 * 1024,
                "permissions": "0o644",
                "owner": "user",
                "group": "users"
            }),
        )
        .await
        .expect("Failed to create filesystem event");

    // Snapshot the entire event with redactions
    assert_json_snapshot!(event, {
        ".id" => "[event-id]",
        ".ts_ingest" => "[timestamp]",
        ".host" => "[hostname]",
    });

    // Use the helper for more complex snapshots
    let helper = SnapshotTestHelper::new().with_redactions();
    helper.snapshot(&event.payload, "filesystem_event_payload");
}

/// Example combining all features
#[rstest]
#[case::created("file.created", true)]
#[case::modified("file.modified", false)]
#[case::deleted("file.deleted", true)]
#[sinex_test]
#[traced_test]
async fn test_filesystem_event_handling(
    #[case] event_type: &str,
    #[case] should_trigger_scan: bool,
) {
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Log test execution
    tracing::info!("Testing filesystem event: {}", event_type);

    // Create the event
    let event = ctx
        .create_test_event(
            "fs-watcher",
            event_type,
            json!({
                "path": "/test/file.txt",
                "trigger_scan": should_trigger_scan
            }),
        )
        .await
        .expect("Failed to create event");

    // Verify the event
    assert_eq!(event.event_type.as_str(), event_type);
    assert_eq!(
        event.payload["trigger_scan"].as_bool().unwrap(),
        should_trigger_scan
    );

    // Snapshot test for each case
    ctx.snapshot_event(&event, Some(event_type));

    // Check that appropriate log was created
    tracing::debug!("Event processing complete for {}", event_type);
}

#[cfg(test)]
mod snapshot_organization {
    use super::*;

    /// Example showing how to organize snapshots by feature
    #[sinex_test]
    async fn test_terminal_command_snapshots() {
        let ctx = TestContext::new()
            .await
            .expect("Failed to create test context");

        // Create various terminal commands
        let commands = vec![
            ("ls -la", 0, 150),
            ("git status", 0, 200),
            ("cargo build", 1, 5000),
        ];

        for (cmd, exit_code, duration_ms) in commands {
            let event = ctx
                .create_test_event(
                    "terminal",
                    "command.executed",
                    json!({
                        "command": cmd,
                        "exit_code": exit_code,
                        "duration_ms": duration_ms,
                        "working_dir": "/project"
                    }),
                )
                .await
                .expect("Failed to create terminal event");

            // Organized snapshot naming
            let snapshot_name = format!("terminal_cmd_{}", cmd.replace(' ', "_").replace('-', "_"));
            ctx.snapshot(&event.payload, Some(&snapshot_name));
        }
    }
}
