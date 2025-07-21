//! Demonstration of test factories in action
//!
//! This integration test shows how test factories dramatically simplify
//! complex test scenario setup compared to manual event creation.

use crate::common::test_macros::*;
use crate::common::prelude::*;
use crate::common::test_factories::{
    UserActivityFactory, SystemEventFactory, FileSystemScenarioFactory,
    WorkflowFactory, ErrorScenarioFactory, scenarios
};
use crate::common::query_helpers::TestQueries;
use crate::common::automaton_testing;
use sinex_events::{event_types, sources};

/// Test a complete user session with realistic activity patterns
#[sinex_test(timeout = 60)]
async fn test_user_session_workflow(ctx: TestContext) -> TestResult {
    // Create a 30-minute user session with 15 activities
    let session_events = UserActivityFactory::create_user_session(30, 15);
    
    // Insert all events
    ctx.insert_events(&session_events).await?;
    
    // Verify session structure
    let events = TestQueries::get_recent_events(ctx.pool(), 100).await?;
    assert!(events.len() >= session_events.len());
    
    // Check session start/end events exist
    let session_starts = events.iter()
        .filter(|e| e.event_type == event_types::shell::SESSION_STARTED)
        .count();
    let session_ends = events.iter()
        .filter(|e| e.event_type == event_types::shell::SESSION_ENDED)
        .count();
    
    assert_eq!(session_starts, 1);
    assert_eq!(session_ends, 1);
    
    // Verify mixed activity types
    let file_ops = events.iter()
        .filter(|e| e.source == sources::FS)
        .count();
    let shell_cmds = events.iter()
        .filter(|e| e.source == sources::SHELL_KITTY)
        .count();
    let window_events = events.iter()
        .filter(|e| e.source == sources::WM_HYPRLAND)
        .count();
    let clipboard_events = events.iter()
        .filter(|e| e.source == sources::CLIPBOARD)
        .count();
    
    assert!(file_ops > 0, "Should have file operations");
    assert!(shell_cmds > 0, "Should have shell commands");
    assert!(window_events > 0, "Should have window events");
    assert!(clipboard_events > 0, "Should have clipboard events");

/// Test development workflow scenario
#[sinex_test(timeout = 45)]
async fn test_development_workflow(ctx: TestContext) -> TestResult {
    let dev_events = UserActivityFactory::create_development_session();
    
    ctx.insert_events(&dev_events).await?;
    
    // Verify git workflow
    let git_commands = TestQueries::get_events_by_type_and_pattern(
        ctx.pool(),
        event_types::shell::COMMAND_EXECUTED,
        "%git%"
    ).await?;
    
    assert!(git_commands.len() >= 4, "Should have multiple git commands");
    
    // Verify file modifications
    let file_mods = TestQueries::get_events_by_type(
        ctx.pool(),
        event_types::filesystem::FILE_MODIFIED
    ).await?;
    
    assert_eq!(file_mods.len(), 3, "Should have modified 3 files");
    
    // Verify test execution
    let test_runs = TestQueries::get_events_by_type_and_pattern(
        ctx.pool(),
        event_types::shell::COMMAND_EXECUTED,
        "%cargo test%"
    ).await?;
    
    assert_eq!(test_runs.len(), 1, "Should have run tests once");

/// Test system monitoring with resource pressure
#[sinex_test(timeout = 50)]
async fn test_system_monitoring_and_pressure(ctx: TestContext) -> TestResult {
    // Create normal monitoring events
    let monitoring_events = SystemEventFactory::create_system_monitoring(10, 30);
    ctx.insert_events(&monitoring_events).await?;
    
    // Create resource pressure scenario
    let pressure_events = SystemEventFactory::create_resource_pressure();
    ctx.insert_events(&pressure_events).await?;
    
    // Verify health summaries
    let health_summaries = TestQueries::get_events_by_type(
        ctx.pool(),
        event_types::sinex::SYSTEM_HEALTH_SUMMARY
    ).await?;
    
    assert!(!health_summaries.is_empty());
    
    // Check for resource warnings
    let warnings = TestQueries::get_events_by_type(
        ctx.pool(),
        event_types::sinex::AUTOMATON_ERROR
    ).await?;
    
    let resource_warnings: Vec<_> = warnings.iter()
        .filter(|e| {
            e.payload.get("error_type")
                .and_then(|v| v.as_str())
                .map(|s| s == "resource_warning")
                .unwrap_or(false)
        })
        .collect();
    
    assert!(!resource_warnings.is_empty(), "Should have resource warnings");

/// Test complete file workflow with factories
#[sinex_test(timeout = 40)]
async fn test_file_workflow_scenario(ctx: TestContext) -> TestResult {
    let file_events = FileSystemScenarioFactory::create_file_workflow("/tmp/document.md");
    
    ctx.insert_events(&file_events).await?;
    
    // Verify workflow progression
    let fs_events = TestQueries::get_events_by_source(ctx.pool(), sources::FS).await?;
    
    // Should have: create + multiple modifications
    let creates = fs_events.iter()
        .filter(|e| e.event_type == event_types::filesystem::FILE_CREATED)
        .count();
    let modifies = fs_events.iter()
        .filter(|e| e.event_type == event_types::filesystem::FILE_MODIFIED)
        .count();
    
    assert_eq!(creates, 1);
    assert!(modifies >= 5);
    
    // Verify size progression
    let mod_events: Vec<_> = fs_events.iter()
        .filter(|e| e.event_type == event_types::filesystem::FILE_MODIFIED)
        .collect();
    
    for window in mod_events.windows(2) {
        let prev_size = window[0].payload["size_bytes"].as_u64().unwrap_or(0);
        let curr_size = window[1].payload["size_bytes"].as_u64().unwrap_or(0);
        assert!(curr_size > prev_size, "File should grow over time");
    }
    
    Ok(())
}

/// Test build process scenario
#[sinex_test(timeout = 45)]
async fn test_build_process_scenario(ctx: TestContext) -> TestResult {
    let build_events = FileSystemScenarioFactory::create_build_process();
    
    ctx.insert_events(&build_events).await?;
    
    // Verify build command
    let build_cmds = TestQueries::get_events_by_type_and_pattern(
        ctx.pool(),
        event_types::shell::COMMAND_EXECUTED,
        "%cargo build%"
    ).await?;
    
    assert_eq!(build_cmds.len(), 1);
    
    // Verify artifact creation
    let artifacts = TestQueries::get_events_by_source(ctx.pool(), sources::FS).await?;
    
    let dirs_created = artifacts.iter()
        .filter(|e| e.event_type == event_types::filesystem::DIR_CREATED)
        .count();
    let files_created = artifacts.iter()
        .filter(|e| e.event_type == event_types::filesystem::FILE_CREATED)
        .count();
    
    assert!(dirs_created >= 3, "Should create build directories");
    assert!(files_created >= 20, "Should create many object files");
    
    // Verify final binary
    let binary = artifacts.iter()
        .find(|e| e.payload["path"].as_str() == Some("target/release/myapp"))
        .expect("Should have final binary");
    
    assert_eq!(binary.payload["size_bytes"], json!(5 * 1024 * 1024));

/// Test git workflow with factories
#[sinex_test(timeout = 35)]
async fn test_git_workflow(ctx: TestContext) -> TestResult {
    let git_events = WorkflowFactory::create_git_workflow();
    
    ctx.insert_events(&git_events).await?;
    
    // Verify workflow order
    let shell_events = TestQueries::get_events_by_source(ctx.pool(), sources::SHELL_KITTY).await?;
    
    let commands: Vec<&str> = shell_events.iter()
        .filter_map(|e| e.payload["command"].as_str())
        .collect();
    
    // Verify expected command sequence
    assert!(commands.contains(&"git status"));
    assert!(commands.contains(&"git add src/"));
    assert!(commands.contains(&"git commit -m 'feat: Add user authentication'"));
    assert!(commands.contains(&"git push origin main"));
    
    // Verify timing - push should be last
    let push_event = shell_events.iter()
        .find(|e| e.payload["command"].as_str() == Some("git push origin main"))
        .expect("Should have push event");
    
    let push_time = push_event.ts_orig.unwrap_or(push_event.ts_ingest);
    
    for event in &shell_events {
        if event.id != push_event.id {
            let event_time = event.ts_orig.unwrap_or(event.ts_ingest);
            assert!(event_time <= push_time, "Push should be last");
        }
    }
    
    Ok(())
}

/// Test error cascade and recovery
#[sinex_test(timeout = 40)]
async fn test_error_cascade_scenario(ctx: TestContext) -> TestResult {
    let error_events = ErrorScenarioFactory::create_error_cascade();
    
    ctx.insert_events(&error_events).await?;
    
    // Verify error progression
    let errors = TestQueries::get_events_by_type(
        ctx.pool(),
        event_types::sinex::AUTOMATON_ERROR
    ).await?;
    
    assert!(errors.len() >= 2, "Should have multiple errors");
    
    // Verify failed operations
    let failed_ops = TestQueries::get_all_events(ctx.pool()).await?
        .into_iter()
        .filter(|e| {
            e.payload.get("failed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        })
        .count();
    
    assert_eq!(failed_ops, 5, "Should have 5 failed write attempts");
    
    // Verify recovery
    let recovery_events = TestQueries::get_events_by_type(
        ctx.pool(),
        event_types::sinex::AUTOMATON_HEARTBEAT
    ).await?;
    
    let recovered = recovery_events.iter()
        .any(|e| {
            e.payload.get("message")
                .and_then(|v| v.as_str())
                .map(|s| s.contains("recovered"))
                .unwrap_or(false)
        });
    
    assert!(recovered, "Should show recovery");

/// Test mixed workload scenario
#[sinex_test(timeout = 60)]
async fn test_mixed_workload(ctx: TestContext) -> TestResult {
    let mixed_events = scenarios::mixed_workload(15);
    
    ctx.insert_events(&mixed_events).await?;
    
    // Verify we have a good mix of event types
    let all_events = TestQueries::get_all_events(ctx.pool()).await?;
    
    let sources: std::collections::HashSet<_> = all_events.iter()
        .map(|e| &e.source)
        .collect();
    
    assert!(sources.contains(&sources::SHELL_KITTY.to_string()));
    assert!(sources.contains(&sources::FS.to_string()));
    assert!(sources.contains(&sources::WM_HYPRLAND.to_string()));
    assert!(sources.contains(&sources::SINEX.to_string()));
    
    // Verify chronological ordering
    let mut sorted_events = all_events.clone();
    sorted_events.sort_by_key(|e| e.ts_orig.unwrap_or(e.ts_ingest));
    
    for i in 1..sorted_events.len() {
        let prev_time = sorted_events[i-1].ts_orig.unwrap_or(sorted_events[i-1].ts_ingest);
        let curr_time = sorted_events[i].ts_orig.unwrap_or(sorted_events[i].ts_ingest);
        assert!(prev_time <= curr_time, "Events should be chronologically ordered");
    }
    
    Ok(())
}

/// Test stress scenario with many events
#[sinex_test(timeout = 60)]
async fn test_stress_scenario(ctx: TestContext) -> TestResult {
    let stress_events = scenarios::stress_test(1000);
    
    // Batch insert for efficiency
    let start = std::time::Instant::now();
    ctx.insert_events(&stress_events).await?;
    let insert_duration = start.elapsed();
    
    println!("Inserted 1000 events in {:?}", insert_duration);
    
    // Verify all inserted
    let count = TestQueries::count_events_by_pattern(
        ctx.pool(),
        "%stress-test%"
    ).await?;
    
    assert_eq!(count, 1000, "All stress test events should be inserted");
    
    // Verify timing spacing
    let events = TestQueries::get_events_by_type_and_pattern(
        ctx.pool(),
        event_types::shell::COMMAND_EXECUTED,
        "%stress-test-cmd%"
    ).await?;
    
    // Check first few for proper spacing
    for i in 1..5.min(events.len()) {
        let prev_time = events[i-1].ts_orig.unwrap_or(events[i-1].ts_ingest);
        let curr_time = events[i].ts_orig.unwrap_or(events[i].ts_ingest);
        let diff = (curr_time - prev_time).num_milliseconds();
        
        // Should be approximately 100ms apart
        assert!(diff >= 90 && diff <= 110, "Events should be ~100ms apart, got {}ms", diff);
    }
    
    Ok(())
}

/// Test network failure scenario
#[sinex_test(timeout = 45)]
async fn test_network_failure_scenario(ctx: TestContext) -> TestResult {
    let network_events = ErrorScenarioFactory::create_network_failure();
    
    ctx.insert_events(&network_events).await?;
    
    // Verify degradation pattern
    let curl_events = TestQueries::get_events_by_type_and_pattern(
        ctx.pool(),
        event_types::shell::COMMAND_EXECUTED,
        "%curl%"
    ).await?;
    
    // Check response times increase
    let slow_events: Vec<_> = curl_events.iter()
        .filter(|e| e.payload.get("warning").is_some())
        .collect();
    
    assert_eq!(slow_events.len(), 3, "Should have 3 slow responses");
    
    // Verify failures
    let failed_events = TestQueries::get_events_by_type(
        ctx.pool(), 
        event_types::shell::COMMAND_FAILED
    ).await?;
    
    assert!(failed_events.len() >= 4, "Should have multiple failures");
    
    // Verify fallback
    let fallback = curl_events.iter()
        .find(|e| {
            e.payload.get("fallback")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        });
    
    assert!(fallback.is_some(), "Should have fallback event");
    
    Ok(())
}

/// Demonstrate code reduction with factories
#[sinex_test(timeout = 40)]
async fn test_compare_manual_vs_factory(ctx: TestContext) -> TestResult {
    // OLD WAY: Manual event creation (verbose)
    let manual_events = vec![
        TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::SESSION_STARTED)
            .with_field("terminal", json!("kitty"))
            .with_field("pid", json!(12345))
            .with_field("user", json!("testuser"))
            .build(),
        TestEventBuilder::new(sources::WM_HYPRLAND, event_types::window_manager::WINDOW_OPENED)
            .with_field("window_id", json!(1001))
            .with_field("app_name", json!("kitty"))
            .with_field("title", json!("Terminal"))
            .build(),
        TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
            .with_field("command", json!("ls -la"))
            .with_field("exit_code", json!(0))
            .with_field("duration_ms", json!(50))
            .build(),
        // ... would need many more events for realistic scenario
    ];
    
    // NEW WAY: Factory creation (concise)
    let factory_events = UserActivityFactory::create_user_session(5, 3);
    
    // Both create valid events
    assert!(!manual_events.is_empty());
    assert!(!factory_events.is_empty());
    
    // Factory creates more comprehensive scenarios
    assert!(factory_events.len() > manual_events.len());
    
    // Insert factory events to verify they work
    ctx.insert_events(&factory_events).await?;
    
    let inserted = TestQueries::get_recent_events(ctx.pool(), 100).await?;
    assert!(inserted.len() >= factory_events.len());
    
    println!("Manual approach: {} lines of code for {} events", 
             15 * manual_events.len(), manual_events.len());
    println!("Factory approach: 1 line of code for {} events", 
             factory_events.len());
    
    Ok(())
}

// Helper module for test-specific queries
mod TestQueries {
    use super::*;
    use sinex_db::queries::EventQueries;
    
    /// Get events by type with pattern matching
    pub async fn get_events_by_type_and_pattern(
        pool: &DbPool,
        event_type: &str,
        pattern: &str
    ) -> AnyhowResult<Vec<RawEvent>> {
        // This is a simplified version - in real implementation would use proper query
        let events = EventQueries::get_by_event_type(event_type.to_string(), None, None)
            .fetch_all(pool)
            .await?;
        
        Ok(events.into_iter()
            .filter(|e| {
                serde_json::to_string(&e.payload)
                    .map(|s| s.contains(pattern.trim_matches('%')))
                    .unwrap_or(false)
            })
            .collect())
    }
    
    /// Count events matching a pattern
    pub async fn count_events_by_pattern(
        pool: &DbPool,
        pattern: &str
    ) -> AnyhowResult<usize> {
        let all_events = EventQueries::get_recent(None, None)
            .fetch_all(pool)
            .await?;
        
        Ok(all_events.into_iter()
            .filter(|e| {
                serde_json::to_string(&e.payload)
                    .map(|s| s.contains(pattern.trim_matches('%')))
                    .unwrap_or(false)
            })
            .count())
    }
}