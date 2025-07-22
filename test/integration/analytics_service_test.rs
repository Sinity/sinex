// Comprehensive tests for AnalyticsService
//
// Tests all analytics methods with focus on aggregation logic,
// time-based filtering, and accurate data insights.

use crate::common::prelude::*;
use sinex_services::AnalyticsService;
use sinex_events::event_types::{shell, filesystem, window_manager, clipboard, sinex};

/// Helper to create test events with specific timestamps and content
async fn create_analytics_test_event(
    pool: &DbPool,
    source: &str,
    event_type: &str,
    payload_content: Value,
    time_offset: Option<ChronoDuration>,
) -> TestResult {
    let factory = EventFactory::new(source);
    let mut event = factory.create_event(event_type, payload_content);
    
    // Set host
    event.host = "analytics-test-host".to_string();
    
    // Set timestamp if provided
    if let Some(offset) = time_offset {
        let timestamp = Utc::now() - offset;
        event.ts_orig = Some(timestamp);
    }

    insert_event(pool, &event).await?;

    Ok(())
}

/// Create diverse test dataset for analytics testing
async fn setup_analytics_test_data(pool: &DbPool) -> TestResult {
    // Create various event types for analytics
    
    // Shell commands with specific frequencies
    let commands = [
        ("git status", 8),
        ("cargo build", 5),
        ("ls -la", 3),
        ("cd /home", 2),
        ("vim file.rs", 1),
    ];

    for (command, count) in commands {
        for i in 0..count {
            create_analytics_test_event(
                pool,
                sources::SHELL_KITTY,
                shell::COMMAND_EXECUTED,
                json!({
                    "command": command,
                    "exit_code": 0,
                    "duration_ms": 100 + i * 10
                }),
                Some(ChronoDuration::minutes(5 * i as i64)),
            )
            .await?;
        }
    }
    
    // Add filesystem events
    for i in 0..5 {
        create_analytics_test_event(
            pool,
            sources::FS,
            filesystem::FILE_MODIFIED,
            json!({
                "path": format!("/tmp/test{}.txt", i),
                "size": 1024 * (i + 1)
            }),
            Some(ChronoDuration::minutes(10 * i as i64)),
        )
        .await?;
    }
    
    // Add window manager events
    for i in 0..3 {
        create_analytics_test_event(
            pool,
            sources::WM_HYPRLAND,
            window_manager::WINDOW_FOCUSED,
            json!({
                "window_title": format!("Window {}", i),
                "window_class": "test-app"
            }),
            Some(ChronoDuration::minutes(15 * i as i64)),
        )
        .await?;
    }
    
    // Add clipboard event
    create_analytics_test_event(
        pool,
        sources::CLIPBOARD,
        clipboard::COPIED,
        json!({
            "content_type": "text",
            "content_length": 50
        }),
        Some(ChronoDuration::minutes(20)),
    )
    .await?;
    
    // Add old system event (outside typical time ranges)
    let factory = EventFactory::new(sources::SYSTEMD);
    let mut system_event = factory.create_event(
        sinex::PROCESS_STARTED,
        json!({
            "uptime_seconds": 0,
            "kernel_version": "6.1.0"
        })
    );
    system_event.ts_orig = Some(Utc::now() - ChronoDuration::days(2));
    system_event.host = "analytics-test-host".to_string();
    insert_event(pool, &system_event).await?;
    
    Ok(())
}

#[sinex_test]
async fn test_get_event_count_by_source_no_time_filter(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let counts = service.get_event_count_by_source(None, None).await?;

    // Verify expected source counts
    assert_eq!(counts.get(sources::FS), Some(&5), "Filesystem events should be 5");
    assert_eq!(
        counts.get(sources::SHELL_KITTY),
        Some(&19),
        "Shell events should be 19 (8+5+3+2+1)"
    );
    assert_eq!(
        counts.get(sources::WM_HYPRLAND),
        Some(&3),
        "Window manager events should be 3"
    );
    assert_eq!(
        counts.get(sources::CLIPBOARD),
        Some(&1),
        "Clipboard events should be 1"
    );
    assert_eq!(counts.get(sources::SYSTEMD), Some(&1), "System events should be 1");

    // Total should be correct
    let total: i64 = counts.values().sum();
    assert_eq!(total, 29, "Total event count should be 29");

    Ok(())
}

#[sinex_test]
async fn test_get_event_count_by_source_with_time_filter(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    // Query last hour only
    let start = Utc::now() - ChronoDuration::hours(1);
    let end = Utc::now();
    
    let counts = service.get_event_count_by_source(Some(start), Some(end)).await?;

    // Should exclude the 2-day old system event
    assert!(
        counts.get(sources::SYSTEMD).is_none() || *counts.get(sources::SYSTEMD).unwrap() == 0,
        "Old system event should be excluded"
    );

    // All other events should be included
    let total: i64 = counts.values().sum();
    assert_eq!(total, 28, "Should have 28 events within the last hour");

    Ok(())
}

#[sinex_test]
async fn test_get_most_frequent_commands(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let top_commands = service.get_most_frequent_commands(3, None, None).await?;

    assert_eq!(top_commands.len(), 3, "Should return top 3 commands");
    
    // Verify order and counts
    assert_eq!(top_commands[0], ("git status".to_string(), 8));
    assert_eq!(top_commands[1], ("cargo build".to_string(), 5));
    assert_eq!(top_commands[2], ("ls -la".to_string(), 3));

    Ok(())
}

#[sinex_test]
async fn test_get_event_timeline(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let start = Utc::now() - ChronoDuration::hours(2);
    let end = Utc::now();
    
    let timeline = service.get_event_timeline(5, start, end).await?;

    assert!(!timeline.is_empty(), "Timeline should have data");
    
    // Verify chronological order
    for window in timeline.windows(2) {
        assert!(
            window[0].bucket <= window[1].bucket,
            "Timeline should be chronologically ordered"
        );
    }
    
    // Each bucket should have a count
    for entry in &timeline {
        assert!(entry.count > 0, "Each timeline entry should have events");
    }

    Ok(())
}

#[sinex_test]
async fn test_get_file_activity_summary(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = AnalyticsService::new(pool.clone());

    // Create specific file activity
    for i in 0..3 {
        create_analytics_test_event(
            &pool,
            sources::FS,
            filesystem::FILE_MODIFIED,
            json!({
                "path": "/project/src/main.rs",
                "size": 1024 + i * 100
            }),
            None,
        )
        .await?;
    }
    
    for i in 0..2 {
        create_analytics_test_event(
            &pool,
            sources::FS,
            filesystem::FILE_MODIFIED,
            json!({
                "path": "/project/Cargo.toml",
                "size": 500
            }),
            None,
        )
        .await?;
    }

    let file_activity = service.get_file_activity_summary(5, None, None).await?;

    assert_eq!(file_activity.len(), 2, "Should have 2 files");
    assert_eq!(file_activity[0], ("/project/src/main.rs".to_string(), 3));
    assert_eq!(file_activity[1], ("/project/Cargo.toml".to_string(), 2));

    Ok(())
}

#[sinex_test]
async fn test_get_event_type_distribution(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let distribution = service.get_event_type_distribution(None, None).await?;

    // Verify distribution
    assert_eq!(
        distribution.get(shell::COMMAND_EXECUTED),
        Some(&19),
        "Should have 19 command events"
    );
    assert_eq!(
        distribution.get(filesystem::FILE_MODIFIED),
        Some(&5),
        "Should have 5 file modified events"
    );
    assert_eq!(
        distribution.get(window_manager::WINDOW_FOCUSED),
        Some(&3),
        "Should have 3 window focused events"
    );
    assert_eq!(
        distribution.get(clipboard::COPIED),
        Some(&1),
        "Should have 1 clipboard event"
    );
    assert_eq!(
        distribution.get(sinex::PROCESS_STARTED),
        Some(&1),
        "Should have 1 boot event"
    );

    Ok(())
}

#[sinex_test]
async fn test_get_hourly_activity_pattern(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = AnalyticsService::new(pool.clone());

    // Create events at specific hours
    let now = Utc::now();
    let base_hour = now.hour();
    
    // Create events in different hours
    for hour_offset in 0..3 {
        let event_time = now - ChronoDuration::hours(hour_offset as i64);
        let factory = EventFactory::new(sources::SHELL_KITTY);
        let mut event = factory.create_event(
            shell::COMMAND_EXECUTED,
            json!({
                "command": format!("test-{}", hour_offset),
                "exit_code": 0
            })
        );
        event.ts_orig = Some(event_time);
        insert_event(&pool, &event).await?;
    }

    let pattern = service.get_hourly_activity_pattern(7).await?;

    // Should have data for recent hours
    assert!(!pattern.is_empty(), "Should have hourly activity data");
    
    // All hours should be between 0-23
    for (hour, _) in &pattern {
        assert!(*hour < 24, "Hour should be 0-23");
    }

    Ok(())
}

#[sinex_test]
async fn test_analytics_with_empty_database(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = AnalyticsService::new(pool.clone());

    // Test all methods with empty database
    let counts = service.get_event_count_by_source(None, None).await?;
    assert!(counts.is_empty(), "Should have no source counts");

    let commands = service.get_most_frequent_commands(10, None, None).await?;
    assert!(commands.is_empty(), "Should have no commands");

    let timeline = service.get_event_timeline(
        5,
        Utc::now() - ChronoDuration::hours(1),
        Utc::now()
    ).await?;
    assert!(timeline.is_empty(), "Should have empty timeline");

    let file_activity = service.get_file_activity_summary(10, None, None).await?;
    assert!(file_activity.is_empty(), "Should have no file activity");

    let distribution = service.get_event_type_distribution(None, None).await?;
    assert!(distribution.is_empty(), "Should have no event types");

    let pattern = service.get_hourly_activity_pattern(7).await?;
    assert!(pattern.is_empty(), "Should have no hourly pattern");

    Ok(())
}

#[sinex_test]
async fn test_analytics_with_mixed_hosts(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = AnalyticsService::new(pool.clone());

    // Create events from different hosts
    let hosts = ["host-a", "host-b", "host-c"];
    
    for (i, host) in hosts.iter().enumerate() {
        let factory = EventFactory::new(sources::SHELL_KITTY);
        let mut event = factory.create_event(
            shell::COMMAND_EXECUTED,
            json!({
                "command": "ls",
                "exit_code": 0
            })
        );
        event.host = host.to_string();
        event.ts_orig = Some(Utc::now() - ChronoDuration::minutes(i as i64));
        insert_event(&pool, &event).await?;
    }

    // Analytics should aggregate across all hosts
    let counts = service.get_event_count_by_source(None, None).await?;
    assert_eq!(
        counts.get(sources::SHELL_KITTY),
        Some(&3),
        "Should count events from all hosts"
    );

    Ok(())
}

#[sinex_test]
async fn test_analytics_time_boundary_edge_cases(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = AnalyticsService::new(pool.clone());

    let now = Utc::now();
    
    // Create events at exact boundary times
    let boundary_time = now - ChronoDuration::hours(1);
    
    // Event exactly at start boundary
    let factory = EventFactory::new(sources::SHELL_KITTY);
    let mut event_at_start = factory.create_event(
        shell::COMMAND_EXECUTED,
        json!({"command": "at-start", "exit_code": 0})
    );
    event_at_start.ts_orig = Some(boundary_time);
    insert_event(&pool, &event_at_start).await?;
    
    // Event just before start boundary
    let mut event_before = factory.create_event(
        shell::COMMAND_EXECUTED,
        json!({"command": "before", "exit_code": 0})
    );
    event_before.ts_orig = Some(boundary_time - ChronoDuration::seconds(1));
    insert_event(&pool, &event_before).await?;
    
    // Event just after start boundary
    let mut event_after = factory.create_event(
        shell::COMMAND_EXECUTED,
        json!({"command": "after", "exit_code": 0})
    );
    event_after.ts_orig = Some(boundary_time + ChronoDuration::seconds(1));
    insert_event(&pool, &event_after).await?;

    // Query with exact boundary
    let counts = service.get_event_count_by_source(
        Some(boundary_time),
        Some(now)
    ).await?;

    // Should include event at boundary and after, but not before
    assert_eq!(
        counts.get(sources::SHELL_KITTY),
        Some(&2),
        "Should include events at and after boundary"
    );

    Ok(())
}

#[sinex_test]
async fn test_analytics_performance_with_large_dataset(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = AnalyticsService::new(pool.clone());

    // Create a moderately large dataset for performance testing
    let event_count = 100;
    let start_time = Instant::now();
    
    // Use batch builder for efficient insertion
    let mut builder = BatchEventBuilder::new();
    
    for i in 0..event_count {
        builder.add_event()
            .source(sources::SHELL_KITTY)
            .event_type(shell::COMMAND_EXECUTED)
            .payload(json!({
                "command": format!("command-{}", i % 10),
                "exit_code": 0,
                "duration_ms": i
            }))
            .time_offset(ChronoDuration::seconds(i as i64));
    }
    
    builder.insert_all(&pool).await?;
    
    let insert_duration = start_time.elapsed();
    println!("Inserted {} events in {:?}", event_count, insert_duration);

    // Test query performance
    let query_start = Instant::now();
    let counts = service.get_event_count_by_source(None, None).await?;
    let query_duration = query_start.elapsed();
    
    assert_eq!(
        counts.get(sources::SHELL_KITTY),
        Some(&(event_count as i64)),
        "Should have all events"
    );
    
    println!("Queried event counts in {:?}", query_duration);
    assert!(
        query_duration < Duration::from_millis(100),
        "Query should complete quickly"
    );

    // Test aggregation performance
    let agg_start = Instant::now();
    let commands = service.get_most_frequent_commands(10, None, None).await?;
    let agg_duration = agg_start.elapsed();
    
    assert_eq!(commands.len(), 10, "Should return top 10 commands");
    println!("Aggregated top commands in {:?}", agg_duration);
    assert!(
        agg_duration < Duration::from_millis(200),
        "Aggregation should complete quickly"
    );

    Ok(())
}