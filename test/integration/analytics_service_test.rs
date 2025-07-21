// Comprehensive tests for AnalyticsService
//
// Tests all analytics methods with focus on aggregation logic,
// time-based filtering, and accurate data insights.

use crate::common::prelude::*;
use sinex_events::EventFactory;
use chrono::{Duration, Utc};
use sinex_services::AnalyticsService;
use std::collections::HashMap;

/// Helper to create test events with specific timestamps and content
async fn create_analytics_test_event(
    pool: &DbPool,
    source: &str,
    event_type: &str,
    payload_content: Value,
    time_offset: Option<Duration>,
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
    // Filesystem events - 5 events spread over last 2 hours
    for i in 0..5 {
        create_analytics_test_event(
            pool,
            "fs",
            "file.created",
            json!({
                "path": format!("/test/file_{}.txt", i),
                "size": 1024 * (i + 1)
            }),
            Some(Duration::minutes(20 * i as i64)),
        )
        .await?;
    }

    // Terminal commands - different commands with varying frequencies
    let commands = [
        ("git status", 8), // Most frequent
        ("cargo build", 5),
        ("ls -la", 3),
        ("cd /home", 2),
        ("vim file.rs", 1), // Least frequent
    ];

    for (command, count) in commands {
        for i in 0..count {
            create_analytics_test_event(
                pool,
                "shell.kitty",
                "command.executed",
                json!({
                    "command": command,
                    "exit_code": 0,
                    "duration_ms": 100 + i * 10
                }),
                Some(Duration::minutes(5 * i as i64)),
            )
            .await?;
        }
    }

    // Window manager events - recent
    for i in 0..3 {
        create_analytics_test_event(
            pool,
            "wm.hyprland",
            "window.opened",
            json!({
                "title": format!("Window {}", i),
                "class": "test-app",
                "workspace": i + 1
            }),
            Some(Duration::minutes(10 * i as i64)),
        )
        .await?;
    }

    // Clipboard events - older
    create_analytics_test_event(
        &pool,
        "clipboard",
        "copied",
        json!({
            "content": "test clipboard content",
            "application": "firefox"
        }),
        Some(Duration::hours(3)),
    )
    .await?;

    // System events - very old (outside typical time ranges)
    create_analytics_test_event(
        &pool,
        "system",
        "boot.completed",
        json!({
            "uptime_seconds": 0,
            "kernel_version": "6.1.0"
        }),
        Some(Duration::days(2)),
    )
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_get_event_count_by_source_no_time_filter(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let counts = service.get_event_count_by_source(None, None).await?;

    // Verify expected source counts
    assert_eq!(counts.get("fs"), Some(&5), "Filesystem events should be 5");
    assert_eq!(
        counts.get("shell.kitty"),
        Some(&19),
        "Shell events should be 19 (8+5+3+2+1)"
    );
    assert_eq!(
        counts.get("wm.hyprland"),
        Some(&3),
        "Window manager events should be 3"
    );
    assert_eq!(
        counts.get("clipboard"),
        Some(&1),
        "Clipboard events should be 1"
    );
    assert_eq!(counts.get("system"), Some(&1), "System events should be 1");

    // Total should be correct
    let total: i64 = counts.values().sum();
    assert_eq!(total, 29, "Total event count should be 29");

    Ok(())
}

#[sinex_test]
async fn test_get_event_count_by_source_with_time_filter(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let now = Utc::now();
    let one_hour_ago = now - Duration::hours(1);

    let counts = service
        .get_event_count_by_source(Some(one_hour_ago), Some(now))
        .await?;

    // Only recent events should be included
    assert!(
        counts.get("fs").unwrap_or(&0) >= &2,
        "Should have some recent filesystem events"
    );
    assert!(
        counts.get("shell.kitty").unwrap_or(&0) >= &5,
        "Should have recent shell events"
    );
    assert!(
        counts.get("wm.hyprland").unwrap_or(&0) >= &1,
        "Should have recent window events"
    );

    // Old system event should not be included
    assert_eq!(
        counts.get("system"),
        None,
        "System events should not be in recent timeframe"
    );

    Ok(())
}

#[sinex_test]
async fn test_get_event_count_by_type_no_time_filter(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let counts = service.get_event_count_by_type(None, None).await?;

    // Verify expected event type counts
    assert_eq!(
        counts.get("file.created"),
        Some(&5),
        "file.created events should be 5"
    );
    assert_eq!(
        counts.get("command.executed"),
        Some(&19),
        "command.executed events should be 19"
    );
    assert_eq!(
        counts.get("window.opened"),
        Some(&3),
        "window.opened events should be 3"
    );
    assert_eq!(counts.get("copied"), Some(&1), "copied events should be 1");
    assert_eq!(
        counts.get("boot.completed"),
        Some(&1),
        "boot.completed events should be 1"
    );

    Ok(())
}

#[sinex_test]
async fn test_get_event_count_by_type_with_time_filter(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let now = Utc::now();
    let two_hours_ago = now - Duration::hours(2);

    let counts = service
        .get_event_count_by_type(Some(two_hours_ago), Some(now))
        .await?;

    // Should have recent events but not old system events
    assert!(
        counts.get("file.created").unwrap_or(&0) >= &3,
        "Should have recent file events"
    );
    assert!(
        counts.get("command.executed").unwrap_or(&0) >= &10,
        "Should have recent command events"
    );

    // Old boot event should not be included
    assert_eq!(
        counts.get("boot.completed"),
        None,
        "Boot events should not be in recent timeframe"
    );

    Ok(())
}

#[sinex_test]
async fn test_get_events_over_time_hourly_intervals(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let now = Utc::now();
    let three_hours_ago = now - Duration::hours(3);

    let time_series = service
        .get_events_over_time(three_hours_ago, now, 60)
        .await?;

    // Should have time buckets with events
    assert!(!time_series.is_empty(), "Should have time series data");

    // Verify buckets are in ascending order
    for window in time_series.windows(2) {
        let (prev, curr) = (&window[0], &window[1]);
        assert!(
            prev.0 <= curr.0,
            "Time buckets should be in ascending order"
        );
    }

    // Verify total count matches expected recent events
    let total_events: i64 = time_series.iter().map(|(_, count)| count).sum();
    assert!(
        total_events >= 20,
        "Should have reasonable number of events in timeframe"
    );

    Ok(())
}

#[sinex_test]
async fn test_get_events_over_time_different_intervals(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let now = Utc::now();
    let six_hours_ago = now - Duration::hours(6);

    // Test 30-minute intervals
    let thirty_min_data = service.get_events_over_time(six_hours_ago, now, 30).await?;

    // Test 2-hour intervals
    let two_hour_data = service
        .get_events_over_time(six_hours_ago, now, 120)
        .await?;

    // Smaller intervals should have more buckets
    assert!(
        thirty_min_data.len() >= two_hour_data.len(),
        "30-minute intervals should have more buckets than 2-hour intervals"
    );

    // Total counts should be the same
    let thirty_min_total: i64 = thirty_min_data.iter().map(|(_, count)| count).sum();
    let two_hour_total: i64 = two_hour_data.iter().map(|(_, count)| count).sum();
    assert_eq!(
        thirty_min_total, two_hour_total,
        "Total event counts should match across different intervals"
    );

    Ok(())
}

#[sinex_test]
async fn test_get_top_commands_no_time_filter(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let top_commands = service.get_top_commands(None, None, 10).await?;

    // Should be ordered by frequency (descending)
    assert_eq!(top_commands.len(), 5, "Should have 5 different commands");
    assert_eq!(
        top_commands[0],
        ("git status".to_string(), 8),
        "Most frequent should be git status"
    );
    assert_eq!(
        top_commands[1],
        ("cargo build".to_string(), 5),
        "Second should be cargo build"
    );
    assert_eq!(
        top_commands[2],
        ("ls -la".to_string(), 3),
        "Third should be ls -la"
    );
    assert_eq!(
        top_commands[3],
        ("cd /home".to_string(), 2),
        "Fourth should be cd /home"
    );
    assert_eq!(
        top_commands[4],
        ("vim file.rs".to_string(), 1),
        "Fifth should be vim file.rs"
    );

    Ok(())
}

#[sinex_test]
async fn test_get_top_commands_with_limit(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let top_3_commands = service.get_top_commands(None, None, 3).await?;

    // Should respect limit
    assert_eq!(top_3_commands.len(), 3, "Should only return top 3 commands");
    assert_eq!(top_3_commands[0].1, 8, "First should have count 8");
    assert_eq!(top_3_commands[1].1, 5, "Second should have count 5");
    assert_eq!(top_3_commands[2].1, 3, "Third should have count 3");

    Ok(())
}

#[sinex_test]
async fn test_get_top_commands_with_time_filter(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    let now = Utc::now();
    let thirty_minutes_ago = now - Duration::minutes(30);

    let recent_commands = service
        .get_top_commands(Some(thirty_minutes_ago), Some(now), 10)
        .await?;

    // Should have fewer commands due to time filtering
    assert!(
        !recent_commands.is_empty(),
        "Should have some recent commands"
    );

    // Verify each command has reasonable count for the timeframe
    for (command, count) in &recent_commands {
        assert!(count <= &8, "No command should exceed total count");
        assert!(
            count >= &1,
            "Each command should have at least 1 occurrence"
        );
        assert!(!command.is_empty(), "Commands should not be empty");
    }

    Ok(())
}

#[sinex_test]
async fn test_analytics_with_empty_database(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    // Test all methods with empty database
    let source_counts = service.get_event_count_by_source(None, None).await?;
    assert!(source_counts.is_empty(), "Should have empty source counts");

    let type_counts = service.get_event_count_by_type(None, None).await?;
    assert!(type_counts.is_empty(), "Should have empty type counts");

    let now = Utc::now();
    let one_hour_ago = now - Duration::hours(1);
    let time_series = service.get_events_over_time(one_hour_ago, now, 60).await?;
    assert!(time_series.is_empty(), "Should have empty time series");

    let top_commands = service.get_top_commands(None, None, 10).await?;
    assert!(top_commands.is_empty(), "Should have empty commands list");

    Ok(())
}

#[sinex_test]
async fn test_analytics_with_single_event(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    // Create single test event
    create_analytics_test_event(
        &pool,
        "test.source",
        "test.event",
        json!({"test": "data"}),
        None,
    )
    .await?;

    let source_counts = service.get_event_count_by_source(None, None).await?;
    assert_eq!(source_counts.len(), 1, "Should have exactly one source");
    assert_eq!(
        source_counts.get("test.source"),
        Some(&1),
        "Source should have count 1"
    );

    let type_counts = service.get_event_count_by_type(None, None).await?;
    assert_eq!(type_counts.len(), 1, "Should have exactly one event type");
    assert_eq!(
        type_counts.get("test.event"),
        Some(&1),
        "Event type should have count 1"
    );

    Ok(())
}

#[sinex_test]
async fn test_analytics_time_range_edge_cases(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    let now = Utc::now();

    // Create event exactly at boundary
    create_analytics_test_event(
        &pool,
        "boundary.test",
        "boundary.event",
        json!({"boundary": true}),
        Some(Duration::hours(1)), // 1 hour ago
    )
    .await?;

    // Test time range that exactly includes the event
    let exactly_one_hour_ago = now - Duration::hours(1);
    let source_counts = service
        .get_event_count_by_source(Some(exactly_one_hour_ago), Some(now))
        .await?;

    // Should include the boundary event
    assert_eq!(
        source_counts.get("boundary.test").unwrap_or(&0),
        &1,
        "Should include event at exact boundary"
    );

    // Test time range that excludes the event
    let fifty_minutes_ago = now - Duration::minutes(50);
    let source_counts_excluded = service
        .get_event_count_by_source(Some(fifty_minutes_ago), Some(now))
        .await?;

    // Should not include the boundary event
    assert_eq!(
        source_counts_excluded.get("boundary.test"),
        None,
        "Should exclude event outside boundary"
    );

    Ok(())
}

#[sinex_test]
async fn test_get_top_commands_only_command_events(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    // Create mixed events - only command.executed should be included
    create_analytics_test_event(
        &pool,
        "shell.kitty",
        "command.executed",
        json!({"command": "test command"}),
        None,
    )
    .await?;

    create_analytics_test_event(
        &pool,
        "shell.kitty",
        "session.started",
        json!({"shell": "bash"}),
        None,
    )
    .await?;

    create_analytics_test_event(
        &pool,
        "fs",
        "file.created",
        json!({"path": "/test", "command": "not a real command"}),
        None,
    )
    .await?;

    let top_commands = service.get_top_commands(None, None, 10).await?;

    // Should only find the one actual command event
    assert_eq!(
        top_commands.len(),
        1,
        "Should only find command.executed events"
    );
    assert_eq!(
        top_commands[0],
        ("test command".to_string(), 1),
        "Should find the test command"
    );

    Ok(())
}

#[sinex_test]
async fn test_analytics_aggregation_accuracy(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    // Create precisely controlled test data
    let test_sources = ["source_a", "source_b", "source_c"];
    let test_types = ["type_x", "type_y"];

    let mut expected_source_counts = HashMap::new();
    let mut expected_type_counts = HashMap::new();

    // Create events with known distribution
    for (i, source) in test_sources.iter().enumerate() {
        for (j, event_type) in test_types.iter().enumerate() {
            let count = (i + 1) * (j + 1); // source_a: 1,2  source_b: 2,4  source_c: 3,6

            for _ in 0..count {
                create_analytics_test_event(
                    &pool,
                    source,
                    event_type,
                    json!({"test": "precision"}),
                    None,
                )
                .await?;
            }

            *expected_source_counts
                .entry(source.to_string())
                .or_insert(0) += count as i64;
            *expected_type_counts
                .entry(event_type.to_string())
                .or_insert(0) += count as i64;
        }
    }

    let source_counts = service.get_event_count_by_source(None, None).await?;
    let type_counts = service.get_event_count_by_type(None, None).await?;

    // Verify exact accuracy
    for (source, expected_count) in expected_source_counts {
        assert_eq!(
            source_counts.get(&source),
            Some(&expected_count),
            "Source {} should have exactly {} events",
            source,
            expected_count
        );
    }

    for (event_type, expected_count) in expected_type_counts {
        assert_eq!(
            type_counts.get(&event_type),
            Some(&expected_count),
            "Event type {} should have exactly {} events",
            event_type,
            expected_count
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_activity_heatmap_legacy_method(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    setup_analytics_test_data(&pool).await?;

    // Test the legacy activity_heatmap method
    let heatmap = service.activity_heatmap(60, 10).await?;

    // Should have some activity periods
    assert!(!heatmap.is_empty(), "Should have activity data");

    // Should be ordered by count (descending)
    for window in heatmap.windows(2) {
        let (prev, curr) = (&window[0], &window[1]);
        assert!(
            prev.1 >= curr.1,
            "Heatmap should be ordered by count descending"
        );
    }

    // All counts should be positive
    for (timestamp, count) in &heatmap {
        assert!(count > &0, "All activity counts should be positive");
        assert!(
            timestamp <= &Utc::now(),
            "All timestamps should be in the past"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_analytics_large_dataset_performance(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let service = AnalyticsService::new(pool.clone());

    // Create a larger dataset to test performance
    let start_time = std::time::Instant::now();

    for i in 0..100 {
        create_analytics_test_event(
            &pool,
            &format!("perf_source_{}", i % 5),
            &format!("perf_type_{}", i % 3),
            json!({"sequence": i, "performance_test": true}),
            Some(Duration::minutes(i % 60)),
        )
        .await?;
    }

    let setup_duration = start_time.elapsed();
    println!("Setup 100 events in {:?}", setup_duration);

    // Test analytics methods performance
    let analytics_start = std::time::Instant::now();

    let source_counts = service.get_event_count_by_source(None, None).await?;
    let type_counts = service.get_event_count_by_type(None, None).await?;
    let top_commands = service.get_top_commands(None, None, 10).await?;

    let analytics_duration = analytics_start.elapsed();
    println!("Analytics queries completed in {:?}", analytics_duration);

    // Verify results are reasonable
    assert!(source_counts.len() <= 5, "Should have at most 5 sources");
    assert!(type_counts.len() <= 3, "Should have at most 3 event types");

    // Performance should be reasonable (less than 1 second for 100 events)
    assert!(
        analytics_duration.as_secs() < 1,
        "Analytics should complete quickly"
    );

    Ok(())
}
