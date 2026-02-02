// Service integration tests covering cross-service flows.

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::EventSource;
use sinex_services::AnalyticsService;
use std::sync::Arc;
use std::time::Instant;
use time::Duration;
use xtask::sandbox::dataset_seeds::{seed_events_via_scope, EventSpec, SeedClock};
use xtask::sandbox::prelude::*;

/// Test cross-service data flow: Event creation -> Analytics -> Repository queries
#[sinex_test]
async fn test_cross_service_data_flow(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing cross-service data flow integration");
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::new();

    // Create a semantic dataset inline
    let events = vec![
        EventSpec::new("shell.bash", "command.executed")
            .with_payload(json!({"command": "ls", "exit_code": 0})),
        EventSpec::new("shell.bash", "command.executed")
            .with_payload(json!({"command": "git status", "exit_code": 0})),
        EventSpec::new("fs-watcher", "file.created")
            .with_payload(json!({"path": "/tmp/test.txt", "size": 100})),
    ];
    seed_events_via_scope(scope.ctx(), &clock, events).await?;
    scope.wait_for_event_count(3).await?;

    let analytics = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let source_counts = analytics.get_event_count_by_source(None, None).await?;
    assert_eq!(source_counts.get("shell.bash"), Some(&2));
    assert_eq!(source_counts.get("fs-watcher"), Some(&1));

    let type_counts = analytics.get_event_count_by_type(None, None).await?;
    assert_eq!(type_counts.get("command.executed"), Some(&2));
    assert_eq!(type_counts.get("file.created"), Some(&1));

    let events_by_source = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("fs-watcher"),
            sinex_primitives::Pagination::new(Some(10), None),
        )
        .await?;
    assert_eq!(events_by_source.len(), 1, "Should find fs-watcher event");

    let recent_events = ctx.pool.events().get_recent(10).await?;
    assert!(recent_events.len() >= 3, "Should find all recent events");

    tracing::info!("Cross-service integration test completed successfully");
    scope.shutdown().await?;
    Ok(())
}

/// Test service initialization and basic functionality
#[sinex_test]
async fn test_service_initialization(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    tracing::info!("Testing service initialization");

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let counts = service.get_event_count_by_source(None, None).await?;
    assert!(
        counts.is_empty(),
        "Empty database should return empty counts"
    );

    let clock = SeedClock::new();
    seed_events_via_scope(
        scope.ctx(),
        &clock,
        vec![EventSpec::new("test-source", "test.event").with_payload(json!({"test": "data"}))],
    )
    .await?;

    scope.wait_for_source_events("test-source", 1).await?;
    let updated_counts = service.get_event_count_by_source(None, None).await?;
    assert_eq!(updated_counts.get("test-source"), Some(&1));

    scope.shutdown().await?;
    Ok(())
}

/// Test service error handling and resilience
#[sinex_test]
async fn test_service_error_handling(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing service error handling patterns");
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::new();

    seed_events_via_scope(
        scope.ctx(),
        &clock,
        vec![
            EventSpec::new("error-test", "problematic.event").with_payload(json!({
                "malformed_json": "{ incomplete json",
                "null_values": null,
                "empty_string": "",
                "very_long_content": "x".repeat(100000),
                "special_characters": "Special chars with SQL'; DROP TABLE events; --"
            })),
        ],
    )
    .await?;

    scope.wait_for_source_events("error-test", 1).await?;

    let analytics = AnalyticsService::new(ctx.pool.clone());
    let counts = analytics.get_event_count_by_source(None, None).await?;
    assert_eq!(counts.get("error-test"), Some(&1));

    let events = ctx.pool.events().get_recent(10).await?;
    assert!(!events.is_empty(), "Should find the problematic event");

    let found_event = events.iter().find(|e| e.source.as_str() == "error-test");
    assert!(found_event.is_some(), "Should find error-test event");

    if let Some(event) = found_event {
        assert!(
            event.payload["special_characters"].is_string(),
            "Should handle special characters safely"
        );
    }

    scope.shutdown().await?;
    Ok(())
}

/// Test service performance under load
#[sinex_test]
#[ignore = "long"]
async fn test_service_performance_integration(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing service performance under load");
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let start_time = Instant::now();
    let clock = SeedClock::new();

    let desired_events = 24usize;
    let specs: Vec<EventSpec> = (0..desired_events)
        .map(|idx| {
            EventSpec::new(
                format!("perf-source-{}", idx % 5),
                format!("perf.event.{}", idx % 3),
            )
            .with_payload(json!({
                "sequence": idx,
                "data": format!("Performance test data item {}", idx),
                "metadata": {
                    "batch": idx / 10,
                    "category": if idx % 2 == 0 { "even" } else { "odd" }
                }
            }))
            .at(clock.tick(60_000)) // Advance by 1 minute each event
        })
        .collect();

    seed_events_via_scope(scope.ctx(), &clock, specs).await?;
    scope.wait_for_event_count(desired_events).await?;

    let setup_duration = start_time.elapsed();
    tracing::info!(
        "Created {} test events in {:?}",
        desired_events,
        setup_duration
    );

    let analytics_start = Instant::now();
    let analytics = AnalyticsService::new(ctx.pool.clone());
    let source_counts = analytics.get_event_count_by_source(None, None).await?;
    let type_counts = analytics.get_event_count_by_type(None, None).await?;
    let analytics_duration = analytics_start.elapsed();

    assert_eq!(source_counts.len(), 5, "Should have 5 different sources");
    assert_eq!(type_counts.len(), 3, "Should have 3 different event types");

    let total_events: i64 = source_counts.values().sum();
    assert!(
        total_events >= desired_events as i64,
        "Should count at least the inserted events"
    );

    let query_start = Instant::now();
    let recent_events = ctx.pool.events().get_recent(desired_events as i64).await?;
    let by_source_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("perf-source-1"),
            sinex_primitives::Pagination::new(Some(20), None),
        )
        .await?;
    let query_duration = query_start.elapsed();

    assert_eq!(recent_events.len(), desired_events);
    assert!(!by_source_events.is_empty());

    assert!(
        analytics_duration.as_millis() < 1000,
        "Analytics should complete quickly"
    );
    assert!(
        query_duration.as_millis() < 1000,
        "Queries should complete quickly"
    );

    scope.shutdown().await?;
    Ok(())
}

/// Test service lifecycle and cleanup
#[sinex_test]
async fn test_service_lifecycle(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing service lifecycle management");
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::new();

    let specs: Vec<EventSpec> = (0..5)
        .map(|i| {
            EventSpec::new("lifecycle-test", format!("lifecycle.event.{i}"))
                .with_payload(json!({
                    "step": i,
                    "description": format!("Lifecycle test step {}", i)
                }))
                .at(clock.tick(60_000)) // Advance by 1 minute each
        })
        .collect();
    seed_events_via_scope(scope.ctx(), &clock, specs).await?;
    scope.wait_for_source_events("lifecycle-test", 5).await?;

    {
        let analytics = AnalyticsService::new(ctx.pool.clone());
        let counts = analytics.get_event_count_by_source(None, None).await?;
        assert_eq!(counts.get("lifecycle-test"), Some(&5));
    }

    {
        let analytics = AnalyticsService::new(ctx.pool.clone());
        let counts = analytics.get_event_count_by_source(None, None).await?;
        assert_eq!(
            counts.get("lifecycle-test"),
            Some(&5),
            "Data should persist across service restarts"
        );
    }

    let analytics1 = AnalyticsService::new(ctx.pool.clone());
    let analytics2 = AnalyticsService::new(ctx.pool.clone());

    let (counts1, counts2) = tokio::join!(
        analytics1.get_event_count_by_source(None, None),
        analytics2.get_event_count_by_source(None, None)
    );
    assert_eq!(counts1?, counts2?);

    scope.shutdown().await?;
    Ok(())
}

/// Test service integration with time-based operations
#[sinex_test]
async fn test_time_based_service_integration(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing time-based service integration");
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::new();
    let now = clock.now();

    let thirty_mins_ago = now - Duration::minutes(30);
    let one_day_ago = now - Duration::days(1);

    seed_events_via_scope(
        scope.ctx(),
        &clock,
        vec![
            EventSpec::new("time-test", "recent.event")
                .with_payload(json!({"description": "Recent event"}))
                .at(thirty_mins_ago),
            EventSpec::new("time-test", "old.event")
                .with_payload(json!({"description": "Old event"}))
                .at(one_day_ago),
        ],
    )
    .await?;

    scope.wait_for_event_count(2).await?;
    let analytics = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    let one_hour_ago = now - Duration::hours(1);
    let recent_counts = analytics
        .get_event_count_by_source(Some(one_hour_ago), Some(now))
        .await?;
    let all_counts = analytics.get_event_count_by_source(None, None).await?;

    assert_eq!(recent_counts.get("time-test"), Some(&1));
    assert_eq!(all_counts.get("time-test"), Some(&2));

    let three_hours_ago = now - Duration::hours(3);
    let time_series = analytics
        .get_events_over_time(three_hours_ago, now, 60)
        .await?;
    let total_in_series: i64 = time_series.iter().map(|(_, count)| *count).sum();
    assert_eq!(total_in_series, 1);

    scope.shutdown().await?;
    Ok(())
}

/// Test service configuration patterns
#[sinex_test]
async fn test_service_configuration(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing service configuration patterns");
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::new();

    let analytics = AnalyticsService::new(ctx.pool.clone());
    seed_events_via_scope(
        scope.ctx(),
        &clock,
        vec![
            EventSpec::new("config-test", "config.event").with_payload(json!({
                "configuration": "test",
                "service_integration": true
            })),
        ],
    )
    .await?;

    scope.wait_for_source_events("config-test", 1).await?;
    let counts = analytics.get_event_count_by_source(None, None).await?;
    assert_eq!(counts.get("config-test"), Some(&1));

    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("config-test"),
            sinex_primitives::Pagination::new(Some(10), None),
        )
        .await?;
    assert_eq!(events.len(), 1);

    scope.shutdown().await?;
    Ok(())
}

/// Test error propagation across services
#[sinex_test]
async fn test_cross_service_error_handling(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing cross-service error handling");
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::new();

    seed_events_via_scope(
        scope.ctx(),
        &clock,
        vec![
            EventSpec::new("error-propagation", "error.event").with_payload(json!({
                "potentially_problematic": true,
                "large_field": "x".repeat(10000),
                "special_chars": "Testing 'quotes' and \"double quotes\" and `backticks`"
            })),
        ],
    )
    .await?;

    scope.wait_for_source_events("error-propagation", 1).await?;

    let analytics = AnalyticsService::new(ctx.pool.clone());
    let analytics_result = analytics.get_event_count_by_source(None, None).await;
    assert!(
        analytics_result.is_ok(),
        "Analytics should handle problematic events"
    );

    let events = ctx.pool.events().get_recent(10).await?;
    assert!(!events.is_empty(), "Should find the problematic event");

    let found_event = events
        .iter()
        .find(|e| e.source.as_str() == "error-propagation");
    assert!(found_event.is_some(), "Should find error-propagation event");

    scope.shutdown().await?;
    Ok(())
}
