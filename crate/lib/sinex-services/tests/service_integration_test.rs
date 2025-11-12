// Service integration tests covering cross-service flows.

use chrono::{Duration, Utc};
use serde_json::json;
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::types::domain::{EventSource, EventType};
use sinex_services::AnalyticsService;
use sinex_test_utils::prelude::*;
use sinex_test_utils::TestResult;

// =============================================================================
// SERVICE INTEGRATION PATTERNS
// =============================================================================

/// Helper to create test events with specific timestamps using modern patterns
async fn create_test_event_with_timestamp(
    ctx: &TestContext,
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
    timestamp: chrono::DateTime<Utc>,
) -> TestResult<sinex_core::db::models::Event> {
    let event = sinex_core::Event::test_event(
        sinex_core::types::domain::EventSource::from(source),
        sinex_core::types::domain::EventType::from(event_type),
        payload,
    )
    .at_time(timestamp);

    ctx.pool.events().insert(event).await.map_err(Into::into)
}

/// Test cross-service data flow: Event creation -> Analytics -> Repository queries
#[sinex_test]
async fn test_cross_service_data_flow(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing cross-service data flow integration");

    // 1. Create events through TestContext (simulating ingest service)
    let events = vec![
        ctx.create_test_event(
            "fs-watcher",
            "file.created",
            json!({
                "path": "/home/user/documents/project.md",
                "size": 2048,
                "content": "Project documentation with key insights"
            }),
        )
        .await?,
        ctx.create_test_event(
            "terminal",
            "command.executed",
            json!({
                "command": "git commit -m 'Add documentation'",
                "exit_code": 0,
                "directory": "/home/user/documents"
            }),
        )
        .await?,
        ctx.create_test_event(
            "desktop",
            "window.focused",
            json!({
                "title": "VSCode - project.md",
                "application": "code",
                "workspace": "main"
            }),
        )
        .await?,
    ];

    // 2. Test Analytics Service integration
    let analytics = AnalyticsService::new(ctx.pool.clone());

    let source_counts = analytics.get_event_count_by_source(None, None).await?;
    assert_eq!(source_counts.get("fs-watcher"), Some(&1));
    assert_eq!(source_counts.get("terminal"), Some(&1));
    assert_eq!(source_counts.get("desktop"), Some(&1));

    let type_counts = analytics.get_event_count_by_type(None, None).await?;
    assert_eq!(type_counts.get("file.created"), Some(&1));
    assert_eq!(type_counts.get("command.executed"), Some(&1));
    assert_eq!(type_counts.get("window.focused"), Some(&1));

    // 3. Test repository pattern queries
    let events_by_source = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("fs-watcher"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    assert_eq!(events_by_source.len(), 1, "Should find fs-watcher event");

    // 4. Test recent events query
    let recent_events = ctx.pool.events().get_recent(10).await?;
    assert!(recent_events.len() >= 3, "Should find all recent events");

    tracing::info!("Cross-service integration test completed successfully");
    Ok(())
}

/// Test service initialization and basic functionality
#[sinex_test]
async fn test_service_initialization(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing service initialization");

    // Test Analytics Service initialization
    let service = AnalyticsService::new(ctx.pool.clone());

    // Test empty database handling
    let counts = service.get_event_count_by_source(None, None).await?;
    assert!(
        counts.is_empty(),
        "Empty database should return empty counts"
    );

    // Create a test event and verify service can process it
    ctx.create_test_event("test-source", "test.event", json!({"test": "data"}))
        .await?;

    let updated_counts = service.get_event_count_by_source(None, None).await?;
    assert_eq!(updated_counts.get("test-source"), Some(&1));

    Ok(())
}

/// Test service error handling and resilience
#[sinex_test]
async fn test_service_error_handling(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing service error handling patterns");

    // Create test event with potentially problematic data
    let event = ctx
        .create_test_event(
            "error-test",
            "problematic.event",
            json!({
                "malformed_json": "{ incomplete json",
                "null_values": null,
                "empty_string": "",
                "very_long_content": "x".repeat(100000),
                "special_characters": "🚀💻🔥 Special chars with SQL'; DROP TABLE events; --"
            }),
        )
        .await?;

    // Test Analytics Service resilience
    let analytics = AnalyticsService::new(ctx.pool.clone());
    let counts = analytics.get_event_count_by_source(None, None).await?;
    assert_eq!(counts.get("error-test"), Some(&1));

    // Test repository operations with problematic data
    let events = ctx.pool.events().get_recent(10).await?;
    assert!(!events.is_empty(), "Should find the problematic event");

    // Test database integrity with special characters
    let found_event = events.iter().find(|e| e.source.as_str() == "error-test");
    assert!(found_event.is_some(), "Should find error-test event");

    if let Some(event) = found_event {
        assert!(
            event.payload["special_characters"].is_string(),
            "Should handle special characters safely"
        );
    }

    Ok(())
}

/// Test service performance under load
#[sinex_test]
async fn test_service_performance_integration(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing service performance under load");

    let start_time = std::time::Instant::now();

    // Create a substantial dataset
    let mut events = Vec::new();
    for i in 0..50 {
        let event = ctx
            .create_test_event(
                &format!("perf-source-{}", i % 5),
                &format!("perf.event.{}", i % 3),
                json!({
                    "sequence": i,
                    "data": format!("Performance test data item {}", i),
                    "timestamp": Utc::now().to_rfc3339(),
                    "metadata": {
                        "batch": i / 10,
                        "category": if i % 2 == 0 { "even" } else { "odd" }
                    }
                }),
            )
            .await?;
        events.push(event);
    }

    let setup_duration = start_time.elapsed();
    tracing::info!("Created 50 test events in {:?}", setup_duration);

    // Test Analytics Service performance
    let analytics_start = std::time::Instant::now();
    let analytics = AnalyticsService::new(ctx.pool.clone());

    let source_counts = analytics.get_event_count_by_source(None, None).await?;
    let type_counts = analytics.get_event_count_by_type(None, None).await?;

    let analytics_duration = analytics_start.elapsed();
    tracing::info!("Analytics queries completed in {:?}", analytics_duration);

    // Verify analytics results
    assert_eq!(source_counts.len(), 5, "Should have 5 different sources");
    assert_eq!(type_counts.len(), 3, "Should have 3 different event types");

    let total_events: i64 = source_counts.values().sum();
    assert_eq!(total_events, 50, "Should count all 50 events");

    // Test repository query performance
    let query_start = std::time::Instant::now();

    let recent_events = ctx.pool.events().get_recent(50).await?;
    let by_source_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("perf-source-1"),
            sinex_core::types::Pagination::new(Some(20), None),
        )
        .await?;

    let query_duration = query_start.elapsed();
    tracing::info!("Repository queries completed in {:?}", query_duration);

    assert_eq!(recent_events.len(), 50, "Should find all 50 events");
    assert!(
        !by_source_events.is_empty(),
        "Should find source-specific events"
    );

    // Performance assertions (reasonable thresholds for CI)
    assert!(
        analytics_duration.as_millis() < 1000,
        "Analytics should complete quickly"
    );
    assert!(
        query_duration.as_millis() < 1000,
        "Queries should complete quickly"
    );

    Ok(())
}

/// Test service lifecycle and cleanup
#[sinex_test]
async fn test_service_lifecycle(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing service lifecycle management");

    // Create test data
    for i in 0..5 {
        ctx.create_test_event(
            "lifecycle-test",
            &format!("lifecycle.event.{}", i),
            json!({
                "step": i,
                "description": format!("Lifecycle test step {}", i)
            }),
        )
        .await?;
    }

    // Test service creation and operation
    {
        let analytics = AnalyticsService::new(ctx.pool.clone());
        let counts = analytics.get_event_count_by_source(None, None).await?;
        assert_eq!(counts.get("lifecycle-test"), Some(&5));
    } // Service drops here

    // Test service recreation (simulating restart)
    {
        let analytics = AnalyticsService::new(ctx.pool.clone());
        let counts = analytics.get_event_count_by_source(None, None).await?;
        assert_eq!(
            counts.get("lifecycle-test"),
            Some(&5),
            "Data should persist across service restarts"
        );
    }

    // Test multiple concurrent service instances
    let analytics1 = AnalyticsService::new(ctx.pool.clone());
    let analytics2 = AnalyticsService::new(ctx.pool.clone());

    let (counts1, counts2) = tokio::join!(
        analytics1.get_event_count_by_source(None, None),
        analytics2.get_event_count_by_source(None, None)
    );

    let counts1 = counts1?;
    let counts2 = counts2?;

    assert_eq!(
        counts1, counts2,
        "Concurrent service instances should return consistent results"
    );

    Ok(())
}

/// Test service integration with time-based operations
#[sinex_test]
async fn test_time_based_service_integration(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing time-based service integration");

    let now = Utc::now();

    // Create events with specific timestamps using modern pattern
    let recent_event = create_test_event_with_timestamp(
        &ctx,
        "time-test",
        "recent.event",
        json!({"description": "Recent event"}),
        now - Duration::minutes(30),
    )
    .await?;

    let old_event = create_test_event_with_timestamp(
        &ctx,
        "time-test",
        "old.event",
        json!({"description": "Old event"}),
        now - Duration::days(1),
    )
    .await?;

    // Test Analytics with time filtering
    let analytics = AnalyticsService::new(ctx.pool.clone());

    let one_hour_ago = now - Duration::hours(1);
    let recent_counts = analytics
        .get_event_count_by_source(Some(one_hour_ago), Some(now))
        .await?;
    let all_counts = analytics.get_event_count_by_source(None, None).await?;

    assert_eq!(
        recent_counts.get("time-test"),
        Some(&1),
        "Should find only recent event"
    );
    assert_eq!(
        all_counts.get("time-test"),
        Some(&2),
        "Should find both events without time filter"
    );

    // Test time series analysis
    let three_hours_ago = now - Duration::hours(3);
    let time_series = analytics
        .get_events_over_time(three_hours_ago, now, 60)
        .await?;

    assert!(!time_series.is_empty(), "Should have time series data");
    let total_in_series: i64 = time_series.iter().map(|(_, count)| count).sum();
    assert_eq!(
        total_in_series, 1,
        "Should find recent event in time series"
    );

    Ok(())
}

/// Test service configuration patterns
#[sinex_test]
async fn test_service_configuration(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing service configuration patterns");

    // Service should accept the pool and work with repository pattern
    let pool = ctx.pool.clone();
    let analytics = AnalyticsService::new(pool.clone());

    // Create test event
    let event = ctx
        .create_test_event(
            "config-test",
            "config.event",
            json!({
                "configuration": "test",
                "service_integration": true
            }),
        )
        .await?;

    // Verify service can operate on the data
    let counts = analytics.get_event_count_by_source(None, None).await?;
    assert_eq!(counts.get("config-test"), Some(&1));

    // Verify repository access works consistently
    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("config-test"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    assert_eq!(events.len(), 1);

    Ok(())
}

/// Test error propagation across services
#[sinex_test]
async fn test_cross_service_error_handling(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing cross-service error handling");

    // Create event that might cause issues
    let problematic_event = ctx
        .create_test_event(
            "error-propagation",
            "error.event",
            json!({
                "potentially_problematic": true,
                "large_field": "x".repeat(10000),
                "special_chars": "Testing 'quotes' and \"double quotes\" and `backticks`"
            }),
        )
        .await?;

    // Test that analytics service handles the event gracefully
    let analytics = AnalyticsService::new(ctx.pool.clone());

    let analytics_result = analytics.get_event_count_by_source(None, None).await;
    assert!(
        analytics_result.is_ok(),
        "Analytics should handle problematic events"
    );

    // Test repository queries with problematic data
    let events = ctx.pool.events().get_recent(10).await?;
    assert!(!events.is_empty(), "Should find the problematic event");

    // Verify data integrity
    let found_event = events
        .iter()
        .find(|e| e.source.as_str() == "error-propagation");
    assert!(found_event.is_some(), "Should find error-propagation event");

    Ok(())
}
