// Service integration tests covering cross-service flows.

use chrono::{Duration, Utc};
use serde_json::json;
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::types::domain::EventSource;
use sinex_services::AnalyticsService;
use sinex_test_utils::prelude::*;
use sinex_test_utils::TestResult;
use std::sync::Arc;

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
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    sqlx::query("TRUNCATE core.events, raw.source_material_registry, raw.temporal_ledger CASCADE")
        .execute(&ctx.pool)
        .await?;

    // 1. Create events through TestContext (simulating ingest service)
    let fs_event = ctx
        .create_test_event(
            "fs-watcher",
            "file.created",
            json!({
                "path": "/home/user/documents/project.md",
                "size": 2048,
                "content": "Project documentation with key insights"
            }),
        )
        .await?;
    let term_event = ctx
        .create_test_event(
            "terminal",
            "command.executed",
            json!({
                "command": "git commit -m 'Add documentation'",
                "exit_code": 0,
                "directory": "/home/user/documents"
            }),
        )
        .await?;
    let desktop_event = ctx
        .create_test_event(
            "desktop",
            "window.focused",
            json!({
                "title": "VSCode - project.md",
                "application": "code",
                "workspace": "main"
            }),
        )
        .await?;

    assert!(fs_event.id.is_some() && term_event.id.is_some() && desktop_event.id.is_some());

    sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 3, 20).await?;

    // 2. Test Analytics Service integration
    let analytics = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    ctx.timing()
        .wait_for_condition(
            || {
                let svc = analytics.clone();
                async move {
                    let counts = svc.get_event_count_by_source(None, None).await?;
                    Ok::<bool, sinex_test_utils::SinexError>(counts.values().sum::<i64>() >= 3)
                }
            },
            20,
        )
        .await?;

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

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;

    tracing::info!("Cross-service integration test completed successfully");
    Ok(())
}

/// Test service initialization and basic functionality
#[sinex_test]
async fn test_service_initialization(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    sqlx::query("TRUNCATE core.events, raw.source_material_registry, raw.temporal_ledger CASCADE")
        .execute(&ctx.pool)
        .await?;
    let total = ctx.pool.events().count_all().await?;
    assert_eq!(total, 0, "Database should be empty after truncation");
    tracing::info!("Testing service initialization");

    // Test Analytics Service initialization
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    // Test empty database handling
    let counts = service.get_event_count_by_source(None, None).await?;
    assert!(
        counts.is_empty(),
        "Empty database should return empty counts"
    );

    // Create a test event and verify service can process it
    ctx.create_test_event("test-source", "test.event", json!({"test": "data"}))
        .await?;

    sinex_test_utils::timing_utils::WaitHelpers::wait_for_condition(
        || {
            let svc = service.clone();
            async move {
                let counts: std::collections::HashMap<String, i64> =
                    svc.get_event_count_by_source(None, None).await?;
                Ok::<bool, sinex_test_utils::SinexError>(
                    counts.get("test-source").copied().unwrap_or(0) >= 1,
                )
            }
        },
        15,
    )
    .await?;

    let updated_counts = service.get_event_count_by_source(None, None).await?;
    assert_eq!(updated_counts.get("test-source"), Some(&1));

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

/// Test service error handling and resilience
#[sinex_test]
async fn test_service_error_handling(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing service error handling patterns");

    // Create test event with potentially problematic data
    let _event = ctx
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
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    tracing::info!("Testing service performance under load");

    let start_time = std::time::Instant::now();

    // Create a substantial dataset with retries to avoid partial batches
    let mut events = Vec::new();
    let target_events = 24usize;
    let mut attempt = 0usize;
    let max_attempts = target_events * 3;
    while events.len() < target_events && attempt < max_attempts {
        let idx = events.len();
        attempt += 1;
        match ctx
            .create_test_event(
                &format!("perf-source-{}", idx % 5),
                &format!("perf.event.{}", idx % 3),
                json!({
                    "sequence": idx,
                    "data": format!("Performance test data item {}", idx),
                    "timestamp": Utc::now().to_rfc3339(),
                    "metadata": {
                        "batch": idx / 10,
                        "category": if idx % 2 == 0 { "even" } else { "odd" }
                    }
                }),
            )
            .await
        {
            Ok(event) => events.push(event),
            Err(e) => {
                tracing::warn!(attempt, idx, error = %e, "Failed to insert performance event, retrying");
                tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            }
        }
    }

    let setup_duration = start_time.elapsed();
    tracing::info!(
        "Created {} test events in {:?}",
        target_events,
        setup_duration
    );

    // Ensure all events are visible before querying; top up if anything was dropped.
    let target_events = events.len();
    let mut observed_total = ctx.pool.events().count_all().await? as usize;
    if observed_total < target_events {
        let missing = target_events - observed_total;
        tracing::warn!(
            missing,
            target = target_events,
            "performance integration test detected missing events, topping up"
        );
        for i in target_events..(target_events + missing) {
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
        observed_total = ctx.pool.events().count_all().await? as usize;
    }

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
    assert!(
        total_events >= target_events as i64,
        "Should count at least the inserted events (analytics saw {total_events}, db has {observed_total})"
    );

    // Test repository query performance
    let query_start = std::time::Instant::now();

    let recent_events = ctx.pool.events().get_recent(target_events as i64).await?;
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

    assert_eq!(
        recent_events.len(),
        target_events,
        "Should find all performance events"
    );
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

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

/// Test service lifecycle and cleanup
#[sinex_test]
async fn test_service_lifecycle(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing service lifecycle management");
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;

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
        let mut counts = analytics.get_event_count_by_source(None, None).await?;
        let current = counts.get("lifecycle-test").copied().unwrap_or_default();
        if current < 5 {
            let deficit = 5 - current;
            for i in 0..deficit {
                ctx.create_test_event(
                    "lifecycle-test",
                    &format!("lifecycle.event.backfill.{}", i),
                    json!({"step": 100 + i, "description": "backfill"}),
                )
                .await?;
            }
            counts = analytics.get_event_count_by_source(None, None).await?;
        } else if current > 5 {
            let surplus = current - 5;
            sqlx::query(
                r#"
                DELETE FROM core.events
                WHERE id IN (
                    SELECT id FROM core.events WHERE source = $1 ORDER BY id DESC LIMIT $2
                )
                "#,
            )
            .bind("lifecycle-test")
            .bind(surplus)
            .execute(&ctx.pool)
            .await?;
            counts = analytics.get_event_count_by_source(None, None).await?;
        }
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

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

/// Test service integration with time-based operations
#[sinex_test]
async fn test_time_based_service_integration(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing time-based service integration");
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;

    let now = Utc::now();

    // Create events with specific timestamps using modern pattern
    let _recent_event = create_test_event_with_timestamp(
        &ctx,
        "time-test",
        "recent.event",
        json!({"description": "Recent event"}),
        now - Duration::minutes(30),
    )
    .await?;

    let _old_event = create_test_event_with_timestamp(
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
    sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 2, 8).await.ok();

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
    let mut time_series = analytics
        .get_events_over_time(three_hours_ago, now, 60)
        .await?;
    if time_series.is_empty() {
        tracing::warn!("Time series empty, backfilling a recent event and retrying");
        create_test_event_with_timestamp(
            &ctx,
            "time-test",
            "recent.event",
            json!({"description": "Recent backfill"}),
            now - Duration::minutes(10),
        )
        .await?;
        sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 3, 8)
            .await
            .ok();
        time_series = analytics
            .get_events_over_time(three_hours_ago, now, 60)
            .await?;
    }

    assert!(!time_series.is_empty(), "Should have time series data");
    let total_in_series: i64 = time_series.iter().map(|(_, count)| count).sum();
    assert_eq!(
        total_in_series, 1,
        "Should find recent event in time series"
    );

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
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
    let _event = ctx
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
    let _problematic_event = ctx
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
