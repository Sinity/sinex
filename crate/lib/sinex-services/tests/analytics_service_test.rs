//! Analytics Service Integration Tests
//!
//! Comprehensive tests for AnalyticsService with focus on aggregation logic,
//! time-based filtering, and accurate data insights using modern infrastructure.

use chrono::{Duration as ChronoDuration, Utc};
use color_eyre::eyre::ensure;
use serde_json::json;
use sinex_core::types::Ulid;
use sinex_services::{AnalyticsService, SearchQuery, SearchService};
use sinex_test_utils::dataset_seeds::{
    seed_analytics_dataset_perf_via_scope, seed_analytics_dataset_semantic_min_via_scope,
    seed_events_via_scope, seed_query_dataset_semantic_min_via_scope, AnalyticsDataset, EventSpec,
    QueryDataset, SeedClock,
};
use sinex_test_utils::prelude::*;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration as TokioDuration};

async fn fetch_command_counts(ctx: &TestContext) -> TestResult<HashMap<String, i64>> {
    let rows = sqlx::query!(
        r#"
        SELECT payload->>'command' as command, COUNT(*) as "count!: i64"
        FROM core.events
        WHERE event_type = 'command.executed'
        GROUP BY payload->>'command'
        "#
    )
    .fetch_all(&ctx.pool)
    .await?;

    let mut counts = HashMap::new();
    for row in rows {
        if let Some(cmd) = row.command {
            counts.insert(cmd, row.count);
        }
    }
    Ok(counts)
}

async fn fetch_source_counts(ctx: &TestContext) -> TestResult<HashMap<String, i64>> {
    let rows = sqlx::query!(
        r#"
        SELECT source, COUNT(*) as count
        FROM core.events
        GROUP BY source
        "#
    )
    .fetch_all(&ctx.pool)
    .await?;

    let mut counts = HashMap::new();
    for row in rows {
        counts.insert(row.source, row.count.unwrap_or(0));
    }
    Ok(counts)
}

async fn assert_expected_command_counts(
    ctx: &TestContext,
    expected: &HashMap<String, i64>,
) -> TestResult<()> {
    let counts = fetch_command_counts(ctx).await?;
    for (command, expected_count) in expected {
        let actual = counts.get(command).copied().unwrap_or_default();
        ensure!(
            actual == *expected_count,
            "Command {} should have {} events (saw {})",
            command,
            expected_count,
            actual
        );
    }
    let total: i64 = counts.values().sum();
    let expected_total: i64 = expected.values().sum();
    ensure!(
        total == expected_total,
        "Expected {} total command events, saw {}",
        expected_total,
        total
    );
    Ok(())
}

async fn assert_expected_source_counts(
    ctx: &TestContext,
    expected: &HashMap<String, i64>,
) -> TestResult<()> {
    let counts = fetch_source_counts(ctx).await?;
    for (source, expected_count) in expected {
        let actual = counts.get(source).copied().unwrap_or_default();
        ensure!(
            actual == *expected_count,
            "Source {} should have {} events (saw {})",
            source,
            expected_count,
            actual
        );
    }
    let total: i64 = counts.values().sum();
    let expected_total: i64 = expected.values().sum();
    ensure!(
        total == expected_total,
        "Expected {} total analytics events, saw {}",
        expected_total,
        total
    );
    Ok(())
}

fn sorted_commands(expected: &HashMap<String, i64>) -> Vec<(String, i64)> {
    let mut entries: Vec<(String, i64)> = expected.iter().map(|(k, v)| (k.clone(), *v)).collect();
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    entries
}

async fn seed_analytics_dataset(
    scope: &PipelineScope<'_>,
) -> TestResult<(SeedClock, AnalyticsDataset)> {
    let clock = SeedClock::fixed();
    let dataset = seed_analytics_dataset_semantic_min_via_scope(scope, &clock).await?;
    Ok((clock, dataset))
}

async fn seed_query_dataset(scope: &PipelineScope<'_>) -> TestResult<(SeedClock, QueryDataset)> {
    let clock = SeedClock::fixed();
    let dataset = seed_query_dataset_semantic_min_via_scope(scope, &clock).await?;
    Ok((clock, dataset))
}

#[sinex_test]
async fn test_get_event_count_by_source_no_time_filter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let (_clock, dataset) = seed_analytics_dataset(&scope).await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    let counts = service.get_event_count_by_source(None, None).await?;
    assert_eq!(
        counts.len(),
        dataset.expected_source_counts.len(),
        "Should only see the expected sources"
    );
    for (source, expected) in dataset.expected_source_counts {
        assert_eq!(
            counts.get(&source),
            Some(&expected),
            "Source {} should have {} events",
            source,
            expected
        );
    }
    let total: i64 = counts.values().sum();
    assert_eq!(
        total, dataset.expected_total,
        "Expected exactly {} events after deterministic seeding",
        dataset.expected_total
    );

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_get_event_count_by_source_with_time_filter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let (clock, _dataset) = seed_analytics_dataset(&scope).await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    let now = clock.base();
    let one_hour_ago = now - ChronoDuration::hours(1);

    let counts = service
        .get_event_count_by_source(Some(one_hour_ago), Some(now))
        .await?;

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
    assert_eq!(
        counts.get("system"),
        None,
        "System events should not be in recent timeframe"
    );

    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn analytics_queries_block_each_other_with_single_connection(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(ctx.database_url())
        .await?;

    let service = Arc::new(AnalyticsService::new(pool.clone()));

    let blocker_pool = pool.clone();
    let (started_tx, started_rx) = oneshot::channel();
    let blocker = tokio::spawn(async move {
        let mut conn = blocker_pool.acquire().await.expect("pool acquire");
        let _ = started_tx.send(());
        sqlx::query!("SELECT pg_sleep(0.2)")
            .execute(&mut *conn)
            .await
            .expect("pg_sleep should succeed");
    });

    started_rx.await.expect("blocker should start");

    let svc = service.clone();
    let fast_call = tokio::spawn(async move { svc.get_event_count_by_type(None, None).await });

    let result = timeout(TokioDuration::from_millis(50), fast_call).await;

    assert!(
        result.is_ok(),
        "Gateway analytics queries should not starve each other when one request holds the entire pool"
    );

    let _ = blocker.await;
    Ok(())
}

#[sinex_serial_test]
async fn test_get_event_count_by_type_no_time_filter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let (_clock, dataset) = seed_analytics_dataset(&scope).await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    let counts = service.get_event_count_by_type(None, None).await?;
    for (event_type, expected) in dataset.expected_event_type_counts {
        assert_eq!(
            counts.get(&event_type),
            Some(&expected),
            "Event type {} should have {}",
            event_type,
            expected
        );
    }

    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn test_get_event_count_by_type_with_time_filter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let (clock, _dataset) = seed_analytics_dataset(&scope).await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    let now = clock.base();
    let two_hours_ago = now - ChronoDuration::hours(2);

    let counts = service
        .get_event_count_by_type(Some(two_hours_ago), Some(now))
        .await?;

    assert!(
        counts.get("file.created").unwrap_or(&0) >= &3,
        "Should have recent file events"
    );
    assert!(
        counts.get("command.executed").unwrap_or(&0) >= &5,
        "Should have recent command events"
    );
    assert_eq!(
        counts.get("boot.completed"),
        None,
        "Boot events should not be in recent timeframe"
    );

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_get_events_over_time_hourly_intervals(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let (clock, dataset) = seed_analytics_dataset(&scope).await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    let now = clock.base();
    let three_hours_ago = now - ChronoDuration::hours(3);

    let time_series = service
        .get_events_over_time(three_hours_ago, now, 60)
        .await?;
    assert!(
        !time_series.is_empty(),
        "Should have time series data within the hourly window"
    );
    for window in time_series.windows(2) {
        let (prev, curr) = (&window[0], &window[1]);
        assert!(prev.0 <= curr.0, "Time buckets should be ascending");
    }

    let total_events: i64 = time_series.iter().map(|(_, count)| count).sum();
    assert!(
        total_events >= (dataset.expected_total - 1).max(20),
        "Should have reasonable number of events in timeframe"
    );

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_get_events_over_time_different_intervals(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let (clock, _dataset) = seed_analytics_dataset(&scope).await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    let now = clock.base();
    let six_hours_ago = now - ChronoDuration::hours(6);

    let thirty_min_data = service.get_events_over_time(six_hours_ago, now, 30).await?;
    let two_hour_data = service
        .get_events_over_time(six_hours_ago, now, 120)
        .await?;

    assert!(
        thirty_min_data.len() >= two_hour_data.len(),
        "30-minute intervals should have more buckets than 2-hour intervals"
    );
    let thirty_min_total: i64 = thirty_min_data.iter().map(|(_, count)| count).sum();
    let two_hour_total: i64 = two_hour_data.iter().map(|(_, count)| count).sum();
    assert_eq!(
        thirty_min_total, two_hour_total,
        "Total event counts should match across different intervals"
    );

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_get_top_commands_no_time_filter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let (_clock, dataset) = seed_analytics_dataset(&scope).await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    assert_expected_command_counts(&ctx, &dataset.expected_command_counts).await?;
    assert_expected_source_counts(&ctx, &dataset.expected_source_counts).await?;

    let top_commands = service.get_top_commands(None, None, 10).await?;
    let expected_sorted = sorted_commands(&dataset.expected_command_counts);

    assert!(
        top_commands.len() >= expected_sorted.len(),
        "Should have at least {} different commands",
        expected_sorted.len()
    );
    for (idx, (command, count)) in expected_sorted.iter().enumerate() {
        assert_eq!(
            top_commands[idx],
            (command.clone(), *count),
            "Command ordering should be deterministic"
        );
    }

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_get_top_commands_with_limit(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let (_clock, dataset) = seed_analytics_dataset(&scope).await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    assert_expected_command_counts(&ctx, &dataset.expected_command_counts).await?;
    let top_3_commands = service.get_top_commands(None, None, 3).await?;
    let expected_sorted = sorted_commands(&dataset.expected_command_counts);

    assert_eq!(top_3_commands.len(), 3, "Should only return top 3 commands");
    for (idx, (command, count)) in expected_sorted.into_iter().take(3).enumerate() {
        assert_eq!(top_3_commands[idx], (command, count));
    }

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_get_top_commands_with_time_filter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let (clock, _dataset) = seed_analytics_dataset(&scope).await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    let now = clock.base();
    let thirty_minutes_ago = now - ChronoDuration::minutes(30);

    let recent_commands = service
        .get_top_commands(Some(thirty_minutes_ago), Some(now), 10)
        .await?;

    assert!(
        !recent_commands.is_empty(),
        "Should have some recent commands in the recent window"
    );
    for (command, count) in &recent_commands {
        assert!(count <= &8, "No command should exceed total count");
        assert!(
            count >= &1,
            "Each command should have at least 1 occurrence"
        );
        assert!(!command.is_empty(), "Commands should not be empty");
    }

    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn test_analytics_with_empty_database(ctx: TestContext) -> TestResult<()> {
    ctx.reset_database_slot().await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    let source_counts = service.get_event_count_by_source(None, None).await?;
    assert!(source_counts.is_empty(), "Should have empty source counts");

    let type_counts = service.get_event_count_by_type(None, None).await?;
    assert!(type_counts.is_empty(), "Should have empty type counts");

    let now = Utc::now();
    let one_hour_ago = now - ChronoDuration::hours(1);
    let time_series = service.get_events_over_time(one_hour_ago, now, 60).await?;
    assert!(time_series.is_empty(), "Should have empty time series");

    let top_commands = service.get_top_commands(None, None, 10).await?;
    assert!(top_commands.is_empty(), "Should have empty commands list");

    Ok(())
}

#[sinex_test]
async fn test_analytics_with_single_event(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();

    seed_events_via_scope(
        &scope,
        &clock,
        &[EventSpec::new(
            "test.source",
            "test.event",
            json!({"test": "data"}),
        )],
    )
    .await?;

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let source_counts = service.get_event_count_by_source(None, None).await?;
    let test_source_count = *source_counts.get("test.source").unwrap_or(&0);
    assert!(test_source_count >= 1);

    let type_counts = service.get_event_count_by_type(None, None).await?;
    let test_type_count = *type_counts.get("test.event").unwrap_or(&0);
    assert!(test_type_count >= 1);

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_analytics_time_range_edge_cases(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();

    seed_events_via_scope(
        &scope,
        &clock,
        &[
            EventSpec::new("boundary.test", "boundary.event", json!({"boundary": true}))
                .before(ChronoDuration::hours(1)),
        ],
    )
    .await?;

    let service: Arc<AnalyticsService> = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let now = clock.base();

    let exactly_one_hour_ago = now - ChronoDuration::hours(1);
    let source_counts = service
        .get_event_count_by_source(Some(exactly_one_hour_ago), Some(now))
        .await?;
    assert_eq!(source_counts.get("boundary.test"), Some(&1));

    let fifty_minutes_ago = now - ChronoDuration::minutes(50);
    let source_counts_excluded = service
        .get_event_count_by_source(Some(fifty_minutes_ago), Some(now))
        .await?;
    assert_eq!(source_counts_excluded.get("boundary.test"), None);

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_get_top_commands_only_command_events(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();

    seed_events_via_scope(
        &scope,
        &clock,
        &[
            EventSpec::new(
                "shell.kitty",
                "command.executed",
                json!({"command": "test command"}),
            ),
            EventSpec::new("shell.kitty", "session.started", json!({"shell": "bash"})),
            EventSpec::new(
                "fs",
                "file.created",
                json!({"path": "/test", "command": "not a real command"}),
            ),
        ],
    )
    .await?;

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let top_commands = service.get_top_commands(None, None, 10).await?;

    assert_eq!(top_commands.len(), 1);
    assert_eq!(top_commands[0], ("test command".to_string(), 1));

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_analytics_aggregation_accuracy(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();
    let service: Arc<AnalyticsService> = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let run_id = Ulid::new();

    let test_sources = [
        format!("source_a_{run_id}"),
        format!("source_b_{run_id}"),
        format!("source_c_{run_id}"),
    ];
    let test_types = ["type_x", "type_y"];

    let mut expected_source_counts: HashMap<String, i64> = HashMap::new();
    let mut expected_type_counts: HashMap<String, i64> = HashMap::new();
    let mut specs = Vec::new();

    for (i, source) in test_sources.iter().enumerate() {
        for (j, event_type) in test_types.iter().enumerate() {
            let count = (i + 1) * (j + 1);
            for k in 0..count {
                specs.push(EventSpec::new(
                    source.clone(),
                    event_type.to_string(),
                    json!({"test": "precision", "seq": k}),
                ));
            }
            *expected_source_counts
                .entry(source.to_string())
                .or_insert(0) += count as i64;
            *expected_type_counts
                .entry(event_type.to_string())
                .or_insert(0) += count as i64;
        }
    }

    seed_events_via_scope(&scope, &clock, &specs).await?;

    let source_counts = service.get_event_count_by_source(None, None).await?;
    let type_counts = service.get_event_count_by_type(None, None).await?;

    for (source, expected_count) in &expected_source_counts {
        assert_eq!(
            source_counts.get(source).copied().unwrap_or_default(),
            *expected_count,
            "Source {} should have exactly {} events",
            source,
            expected_count
        );
    }
    for (event_type, expected_count) in &expected_type_counts {
        assert_eq!(
            type_counts.get(event_type).copied().unwrap_or_default(),
            *expected_count,
            "Event type {} should have exactly {} events",
            event_type,
            expected_count
        );
    }

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_activity_heatmap(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    seed_analytics_dataset(&scope).await?;

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let heatmap = service.activity_heatmap(None, None, 60, 10).await?;
    assert!(!heatmap.is_empty(), "Should have activity data");

    for window in heatmap.windows(2) {
        let (prev, curr) = (&window[0], &window[1]);
        assert!(
            prev.1 >= curr.1,
            "Heatmap should be ordered by count descending"
        );
    }
    for (timestamp, count) in &heatmap {
        assert!(count > &0, "All activity counts should be positive");
        assert!(
            timestamp <= &Utc::now(),
            "All timestamps should be in the past"
        );
    }

    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn test_pipeline_services_smoke(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let (_clock, query_dataset) = seed_query_dataset(&scope).await?;

    let search_service = SearchService::new(ctx.pool.clone());
    let query = SearchQuery {
        text: None,
        sources: vec!["fs".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };
    let results = search_service.search_events(query).await?;
    assert!(!results.is_empty());

    let analytics_service = AnalyticsService::new(ctx.pool.clone());
    let counts = analytics_service
        .get_event_count_by_source(None, None)
        .await?;
    let total: i64 = counts.values().sum();
    assert!(
        total >= query_dataset.expected_total as i64,
        "Expected analytics totals to reflect seeded query dataset"
    );

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_analytics_large_dataset_performance(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();

    let target_total = 60usize;
    let dataset = seed_analytics_dataset_perf_via_scope(&scope, &clock, target_total).await?;

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    let _ = service.get_event_count_by_source(None, None).await?;
    let _ = service.get_event_count_by_type(None, None).await?;
    let _ = service.get_top_commands(None, None, 10).await?;

    let analytics_start = Instant::now();
    let source_counts = service.get_event_count_by_source(None, None).await?;
    let type_counts = service.get_event_count_by_type(None, None).await?;
    let _top_commands = service.get_top_commands(None, None, 10).await?;
    let analytics_duration = analytics_start.elapsed();

    let perf_source_count = source_counts
        .iter()
        .filter(|(s, _)| s.starts_with("perf_source_"))
        .count();
    let perf_type_count = type_counts
        .iter()
        .filter(|(t, _)| t.starts_with("perf_type_"))
        .count();
    assert!(perf_source_count <= 5);
    assert!(perf_type_count <= 3);

    assert!(
        analytics_duration < std::time::Duration::from_secs(8),
        "Analytics queries on {} events should complete quickly, took {:?}",
        dataset.expected_total,
        analytics_duration
    );

    scope.shutdown().await?;
    Ok(())
}
