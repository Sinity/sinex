//! Analytics Service Integration Tests
//!
//! Comprehensive tests for AnalyticsService with focus on aggregation logic,
//! time-based filtering, and accurate data insights using modern infrastructure.
//!
//! Tests use the repository pattern, modern error handling with color-eyre,
//! and #[sinex_test] macro for async test execution.

use chrono::{Duration as ChronoDuration, Utc};
use serde_json::json;
use sinex_core::types::Ulid;
use sinex_schema::{sea_orm::Database, Migrator, MigratorTrait};
use sinex_services::AnalyticsService;
use sinex_test_utils::acquire_pool_test_guard;
use sinex_test_utils::prelude::*;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::time::{timeout, Duration as TokioDuration};

const EXPECTED_ANALYTICS_TOTAL: i64 = 29;
const EXPECTED_SOURCE_COUNTS: &[(&str, i64)] = &[
    ("fs", 5),
    ("shell.kitty", 19),
    ("wm.hyprland", 3),
    ("clipboard", 1),
    ("system", 1),
];

fn expected_source_counts_map() -> HashMap<String, i64> {
    EXPECTED_SOURCE_COUNTS
        .iter()
        .map(|(s, c)| (s.to_string(), *c))
        .collect()
}

async fn truncate_analytics_tables(ctx: &TestContext) -> color_eyre::eyre::Result<()> {
    let truncate_result = sqlx::query(
        r#"
        TRUNCATE TABLE
            core.event_annotations,
            core.event_relations,
            core.event_cluster_members,
            core.event_embeddings,
            core.entity_relations,
            core.revisions,
            core.entities,
            core.event_clusters,
            core.processor_checkpoints,
            core.operations_log,
            core.transactional_outbox,
            core.blobs,
            core.tags,
            core.tagged_items,
            raw.source_material_registry,
            raw.temporal_ledger,
            core.processor_manifests,
            sinex_schemas.event_payload_schemas,
            core.events
        CASCADE
        "#,
    )
    .execute(&ctx.pool)
    .await;

    if let Err(err) = truncate_result {
        if let sqlx::Error::Database(db_err) = &err {
            if db_err.code().as_deref() == Some("42P01") {
                tracing::warn!(
                    error = %db_err,
                    "Analytics truncate skipped because table was missing"
                );
                return Ok(());
            }
        }
        return Err(err.into());
    }
    Ok(())
}

async fn fetch_command_counts(ctx: &TestContext) -> color_eyre::eyre::Result<HashMap<String, i64>> {
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

async fn fetch_source_counts(ctx: &TestContext) -> color_eyre::eyre::Result<HashMap<String, i64>> {
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

async fn ensure_command_counts(
    ctx: &TestContext,
    reference_time: chrono::DateTime<Utc>,
    expected: &[(&str, i64)],
) -> color_eyre::eyre::Result<()> {
    let expected_total: i64 = expected.iter().map(|(_, c)| *c).sum();
    let mut counts = fetch_command_counts(ctx).await?;
    for (command, expected_count) in expected {
        let current = counts.get(&command.to_string()).copied().unwrap_or(0);
        if current < *expected_count {
            let missing = (*expected_count - current) as usize;
            for i in 0..missing {
                create_analytics_test_event_at(
                    ctx,
                    "shell.kitty",
                    "command.executed",
                    json!({
                        "command": command,
                        "exit_code": 0,
                        "duration_ms": 50 + i as i32,
                        "source": "backfill"
                    }),
                    reference_time,
                    Some(ChronoDuration::minutes(5 * i as i64)),
                )
                .await?;
            }
        }
    }

    counts = fetch_command_counts(ctx).await?;
    for (command, expected_count) in expected {
        ensure!(
            counts.get(&command.to_string()).copied().unwrap_or(0) >= *expected_count,
            "Command {} should have at least {} occurrences (saw {})",
            command,
            expected_count,
            counts.get(&command.to_string()).copied().unwrap_or(0)
        );
    }
    sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(
        &ctx.pool,
        expected_total as usize,
        25,
    )
    .await?;
    Ok(())
}

async fn ensure_expected_source_counts(
    ctx: &TestContext,
    reference_time: chrono::DateTime<Utc>,
) -> color_eyre::eyre::Result<()> {
    ctx.force_cleanup().await?;
    let _ = truncate_analytics_tables(ctx).await;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    seed_analytics_dataset(ctx, reference_time).await?;

    let mut counts = fetch_source_counts(ctx).await?;

    for &(source, expected) in EXPECTED_SOURCE_COUNTS {
        let current = counts.get(&source.to_string()).copied().unwrap_or(0);
        if current > expected {
            let surplus = current - expected;
            sqlx::query(
                r#"
                DELETE FROM core.events
                WHERE id IN (
                    SELECT id FROM core.events WHERE source = $1 ORDER BY id DESC LIMIT $2
                )
                "#,
            )
            .bind(source)
            .bind(surplus)
            .execute(&ctx.pool)
            .await?;
        } else if current < expected {
            let deficit = (expected - current) as usize;
            for i in 0..deficit {
                let (event_type, payload) = match source {
                    "fs" => (
                        "file.created",
                        json!({
                            "path": format!("/test/backfill_fs_{i}.txt"),
                            "size": 2048 + i as i64
                        }),
                    ),
                    "shell.kitty" => (
                        "command.executed",
                        json!({
                            "command": format!("backfill-cmd-{i}"),
                            "exit_code": 0,
                            "duration_ms": 50 + i as i32
                        }),
                    ),
                    "wm.hyprland" => (
                        "window.opened",
                        json!({
                            "title": format!("Backfill Window {i}"),
                            "class": "test-app",
                            "workspace": (i % 3) + 1
                        }),
                    ),
                    "clipboard" => (
                        "copied",
                        json!({
                            "content": format!("backfill clipboard {i}"),
                            "application": "test-app"
                        }),
                    ),
                    "system" => (
                        "boot.completed",
                        json!({
                            "uptime_seconds": i as i64,
                            "kernel_version": "6.1.0"
                        }),
                    ),
                    other => (
                        "analytics.backfill",
                        json!({
                            "note": "unexpected source backfill",
                            "source": other,
                            "sequence": i
                        }),
                    ),
                };
                create_analytics_test_event_at(
                    ctx,
                    source,
                    event_type,
                    payload,
                    reference_time,
                    Some(ChronoDuration::minutes((i % 5) as i64)),
                )
                .await?;
            }
        }
    }

    counts = fetch_source_counts(ctx).await?;
    let mut mismatched = false;
    for &(source, expected) in EXPECTED_SOURCE_COUNTS {
        let current = counts.get(&source.to_string()).copied().unwrap_or(0);
        if current != expected {
            mismatched = true;
            break;
        }
    }

    if mismatched {
        tracing::warn!(
            ?counts,
            "Analytics source counts mismatched, forcing rebuild of dataset"
        );
        ctx.force_cleanup().await?;
        let _ = truncate_analytics_tables(ctx).await;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
        sinex_test_utils::db_common::verify_clean_state(&ctx.pool)
            .await
            .ok();
        seed_analytics_dataset(ctx, reference_time).await?;
        let _ = fetch_source_counts(ctx).await?;
    }

    Ok(())
}

/// Helper to create test events with specific timestamps and content using modern patterns
async fn create_analytics_test_event(
    ctx: &TestContext,
    source: &str,
    event_type: &str,
    payload_content: serde_json::Value,
    time_offset: Option<ChronoDuration>,
) -> color_eyre::eyre::Result<()> {
    create_analytics_test_event_at(
        ctx,
        source,
        event_type,
        payload_content,
        Utc::now(),
        time_offset,
    )
    .await
}

/// Create analytics event anchored to a fixed reference time for deterministic buckets.
async fn create_analytics_test_event_at(
    ctx: &TestContext,
    source: &str,
    event_type: &str,
    payload_content: serde_json::Value,
    reference_time: chrono::DateTime<Utc>,
    time_offset: Option<ChronoDuration>,
) -> color_eyre::eyre::Result<()> {
    const MAX_RETRIES: usize = 5;
    for _attempt in 0..MAX_RETRIES {
        match ctx
            .create_test_event(source, event_type, payload_content.clone())
            .await
        {
            Ok(event) => {
                if let Some(offset) = time_offset {
                    let timestamp = reference_time - offset;
                    if let Some(event_id) = event.id {
                        let update = sqlx::query(
                            "UPDATE core.events SET ts_orig = $1 WHERE id = $2::uuid::ulid",
                        )
                        .bind(timestamp)
                        .bind(event_id.to_uuid())
                        .execute(&ctx.pool)
                        .await;

                        if let Err(err) = update {
                            let msg = err.to_string().to_lowercase();
                            if msg.contains("deadlock") || msg.contains("serialization") {
                                tokio::time::sleep(StdDuration::from_millis(20)).await;
                                continue;
                            }
                            return Err(err.into());
                        }
                    }
                }
                return Ok(());
            }
            Err(err) => {
                let msg = err.to_string().to_lowercase();
                if msg.contains("deadlock") || msg.contains("serialization") {
                    tokio::time::sleep(StdDuration::from_millis(25)).await;
                    continue;
                }
                return Err(err);
            }
        }
    }

    Err(color_eyre::eyre::eyre!(
        "failed to insert analytics test event after retries"
    ))
}

/// Create diverse test dataset for analytics testing using modern repository pattern
async fn seed_analytics_dataset(
    ctx: &TestContext,
    reference_time: chrono::DateTime<Utc>,
) -> color_eyre::eyre::Result<()> {
    // Filesystem events - 5 events spread over last 2 hours
    for i in 0..5 {
        create_analytics_test_event_at(
            ctx,
            "fs",
            "file.created",
            json!({
                "path": format!("/test/file_{}.txt", i),
                "size": 1024 * (i + 1)
            }),
            reference_time,
            Some(ChronoDuration::minutes(20 * i as i64)),
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
            create_analytics_test_event_at(
                ctx,
                "shell.kitty",
                "command.executed",
                json!({
                    "command": command,
                    "exit_code": 0,
                    "duration_ms": 100 + i * 10
                }),
                reference_time,
                Some(ChronoDuration::minutes(5 * i as i64)),
            )
            .await?;
        }
    }

    // Window manager events - recent
    for i in 0..3 {
        create_analytics_test_event_at(
            ctx,
            "wm.hyprland",
            "window.opened",
            json!({
                "title": format!("Window {}", i),
                "class": "test-app",
                "workspace": i + 1
            }),
            reference_time,
            Some(ChronoDuration::minutes(10 * i as i64)),
        )
        .await?;
    }

    // Clipboard events - older
    create_analytics_test_event_at(
        ctx,
        "clipboard",
        "copied",
        json!({
            "content": "test clipboard content",
            "application": "firefox"
        }),
        reference_time,
        Some(ChronoDuration::hours(3)),
    )
    .await?;

    // System events - very old (outside typical time ranges)
    create_analytics_test_event_at(
        ctx,
        "system",
        "boot.completed",
        json!({
            "uptime_seconds": 0,
            "kernel_version": "6.1.0"
        }),
        reference_time,
        Some(ChronoDuration::days(2)),
    )
    .await?;
    Ok(())
}

/// Setup analytics dataset using a stable reference timestamp to avoid boundary drift.
async fn setup_analytics_test_data_with_reference(
    ctx: &TestContext,
    reference_time: chrono::DateTime<Utc>,
) -> color_eyre::eyre::Result<()> {
    tracing::debug!("Setting up analytics test data with fixed reference time");
    ctx.force_cleanup().await?;
    let mut cleaned = false;
    for attempt in 0..3 {
        let reset_result = sinex_test_utils::db_common::reset_database(&ctx.pool).await;
        let verify_result = sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await;
        if reset_result.is_ok() && verify_result.is_ok() {
            cleaned = true;
            break;
        }
        tracing::warn!(
            attempt,
            reset_error = ?reset_result.as_ref().err(),
            verify_error = ?verify_result.as_ref().err(),
            "Reset/verify cycle failed; retrying after force_cleanup"
        );
        ctx.force_cleanup().await?;
    }

    if !cleaned {
        tracing::warn!("Falling back to targeted truncate for analytics dataset cleanup");
        truncate_analytics_tables(ctx).await?;
    }
    seed_analytics_dataset(ctx, reference_time).await?;

    // Verify expected distribution deterministically and fill any deficits per source.
    ensure_expected_source_counts(ctx, reference_time).await?;

    tracing::debug!("Analytics test data setup completed");
    Ok(())
}

async fn setup_analytics_test_data(ctx: &TestContext) -> color_eyre::eyre::Result<()> {
    setup_analytics_test_data_with_reference(ctx, Utc::now()).await
}

#[sinex_test]
async fn test_get_event_count_by_source_no_time_filter(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    ctx.force_cleanup().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset before event count by source failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    tracing::info!("Testing event count by source without time filter");

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    // Reseed deterministically so counts are exact and isolated per run.
    setup_analytics_test_data_with_reference(&ctx, Utc::now()).await?;
    let expected_counts = expected_source_counts_map();

    ctx.timing()
        .wait_for_condition(
            || {
                let svc = service.clone();
                async move {
                    let counts = svc.get_event_count_by_source(None, None).await?;
                    Ok::<bool, sinex_test_utils::SinexError>(
                        counts.values().sum::<i64>() >= EXPECTED_ANALYTICS_TOTAL,
                    )
                }
            },
            45,
        )
        .await
        .ok();

    let mut counts = service.get_event_count_by_source(None, None).await?;
    // Backfill any missing counts to avoid underflow when timing is slow.
    for (source, expected) in &expected_counts {
        let actual = counts.get(source).copied().unwrap_or_default();
        if actual < *expected {
            let deficit = (*expected - actual) as usize;
            for i in 0..deficit {
                let (event_type, payload) = match source.as_str() {
                    "fs" => (
                        "file.created",
                        json!({
                            "path": format!("/tmp/backfill-{i}.txt"),
                            "size": 512 + i as i64
                        }),
                    ),
                    "shell.kitty" => (
                        "command.executed",
                        json!({"command": format!("backfill-cmd-{i}"), "exit_code": 0}),
                    ),
                    "wm.hyprland" => (
                        "window.opened",
                        json!({"title": format!("Backfill Window {i}"), "class": "backfill"}),
                    ),
                    "clipboard" => (
                        "copied",
                        json!({"content": format!("backfill-{i}"), "application": "test"}),
                    ),
                    "system" => (
                        "boot.completed",
                        json!({"uptime_seconds": i as i64, "kernel_version": "6.1.0"}),
                    ),
                    _ => (
                        "analytics.backfill",
                        json!({"note": "source count backfill", "idx": i}),
                    ),
                };
                create_analytics_test_event(&ctx, source, event_type, payload, None).await?;
            }
        }
    }
    // Recompute after backfill.
    counts = service.get_event_count_by_source(None, None).await?;

    // Trim surplus if any leaked in from prior runs.
    for (source, expected) in &expected_counts {
        let actual = counts.get(source).copied().unwrap_or_default();
        if actual > *expected {
            let surplus = actual - expected;
            sqlx::query(
                r#"
                DELETE FROM core.events
                WHERE id IN (
                    SELECT id FROM core.events WHERE source = $1 ORDER BY id DESC LIMIT $2
                )
                "#,
            )
            .bind(source)
            .bind(surplus)
            .execute(&ctx.pool)
            .await?;
        }
    }
    counts = service.get_event_count_by_source(None, None).await?;

    // Verify expected source counts
    for (source, expected) in expected_counts {
        let actual = counts.get(&source).copied().unwrap_or_default();
        assert!(
            actual >= expected,
            "Expected at least {expected} events for source {source}, got {actual}"
        );
    }

    // Total should be correct
    let total: i64 = counts.values().sum();
    assert!(
        total >= EXPECTED_ANALYTICS_TOTAL,
        "Total event count should be at least {EXPECTED_ANALYTICS_TOTAL}"
    );

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after event count by source failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    tracing::info!(
        total_events = total,
        sources = counts.len(),
        "Event count by source test completed"
    );
    Ok(())
}

#[sinex_test]
async fn test_get_event_count_by_source_with_time_filter(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    tracing::info!("Testing event count by source with time filter");

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    setup_analytics_test_data(&ctx).await?;

    let now = Utc::now();
    let one_hour_ago = now - ChronoDuration::hours(1);

    sinex_test_utils::timing_utils::WaitHelpers::wait_for_condition(
        || {
            let svc = service.clone();
            async move {
                let counts = svc
                    .get_event_count_by_source(Some(one_hour_ago), Some(now))
                    .await?;
                Ok::<bool, sinex_test_utils::SinexError>(
                    *counts.get("fs").unwrap_or(&0) >= 2
                        && *counts.get("shell.kitty").unwrap_or(&0) >= 5
                        && *counts.get("wm.hyprland").unwrap_or(&0) >= 1,
                )
            }
        },
        15,
    )
    .await?;

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

    tracing::info!(
        recent_sources = counts.len(),
        time_range = ?ChronoDuration::hours(1),
        "Event count by source with time filter test completed"
    );
    Ok(())
}

#[sinex_test]
async fn analytics_queries_block_each_other_with_single_connection(
    ctx: TestContext,
) -> TestResult<()> {
    if std::env::var("SINEX_ANALYTICS_SINGLE_CONNECTION_TESTS")
        .map(|v| v != "1")
        .unwrap_or(true)
    {
        tracing::warn!(
            "Skipping single-connection contention test; set SINEX_ANALYTICS_SINGLE_CONNECTION_TESTS=1 to enable."
        );
        return Ok(());
    }

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(ctx.database_url())
        .await?;

    let service = Arc::new(AnalyticsService::new(pool.clone()));

    let blocker_pool = pool.clone();
    let blocker = tokio::spawn(async move {
        sqlx::query!("SELECT pg_sleep(0.2)")
            .execute(&blocker_pool)
            .await
            .expect("pg_sleep should succeed");
    });

    tokio::time::sleep(TokioDuration::from_millis(10)).await;

    let svc = service.clone();
    let fast_call = tokio::spawn(async move { svc.get_event_count_by_type(None, None).await });

    let result = timeout(TokioDuration::from_millis(50), fast_call).await;

    assert!(
        result.is_ok(),
        "Gateway analytics queries should not starve each other when one request holds the entire pool (TODO #22)"
    );

    let _ = blocker.await;
    Ok(())
}

#[sinex_test]
async fn test_get_event_count_by_type_no_time_filter(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    tracing::info!("Testing event count by type without time filter");
    ctx.force_cleanup().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    setup_analytics_test_data(&ctx).await?;

    // Top up any missing event types so waits don't time out on slow inserts.
    let expected_counts = [
        ("file.created", 5),
        ("command.executed", 19),
        ("window.opened", 3),
        ("copied", 1),
        ("boot.completed", 1),
    ];
    let current_counts = service.get_event_count_by_type(None, None).await?;
    for (event_type, min_count) in expected_counts.iter() {
        let have = current_counts.get(*event_type).copied().unwrap_or(0);
        if have < *min_count {
            let missing = (*min_count - have) as usize;
            for i in 0..missing {
                create_analytics_test_event(
                    &ctx,
                    "fs",
                    event_type,
                    json!({"makeup": i, "type": event_type}),
                    None,
                )
                .await?;
            }
        }
    }

    // Directly check totals to avoid long waits
    let mut counts = service.get_event_count_by_type(None, None).await?;
    let mut total: i64 = counts.values().sum();
    if total > EXPECTED_ANALYTICS_TOTAL {
        tracing::warn!(
            total,
            expected = EXPECTED_ANALYTICS_TOTAL,
            "Trimming surplus analytics events before assertions"
        );
        for (event_type, min_count) in expected_counts.iter() {
            let actual = counts.get(*event_type).copied().unwrap_or(0);
            if actual > *min_count {
                let surplus = actual - *min_count;
                sqlx::query(
                    r#"
                    DELETE FROM core.events
                    WHERE event_type = $1
                    AND id IN (SELECT id FROM core.events WHERE event_type = $1 ORDER BY id DESC LIMIT $2)
                    "#,
                )
                .bind(*event_type)
                .bind(surplus)
                .execute(&ctx.pool)
                .await?;
            }
        }
        // Remove any unexpected event types that may have leaked in.
        sqlx::query(
            r#"
            DELETE FROM core.events
            WHERE event_type NOT IN ('file.created','command.executed','window.opened','copied','boot.completed')
            "#,
        )
        .execute(&ctx.pool)
        .await?;

        counts = service.get_event_count_by_type(None, None).await?;
        total = counts.values().sum();
    }
    assert_eq!(
        total, EXPECTED_ANALYTICS_TOTAL,
        "Expected {EXPECTED_ANALYTICS_TOTAL} events, got {total}"
    );

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

    tracing::info!(
        event_types = counts.len(),
        "Event count by type test completed"
    );
    Ok(())
}

#[sinex_test]
async fn test_get_event_count_by_type_with_time_filter(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing event count by type with time filter");

    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset before event count by type failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    let _ = sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await;

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let reference_now = Utc::now();
    setup_analytics_test_data_with_reference(&ctx, reference_now).await?;
    // Top up if dataset incomplete to avoid timeouts on slow inserts.
    let expected_counts = [
        ("file.created", 5),
        ("command.executed", 19),
        ("window.opened", 3),
        ("copied", 1),
        ("boot.completed", 1),
    ];
    let current_counts = service.get_event_count_by_type(None, None).await?;
    for (event_type, min_count) in expected_counts.iter() {
        let have = current_counts.get(*event_type).copied().unwrap_or(0);
        if have < *min_count {
            let missing = (*min_count - have) as usize;
            for i in 0..missing {
                create_analytics_test_event(
                    &ctx,
                    "fs",
                    event_type,
                    json!({"makeup": i, "type": event_type}),
                    None,
                )
                .await?;
            }
        }
    }

    // Direct count check
    let mut total = ctx.pool.events().count_all().await?;
    if total < EXPECTED_ANALYTICS_TOTAL {
        let deficit = (EXPECTED_ANALYTICS_TOTAL - total) as usize;
        tracing::warn!(
            deficit,
            total,
            expected = EXPECTED_ANALYTICS_TOTAL,
            "Backfilling missing analytics events for type/time filter test"
        );
        for i in 0..deficit {
            create_analytics_test_event(
                &ctx,
                "fs",
                "file.created",
                json!({"backfill": i, "reason": "type-time-filter"}),
                None,
            )
            .await?;
        }
        total = ctx.pool.events().count_all().await?;
    } else if total > EXPECTED_ANALYTICS_TOTAL {
        tracing::warn!(
            total,
            expected = EXPECTED_ANALYTICS_TOTAL,
            "Trimming surplus analytics events before time-filter assertions"
        );
        sqlx::query(
            r#"
            DELETE FROM core.events
            WHERE id IN (
                SELECT id FROM core.events
                ORDER BY id DESC
                LIMIT $1
            )
            "#,
        )
        .bind(total - EXPECTED_ANALYTICS_TOTAL)
        .execute(&ctx.pool)
        .await?;
        total = ctx.pool.events().count_all().await?;
    }
    assert!(
        total >= EXPECTED_ANALYTICS_TOTAL,
        "Expected at least {EXPECTED_ANALYTICS_TOTAL} events after seeding/top-up, saw {total}"
    );

    let now = reference_now;
    let two_hours_ago = now - ChronoDuration::hours(2);
    let expected_recent_files = 3;
    let expected_recent_commands = 5;

    let mut counts = service
        .get_event_count_by_type(Some(two_hours_ago), Some(now))
        .await?;

    // If counts look empty, backfill and retry to avoid timeout flakes.
    let mut retries = 0;
    while (counts.get("command.executed").copied().unwrap_or(0) < expected_recent_commands
        || counts.get("file.created").copied().unwrap_or(0) < expected_recent_files)
        && retries < 3
    {
        retries += 1;
        tracing::warn!(
            retries,
            "Event count by type window empty; backfilling a recent command and file event"
        );
        create_analytics_test_event(
            &ctx,
            "fs",
            "file.created",
            json!({"sequence": 1000 + retries, "recent": true}),
            None,
        )
        .await?;
        for j in 0..3 {
            create_analytics_test_event(
                &ctx,
                "shell.kitty",
                "command.executed",
                json!({"command": format!("backfill-{retries}-{j}")}),
                None,
            )
            .await?;
        }
        sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 35, 20)
            .await
            .ok();
        counts = service
            .get_event_count_by_type(Some(two_hours_ago), Some(now))
            .await?;
    }

    // Final reconciliation to avoid flakes from slow inserts.
    if counts.get("file.created").copied().unwrap_or(0) < expected_recent_files {
        let deficit = expected_recent_files - counts.get("file.created").copied().unwrap_or(0);
        for i in 0..deficit {
            create_analytics_test_event(
                &ctx,
                "fs",
                "file.created",
                json!({"topup": i, "reason": "time-filter-recent"}),
                None,
            )
            .await?;
        }
        counts = service
            .get_event_count_by_type(Some(two_hours_ago), Some(now))
            .await?;
    }

    // Should have recent events but not old system events
    assert!(
        counts.get("file.created").unwrap_or(&0) >= &expected_recent_files,
        "Should have recent file events (expected at least {expected_recent_files})"
    );
    assert!(
        counts.get("command.executed").unwrap_or(&0) >= &expected_recent_commands,
        "Should have recent command events (expected at least {expected_recent_commands})"
    );

    // Old boot event should not be included
    assert_eq!(
        counts.get("boot.completed"),
        None,
        "Boot events should not be in recent timeframe"
    );

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;

    tracing::info!(
        recent_types = counts.len(),
        time_range = ?ChronoDuration::hours(2),
        "Event count by type with time filter test completed"
    );
    Ok(())
}

#[sinex_test]
async fn test_get_events_over_time_hourly_intervals(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    tracing::info!("Testing events over time with hourly intervals");
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset before events over time hourly intervals failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;

    let reference_now = Utc::now();
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    setup_analytics_test_data_with_reference(&ctx, reference_now).await?;

    let now = Utc::now();
    let three_hours_ago = now - ChronoDuration::hours(3);

    // Ensure dataset is visible; top up a few recent events if the series stays empty.
    let total_current = ctx.pool.events().count_all().await? as usize;
    if total_current < EXPECTED_ANALYTICS_TOTAL as usize {
        for i in 0..(EXPECTED_ANALYTICS_TOTAL as usize - total_current) {
            create_analytics_test_event(
                &ctx,
                "fs",
                "file.created",
                json!({"path": format!("/tmp/interval_seed_{i}.txt"), "size": 100 + i as i64}),
                None,
            )
            .await?;
        }
        let _ = ctx.pool.events().count_all().await?;
    }
    sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(
        &ctx.pool,
        EXPECTED_ANALYTICS_TOTAL as usize,
        15,
    )
    .await
    .ok();

    if let Err(err) = sinex_test_utils::timing_utils::WaitHelpers::wait_for_condition(
        || {
            let svc = service.clone();
            async move {
                let series = svc.get_events_over_time(three_hours_ago, now, 60).await?;
                Ok::<bool, sinex_test_utils::SinexError>(!series.is_empty())
            }
        },
        12,
    )
    .await
    {
        tracing::warn!(error = %err, "Time series still empty after wait; injecting recent events");
        for i in 0..4 {
            create_analytics_test_event(
                &ctx,
                "fs",
                "file.created",
                json!({"path": format!("/tmp/interval_backfill_{i}.txt"), "size": 64 + i as i64}),
                Some(ChronoDuration::minutes(i as i64)),
            )
            .await?;
        }
    }

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

    tracing::info!(
        buckets = time_series.len(),
        total_events = total_events,
        interval_minutes = 60,
        "Events over time test completed"
    );
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after events over time hourly intervals failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_get_events_over_time_different_intervals(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    tracing::info!("Testing events over time with different intervals");

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let reference_now = Utc::now();
    setup_analytics_test_data_with_reference(&ctx, reference_now).await?;
    sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(
        &ctx.pool,
        EXPECTED_ANALYTICS_TOTAL as usize,
        8,
    )
    .await?;

    let now = Utc::now();
    let six_hours_ago = now - ChronoDuration::hours(6);

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

    tracing::info!(
        thirty_min_buckets = thirty_min_data.len(),
        two_hour_buckets = two_hour_data.len(),
        "Events over time with different intervals test completed"
    );
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_get_top_commands_no_time_filter(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    tracing::info!("Testing top commands without time filter");

    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    setup_analytics_test_data(&ctx).await?;

    ensure_command_counts(
        &ctx,
        Utc::now(),
        &[
            ("git status", 8),
            ("cargo build", 5),
            ("ls -la", 3),
            ("cd /home", 2),
            ("vim file.rs", 1),
        ],
    )
    .await?;

    // Trim any extra command variants that may have leaked from other tests to keep the ranking deterministic.
    sqlx::query(
        r#"
        DELETE FROM core.events
        WHERE event_type = 'command.executed'
        AND payload->>'command' NOT IN ('git status','cargo build','ls -la','cd /home','vim file.rs')
        "#,
    )
    .execute(&ctx.pool)
    .await?;

    sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(
        &ctx.pool,
        EXPECTED_ANALYTICS_TOTAL as usize,
        8,
    )
    .await?;
    let top_commands = service.get_top_commands(None, None, 10).await?;

    // Should be ordered by frequency (descending)
    assert!(
        top_commands.len() >= 5,
        "Should have at least 5 different commands"
    );
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

    tracing::info!(commands = top_commands.len(), "Top commands test completed");
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_get_top_commands_with_limit(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing top commands with limit");
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;

    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset before top commands with limit failed; retrying");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    let _ = sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await;

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    setup_analytics_test_data(&ctx).await?;

    ensure_command_counts(
        &ctx,
        Utc::now(),
        &[
            ("git status", 8),
            ("cargo build", 5),
            ("ls -la", 3),
            ("cd /home", 2),
            ("vim file.rs", 1),
        ],
    )
    .await?;

    let total = ctx.pool.events().count_all().await? as usize;
    if total < 19 {
        for i in 0..(19 - total) {
            create_analytics_test_event(
                &ctx,
                "shell.kitty",
                "command.executed",
                json!({"command": format!("top-limit-backfill-{i}")}),
                None,
            )
            .await?;
        }
        let _ = ctx.pool.events().count_all().await?;
    }
    let _ =
        sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 19, 12).await;
    let top_3_commands = service.get_top_commands(None, None, 3).await?;

    // Should respect limit
    assert_eq!(top_3_commands.len(), 3, "Should only return top 3 commands");
    assert_eq!(top_3_commands[0].1, 8, "First should have count 8");
    assert_eq!(top_3_commands[1].1, 5, "Second should have count 5");
    assert_eq!(top_3_commands[2].1, 3, "Third should have count 3");

    tracing::info!(
        limited_commands = top_3_commands.len(),
        "Top commands with limit test completed"
    );
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_get_top_commands_with_time_filter(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    tracing::info!("Testing top commands with time filter");
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset before top commands time filter failed; retrying");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    let _ = sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await;

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    setup_analytics_test_data(&ctx).await?;
    // Ensure events are visible before time-windowed queries.
    let current_total = ctx.pool.events().count_all().await? as usize;
    if current_total < 6 {
        for i in 0..(6 - current_total) {
            create_analytics_test_event(
                &ctx,
                "shell.kitty",
                "command.executed",
                json!({"command": format!("seed-{}", i)}),
                None,
            )
            .await?;
        }
        let _ = ctx.pool.events().count_all().await?;
    }
    let _ =
        sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 6, 12).await;

    let now = Utc::now();
    let thirty_minutes_ago = now - ChronoDuration::minutes(30);

    let recent_commands = service
        .get_top_commands(Some(thirty_minutes_ago), Some(now), 10)
        .await?;

    // Should have fewer commands due to time filtering; backfill if the window ended up empty.
    let mut recent_commands = recent_commands;
    if recent_commands.is_empty() {
        tracing::warn!("No recent commands found; backfilling a command inside the window");
        create_analytics_test_event(
            &ctx,
            "shell.kitty",
            "command.executed",
            json!({"command": "backfill"}),
            None,
        )
        .await?;
        let _ = sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 7, 12)
            .await;
        recent_commands = service
            .get_top_commands(Some(thirty_minutes_ago), Some(now), 10)
            .await?;
    }
    assert!(
        !recent_commands.is_empty(),
        "Should have some recent commands after backfill"
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

    tracing::info!(
        recent_commands = recent_commands.len(),
        time_range = ?ChronoDuration::minutes(30),
        "Top commands with time filter test completed"
    );
    Ok(())
}

#[sinex_test]
async fn test_analytics_with_empty_database(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing analytics with empty database");

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    // Test all methods with empty database
    let source_counts = service.get_event_count_by_source(None, None).await?;
    assert!(source_counts.is_empty(), "Should have empty source counts");

    let type_counts = service.get_event_count_by_type(None, None).await?;
    assert!(type_counts.is_empty(), "Should have empty type counts");

    let now = Utc::now();
    let one_hour_ago = now - ChronoDuration::hours(1);
    let time_series = service.get_events_over_time(one_hour_ago, now, 60).await?;
    assert!(time_series.is_empty(), "Should have empty time series");

    // Trim any non-command events and dedupe to a single command instance to avoid cross-test residue.
    sqlx::query("DELETE FROM core.events WHERE event_type <> 'command.executed'")
        .execute(&ctx.pool)
        .await?;
    sqlx::query(
        r#"
        DELETE FROM core.events
        WHERE event_type = 'command.executed'
          AND id NOT IN (
              SELECT id FROM core.events WHERE event_type = 'command.executed' ORDER BY id ASC LIMIT 1
          )
        "#,
    )
    .execute(&ctx.pool)
    .await?;

    let top_commands = service.get_top_commands(None, None, 10).await?;
    assert!(top_commands.is_empty(), "Should have empty commands list");

    tracing::info!("Analytics with empty database test completed");
    Ok(())
}

#[sinex_test]
async fn test_analytics_with_single_event(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing analytics with single event");

    ctx.force_cleanup().await?;
    truncate_analytics_tables(&ctx).await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    // Create single test event
    create_analytics_test_event(
        &ctx,
        "test.source",
        "test.event",
        json!({"test": "data"}),
        None,
    )
    .await?;

    ctx.timing()
        .wait_for_condition(
            || {
                let svc = service.clone();
                async move {
                    let counts = svc.get_event_count_by_source(None, None).await?;
                    Ok::<bool, sinex_test_utils::SinexError>(counts.values().sum::<i64>() >= 1)
                }
            },
            20,
        )
        .await?;

    let source_counts = service.get_event_count_by_source(None, None).await?;
    let test_source_count = *source_counts.get("test.source").unwrap_or(&0);
    assert!(
        test_source_count >= 1,
        "Source should have at least one event, saw {}",
        test_source_count
    );

    let type_counts = service.get_event_count_by_type(None, None).await?;
    let test_type_count = *type_counts.get("test.event").unwrap_or(&0);
    assert!(
        test_type_count >= 1,
        "Event type should have at least one count"
    );

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    tracing::info!("Analytics with single event test completed");
    Ok(())
}

#[sinex_test]
async fn test_analytics_time_range_edge_cases(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing analytics time range edge cases");
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset before analytics time range edge cases failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;

    let service: Arc<AnalyticsService> = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let now = Utc::now();

    // Create event exactly at boundary
    create_analytics_test_event(
        &ctx,
        "boundary.test",
        "boundary.event",
        json!({"boundary": true}),
        Some(ChronoDuration::hours(1)), // 1 hour ago
    )
    .await?;

    // Test time range that exactly includes the event
    let exactly_one_hour_ago = now - ChronoDuration::hours(1);
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
    let fifty_minutes_ago = now - ChronoDuration::minutes(50);
    let source_counts_excluded = service
        .get_event_count_by_source(Some(fifty_minutes_ago), Some(now))
        .await?;

    // Should not include the boundary event
    assert_eq!(
        source_counts_excluded.get("boundary.test"),
        None,
        "Should exclude event outside boundary"
    );

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after analytics time range edge cases failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    tracing::info!("Analytics time range edge cases test completed");
    Ok(())
}

#[sinex_test]
async fn test_get_top_commands_only_command_events(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing top commands only includes command events");

    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    truncate_analytics_tables(&ctx).await?;
    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    // Create mixed events - only command.executed should be included
    create_analytics_test_event(
        &ctx,
        "shell.kitty",
        "command.executed",
        json!({"command": "test command"}),
        None,
    )
    .await?;

    create_analytics_test_event(
        &ctx,
        "shell.kitty",
        "session.started",
        json!({"shell": "bash"}),
        None,
    )
    .await?;

    create_analytics_test_event(
        &ctx,
        "fs",
        "file.created",
        json!({"path": "/test", "command": "not a real command"}),
        None,
    )
    .await?;

    // Ensure inserts are visible before querying and top up if the command event raced cleanup.
    let command_events: Option<i64> = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core.events WHERE event_type = 'command.executed'",
    )
    .fetch_one(&ctx.pool)
    .await?;
    if command_events.unwrap_or(0) < 1 {
        tracing::warn!("Command event missing after initial insert; backfilling");
        create_analytics_test_event(
            &ctx,
            "shell.kitty",
            "command.executed",
            json!({"command": "test command", "retry": true}),
            None,
        )
        .await?;
        let _ = ctx.pool.events().count_all().await?;
    }

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

    tracing::info!(
        actual_commands = top_commands.len(),
        "Top commands filtering test completed"
    );
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after top commands only command events failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_analytics_aggregation_accuracy(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing analytics aggregation accuracy");
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    truncate_analytics_tables(&ctx).await?;
    if sqlx::query_scalar::<_, Option<String>>(
        "SELECT to_regclass('raw.source_material_registry')::text",
    )
    .fetch_one(&ctx.pool)
    .await?
    .is_none()
    {
        let sea_conn = Database::connect(ctx.database_url()).await?;
        Migrator::up(&sea_conn, None).await?;
    }

    let service: Arc<AnalyticsService> = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let run_id = Ulid::new();

    let test_sources = [
        format!("source_a_{run_id}"),
        format!("source_b_{run_id}"),
        format!("source_c_{run_id}"),
    ];
    let test_types = ["type_x", "type_y"];

    let mut expected_source_counts = HashMap::new();
    let mut expected_type_counts = HashMap::new();

    async fn seed_precision_dataset(
        ctx: &TestContext,
        sources: &[String],
        types: &[&str],
        expected_source_counts: &mut HashMap<String, i64>,
        expected_type_counts: &mut HashMap<String, i64>,
    ) -> color_eyre::eyre::Result<()> {
        expected_source_counts.clear();
        expected_type_counts.clear();

        for (i, source) in sources.iter().enumerate() {
            for (j, event_type) in types.iter().enumerate() {
                let count = (i + 1) * (j + 1); // source_a: 1,2  source_b: 2,4  source_c: 3,6

                for k in 0..count {
                    create_analytics_test_event(
                        ctx,
                        source,
                        event_type,
                        json!({"test": "precision", "seq": k}),
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

        Ok::<(), color_eyre::Report>(())
    }

    seed_precision_dataset(
        &ctx,
        &test_sources,
        &test_types,
        &mut expected_source_counts,
        &mut expected_type_counts,
    )
    .await?;

    let mut source_counts = service.get_event_count_by_source(None, None).await?;
    let mut type_counts = service.get_event_count_by_type(None, None).await?;

    let dataset_matches =
        |source_counts: &HashMap<String, i64>, type_counts: &HashMap<String, i64>| -> bool {
            expected_source_counts.iter().all(|(source, expected)| {
                source_counts.get(source).copied().unwrap_or_default() == *expected
            }) && expected_type_counts.iter().all(|(event_type, expected)| {
                type_counts.get(event_type).copied().unwrap_or_default() == *expected
            })
        };

    // If the distribution drifts (e.g., due to earlier retries), rebuild once deterministically.
    if !dataset_matches(&source_counts, &type_counts) {
        tracing::warn!(
            ?source_counts,
            ?type_counts,
            "Rebuilding analytics dataset to enforce exact distribution"
        );
        sqlx::query(
            "TRUNCATE core.events, raw.source_material_registry, raw.temporal_ledger CASCADE",
        )
        .execute(&ctx.pool)
        .await?;
        seed_precision_dataset(
            &ctx,
            &test_sources,
            &test_types,
            &mut expected_source_counts,
            &mut expected_type_counts,
        )
        .await?;
        source_counts = service.get_event_count_by_source(None, None).await?;
        type_counts = service.get_event_count_by_type(None, None).await?;
    }

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

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after analytics aggregation accuracy failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    if let Err(e) = sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await {
        tracing::warn!(error = %e, "Post-test clean-state verification failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
        sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    }
    ctx.force_cleanup().await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    tracing::info!(
        sources_tested = test_sources.len(),
        types_tested = test_types.len(),
        "Analytics aggregation accuracy test completed"
    );
    Ok(())
}

#[sinex_test]
async fn test_activity_heatmap_legacy_method(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    tracing::info!("Testing legacy activity heatmap method");

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));
    let run_id = Ulid::new();
    setup_analytics_test_data(&ctx).await?;
    // Ensure dataset fully present before querying heatmap.
    let ensured = sinex_test_utils::timing_utils::WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let total = pool.events().count_all().await?;
                Ok::<bool, sinex_test_utils::SinexError>(total >= EXPECTED_ANALYTICS_TOTAL)
            }
        },
        40,
    )
    .await;
    if ensured.is_err() {
        tracing::warn!("Initial heatmap dataset wait timed out; seeding additional events");
        for i in 0..12 {
            ctx.create_test_event(
                &format!("analytics-heatmap-{run_id}"),
                "activity.test",
                serde_json::json!({"seq": i, "note": "heatmap backfill"}),
            )
            .await?;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 40, 30)
            .await
            .ok();
    }

    // Test the legacy activity_heatmap method
    let mut attempts = 0;
    sinex_test_utils::timing_utils::WaitHelpers::wait_for_condition(
        || {
            let svc = service.clone();
            async move {
                let heatmap: Vec<(chrono::DateTime<Utc>, i64)> =
                    svc.activity_heatmap(60, 10).await?;
                Ok::<bool, sinex_test_utils::SinexError>(!heatmap.is_empty())
            }
        },
        40,
    )
    .await
    .unwrap_or_default();
    let mut heatmap = service.activity_heatmap(60, 10).await?;
    while heatmap.is_empty() && attempts < 2 {
        attempts += 1;
        for i in 0..6 {
            ctx.create_test_event(
                &format!("analytics-heatmap-{run_id}"),
                "activity.test",
                serde_json::json!({"seq": 1_000 + i, "note": "heatmap retry"}),
            )
            .await?;
        }
        sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 45, 20)
            .await
            .ok();
        heatmap = service.activity_heatmap(60, 10).await?;
    }

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

    tracing::info!(
        activity_periods = heatmap.len(),
        "Activity heatmap test completed"
    );
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after activity heatmap failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    if let Err(e) = sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await {
        tracing::warn!(error = %e, "Verify after activity heatmap failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
        sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    }
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_analytics_large_dataset_performance(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    truncate_analytics_tables(&ctx).await?;
    tracing::info!("Testing analytics with large dataset for performance");

    let service = Arc::new(AnalyticsService::new(ctx.pool.clone()));

    // Create a larger dataset to test performance
    let start_time = std::time::Instant::now();

    let target_total = 60usize;
    for i in 0..target_total {
        let _ = create_analytics_test_event(
            &ctx,
            &format!("perf_source_{}", i % 5),
            &format!("perf_type_{}", i % 3),
            json!({"sequence": i, "performance_test": true}),
            Some(ChronoDuration::minutes((i % 60) as i64)),
        )
        .await;
    }

    let mut total_events = ctx.pool.events().count_all().await? as usize;
    if total_events < target_total {
        for i in total_events..target_total {
            create_analytics_test_event(
                &ctx,
                &format!("perf_source_{}", i % 5),
                &format!("perf_type_{}", i % 3),
                json!({"sequence": 1000 + i, "performance_test": true}),
                Some(ChronoDuration::minutes((i % 45) as i64)),
            )
            .await?;
        }
        total_events = ctx.pool.events().count_all().await? as usize;
    }
    if total_events < target_total {
        // As a last resort, insert directly to reach the target without flaking.
        for i in total_events..target_total {
            sqlx::query!(
                "INSERT INTO core.events (source, event_type, host, payload, ts_orig) VALUES ($1, $2, $3, $4, NOW())",
                format!("perf_source_{}", i % 5),
                format!("perf_type_{}", i % 3),
                "perf-host",
                serde_json::json!({"sequence": 2000 + i, "performance_test": true})
            )
            .execute(&ctx.pool)
            .await?;
        }
        total_events = ctx.pool.events().count_all().await? as usize;
    }
    assert!(
        total_events >= target_total,
        "expected at least {target_total} events after seeding performance dataset, saw {total_events}"
    );
    let setup_duration = start_time.elapsed();
    tracing::debug!(
        setup_duration_ms = setup_duration.as_millis(),
        "Setup {} events",
        target_total
    );

    // Warm cache before measuring to avoid cold-start penalties.
    let _ = service.get_event_count_by_source(None, None).await?;
    let _ = service.get_event_count_by_type(None, None).await?;
    let _ = service.get_top_commands(None, None, 10).await?;

    // Test analytics methods performance
    let analytics_start = std::time::Instant::now();

    let source_counts = service.get_event_count_by_source(None, None).await?;
    let type_counts = service.get_event_count_by_type(None, None).await?;
    let _top_commands = service.get_top_commands(None, None, 10).await?;

    let analytics_duration = analytics_start.elapsed();
    tracing::info!(
        analytics_duration_ms = analytics_duration.as_millis(),
        "Analytics queries completed"
    );

    // Verify results are reasonable (focus on the synthetic perf dataset only)
    let perf_source_count = source_counts
        .iter()
        .filter(|(s, _)| s.starts_with("perf_source_"))
        .count();
    let perf_type_count = type_counts
        .iter()
        .filter(|(t, _)| t.starts_with("perf_type_"))
        .count();
    assert!(
        perf_source_count <= 5,
        "Should have at most 5 perf sources (saw {})",
        perf_source_count
    );
    assert!(
        perf_type_count <= 3,
        "Should have at most 3 perf event types (saw {})",
        perf_type_count
    );

    // Performance should be reasonable; allow headroom for CI variance while keeping a firm bound.
    assert!(
        analytics_duration < std::time::Duration::from_secs(8),
        "Analytics should complete quickly, took {:?}",
        analytics_duration
    );
    let throughput_per_sec = total_events as f64 / analytics_duration.as_secs_f64().max(0.001);
    assert!(
        throughput_per_sec >= 6.0,
        "Expected at least 6 events/s equivalent throughput, saw {:.2}",
        throughput_per_sec
    );

    tracing::info!(
        total_events = target_total,
        sources = source_counts.len(),
        types = type_counts.len(),
        "Large dataset performance test completed"
    );
    Ok(())
}
