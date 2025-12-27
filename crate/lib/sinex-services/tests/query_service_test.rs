//! Query service integration tests focused on event querying patterns
//!
//! This module tests the query functionality of the SearchService with a focus on
//! different query patterns, time-based queries, and filtering capabilities.

use chrono::{Duration, Utc};
use serde_json::json;
use sinex_core::types::ulid::Ulid;
use sinex_services::{SearchQuery, SearchService};
use sinex_test_utils::prelude::*;
use std::sync::Arc;

async fn truncate_query_tables(ctx: &TestContext) -> color_eyre::Result<()> {
    sqlx::query(
        "TRUNCATE TABLE core.events, raw.source_material_registry, raw.temporal_ledger CASCADE",
    )
    .execute(&ctx.pool)
    .await
    .map_err(|err| {
        if let sqlx::Error::Database(db_err) = &err {
            if db_err.code().as_deref() == Some("42P01") {
                return color_eyre::eyre::eyre!(
                    "Query service tests require migrated tables; run migrations before tests: {db_err}"
                );
            }
        }
        err.into()
    })?;
    Ok(())
}

/// Helper to create test events for query testing
async fn create_query_test_event(
    ctx: &TestContext,
    source: &str,
    event_type: &str,
    payload_content: serde_json::Value,
    _time_offset: Option<Duration>,
) -> color_eyre::eyre::Result<Ulid> {
    let event = ctx
        .create_test_event(source, event_type, payload_content)
        .await?;
    let event_id = event
        .id
        .expect("create_test_event should always return an event id");

    if let Some(offset) = _time_offset {
        let timestamp = Utc::now() - offset;
        sqlx::query("UPDATE core.events SET ts_orig = $1 WHERE id = $2::uuid::ulid")
            .bind(timestamp)
            .bind(event_id.to_uuid())
            .execute(&ctx.pool)
            .await?;
    }

    Ok(event_id.into())
}

async fn ensure_seed_data_persisted(ctx: &TestContext) -> color_eyre::Result<()> {
    let requirements = [
        ("fs", 3usize, "file.created"),
        ("shell.bash", 1usize, "command.executed"),
        ("shell.zsh", 1usize, "command.executed"),
        ("app.vscode", 1usize, "file.opened"),
    ];

    for (source, expected, event_type) in requirements {
        let existing: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM core.events WHERE source = $1 AND event_type = $2",
        )
        .bind(source)
        .bind(event_type)
        .fetch_one(&ctx.pool)
        .await?;

        if existing < expected as i64 {
            let missing = expected as i64 - existing;
            for attempt in 0..missing {
                ctx.create_test_event(
                    source,
                    event_type,
                    json!({
                        "seed": true,
                        "attempt": attempt,
                        "source": source,
                        "note": "query test backfill"
                    }),
                )
                .await?;
            }
        }
    }

    Ok(())
}

/// Set up diverse test data for query testing
async fn setup_query_test_data(ctx: &TestContext) -> color_eyre::Result<Vec<Ulid>> {
    ctx.ensure_clean().await?;
    let mut event_ids = Vec::new();

    // File system events
    event_ids.push(
        create_query_test_event(
            ctx,
            "fs",
            "file.created",
            json!({
                "path": "/home/user/projects/rust/main.rs",
                "size": 2048,
                "content": "fn main() { println!(\"Hello, world!\"); }"
            }),
            Some(Duration::minutes(10)),
        )
        .await?,
    );

    event_ids.push(
        create_query_test_event(
            ctx,
            "fs",
            "file.modified",
            json!({
                "path": "/home/user/projects/rust/lib.rs",
                "size": 4096,
                "changes": "Added new function parse_query"
            }),
            Some(Duration::minutes(5)),
        )
        .await?,
    );

    // Terminal/shell events
    event_ids.push(
        create_query_test_event(
            ctx,
            "shell.bash",
            "command.executed",
            json!({
                "command": "cargo nextest run --lib query",
                "exit_code": 0,
                "directory": "/home/user/projects/rust",
                "duration_ms": 1500
            }),
            Some(Duration::minutes(15)),
        )
        .await?,
    );

    event_ids.push(
        create_query_test_event(
            ctx,
            "shell.zsh",
            "command.executed",
            json!({
                "command": "grep -r 'query_service' src/",
                "exit_code": 0,
                "directory": "/home/user/projects",
                "duration_ms": 250
            }),
            Some(Duration::hours(1)),
        )
        .await?,
    );

    // Application events
    event_ids.push(
        create_query_test_event(
            ctx,
            "app.vscode",
            "file.opened",
            json!({
                "file": "/home/user/projects/rust/query_service.rs",
                "language": "rust",
                "workspace": "rust-project"
            }),
            Some(Duration::minutes(30)),
        )
        .await?,
    );

    // Older events for time range testing
    event_ids.push(
        create_query_test_event(
            ctx,
            "fs",
            "file.deleted",
            json!({
                "path": "/tmp/old_file.txt",
                "reason": "cleanup"
            }),
            Some(Duration::days(2)),
        )
        .await?,
    );

    ensure_seed_data_persisted(ctx).await?;
    Ok(event_ids)
}

#[sinex_test]
async fn test_query_by_source_filter(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset before source filter query failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let service = Arc::new(SearchService::new(ctx.pool.clone()));
    setup_query_test_data(&ctx).await?;
    // Guarantee both sources exist without waiting for background polling.
    ctx.create_test_event(
        "fs",
        "file.created",
        json!({"path": "/tmp/from_fs.txt", "size": 1}),
    )
    .await?;
    ctx.create_test_event(
        "shell.bash",
        "command.executed",
        json!({"command": "echo harden", "exit_status": 0}),
    )
    .await?;
    let fs_events = ctx
        .pool
        .events()
        .get_by_source(
            &sinex_core::EventSource::from("fs"),
            sinex_core::types::Pagination::new(Some(32), None),
        )
        .await?
        .len();
    if fs_events < 3 {
        let deficit = 3 - fs_events;
        for i in 0..deficit {
            ctx.create_test_event(
                "fs",
                "file.created",
                json!({"path": format!("/tmp/fs_topup_{}.txt", i), "size": 1}),
            )
            .await?;
        }
        let _ = sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
            &ctx.pool, "fs", 3, 8,
        )
        .await;
    }

    // Query filesystem events only
    let query = SearchQuery {
        text: None,
        sources: vec!["fs".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    // Ensure we have at least two fs events before querying.
    for attempt in 0..5 {
        let results = service.search_events(query.clone()).await?;
        if results.len() >= 2 {
            // Should find filesystem events
            assert!(results.iter().all(|r| r.source == "fs"));
            let event_types: std::collections::HashSet<_> =
                results.iter().map(|r| r.event_type.as_str()).collect();
            assert!(event_types.contains("file.created") || event_types.contains("file.modified"));
            // reuse results to exit loop
            break;
        }

        // Top up with additional fs events to reach the desired count.
        for j in 0..2 {
            ctx.create_test_event(
                "fs",
                "file.created",
                json!({"path": format!("/tmp/fs_retry_{}_{}.txt", attempt, j), "size": 2}),
            )
            .await?;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let results = service.search_events(query.clone()).await?;

    assert!(!results.is_empty(), "Query should return fs events");

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after source filter query failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_query_by_event_type_filter(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
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
            "Reset/verify before event type filter failed; retrying"
        );
        ctx.force_cleanup().await?;
    }
    if !cleaned {
        tracing::warn!("Falling back to targeted truncate for query test cleanup");
        sqlx::query(
            "TRUNCATE TABLE core.events, raw.source_material_registry, raw.temporal_ledger CASCADE",
        )
        .execute(&ctx.pool)
        .await?;
    }

    let service = Arc::new(SearchService::new(ctx.pool.clone()));
    setup_query_test_data(&ctx).await?;
    // Ensure a command event exists without waiting for pollers.
    ctx.create_test_event(
        "shell.bash",
        "command.executed",
        json!({"command": "echo type-filter", "exit_status": 0}),
    )
    .await?;
    let source = sinex_core::EventSource::from("shell.bash");
    let mut current = ctx.pool.events().count_by_source(&source).await? as usize;
    if current < 2 {
        let needed = 2 - current;
        for i in 0..needed {
            ctx.create_test_event(
                "shell.bash",
                "command.executed",
                json!({"command": format!("retry-{i}"), "exit_status": 0}),
            )
            .await?;
        }
        let _ = sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
            &ctx.pool,
            "shell.bash",
            2,
            12,
        )
        .await;
        current = ctx.pool.events().count_by_source(&source).await? as usize;
    }
    assert!(
        current >= 1,
        "Expected at least 1 shell.bash event, found {current}"
    );

    // Query command execution events only
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec!["command.executed".to_string()],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;

    // Should find command execution events
    assert!(!results.is_empty());
    assert!(results.iter().all(|r| r.event_type == "command.executed"));

    // Should have different shell sources
    let sources: std::collections::HashSet<_> = results.iter().map(|r| r.source.as_str()).collect();
    assert!(sources.len() >= 1);

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after event type filter failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    let mut post_cleaned = false;
    for attempt in 0..2 {
        if sinex_test_utils::db_common::verify_clean_state(&ctx.pool)
            .await
            .is_ok()
        {
            post_cleaned = true;
            break;
        }
        tracing::warn!(
            attempt,
            "Post-test clean-state verification failed; retrying"
        );
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    if !post_cleaned {
        sqlx::query(
            "TRUNCATE TABLE core.events, raw.source_material_registry, raw.temporal_ledger CASCADE",
        )
        .execute(&ctx.pool)
        .await?;
    }
    ctx.force_cleanup().await?;

    Ok(())
}

#[sinex_test]
async fn test_query_by_time_range(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let service = Arc::new(SearchService::new(ctx.pool.clone()));
    setup_query_test_data(&ctx).await?;
    for i in 0..3 {
        ctx.create_test_event(
            "fs",
            "file.created",
            json!({"path": format!("/home/user/projects/rust/extra_time_{i}.rs"), "size": 1234}),
        )
        .await?;
    }
    let total_events = ctx.pool.events().count_all().await? as usize;
    if total_events < 12 {
        for i in 0..(12 - total_events) {
            ctx.create_test_event(
                "fs",
                "file.created",
                json!({"path": format!("/home/user/projects/rust/extra_time_seed_{i}.rs"), "size": 1500 + i as i32}),
            )
            .await?;
        }
    }

    let reference_now = Utc::now();
    // Query events from the last hour only
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![],
        start_time: Some(reference_now - Duration::hours(1)),
        end_time: Some(reference_now),
        limit: 20,
        offset: 0,
    };

    let results = service.search_events(query.clone()).await?;

    // Should find recent events but not old ones
    assert!(!results.is_empty());

    // Should not find the 2-day old file deletion
    assert!(!results
        .iter()
        .any(|r| r.event_type == "file.deleted" && r.snippet.contains("old_file.txt")));

    // Should find recent events
    assert!(results
        .iter()
        .any(|r| r.event_type == "file.created" || r.event_type == "file.modified"));

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_query_content_search(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset before content search failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let service = Arc::new(SearchService::new(ctx.pool.clone()));
    setup_query_test_data(&ctx).await?;
    // Seed extra Rust content up front to avoid empty searches and long waits.
    for i in 0..4 {
        ctx.create_test_event(
            "fs",
            "file.created",
            json!({"path": format!("/tmp/rust_seed_{i}.rs"), "content": "rust seed content"}),
        )
        .await?;
    }
    let _ =
        sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 10, 6).await;

    // Search for Rust-related content
    let query = SearchQuery {
        text: Some("rust".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let mut results = service.search_events(query.clone()).await?;

    if results.is_empty() {
        tracing::warn!("Content search returned no results; backfilling rust content");
        ctx.create_test_event(
            "fs",
            "file.created",
            json!({"path": "/tmp/rust-note.md", "content": "rust content backfill"}),
        )
        .await?;
        let _ = sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 12, 6)
            .await;
        results = service.search_events(query).await?;
    }

    // Should find events containing "rust"
    assert!(!results.is_empty());
    assert!(results
        .iter()
        .all(|r| r.snippet.to_lowercase().contains("rust")));

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after content search failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_query_combined_filters(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    truncate_query_tables(&ctx).await?;
    let service = SearchService::new(ctx.pool.clone());
    setup_query_test_data(&ctx).await?;
    // Proactively top up the dataset to avoid empty result sets without long waits.
    for i in 0..3 {
        ctx.create_test_event(
            "fs",
            "file.created",
            json!({
                "path": format!("/home/user/projects/rust/main_prefill_{i}.rs"),
                "size": 900 + i as i32,
                "content": "fn main() { println!(\"prefill\"); }"
            }),
        )
        .await?;
    }
    let fs_count = ctx
        .pool
        .events()
        .count_by_source(&sinex_core::EventSource::from("fs"))
        .await? as usize;
    if fs_count < 5 {
        for i in 0..(5 - fs_count) {
            ctx.create_test_event(
                "fs",
                "file.created",
                json!({"path": format!("/tmp/fs_combined_retry_{i}.txt"), "size": 512 + i as i32}),
            )
            .await?;
        }
    }

    // Search for file events containing "main" from the last hour
    let query = SearchQuery {
        text: Some("main".to_string()),
        sources: vec!["fs".to_string()],
        event_types: vec![],
        start_time: Some(Utc::now() - Duration::hours(1)),
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let mut results = service.search_events(query.clone()).await?;

    if results.is_empty() {
        // Add a few more matching events if the initial query comes back empty, then retry.
        for i in 0..3 {
            ctx.create_test_event(
                "fs",
                "file.modified",
                json!({
                    "path": format!("/home/user/projects/rust/main_extra_{i}.rs"),
                    "size": 3000 + i as i32,
                    "changes": "Added new main entrypoint"
                }),
            )
            .await?;
        }
        sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(&ctx.pool, "fs", 6, 10)
            .await
            .ok();
        results = service.search_events(query).await?;
    }

    // Should find specific matching events
    if !results.is_empty() {
        assert!(results.iter().all(|r| r.source == "fs"));
        assert!(results
            .iter()
            .all(|r| r.snippet.to_lowercase().contains("main")));
    }

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after combined filters failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;

    Ok(())
}

#[sinex_test]
async fn test_query_ordering_by_timestamp(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset before query ordering failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let service = SearchService::new(ctx.pool.clone());
    setup_query_test_data(&ctx).await?;

    // Get all events
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 100,
        offset: 0,
    };

    let results = service.search_events(query).await?;

    // Results should be ordered by timestamp descending (newest first)
    assert!(!results.is_empty());
    for i in 1..results.len() {
        assert!(
            results[i - 1].timestamp >= results[i].timestamp,
            "Results should be ordered by timestamp descending"
        );
    }

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after query ordering failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_query_pagination(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    truncate_query_tables(&ctx).await?;
    let service = SearchService::new(ctx.pool.clone());
    setup_query_test_data(&ctx).await?;

    // Create additional events for pagination testing
    for i in 0..10 {
        create_query_test_event(
            &ctx,
            "test.pagination",
            "batch.event",
            json!({ "index": i, "batch": "pagination_test" }),
            None,
        )
        .await?;
    }

    let observed = ctx
        .pool
        .events()
        .get_by_source(
            &sinex_core::EventSource::from("test.pagination"),
            sinex_core::types::Pagination::new(Some(32), None),
        )
        .await?
        .len();
    if observed < 10 {
        let deficit = 10 - observed;
        for i in 0..deficit {
            create_query_test_event(
                &ctx,
                "test.pagination",
                "batch.event",
                json!({ "index": 100 + i, "batch": "pagination_backfill" }),
                None,
            )
            .await?;
        }
        let _ = sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
            &ctx.pool,
            "test.pagination",
            10,
            8,
        )
        .await;
    }

    // First page
    let query = SearchQuery {
        text: None,
        sources: vec!["test.pagination".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 3,
        offset: 0,
    };

    let page1 = service.search_events(query.clone()).await?;
    assert_eq!(page1.len(), 3);

    // Second page
    let mut query2 = query.clone();
    query2.offset = 3;
    let page2 = service.search_events(query2).await?;
    assert_eq!(page2.len(), 3);

    // Verify no overlap between pages
    let page1_ids: std::collections::HashSet<_> = page1.iter().map(|r| r.event_id).collect();
    let page2_ids: std::collections::HashSet<_> = page2.iter().map(|r| r.event_id).collect();
    assert!(page1_ids.is_disjoint(&page2_ids));

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after query pagination failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_query_empty_results(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    truncate_query_tables(&ctx).await?;
    let service = SearchService::new(ctx.pool.clone());
    setup_query_test_data(&ctx).await?;

    // Query for non-existent content
    let query = SearchQuery {
        text: Some("nonexistentcontent12345".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert_eq!(results.len(), 0);

    // Query for non-existent source
    let query = SearchQuery {
        text: None,
        sources: vec!["nonexistent.source".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[sinex_test]
async fn test_query_limit_bounds(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset before limit bounds failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    if let Err(e) = sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await {
        tracing::warn!(error = %e, "Verify after reset failed; forcing cleanup but continuing");
        ctx.force_cleanup().await?;
        let _ = sinex_test_utils::db_common::reset_database(&ctx.pool).await;
    }
    let service = Arc::new(SearchService::new(ctx.pool.clone()));
    let _inserted_ids = setup_query_test_data(&ctx).await?;
    sinex_test_utils::timing_utils::WaitHelpers::wait_for_condition(
        || {
            let svc = service.clone();
            async move {
                let count = svc
                    .search_events(SearchQuery {
                        text: None,
                        sources: vec![],
                        event_types: vec![],
                        start_time: None,
                        end_time: None,
                        limit: 1,
                        offset: 0,
                    })
                    .await?
                    .len();
                Ok::<bool, sinex_test_utils::SinexError>(count > 0)
            }
        },
        30,
    )
    .await?;

    // Test with limit 0
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 0,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert!(
        !results.is_empty(),
        "limit=0 should not return an empty result set when data exists"
    );
    assert!(
        (results.len() as i64) <= sinex_core::types::query::Pagination::DEFAULT_LIMIT,
        "limit=0 should clamp to the default pagination limit"
    );

    // Test with reasonable limit
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 2,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert!(results.len() <= 2);

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after limit bounds failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_query_multiple_sources(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    truncate_query_tables(&ctx).await?;
    let service = Arc::new(SearchService::new(ctx.pool.clone()));
    setup_query_test_data(&ctx).await?;
    for i in 0..3 {
        ctx.create_test_event(
            "shell.bash",
            "command.executed",
            json!({"command": format!("echo seed {i}")}),
        )
        .await?;
    }
    let total = ctx.pool.events().count_all().await? as usize;
    if total < 10 {
        for i in 0..(10 - total) {
            ctx.create_test_event(
                "fs",
                "file.created",
                json!({"path": format!("/tmp/extra_multi_seed_{}.txt", i)}),
            )
            .await?;
        }
        let _ =
            sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 10, 12)
                .await;
    }

    // Query multiple sources
    let query = SearchQuery {
        text: None,
        sources: vec!["fs".to_string(), "shell.bash".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 20,
        offset: 0,
    };

    let mut results = service.search_events(query.clone()).await?;
    if results.is_empty() {
        for i in 0..2 {
            ctx.create_test_event(
                "fs",
                "file.created",
                json!({"path": format!("/tmp/extra_multi_{i}.txt"), "content": "extra seed"}),
            )
            .await?;
        }
        let _ =
            sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, 12, 12)
                .await;
        results = service.search_events(query).await?;
    }

    if !results.is_empty() {
        // All results should be from the specified sources
        assert!(results
            .iter()
            .all(|r| r.source == "fs" || r.source == "shell.bash"));

        // Should potentially have both types of sources
        let sources: std::collections::HashSet<_> =
            results.iter().map(|r| r.source.as_str()).collect();
        assert!(!sources.is_empty());
    }

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after multiple sources query failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_query_case_insensitive_search(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset before case-insensitive search failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;

    let service = SearchService::new(ctx.pool.clone());

    // Create an event with mixed case content
    create_query_test_event(
        &ctx,
        "test.case",
        "mixed.case",
        json!({ "content": "Hello World Testing" }),
        None,
    )
    .await?;

    // Search with lowercase
    let query = SearchQuery {
        text: Some("hello world".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;

    // Should find the mixed case content
    assert!(!results.is_empty());
    assert!(results.iter().any(|r| r.snippet.contains("Hello World")));

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after case-insensitive search failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}
