//! Query service integration tests focused on event querying patterns
//!
//! This module tests the query functionality of the `SearchService` with a focus on
//! different query patterns, time-based queries, and filtering capabilities.

use color_eyre::eyre::ensure;
use serde_json::json;
use sinex_services::{SearchQuery, SearchService};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use time::Duration;
use xtask::sandbox::dataset_seeds::{
    seed_events_via_scope, seed_query_dataset_semantic_min_via_scope, EventSpec, QueryDataset,
    SeedClock,
};
use xtask::sandbox::prelude::*;

async fn seed_query_dataset(scope: &PipelineScope<'_>) -> TestResult<(SeedClock, QueryDataset)> {
    let clock = SeedClock::new();
    let dataset = seed_query_dataset_semantic_min_via_scope(scope.ctx(), &clock).await?;
    scope.wait_for_event_count(dataset.events.len()).await?;
    Ok((clock, dataset))
}

#[sinex_serial_test]
async fn test_query_by_source_filter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    seed_query_dataset(&scope).await?;
    let service = Arc::new(SearchService::new(ctx.pool.clone()));

    let query = SearchQuery {
        text: None,
        sources: vec!["fs-watcher".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert!(!results.is_empty(), "Query should return fs-watcher events");
    assert!(results.iter().all(|r| r.source == "fs-watcher"));

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_query_by_event_type_filter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    seed_query_dataset(&scope).await?;
    let service = Arc::new(SearchService::new(ctx.pool.clone()));

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
    assert!(!results.is_empty(), "Expected command.executed results");
    assert!(results.iter().all(|r| r.event_type == "command.executed"));

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_query_by_time_range(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let (clock, _dataset) = seed_query_dataset(&scope).await?;
    let service = Arc::new(SearchService::new(ctx.pool.clone()));

    let reference_now = clock.now();
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![],
        start_time: Some(reference_now - Duration::hours(1)),
        end_time: Some(reference_now),
        limit: 20,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    // Events may or may not be in this timeframe depending on seeding
    for result in &results {
        assert!(result.timestamp <= reference_now);
    }

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_query_content_search(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::new();

    seed_events_via_scope(
        scope.ctx(),
        &clock,
        vec![
            EventSpec::new("shell.bash", "command.executed").with_payload(
                json!({"command": "cargo build --release", "content": "building rust project"}),
            ),
        ],
    )
    .await?;
    scope.wait_for_source_events("shell.bash", 1).await?;

    let service = Arc::new(SearchService::new(ctx.pool.clone()));
    let query = SearchQuery {
        text: Some("rust".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert!(
        !results.is_empty(),
        "Content search should find rust payloads"
    );

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_query_combined_filters(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::new();

    seed_events_via_scope(
        scope.ctx(),
        &clock,
        vec![EventSpec::new("fs-watcher", "file.created")
            .with_payload(json!({"path": "/project/src/main.rs", "size": 500}))],
    )
    .await?;
    scope.wait_for_source_events("fs-watcher", 1).await?;

    let service = Arc::new(SearchService::new(ctx.pool.clone()));
    let query = SearchQuery {
        text: Some("main".to_string()),
        sources: vec!["fs-watcher".to_string()],
        event_types: vec![],
        start_time: Some(clock.now() - Duration::hours(1)),
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert!(!results.is_empty(), "Combined filters should match results");
    assert!(results.iter().all(|r| r.source == "fs-watcher"));

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_query_ordering_by_timestamp(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    seed_query_dataset(&scope).await?;
    let service = Arc::new(SearchService::new(ctx.pool.clone()));

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
    assert!(!results.is_empty());
    for i in 1..results.len() {
        assert!(
            results[i - 1].timestamp >= results[i].timestamp,
            "Results should be ordered by timestamp descending"
        );
    }

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_query_pagination(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::new();
    let service = Arc::new(SearchService::new(ctx.pool.clone()));

    let pagination_specs: Vec<EventSpec> = (0..10)
        .map(|i| {
            EventSpec::new("test.pagination", "batch.event")
                .with_payload(json!({ "index": i, "batch": "pagination_test" }))
                .at(clock.tick(60_000)) // Advance by 1 minute each
        })
        .collect();
    seed_events_via_scope(scope.ctx(), &clock, pagination_specs).await?;
    scope.wait_for_source_events("test.pagination", 10).await?;

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

    let mut query2 = query.clone();
    query2.offset = 3;
    let page2 = service.search_events(query2).await?;
    assert_eq!(page2.len(), 3);

    let page1_ids: HashSet<_> = page1.iter().map(|r| r.event_id).collect();
    let page2_ids: HashSet<_> = page2.iter().map(|r| r.event_id).collect();
    assert!(page1_ids.is_disjoint(&page2_ids));

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_query_pagination_stable_during_concurrent_ingestion(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::new();
    let service = Arc::new(SearchService::new(ctx.pool.clone()));

    let source = "concurrent.ingest";
    let event_type = "concurrent.event";
    let total_events = 25usize;
    let start_time = clock.now() - Duration::minutes(1);
    let end_time = clock.now() + Duration::minutes(10);

    let progress = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let scope_ref = &scope;
    let ingest_task = {
        let progress = progress.clone();
        let done = done.clone();
        let scope = scope_ref;
        let clock = SeedClock::new();
        async move {
            for seq in 0..total_events {
                let ts = clock.tick(1000); // Advance by 1 second each
                let overrides = EventOverrides {
                    ts_orig: Some(ts.format_rfc3339()),
                    ..Default::default()
                };
                scope
                    .publish_with_overrides(
                        sinex_primitives::DynamicPayload::new(
                            source,
                            event_type,
                            json!({ "seq": seq }),
                        ),
                        overrides,
                    )
                    .await?;
                progress.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
            done.store(true, Ordering::SeqCst);
            Ok::<(), color_eyre::Report>(())
        }
    };

    let query_task = {
        let progress = progress.clone();
        let done = done.clone();
        let service = service.clone();
        async move {
            let mut last_count = 0usize;
            while !done.load(Ordering::SeqCst) {
                let query = SearchQuery {
                    text: None,
                    sources: vec![source.to_string()],
                    event_types: vec![event_type.to_string()],
                    start_time: Some(start_time),
                    end_time: Some(end_time),
                    limit: total_events as i32,
                    offset: 0,
                };
                let results = service.search_events(query).await?;
                for idx in 1..results.len() {
                    ensure!(
                        results[idx - 1].timestamp >= results[idx].timestamp,
                        "results must remain ordered by timestamp during ingestion"
                    );
                }
                let mut ids = HashSet::new();
                for result in &results {
                    ensure!(
                        ids.insert(result.event_id),
                        "duplicate event id found during ingestion"
                    );
                }
                let expected_min = progress.load(Ordering::SeqCst);
                ensure!(
                    results.len() >= expected_min,
                    "expected at least {expected_min} results during ingestion, saw {}",
                    results.len()
                );
                ensure!(
                    results.len() >= last_count,
                    "result count regressed during ingestion ({last_count} -> {})",
                    results.len()
                );
                last_count = results.len();
                tokio::task::yield_now().await;
            }
            Ok::<(), color_eyre::Report>(())
        }
    };

    let (ingest_result, query_result) = tokio::join!(ingest_task, query_task);
    ingest_result?;
    query_result?;

    scope.wait_for_source_events(source, total_events).await?;

    let base_query = SearchQuery {
        text: None,
        sources: vec![source.to_string()],
        event_types: vec![event_type.to_string()],
        start_time: Some(start_time),
        end_time: Some(end_time),
        limit: 10,
        offset: 0,
    };

    let page1 = service.search_events(base_query.clone()).await?;
    let mut query2 = base_query.clone();
    query2.offset = 10;
    let page2 = service.search_events(query2).await?;
    let mut query3 = base_query.clone();
    query3.offset = 20;
    let page3 = service.search_events(query3).await?;

    let mut paged_ids = HashSet::new();
    for page in [&page1, &page2, &page3] {
        for idx in 1..page.len() {
            ensure!(
                page[idx - 1].timestamp >= page[idx].timestamp,
                "paged results must remain ordered by timestamp"
            );
        }
        for result in page {
            ensure!(
                paged_ids.insert(result.event_id),
                "pagination returned duplicate event ids"
            );
        }
    }

    ensure!(
        paged_ids.len() == total_events,
        "pagination should cover all events (expected {total_events}, got {})",
        paged_ids.len()
    );

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_query_empty_results(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    seed_query_dataset(&scope).await?;
    let service = SearchService::new(ctx.pool.clone());

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

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_query_limit_bounds(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    seed_query_dataset(&scope).await?;
    let service = Arc::new(SearchService::new(ctx.pool.clone()));

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
        (results.len() as i64) <= sinex_primitives::Pagination::DEFAULT_LIMIT,
        "limit=0 should clamp to the default pagination limit"
    );

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

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_query_multiple_sources(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    seed_query_dataset(&scope).await?;
    let service = Arc::new(SearchService::new(ctx.pool.clone()));

    let query = SearchQuery {
        text: None,
        sources: vec!["fs-watcher".to_string(), "shell.bash".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 20,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert!(!results.is_empty(), "Expected multi-source results");
    assert!(results
        .iter()
        .all(|r| r.source == "fs-watcher" || r.source == "shell.bash"));

    scope.shutdown().await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_query_case_insensitive_search(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::new();

    seed_events_via_scope(
        scope.ctx(),
        &clock,
        vec![EventSpec::new("test.case", "mixed.case")
            .with_payload(json!({ "content": "Hello World Testing" }))],
    )
    .await?;
    scope.wait_for_source_events("test.case", 1).await?;

    let service = SearchService::new(ctx.pool.clone());
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
    assert!(!results.is_empty());
    assert!(results.iter().any(|r| r.snippet.contains("Hello World")));

    scope.shutdown().await?;
    Ok(())
}
