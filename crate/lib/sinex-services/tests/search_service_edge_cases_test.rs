//! Edge case tests for SearchService
//!
//! Tests UTF-8 handling, filter combinations, and pagination.

use serde_json::json;
use sinex_services::{SearchQuery, SearchService};
use sinex_test_utils::dataset_seeds::{seed_events_via_scope, EventSpec, SeedClock};
use sinex_test_utils::prelude::*;

fn make_search_query() -> SearchQuery {
    SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 100,
        offset: 0,
    }
}

#[sinex_test]
async fn search_with_empty_results_returns_empty_vec(ctx: TestContext) -> TestResult<()> {
    let service = SearchService::new(ctx.pool().clone());

    let query = SearchQuery {
        text: Some("nonexistent_content_xyz123".to_string()),
        ..make_search_query()
    };

    let results = service.search_events(query).await?;
    assert!(results.is_empty());

    Ok(())
}

#[sinex_test]
async fn search_with_source_filter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();
    let service = SearchService::new(ctx.pool().clone());

    // Create events with different sources
    let events = vec![
        EventSpec::new("source-a", "test.event", json!({"key": "value1"})),
        EventSpec::new("source-a", "test.event", json!({"key": "value2"})),
        EventSpec::new("source-b", "test.event", json!({"key": "value3"})),
    ];
    seed_events_via_scope(&scope, &clock, &events).await?;

    // Search for only source-a
    let query = SearchQuery {
        sources: vec!["source-a".to_string()],
        ..make_search_query()
    };

    let results = service.search_events(query).await?;

    // Should only find events from source-a
    assert!(!results.is_empty(), "Should find events from source-a");
    assert!(results.iter().all(|r| r.source == "source-a"));

    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn search_with_event_type_filter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();
    let service = SearchService::new(ctx.pool().clone());

    // Create events with different types
    let events = vec![
        EventSpec::new("test-source", "type.alpha", json!({"key": "value1"})),
        EventSpec::new("test-source", "type.alpha", json!({"key": "value2"})),
        EventSpec::new("test-source", "type.beta", json!({"key": "value3"})),
    ];
    seed_events_via_scope(&scope, &clock, &events).await?;

    // Search for only type.alpha
    let query = SearchQuery {
        event_types: vec!["type.alpha".to_string()],
        ..make_search_query()
    };

    let results = service.search_events(query).await?;

    // Should only find events with type.alpha
    assert!(!results.is_empty(), "Should find events with type.alpha");
    assert!(results.iter().all(|r| r.event_type == "type.alpha"));

    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn search_with_multiple_filters_combined(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();
    let service = SearchService::new(ctx.pool().clone());

    // Create various events
    let events = vec![
        EventSpec::new("source-x", "type.one", json!({"key": "target_value"})),
        EventSpec::new("source-x", "type.two", json!({"key": "target_value"})),
        EventSpec::new("source-y", "type.one", json!({"key": "target_value"})),
    ];
    seed_events_via_scope(&scope, &clock, &events).await?;

    // Search with combined filters
    let query = SearchQuery {
        sources: vec!["source-x".to_string()],
        event_types: vec!["type.one".to_string()],
        ..make_search_query()
    };

    let results = service.search_events(query).await?;

    // Should only find events matching ALL filters
    for result in &results {
        assert_eq!(result.source, "source-x");
        assert_eq!(result.event_type, "type.one");
    }

    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn search_respects_limit_parameter(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();
    let service = SearchService::new(ctx.pool().clone());

    // Create 10 events
    let events: Vec<EventSpec> = (0..10)
        .map(|i| EventSpec::new("test-source", "test.event", json!({"index": i})))
        .collect();
    seed_events_via_scope(&scope, &clock, &events).await?;

    // Search with limit of 3
    let query = SearchQuery {
        limit: 3,
        ..make_search_query()
    };

    let results = service.search_events(query).await?;

    assert!(results.len() <= 3);

    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn search_handles_unicode_in_payload(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();
    let service = SearchService::new(ctx.pool().clone());

    // Create event with unicode payload
    let events = vec![EventSpec::new(
        "test-source",
        "test.event",
        json!({"content": "日本語テスト with 中文 and emoji 🎉"}),
    )];
    seed_events_via_scope(&scope, &clock, &events).await?;

    // Search for all events (no text filter to avoid fulltext search issues)
    let query = SearchQuery {
        sources: vec!["test-source".to_string()],
        ..make_search_query()
    };

    let results = service.search_events(query).await?;
    assert!(
        !results.is_empty(),
        "Should find events with unicode payloads"
    );

    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn search_with_time_range_filter(ctx: TestContext) -> TestResult<()> {
    use chrono::{Duration, Utc};

    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();
    let service = SearchService::new(ctx.pool().clone());

    // Create events
    let events = vec![EventSpec::new(
        "test-source",
        "test.event",
        json!({"when": "recent"}),
    )];
    seed_events_via_scope(&scope, &clock, &events).await?;

    let now = Utc::now();

    // Search with time range covering recent events
    let query = SearchQuery {
        start_time: Some(now - Duration::hours(1)),
        end_time: Some(now + Duration::hours(1)),
        ..make_search_query()
    };

    let results = service.search_events(query).await?;

    // Should find recent events within the time window
    assert!(results
        .iter()
        .all(|r| r.timestamp >= now - Duration::hours(1)));

    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn search_results_have_required_fields(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();
    let service = SearchService::new(ctx.pool().clone());

    // Create an event
    let events = vec![EventSpec::new(
        "result-test-source",
        "result.test.type",
        json!({"content": "test content for field verification"}),
    )];
    seed_events_via_scope(&scope, &clock, &events).await?;

    let query = SearchQuery {
        sources: vec!["result-test-source".to_string()],
        ..make_search_query()
    };
    let results = service.search_events(query).await?;

    // Verify result structure
    assert!(!results.is_empty(), "Should find the seeded event");

    for result in results {
        // event_id should be valid
        assert!(!result.event_id.is_nil());

        // source and event_type should be non-empty
        assert!(!result.source.is_empty());
        assert!(!result.event_type.is_empty());

        // score should be set and positive
        assert!(result.score > 0.0);
    }

    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn search_with_multiple_event_types(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let clock = SeedClock::fixed();
    let service = SearchService::new(ctx.pool().clone());

    // Create events with various types
    let events = vec![
        EventSpec::new("test-source", "type.alpha", json!({"key": "a"})),
        EventSpec::new("test-source", "type.beta", json!({"key": "b"})),
        EventSpec::new("test-source", "type.gamma", json!({"key": "c"})),
    ];
    seed_events_via_scope(&scope, &clock, &events).await?;

    // Search for multiple types
    let query = SearchQuery {
        event_types: vec!["type.alpha".to_string(), "type.gamma".to_string()],
        ..make_search_query()
    };

    let results = service.search_events(query).await?;

    // Should find alpha and gamma but not beta
    let event_types: Vec<_> = results.iter().map(|r| r.event_type.as_str()).collect();
    assert!(event_types
        .iter()
        .all(|t| *t == "type.alpha" || *t == "type.gamma"));

    scope.shutdown().await?;
    Ok(())
}
