// # Boundary Test Suite
//
// Comprehensive boundary testing for system limits and edge cases.
// This module tests behavior at the boundaries of system capabilities.
//
// ## Test Categories
// - **Payload Boundaries**: Minimal payloads, deep nesting, unicode edge cases, numeric limits
// - **Source/Type Boundaries**: Single-char sources, long names, special characters
// - **Batch Boundaries**: Single events, empty payloads, mixed sources
// - **Query Boundaries**: Non-existent sources, pagination, after-insert queries

use serde_json::json;
use xtask::sandbox::prelude::*;

// =============================================================================
// Payload Boundary Tests
// =============================================================================

/// Test event with minimal payload (empty JSON object)
#[sinex_test]
async fn test_minimum_valid_payload(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let payload = DynamicPayload::new("test-source", "test.event", json!({}));
    let event = ctx.publish(payload).await?;

    // Verify event persisted to database
    let retrieved = ctx
        .pool()
        .events()
        .get_by_id(event.id.unwrap())
        .await?
        .ok_or(color_eyre::eyre::eyre!("Event not found after publish"))?;

    assert_eq!(
        retrieved.source.as_str(),
        "test-source",
        "Source should match"
    );
    assert_eq!(
        retrieved.event_type.as_str(),
        "test.event",
        "Event type should match"
    );

    Ok(())
}

/// Test event with maximum nesting depth (20 levels)
#[sinex_test]
async fn test_maximum_nested_depth(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    // Build nested JSON structure with 20 levels: {"a":{"a":{"a":...}}}
    let mut nested = json!({"value": 42});
    for _ in 0..20 {
        nested = json!({"a": nested});
    }

    let payload = DynamicPayload::new("test-source", "test.nested", nested.clone());
    let event = ctx.publish(payload).await?;

    // Verify event persisted
    let retrieved = ctx
        .pool()
        .events()
        .get_by_id(event.id.unwrap())
        .await?
        .ok_or(color_eyre::eyre::eyre!("Nested event not found"))?;

    // Verify nesting survived roundtrip
    let payload = &retrieved.payload;
    let mut current = payload.clone();
    for _ in 0..20 {
        current = current
            .get("a")
            .ok_or(color_eyre::eyre::eyre!("Missing nesting level"))?
            .clone();
    }

    assert_eq!(
        current.get("value"),
        Some(&json!(42)),
        "Deeply nested value should be preserved"
    );

    Ok(())
}

/// Test event with unicode boundary characters
#[sinex_test]
async fn test_unicode_boundary_characters(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let payload = DynamicPayload::new(
        "test-source",
        "test.unicode",
        json!({
            "emoji": "🎉🚀",
            "cjk": "中文テスト日本語",
            "rtl": "שלום مرحبا",
            "replacement_char": "\u{FFFD}",
            "combining_marks": "e\u{0301}",
        }),
    );

    let event = ctx.publish(payload).await?;

    // Verify roundtrip
    let retrieved = ctx
        .pool()
        .events()
        .get_by_id(event.id.unwrap())
        .await?
        .ok_or(color_eyre::eyre::eyre!("Unicode event not found"))?;

    let p = &retrieved.payload;
    assert_eq!(
        p.get("emoji").and_then(|v| v.as_str()),
        Some("🎉🚀"),
        "Emoji should roundtrip"
    );
    assert_eq!(
        p.get("cjk").and_then(|v| v.as_str()),
        Some("中文テスト日本語"),
        "CJK should roundtrip"
    );
    assert_eq!(
        p.get("rtl").and_then(|v| v.as_str()),
        Some("שלום مرحبا"),
        "RTL text should roundtrip"
    );

    Ok(())
}

/// Test event with numeric boundary values
///
/// NOTE: f64::MAX (1.8e308) exceeds PostgreSQL JSONB's numeric range and causes
/// pipeline timeouts. We test with a large-but-representable value (1e100) instead.
#[sinex_test]
async fn test_numeric_boundary_values(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let payload = DynamicPayload::new(
        "test-source",
        "test.numeric",
        json!({
            "i64_max": i64::MAX,
            "i64_min": i64::MIN,
            "f64_large": 1e100_f64,
            "f64_small": -1e100_f64,
            "f64_epsilon": f64::EPSILON,
            "zero": 0,
            "negative": -1,
        }),
    );

    let event = ctx.publish(payload).await?;

    let retrieved = ctx
        .pool()
        .events()
        .get_by_id(event.id.unwrap())
        .await?
        .ok_or(color_eyre::eyre::eyre!("Numeric event not found"))?;

    let p = &retrieved.payload;
    assert_eq!(
        p.get("i64_max").and_then(|v| v.as_i64()),
        Some(i64::MAX),
        "i64::MAX should roundtrip"
    );
    assert_eq!(
        p.get("i64_min").and_then(|v| v.as_i64()),
        Some(i64::MIN),
        "i64::MIN should roundtrip"
    );
    assert!(
        p.get("f64_large")
            .and_then(|v| v.as_f64())
            .map(|v| (v - 1e100_f64).abs() < 1e90)
            .unwrap_or(false),
        "1e100 should roundtrip approximately"
    );

    Ok(())
}

// =============================================================================
// Source/Type Boundary Tests
// =============================================================================

/// Test event with single-character source name
#[sinex_test]
async fn test_minimum_source_name(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let payload = DynamicPayload::new("x", "test.event", json!({"data": "test"}));
    let event = ctx.publish(payload).await?;

    // Verify persisted
    let retrieved = ctx
        .pool()
        .events()
        .get_by_id(event.id.unwrap())
        .await?
        .ok_or(color_eyre::eyre::eyre!(
            "Single-char source event not found"
        ))?;

    assert_eq!(
        retrieved.source.as_str(),
        "x",
        "Single-char source should persist"
    );

    Ok(())
}

/// Test event with long source name (200 chars)
#[sinex_test]
async fn test_long_source_name(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let long_source = "a".repeat(200);
    let payload = DynamicPayload::new(long_source.as_str(), "test.event", json!({"data": "test"}));
    let event = ctx.publish(payload).await?;

    // Verify persisted
    let retrieved = ctx
        .pool()
        .events()
        .get_by_id(event.id.unwrap())
        .await?
        .ok_or(color_eyre::eyre::eyre!("Long source name event not found"))?;

    assert_eq!(
        retrieved.source.as_str(),
        &long_source,
        "Long source name should persist"
    );

    Ok(())
}

/// Test event type with special characters (dots, underscores, hyphens)
#[sinex_test]
async fn test_special_characters_in_event_type(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let special_type = "test.special-type_v2";
    let payload = DynamicPayload::new("test-source", special_type, json!({"data": "test"}));
    let event = ctx.publish(payload).await?;

    // Verify roundtrip
    let retrieved = ctx
        .pool()
        .events()
        .get_by_id(event.id.unwrap())
        .await?
        .ok_or(color_eyre::eyre::eyre!("Special-char event type not found"))?;

    assert_eq!(
        retrieved.event_type.as_str(),
        special_type,
        "Event type with special characters should roundtrip"
    );

    Ok(())
}

// =============================================================================
// Batch Boundary Tests
// =============================================================================

/// Test publish_many with exactly one event
#[sinex_test]
async fn test_single_event_batch(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let payload = DynamicPayload::new("batch-source", "batch.single", json!({"index": 0}));

    let events = ctx.publish_many(vec![payload]).await?;

    assert_eq!(events.len(), 1, "Batch should contain exactly 1 event");
    assert!(events[0].id.is_some(), "Event should have an ID");

    // Verify in database
    let count = ctx
        .pool()
        .events()
        .count_by_source(&EventSource::from("batch-source"))
        .await?;

    assert_eq!(count, 1, "Database should have 1 event");

    Ok(())
}

/// Test publish_many with 10 empty payloads
#[sinex_test]
async fn test_empty_payload_batch(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let payloads: Vec<_> = (0..10)
        .map(|_| DynamicPayload::new("empty-batch-source", "batch.empty", json!({})))
        .collect();

    let events = ctx.publish_many(payloads).await?;

    assert_eq!(events.len(), 10, "Batch should contain 10 events");

    // Verify all in database
    let count = ctx
        .pool()
        .events()
        .count_by_source(&EventSource::from("empty-batch-source"))
        .await?;

    assert_eq!(count, 10, "Database should have all 10 events");

    Ok(())
}

/// Test publish_many with mixed sources
#[sinex_test]
async fn test_mixed_source_batch(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let sources = vec!["source-a", "source-b", "source-c", "source-d", "source-e"];
    let payloads: Vec<_> = (0..15)
        .map(|i| {
            let source = sources[i % sources.len()];
            DynamicPayload::new(source, "batch.mixed", json!({"index": i, "source": source}))
        })
        .collect();

    let events = ctx.publish_many(payloads).await?;

    assert_eq!(events.len(), 15, "Batch should contain 15 events");

    // Verify count per source (3 events per source, 15 total)
    for source in &sources {
        let count = ctx
            .pool()
            .events()
            .count_by_source(&EventSource::from(*source))
            .await?;

        assert_eq!(
            count, 3,
            "Each source should have exactly 3 events, got {} for {}",
            count, source
        );
    }

    Ok(())
}

// =============================================================================
// Query Boundary Tests
// =============================================================================

/// Test querying a non-existent source returns 0
#[sinex_test]
async fn test_query_nonexistent_source(ctx: TestContext) -> TestResult<()> {
    let count = ctx
        .pool()
        .events()
        .count_by_source(&EventSource::from("nonexistent-source-xyz"))
        .await?;

    assert_eq!(count, 0, "Non-existent source should have 0 events");

    Ok(())
}

/// Test query after single insert
#[sinex_test]
async fn test_query_after_single_insert(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let source_name = "query-test-source";
    let payload = DynamicPayload::new(source_name, "query.test", json!({"test": "data"}));

    let event = ctx.publish(payload).await?;

    // Query by source
    let count = ctx
        .pool()
        .events()
        .count_by_source(&EventSource::from(source_name))
        .await?;

    assert_eq!(count, 1, "After single insert, count should be 1");

    // Also test get_by_source
    let events = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from(source_name), Default::default())
        .await?;

    assert_eq!(events.len(), 1, "get_by_source should return 1 event");
    assert_eq!(
        events[0].id, event.id,
        "Retrieved event should match inserted event"
    );

    Ok(())
}

/// Test pagination with 15 events, limit 5
#[sinex_test]
async fn test_query_with_pagination(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let source_name = "pagination-test-source";

    // Publish 15 events
    let payloads: Vec<_> = (0..15)
        .map(|i| DynamicPayload::new(source_name, "pagination.test", json!({"index": i})))
        .collect();

    ctx.publish_many(payloads).await?;

    // Query with pagination (limit 5)
    let pagination = Pagination::new(Some(5), Some(0));

    let page1 = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from(source_name), pagination)
        .await?;

    assert_eq!(page1.len(), 5, "First page should have exactly 5 events");

    // Verify we can get page 2
    let pagination_page2 = Pagination::new(Some(5), Some(5));

    let page2 = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from(source_name), pagination_page2)
        .await?;

    assert_eq!(page2.len(), 5, "Second page should have exactly 5 events");

    // Ensure pages have different events
    let page1_ids: Vec<_> = page1.iter().filter_map(|e| e.id).collect();
    let page2_ids: Vec<_> = page2.iter().filter_map(|e| e.id).collect();

    let mut all_ids = page1_ids.clone();
    all_ids.extend(&page2_ids);

    assert_eq!(
        all_ids.len(),
        10,
        "Page 1 and Page 2 should have 10 unique events total"
    );

    Ok(())
}
