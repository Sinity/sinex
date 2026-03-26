//! Composable Event Query Engine Tests
//!
//! Comprehensive test suite for the composable event query engine.
//! Tests cover:
//! - Filter composition (sources, types, payload filters)
//! - Cursor-based pagination (forward/backward)
//! - Text search with relevance scoring
//! - Payload filters (Contains, `HasKey`, Path operators)
//! - Filter composition (And, Or, Not)
//! - Aggregation modes (Count, `CountBy`, `TimeSeries`, `SourceStats`)
//! - Lineage traversal (ancestors, descendants, depth limits)
//! - Edge cases (empty results, estimates, defaults)

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_db::{DynamicPayload, Id};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::query::{
    AggregationMode, Cursor, EventQuery, EventQueryResult, GroupByField, LineageDirection,
    LineageQuery, PathOp, PayloadFilter, SortDirection, TimeSeriesOrder,
};
use std::str::FromStr;
use uuid::Uuid;
use xtask::sandbox::prelude::*;

// ============================================================================
// FILTER COMPOSITION TESTS
// ============================================================================

/// Test: Source + Type filter combination
#[sinex_test]
async fn test_filter_source_and_type(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("filter-test")).await?;

    // Create event 1: source-a, type-a
    let _e1 = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("source-a", "type-a", json!({"key": "value1"}))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    // Create event 2: source-a, type-b (should NOT match type filter)
    let _e2 = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("source-a", "type-b", json!({"key": "value2"}))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    // Create event 3: source-b, type-a (should NOT match source filter)
    let _e3 = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("source-b", "type-a", json!({"key": "value3"}))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    // Query with both filters
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("source-a")],
            event_types: vec![EventType::from_static("type-a")],
            ..Default::default()
        })
        .await?;

    match result {
        EventQueryResult::Events { events, .. } => {
            assert_eq!(events.len(), 1, "Should match exactly one event");
            assert_eq!(events[0].event.source.as_str(), "source-a");
            assert_eq!(events[0].event.event_type.as_str(), "type-a");
        }
        _ => panic!("Expected Events result"),
    }

    Ok(())
}

// ============================================================================
// CURSOR PAGINATION TESTS
// ============================================================================

/// Test: Forward pagination with cursor
#[sinex_test]
async fn test_cursor_forward_pagination(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("pagination-test")).await?;

    // Insert 15 events
    for i in 0..15 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new("test-source", "test.type", json!({"index": i}))
                    .from_material(material_id)
                    .build()?,
            )
            .await?;
    }

    // Query page 1 with limit=5
    let page1 = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("test-source")],
            limit: 5,
            direction: SortDirection::Desc,
            ..Default::default()
        })
        .await?;

    let (page1_events, next_cursor) = match page1 {
        EventQueryResult::Events {
            events,
            next_cursor,
            ..
        } => (events, next_cursor),
        _ => panic!("Expected Events result"),
    };

    assert_eq!(page1_events.len(), 5);
    assert!(
        next_cursor.is_some(),
        "Should have next_cursor for pagination"
    );

    let cursor_val = next_cursor.unwrap();

    // Query page 2 using next_cursor
    let cursor_uuid = Uuid::from_str(&cursor_val)
        .map_err(|e| sinex_primitives::SinexError::parse(format!("Invalid cursor UUIDv7: {e}")))?;
    let page2 = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("test-source")],
            limit: 5,
            cursor: Some(Cursor {
                after: Some(Id::from_uuid(cursor_uuid)),
                before: None,
            }),
            direction: SortDirection::Desc,
            ..Default::default()
        })
        .await?;

    let page2_events = match page2 {
        EventQueryResult::Events { events, .. } => events,
        _ => panic!("Expected Events result"),
    };

    assert_eq!(page2_events.len(), 5);

    // Verify no duplicates between pages
    let page1_ids: Vec<_> = page1_events.iter().filter_map(|e| e.event.id).collect();
    let page2_ids: Vec<_> = page2_events.iter().filter_map(|e| e.event.id).collect();

    for id in &page2_ids {
        assert!(
            !page1_ids.contains(id),
            "Page 2 should not contain events from page 1"
        );
    }

    Ok(())
}

/// Test: Ascending direction cursor pagination
#[sinex_test]
async fn test_cursor_ascending_direction(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("asc-test")).await?;

    // Insert 5 events
    for i in 0..5 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new("asc-source", "test.type", json!({"index": i}))
                    .from_material(material_id)
                    .build()?,
            )
            .await?;
    }

    // Query with ascending direction
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("asc-source")],
            direction: SortDirection::Asc,
            limit: 10,
            ..Default::default()
        })
        .await?;

    let events = match result {
        EventQueryResult::Events { events, .. } => events,
        _ => panic!("Expected Events result"),
    };

    // Verify ascending order (earlier UUIDv7 IDs first)
    for i in 0..events.len() - 1 {
        let id1 = events[i].event.id.unwrap();
        let id2 = events[i + 1].event.id.unwrap();
        assert!(
            id1.to_string() < id2.to_string(),
            "Events should be in ascending order"
        );
    }

    Ok(())
}

// ============================================================================
// TEXT SEARCH TESTS
// ============================================================================

/// Test: Text search with relevance score and snippet
#[sinex_test]
async fn test_text_search_with_relevance(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("text-search")).await?;

    // Create event with searchable text
    let _match_event = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "search-source",
                "document.indexed",
                json!({"content": "This is a special searchterm in the document"}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;

    // Create event without the search term
    let _no_match_event = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "search-source",
                "document.indexed",
                json!({"content": "This is a regular document"}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;

    // Text search query
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("search-source")],
            payload: Some(PayloadFilter::TextSearch {
                text: "searchterm".to_string(),
            }),
            ..Default::default()
        })
        .await?;

    let events = match result {
        EventQueryResult::Events { events, .. } => events,
        _ => panic!("Expected Events result"),
    };

    assert_eq!(
        events.len(),
        1,
        "Should match only the event with searchterm"
    );
    assert!(
        events[0].relevance_score.is_some(),
        "Text search should populate relevance_score"
    );
    assert!(events[0].relevance_score.unwrap() > 0.0);

    Ok(())
}

// ============================================================================
// PAYLOAD FILTER TESTS
// ============================================================================

/// Test: PayloadFilter::Contains
#[sinex_test]
async fn test_payload_filter_contains(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("contains-test")).await?;

    // Event matching the filter
    let _match_event = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "filter-source",
                "test.type",
                json!({"color": "blue", "size": 42, "other": "data"}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;

    // Event not matching (missing 'color': 'blue')
    let _no_match = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "filter-source",
                "test.type",
                json!({"color": "red", "size": 42}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;

    // Query with Contains filter
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("filter-source")],
            payload: Some(PayloadFilter::Contains {
                value: json!({"color": "blue"}),
            }),
            ..Default::default()
        })
        .await?;

    let events = match result {
        EventQueryResult::Events { events, .. } => events,
        _ => panic!("Expected Events result"),
    };

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.payload["color"], json!("blue"));

    Ok(())
}

/// Test: PayloadFilter::HasKey
#[sinex_test]
async fn test_payload_filter_has_key(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("haskey-test")).await?;

    // Event with the key
    let _with_key = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "key-source",
                "test.type",
                json!({"special_key": "exists", "other": "value"}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;

    // Event without the key
    let _without_key = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "key-source",
                "test.type",
                json!({"other": "value", "another": "field"}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;

    // Query with HasKey filter
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("key-source")],
            payload: Some(PayloadFilter::HasKey {
                key: "special_key".to_string(),
            }),
            ..Default::default()
        })
        .await?;

    let events = match result {
        EventQueryResult::Events { events, .. } => events,
        _ => panic!("Expected Events result"),
    };

    assert_eq!(events.len(), 1);
    assert!(events[0].event.payload.get("special_key").is_some());

    Ok(())
}

/// Test: PayloadFilter::Path with Gt operator
#[sinex_test]
async fn test_payload_filter_path_gt(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("path-gt-test")).await?;

    // Small value (should NOT match)
    let _small = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("path-source", "test.type", json!({"size": 512}))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    // Large value (should match)
    let _large = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("path-source", "test.type", json!({"size": 2048}))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    // Query with Path > 1000
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("path-source")],
            payload: Some(PayloadFilter::Path {
                path: "size".to_string(),
                op: PathOp::Gt(json!(1000)),
            }),
            ..Default::default()
        })
        .await?;

    let events = match result {
        EventQueryResult::Events { events, .. } => events,
        _ => panic!("Expected Events result"),
    };

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.payload["size"], json!(2048));

    Ok(())
}

/// Test: numeric payload-path comparisons ignore non-numeric JSON instead of erroring.
#[sinex_test]
async fn test_payload_filter_path_gt_ignores_non_numeric_values(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("path-gt-mixed-types")).await?;

    let _string_value = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("path-source-mixed", "test.type", json!({"size": "huge"}))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    let _numeric_value = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("path-source-mixed", "test.type", json!({"size": 2048}))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("path-source-mixed")],
            payload: Some(PayloadFilter::Path {
                path: "size".to_string(),
                op: PathOp::Gt(json!(1000)),
            }),
            ..Default::default()
        })
        .await?;

    let events = match result {
        EventQueryResult::Events { events, .. } => events,
        _ => panic!("Expected Events result"),
    };

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.payload["size"], json!(2048));

    Ok(())
}

// ============================================================================
// PAYLOAD FILTER COMPOSITION TESTS
// ============================================================================

/// Test: PayloadFilter And/Or/Not composition
#[sinex_test]
async fn test_payload_filter_composition(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("composition-test")).await?;

    // Event 1: alpha, size=10
    let _e1 = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "comp-source",
                "test.type",
                json!({"category": "alpha", "size": 10}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;

    // Event 2: beta, size=20
    let _e2 = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "comp-source",
                "test.type",
                json!({"category": "beta", "size": 20}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;

    // Event 3: alpha, size=30
    let _e3 = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "comp-source",
                "test.type",
                json!({"category": "alpha", "size": 30}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;

    // Query: (category=alpha) AND (size > 15)
    let result_and = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("comp-source")],
            payload: Some(PayloadFilter::And {
                filters: vec![
                    PayloadFilter::Contains {
                        value: json!({"category": "alpha"}),
                    },
                    PayloadFilter::Path {
                        path: "size".to_string(),
                        op: PathOp::Gt(json!(15)),
                    },
                ],
            }),
            ..Default::default()
        })
        .await?;

    let and_events = match result_and {
        EventQueryResult::Events { events, .. } => events,
        _ => panic!("Expected Events result"),
    };

    assert_eq!(
        and_events.len(),
        1,
        "Should match only event 3 (alpha AND size>15)"
    );
    assert_eq!(and_events[0].event.payload["size"], json!(30));

    // Query: (category=alpha) OR (size=20)
    let result_or = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("comp-source")],
            payload: Some(PayloadFilter::Or {
                filters: vec![
                    PayloadFilter::Contains {
                        value: json!({"category": "alpha"}),
                    },
                    PayloadFilter::Contains {
                        value: json!({"size": 20}),
                    },
                ],
            }),
            ..Default::default()
        })
        .await?;

    let or_events = match result_or {
        EventQueryResult::Events { events, .. } => events,
        _ => panic!("Expected Events result"),
    };

    assert_eq!(
        or_events.len(),
        3,
        "Should match 3 events (2 alpha + 1 with size=20)"
    );

    // Query: NOT (category=beta)
    let result_not = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("comp-source")],
            payload: Some(PayloadFilter::Not {
                filter: Box::new(PayloadFilter::Contains {
                    value: json!({"category": "beta"}),
                }),
            }),
            ..Default::default()
        })
        .await?;

    let not_events = match result_not {
        EventQueryResult::Events { events, .. } => events,
        _ => panic!("Expected Events result"),
    };

    assert_eq!(not_events.len(), 2, "Should exclude beta event");

    Ok(())
}

// ============================================================================
// AGGREGATION TESTS
// ============================================================================

/// Test: Aggregation mode Count
#[sinex_test]
async fn test_aggregation_count(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("count-agg")).await?;

    // Insert 5 events
    for i in 0..5 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new("agg-source", "agg.type", json!({"index": i}))
                    .from_material(material_id)
                    .build()?,
            )
            .await?;
    }

    // Query with Count aggregation
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("agg-source")],
            aggregation: Some(AggregationMode::Count),
            ..Default::default()
        })
        .await?;

    match result {
        EventQueryResult::Count { count } => {
            assert_eq!(count, 5);
        }
        _ => panic!("Expected Count result"),
    }

    Ok(())
}

/// Test: Aggregation CountBy Source
#[sinex_test]
async fn test_aggregation_count_by_source(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("countby-source")).await?;

    // Insert events from different sources
    for _ in 0..2 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new("source-a", "agg.type", json!({}))
                    .from_material(material_id)
                    .build()?,
            )
            .await?;
    }

    for _ in 0..3 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new("source-b", "agg.type", json!({}))
                    .from_material(material_id)
                    .build()?,
            )
            .await?;
    }

    for _ in 0..1 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new("source-c", "agg.type", json!({}))
                    .from_material(material_id)
                    .build()?,
            )
            .await?;
    }

    // Query with CountBy Source
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            aggregation: Some(AggregationMode::CountBy {
                field: GroupByField::Source,
                limit: 10,
            }),
            ..Default::default()
        })
        .await?;

    match result {
        EventQueryResult::GroupedCounts { groups } => {
            // Should be ordered by count DESC: source-b(3), source-a(2), source-c(1)
            let sb = groups.iter().find(|g| g.key == "source-b");
            let sa = groups.iter().find(|g| g.key == "source-a");
            let sc = groups.iter().find(|g| g.key == "source-c");

            assert!(sb.is_some() && sb.unwrap().count >= 3);
            assert!(sa.is_some() && sa.unwrap().count >= 2);
            assert!(sc.is_some() && sc.unwrap().count >= 1);
        }
        _ => panic!("Expected GroupedCounts result"),
    }

    Ok(())
}

/// Test: Aggregation CountBy PayloadPath
#[sinex_test]
async fn test_aggregation_count_by_payload_path(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("countby-path")).await?;

    // Insert events with different category values
    for _ in 0..2 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new("path-source", "agg.type", json!({"category": "alpha"}))
                    .from_material(material_id)
                    .build()?,
            )
            .await?;
    }

    for _ in 0..3 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new("path-source", "agg.type", json!({"category": "beta"}))
                    .from_material(material_id)
                    .build()?,
            )
            .await?;
    }

    // Query with CountBy PayloadPath
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("path-source")],
            aggregation: Some(AggregationMode::CountBy {
                field: GroupByField::PayloadPath("category".to_string()),
                limit: 10,
            }),
            ..Default::default()
        })
        .await?;

    match result {
        EventQueryResult::GroupedCounts { groups } => {
            let alpha = groups.iter().find(|g| g.key == "alpha");
            let beta = groups.iter().find(|g| g.key == "beta");

            assert!(alpha.is_some() && alpha.unwrap().count >= 2);
            assert!(beta.is_some() && beta.unwrap().count >= 3);
        }
        _ => panic!("Expected GroupedCounts result"),
    }

    Ok(())
}

/// Test: CountBy PayloadPath binds odd JSON keys instead of formatting SQL.
#[sinex_test]
async fn test_aggregation_count_by_payload_path_with_quote_in_key(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("countby-path-quoted")).await?;
    let quoted_key = "category' weird";

    for _ in 0..2 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new(
                    "path-source-quoted",
                    "agg.type",
                    json!({quoted_key: "alpha"}),
                )
                .from_material(material_id)
                .build()?,
            )
            .await?;
    }

    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("path-source-quoted")],
            aggregation: Some(AggregationMode::CountBy {
                field: GroupByField::PayloadPath(quoted_key.to_string()),
                limit: 10,
            }),
            ..Default::default()
        })
        .await?;

    match result {
        EventQueryResult::GroupedCounts { groups } => {
            let alpha = groups.iter().find(|g| g.key == "alpha");
            assert!(alpha.is_some() && alpha.unwrap().count >= 2);
        }
        _ => panic!("Expected GroupedCounts result"),
    }

    Ok(())
}

/// Test: Aggregation TimeSeries
#[sinex_test]
async fn test_aggregation_time_series(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("timeseries")).await?;

    // Insert events — they'll have different ingest timestamps
    for i in 0..5 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new("ts-source", "ts.type", json!({"index": i}))
                    .from_material_at(material_id, i64::from(i))
                    .build()?,
            )
            .await?;
    }

    // Query with TimeSeries aggregation
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("ts-source")],
            aggregation: Some(AggregationMode::TimeSeries {
                interval_minutes: 60,
                order: TimeSeriesOrder::TimeAsc,
            }),
            ..Default::default()
        })
        .await?;

    match result {
        EventQueryResult::TimeSeries { buckets } => {
            assert!(!buckets.is_empty(), "Should return time buckets");
            // Verify buckets are returned (actual count depends on timing spread)
        }
        _ => panic!("Expected TimeSeries result"),
    }

    Ok(())
}

#[sinex_test]
async fn test_aggregation_time_series_buckets_by_original_timestamp(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("timeseries-original-timestamp"))
        .await?;

    for ts in [
        "2024-01-01T10:15:00Z",
        "2024-01-01T10:45:00Z",
        "2024-01-01T11:05:00Z",
    ] {
        let mut event = DynamicPayload::new("ts-orig-source", "ts.type", json!({ "ts": ts }))
            .from_material(material_id)
            .build()?;
        event.ts_orig = Some(Timestamp::parse_rfc3339(ts)?);
        let _ = ctx.pool.events().insert(event).await?;
    }

    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("ts-orig-source")],
            aggregation: Some(AggregationMode::TimeSeries {
                interval_minutes: 60,
                order: TimeSeriesOrder::TimeAsc,
            }),
            ..Default::default()
        })
        .await?;

    match result {
        EventQueryResult::TimeSeries { buckets } => {
            assert_eq!(buckets.len(), 2, "expected one bucket per event hour");
            assert_eq!(
                buckets[0].bucket,
                Timestamp::parse_rfc3339("2024-01-01T10:00:00Z")?
            );
            assert_eq!(buckets[0].count, 2);
            assert_eq!(
                buckets[1].bucket,
                Timestamp::parse_rfc3339("2024-01-01T11:00:00Z")?
            );
            assert_eq!(buckets[1].count, 1);
        }
        _ => panic!("Expected TimeSeries result"),
    }

    Ok(())
}

/// Test: Aggregation SourceStats
#[sinex_test]
async fn test_aggregation_source_stats(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("sourcestats")).await?;

    // Insert events from 2 sources
    for i in 0..3 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new(
                    "stats-a",
                    if i % 2 == 0 { "type-1" } else { "type-2" },
                    json!({}),
                )
                .from_material(material_id)
                .build()?,
            )
            .await?;
    }

    for i in 0..2 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new(
                    "stats-b",
                    if i == 0 { "type-1" } else { "type-3" },
                    json!({}),
                )
                .from_material(material_id)
                .build()?,
            )
            .await?;
    }

    // Query with SourceStats aggregation
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            aggregation: Some(AggregationMode::SourceStats { limit: 10 }),
            ..Default::default()
        })
        .await?;

    match result {
        EventQueryResult::SourceStats { sources } => {
            let stats_a = sources.iter().find(|s| s.source.as_str() == "stats-a");
            let stats_b = sources.iter().find(|s| s.source.as_str() == "stats-b");

            assert!(stats_a.is_some());
            assert!(stats_a.unwrap().event_count >= 3);
            assert!(stats_a.unwrap().event_type_count >= 2);

            assert!(stats_b.is_some());
            assert!(stats_b.unwrap().event_count >= 2);
        }
        _ => panic!("Expected SourceStats result"),
    }

    Ok(())
}

// ============================================================================
// EDGE CASES
// ============================================================================

/// Test: Empty results when no events match
#[sinex_test]
async fn test_empty_results(ctx: TestContext) -> TestResult<()> {
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("nonexistent-source")],
            limit: 10,
            ..Default::default()
        })
        .await?;

    match result {
        EventQueryResult::Events {
            events,
            next_cursor,
            ..
        } => {
            assert!(events.is_empty());
            assert!(next_cursor.is_none());
        }
        _ => panic!("Expected Events result"),
    }

    Ok(())
}

/// Test: Total estimate when requested
#[sinex_test]
async fn test_total_estimate(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("estimate-test")).await?;

    // Insert 10 events
    for i in 0..10 {
        let _event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new("est-source", "est.type", json!({"i": i}))
                    .from_material(material_id)
                    .build()?,
            )
            .await?;
    }

    // Query with include_total_estimate
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("est-source")],
            include_total_estimate: true,
            limit: 5,
            ..Default::default()
        })
        .await?;

    match result {
        EventQueryResult::Events { total_estimate, .. } => {
            assert!(
                total_estimate.is_some(),
                "Total estimate should be Some when requested"
            );
            assert!(total_estimate.unwrap() > 0);
        }
        _ => panic!("Expected Events result"),
    }

    Ok(())
}

/// Test: Default query returns recent events in descending order
#[sinex_test]
async fn test_default_query_descending(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("default-test")).await?;

    // Insert 3 events
    let mut inserted_ids = Vec::new();
    for i in 0..3 {
        let event = ctx
            .pool
            .events()
            .insert(
                DynamicPayload::new("default-source", "default.type", json!({"index": i}))
                    .from_material(material_id)
                    .build()?,
            )
            .await?;

        if let Some(id) = event.id {
            inserted_ids.push(id);
        }
    }

    // Query with default EventQuery
    let result = ctx
        .pool
        .events()
        .query(EventQuery {
            sources: vec![EventSource::from_static("default-source")],
            ..Default::default()
        })
        .await?;

    let events = match result {
        EventQueryResult::Events { events, .. } => events,
        _ => panic!("Expected Events result"),
    };

    assert!(!events.is_empty());

    // Verify events come in descending order (most recent first)
    for i in 0..events.len() - 1 {
        let id1 = events[i].event.id.unwrap();
        let id2 = events[i + 1].event.id.unwrap();
        assert!(
            id1.to_string() > id2.to_string(),
            "Default query should return events in descending order"
        );
    }

    Ok(())
}

// ============================================================================
// LINEAGE TESTS
// ============================================================================

/// Test: Ancestor lineage traversal
#[sinex_test]
async fn test_lineage_ancestors(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("lineage-ancestors"))
        .await?;

    // Create chain: A → B → C (B has parent A, C has parent B)
    let event_a = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("test-source", "test.type", json!({"label": "A"}))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    let event_b = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("test-source", "test.type", json!({"label": "B"}))
                .from_parents(vec![event_a.id.unwrap()])?
                .build()?,
        )
        .await?;

    let event_c = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("test-source", "test.type", json!({"label": "C"}))
                .from_parents(vec![event_b.id.unwrap()])?
                .build()?,
        )
        .await?;

    // Query lineage for C (ancestors)
    let result = ctx
        .pool
        .events()
        .lineage(LineageQuery {
            event_id: event_c.id.unwrap(),
            direction: LineageDirection::Ancestors,
            max_depth: u32::MAX,
        })
        .await?;

    assert_eq!(
        result.root.payload.get("label").and_then(|v| v.as_str()),
        Some("C")
    );
    assert_eq!(
        result.ancestors.len(),
        2,
        "Should have 2 ancestors (B and A)"
    );

    // Verify depth ordering
    let by_depth: Vec<_> = result.ancestors.iter().map(|n| n.depth).collect();
    assert!(
        by_depth[0] < by_depth[1],
        "Closer ancestor should have lower depth"
    );

    Ok(())
}

/// Test: Descendant lineage traversal
#[sinex_test]
async fn test_lineage_descendants(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("lineage-descendants"))
        .await?;

    // Create: A spawns B and C (both have parent A)
    let event_a = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("test-source", "test.type", json!({"label": "A"}))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    let _event_b = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("test-source", "test.type", json!({"label": "B"}))
                .from_parents(vec![event_a.id.unwrap()])?
                .build()?,
        )
        .await?;

    let _event_c = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("test-source", "test.type", json!({"label": "C"}))
                .from_parents(vec![event_a.id.unwrap()])?
                .build()?,
        )
        .await?;

    // Query lineage for A (descendants)
    let result = ctx
        .pool
        .events()
        .lineage(LineageQuery {
            event_id: event_a.id.unwrap(),
            direction: LineageDirection::Descendants,
            max_depth: u32::MAX,
        })
        .await?;

    assert_eq!(
        result.root.payload.get("label").and_then(|v| v.as_str()),
        Some("A")
    );
    assert_eq!(
        result.descendants.len(),
        2,
        "Should have 2 direct descendants (B and C)"
    );

    Ok(())
}

/// Test: Max depth limits lineage traversal
#[sinex_test]
async fn test_lineage_max_depth(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some("lineage-depth")).await?;

    // Create chain: A→B→C→D (4 deep)
    let event_a = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("test-source", "test.type", json!({"label": "A"}))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    let event_b = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("test-source", "test.type", json!({"label": "B"}))
                .from_parents(vec![event_a.id.unwrap()])?
                .build()?,
        )
        .await?;

    let event_c = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("test-source", "test.type", json!({"label": "C"}))
                .from_parents(vec![event_b.id.unwrap()])?
                .build()?,
        )
        .await?;

    let event_d = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("test-source", "test.type", json!({"label": "D"}))
                .from_parents(vec![event_c.id.unwrap()])?
                .build()?,
        )
        .await?;

    // Query with max_depth=2 (should only get B and C)
    let result = ctx
        .pool
        .events()
        .lineage(LineageQuery {
            event_id: event_d.id.unwrap(),
            direction: LineageDirection::Ancestors,
            max_depth: 2,
        })
        .await?;

    assert_eq!(
        result.ancestors.len(),
        2,
        "Should only return ancestors within depth 2"
    );

    // All returned ancestors should have depth <= 2
    for node in &result.ancestors {
        assert!(
            node.depth <= 2,
            "Ancestor depth should not exceed max_depth"
        );
    }

    Ok(())
}
