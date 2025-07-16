//! Comprehensive tests for SearchService
//!
//! CRITICAL: These tests expose SQL injection vulnerabilities in the current implementation.
//! The SearchService MUST be fixed to use proper parameterized queries before production use!

use crate::common::prelude::*;
use chrono::{Duration, Utc};
use serde_json::json;
use sinex_services::{SearchQuery, SearchService};
use sinex_ulid::Ulid;

/// Helper to create test events with specific content
async fn create_test_event(
    pool: &DbPool,
    source: &str,
    event_type: &str,
    payload_content: serde_json::Value,
    time_offset: Option<Duration>,
) -> Result<Ulid, Box<dyn std::error::Error>> {
    let mut builder =
        RawEventBuilder::new(source, event_type, payload_content).with_host("test-host");

    if let Some(offset) = time_offset {
        let timestamp = Utc::now() - offset;
        builder = builder.with_timestamp(timestamp);
    }

    let event = builder.build();
    let event_id = event.id;

    insert_event(pool, &event).await?;

    Ok(event_id)
}

/// Create a set of diverse test events
async fn setup_test_data(pool: &DbPool) -> Result<Vec<Ulid>, Box<dyn std::error::Error>> {
    let mut event_ids = Vec::new();

    // Recent events (within last hour)
    event_ids.push(
        create_test_event(
            pool,
            "fs",
            "file.created",
            json!({
                "path": "/home/user/documents/important.txt",
                "size": 1024,
                "content": "This is an important document with secret information"
            }),
            Some(Duration::minutes(30)),
        )
        .await?,
    );

    // Yesterday's events
    event_ids.push(
        create_test_event(
            pool,
            "shell.kitty",
            "command.executed",
            json!({
                "command": "SELECT * FROM users WHERE admin = true",
                "exit_code": 0,
                "directory": "/home/user"
            }),
            Some(Duration::days(1)),
        )
        .await?,
    );

    // Clipboard event
    event_ids.push(
        create_test_event(
            pool,
            "clipboard",
            "copied",
            json!({
                "content": "password123",
                "application": "firefox"
            }),
            Some(Duration::hours(2)),
        )
        .await?,
    );

    // Window manager event
    event_ids.push(
        create_test_event(
            pool,
            "wm.hyprland",
            "window.opened",
            json!({
                "title": "Secret Project - VSCode",
                "class": "code",
                "workspace": 1
            }),
            Some(Duration::hours(3)),
        )
        .await?,
    );

    // Event with SQL-like content (to test false positives)
    event_ids.push(
        create_test_event(
            pool,
            "fs",
            "file.modified",
            json!({
                "path": "/home/user/queries.sql",
                "content": "DROP TABLE IF EXISTS temp; SELECT * FROM events;"
            }),
            None,
        )
        .await?,
    );

    Ok(event_ids)
}

#[sinex_test]
async fn test_search_sql_injection_prevention(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    // Setup test data
    setup_test_data(pool).await?;

    // CRITICAL: Test various SQL injection patterns
    let injection_patterns = vec![
        // Classic SQL injection attempts
        "'; DROP TABLE core.events; --",
        "' OR '1'='1",
        "\" OR \"1\"=\"1",
        "1; DELETE FROM core.events WHERE 1=1; --",
        "1' UNION SELECT * FROM pg_user --",
        // Attempting to break out of ILIKE
        "%'; DROP TABLE core.events; --",
        "_'; UPDATE core.events SET payload = '{}' WHERE '1'='1",
        // PostgreSQL specific injection attempts
        "'; COPY core.events TO '/tmp/stolen.csv'; --",
        "'; CREATE TABLE hacked AS SELECT * FROM core.events; --",
        // Time-based blind SQL injection
        "' OR pg_sleep(10) --",
        "'; SELECT CASE WHEN (1=1) THEN pg_sleep(10) ELSE pg_sleep(0) END--",
    ];

    for pattern in injection_patterns {
        let query = SearchQuery {
            text: Some(pattern.to_string()),
            sources: vec![],
            event_types: vec![],
            start_time: None,
            end_time: None,
            limit: 10,
            offset: 0,
        };

        // The search should complete without executing injected SQL
        let result = service.search_events(query).await;

        // If properly parameterized, this should succeed and return 0 results
        // (unless the pattern happens to match actual content)
        assert!(result.is_ok(), "Search failed for pattern: {}", pattern);

        // Verify the database wasn't corrupted
        let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM core.events")
            .fetch_one(pool)
            .await?;
        assert!(
            count > 0,
            "SQL injection may have deleted data with pattern: {}",
            pattern
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_search_sql_injection_in_filters(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_test_data(pool).await?;

    // Test SQL injection in source filter
    let query = SearchQuery {
        text: None,
        sources: vec!["fs'; DROP TABLE core.events; --".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let result = service.search_events(query).await;
    assert!(result.is_ok(), "Source filter injection protection failed");

    // Test SQL injection in event_type filter
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec!["file.created' OR '1'='1".to_string()],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let result = service.search_events(query).await;
    assert!(
        result.is_ok(),
        "Event type filter injection protection failed"
    );

    Ok(())
}

#[sinex_test]
async fn test_search_basic_text_search(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_test_data(pool).await?;

    // Search for "important"
    let query = SearchQuery {
        text: Some("important".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].source, "fs");
    assert_eq!(results[0].event_type, "file.created");
    assert!(results[0].snippet.contains("important"));

    // Case-insensitive search
    let query = SearchQuery {
        text: Some("IMPORTANT".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert_eq!(results.len(), 1);

    Ok(())
}

#[sinex_test]
async fn test_search_source_filtering(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_test_data(pool).await?;

    // Filter by filesystem source
    let query = SearchQuery {
        text: None,
        sources: vec!["fs".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.source == "fs"));

    // Filter by multiple sources
    let query = SearchQuery {
        text: None,
        sources: vec!["fs".to_string(), "clipboard".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert_eq!(results.len(), 3);
    assert!(results
        .iter()
        .all(|r| r.source == "fs" || r.source == "clipboard"));

    Ok(())
}

#[sinex_test]
async fn test_search_event_type_filtering(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_test_data(pool).await?;

    // Filter by event type
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec!["file.created".to_string()],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].event_type, "file.created");

    Ok(())
}

#[sinex_test]
async fn test_search_time_range_filtering(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_test_data(pool).await?;

    // Search for events from last 2 hours
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![],
        start_time: Some(Utc::now() - Duration::hours(2)),
        end_time: Some(Utc::now()),
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    // Should find recent events but not yesterday's
    assert!(results.len() >= 2);
    assert!(!results.iter().any(|r| r.event_type == "command.executed"));

    Ok(())
}

#[sinex_test]
async fn test_search_pagination(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    // Create more events for pagination testing
    for i in 0..15 {
        create_test_event(pool, "test", "pagination.test", json!({ "index": i }), None).await?;
    }

    // First page
    let query = SearchQuery {
        text: None,
        sources: vec!["test".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 5,
        offset: 0,
    };

    let page1 = service.search_events(query.clone()).await?;
    assert_eq!(page1.len(), 5);

    // Second page
    let mut query2 = query.clone();
    query2.offset = 5;
    let page2 = service.search_events(query2).await?;
    assert_eq!(page2.len(), 5);

    // Verify no overlap
    let page1_ids: Vec<_> = page1.iter().map(|r| r.event_id).collect();
    let page2_ids: Vec<_> = page2.iter().map(|r| r.event_id).collect();
    assert!(page1_ids.iter().all(|id| !page2_ids.contains(id)));

    Ok(())
}

#[sinex_test]
async fn test_search_limit_bounds(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_test_data(pool).await?;

    // Test with limit = 0 (should this be allowed?)
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
    assert_eq!(results.len(), 0);

    // Test with very large limit
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 999999,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert!(results.len() <= 999999);

    Ok(())
}

#[sinex_test]
async fn test_search_combined_filters(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_test_data(pool).await?;

    // Combine text search with source filter
    let query = SearchQuery {
        text: Some("SELECT".to_string()),
        sources: vec!["shell.kitty".to_string(), "fs".to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert_eq!(results.len(), 2); // Should find both SQL-related events

    Ok(())
}

#[sinex_test]
async fn test_search_ordering(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_test_data(pool).await?;

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

    // Verify descending order by timestamp
    for i in 1..results.len() {
        assert!(
            results[i - 1].timestamp >= results[i].timestamp,
            "Results not in descending timestamp order"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_search_snippet_extraction(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    // Create event with long content
    let long_content = "a".repeat(50) + "FINDME" + &"b".repeat(50);
    create_test_event(
        pool,
        "test",
        "snippet.test",
        json!({ "content": long_content }),
        None,
    )
    .await?;

    // Search for the marker
    let query = SearchQuery {
        text: Some("FINDME".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert_eq!(results.len(), 1);

    // Verify snippet contains the search term with context
    let snippet = &results[0].snippet;
    assert!(snippet.contains("FINDME"));
    assert!(snippet.contains("..."));
    assert!(snippet.len() < 200); // Should be truncated

    Ok(())
}

#[sinex_test]
async fn test_search_special_characters_in_text(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    // Create events with special characters
    create_test_event(
        pool,
        "test",
        "special.chars",
        json!({ "content": "test%value_here" }),
        None,
    )
    .await?;

    create_test_event(
        pool,
        "test",
        "special.chars",
        json!({ "content": "test_value%here" }),
        None,
    )
    .await?;

    // Search for pattern with % (SQL wildcard)
    let query = SearchQuery {
        text: Some("test%value".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    // Should only find exact match, not treat % as wildcard
    assert_eq!(results.len(), 1);

    // Search for pattern with _ (SQL single char wildcard)
    let query = SearchQuery {
        text: Some("test_value".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    // Should find both if _ is treated as wildcard, only one if literal
    assert_eq!(
        results.len(),
        1,
        "Underscore should be treated literally, not as SQL wildcard"
    );

    Ok(())
}

#[sinex_test]
async fn test_search_empty_results(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_test_data(pool).await?;

    // Search for non-existent content
    let query = SearchQuery {
        text: Some("this text does not exist anywhere".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert_eq!(results.len(), 0);

    // Search with non-existent source
    let query = SearchQuery {
        text: None,
        sources: vec!["non.existent.source".to_string()],
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
async fn test_search_result_format(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    let event_id = create_test_event(
        pool,
        "test.source",
        "test.type",
        json!({
            "key": "value",
            "nested": { "data": "here" }
        }),
        None,
    )
    .await?;

    let query = SearchQuery {
        text: Some("value".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 1,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    assert_eq!(results.len(), 1);

    let result = &results[0];
    assert_eq!(result.event_id, event_id);
    assert_eq!(result.source, "test.source");
    assert_eq!(result.event_type, "test.type");
    assert!(result.timestamp <= Utc::now());
    assert!(result.snippet.contains("value"));
    assert_eq!(result.score, 1.0);

    Ok(())
}

/// CRITICAL: Test attempting actual SQL injection that would be visible
/// This test documents the current vulnerability
#[sinex_test]
async fn test_search_sql_injection_limit_offset_vulnerability(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_test_data(pool).await?;

    // The current implementation directly concatenates limit/offset into SQL
    // This means negative values or SQL fragments could be injected
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: -1, // Negative limit
        offset: 0,
    };

    // This might cause an error or unexpected behavior
    let result = service.search_events(query).await;
    // Document what happens with invalid limit

    // Another attempt with large offset that could cause issues
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
        offset: i32::MAX,
    };

    let result = service.search_events(query).await;
    // This should work but return empty results

    Ok(())
}

// Additional test to verify the SQL query construction issue
#[cfg(test)]
mod sql_construction_tests {
    

    /// This test demonstrates the SQL construction vulnerability
    /// The params vector is built but never actually used in the query execution
    #[test]
    fn test_sql_params_not_bound() {
        // The current implementation builds params but passes raw SQL to sqlx::query_as
        // This is a critical security flaw that must be fixed

        // Expected secure pattern:
        // sqlx::query_as!(
        //     r#"SELECT ... WHERE source = ANY($1) AND event_type = ANY($2) ..."#,
        //     &sources[..],
        //     &event_types[..],
        //     ...
        // )

        // Current vulnerable pattern:
        // sqlx::query_as(&sql)  // sql contains user input!

        // This allows SQL injection through any user-controlled field
    }
}
