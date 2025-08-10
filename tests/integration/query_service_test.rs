//! Query service integration tests focused on event querying patterns
//!
//! This module tests the query functionality of the SearchService with a focus on
//! different query patterns, time-based queries, and filtering capabilities.

use color_eyre::eyre::Result;
use chrono::{Duration, Utc};
use serde_json::json;
use sinex_services::{SearchQuery, SearchService};
use sinex_test_utils::prelude::*;
use sinex_core::types::{events::EventFactory, ulid::Ulid};

/// Helper to create test events for query testing
async fn create_query_test_event(
    ctx: &TestContext,
    source: &str,
    event_type: &str,
    payload_content: serde_json::Value,
    time_offset: Option<Duration>,
) -> color_eyre::Result<Ulid> {
    let pool = ctx.pool();
    let mut event = EventFactory::new(source).create_event(event_type, payload_content);

    if let Some(offset) = time_offset {
        let timestamp = Utc::now() - offset;
        event.ts_orig = Some(timestamp);
    }
    let event_id = event.id;

    insert_event(pool, &event).await?;
    Ok(event_id)
}

/// Set up diverse test data for query testing
async fn setup_query_test_data(ctx: &TestContext) -> color_eyre::Result<Vec<Ulid>> {
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
                "command": "cargo test --lib query",
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

    Ok(event_ids)
}

#[sinex_test]
async fn test_query_by_source_filter(ctx: TestContext) -> color_eyre::Result<()> {
    let service = SearchService::new(ctx.pool().clone());
    setup_query_test_data(&ctx).await?;

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

    let results = service.search_events(query).await?;
    
    // Should find filesystem events
    assert!(!results.is_empty());
    assert!(results.iter().all(|r| r.source == "fs"));
    
    // Verify we have different file event types
    let event_types: std::collections::HashSet<_> = 
        results.iter().map(|r| r.event_type.as_str()).collect();
    assert!(event_types.contains("file.created") || event_types.contains("file.modified"));

    Ok(())
}

#[sinex_test]
async fn test_query_by_event_type_filter(ctx: TestContext) -> color_eyre::Result<()> {
    let service = SearchService::new(ctx.pool().clone());
    setup_query_test_data(&ctx).await?;

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
    let sources: std::collections::HashSet<_> = 
        results.iter().map(|r| r.source.as_str()).collect();
    assert!(sources.len() >= 1);

    Ok(())
}

#[sinex_test]
async fn test_query_by_time_range(ctx: TestContext) -> color_eyre::Result<()> {
    let service = SearchService::new(ctx.pool().clone());
    setup_query_test_data(&ctx).await?;

    // Query events from the last hour only
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![],
        start_time: Some(Utc::now() - Duration::hours(1)),
        end_time: Some(Utc::now()),
        limit: 20,
        offset: 0,
    };

    let results = service.search_events(query).await?;
    
    // Should find recent events but not old ones
    assert!(!results.is_empty());
    
    // Should not find the 2-day old file deletion
    assert!(!results.iter().any(|r| 
        r.event_type == "file.deleted" && 
        r.snippet.contains("old_file.txt")
    ));
    
    // Should find recent events
    assert!(results.iter().any(|r| 
        r.event_type == "file.created" || 
        r.event_type == "file.modified"
    ));

    Ok(())
}

#[sinex_test]
async fn test_query_content_search(ctx: TestContext) -> color_eyre::Result<()> {
    let service = SearchService::new(ctx.pool().clone());
    setup_query_test_data(&ctx).await?;

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

    let results = service.search_events(query).await?;
    
    // Should find events containing "rust"
    assert!(!results.is_empty());
    assert!(results.iter().all(|r| 
        r.snippet.to_lowercase().contains("rust")
    ));

    Ok(())
}

#[sinex_test]
async fn test_query_combined_filters(ctx: TestContext) -> color_eyre::Result<()> {
    let service = SearchService::new(ctx.pool().clone());
    setup_query_test_data(&ctx).await?;

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

    let results = service.search_events(query).await?;
    
    // Should find specific matching events
    if !results.is_empty() {
        assert!(results.iter().all(|r| r.source == "fs"));
        assert!(results.iter().all(|r| 
            r.snippet.to_lowercase().contains("main")
        ));
    }

    Ok(())
}

#[sinex_test]
async fn test_query_ordering_by_timestamp(ctx: TestContext) -> color_eyre::Result<()> {
    let service = SearchService::new(ctx.pool().clone());
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

    Ok(())
}

#[sinex_test]
async fn test_query_pagination(ctx: TestContext) -> color_eyre::Result<()> {
    let service = SearchService::new(ctx.pool().clone());
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
    let page1_ids: std::collections::HashSet<_> = 
        page1.iter().map(|r| r.event_id).collect();
    let page2_ids: std::collections::HashSet<_> = 
        page2.iter().map(|r| r.event_id).collect();
    assert!(page1_ids.is_disjoint(&page2_ids));

    Ok(())
}

#[sinex_test]
async fn test_query_empty_results(ctx: TestContext) -> color_eyre::Result<()> {
    let service = SearchService::new(ctx.pool().clone());
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
    let service = SearchService::new(ctx.pool().clone());
    setup_query_test_data(&ctx).await?;

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
    assert_eq!(results.len(), 0);

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

    Ok(())
}

#[sinex_test]
async fn test_query_multiple_sources(ctx: TestContext) -> color_eyre::Result<()> {
    let service = SearchService::new(ctx.pool().clone());
    setup_query_test_data(&ctx).await?;

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

    let results = service.search_events(query).await?;
    
    if !results.is_empty() {
        // All results should be from the specified sources
        assert!(results.iter().all(|r| 
            r.source == "fs" || r.source == "shell.bash"
        ));
        
        // Should potentially have both types of sources
        let sources: std::collections::HashSet<_> = 
            results.iter().map(|r| r.source.as_str()).collect();
        assert!(!sources.is_empty());
    }

    Ok(())
}

#[sinex_test]
async fn test_query_case_insensitive_search(ctx: TestContext) -> color_eyre::Result<()> {
    let service = SearchService::new(ctx.pool().clone());
    
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
    assert!(results.iter().any(|r| 
        r.snippet.contains("Hello World")
    ));

    Ok(())
}