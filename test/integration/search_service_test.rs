// Comprehensive tests for SearchService
//
// Tests search functionality including:
// - Full-text search across event payloads
// - Filtering by source, event type, and time range
// - SQL injection prevention
// - Performance with large datasets
// - Complex query combinations

use crate::common::prelude::*;
use sinex_services::{SearchQuery, SearchService};
use sinex_events::event_types::{shell, filesystem, window_manager, clipboard};

/// Helper to create test events with specific content
async fn create_searchable_event(
    pool: &DbPool,
    source: &str,
    event_type: &str,
    payload_content: Value,
    time_offset: Option<ChronoDuration>,
) -> TestResult {
    let factory = EventFactory::new(source);
    let mut event = factory.create_event(event_type, payload_content);
    
    if let Some(offset) = time_offset {
        event.ts_orig = Some(Utc::now() - offset);
    }
    
    insert_event(pool, &event).await?;
    Ok(())
}

/// Create a set of diverse test events for search testing
async fn setup_search_test_data(pool: &DbPool) -> TestResult {
    // Recent events (within last hour)
    create_searchable_event(
        pool,
        sources::FS,
        filesystem::FILE_CREATED,
        json!({
            "path": "/home/user/documents/important.txt",
            "size": 1024,
            "content": "This is an important document with confidential information"
        }),
        Some(ChronoDuration::minutes(30)),
    )
    .await?;

    // Yesterday's events
    create_searchable_event(
        pool,
        sources::SHELL_KITTY,
        shell::COMMAND_EXECUTED,
        json!({
            "command": "git commit -m 'Fix important bug in authentication'",
            "exit_code": 0,
            "directory": "/home/user/project"
        }),
        Some(ChronoDuration::days(1)),
    )
    .await?;

    // Clipboard event
    create_searchable_event(
        pool,
        sources::CLIPBOARD,
        clipboard::COPIED,
        json!({
            "content": "meeting notes: discuss project timeline",
            "application": "firefox"
        }),
        Some(ChronoDuration::hours(2)),
    )
    .await?;

    // Window manager event
    create_searchable_event(
        pool,
        sources::WM_HYPRLAND,
        window_manager::WINDOW_FOCUSED,
        json!({
            "title": "Project Alpha - VSCode",
            "class": "code",
            "workspace": 1
        }),
        Some(ChronoDuration::hours(3)),
    )
    .await?;

    // Event with technical content
    create_searchable_event(
        pool,
        sources::FS,
        filesystem::FILE_MODIFIED,
        json!({
            "path": "/home/user/code/database.sql",
            "content": "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);"
        }),
        None,
    )
    .await?;

    // Multiple events with similar keywords
    for i in 0..3 {
        create_searchable_event(
            pool,
            sources::SHELL_KITTY,
            shell::COMMAND_EXECUTED,
            json!({
                "command": format!("grep -r 'important' file{}.txt", i),
                "exit_code": 0
            }),
            Some(ChronoDuration::minutes(10 * i as i64)),
        )
        .await?;
    }

    Ok(())
}

#[sinex_test]
async fn test_basic_text_search(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_search_test_data(&pool).await?;

    // Search for "important"
    let query = SearchQuery {
        text: Some("important".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
    };

    let results = service.search(query).await?;
    
    // Should find multiple events containing "important"
    assert!(results.len() >= 4, "Should find at least 4 events with 'important'");
    
    // Verify all results contain the search term
    for event in &results {
        let payload_str = event.payload.to_string().to_lowercase();
        assert!(
            payload_str.contains("important"),
            "Result should contain search term"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_search_with_source_filter(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_search_test_data(&pool).await?;

    // Search only in filesystem events
    let query = SearchQuery {
        text: Some("important".to_string()),
        sources: vec![sources::FS.to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
    };

    let results = service.search(query).await?;
    
    // Should only find filesystem events
    assert!(!results.is_empty(), "Should find filesystem events");
    for event in &results {
        assert_eq!(event.source, sources::FS, "Should only return FS events");
    }

    Ok(())
}

#[sinex_test]
async fn test_search_with_event_type_filter(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_search_test_data(&pool).await?;

    // Search only command executed events
    let query = SearchQuery {
        text: None,
        sources: vec![],
        event_types: vec![event_types::COMMAND_EXECUTED.to_string()],
        start_time: None,
        end_time: None,
        limit: 20,
    };

    let results = service.search(query).await?;
    
    // Should only find command events
    assert!(!results.is_empty(), "Should find command events");
    for event in &results {
        assert_eq!(
            event.event_type, 
            shell::COMMAND_EXECUTED,
            "Should only return command events"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_search_with_time_range(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_search_test_data(&pool).await?;

    // Search only recent events (last 2 hours)
    let query = SearchQuery {
        text: Some("important".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: Some(Utc::now() - ChronoDuration::hours(2)),
        end_time: Some(Utc::now()),
        limit: 10,
    };

    let results = service.search(query).await?;
    
    // Should exclude older events
    assert!(!results.is_empty(), "Should find recent events");
    for event in &results {
        let event_time = event.ts_orig.unwrap_or(event.ts_ingest);
        assert!(
            event_time > Utc::now() - ChronoDuration::hours(2),
            "Should only return recent events"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_search_sql_injection_prevention(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_search_test_data(&pool).await?;

    // Test various SQL injection patterns
    let injection_patterns = vec![
        "'; DROP TABLE core.events; --",
        "' OR '1'='1",
        "\" OR \"1\"=\"1",
        "1; DELETE FROM core.events WHERE 1=1; --",
        "1' UNION SELECT * FROM pg_user --",
        "%'; DROP TABLE core.events; --",
        "_'; UPDATE core.events SET payload = '{}' WHERE '1'='1",
        "'; COPY core.events TO '/tmp/stolen.csv'; --",
        "'; CREATE TABLE hacked AS SELECT * FROM core.events; --",
        "' OR pg_sleep(10) --",
    ];

    for pattern in injection_patterns {
        let query = SearchQuery {
            text: Some(pattern.to_string()),
            sources: vec![],
            event_types: vec![],
            start_time: None,
            end_time: None,
            limit: 10,
        };

        // Should not panic or execute injection
        let result = service.search(query).await;
        
        // The query should either:
        // 1. Return no results (pattern doesn't match any content)
        // 2. Return results if the pattern happens to match content
        // But it should NEVER execute the SQL injection
        assert!(result.is_ok(), "Search should handle injection attempt safely");
        
        // Verify database is still intact
        let count_result = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM core.events"
        )
        .fetch_one(pool)
        .await;
        
        assert!(count_result.is_ok(), "Database should still be intact");
    }

    Ok(())
}

#[sinex_test]
async fn test_case_insensitive_search(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    // Create events with mixed case
    create_searchable_event(
        &pool,
        sources::SHELL_KITTY,
        shell::COMMAND_EXECUTED,
        json!({
            "command": "echo 'IMPORTANT MESSAGE'",
            "exit_code": 0
        }),
        None,
    )
    .await?;

    create_searchable_event(
        &pool,
        sources::SHELL_KITTY,
        shell::COMMAND_EXECUTED,
        json!({
            "command": "echo 'Important Notice'",
            "exit_code": 0
        }),
        None,
    )
    .await?;

    // Search with lowercase
    let query = SearchQuery {
        text: Some("important".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
    };

    let results = service.search(query).await?;
    assert!(results.len() >= 2, "Should find both mixed-case matches");

    Ok(())
}

#[sinex_test]
async fn test_partial_word_matching(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    // Create events with partial matches
    create_searchable_event(
        &pool,
        sources::FS,
        filesystem::FILE_CREATED,
        json!({
            "path": "/docs/authentication_module.rs",
            "size": 2048
        }),
        None,
    )
    .await?;

    // Search for partial word
    let query = SearchQuery {
        text: Some("auth".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
    };

    let results = service.search(query).await?;
    assert!(!results.is_empty(), "Should find partial matches");

    Ok(())
}

#[sinex_test]
async fn test_complex_combined_filters(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_search_test_data(&pool).await?;

    // Complex query with all filters
    let query = SearchQuery {
        text: Some("project".to_string()),
        sources: vec![sources::SHELL_KITTY.to_string(), sources::WM_HYPRLAND.to_string()],
        event_types: vec![],
        start_time: Some(Utc::now() - ChronoDuration::days(2)),
        end_time: Some(Utc::now()),
        limit: 5,
    };

    let results = service.search(query).await?;
    
    // Verify all filters are applied
    for event in &results {
        assert!(
            event.source == sources::SHELL_KITTY || event.source == sources::WM_HYPRLAND,
            "Should only return specified sources"
        );
        
        let payload_str = event.payload.to_string().to_lowercase();
        assert!(
            payload_str.contains("project"),
            "Should contain search term"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_search_empty_text(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    setup_search_test_data(&pool).await?;

    // Search with empty text (should return all events matching other filters)
    let query = SearchQuery {
        text: None,
        sources: vec![sources::FS.to_string()],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
    };

    let results = service.search(query).await?;
    assert!(!results.is_empty(), "Should return FS events without text filter");
    
    for event in &results {
        assert_eq!(event.source, sources::FS);
    }

    Ok(())
}

#[sinex_test]
async fn test_search_result_ordering(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    // Create events with specific timestamps
    for i in 0..5 {
        create_searchable_event(
            &pool,
            sources::SHELL_KITTY,
            shell::COMMAND_EXECUTED,
            json!({
                "command": format!("test command {}", i),
                "order": i
            }),
            Some(ChronoDuration::minutes(i as i64 * 10)),
        )
        .await?;
    }

    let query = SearchQuery {
        text: Some("test".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
    };

    let results = service.search(query).await?;
    
    // Results should be ordered by timestamp (most recent first)
    for window in results.windows(2) {
        let time1 = window[0].ts_orig.unwrap_or(window[0].ts_ingest);
        let time2 = window[1].ts_orig.unwrap_or(window[1].ts_ingest);
        assert!(
            time1 >= time2,
            "Results should be ordered by timestamp descending"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_search_with_limit(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    // Create many events
    for i in 0..20 {
        create_searchable_event(
            &pool,
            sources::SHELL_KITTY,
            shell::COMMAND_EXECUTED,
            json!({
                "command": format!("echo 'test message {}'", i),
                "index": i
            }),
            None,
        )
        .await?;
    }

    // Search with limit
    let query = SearchQuery {
        text: Some("test".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 5,
    };

    let results = service.search(query).await?;
    assert_eq!(results.len(), 5, "Should respect limit");

    Ok(())
}

#[sinex_test]
async fn test_search_special_characters(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    // Create events with special characters
    let special_contents = vec![
        "user@example.com",
        "price: $99.99",
        "math: 2+2=4",
        "path: /home/user/file.txt",
        "regex: ^[a-z]+$",
        "quote: \"hello world\"",
    ];

    for content in &special_contents {
        create_searchable_event(
            &pool,
            sources::CLIPBOARD,
            clipboard::COPIED,
            json!({
                "content": content,
                "source": "test"
            }),
            None,
        )
        .await?;
    }

    // Search for content with special characters
    let query = SearchQuery {
        text: Some("user@example.com".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 10,
    };

    let results = service.search(query).await?;
    assert!(!results.is_empty(), "Should find event with email address");

    Ok(())
}

#[sinex_test]
async fn test_search_performance_with_large_dataset(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let service = SearchService::new(pool.clone());

    // Create a moderately large dataset
    let event_count = 100;
    let batch_builder = BatchEventBuilder::new();
    
    for i in 0..event_count {
        batch_builder.add_event()
            .source(sources::SHELL_KITTY)
            .event_type(event_types::COMMAND_EXECUTED)
            .payload(json!({
                "command": if i % 10 == 0 { 
                    "important command" 
                } else { 
                    format!("regular command {}", i) 
                },
                "index": i
            }));
    }
    
    batch_builder.insert_all(&pool).await?;

    // Time the search
    let start = Instant::now();
    let query = SearchQuery {
        text: Some("important".to_string()),
        sources: vec![],
        event_types: vec![],
        start_time: None,
        end_time: None,
        limit: 20,
    };

    let results = service.search(query).await?;
    let duration = start.elapsed();

    assert_eq!(results.len(), 10, "Should find 10% of events");
    println!("Search completed in {:?}", duration);
    assert!(
        duration < Duration::from_millis(500),
        "Search should complete quickly even with large dataset"
    );

    Ok(())
}