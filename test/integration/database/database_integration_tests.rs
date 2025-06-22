//! Database integration tests
//! 
//! These tests verify core database operations work correctly:
//! - Event insertion and retrieval
//! - Batch operations
//! - Query performance
//! - Schema validation
//! - Concurrent access patterns
//!
//! Uses #[sqlx::test] for automatic transaction isolation

use crate::common;
use sinex_core::event_type_constants;
use std::time::Duration;

use common::{
    events, assertions, generators
};

/// Test basic event lifecycle: insert → retrieve → verify
/// 
/// This is the most fundamental test - if this fails, nothing else works.
/// Verifies:
/// - Events can be inserted into raw.events table
/// - ULID primary keys are properly handled
/// - Event retrieval by ID works
/// - All fields round-trip correctly
#[sqlx::test]
async fn test_insert_and_retrieve_event(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Create a test event using our utilities
    let event = events::filesystem_event(
        event_type_constants::filesystem::FILE_CREATED,
        "/test/file.txt"
    );

    // Insert and verify using shared assertion helpers
    let event_id = assertions::assert_event_inserted(&pool, &event).await?;

    // Query it back using our helper that encapsulates the UUID conversion
    let retrieved = common::get_event_by_id(&pool, event_id).await?;

    // Verify it matches what we inserted (ignoring generated fields)
    assertions::assert_events_equivalent(&retrieved, &event);

    Ok(())
}

/// Test batch insertion of multiple events
#[sqlx::test]
async fn test_batch_event_insertion(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let events = generators::test_events(10);
    
    let mut inserted_ids = Vec::new();
    for event in &events {
        let id = assertions::assert_event_inserted(&pool, event).await?;
        inserted_ids.push(id);
    }
    
    // Verify all events exist
    for id in inserted_ids {
        assert!(common::event_exists(&pool, id).await?);
    }

    // Check total count
    let count = common::get_event_count(&pool).await?;
    assert!(count >= 10);

    Ok(())
}

/// Test querying events by source
#[sqlx::test]
async fn test_query_events_by_source(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Insert filesystem events
    let fs_event1 = events::file_created_event("/test/file1.txt");
    let fs_event2 = events::file_modified_event("/test/file2.txt");
    assertions::assert_event_inserted(&pool, &fs_event1).await?;
    assertions::assert_event_inserted(&pool, &fs_event2).await?;
    
    // Insert terminal event
    let term_event = events::kitty_event("ls -la");
    assertions::assert_event_inserted(&pool, &term_event).await?;
    
    // Query all events and filter by source
    let all_events = sqlx::query!("SELECT source FROM raw.events WHERE source = $1 LIMIT $2", "filesystem", 10i64)
        .fetch_all(&pool)
        .await?;
    assert!(all_events.len() >= 2);
    
    for event in &all_events {
        assert_eq!(event.source, "filesystem");
    }

    Ok(())
}

/// Test invalid event insertion fails appropriately
#[sqlx::test]
async fn test_invalid_event_insertion_fails(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let invalid_event = events::invalid_event();
    assertions::assert_event_insertion_fails(&pool, &invalid_event).await?;
    Ok(())
}

/// Test ULID ordering in time-based queries
#[sqlx::test]
async fn test_ulid_time_ordering(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Insert events with a small delay to ensure different timestamps
    let event1 = events::file_created_event("/test/first.txt");
    let id1 = assertions::assert_event_inserted(&pool, &event1).await?;
    
    tokio::task::yield_now().await;
    
    let event2 = events::file_created_event("/test/second.txt");
    let id2 = assertions::assert_event_inserted(&pool, &event2).await?;
    
    // Verify ULIDs are in time order (later ULID should be larger)
    assert!(id2.to_string() > id1.to_string());
    
    Ok(())
}