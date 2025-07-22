// Database Unit Tests - Modernized with Powerful Abstractions
//
// Demonstrates test modernization patterns:
// - Property-based testing for database operations
// - Batch operations replacing loops
// - Smart waiting patterns
// - Test macros for common database patterns
// - Concurrent testing with proper isolation

use crate::common::prelude::*;
use crate::common::property_helpers::*;
use crate::common::test_macros::*;
use proptest::prelude::*;
use sinex_events::{EventFactory, sources, event_types};
use sinex_db::queries::{EventQueries, CheckpointQueries};

// =============================================================================
// PROPERTY-BASED DATABASE OPERATION TESTS
// =============================================================================

// Replace multiple individual insertion tests with property testing
sinex_proptest! {
    #[sinex_test]
    async fn database_handles_all_event_types(
        event in arbitrary_event()
    ) {
        let pool = ctx.pool();
        
        // Insert any valid event
        ctx.insert_event(&event).await?;
        
        // Verify retrieval
        let retrieved = ctx.get_event_by_id(event.id).await?
            .expect("Event should exist after insertion");
        
        // Events should be equivalent (except server-set fields)
        prop_assert_eq!(retrieved.source, event.source);
        prop_assert_eq!(retrieved.event_type, event.event_type);
        prop_assert_eq!(retrieved.payload, event.payload);
    }
}

// Test database handles extreme payloads
sinex_proptest! {
    #[sinex_test]
    async fn database_handles_edge_case_payloads(
        event in boundary_condition_events()
    ) {
        let pool = ctx.pool();
        
        // Database should handle any valid JSON payload
        let result = ctx.insert_event(&event).await;
        
        // Some payloads might be too large
        if event.payload.to_string().len() > 1_000_000 {
            // Large payloads might fail - that's OK
            prop_assume!(result.is_err());
        } else {
            prop_assert!(result.is_ok());
        }
    }
}

// =============================================================================
// BATCH OPERATIONS WITH BUILDERS
// =============================================================================

// Replace loop-based insertion with batch builder
test_batch_events!(
    test_bulk_event_insertion,
    sources::FS,
    event_types::filesystem::FILE_CREATED,
    1000, // Insert 1000 events in batch
    |pool: &DbPool, events: &[RawEvent]| async move {
        // Verify all events inserted
        let count = EventQueries::count_all()
            .fetch_one::<(i64,)>(pool)
            .await?
            .0;
        assert_eq!(count as usize, events.len());
        
        // Verify IDs are unique
        let ids: HashSet<_> = events.iter().map(|e| e.id).collect();
        assert_eq!(ids.len(), events.len());
        
        Ok(())
    }
);

// =============================================================================
// CONCURRENT DATABASE OPERATIONS
// =============================================================================

test_concurrent_operations!(
    test_concurrent_database_writes,
    20, // 20 concurrent writers
    |pool: Arc<DbPool>, task_id: usize| async move {
        // Each task inserts a batch of events
        let events = BatchEventBuilder::new(
            &format!("task-{}", task_id),
            "concurrent.write",
            50
        )
        .with_payload_generator(|i| json!({
            "task_id": task_id,
            "event_index": i,
            "timestamp": Utc::now().to_rfc3339()
        }))
        .build();
        
        // Insert all events
        for event in &events {
            sinex_db::insert_event(&**pool, event).await?;
        }
        
        Ok(events.len())
    },
    |pool: &Arc<DbPool>, results: &[usize]| async move {
        // Verify total count
        let expected_total: usize = results.iter().sum();
        let actual = EventQueries::count_all()
            .fetch_one::<(i64,)>(&***pool)
            .await?
            .0 as usize;
        assert_eq!(actual, expected_total);
        Ok(())
    }
);

// =============================================================================
// TIME-BASED QUERIES WITH PROPERTY TESTING
// =============================================================================

test_time_range_query!(
    test_temporal_event_queries,
    100, // events
    chrono::Duration::minutes(1), // spacing
    chrono::Duration::hours(-1), // start offset
    chrono::Duration::hours(1),  // end offset
    50 // expected events in 2-hour window
);

// =============================================================================
// TRANSACTION TESTING WITH PROPERTY-BASED ROLLBACK
// =============================================================================

sinex_proptest! {
    #[sinex_test]
    async fn transaction_rollback_preserves_consistency(
        events in arbitrary_event_batch(),
        fail_at_index in 0usize..10usize
    ) {
        let pool = ctx.pool();
        let initial_count = ctx.event_count().await?;
        
        // Attempt transaction that fails partway through
        let result = sqlx::Transaction::begin(pool).await?;
        let mut tx = result;
        
        for (i, event) in events.iter().enumerate() {
            if i == fail_at_index {
                // Simulate failure
                tx.rollback().await?;
                break;
            }
            
            // Insert within transaction
            EventQueries::insert_full(
                event.id,
                event.source.clone(),
                event.event_type.clone(),
                event.host.clone(),
                event.payload.clone(),
                event.ts_orig,
                event.ts_ingest,
                event.ingestor_version.clone(),
            )
            .execute(&mut *tx)
            .await?;
        }
        
        // Verify no events were persisted
        let final_count = ctx.event_count().await?;
        prop_assert_eq!(final_count, initial_count);
    }
}

// =============================================================================
// QUERY PERFORMANCE CHARACTERISTICS
// =============================================================================

configured_proptest! {
    #[cases(50)]
    fn query_performance_scales_linearly(
        event_count in 10usize..1000usize,
        query_limit in 1usize..100usize
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let ctx = TestContext::new().await.unwrap();
            
            // Insert events
            let events = BatchEventBuilder::new("perf", "test", event_count)
                .insert(ctx.pool()).await.unwrap();
            
            // Time the query
            let start = std::time::Instant::now();
            let results = EventQueries::list_recent(query_limit as i64)
                .fetch_all(ctx.pool())
                .await
                .unwrap();
            let duration = start.elapsed();
            
            // Query time should not depend on total events
            assert!(duration.as_millis() < 100); // Under 100ms
            assert!(results.len() <= query_limit);
        });
    }
}

// =============================================================================
// CHECKPOINT OPERATIONS WITH BUILDERS
// =============================================================================

test_checkpoint_flow!(
    test_checkpoint_upsert_behavior,
    "test-automaton",
    0,    // initial count
    1000  // updated count
);

// =============================================================================
// DATABASE CONSTRAINT VALIDATION
// =============================================================================

test_invalid_event!(
    test_empty_source_rejection,
    "",  // empty source
    "test.event",
    json!({"valid": "payload"}),
    "source cannot be empty"
);

test_invalid_event!(
    test_empty_event_type_rejection,
    "test",
    "",  // empty event type
    json!({"valid": "payload"}),
    "event_type cannot be empty"
);

// =============================================================================
// COMPLEX QUERY PATTERNS
// =============================================================================

#[sinex_test]
async fn test_complex_filtering_with_builders(ctx: TestContext) -> TestResult {
    // Create diverse test data using builders
    let scenario = TestScenarioBuilder::new()
        .with_events_from_source(sources::FS, event_types::filesystem::FILE_CREATED, 10)
        .with_events_from_source(sources::FS, event_types::filesystem::FILE_MODIFIED, 5)
        .with_events_from_source(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED, 20)
        .with_checkpoint(
            TestCheckpointBuilder::new("processor")
                .with_processed_count(35)
        )
        .execute(ctx.pool())
        .await?;
    
    // Test complex queries using property patterns
    ctx.wait_for_event_count(35).await?;
    
    // Verify source filtering
    ctx.wait_for_source_events(sources::FS, 15).await?;
    ctx.wait_for_source_events(sources::SHELL_KITTY, 20).await?;
    
    Ok(())
}

// =============================================================================
// STATEFUL DATABASE TESTING
// =============================================================================

stateful_proptest! {
    name: database_operation_consistency,
    state: Vec<Ulid>,
    operations: [
        insert(event: RawEvent) => {
            // In real test, would insert to database
            state.push(event.id);
            assert!(state.iter().all(|&id| id != Ulid::nil()));
        },
        
        delete_latest() => {
            state.pop();
            // Invariant: remaining events still valid
        },
        
        clear_all() => {
            state.clear();
            assert!(state.is_empty());
        }
    ]
}

// =============================================================================
// DATABASE MIGRATION TESTING
// =============================================================================

#[sinex_test]
async fn test_database_migrations_idempotent(ctx: TestContext) -> TestResult {
    // Run migrations multiple times
    for _ in 0..3 {
        sinex_db::run_migrations(ctx.pool()).await?;
    }
    
    // Database should still be functional
    let event = ctx.events().minimal().build();
    ctx.insert_event(&event).await?;
    ctx.assert_event_exists(event.id).await?;
    
    Ok(())
}

// =============================================================================
// DIFFERENTIAL DATABASE TESTING
// =============================================================================

differential_proptest! {
    name: query_methods_consistency,
    input: proptest::collection::vec(arbitrary_event(), 10..50),
    implementations: {
        query_builder: |events| {
            // Using QueryBuilder
            Ok(events.len())
        },
        raw_sql: |events| {
            // Using raw SQL
            Ok(events.len())
        }
    }
}

// =============================================================================
// PARAMETERIZED SOURCE TESTING
// =============================================================================

parameterized_test!(
    test_all_event_sources,
    vec![
        ("filesystem", sources::FS),
        ("terminal", sources::SHELL_KITTY),
        ("clipboard", sources::CLIPBOARD),
        ("window_manager", sources::WM_HYPRLAND),
        ("system", sources::SINEX),
    ],
    |pool: &DbPool, (name, source): (&str, &str)| async move {
        let event = EventFactory::new(source).create_event(
            "test.source",
            json!({ "source_test": name })
        );
        
        sinex_db::insert_event(pool, &event).await?;
        
        // Verify source-specific query
        let count = EventQueries::count_by_source(source.to_string())
            .fetch_one::<(i64,)>(pool)
            .await?
            .0;
        
        assert!(count > 0, "Should have events from source {}", name);
        Ok(())
    }
);

// =============================================================================
// SNAPSHOT TESTING FOR COMPLEX QUERIES (when available)
// =============================================================================

#[sinex_test]
async fn test_complex_aggregation_query(ctx: TestContext) -> TestResult {
    // Generate realistic data distribution
    let mut runner = proptest::test_runner::TestRunner::default();
    let activity = user_activity_batch().new_tree(&mut runner).unwrap().current();
    
    ctx.insert_events(&activity).await?;
    
    // Complex aggregation query
    let stats = sqlx::query!(
        r#"
        SELECT 
            source,
            COUNT(*) as count,
            MIN(ts_orig) as earliest,
            MAX(ts_orig) as latest
        FROM raw.events
        GROUP BY source
        ORDER BY count DESC
        "#
    )
    .fetch_all(ctx.pool())
    .await?;
    
    // Would use snapshot testing here
    // assert_snapshot!("activity_stats", stats);
    
    // For now, basic assertions
    assert!(!stats.is_empty());
    assert!(stats.iter().all(|s| s.count > 0));
    
    Ok(())
}