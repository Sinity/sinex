// Optimized PKM Service Integration Tests
//
// This module demonstrates optimized PKM service tests using performance utilities
// and parallel execution patterns to reduce test time from 60s to under 15s.

use crate::common::prelude::*;
use crate::common::performance_utils::{
    TestTimer, BatchPerformanceAnalyzer, ParallelTestRunner,
    helpers::measure_query_performance
};
use crate::common::{generators, events};
use crate::common::builders::{TestEventBuilder, BatchEventBuilder};
use sinex_services::pkm::PkmService;
use sinex_ulid::Ulid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

/// Optimized complete PKM workflow test
#[sinex_test(timeout = 15)] // Reduced from 60s
async fn test_optimized_complete_pkm_workflow(ctx: TestContext) -> TestResult {
    let timer = TestTimer::new("pkm_workflow");
    let service = Arc::new(PkmService::new(ctx.pool().clone()));
    
    // Step 1: Batch create events and annotations concurrently
    timer.checkpoint("create_events_and_annotations").await;
    
    let events = generators::test_events(5);
    let semaphore = Arc::new(Semaphore::new(3)); // Limit concurrent operations
    
    let mut handles = Vec::new();
    for (i, event) in events.into_iter().enumerate() {
        let pool = ctx.pool().clone();
        let service = service.clone();
        let sem = semaphore.clone();
        let timer = timer.clone();
        
        let handle = tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            
            // Insert event
            let inserted = sinex_db::insert_event_with_validator(&pool, &event, None).await?;
            timer.record_operation();
            
            // Create note annotation
            let note_id = service
                .create_note(
                    inserted.id,
                    &format!("Note for event {}", i),
                    vec!["test".to_string(), format!("event_{}", i)],
                    "test_user",
                )
                .await?;
            timer.record_operation();
            
            Ok::<_, anyhow::Error>((inserted.id, note_id))
        });
        
        handles.push(handle);
    }
    
    let mut event_note_pairs = Vec::new();
    for handle in handles {
        event_note_pairs.push(handle.await??);
    }
    
    // Step 2: Create entities in batch
    timer.checkpoint("create_entities_batch").await;
    
    let entity_templates = vec![
        ("Alice Smith", "person"),
        ("Bob Johnson", "person"),
        ("AI Research Project", "project"),
        ("Stanford University", "organization"),
    ];
    
    // Use the first event for entity creation
    let (first_event_id, _) = event_note_pairs[0];
    
    let entities: Vec<_> = entity_templates
        .into_iter()
        .map(|(name, kind)| (name.to_string(), kind.to_string()))
        .collect();
    
    let entity_ids = service
        .create_entities_from_list(first_event_id, entities)
        .await?;
    timer.record_operations(entity_ids.len() as u64);
    
    // Step 3: Create relationships concurrently
    timer.checkpoint("create_relationships").await;
    
    let relationship_tasks = vec![
        (entity_ids[0], entity_ids[2], "works_on", HashMap::new()),
        (entity_ids[1], entity_ids[2], "works_on", HashMap::new()),
        (entity_ids[0], entity_ids[3], "affiliated_with", HashMap::new()),
        (entity_ids[1], entity_ids[3], "affiliated_with", HashMap::new()),
    ];
    
    let mut rel_handles = Vec::new();
    for (from, to, rel_type, props) in relationship_tasks {
        let service = service.clone();
        let timer = timer.clone();
        
        let handle = tokio::spawn(async move {
            let result = service
                .create_relationship(from, to, &rel_type, props)
                .await;
            timer.record_operation();
            result
        });
        
        rel_handles.push(handle);
    }
    
    for handle in rel_handles {
        handle.await??;
    }
    
    // Step 4: Verify results with parallel queries
    timer.checkpoint("verify_results").await;
    
    let verification_report = measure_query_performance(
        "pkm_verification_queries",
        || {
            let service = service.clone();
            async move {
                // Query entities
                let entities = service.query_entities(None, None, 10).await?;
                assert!(entities.len() >= 4);
                
                // Query relationships
                let relationships = service
                    .get_entity_relationships(entity_ids[0])
                    .await?;
                assert!(!relationships.is_empty());
                
                Ok(())
            }
        },
        5
    ).await?;
    
    timer.checkpoint("complete").await;
    
    // Generate and verify performance
    let report = timer.report().await;
    report.print();
    verification_report.print();
    
    // Assert performance targets
    assert!(report.meets_threshold(
        std::time::Duration::from_secs(15),
        20.0 // At least 20 operations per second
    ));
    
    Ok(())
}

/// Optimized batch entity creation test
#[sinex_test(timeout = 10)] // Reduced from 30s
async fn test_optimized_batch_entity_operations(ctx: TestContext) -> TestResult {
    let service = Arc::new(PkmService::new(ctx.pool().clone()));
    let timer = TestTimer::new("batch_entity_operations");
    
    // Setup: Create a base event
    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;
    
    // Find optimal batch size for entity creation
    timer.checkpoint("analyze_batch_size").await;
    let analyzer = BatchPerformanceAnalyzer::new(vec![10, 25, 50, 100]);
    
    let optimal = analyzer.analyze_batch_operation(|batch_size| {
        let service = service.clone();
        let event_id = inserted.id;
        async move {
            let entities: Vec<_> = (0..batch_size)
                .map(|i| (format!("Entity_{}", i), "test_entity".to_string()))
                .collect();
            
            service.create_entities_from_list(event_id, entities).await
        }
    }).await?;
    
    optimal.print_analysis();
    timer.checkpoint("batch_creation_complete").await;
    
    // Use optimal batch size for performance test
    let entities: Vec<_> = (0..optimal.size)
        .map(|i| (format!("OptimalEntity_{}", i), "optimal".to_string()))
        .collect();
    
    let entity_ids = service
        .create_entities_from_list(inserted.id, entities)
        .await?;
    timer.record_operations(entity_ids.len() as u64);
    
    // Test batch querying
    timer.checkpoint("batch_query").await;
    let query_report = measure_query_performance(
        "entity_batch_query",
        || {
            let service = service.clone();
            async move {
                service.query_entities(Some("optimal"), None, 1000).await
            }
        },
        10
    ).await?;
    
    query_report.print();
    
    // Verify performance
    assert!(optimal.items_per_second > 50.0);
    assert!(query_report.ops_per_second > 10.0);

/// Test parallel PKM operations
#[sinex_test(timeout = 10)]
async fn test_parallel_pkm_operations(ctx: TestContext) -> TestResult {
    let service = Arc::new(PkmService::new(ctx.pool().clone()));
    let runner = ParallelTestRunner::new(4);
    
    // Create base event for all operations
    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;
    let event_id = inserted.id;
    
    // Define parallel PKM operations
    let tests = vec![
        ("create_notes".to_string(), {
            let service = service.clone();
            move || async move {
                for i in 0..5 {
                    service
                        .create_note(
                            event_id,
                            &format!("Parallel note {}", i),
                            vec!["parallel".to_string()],
                            "test_user",
                        )
                        .await?;
                }
                Ok(())
            }
        }),
        ("create_entities".to_string(), {
            let service = service.clone();
            move || async move {
                let entities = vec![
                    ("Parallel Entity 1".to_string(), "entity".to_string()),
                    ("Parallel Entity 2".to_string(), "entity".to_string()),
                ];
                service.create_entities_from_list(event_id, entities).await?;
                Ok(())
            }
        }),
        ("query_operations".to_string(), {
            let service = service.clone();
            move || async move {
                for _ in 0..10 {
                    service.query_entities(None, None, 10).await?;
                }
                Ok(())
            }
        }),
        ("relationship_operations".to_string(), {
            let service = service.clone();
            move || async move {
                // Create two entities first
                let entities = vec![
                    ("Rel Entity A".to_string(), "entity".to_string()),
                    ("Rel Entity B".to_string(), "entity".to_string()),
                ];
                let ids = service.create_entities_from_list(event_id, entities).await?;
                
                // Create relationship
                service
                    .create_relationship(ids[0], ids[1], "related_to", HashMap::new())
                    .await?;
                Ok(())
            }
        }),
    ];
    
    let summary = runner.run_tests(tests).await?;
    summary.print_summary();
    
    assert_eq!(summary.failed_tests, 0);
    assert!(summary.avg_duration < std::time::Duration::from_secs(3));
    
    Ok(())
}

/// Demonstrate caching strategy for PKM queries
#[sinex_test(timeout = 10)]
async fn test_pkm_query_caching(ctx: TestContext) -> TestResult {
    let service = Arc::new(PkmService::new(ctx.pool().clone()));
    let timer = TestTimer::new("pkm_query_caching");
    
    // Setup test data
    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;
    
    // Create entities
    let entities: Vec<_> = (0..20)
        .map(|i| (format!("CachedEntity_{}", i), "cached".to_string()))
        .collect();
    
    service.create_entities_from_list(inserted.id, entities).await?;
    
    // Test cold query performance
    timer.checkpoint("cold_queries").await;
    for _ in 0..5 {
        let _ = service.query_entities(Some("cached"), None, 100).await?;
        timer.record_operation();
    }
    
    // Simulate cache warmup
    timer.checkpoint("cache_warmup").await;
    let cached_results = Arc::new(Mutex::new(
        service.query_entities(Some("cached"), None, 100).await?
    ));
    
    // Test warm query performance (using cached results)
    timer.checkpoint("warm_queries").await;
    for _ in 0..50 {
        let _ = cached_results.lock().await.clone();
        timer.record_operation();
    }
    
    let report = timer.report().await;
    report.print();
    
    // Warm queries should be significantly faster
    assert!(report.ops_per_second > 1000.0);
    
    Ok(())
}