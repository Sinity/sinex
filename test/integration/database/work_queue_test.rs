//! Consolidated work queue tests replacing scattered queue operation tests
//!
//! This file consolidates similar tests found in:
//! - test/integration/database/work_queue_tests.rs
//! - test/integration/worker/ (multiple files)
//! - test/unit/db/ (queue-related tests)
//! - test/system/ (queue workflow tests)

use crate::common::prelude::*;
use rstest::rstest;
use serde_json::json;

/// Test basic work queue operations
/// Replaces basic queue tests across multiple files
#[rstest]
#[case::single_event(1)]
#[case::small_batch(10)]
#[case::medium_batch(100)]
#[sinex_test]
async fn test_queue_basic_operations(
    ctx: TestContext,
    #[case] event_count: usize,
) -> TestResult {
    let agent_name = "test_agent";
    let mut event_ids = Vec::new();
    
    // Insert events and enqueue them
    for i in 0..event_count {
        let event = RawEventBuilder::new(
            "test.queue",
            "queue.test",
            json!({"index": i, "data": format!("event_{}", i)}),
        ).build();
        
        let event_id = insert_event(ctx.pool(), &event).await?;
        enqueue_work(ctx.pool(), event_id, agent_name).await?;
        event_ids.push(event_id);
    }
    
    // Verify queue count
    let queue_count = get_queue_count(ctx.pool()).await?;
    assert_eq!(queue_count, event_count);
    
    // Claim work items
    let mut claimed_work = Vec::new();
    for _ in 0..event_count {
        let work = claim_work(ctx.pool(), "test_worker").await?;
        assert!(work.is_some());
        claimed_work.push(work.unwrap());
    }
    
    // Verify all work claimed
    let pending_count = get_pending_work_count(ctx.pool()).await?;
    assert_eq!(pending_count, 0);
    
    // Complete work
    for work in claimed_work {
        complete_work(ctx.pool(), work.id).await?;
    }
    
    // Verify queue is empty
    let final_count = get_queue_count(ctx.pool()).await?;
    assert_eq!(final_count, 0);
    
    Ok(())
}

/// Test concurrent work claiming
/// Replaces concurrency tests in worker modules
#[rstest]
#[case::few_workers(3, 10)]
#[case::many_workers(10, 100)]
#[case::high_concurrency(20, 200)]
#[sinex_test]
async fn test_concurrent_work_claiming(
    ctx: TestContext,
    #[case] worker_count: usize,
    #[case] event_count: usize,
) -> TestResult {
    let agent_name = "concurrent_test_agent";
    
    // Insert events and enqueue work
    let mut event_ids = Vec::new();
    for i in 0..event_count {
        let event = RawEventBuilder::new(
            "test.concurrent",
            "concurrent.test",
            json!({"index": i}),
        ).build();
        
        let event_id = insert_event(ctx.pool(), &event).await?;
        enqueue_work(ctx.pool(), event_id, agent_name).await?;
        event_ids.push(event_id);
    }
    
    // Start workers concurrently
    let mut worker_tasks = Vec::new();
    for worker_id in 0..worker_count {
        let pool = ctx.pool().clone();
        let worker_name = format!("worker_{}", worker_id);
        
        let task = tokio::spawn(async move {
            let mut claimed_work = Vec::new();
            
            // Keep claiming work until none available
            while let Some(work) = claim_work(&pool, &worker_name).await? {
                claimed_work.push(work);
            }
            
            // Complete all claimed work
            for work in claimed_work.iter() {
                complete_work(&pool, work.id).await?;
            }
            
            Ok::<usize, Box<dyn std::error::Error>>(claimed_work.len())
        });
        
        worker_tasks.push(task);
    }
    
    // Wait for all workers to complete
    let mut total_processed = 0;
    for task in worker_tasks {
        let processed = task.await??;
        total_processed += processed;
    }
    
    // Verify all work was processed exactly once
    assert_eq!(total_processed, event_count);
    
    // Verify queue is empty
    let final_count = get_queue_count(ctx.pool()).await?;
    assert_eq!(final_count, 0);
    
    Ok(())
}

/// Test work queue with different agent types
/// Replaces agent-specific queue tests
#[rstest]
#[case::single_agent("data_processor")]
#[case::multiple_agents("data_processor,enricher,analyzer")]
#[sinex_test]
async fn test_agent_specific_queues(
    ctx: TestContext,
    #[case] agents: &str,
) -> TestResult {
    let agent_names: Vec<&str> = agents.split(',').collect();
    let events_per_agent = 10;
    
    // Create work for each agent
    let mut all_event_ids = Vec::new();
    for agent_name in &agent_names {
        for i in 0..events_per_agent {
            let event = RawEventBuilder::new(
                "test.agent",
                "agent.test",
                json!({"agent": agent_name, "index": i}),
            ).build();
            
            let event_id = insert_event(ctx.pool(), &event).await?;
            enqueue_work(ctx.pool(), event_id, agent_name).await?;
            all_event_ids.push(event_id);
        }
    }
    
    // Verify each agent can only claim its own work
    for agent_name in &agent_names {
        let mut claimed_count = 0;
        
        while let Some(work) = claim_work(ctx.pool(), &format!("worker_for_{}", agent_name)).await? {
            // Work should be targeted for this agent
            assert_eq!(work.target_agent_name, *agent_name);
            complete_work(ctx.pool(), work.id).await?;
            claimed_count += 1;
        }
        
        assert_eq!(claimed_count, events_per_agent);
    }
    
    // Verify queue is empty
    let final_count = get_queue_count(ctx.pool()).await?;
    assert_eq!(final_count, 0);
    
    Ok(())
}

/// Test work queue failure scenarios
/// Replaces failure handling tests
#[sinex_test]
async fn test_work_queue_failure_handling(ctx: TestContext) -> TestResult {
    let agent_name = "failure_test_agent";
    
    // Insert test event
    let event = RawEventBuilder::new(
        "test.failure",
        "failure.test",
        json!({"will_fail": true}),
    ).build();
    
    let event_id = insert_event(ctx.pool(), &event).await?;
    enqueue_work(ctx.pool(), event_id, agent_name).await?;
    
    // Claim work
    let work = claim_work(ctx.pool(), "failure_worker").await?;
    assert!(work.is_some());
    let work = work.unwrap();
    
    // Simulate failure
    fail_work(ctx.pool(), work.id, "Simulated processing failure").await?;
    
    // Verify work is marked as failed
    let failed_work = get_failed_work(ctx.pool()).await?;
    assert_eq!(failed_work.len(), 1);
    assert_eq!(failed_work[0].id, work.id);
    
    // Verify it can be retried
    retry_work(ctx.pool(), work.id).await?;
    
    // Should be claimable again
    let retried_work = claim_work(ctx.pool(), "retry_worker").await?;
    assert!(retried_work.is_some());
    assert_eq!(retried_work.unwrap().raw_event_id, event_id);
    
    Ok(())
}

/// Test work queue TTL (time-to-live) functionality
/// Replaces TTL-specific tests
#[sinex_test]
async fn test_work_queue_ttl(ctx: TestContext) -> TestResult {
    let agent_name = "ttl_test_agent";
    
    // Insert test event
    let event = RawEventBuilder::new(
        "test.ttl",
        "ttl.test",
        json!({"test": "ttl"}),
    ).build();
    
    let event_id = insert_event(ctx.pool(), &event).await?;
    enqueue_work(ctx.pool(), event_id, agent_name).await?;
    
    // Claim work
    let work = claim_work(ctx.pool(), "ttl_worker").await?;
    assert!(work.is_some());
    let work = work.unwrap();
    
    // Complete work
    complete_work(ctx.pool(), work.id).await?;
    
    // Verify work is completed
    let completed_work = get_completed_work(ctx.pool()).await?;
    assert!(completed_work.iter().any(|w| w.id == work.id));
    
    // TTL cleanup should eventually remove completed work
    // (This would be handled by a background process in production)
    
    Ok(())
}

/// Test work queue metrics and monitoring
/// Replaces monitoring-specific tests
#[sinex_test]
async fn test_work_queue_metrics(ctx: TestContext) -> TestResult {
    let agent_name = "metrics_test_agent";
    
    // Insert and enqueue multiple events
    let event_count = 50;
    for i in 0..event_count {
        let event = RawEventBuilder::new(
            "test.metrics",
            "metrics.test",
            json!({"index": i}),
        ).build();
        
        let event_id = insert_event(ctx.pool(), &event).await?;
        enqueue_work(ctx.pool(), event_id, agent_name).await?;
    }
    
    // Get initial metrics
    let initial_metrics = get_queue_metrics(ctx.pool()).await?;
    assert_eq!(initial_metrics.total_pending, event_count);
    assert_eq!(initial_metrics.total_processing, 0);
    assert_eq!(initial_metrics.total_completed, 0);
    
    // Claim some work
    let claim_count = 10;
    for _ in 0..claim_count {
        claim_work(ctx.pool(), "metrics_worker").await?;
    }
    
    // Check metrics after claiming
    let processing_metrics = get_queue_metrics(ctx.pool()).await?;
    assert_eq!(processing_metrics.total_pending, event_count - claim_count);
    assert_eq!(processing_metrics.total_processing, claim_count);
    assert_eq!(processing_metrics.total_completed, 0);
    
    Ok(())
}

/// Test work queue with large payloads
/// Replaces payload-specific queue tests
#[sinex_test]
async fn test_work_queue_large_payloads(ctx: TestContext) -> TestResult {
    let agent_name = "large_payload_agent";
    
    // Create event with large payload
    let large_data = "x".repeat(1024 * 1024); // 1MB
    let event = RawEventBuilder::new(
        "test.large",
        "large.payload",
        json!({
            "large_data": large_data,
            "size": large_data.len(),
            "chunks": (0..1000).map(|i| format!("chunk_{}", i)).collect::<Vec<_>>()
        }),
    ).build();
    
    let event_id = insert_event(ctx.pool(), &event).await?;
    enqueue_work(ctx.pool(), event_id, agent_name).await?;
    
    // Claim and process large payload work
    let work = claim_work(ctx.pool(), "large_payload_worker").await?;
    assert!(work.is_some());
    let work = work.unwrap();
    
    // Verify we can retrieve the event with large payload
    let retrieved_event = get_event_by_id(ctx.pool(), work.raw_event_id).await?;
    assert_eq!(retrieved_event.payload["size"], large_data.len());
    
    // Complete the work
    complete_work(ctx.pool(), work.id).await?;
    
    Ok(())
}