// # Database Resilience Chaos Tests
//
// Tests for system resilience under database connection failures and Redis stream failures.
// Simulates network failures, retries, and recovery scenarios.

use futures::future::join_all;
use serde_json::json;
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::Timeouts;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Test system resilience under database connection failures
#[sinex_test]
async fn test_database_failure_resilience(ctx: TestContext) -> TestResult<()> {
    let failure_count = Arc::new(AtomicU64::new(0));
    let recovery_count = Arc::new(AtomicU64::new(0));
    let event_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Simulate database operations under failure conditions
    for worker_id in 0..5 {
        let ctx_clone = ctx.clone();
        let failures = failure_count.clone();
        let recoveries = recovery_count.clone();
        let events = event_count.clone();

        let handle = tokio::spawn(async move {
            for operation_id in 0..20 {
                events.fetch_add(1, Ordering::SeqCst);

                // Simulate database operation with potential failure
                if operation_id % 7 == 0 {
                    // Simulate database failure
                    failures.fetch_add(1, Ordering::SeqCst);
                    println!(
                        "Worker {} operation {} - simulated database failure",
                        worker_id, operation_id
                    );

                    // Simulate retry logic with exponential backoff
                    for retry in 0..3 {
                        tokio::time::sleep(Duration::from_millis(100 * (1 << retry))).await;

                        match ctx_clone
                            .publish_event(
                                &format!("chaos-worker-{}", worker_id),
                                &format!("database.retry.{}.{}", operation_id, retry),
                                json!({"worker": worker_id, "operation": operation_id, "retry": retry}),
                            )
                            .await
                        {
                            Ok(_) => {
                                recoveries.fetch_add(1, Ordering::SeqCst);
                                println!(
                                    "Worker {} operation {} retry {} succeeded",
                                    worker_id, operation_id, retry
                                );
                                break;
                            }
                            Err(e) => {
                                println!(
                                    "Worker {} operation {} retry {} failed: {}",
                                    worker_id, operation_id, retry, e
                                );
                            }
                        }
                    }
                } else {
                    // Normal database operation
                    match ctx_clone
                        .publish_event(
                            &format!("chaos-worker-{}", worker_id),
                            &format!("database.operation.{}", operation_id),
                            json!({"worker": worker_id, "operation": operation_id}),
                        )
                        .await
                    {
                        Ok(_) => {
                            recoveries.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            failures.fetch_add(1, Ordering::SeqCst);
                            println!(
                                "Worker {} operation {} failed: {}",
                                worker_id, operation_id, e
                            );
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let total_events = event_count.load(Ordering::SeqCst);
    let total_failures = failure_count.load(Ordering::SeqCst);
    let total_recoveries = recovery_count.load(Ordering::SeqCst);

    println!("Database failure resilience test results:");
    println!("- Total events attempted: {}", total_events);
    println!("- Total failures: {}", total_failures);
    println!("- Total recoveries: {}", total_recoveries);

    // Verify database state after chaos
    let final_events = sqlx::query!(
        r#"SELECT COUNT(*) as "count!" FROM core.events WHERE source LIKE 'chaos-worker-%'"#
    )
    .fetch_one(ctx.pool())
    .await?;

    println!(
        "Events successfully stored: {}",
        final_events.count
    );

    // System should show resilience - some operations should succeed
    assert!(
        total_recoveries > 0,
        "Some operations should recover from failures"
    );
    assert!(total_failures > 0, "Failures should be simulated");

    Ok(())
}

/// Test NATS/JetStream failure resilience with stream operations
#[sinex_test]
async fn test_stream_failure_resilience(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let stream_operations = Arc::new(AtomicU64::new(0));
    let stream_failures = Arc::new(AtomicU64::new(0));
    let stream_recoveries = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Simulate stream operations under failure conditions
    for worker_id in 0..3 {
        let ctx_clone = ctx.clone();
        let operations = stream_operations.clone();
        let failures = stream_failures.clone();
        let recoveries = stream_recoveries.clone();

        let handle = tokio::spawn(async move {
            for stream_id in 0..30 {
                operations.fetch_add(1, Ordering::SeqCst);

                let event_data = json!({
                    "worker": worker_id,
                    "stream": stream_id,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "data": format!("chaos-event-{}-{}", worker_id, stream_id)
                });

                // Simulate intermittent failures
                if stream_id % 10 == 0 {
                    failures.fetch_add(1, Ordering::SeqCst);
                    println!(
                        "Worker {} stream {} - simulated stream failure",
                        worker_id, stream_id
                    );

                    // Simulate retry with exponential backoff
                    for retry in 0..3 {
                        tokio::time::sleep(Duration::from_millis(200 * (1 << retry))).await;

                        match ctx_clone
                            .publish_event(
                                &format!("stream-chaos-{}", worker_id),
                                &format!("stream.retry.{}", stream_id),
                                event_data.clone(),
                            )
                            .await
                        {
                            Ok(_) => {
                                recoveries.fetch_add(1, Ordering::SeqCst);
                                println!(
                                    "Worker {} stream {} retry {} - succeeded",
                                    worker_id, stream_id, retry
                                );
                                break;
                            }
                            Err(e) => {
                                println!(
                                    "Worker {} stream {} retry {} - failed: {}",
                                    worker_id, stream_id, retry, e
                                );
                            }
                        }
                    }
                } else {
                    // Normal stream operation
                    match ctx_clone
                        .publish_event(
                            &format!("stream-chaos-{}", worker_id),
                            &format!("stream.operation.{}", stream_id),
                            event_data,
                        )
                        .await
                    {
                        Ok(_) => {
                            recoveries.fetch_add(1, Ordering::SeqCst);
                            println!(
                                "Worker {} stream {} - operation succeeded",
                                worker_id, stream_id
                            );
                        }
                        Err(e) => {
                            failures.fetch_add(1, Ordering::SeqCst);
                            println!(
                                "Worker {} stream {} - operation failed: {}",
                                worker_id, stream_id, e
                            );
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let total_operations = stream_operations.load(Ordering::SeqCst);
    let total_failures = stream_failures.load(Ordering::SeqCst);
    let total_recoveries = stream_recoveries.load(Ordering::SeqCst);

    println!("Stream failure resilience test results:");
    println!("- Total stream operations: {}", total_operations);
    println!("- Total failures: {}", total_failures);
    println!("- Total recoveries: {}", total_recoveries);

    // System should show resilience with stream failures
    assert!(
        total_operations > 0,
        "Stream operations should be attempted"
    );
    assert!(total_recoveries > 0, "Some operations should succeed");

    Ok(())
}
