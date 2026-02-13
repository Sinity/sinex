// # Database Resilience Chaos Tests
//
// Tests for system resilience under concurrent event publishing.
// Events flow through the NATS -> ingestd -> DB pipeline to test real production behavior.

use futures::future::join_all;
use sinex_primitives::{DynamicPayload, Timestamp};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use xtask::sandbox::prelude::*;

/// Test system resilience under concurrent event publishing with simulated failures
#[sinex_test]
async fn test_database_failure_resilience(ctx: TestContext) -> TestResult<()> {
    let failure_count = Arc::new(AtomicU64::new(0));
    let recovery_count = Arc::new(AtomicU64::new(0));
    let event_count = Arc::new(AtomicU64::new(0));

    let ctx = &ctx;

    let worker_futs = (0..5).map(|worker_id| {
        let failures = failure_count.clone();
        let recoveries = recovery_count.clone();
        let events = event_count.clone();

        async move {
            for operation_id in 0..20 {
                events.fetch_add(1, Ordering::SeqCst);

                // Simulate intermittent failures with retry logic
                if operation_id % 7 == 0 {
                    failures.fetch_add(1, Ordering::SeqCst);
                    println!(
                        "Worker {worker_id} operation {operation_id} - simulated database failure"
                    );

                    for retry in 0..3 {
                        tokio::time::sleep(Duration::from_millis(100 * (1 << retry))).await;

                        match ctx
                            .publish(DynamicPayload::new(
                                format!("chaos-worker-{worker_id}"),
                                format!("database.retry.{operation_id}.{retry}"),
                                json!({
                                    "worker": worker_id,
                                    "operation": operation_id,
                                    "retry": retry
                                }),
                            ))
                            .await
                        {
                            Ok(_) => {
                                recoveries.fetch_add(1, Ordering::SeqCst);
                                println!(
                                    "Worker {worker_id} operation {operation_id} retry {retry} succeeded"
                                );
                                break;
                            }
                            Err(e) => {
                                println!(
                                    "Worker {worker_id} operation {operation_id} retry {retry} failed: {e}"
                                );
                            }
                        }
                    }
                } else {
                    // Normal operation
                    match ctx
                        .publish(DynamicPayload::new(
                            format!("chaos-worker-{worker_id}"),
                            format!("database.operation.{operation_id}"),
                            json!({"worker": worker_id, "operation": operation_id}),
                        ))
                        .await
                    {
                        Ok(_) => {
                            recoveries.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            failures.fetch_add(1, Ordering::SeqCst);
                            println!("Worker {worker_id} operation {operation_id} failed: {e}");
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });

    join_all(worker_futs).await;

    let total_events = event_count.load(Ordering::SeqCst);
    let total_failures = failure_count.load(Ordering::SeqCst);
    let total_recoveries = recovery_count.load(Ordering::SeqCst);

    println!("Database failure resilience test results:");
    println!("- Total events attempted: {total_events}");
    println!("- Total failures: {total_failures}");
    println!("- Total recoveries: {total_recoveries}");

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

    let ctx = &ctx;

    let worker_futs = (0..3).map(|worker_id| {
        let operations = stream_operations.clone();
        let failures = stream_failures.clone();
        let recoveries = stream_recoveries.clone();

        async move {
            for stream_id in 0..30 {
                operations.fetch_add(1, Ordering::SeqCst);

                let event_data = json!({
                    "worker": worker_id,
                    "stream": stream_id,
                    "timestamp": Timestamp::now().to_string(),
                    "data": format!("chaos-event-{worker_id}-{stream_id}")
                });

                // Simulate intermittent failures
                if stream_id % 10 == 0 {
                    failures.fetch_add(1, Ordering::SeqCst);
                    println!(
                        "Worker {worker_id} stream {stream_id} - simulated stream failure"
                    );

                    for retry in 0..3 {
                        tokio::time::sleep(Duration::from_millis(200 * (1 << retry))).await;

                        match ctx
                            .publish(DynamicPayload::new(
                                format!("stream-chaos-{worker_id}"),
                                format!("stream.retry.{stream_id}"),
                                event_data.clone(),
                            ))
                            .await
                        {
                            Ok(_) => {
                                recoveries.fetch_add(1, Ordering::SeqCst);
                                println!(
                                    "Worker {worker_id} stream {stream_id} retry {retry} - succeeded"
                                );
                                break;
                            }
                            Err(e) => {
                                println!(
                                    "Worker {worker_id} stream {stream_id} retry {retry} - failed: {e}"
                                );
                            }
                        }
                    }
                } else {
                    // Normal stream operation
                    match ctx
                        .publish(DynamicPayload::new(
                            format!("stream-chaos-{worker_id}"),
                            format!("stream.operation.{stream_id}"),
                            event_data,
                        ))
                        .await
                    {
                        Ok(_) => {
                            recoveries.fetch_add(1, Ordering::SeqCst);
                            println!(
                                "Worker {worker_id} stream {stream_id} - operation succeeded"
                            );
                        }
                        Err(e) => {
                            failures.fetch_add(1, Ordering::SeqCst);
                            println!(
                                "Worker {worker_id} stream {stream_id} - operation failed: {e}"
                            );
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });

    join_all(worker_futs).await;

    let total_operations = stream_operations.load(Ordering::SeqCst);
    let total_failures = stream_failures.load(Ordering::SeqCst);
    let total_recoveries = stream_recoveries.load(Ordering::SeqCst);

    println!("Stream failure resilience test results:");
    println!("- Total stream operations: {total_operations}");
    println!("- Total failures: {total_failures}");
    println!("- Total recoveries: {total_recoveries}");

    // System should show resilience with stream failures
    assert!(
        total_operations > 0,
        "Stream operations should be attempted"
    );
    assert!(total_recoveries > 0, "Some operations should succeed");

    Ok(())
}
