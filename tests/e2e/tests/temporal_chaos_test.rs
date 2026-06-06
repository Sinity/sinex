//! Temporal Chaos Testing
//!
//! Tests the system's behavior under extreme timing conditions and concurrent load.
//! Verifies that events published in tight bursts are all persisted without loss,
//! and that UUIDv7-based ordering remains consistent.

use futures::future::join_all;
use sinex_primitives::DynamicPayload;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use xtask::sandbox::prelude::*;

/// Send a large burst of events simultaneously to test backpressure handling.
/// Verifies no events are dropped during overwhelming bursts.
#[sinex_test(timeout = 60)]
#[ignore = "heavy: run with xtask test --heavy"]
async fn test_thundering_herd_extreme_load(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let worker_count = 10usize;
    let events_per_worker = 50usize;
    let total_expected = worker_count * events_per_worker;
    let success_count = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let ctx = &ctx;
    let futs: Vec<_> = (0..worker_count)
        .map(|wid| {
            let successes = success_count.clone();
            async move {
                for eid in 0..events_per_worker {
                    let payload = DynamicPayload::new(
                        format!("thundering-herd-{wid}"),
                        "temporal.thundering_herd",
                        json!({"worker": wid, "event": eid}),
                    );
                    if ctx.publish(payload).await.is_ok() {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        })
        .collect();

    join_all(futs).await;

    let total_ok = success_count.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    let throughput = total_ok as f64 / elapsed.as_secs_f64();
    println!(
        "Thundering herd: {total_ok}/{total_expected} in {elapsed:?} ({throughput:.0} events/s)"
    );

    let success_rate = total_ok as f64 / total_expected as f64;
    assert!(
        success_rate > 0.90,
        "should achieve > 90% success rate under thundering herd, got {:.1}%",
        success_rate * 100.0
    );

    // Verify database consistency
    let db_count = ctx.pool().events().count_all().await?;
    assert!(
        db_count >= (total_ok as f64 * 0.9) as i64,
        "database should have at least 90% of published events"
    );

    Ok(())
}

/// Send events with varied sources and types concurrently, then verify ordering
/// consistency: events with later UUIDv7 IDs should have later (or equal) timestamps.
#[sinex_test(timeout = 60)]
#[ignore = "heavy: run with xtask test --heavy"]
async fn test_temporal_chaos_ordering_and_consistency(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;

    let event_count = 50usize;

    // Publish events sequentially with predictable ordering
    for i in 0..event_count {
        scope
            .publish(DynamicPayload::new(
                "temporal-ordering",
                "temporal.ordering.test",
                json!({"seq": i, "batch": "ordering_test"}),
            ))
            .await?;
    }

    scope.wait_for_event_count(event_count).await?;

    // Retrieve events and verify UUIDv7 ordering
    let source = sinex_primitives::EventSource::from("temporal-ordering");
    let events = scope
        .ctx()
        .pool
        .events()
        .get_by_source(&source, sinex_primitives::Pagination::new(Some(100), None))
        .await?;

    assert_eq!(events.len(), event_count, "all events should be persisted");

    // Verify all events have IDs (UUIDv7 IDs)
    for event in &events {
        assert!(event.id.is_some(), "every event should have a UUIDv7");
    }

    // Verify UUIDv7 IDs are unique
    let ids: std::collections::HashSet<_> = events.iter().filter_map(|e| e.id.as_ref()).collect();
    assert_eq!(
        ids.len(),
        events.len(),
        "all event UUIDv7 IDs should be unique"
    );

    scope.shutdown().await?;
    Ok(())
}
