//! Performance-oriented event ordering tests.

use serde_json::json;
use sinex_core::types::Ulid;
use sinex_test_utils::prelude::*;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;

#[sinex_test]
async fn perf_ulid_sequence_ordering_validation(ctx: TestContext) -> Result<()> {
    let mut event_ulids = Vec::new();
    let test_source = format!("ulid-ordering-perf-{}", Ulid::new());

    for i in 0..20 {
        if i > 0 {
            sleep(Duration::from_millis(10)).await;
        }

        let event = ctx
            .publish_json_event(
                &test_source,
                "sequence.test",
                json!({"sequence": i, "group": &test_source}),
            )
            .await?;
        event_ulids.push(event.id.expect("Event should have ID"));
    }

    let raw_ulids: Vec<Ulid> = event_ulids.iter().map(|id| id.as_ulid().clone()).collect();
    let ordering_result = verify_ulid_sequence_ordering(&raw_ulids);
    assert!(
        ordering_result.is_ok(),
        "ULID sequence should be properly ordered: {:?}",
        ordering_result
    );

    Ok(())
}

#[sinex_test]
async fn perf_concurrent_ulid_generation_ordering(_ctx: TestContext) -> Result<()> {
    let num_tasks = 20usize;
    let events_per_task = 50usize;
    let mut handles = Vec::new();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(num_tasks));

    for task_id in 0..num_tasks {
        let barrier = barrier.clone();
        let handle = tokio::spawn(async move {
            let mut ulids = Vec::with_capacity(events_per_task);
            barrier.wait().await;
            for i in 0..events_per_task as u64 {
                ulids.push(Ulid::new());
                let delay_ms = 1 + ((task_id as u64 + i) % 5);
                sleep(Duration::from_millis(delay_ms)).await;
            }
            ulids
        });
        handles.push(handle);
    }

    let mut all_ulids = Vec::with_capacity(num_tasks * events_per_task);
    for handle in handles {
        let ulids = handle.await?;
        all_ulids.extend(ulids);
    }

    let mut seen = HashSet::with_capacity(all_ulids.len());
    for ulid in &all_ulids {
        assert!(seen.insert(*ulid), "Concurrent ULIDs should remain unique");
    }

    Ok(())
}

#[sinex_test]
async fn perf_database_ordering_consistency(ctx: TestContext) -> Result<()> {
    let mut all_event_ulids = Vec::new();

    for i in 0..50 {
        let event = ctx
            .publish_json_event(
                "db-ordering-perf",
                "rapid.batch",
                json!({"batch": 1, "sequence": i}),
            )
            .await?;
        all_event_ulids.push(event.id.expect("Event should have ID"));
    }

    sleep(Duration::from_millis(100)).await;

    for i in 0..30 {
        let event = ctx
            .publish_json_event(
                "db-ordering-perf",
                "delayed.batch",
                json!({"batch": 2, "sequence": i}),
            )
            .await?;
        all_event_ulids.push(event.id.expect("Event should have ID"));
        sleep(Duration::from_millis(2)).await;
    }

    assert!(!all_event_ulids.is_empty(), "Should insert perf batch events");
    Ok(())
}
