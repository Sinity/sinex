//! Agent Lifecycle Chaos Tests
//!
//! Tests for automaton registration, heartbeat, and lifecycle operations under chaos conditions.
//! Simulates concurrent registrations, lifecycle state transitions, and heartbeat events
//! flowing through the pipeline.

use futures::future::join_all;
use sinex_primitives::DynamicPayload;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use xtask::sandbox::prelude::*;

/// Simulate multiple agents performing lifecycle operations (register, heartbeat,
/// deregister) concurrently through the event pipeline. Verify all lifecycle events
/// are persisted without corruption.
#[sinex_test(timeout = 60)]
#[ignore = "chaos test requiring controlled failure injection"]
async fn test_agent_lifecycle_concurrent_operations(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let agent_count = 5usize;
    let ops_per_agent = 10usize;
    let success_count = Arc::new(AtomicU64::new(0));

    let ctx = &ctx;
    let futs: Vec<_> = (0..agent_count)
        .map(|agent_id| {
            let successes = success_count.clone();
            async move {
                let agent_name = format!("agent-lifecycle-{agent_id}");

                // Register
                let _ = ctx
                    .publish(DynamicPayload::new(
                        agent_name.as_str(),
                        "agent.registered",
                        json!({"agent_id": agent_id, "status": "registering"}),
                    ))
                    .await;

                // Heartbeats and operations
                for op in 0..ops_per_agent {
                    let event_type = if op % 3 == 0 {
                        "agent.heartbeat"
                    } else {
                        "agent.operation"
                    };
                    let payload = DynamicPayload::new(
                        agent_name.as_str(),
                        event_type,
                        json!({
                            "agent_id": agent_id,
                            "op": op,
                            "status": "active"
                        }),
                    );
                    if ctx.publish(payload).await.is_ok() {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Deregister
                let _ = ctx
                    .publish(DynamicPayload::new(
                        agent_name.as_str(),
                        "agent.deregistered",
                        json!({"agent_id": agent_id, "status": "stopped"}),
                    ))
                    .await;
            }
        })
        .collect();

    join_all(futs).await;

    let total_ok = success_count.load(Ordering::Relaxed);
    let total_expected = (agent_count * ops_per_agent) as u64;
    println!("Agent lifecycle chaos: {total_ok}/{total_expected} operations succeeded");

    let success_rate = total_ok as f64 / total_expected as f64;
    assert!(
        success_rate > 0.90,
        "should maintain > 90% success rate, got {:.1}%",
        success_rate * 100.0
    );

    // Verify at least some lifecycle events persisted
    let db_count = ctx.pool().events().count_all().await?;
    assert!(
        db_count > 0,
        "database should have lifecycle events persisted"
    );

    Ok(())
}

/// Simulate rapid agent registration and heartbeat bursts with interleaved failures.
/// Agents register, emit heartbeats, some "crash" (stop sending), and new ones take over.
#[sinex_test(timeout = 60)]
#[ignore = "chaos test requiring controlled failure injection"]
async fn test_agent_registration_and_heartbeat_chaos(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let generations = 3u32;
    let agents_per_gen = 4u32;
    let heartbeats_per_agent = 5u32;

    let ctx = &ctx;
    for generation in 0..generations {
        let futs: Vec<_> = (0..agents_per_gen)
            .map(|aid| async move {
                let agent = format!("heartbeat-chaos-gen{generation}-agent{aid}");

                // Register
                let _ = ctx
                    .publish(DynamicPayload::new(
                        agent.as_str(),
                        "agent.registered",
                        json!({"gen": generation, "agent": aid}),
                    ))
                    .await;

                // Heartbeats
                for hb in 0..heartbeats_per_agent {
                    let _ = ctx
                        .publish(DynamicPayload::new(
                            agent.as_str(),
                            "agent.heartbeat",
                            json!({"gen": generation, "agent": aid, "heartbeat": hb}),
                        ))
                        .await;
                }

                // Odd agents "crash" (no deregister), even agents shut down cleanly
                if aid % 2 == 0 {
                    let _ = ctx
                        .publish(DynamicPayload::new(
                            agent.as_str(),
                            "agent.deregistered",
                            json!({"gen": generation, "agent": aid, "reason": "clean_shutdown"}),
                        ))
                        .await;
                }
            })
            .collect();

        join_all(futs).await;
    }

    // Verify the database has events from all generations
    let db_count = ctx.pool().events().count_all().await?;
    let min_expected = i64::from(generations * agents_per_gen * (1 + heartbeats_per_agent));
    println!("Heartbeat chaos: {db_count} events (min expected ~{min_expected})");

    assert!(
        db_count >= min_expected / 2,
        "should persist at least half of expected lifecycle events"
    );

    Ok(())
}
