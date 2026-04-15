//! Integration tests for node coordination using the KV-based leader election.

use crate::support::runtime::TestRuntimeBuilder;
use sinex_node_sdk::SinexError;
use sinex_node_sdk::Uuid;
use sinex_node_sdk::{InstanceMode, NodeCoordination};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};
use xtask::sandbox::{TestContext, sinex_test, timing::Timeouts};

const COORDINATION_TIMEOUT: Duration = Duration::from_secs(Timeouts::QUICK);

#[sinex_test]
async fn test_node_coordination_initialization() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let runtime = TestRuntimeBuilder::new(&ctx, "coordination-init")
        .build()
        .await?;

    let coordination = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("init-{}", Uuid::now_v7().to_string().to_lowercase()),
    )
    .await?;

    assert_eq!(coordination.current_mode(), InstanceMode::Standby);

    Ok(())
}

#[sinex_test]
async fn test_single_instance_becomes_leader() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let runtime = TestRuntimeBuilder::new(&ctx, "coordination-single")
        .build()
        .await?;

    let mut coordination = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("leader-{}", Uuid::now_v7().to_string().to_lowercase()),
    )
    .await?;

    let processed = Arc::new(AtomicBool::new(false));
    let processed_flag = processed.clone();

    timeout(
        COORDINATION_TIMEOUT,
        coordination.run_coordination_loop(move || {
            let processed_flag = processed_flag.clone();
            async move {
                processed_flag.store(true, Ordering::SeqCst);
                Ok::<(), SinexError>(())
            }
        }),
    )
    .await??;

    // Timeout bounds the test; clean loop exit is also acceptable.
    assert!(processed.load(Ordering::SeqCst));

    Ok(())
}

#[sinex_test]
async fn test_multi_instance_leader_election() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let runtime = TestRuntimeBuilder::new(&ctx, "coordination-multi")
        .build()
        .await?;

    let mut coord1 = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("multi-{}", Uuid::now_v7().to_string().to_lowercase()),
    )
    .await?;
    let mut coord2 = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("multi-{}", Uuid::now_v7().to_string().to_lowercase()),
    )
    .await?;
    let mut coord3 = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("multi-{}", Uuid::now_v7().to_string().to_lowercase()),
    )
    .await?;

    let processing_count = Arc::new(AtomicU32::new(0));
    let leader_entered = Arc::new(Notify::new());
    let hold_leader = Arc::new(Notify::new());

    let count1 = processing_count.clone();
    let entered1 = leader_entered.clone();
    let hold1 = hold_leader.clone();
    let handle1 = tokio::spawn(async move {
        coord1
            .run_coordination_loop(move || {
                let count = count1.clone();
                let entered = entered1.clone();
                let hold = hold1.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    entered.notify_waiters();
                    hold.notified().await;
                    Ok::<(), SinexError>(())
                }
            })
            .await?;
        Ok::<(), color_eyre::Report>(())
    });

    let count2 = processing_count.clone();
    let entered2 = leader_entered.clone();
    let hold2 = hold_leader.clone();
    let handle2 = tokio::spawn(async move {
        coord2
            .run_coordination_loop(move || {
                let count = count2.clone();
                let entered = entered2.clone();
                let hold = hold2.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    entered.notify_waiters();
                    hold.notified().await;
                    Ok::<(), SinexError>(())
                }
            })
            .await?;
        Ok::<(), color_eyre::Report>(())
    });

    let count3 = processing_count.clone();
    let entered3 = leader_entered.clone();
    let hold3 = hold_leader.clone();
    let handle3 = tokio::spawn(async move {
        coord3
            .run_coordination_loop(move || {
                let count = count3.clone();
                let entered = entered3.clone();
                let hold = hold3.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    entered.notify_waiters();
                    hold.notified().await;
                    Ok::<(), SinexError>(())
                }
            })
            .await?;
        Ok::<(), color_eyre::Report>(())
    });

    timeout(COORDINATION_TIMEOUT, leader_entered.notified()).await?;
    assert_eq!(
        processing_count.load(Ordering::SeqCst),
        1,
        "exactly one contender should enter the leader callback"
    );

    handle1.abort();
    handle2.abort();
    handle3.abort();

    for handle in [handle1, handle2, handle3] {
        let join_result = handle.await;
        let join_error = join_result.expect_err("coordination tasks should be cancelled");
        assert!(
            join_error.is_cancelled(),
            "unexpected join error: {join_error}"
        );
    }

    Ok(())
}
