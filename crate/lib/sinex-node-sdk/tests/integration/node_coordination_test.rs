//! Integration tests for node coordination using the KV-based leader election.

use crate::support::runtime::TestRuntimeBuilder;
use sinex_node_sdk::SinexError;
use sinex_node_sdk::Ulid;
use sinex_node_sdk::{InstanceMode, NodeCoordination};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use xtask::sandbox::{sinex_test, timing::Timeouts, TestContext};

const COORDINATION_TIMEOUT: Duration = Duration::from_secs(Timeouts::SHORT);

#[sinex_test]
async fn test_node_coordination_initialization() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let runtime = TestRuntimeBuilder::new(&ctx, "coordination-init")
        .build()
        .await?;

    let coordination = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("init-{}", Ulid::new().to_string().to_lowercase()),
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
        format!("leader-{}", Ulid::new().to_string().to_lowercase()),
    )
    .await?;

    let processed = Arc::new(AtomicBool::new(false));
    let processed_flag = processed.clone();

    let _result = timeout(
        COORDINATION_TIMEOUT,
        coordination.run_coordination_loop(move || {
            let processed_flag = processed_flag.clone();
            async move {
                processed_flag.store(true, Ordering::SeqCst);
                Ok::<(), SinexError>(())
            }
        }),
    )
    .await;

    // The loop is infinite; timeout cancels it. What matters is the side effect.
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
        format!("multi-{}", Ulid::new().to_string().to_lowercase()),
    )
    .await?;
    let mut coord2 = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("multi-{}", Ulid::new().to_string().to_lowercase()),
    )
    .await?;
    let mut coord3 = NodeCoordination::from_runtime(
        &runtime.runtime,
        format!("multi-{}", Ulid::new().to_string().to_lowercase()),
    )
    .await?;

    let processing_count = Arc::new(AtomicU32::new(0));

    let count1 = processing_count.clone();
    let handle1 = tokio::spawn(async move {
        let _ = timeout(
            COORDINATION_TIMEOUT,
            coord1.run_coordination_loop(move || {
                let count = count1.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<(), SinexError>(())
                }
            }),
        )
        .await;
    });

    let count2 = processing_count.clone();
    let handle2 = tokio::spawn(async move {
        let _ = timeout(
            COORDINATION_TIMEOUT,
            coord2.run_coordination_loop(move || {
                let count = count2.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<(), SinexError>(())
                }
            }),
        )
        .await;
    });

    let count3 = processing_count.clone();
    let handle3 = tokio::spawn(async move {
        let _ = timeout(
            COORDINATION_TIMEOUT,
            coord3.run_coordination_loop(move || {
                let count = count3.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<(), SinexError>(())
                }
            }),
        )
        .await;
    });

    let (result1, result2, result3) = tokio::join!(handle1, handle2, handle3);

    // All tasks should complete (the timeout just cancels the infinite loop)
    result1.unwrap();
    result2.unwrap();
    result3.unwrap();
    assert!(processing_count.load(Ordering::SeqCst) > 0);

    Ok(())
}
