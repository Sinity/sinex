//! Integration tests for satellite coordination using the KV-based leader election.

#[path = "../support/mod.rs"]
mod support;

use sinex_core::SinexError;
use sinex_core::Ulid;
use sinex_node_sdk::{InstanceMode, NodeCoordination};
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use support::runtime::TestRuntimeBuilder;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

const COORDINATION_TIMEOUT: Duration = Duration::from_secs(2);

#[sinex_test]
async fn test_satellite_coordination_initialization() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let runtime = TestRuntimeBuilder::new(&ctx, "coordination-init").build().await?;

    let coordination =
        NodeCoordination::from_runtime(&runtime.runtime, format!("init-{}", Ulid::new()))
            .await?;

    assert_eq!(coordination.current_mode(), InstanceMode::Standby);

    Ok(())
}

#[sinex_test]
async fn test_single_instance_becomes_leader() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let runtime = TestRuntimeBuilder::new(&ctx, "coordination-single").build().await?;

    let mut coordination =
        NodeCoordination::from_runtime(&runtime.runtime, format!("leader-{}", Ulid::new()))
            .await?;

    let processed = Arc::new(AtomicBool::new(false));
    let processed_flag = processed.clone();

    let result = timeout(COORDINATION_TIMEOUT, coordination.run_coordination_loop(|| {
        let processed_flag = processed_flag.clone();
        async move {
            processed_flag.store(true, Ordering::SeqCst);
            Ok::<(), SinexError>(())
        }
    }))
    .await;

    assert!(result.is_ok());
    assert!(processed.load(Ordering::SeqCst));

    Ok(())
}

#[sinex_test]
async fn test_multi_instance_leader_election() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let runtime = TestRuntimeBuilder::new(&ctx, "coordination-multi").build().await?;

    let mut coord1 =
        NodeCoordination::from_runtime(&runtime.runtime, format!("multi-{}", Ulid::new()))
            .await?;
    let mut coord2 =
        NodeCoordination::from_runtime(&runtime.runtime, format!("multi-{}", Ulid::new()))
            .await?;
    let mut coord3 =
        NodeCoordination::from_runtime(&runtime.runtime, format!("multi-{}", Ulid::new()))
            .await?;

    let processing_count = Arc::new(AtomicU32::new(0));

    let count1 = processing_count.clone();
    let handle1 = tokio::spawn(async move {
        timeout(COORDINATION_TIMEOUT, coord1.run_coordination_loop(|| {
            let count = count1.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<(), SinexError>(())
            }
        }))
        .await
        .is_ok()
    });

    let count2 = processing_count.clone();
    let handle2 = tokio::spawn(async move {
        timeout(COORDINATION_TIMEOUT, coord2.run_coordination_loop(|| {
            let count = count2.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<(), SinexError>(())
            }
        }))
        .await
        .is_ok()
    });

    let count3 = processing_count.clone();
    let handle3 = tokio::spawn(async move {
        timeout(COORDINATION_TIMEOUT, coord3.run_coordination_loop(|| {
            let count = count3.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<(), SinexError>(())
            }
        }))
        .await
        .is_ok()
    });

    let (result1, result2, result3) = tokio::join!(handle1, handle2, handle3);

    assert!(result1.unwrap());
    assert!(result2.unwrap());
    assert!(result3.unwrap());
    assert!(processing_count.load(Ordering::SeqCst) > 0);

    Ok(())
}
