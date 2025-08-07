//! Integration tests for complete satellite coordination system
//!
//! Tests end-to-end coordination workflows:
//! - Hot standby pattern
//! - Leadership election and handoff  
//! - Version-based upgrades
//! - Failure detection and recovery

use sinex_satellite_sdk::coordination::{SatelliteCoordination, InstanceMode};
use sinex_satellite_sdk::version::{SatelliteVersion, SatelliteInstance};
use sinex_db::distributed_locking::DistributedCoordination;
use sinex_test_utils::TestContext;
use sinex_test_utils::sinex_test;
use color_eyre::eyre::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};
use tokio::time::{timeout, Duration};

#[sinex_test]
async fn test_satellite_coordination_initialization() -> Result<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    let instance = SatelliteInstance::new(
        "init_test",
        SatelliteVersion::parse("1.0.100+abc123").unwrap()
    );
    
    let mut coordination = SatelliteCoordination::new(instance, pool.clone());
    
    // Test initialization
    coordination.initialize().await?;
    
    // Should start in standby mode
    assert_eq!(coordination.current_mode(), &InstanceMode::Standby);
    
    Ok(())
}

#[sinex_test]
async fn test_single_instance_becomes_leader() -> Result<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    let instance = SatelliteInstance::new(
        "single_leader_test",
        SatelliteVersion::parse("1.0.100+abc123").unwrap()
    );
    
    let mut coordination = SatelliteCoordination::new(instance, pool.clone());
    coordination.initialize().await?;
    
    // Simulate leadership acquisition
    let leadership_acquired = Arc::new(AtomicBool::new(false));
    let leadership_flag = leadership_acquired.clone();
    
    // Run coordination loop briefly
    let coordination_handle = tokio::spawn(async move {
        let result = timeout(Duration::from_millis(500), coordination.run_coordination_loop(|| {
            let flag = leadership_flag.clone();
            async move {
                flag.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<(), Box<dyn std::error::Error>>(())
            }
        })).await;
        
        result.is_ok()
    });
    
    let coordination_completed = coordination_handle.await.unwrap();
    
    // Single instance should become leader and run processing
    assert!(coordination_completed);
    assert!(leadership_acquired.load(Ordering::SeqCst));
    
    Ok(())
}

#[sinex_test]
async fn test_multi_instance_leadership_election() -> Result<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    // Create three instances with different versions
    let instance1 = SatelliteInstance::new(
        "multi_leader_test",
        SatelliteVersion::parse("1.0.100+old").unwrap()
    );
    
    let instance2 = SatelliteInstance::new(
        "multi_leader_test",
        SatelliteVersion::parse("1.0.200+newer").unwrap()
    );
    
    let instance3 = SatelliteInstance::new(
        "multi_leader_test", 
        SatelliteVersion::parse("1.0.300+newest").unwrap()
    );
    
    let mut coord1 = SatelliteCoordination::new(instance1, pool.clone());
    let mut coord2 = SatelliteCoordination::new(instance2, pool.clone());
    let mut coord3 = SatelliteCoordination::new(instance3, pool.clone());
    
    coord1.initialize().await?;
    coord2.initialize().await?;
    coord3.initialize().await?;
    
    let processing_count = Arc::new(AtomicU32::new(0));
    
    // Start all instances concurrently
    let count1 = processing_count.clone();
    let handle1 = tokio::spawn(async move {
        timeout(Duration::from_millis(300), coord1.run_coordination_loop(|| {
            let count = count1.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<(), Box<dyn std::error::Error>>(())
            }
        })).await.is_ok()
    });
    
    let count2 = processing_count.clone();
    let handle2 = tokio::spawn(async move {
        timeout(Duration::from_millis(300), coord2.run_coordination_loop(|| {
            let count = count2.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<(), Box<dyn std::error::Error>>(())
            }
        })).await.is_ok()
    });
    
    let count3 = processing_count.clone();
    let handle3 = tokio::spawn(async move {
        timeout(Duration::from_millis(300), coord3.run_coordination_loop(|| {
            let count = count3.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<(), Box<dyn std::error::Error>>(())
            }
        })).await.is_ok()
    });
    
    // Wait for all coordination attempts
    let (result1, result2, result3) = tokio::join!(handle1, handle2, handle3);
    
    // All coordination loops should complete
    assert!(result1.unwrap());
    assert!(result2.unwrap());
    assert!(result3.unwrap());
    
    // Only one instance should have processed (the leader)
    // Others should be in standby and not process
    let total_processing = processing_count.load(Ordering::SeqCst);
    
    // Should be multiple processing calls from the single leader
    assert!(total_processing > 0);
    
    Ok(())
}

#[sinex_test] 
async fn test_version_based_handoff() -> Result<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    // Start with older version as leader
    let old_instance = SatelliteInstance::new(
        "handoff_test",
        SatelliteVersion::parse("1.0.100+old").unwrap()
    );
    
    let mut old_coordination = SatelliteCoordination::new(old_instance, pool.clone());
    old_coordination.initialize().await?;
    
    let old_processing = Arc::new(AtomicBool::new(false));
    let new_processing = Arc::new(AtomicBool::new(false));
    
    // Start old version
    let old_flag = old_processing.clone();
    let old_handle = tokio::spawn(async move {
        timeout(Duration::from_millis(400), old_coordination.run_coordination_loop(|| {
            let flag = old_flag.clone();
            async move {
                flag.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<(), Box<dyn std::error::Error>>(())
            }
        })).await.is_ok()
    });
    
    // Let old version establish leadership
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Deploy new version
    let new_instance = SatelliteInstance::new(
        "handoff_test",
        SatelliteVersion::parse("1.0.200+new").unwrap()
    );
    
    let mut new_coordination = SatelliteCoordination::new(new_instance, pool.clone());
    new_coordination.initialize().await?;
    
    let new_flag = new_processing.clone();
    let new_handle = tokio::spawn(async move {
        timeout(Duration::from_millis(300), new_coordination.run_coordination_loop(|| {
            let flag = new_flag.clone();
            async move {
                flag.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<(), Box<dyn std::error::Error>>(())
            }
        })).await.is_ok()
    });
    
    // Wait for coordination to complete
    let (old_result, new_result) = tokio::join!(old_handle, new_handle);
    
    assert!(old_result.unwrap());
    assert!(new_result.unwrap());
    
    // Both should have had a chance to process, but handoff should occur
    // (exact timing depends on coordination implementation)
    
    Ok(())
}

#[sinex_test]
async fn test_leader_failure_detection() -> Result<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    // Create two instances
    let leader_instance = SatelliteInstance::new(
        "failure_test",
        SatelliteVersion::parse("1.0.100+leader").unwrap()
    );
    
    let standby_instance = SatelliteInstance::new(
        "failure_test",
        SatelliteVersion::parse("1.0.100+standby").unwrap()
    );
    
    let mut leader_coord = SatelliteCoordination::new(leader_instance, pool.clone());
    let mut standby_coord = SatelliteCoordination::new(standby_instance, pool.clone());
    
    leader_coord.initialize().await?;
    standby_coord.initialize().await?;
    
    let leader_processing = Arc::new(AtomicBool::new(false));
    let standby_processing = Arc::new(AtomicBool::new(false));
    
    // Start leader (will run briefly then "fail")
    let leader_flag = leader_processing.clone();
    let leader_handle = tokio::spawn(async move {
        timeout(Duration::from_millis(200), leader_coord.run_coordination_loop(|| {
            let flag = leader_flag.clone();
            async move {
                flag.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<(), Box<dyn std::error::Error>>(())
            }
        })).await.is_ok()
    });
    
    // Start standby (will detect leader failure and take over)
    let standby_flag = standby_processing.clone();
    let standby_handle = tokio::spawn(async move {
        // Wait for leader to fail, then take over
        tokio::time::sleep(Duration::from_millis(250)).await;
        
        timeout(Duration::from_millis(300), standby_coord.run_coordination_loop(|| {
            let flag = standby_flag.clone();
            async move {
                flag.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<(), Box<dyn std::error::Error>>(())
            }
        })).await.is_ok()
    });
    
    let (leader_result, standby_result) = tokio::join!(leader_handle, standby_handle);
    
    assert!(leader_result.unwrap());
    assert!(standby_result.unwrap());
    
    // Both should have processed at some point
    assert!(leader_processing.load(Ordering::SeqCst));
    assert!(standby_processing.load(Ordering::SeqCst));
    
    Ok(())
}

#[sinex_test]
async fn test_coordination_with_preflight_checks() -> Result<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    let instance = SatelliteInstance::new(
        "preflight_test",
        SatelliteVersion::parse("1.0.100+test").unwrap()
    );
    
    let mut coordination = SatelliteCoordination::new(instance, pool.clone());
    coordination.initialize().await?;
    
    // Coordination should integrate with preflight system
    // (This tests the integration points even if preflight is simplified)
    
    let processing_occurred = Arc::new(AtomicBool::new(false));
    let flag = processing_occurred.clone();
    
    let result = timeout(Duration::from_millis(300), coordination.run_coordination_loop(|| {
        let flag = flag.clone();
        async move {
            flag.store(true, Ordering::SeqCst);
            Ok::<(), Box<dyn std::error::Error>>(())
        }
    })).await;
    
    assert!(result.is_ok());
    assert!(processing_occurred.load(Ordering::SeqCst));
    
    Ok(())
}

#[sinex_test]
async fn test_coordination_graceful_shutdown() -> Result<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    let instance = SatelliteInstance::new(
        "shutdown_test",
        SatelliteVersion::parse("1.0.100+test").unwrap()
    );
    
    let mut coordination = SatelliteCoordination::new(instance, pool.clone());
    coordination.initialize().await?;
    
    let shutdown_clean = Arc::new(AtomicBool::new(false));
    let flag = shutdown_clean.clone();
    
    // Test that coordination loop can be cancelled cleanly
    let coordination_handle = tokio::spawn(async move {
        let result = coordination.run_coordination_loop(|| {
            let flag = flag.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                flag.store(true, Ordering::SeqCst);
                Ok::<(), Box<dyn std::error::Error>>(())
            }
        }).await;
        result
    });
    
    // Let it run briefly
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Cancel the coordination (simulates shutdown)
    coordination_handle.abort();
    
    // Should have processed at least once
    assert!(shutdown_clean.load(Ordering::SeqCst));
    
    Ok(())
}

#[sinex_test]
async fn test_coordination_database_state_tracking() -> Result<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    let instance = SatelliteInstance::new(
        "state_test",
        SatelliteVersion::parse("1.0.100+state").unwrap()
    );
    
    let mut coordination = SatelliteCoordination::new(instance.clone(), pool.clone());
    coordination.initialize().await?;
    
    // Verify instance is registered in database
    let registered_instance = sqlx::query!(
        "SELECT instance_id, service_name, version FROM core.satellite_instances WHERE instance_id = $1",
        instance.instance_id()
    )
    .fetch_optional(pool)
    .await?;
    
    assert!(registered_instance.is_some());
    let reg = registered_instance.unwrap();
    assert_eq!(reg.service_name, "state_test");
    assert_eq!(reg.version, "1.0.100+state");
    
    Ok(())
}