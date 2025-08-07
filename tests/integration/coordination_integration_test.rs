//! Integration tests for satellite coordination system
//! 
//! Tests the complete coordination system including:
//! - Leadership election
//! - Graceful handoff
//! - Hot standby pattern
//! - Advisory lock cleanup

use sinex_core_utils::{CoordinationPrimitive, ResourceGuard};
use sinex_db::distributed_locking::{AdvisoryLock, DistributedCoordination};
use sinex_satellite_sdk::coordination::{SatelliteCoordination, InstanceMode};
use sinex_satellite_sdk::version::{SatelliteVersion, SatelliteInstance};
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn test_coordination_primitive_unified_api(ctx: TestContext) -> color_eyre::Result<()> {
    // Test event counter factory method
    let counter = CoordinationPrimitive::event_counter(100, "test_counter");
    assert_eq!(counter.current_value(), 0);
    
    counter.add(50);
    assert_eq!(counter.current_value(), 50);
    
    // Test if threshold is reached
    counter.add(50);
    assert!(counter.is_complete());
    
    // Test reset and get previous
    let previous = counter.reset_and_get_previous();
    assert_eq!(previous, 100);
    assert_eq!(counter.current_value(), 0);
    
    Ok(())
}

#[sinex_test]
async fn test_coordination_primitive_barrier(ctx: TestContext) -> color_eyre::Result<()> {
    // Test barrier factory method  
    let barrier = CoordinationPrimitive::barrier(3, "worker_sync");
    assert_eq!(barrier.current_value(), 0);
    assert!(!barrier.is_complete());
    
    barrier.add(1); // Worker 1 ready
    barrier.add(1); // Worker 2 ready  
    assert!(!barrier.is_complete());
    
    barrier.add(1); // Worker 3 ready
    assert!(barrier.is_complete());
    
    Ok(())
}

#[sinex_test]
async fn test_coordination_primitive_synchronizer(ctx: TestContext) -> color_eyre::Result<()> {
    // Test synchronizer factory method
    let sync = CoordinationPrimitive::synchronizer("service_ready");
    assert!(!sync.is_complete());
    
    sync.signal(); // Service reports ready
    assert!(sync.is_complete());
    
    Ok(())
}

#[sinex_test]
async fn test_resource_guard_cleanup(ctx: TestContext) -> color_eyre::Result<()> {
    let cleanup_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleanup_called_clone = cleanup_called.clone();
    
    {
        let _guard = ResourceGuard::new("test_resource", move |_resource| async move {
            cleanup_called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        
        // Resource is held here
        assert!(!cleanup_called.load(std::sync::atomic::Ordering::SeqCst));
    } // guard drops here, should trigger cleanup
    
    // Small delay to allow async cleanup to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    
    assert!(cleanup_called.load(std::sync::atomic::Ordering::SeqCst));
    
    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_basic(ctx: TestContext) -> color_eyre::Result<()> {
    let pool = ctx.db_pool();
    
    // Test basic lock acquisition
    let lock1 = AdvisoryLock::try_acquire(pool, "test_lock_1").await?;
    assert!(lock1.is_some());
    
    // Test that same lock cannot be acquired again
    let lock2 = AdvisoryLock::try_acquire(pool, "test_lock_1").await?;
    assert!(lock2.is_none());
    
    // Release first lock
    drop(lock1);
    
    // Now second lock should succeed
    let lock3 = AdvisoryLock::try_acquire(pool, "test_lock_1").await?;
    assert!(lock3.is_some());
    
    Ok(())
}

#[sinex_test]
async fn test_satellite_version_comparison(ctx: TestContext) -> color_eyre::Result<()> {
    let v1 = SatelliteVersion::parse("1.0.100+abc123").unwrap();
    let v2 = SatelliteVersion::parse("1.0.200+def456").unwrap();
    let v3 = SatelliteVersion::parse("1.1.50+ghi789").unwrap();
    
    // Test version ordering (higher commit count = newer)
    assert!(v2 > v1);
    assert!(v3 > v2); // Minor version bump
    assert!(v3 > v1);
    
    Ok(())
}

#[sinex_test]
async fn test_satellite_instance_creation(ctx: TestContext) -> color_eyre::Result<()> {
    let instance = SatelliteInstance::new(
        "test-service",
        SatelliteVersion::parse("1.0.100+abc123").unwrap()
    );
    
    assert_eq!(instance.service_name(), "test-service");
    assert_eq!(instance.version().to_string(), "1.0.100+abc123");
    assert!(!instance.instance_id().to_string().is_empty());
    
    Ok(())
}

#[sinex_test] 
async fn test_coordination_tables_exist(ctx: TestContext) -> color_eyre::Result<()> {
    // Test that coordination tables were created by migration
    let tables = ctx.query_raw(
        "SELECT table_name FROM information_schema.tables 
         WHERE table_schema = 'core' 
         AND table_name IN ('satellite_instances', 'satellite_signals', 'service_leadership')
         ORDER BY table_name"
    ).await?;
    
    assert_eq!(tables.len(), 3);
    assert_eq!(tables[0]["table_name"], "satellite_instances");
    assert_eq!(tables[1]["table_name"], "satellite_signals"); 
    assert_eq!(tables[2]["table_name"], "service_leadership");
    
    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_leadership(ctx: TestContext) -> color_eyre::Result<()> {
    let pool = ctx.db_pool();
    
    // Create coordination instances
    let instance1 = SatelliteInstance::new(
        "test-service",
        SatelliteVersion::parse("1.0.100+abc123").unwrap()
    );
    
    let instance2 = SatelliteInstance::new(
        "test-service", 
        SatelliteVersion::parse("1.0.200+def456").unwrap() // Newer version
    );
    
    let mut coord1 = DistributedCoordination::new(instance1, pool.clone());
    let mut coord2 = DistributedCoordination::new(instance2, pool.clone());
    
    // Register both instances
    coord1.register_instance().await?;
    coord2.register_instance().await?;
    
    // Test leadership acquisition - newer version should win
    let leadership1 = coord1.try_acquire_leadership().await?;
    let leadership2 = coord2.try_acquire_leadership().await?;
    
    // Instance2 has newer version, should get leadership
    assert!(leadership1.is_none());
    assert!(leadership2.is_some());
    
    Ok(())
}

#[sinex_test]
async fn test_satellite_coordination_basic(ctx: TestContext) -> color_eyre::Result<()> {
    let pool = ctx.db_pool();
    
    let instance = SatelliteInstance::new(
        "test-service",
        SatelliteVersion::parse("1.0.100+abc123").unwrap()
    );
    
    let mut coordination = SatelliteCoordination::new(instance, pool.clone());
    
    // Test initialization
    coordination.initialize().await?;
    
    // Test mode checking
    assert_eq!(coordination.current_mode(), &InstanceMode::Standby);
    
    Ok(())
}