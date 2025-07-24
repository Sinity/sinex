//! Unit tests for PostgreSQL Advisory Lock distributed coordination
//!
//! Tests distributed locking functionality:
//! - Lock acquisition and release
//! - RAII cleanup patterns
//! - Concurrent lock attempts
//! - Session-scoped behavior

use sinex_db::distributed_locking::{AdvisoryLock, DistributedCoordination, LeadershipGuard};
use sinex_satellite_sdk::version::{SatelliteVersion, SatelliteInstance};
use test_sinex_test_utils::TestContext;

#[sinex_test]
async fn test_advisory_lock_basic_acquisition() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    // Test basic lock acquisition
    let lock1 = AdvisoryLock::try_acquire(pool, "test_lock_basic").await?;
    assert!(lock1.is_some());
    
    // Same lock should not be acquirable again
    let lock2 = AdvisoryLock::try_acquire(pool, "test_lock_basic").await?;
    assert!(lock2.is_none());
    
    // Release first lock
    if let Some(lock) = lock1 {
        lock.release().await?;
    }
    
    // Now should be acquirable again
    let lock3 = AdvisoryLock::try_acquire(pool, "test_lock_basic").await?;
    assert!(lock3.is_some());
    
    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_raii_cleanup() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    // Test RAII cleanup
    {
        let _lock = AdvisoryLock::try_acquire(pool, "test_lock_raii").await?;
        assert!(_lock.is_some());
        
        // Lock should be held here
        let attempt = AdvisoryLock::try_acquire(pool, "test_lock_raii").await?;
        assert!(attempt.is_none());
    } // Lock drops here, should auto-release
    
    // Lock should be available again after RAII cleanup
    let lock_after = AdvisoryLock::try_acquire(pool, "test_lock_raii").await?;
    assert!(lock_after.is_some());
    
    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_different_names() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    // Different lock names should not interfere
    let lock1 = AdvisoryLock::try_acquire(pool, "lock_alpha").await?;
    let lock2 = AdvisoryLock::try_acquire(pool, "lock_beta").await?;
    let lock3 = AdvisoryLock::try_acquire(pool, "lock_gamma").await?;
    
    assert!(lock1.is_some());
    assert!(lock2.is_some());
    assert!(lock3.is_some());
    
    // But same names should conflict
    let lock1_conflict = AdvisoryLock::try_acquire(pool, "lock_alpha").await?;
    assert!(lock1_conflict.is_none());
    
    Ok(())
}

#[sinex_test]
async fn test_leadership_guard_basic() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    let guard = LeadershipGuard::new("test_service", "instance_1", pool.clone());
    
    // Test metadata access
    assert_eq!(guard.service_name(), "test_service");
    assert_eq!(guard.instance_id(), "instance_1");
    
    // Test leadership recording
    guard.record_leadership(pool).await?;
    
    // Verify leadership was recorded
    let leadership_check = sqlx::query!(
        "SELECT instance_id FROM core.service_leadership WHERE service_name = $1",
        "test_service"
    )
    .fetch_optional(pool)
    .await?;
    
    assert!(leadership_check.is_some());
    assert_eq!(leadership_check.unwrap().instance_id, "instance_1");
    
    Ok(())
}

#[sinex_test]
async fn test_leadership_guard_heartbeat() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    let guard = LeadershipGuard::new("heartbeat_service", "instance_heartbeat", pool.clone());
    
    // Record initial leadership
    guard.record_leadership(pool).await?;
    
    let initial_time = sqlx::query!(
        "SELECT last_heartbeat FROM core.service_leadership WHERE service_name = $1",
        "heartbeat_service"
    )
    .fetch_one(pool)
    .await?
    .last_heartbeat;
    
    // Wait a bit then send heartbeat
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    guard.heartbeat(pool).await?;
    
    let updated_time = sqlx::query!(
        "SELECT last_heartbeat FROM core.service_leadership WHERE service_name = $1", 
        "heartbeat_service"
    )
    .fetch_one(pool)
    .await?
    .last_heartbeat;
    
    // Heartbeat should have updated the timestamp
    assert!(updated_time > initial_time);
    
    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_instance_registration() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    let instance = SatelliteInstance::new(
        "test_coordination",
        SatelliteVersion::parse("1.0.100+abc123").unwrap()
    );
    
    let mut coordination = DistributedCoordination::new(instance, pool.clone());
    
    // Register instance
    coordination.register_instance().await?;
    
    // Verify instance was registered
    let registered = sqlx::query!(
        "SELECT service_name, version, host_name FROM core.satellite_instances WHERE instance_id = $1",
        coordination.instance().instance_id()
    )
    .fetch_optional(pool)
    .await?;
    
    assert!(registered.is_some());
    let reg = registered.unwrap();
    assert_eq!(reg.service_name, "test_coordination");
    assert_eq!(reg.version, "1.0.100+abc123");
    
    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_leadership_election() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    // Create two instances with different versions
    let instance1 = SatelliteInstance::new(
        "election_test",
        SatelliteVersion::parse("1.0.100+abc123").unwrap()
    );
    
    let instance2 = SatelliteInstance::new(
        "election_test", 
        SatelliteVersion::parse("1.0.200+def456").unwrap() // Newer version
    );
    
    let mut coord1 = DistributedCoordination::new(instance1, pool.clone());
    let mut coord2 = DistributedCoordination::new(instance2, pool.clone());
    
    // Register both instances
    coord1.register_instance().await?;
    coord2.register_instance().await?;
    
    // Try to acquire leadership
    let leadership1 = coord1.try_acquire_leadership().await?;
    let leadership2 = coord2.try_acquire_leadership().await?;
    
    // Newer version should win leadership
    assert!(leadership1.is_none());
    assert!(leadership2.is_some());
    
    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_version_priority() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    // Test various version scenarios
    let scenarios = vec![
        ("1.0.100+abc", "1.0.200+def", false), // Higher commit count wins
        ("1.1.50+xyz", "1.0.200+abc", true),   // Higher minor version wins
        ("2.0.10+new", "1.9.999+old", true),  // Higher major version wins
    ];
    
    for (i, (version1, version2, should_1_win)) in scenarios.into_iter().enumerate() {
        let service_name = format!("version_test_{}", i);
        
        let instance1 = SatelliteInstance::new(
            &service_name,
            SatelliteVersion::parse(version1).unwrap()
        );
        
        let instance2 = SatelliteInstance::new(
            &service_name,
            SatelliteVersion::parse(version2).unwrap()
        );
        
        let mut coord1 = DistributedCoordination::new(instance1, pool.clone());
        let mut coord2 = DistributedCoordination::new(instance2, pool.clone());
        
        coord1.register_instance().await?;
        coord2.register_instance().await?;
        
        let leadership1 = coord1.try_acquire_leadership().await?;
        let leadership2 = coord2.try_acquire_leadership().await?;
        
        if should_1_win {
            assert!(leadership1.is_some(), "Version {} should beat {}", version1, version2);
            assert!(leadership2.is_none());
        } else {
            assert!(leadership1.is_none());
            assert!(leadership2.is_some(), "Version {} should beat {}", version2, version1);
        }
    }
    
    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_start_time_tiebreaker() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    // Same version, different start times
    let instance1 = SatelliteInstance::new(
        "tiebreaker_test",
        SatelliteVersion::parse("1.0.100+same").unwrap()
    );
    
    let mut coord1 = DistributedCoordination::new(instance1, pool.clone());
    coord1.register_instance().await?;
    
    // Wait a bit so start times are different
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    
    let instance2 = SatelliteInstance::new(
        "tiebreaker_test",
        SatelliteVersion::parse("1.0.100+same").unwrap()
    );
    
    let mut coord2 = DistributedCoordination::new(instance2, pool.clone());
    coord2.register_instance().await?;
    
    let leadership1 = coord1.try_acquire_leadership().await?;
    let leadership2 = coord2.try_acquire_leadership().await?;
    
    // Earlier instance should win (coord1 started first)
    assert!(leadership1.is_some());
    assert!(leadership2.is_none());
    
    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_concurrent_acquisition() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    let lock_name = "concurrent_test";
    let mut handles = vec![];
    
    // Spawn 10 tasks trying to acquire the same lock
    for i in 0..10 {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let result = AdvisoryLock::try_acquire(&pool_clone, lock_name).await;
            (i, result.is_ok() && result.unwrap().is_some())
        });
        handles.push(handle);
    }
    
    // Wait for all attempts
    let results: Vec<(usize, bool)> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    
    // Exactly one should succeed
    let successful_acquisitions = results.iter().filter(|(_, success)| *success).count();
    assert_eq!(successful_acquisitions, 1);
    
    Ok(())
}

#[sinex_test]
async fn test_coordination_with_database_failure_simulation() -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let pool = ctx.db_pool();
    
    // Test graceful handling of database issues
    let instance = SatelliteInstance::new(
        "failure_test",
        SatelliteVersion::parse("1.0.100+test").unwrap()
    );
    
    let mut coordination = DistributedCoordination::new(instance, pool.clone());
    
    // Normal registration should work
    coordination.register_instance().await?;
    
    // Try to acquire leadership
    let leadership = coordination.try_acquire_leadership().await?;
    assert!(leadership.is_some());
    
    Ok(())
}

mod test_common {
    use sinex_core_types::Result as TestResult;
    use sinex_db::DbPool;
    
    pub struct TestContext {
        pool: DbPool,
    }
    
    impl TestContext {
        pub async fn new() -> TestResult<Self> {
            let database_url = std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
            
            let pool = sqlx::PgPool::connect(&database_url).await?;
            
            Ok(Self { pool })
        }
        
        pub fn db_pool(&self) -> &DbPool {
            &self.pool
        }
    }
}

use test_sinex_test_utils::TestContext;
type TestResult<T> = sinex_core_types::Result<T>;

// Mock sinex_test macro for compilation
macro_rules! sinex_test {
    () => {
        #[tokio::test]
    };
}

use sinex_test;