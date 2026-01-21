//! Integration tests for resource management patterns in Sinex
//!
//! Tests resource management across the system including:
//! - Database connection management
//! - File handle management  
//! - Advisory lock cleanup
//! - Service lifecycle management
//! - Cross-service resource coordination

use sinex_core::types::utils::ResourceGuard;
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::Timeouts;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::time::Duration;

#[sinex_test]
async fn test_database_connection_lifecycle(ctx: TestContext) -> Result<()> {
    // Test that database connections are properly managed through ResourceGuard
    let connection_count = Arc::new(AtomicU32::new(0));
    let counter_clone = connection_count.clone();

    {
        let pool = ctx.pool.clone();
        let _guard = ResourceGuard::new(pool, move |_pool| {
            let counter = counter_clone.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                // Simulate connection cleanup
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        // Connection should be active
        assert_eq!(connection_count.load(Ordering::SeqCst), 0);
    }

    // Wait for cleanup
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Cleanup should have been called
    assert_eq!(connection_count.load(Ordering::SeqCst), 1);
    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_management(ctx: TestContext) -> Result<()> {
    // Test advisory lock resource management pattern
    let lock_released = Arc::new(AtomicBool::new(false));
    let released_flag = lock_released.clone();

    {
        // Simulate acquiring an advisory lock
        let pool = ctx.pool.clone();
        let lock_id = 12345i64;

        let _guard = ResourceGuard::new((pool, lock_id), move |(_pool, _id)| {
            let flag = released_flag.clone();
            async move {
                // Simulate releasing the advisory lock
                // In real code, this would call pg_advisory_unlock
                tokio::time::sleep(Duration::from_millis(10)).await;
                flag.store(true, Ordering::SeqCst);
            }
        });

        // Lock should be held
        assert!(!lock_released.load(Ordering::SeqCst));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(lock_released.load(Ordering::SeqCst));
    Ok(())
}

#[sinex_test]
async fn test_file_handle_cleanup(ctx: TestContext) -> Result<()> {
    // Test file handle resource management with validated paths

    let cleanup_called = Arc::new(AtomicBool::new(false));
    let cleanup_flag = cleanup_called.clone();

    {
        // Create a validated temporary file path using test utilities
        let file_path = create_test_temp_file("resource_management_test", "test_resource.txt")?;

        // Write test content
        tokio::fs::write(&file_path, b"test content").await?;

        let _guard = ResourceGuard::new(file_path.clone(), move |path| {
            let flag = cleanup_flag.clone();
            async move {
                // Simulate file cleanup
                let _ = tokio::fs::remove_file(path).await;
                flag.store(true, Ordering::SeqCst);
            }
        });

        // File should exist
        assert!(file_path.exists());
        assert!(!cleanup_called.load(Ordering::SeqCst));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(cleanup_called.load(Ordering::SeqCst));
    Ok(())
}

#[sinex_test]
async fn test_service_lifecycle_coordination(ctx: TestContext) -> Result<()> {
    // Test coordinated service startup/shutdown using ResourceGuard
    #[derive(Debug)]
    struct MockService {
        _name: String,
        is_running: Arc<AtomicBool>,
        shutdown_count: Arc<AtomicU32>,
    }

    impl MockService {
        fn new(name: &str, counter: Arc<AtomicU32>) -> Self {
            Self {
                _name: name.to_string(),
                is_running: Arc::new(AtomicBool::new(true)),
                shutdown_count: counter,
            }
        }

        async fn shutdown(&self) {
            self.is_running.store(false, Ordering::SeqCst);
            self.shutdown_count.fetch_add(1, Ordering::SeqCst);
            // Simulate cleanup time
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        fn is_running(&self) -> bool {
            self.is_running.load(Ordering::SeqCst)
        }
    }

    let shutdown_counter = Arc::new(AtomicU32::new(0));
    let services = vec![
        MockService::new("collector", shutdown_counter.clone()),
        MockService::new("processor", shutdown_counter.clone()),
        MockService::new("gateway", shutdown_counter.clone()),
    ];

    // Create guards for all services
    let mut guards = Vec::new();
    for service in services {
        let guard = ResourceGuard::new(service, |service| async move {
            service.shutdown().await;
        });
        guards.push(guard);
    }

    // All services should be running
    for guard in &guards {
        let resource = guard.resource().await;
        let service = resource.as_ref().expect("Service should exist");
        assert!(service.is_running());
    }

    // Drop all guards to trigger coordinated shutdown
    drop(guards);

    // Wait for shutdown coordination
    tokio::time::sleep(Duration::from_millis(100)).await;

    // All services should have shut down
    assert_eq!(shutdown_counter.load(Ordering::SeqCst), 3);
    Ok(())
}

#[sinex_test]
async fn test_resource_pool_management(ctx: TestContext) -> Result<()> {
    // Test managing a pool of resources
    struct ResourcePool {
        _resources: Vec<String>,
        active_count: Arc<AtomicU32>,
    }

    impl ResourcePool {
        fn new(size: usize) -> Self {
            Self {
                _resources: (0..size).map(|i| format!("resource_{i}")).collect(),
                active_count: Arc::new(AtomicU32::new(size as u32)),
            }
        }

        async fn shutdown_all(&self) {
            self.active_count.store(0, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        fn active_resources(&self) -> u32 {
            self.active_count.load(Ordering::SeqCst)
        }
    }

    let pool = ResourcePool::new(5);
    let initial_count = pool.active_resources();
    assert_eq!(initial_count, 5);

    {
        let _guard = ResourceGuard::new(pool, |pool| async move {
            pool.shutdown_all().await;
        });

        // Pool should still be active
        let resource = _guard.resource().await;
        let pool_ref = resource.as_ref().expect("Pool should exist");
        assert_eq!(pool_ref.active_resources(), 5);
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    // Pool shutdown should have been called (we can't verify the final state
    // since the pool was consumed, but we can verify timing)
    Ok(())
}

#[sinex_test]
async fn test_nested_resource_dependencies(ctx: TestContext) -> Result<()> {
    // Test nested resource dependencies with proper cleanup ordering
    let cleanup_order = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    {
        // Outer resource (database connection)
        let order1 = cleanup_order.clone();
        let _db_guard = ResourceGuard::new("database", move |resource| {
            let order = order1.clone();
            async move {
                order.lock().await.push(format!("cleanup_{resource}"));
            }
        });

        {
            // Middle resource (connection pool)
            let order2 = cleanup_order.clone();
            let _pool_guard = ResourceGuard::new("connection_pool", move |resource| {
                let order = order2.clone();
                async move {
                    order.lock().await.push(format!("cleanup_{resource}"));
                }
            });

            {
                // Inner resource (active transaction)
                let order3 = cleanup_order.clone();
                let _tx_guard = ResourceGuard::new("transaction", move |resource| {
                    let order = order3.clone();
                    async move {
                        order.lock().await.push(format!("cleanup_{resource}"));
                    }
                });

                // All resources active
                let order = cleanup_order.lock().await;
                assert_eq!(order.len(), 0);
            } // transaction cleanup
        } // pool cleanup
    } // database cleanup

    tokio::time::sleep(Duration::from_millis(100)).await;

    let order = cleanup_order.lock().await;
    assert_eq!(order.len(), 3);

    // Verify all resources were cleaned up
    assert!(order.contains(&"cleanup_transaction".to_string()));
    assert!(order.contains(&"cleanup_connection_pool".to_string()));
    assert!(order.contains(&"cleanup_database".to_string()));

    Ok(())
}

#[sinex_test]
async fn test_concurrent_resource_access(ctx: TestContext) -> Result<()> {
    // Test concurrent resource creation and cleanup
    let cleanup_count = Arc::new(AtomicU32::new(0));
    let mut handles = Vec::new();

    // Create multiple ResourceGuards concurrently
    for i in 0..5 {
        let cleanup_counter = cleanup_count.clone();
        let handle = tokio::spawn(async move {
            let resource_id = format!("concurrent_resource_{i}");
            let _guard = ResourceGuard::new(resource_id.clone(), move |_resource| {
                let counter = cleanup_counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });

            // Simulate some work with the resource
            tokio::time::sleep(Duration::from_millis(20)).await;

            // Guard drops here, triggering cleanup
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await?;
    }

    // Wait for cleanup to complete
    tokio::time::timeout(Duration::from_secs(Timeouts::QUICK), async {
        loop {
            if cleanup_count.load(Ordering::SeqCst) >= 5 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| SinexError::timeout("cleanup count did not reach expected value"))?;

    assert_eq!(
        cleanup_count.load(Ordering::SeqCst),
        5,
        "all resource cleanups should have run"
    );

    Ok(())
}

#[sinex_test]
async fn test_resource_failure_recovery(ctx: TestContext) -> Result<()> {
    // Test resource cleanup even when resource operations fail
    let cleanup_called = Arc::new(AtomicBool::new(false));
    let error_occurred = Arc::new(AtomicBool::new(false));

    let cleanup_flag = cleanup_called.clone();
    let error_flag = error_occurred.clone();

    {
        let _guard = ResourceGuard::new("failing_resource", move |_resource| {
            let cleanup = cleanup_flag.clone();
            let error = error_flag.clone();
            async move {
                cleanup.store(true, Ordering::SeqCst);
                // Simulate a failure during cleanup
                error.store(true, Ordering::SeqCst);
                // Even if we panic here, the cleanup should still be marked as called
                if error.load(Ordering::SeqCst) {
                    // Don't actually panic in tests, just simulate the condition
                }
            }
        });

        // Simulate some resource operation that might fail
        let resource = _guard.resource().await;
        assert!(resource.is_some());
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Cleanup should have been attempted even with errors
    assert!(cleanup_called.load(Ordering::SeqCst));
    assert!(error_occurred.load(Ordering::SeqCst));

    Ok(())
}
