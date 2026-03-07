//! Tests for `LifecycleManager` status transitions and service management
//!
//! Tests lifecycle state transitions, shutdown handling, and health check integration.

use sinex_node_sdk::lifecycle::{LifecycleManager, ServiceStatus};
use xtask::sandbox::sinex_test;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use xtask::sandbox::timing::Timeouts;

#[sinex_test]
async fn lifecycle_manager_starts_in_starting_status() -> TestResult<()> {
    let manager = LifecycleManager::new("test-service".to_string());
    assert_eq!(manager.status(), ServiceStatus::Starting);
    Ok(())
}

#[sinex_test]
async fn status_transitions_are_recorded() -> TestResult<()> {
    let manager = LifecycleManager::new("test-service".to_string());

    manager.set_status(ServiceStatus::Running);
    assert_eq!(manager.status(), ServiceStatus::Running);

    manager.set_status(ServiceStatus::Stopping);
    assert_eq!(manager.status(), ServiceStatus::Stopping);

    manager.set_status(ServiceStatus::Stopped);
    assert_eq!(manager.status(), ServiceStatus::Stopped);

    Ok(())
}

#[sinex_test]
async fn failed_status_can_be_set() -> TestResult<()> {
    let manager = LifecycleManager::new("test-service".to_string());

    manager.set_status(ServiceStatus::Running);
    manager.set_status(ServiceStatus::Failed);
    assert_eq!(manager.status(), ServiceStatus::Failed);

    Ok(())
}

#[sinex_test]
async fn shutdown_flag_initially_false() -> TestResult<()> {
    let manager = LifecycleManager::new("test-service".to_string());
    assert!(!manager.is_shutdown_requested());
    Ok(())
}

#[sinex_test]
async fn health_check_interval_can_be_configured() -> TestResult<()> {
    let manager = LifecycleManager::new("test-service".to_string())
        .with_health_check_interval(std::time::Duration::from_secs(Timeouts::QUICK));

    // Manager should be created successfully with custom interval
    assert_eq!(manager.status(), ServiceStatus::Starting);
    Ok(())
}

#[sinex_test]
async fn heartbeat_interval_can_be_configured() -> TestResult<()> {
    use sinex_primitives::Seconds;

    let manager =
        LifecycleManager::new("test-service".to_string()).with_heartbeat(Seconds::from_secs(10));

    // Manager should be created successfully with custom heartbeat
    assert_eq!(manager.status(), ServiceStatus::Starting);
    Ok(())
}

#[sinex_test]
async fn service_status_display_formats_correctly() -> TestResult<()> {
    assert_eq!(ServiceStatus::Starting.to_string(), "starting");
    assert_eq!(ServiceStatus::Running.to_string(), "running");
    assert_eq!(ServiceStatus::Stopping.to_string(), "stopping");
    assert_eq!(ServiceStatus::Stopped.to_string(), "stopped");
    assert_eq!(ServiceStatus::Failed.to_string(), "failed");
    Ok(())
}

#[sinex_test]
async fn concurrent_status_updates_are_safe() -> TestResult<()> {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let manager = Arc::new(LifecycleManager::new("test-service".to_string()));
    let barrier = Arc::new(Barrier::new(5));

    let statuses = [
        ServiceStatus::Running,
        ServiceStatus::Stopping,
        ServiceStatus::Running,
        ServiceStatus::Failed,
        ServiceStatus::Stopped,
    ];

    let mut handles = Vec::new();

    for status in statuses {
        let manager_clone = manager.clone();
        let barrier_clone = barrier.clone();

        handles.push(tokio::spawn(async move {
            barrier_clone.wait().await;
            manager_clone.set_status(status);
        }));
    }

    for handle in handles {
        handle.await?;
    }

    // Status should be one of the set values (last write wins, but any is valid)
    let final_status = manager.status();
    assert!(
        matches!(
            final_status,
            ServiceStatus::Running
                | ServiceStatus::Stopping
                | ServiceStatus::Failed
                | ServiceStatus::Stopped
        ),
        "Final status should be a valid state: {final_status:?}"
    );

    Ok(())
}

#[sinex_test]
async fn run_with_health_check_sets_running_status() -> TestResult<()> {
    let mut manager = LifecycleManager::new("test-service".to_string())
        .with_health_check_interval(std::time::Duration::from_millis(50));

    // Initialize lifecycle management (sets up signal handlers)
    manager.initialize()?;

    let health_check_count = Arc::new(AtomicUsize::new(0));
    let health_check_count_clone = health_check_count.clone();

    // Run with a health check that counts invocations
    let result = tokio::time::timeout(std::time::Duration::from_millis(800), async {
        manager
            .run_with_health_check(
                || async {
                    tokio::time::sleep(std::time::Duration::from_millis(220)).await;
                    Ok(())
                },
                move || {
                    let count = health_check_count_clone.clone();
                    async move {
                        count.fetch_add(1, Ordering::Relaxed);
                        true // Always healthy
                    }
                },
            )
            .await
    })
    .await;

    let inner_result = result.expect("main task should complete within timeout");
    inner_result?;
    assert!(
        health_check_count.load(Ordering::Relaxed) > 0,
        "health check should execute at least once while the service is running"
    );
    assert_eq!(
        manager.status(),
        ServiceStatus::Stopped,
        "service should transition to stopped after successful run"
    );

    Ok(())
}

#[sinex_test]
async fn shutdown_sets_shutdown_flag_and_terminal_status() -> TestResult<()> {
    let mut manager = LifecycleManager::new("test-service".to_string())
        .with_shutdown_grace_period(std::time::Duration::from_millis(10));
    manager.initialize()?;
    assert!(!manager.is_shutdown_requested());
    assert_eq!(manager.status(), ServiceStatus::Starting);

    manager.shutdown().await?;
    assert!(manager.is_shutdown_requested());
    assert_eq!(manager.status(), ServiceStatus::Stopped);

    Ok(())
}

#[sinex_test]
async fn initialization_can_be_called_multiple_times() -> TestResult<()> {
    let mut manager = LifecycleManager::new("test-service".to_string());

    // First initialization
    manager.initialize()?;

    // Second initialization should also succeed
    // (though typically you wouldn't do this in practice)
    let mut manager2 = LifecycleManager::new("test-service-2".to_string());
    manager2.initialize()?;

    Ok(())
}

#[sinex_test]
async fn status_equality_comparison_works() -> TestResult<()> {
    assert_eq!(ServiceStatus::Starting, ServiceStatus::Starting);
    assert_eq!(ServiceStatus::Running, ServiceStatus::Running);
    assert_eq!(ServiceStatus::Stopping, ServiceStatus::Stopping);
    assert_eq!(ServiceStatus::Stopped, ServiceStatus::Stopped);
    assert_eq!(ServiceStatus::Failed, ServiceStatus::Failed);

    assert_ne!(ServiceStatus::Starting, ServiceStatus::Running);
    assert_ne!(ServiceStatus::Running, ServiceStatus::Stopped);

    Ok(())
}

#[sinex_test]
async fn heartbeat_handle_is_none_without_runtime() -> TestResult<()> {
    let manager = LifecycleManager::new("test-service".to_string());

    // Without hydrating from a runtime, heartbeat handle should be None
    let handle = manager.get_heartbeat_handle();
    assert!(handle.is_none());

    Ok(())
}
