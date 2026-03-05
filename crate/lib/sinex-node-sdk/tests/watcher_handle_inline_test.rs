#![cfg(feature = "messaging")]

use sinex_node_sdk::WatcherHandle;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{Duration, sleep};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_watcher_state_transitions() -> Result<(), Box<dyn std::error::Error>> {
    let mut handle = WatcherHandle::<()>::initialized("test");
    assert!(!handle.is_active());

    let task = tokio::spawn(async {
        sleep(Duration::from_secs(10)).await;
    });
    handle.start(task, None)?;
    assert!(handle.is_active());

    let was_active = handle.is_active();
    assert!(was_active);
    handle.shutdown().await;
    Ok(())
}

#[sinex_test]
async fn test_watcher_running_constructor() -> Result<(), Box<dyn std::error::Error>> {
    let task = tokio::spawn(async {
        sleep(Duration::from_secs(10)).await;
    });
    let handle = WatcherHandle::<()>::running("test", task, None, None);
    assert!(handle.is_active());
    let health = handle.health();
    assert!(health.active);
    Ok(())
}

#[sinex_test]
async fn test_watcher_health_tracking() -> Result<(), Box<dyn std::error::Error>> {
    let handle = WatcherHandle::<()>::initialized("test");

    let health = handle.health();
    assert!(!health.active);
    assert_eq!(health.events_processed, 0);

    handle.record_success();
    let health = handle.health();
    assert_eq!(health.events_processed, 1);
    assert!(health.last_success.is_some());

    handle.record_error("test error".to_string());
    let health = handle.health();
    assert_eq!(health.last_error, Some("test error".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_watcher_shutdown_aborts_task() -> Result<(), Box<dyn std::error::Error>> {
    let flag = Arc::new(AtomicBool::new(false));
    let flag_clone = Arc::clone(&flag);

    let task = tokio::spawn(async move {
        sleep(Duration::from_secs(10)).await;
        flag_clone.store(true, Ordering::SeqCst);
    });

    let mut handle = WatcherHandle::<()>::initialized("test");
    handle.start(task, None)?;

    handle.shutdown().await;
    sleep(Duration::from_millis(100)).await;

    assert!(!flag.load(Ordering::SeqCst));
    Ok(())
}

#[sinex_test]
async fn test_watcher_with_material() -> Result<(), Box<dyn std::error::Error>> {
    let material = "test_context";
    let mut handle = WatcherHandle::initialized("test").with_material(material);

    let task = tokio::spawn(async {});
    handle.start(task, None)?;

    let extracted = handle.take_material();
    assert_eq!(extracted, Some("test_context"));
    assert!(handle.take_material().is_none());
    handle.shutdown().await;
    Ok(())
}

#[sinex_test]
async fn test_watcher_with_forwarder() -> Result<(), Box<dyn std::error::Error>> {
    let main_task = tokio::spawn(async {
        sleep(Duration::from_secs(10)).await;
    });
    let forwarder_task = tokio::spawn(async {
        sleep(Duration::from_secs(10)).await;
    });

    let mut handle = WatcherHandle::<()>::initialized("test");
    handle.start(main_task, Some(forwarder_task))?;
    assert!(handle.is_active());

    handle.shutdown().await;
    Ok(())
}
