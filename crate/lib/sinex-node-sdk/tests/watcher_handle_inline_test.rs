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
    let tracker = handle.health_tracker();

    let was_active = handle.is_active();
    assert!(was_active);
    handle.shutdown().await?;
    assert!(!tracker.read().active);
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
    let tracker = handle.health_tracker();
    handle.shutdown().await?;
    assert!(!tracker.read().active);
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
    let tracker = handle.health_tracker();

    handle.shutdown().await?;
    sleep(Duration::from_millis(100)).await;

    assert!(!flag.load(Ordering::SeqCst));
    assert!(!tracker.read().active);
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
    handle.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn test_watcher_with_forwarder() -> Result<(), Box<dyn std::error::Error>> {
    let main_flag = Arc::new(AtomicBool::new(false));
    let main_flag_clone = Arc::clone(&main_flag);
    let main_task = tokio::spawn(async move {
        sleep(Duration::from_secs(10)).await;
        main_flag_clone.store(true, Ordering::SeqCst);
    });

    let forwarder_flag = Arc::new(AtomicBool::new(false));
    let forwarder_flag_clone = Arc::clone(&forwarder_flag);
    let forwarder_task = tokio::spawn(async move {
        sleep(Duration::from_secs(10)).await;
        forwarder_flag_clone.store(true, Ordering::SeqCst);
    });

    let mut handle = WatcherHandle::<()>::initialized("test");
    handle.start(main_task, Some(forwarder_task))?;
    assert!(handle.is_active());
    let tracker = handle.health_tracker();

    handle.shutdown().await?;
    sleep(Duration::from_millis(100)).await;
    assert!(!main_flag.load(Ordering::SeqCst));
    assert!(!forwarder_flag.load(Ordering::SeqCst));
    assert!(!tracker.read().active);
    Ok(())
}

#[sinex_test]
async fn test_watcher_is_inactive_when_forwarder_finishes(
) -> Result<(), Box<dyn std::error::Error>> {
    let main_task = tokio::spawn(async {
        sleep(Duration::from_secs(10)).await;
    });
    let forwarder = tokio::spawn(async {});

    let mut handle = WatcherHandle::<()>::initialized("test");
    handle.start(main_task, Some(forwarder))?;
    tokio::task::yield_now().await;

    assert!(
        !handle.is_active(),
        "completed forwarders must make the watcher inactive so supervisors can restart it"
    );

    handle.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn test_watcher_shutdown_rejects_panicked_task(
) -> Result<(), Box<dyn std::error::Error>> {
    let task = tokio::spawn(async {
        panic!("watcher panic");
    });
    tokio::task::yield_now().await;

    let mut handle = WatcherHandle::<()>::initialized("test");
    handle.start(task, None)?;

    let error = handle
        .shutdown()
        .await
        .expect_err("panicked watcher tasks must fail shutdown honestly");
    let message = format!("{error:#}");
    assert!(message.contains("Watcher task failed during shutdown"));
    assert!(message.contains("watcher task"));
    assert!(message.contains("test"));
    Ok(())
}

#[sinex_test]
async fn test_watcher_shutdown_rejects_panicked_forwarder(
) -> Result<(), Box<dyn std::error::Error>> {
    let task = tokio::spawn(async {});
    let forwarder = tokio::spawn(async {
        panic!("forwarder panic");
    });
    tokio::task::yield_now().await;

    let mut handle = WatcherHandle::<()>::initialized("test");
    handle.start(task, Some(forwarder))?;

    let error = handle
        .shutdown()
        .await
        .expect_err("panicked watcher forwarders must fail shutdown honestly");
    let message = format!("{error:#}");
    assert!(message.contains("Watcher task failed during shutdown"));
    assert!(message.contains("watcher forwarder"));
    assert!(message.contains("test"));
    Ok(())
}
