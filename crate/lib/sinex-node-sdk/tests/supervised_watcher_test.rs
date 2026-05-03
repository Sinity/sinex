#![cfg(feature = "messaging")]

//! Integration tests for `spawn_supervised_watcher` and `spawn_watcher_with_panic_catch`.
//!
//! These tests verify the behavioral contract documented in `supervised_watcher.rs`:
//! - Clean `Ok(())` exits are handled correctly
//! - Errors are logged and recorded in the health tracker
//! - Panics are caught, logged, and recorded in the health tracker
//! - Restart-with-backoff loops exit on `shutdown_rx`
//! - `max_restarts` causes the supervisor to give up
//! - Pre-signaled shutdown prevents the factory from being called
//! - `spawn_watcher_with_panic_catch` (one-shot variant) covers the same
//!   error and panic paths without the restart machinery

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use parking_lot::RwLock;
use sinex_node_sdk::{
    SupervisedWatcherConfig, WatcherHealth, spawn_supervised_watcher,
    spawn_watcher_with_panic_catch,
};
use sinex_primitives::SinexError;
use xtask::sandbox::sinex_test;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_shutdown() -> (tokio::sync::watch::Sender<bool>, tokio::sync::watch::Receiver<bool>) {
    tokio::sync::watch::channel(false)
}

fn make_health() -> Arc<RwLock<WatcherHealth>> {
    Arc::new(RwLock::new(WatcherHealth::default()))
}

// ---------------------------------------------------------------------------
// spawn_watcher_with_panic_catch — one-shot variant
// ---------------------------------------------------------------------------

#[sinex_test]
async fn panic_catch_clean_exit_terminates_task() -> Result<(), Box<dyn std::error::Error>> {
    let handle = spawn_watcher_with_panic_catch("ok-watcher", None, async { Ok(()) });
    tokio::time::timeout(std::time::Duration::from_secs(5), handle).await??;
    Ok(())
}

#[sinex_test]
async fn panic_catch_error_updates_health_tracker() -> Result<(), Box<dyn std::error::Error>> {
    let health = make_health();
    let health_clone = Arc::clone(&health);

    let handle = spawn_watcher_with_panic_catch(
        "err-watcher",
        Some(health_clone),
        async { Err(SinexError::processing("injected error")) },
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), handle).await??;

    let last_error = health.read().last_error.clone();
    assert!(
        last_error.as_deref().unwrap_or("").contains("injected error"),
        "health tracker should record the error; got: {last_error:?}"
    );
    Ok(())
}

#[sinex_test]
async fn panic_catch_panic_updates_health_tracker() -> Result<(), Box<dyn std::error::Error>> {
    let health = make_health();
    let health_clone = Arc::clone(&health);

    let handle = spawn_watcher_with_panic_catch("panic-watcher", Some(health_clone), async {
        panic!("deliberate one-shot panic");
        #[allow(unreachable_code)]
        Ok::<(), SinexError>(())
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), handle).await??;

    let last_error = health.read().last_error.clone();
    assert!(
        last_error
            .as_deref()
            .unwrap_or("")
            .contains("deliberate one-shot panic"),
        "panic message should appear in health tracker; got: {last_error:?}"
    );
    assert!(
        last_error.as_deref().unwrap_or("").contains("panicked"),
        "health error should indicate a panic; got: {last_error:?}"
    );
    Ok(())
}

#[sinex_test]
async fn panic_catch_no_health_tracker_does_not_panic_supervisor(
) -> Result<(), Box<dyn std::error::Error>> {
    // Supervisor must not panic even when there is no health tracker and the watcher panics.
    let handle = spawn_watcher_with_panic_catch("no-health-panic", None, async {
        panic!("deliberate panic with no health tracker");
        #[allow(unreachable_code)]
        Ok::<(), SinexError>(())
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), handle).await??;
    Ok(())
}

// ---------------------------------------------------------------------------
// spawn_supervised_watcher — restart + shutdown
// ---------------------------------------------------------------------------

#[sinex_test]
async fn supervised_pre_signaled_shutdown_never_calls_factory(
) -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, shutdown_rx) = make_shutdown();
    let _ = shutdown_tx.send(true);

    let called = Arc::new(AtomicU32::new(0));
    let called_clone = Arc::clone(&called);

    let handle = spawn_supervised_watcher(
        "pre-shutdown",
        shutdown_rx,
        None,
        SupervisedWatcherConfig::default(),
        move || {
            called_clone.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        },
    );

    tokio::time::timeout(std::time::Duration::from_secs(5), handle).await??;
    assert_eq!(
        called.load(Ordering::SeqCst),
        0,
        "factory must not be called when shutdown is already signaled"
    );
    Ok(())
}

#[sinex_test]
async fn supervised_shutdown_during_backoff_exits_quickly(
) -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, shutdown_rx) = make_shutdown();

    let handle = spawn_supervised_watcher(
        "backoff-shutdown",
        shutdown_rx,
        None,
        SupervisedWatcherConfig::default(),
        || async { Err(SinexError::processing("transient error")) },
    );

    // Give the first error cycle a moment to start backoff.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let _ = shutdown_tx.send(true);

    tokio::time::timeout(std::time::Duration::from_secs(5), handle).await??;
    Ok(())
}

#[sinex_test]
async fn supervised_error_updates_health_tracker() -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, shutdown_rx) = make_shutdown();

    let health = make_health();
    let health_clone = Arc::clone(&health);

    let call_count = Arc::new(AtomicU32::new(0));
    let call_count_clone = Arc::clone(&call_count);

    let handle = spawn_supervised_watcher(
        "health-err",
        shutdown_rx,
        Some(health_clone),
        SupervisedWatcherConfig { restart_on_failure: true, max_restarts: 0 },
        move || {
            let count = Arc::clone(&call_count_clone);
            async move {
                let n = count.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    Err(SinexError::processing("test error"))
                } else {
                    Ok(())
                }
            }
        },
    );

    // Allow the first error + restart cycle.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let last_error = health.read().last_error.clone();
    assert!(
        last_error.as_deref().unwrap_or("").contains("test error"),
        "health tracker should record the error message; got: {last_error:?}"
    );

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(std::time::Duration::from_secs(5), handle).await??;
    Ok(())
}

#[sinex_test]
async fn supervised_panic_is_caught_and_health_updated() -> Result<(), Box<dyn std::error::Error>>
{
    let (shutdown_tx, shutdown_rx) = make_shutdown();

    let health = make_health();
    let health_clone = Arc::clone(&health);

    let call_count = Arc::new(AtomicU32::new(0));
    let call_count_clone = Arc::clone(&call_count);

    let handle = spawn_supervised_watcher(
        "panic-restart",
        shutdown_rx,
        Some(health_clone),
        SupervisedWatcherConfig { restart_on_failure: true, max_restarts: 0 },
        move || {
            let count = Arc::clone(&call_count_clone);
            async move {
                let n = count.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    panic!("deliberate supervised panic");
                }
                Ok(())
            }
        },
    );

    // Wait for the panic cycle to complete and the restart to run.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let last_error = health.read().last_error.clone();
    assert!(
        last_error.as_deref().unwrap_or("").contains("panicked"),
        "health tracker should note the panic; got: {last_error:?}"
    );
    assert!(
        last_error
            .as_deref()
            .unwrap_or("")
            .contains("deliberate supervised panic"),
        "panic message should be preserved; got: {last_error:?}"
    );
    assert!(
        call_count.load(Ordering::SeqCst) >= 2,
        "factory should be called at least twice after panic+restart"
    );

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(std::time::Duration::from_secs(5), handle).await??;
    Ok(())
}

#[sinex_test]
async fn supervised_max_restarts_causes_exit() -> Result<(), Box<dyn std::error::Error>> {
    let (_shutdown_tx, shutdown_rx) = make_shutdown();

    let call_count = Arc::new(AtomicU32::new(0));
    let call_count_clone = Arc::clone(&call_count);

    let handle = spawn_supervised_watcher(
        "max-restart",
        shutdown_rx,
        None,
        SupervisedWatcherConfig { restart_on_failure: true, max_restarts: 3 },
        move || {
            let count = Arc::clone(&call_count_clone);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err(SinexError::processing("always fails"))
            }
        },
    );

    tokio::time::timeout(std::time::Duration::from_secs(15), handle).await??;

    // With max_restarts=3 and BASE_BACKOFF=1s the supervisor calls:
    //   call 1 → restarts=1 (backoff 1s)
    //   call 2 → restarts=2 (backoff 2s)
    //   call 3 → restarts=3 → give up (max_restarts reached)
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        3,
        "factory should be called exactly max_restarts times"
    );
    Ok(())
}

#[sinex_test]
async fn supervised_log_only_exits_after_first_failure() -> Result<(), Box<dyn std::error::Error>>
{
    let (_shutdown_tx, shutdown_rx) = make_shutdown();

    let called = Arc::new(AtomicBool::new(false));
    let called_clone = Arc::clone(&called);

    let handle = spawn_supervised_watcher(
        "log-only-fail",
        shutdown_rx,
        None,
        SupervisedWatcherConfig::log_only(),
        move || {
            called_clone.store(true, Ordering::SeqCst);
            async { Err(SinexError::processing("single failure")) }
        },
    );

    tokio::time::timeout(std::time::Duration::from_secs(5), handle).await??;
    assert!(called.load(Ordering::SeqCst), "factory should have been called once");
    Ok(())
}
