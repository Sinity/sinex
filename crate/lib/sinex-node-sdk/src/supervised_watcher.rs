//! Supervised watcher task spawning with panic catch, backoff restart, and health reporting.
//!
//! Watcher tasks (filesystem inotify, journald, dbus, udev, terminal-tail, etc.) are
//! long-running and must equal the node's lifetime. A bare `tokio::spawn` drops the
//! `JoinHandle` silently: when the task panics or returns an error, data capture stops
//! and nothing surfaces the failure.
//!
//! `spawn_supervised_watcher` wraps the future in:
//! - panic catching via `AssertUnwindSafe` + `FutureExt::catch_unwind`
//! - structured `tracing::error!` emission on error or panic
//! - optional restart with exponential backoff until `shutdown_rx` fires
//! - optional `WatcherHealth` update on error
//!
//! # Minimal usage (fire-and-forget with logging only)
//!
//! ```rust,ignore
//! use sinex_node_sdk::supervised_watcher::SupervisedWatcherConfig;
//!
//! let handle = spawn_supervised_watcher(
//!     "dbus",
//!     shutdown_rx.clone(),
//!     None,       // no health tracker
//!     SupervisedWatcherConfig::log_only(),
//!     move || async move { dbus_watcher.start_streaming(tx, material).await },
//! );
//! watcher_handle.start(handle, None)?;
//! ```
//!
//! # With backoff restart and health reporting
//!
//! ```rust,ignore
//! let handle = spawn_supervised_watcher(
//!     "udev",
//!     shutdown_rx.clone(),
//!     Some(health_tracker),
//!     SupervisedWatcherConfig::default(),
//!     move || async move { watcher.start_streaming(tx, material).await },
//! );
//! ```

use std::sync::Arc;

use futures::FutureExt as _;
use parking_lot::RwLock;
use sinex_primitives::SinexError;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{error, warn};

use crate::watcher_handle::WatcherHealth;

/// Maximum individual backoff delay between watcher restart attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Base delay for the first restart attempt after a failure.
const BASE_BACKOFF: Duration = Duration::from_secs(1);

/// Configuration for supervised watcher behavior.
#[derive(Debug, Clone)]
pub struct SupervisedWatcherConfig {
    /// Whether to restart the watcher after an error or panic.
    ///
    /// When `true`, the supervisor loops until `shutdown_rx` fires, backing off
    /// between attempts. When `false`, a single failure is logged and the task exits.
    pub restart_on_failure: bool,

    /// Maximum number of restart attempts before giving up (0 = unlimited).
    ///
    /// Only meaningful when `restart_on_failure` is `true`.
    pub max_restarts: u32,
}

impl Default for SupervisedWatcherConfig {
    fn default() -> Self {
        Self {
            restart_on_failure: true,
            max_restarts: 0, // unlimited
        }
    }
}

impl SupervisedWatcherConfig {
    /// Log errors but do not restart — useful for watchers that manage their own
    /// reconnection internally.
    pub fn log_only() -> Self {
        Self {
            restart_on_failure: false,
            max_restarts: 0,
        }
    }
}

/// Spawn a one-shot watcher future with panic catching and health reporting.
///
/// This is the simpler variant for watchers whose setup is done externally
/// (e.g. via `WatcherFactory`) and where restart is handled at a higher level
/// (e.g. `ensure_watchers_running`). It wraps a single `Future` — not a factory —
/// so there is no restart loop. On panic or error, it:
/// - catches the panic via `AssertUnwindSafe` + `FutureExt::catch_unwind`
/// - emits `tracing::error!` with the watcher name and error/panic details
/// - records the failure in `health_tracker` (if provided)
///
/// # Arguments
/// - `watcher_name`: stable identifier used in log lines and health records.
/// - `health_tracker`: optional shared health state to update on error/panic.
/// - `fut`: the watcher future to supervise.
pub fn spawn_watcher_with_panic_catch<Fut>(
    watcher_name: &'static str,
    health_tracker: Option<Arc<RwLock<WatcherHealth>>>,
    fut: Fut,
) -> JoinHandle<()>
where
    Fut: std::future::Future<Output = Result<(), SinexError>> + Send + 'static,
{
    tokio::spawn(async move {
        let outcome = std::panic::AssertUnwindSafe(fut).catch_unwind().await;
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                let error_msg = err.to_string();
                error!(
                    watcher = watcher_name,
                    error = %err,
                    "Watcher task failed"
                );
                if let Some(tracker) = &health_tracker {
                    tracker.write().last_error = Some(error_msg);
                }
            }
            Err(panic_payload) => {
                let panic_msg = format_panic_payload(&panic_payload);
                error!(
                    watcher = watcher_name,
                    panic = %panic_msg,
                    "Watcher task panicked"
                );
                if let Some(tracker) = &health_tracker {
                    tracker.write().last_error =
                        Some(format!("watcher panicked: {panic_msg}"));
                }
            }
        }
    })
}

/// Spawn a supervised watcher task.
///
/// The `factory` closure is called each time the watcher needs to be (re)started.
/// It returns a `Future` that resolves to `Result<(), SinexError>`. On `Ok`, the
/// watcher is considered to have exited cleanly. On `Err` or panic, the error is
/// logged (and, if a `health_tracker` is provided, recorded there) and the watcher
/// is optionally restarted after a backoff delay.
///
/// The returned `JoinHandle` resolves to `()` when the supervisor exits — either
/// because `shutdown_rx` fired, the watcher exited cleanly, or `max_restarts` was
/// exceeded.
///
/// # Arguments
/// - `watcher_name`: stable identifier used in all log lines and health records.
/// - `shutdown_rx`: the node's shutdown receiver; supervisor exits when `*shutdown_rx.borrow() == true`.
/// - `health_tracker`: optional shared health state to update on error.
/// - `config`: restart and limit settings.
/// - `factory`: closure that produces a new watcher future on each start.
pub fn spawn_supervised_watcher<F, Fut>(
    watcher_name: &'static str,
    shutdown_rx: watch::Receiver<bool>,
    health_tracker: Option<Arc<RwLock<WatcherHealth>>>,
    config: SupervisedWatcherConfig,
    factory: F,
) -> JoinHandle<()>
where
    F: Fn() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), SinexError>> + Send + 'static,
{
    tokio::spawn(async move {
        let mut restarts: u32 = 0;
        let mut backoff = BASE_BACKOFF;

        loop {
            // Check for shutdown before starting the watcher.
            if *shutdown_rx.borrow() {
                return;
            }

            // Wrap the future in panic catching.
            let outcome = std::panic::AssertUnwindSafe(factory())
                .catch_unwind()
                .await;

            match outcome {
                // Clean exit — watcher finished normally.
                Ok(Ok(())) => {
                    // If shutdown was requested, exit cleanly.
                    if *shutdown_rx.borrow() {
                        return;
                    }
                    // Unexpected clean exit without shutdown: treat as transient error.
                    warn!(
                        watcher = watcher_name,
                        "Watcher exited unexpectedly without error; will attempt restart"
                    );
                }

                // Watcher returned an error.
                Ok(Err(err)) => {
                    let error_msg = err.to_string();
                    error!(
                        watcher = watcher_name,
                        error = %err,
                        "Watcher task failed"
                    );
                    if let Some(tracker) = &health_tracker {
                        tracker.write().last_error = Some(error_msg);
                    }
                }

                // Watcher panicked.
                Err(panic_payload) => {
                    let panic_msg = format_panic_payload(&panic_payload);
                    error!(
                        watcher = watcher_name,
                        panic = %panic_msg,
                        "Watcher task panicked"
                    );
                    if let Some(tracker) = &health_tracker {
                        tracker.write().last_error =
                            Some(format!("watcher panicked: {panic_msg}"));
                    }
                }
            }

            // Decide whether to restart.
            if !config.restart_on_failure {
                return;
            }

            restarts += 1;
            if config.max_restarts > 0 && restarts >= config.max_restarts {
                error!(
                    watcher = watcher_name,
                    restarts = restarts,
                    max_restarts = config.max_restarts,
                    "Watcher exceeded maximum restart attempts; giving up"
                );
                if let Some(tracker) = &health_tracker {
                    tracker.write().last_error = Some(format!(
                        "watcher exceeded {max} restart attempts",
                        max = config.max_restarts
                    ));
                }
                return;
            }

            warn!(
                watcher = watcher_name,
                restart_attempt = restarts,
                backoff_ms = backoff.as_millis(),
                "Watcher will restart after backoff"
            );

            // Wait for backoff or shutdown.
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = shutdown_notified(&shutdown_rx) => {
                    return;
                }
            }

            // Exponential backoff with cap.
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    })
}

/// Returns a future that resolves when `shutdown_rx` transitions to `true`.
async fn shutdown_notified(shutdown_rx: &watch::Receiver<bool>) {
    let mut rx = shutdown_rx.clone();
    // Use changed() loop so we don't miss a value that was set before we start waiting.
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            // Channel closed — treat as shutdown.
            return;
        }
    }
}

fn format_panic_payload(payload: &dyn std::any::Any) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}
