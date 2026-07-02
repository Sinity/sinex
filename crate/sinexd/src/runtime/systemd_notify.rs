use std::sync::mpsc;
use std::thread::JoinHandle as ThreadJoinHandle;
use std::time::Duration;

use sd_notify::NotifyState;
use tracing::{debug, warn};

fn watchdog_interval() -> Option<Duration> {
    let mut usec = 0_u64;
    if !sd_notify::watchdog_enabled(false, &mut usec) || usec == 0 {
        return None;
    }

    Some(Duration::from_micros((usec / 2).max(1)))
}

/// When set, this process is being hosted inside another sinex daemon
/// (typically `sinexd`) and individual in-process modules MUST NOT send
/// `READY=1` / `STOPPING=1` — only the top-level supervisor's `sd_notify` is
/// authoritative for systemd. A fire-once monitor binding emitting
/// `STOPPING=1` would otherwise tell systemd that the entire host daemon
/// is shutting down.
const HOSTED_MODE_ENV: &str = "SINEX_SD_NOTIFY_HOSTED";

fn is_hosted() -> bool {
    matches!(
        std::env::var(HOSTED_MODE_ENV).as_deref(),
        Ok("1" | "true" | "yes")
    )
}

pub fn notify_ready(component: &str) {
    if is_hosted() {
        return;
    }
    notify_ready_unhosted(component);
}

pub fn notify_stopping(component: &str) {
    if is_hosted() {
        return;
    }
    notify_stopping_unhosted(component);
}

/// Variant that always sends READY=1, bypassing the hosted-mode latch.
/// Use only from the top-level supervisor that owns the systemd unit.
pub fn notify_ready_unhosted(component: &str) {
    if let Err(error) = sd_notify::notify(false, &[NotifyState::Ready]) {
        warn!(component, error = %error, "Failed to notify systemd ready state");
    }
}

/// Variant that always sends STOPPING=1, bypassing the hosted-mode latch.
/// Use only from the top-level supervisor that owns the systemd unit.
pub fn notify_stopping_unhosted(component: &str) {
    if let Err(error) = sd_notify::notify(false, &[NotifyState::Stopping]) {
        warn!(component, error = %error, "Failed to notify systemd stopping state");
    }
}

pub struct WatchdogHandle {
    shutdown_tx: mpsc::Sender<()>,
    join_handle: ThreadJoinHandle<()>,
}

/// Mark this process as running in hosted mode for `sd_notify` purposes.
///
/// Sets the `SINEX_SD_NOTIFY_HOSTED=1` env var so any subsequent calls to
/// [`notify_ready`] / [`notify_stopping`] / [`spawn_watchdog`] from
/// in-process modules become no-ops. Only the top-level supervisor (the
/// host with main PID under systemd) should still call `sd_notify`.
///
/// # Safety
/// `std::env::set_var` is `unsafe` in edition 2024; callers that invoke
/// this from a single-threaded startup (before tokio runtime starts
/// spawning) are safe.
pub fn enter_hosted_mode() {
    // SAFETY: invoked from the top-level supervisor's startup before any
    // worker threads / bindings are spawned.
    unsafe { std::env::set_var(HOSTED_MODE_ENV, "1") };
}

/// Spawn the systemd watchdog pinger on a dedicated OS thread.
///
/// A tokio task can be starved by long-running blocking work on the runtime
/// (e.g. large COPY batches in the event-engine persistence path), which has
/// caused systemd to SIGTERM sinexd mid-batch. Running the ping loop on a
/// `std::thread` with `recv_timeout` guarantees the watchdog never shares an
/// executor with heavy work, so the daemon keeps its WATCHDOG=1 messages
/// flowing as long as the OS scheduler runs threads at all.
pub fn spawn_watchdog(component: &'static str) -> Option<WatchdogHandle> {
    if is_hosted() {
        return None;
    }
    spawn_watchdog_unhosted(component)
}

/// Variant that always spawns the watchdog, bypassing the hosted-mode
/// latch. Use only from the top-level supervisor.
pub fn spawn_watchdog_unhosted(component: &'static str) -> Option<WatchdogHandle> {
    let interval = watchdog_interval()?;
    debug!(
        component,
        watchdog_interval_ms = interval.as_millis(),
        "Systemd watchdog enabled"
    );

    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
    let join_handle = std::thread::Builder::new()
        .name(format!("watchdog-{component}"))
        .spawn(move || {
            loop {
                match shutdown_rx.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(error) = sd_notify::notify(false, &[NotifyState::Watchdog]) {
                            warn!(component, error = %error, "Failed to notify systemd watchdog state");
                        }
                    }
                }
            }
        })
        .ok()?;

    Some(WatchdogHandle {
        shutdown_tx,
        join_handle,
    })
}

pub async fn stop_watchdog(handle: Option<WatchdogHandle>, component: &str) {
    let Some(handle) = handle else {
        return;
    };

    let WatchdogHandle {
        shutdown_tx,
        join_handle,
    } = handle;
    let _ = shutdown_tx.send(());
    // Joining a std thread blocks; do it on a blocking task to avoid stalling
    // the caller's async runtime if the thread is mid-syscall.
    let join_result = tokio::task::spawn_blocking(move || join_handle.join()).await;
    match join_result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => warn!(component, "Watchdog thread panicked during shutdown"),
        Err(error) => warn!(component, error = %error, "Failed to join watchdog thread cleanly"),
    }
}

#[cfg(test)]
#[path = "systemd_notify_test.rs"]
mod tests;
