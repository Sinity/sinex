use std::time::Duration;

use sd_notify::NotifyState;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

fn watchdog_interval() -> Option<Duration> {
    let mut usec = 0_u64;
    if !sd_notify::watchdog_enabled(false, &mut usec) || usec == 0 {
        return None;
    }

    Some(Duration::from_micros((usec / 2).max(1)))
}

pub fn notify_ready(component: &str) {
    if let Err(error) = sd_notify::notify(false, &[NotifyState::Ready]) {
        warn!(component, error = %error, "Failed to notify systemd ready state");
    }
}

pub fn notify_stopping(component: &str) {
    if let Err(error) = sd_notify::notify(false, &[NotifyState::Stopping]) {
        warn!(component, error = %error, "Failed to notify systemd stopping state");
    }
}

pub struct WatchdogHandle {
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    join_handle: JoinHandle<()>,
}

pub fn spawn_watchdog(component: &'static str) -> Option<WatchdogHandle> {
    let interval = watchdog_interval()?;
    debug!(
        component,
        watchdog_interval_ms = interval.as_millis(),
        "Systemd watchdog enabled"
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        let mut shutdown_rx = shutdown_rx;
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(error) = sd_notify::notify(false, &[NotifyState::Watchdog]) {
                        warn!(component, error = %error, "Failed to notify systemd watchdog state");
                    }
                }
                _ = &mut shutdown_rx => {
                    break;
                }
            }
        }
    });

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
    match join_handle.await {
        Ok(()) => {}
        Err(error) => {
            warn!(component, error = %error, "Failed to stop systemd watchdog task cleanly");
        }
    }
}

#[cfg(test)]
mod tests {
    // Small inline tests are justified here because they exercise private
    // watchdog interval logic and process-global environment handling directly.
    use super::{notify_ready, notify_stopping, spawn_watchdog, stop_watchdog};
    use color_eyre::eyre::eyre;
    use std::process;
    use std::sync::LazyLock;
    use tempfile::tempdir;
    use tokio::net::UnixDatagram;
    use tokio::time::{Duration, timeout};
    use xtask::sandbox::sinex_test;

    static ENV_LOCK: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    fn restore_var(key: &str, value: Option<std::ffi::OsString>) {
        match value {
            Some(value) => {
                unsafe { std::env::set_var(key, value) };
            }
            None => {
                unsafe { std::env::remove_var(key) };
            }
        }
    }

    #[sinex_test]
    async fn notify_preserves_socket_for_followup_messages() -> xtask::sandbox::TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        let dir = tempdir()?;
        let socket_path = dir.path().join("notify.sock");
        let listener = UnixDatagram::bind(&socket_path)?;
        let old_notify_socket = std::env::var_os("NOTIFY_SOCKET");

        unsafe { std::env::set_var("NOTIFY_SOCKET", &socket_path) };

        let result = async {
            let mut buf = [0_u8; 128];

            notify_ready("test-component");
            let ready_len = timeout(Duration::from_secs(1), listener.recv(&mut buf))
                .await??
                .max(0);
            let ready_msg = std::str::from_utf8(&buf[..ready_len])?;
            assert!(ready_msg.contains("READY=1"));
            assert_eq!(
                std::env::var_os("NOTIFY_SOCKET").as_deref(),
                Some(socket_path.as_os_str())
            );

            notify_stopping("test-component");
            let stopping_len = timeout(Duration::from_secs(1), listener.recv(&mut buf))
                .await??
                .max(0);
            let stopping_msg = std::str::from_utf8(&buf[..stopping_len])?;
            assert!(stopping_msg.contains("STOPPING=1"));

            Ok::<(), color_eyre::Report>(())
        }
        .await;

        restore_var("NOTIFY_SOCKET", old_notify_socket);
        result?;
        Ok(())
    }

    #[sinex_test]
    async fn watchdog_task_emits_ping_when_enabled() -> xtask::sandbox::TestResult<()> {
        let _guard = ENV_LOCK.lock().await;
        let dir = tempdir()?;
        let socket_path = dir.path().join("watchdog.sock");
        let listener = UnixDatagram::bind(&socket_path)?;
        let old_notify_socket = std::env::var_os("NOTIFY_SOCKET");
        let old_watchdog_usec = std::env::var_os("WATCHDOG_USEC");
        let old_watchdog_pid = std::env::var_os("WATCHDOG_PID");

        unsafe {
            std::env::set_var("NOTIFY_SOCKET", &socket_path);
            std::env::set_var("WATCHDOG_USEC", "50000");
            std::env::set_var("WATCHDOG_PID", process::id().to_string());
        }

        let result = async {
            let handle = spawn_watchdog("test-component")
                .ok_or_else(|| eyre!("watchdog task should start when env is configured"))?;
            let mut buf = [0_u8; 128];
            let msg_len = timeout(Duration::from_secs(1), listener.recv(&mut buf)).await??;
            stop_watchdog(Some(handle), "test-component").await;
            let msg = std::str::from_utf8(&buf[..msg_len])?;
            assert!(msg.contains("WATCHDOG=1"));
            Ok::<(), color_eyre::Report>(())
        }
        .await;

        restore_var("NOTIFY_SOCKET", old_notify_socket);
        restore_var("WATCHDOG_USEC", old_watchdog_usec);
        restore_var("WATCHDOG_PID", old_watchdog_pid);
        result?;
        Ok(())
    }
}
