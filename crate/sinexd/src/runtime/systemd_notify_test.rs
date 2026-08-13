// Small inline tests are justified here because they exercise private
// watchdog interval logic and process-global environment handling directly.
use super::{
    HostedReadiness, HostedReadinessState, notify_ready, notify_stopping, spawn_watchdog,
    stop_watchdog,
};
use crate::runtime::SinexError;
use std::process;
use std::sync::LazyLock;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tempfile::tempdir;
use tokio::net::UnixDatagram;
use tokio::time::{Duration, timeout};
use xtask::sandbox::sinex_test;

static ENV_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

#[sinex_test]
async fn hosted_readiness_handles_zero_workers_and_shutdown() -> xtask::sandbox::TestResult<()> {
    let zero = HostedReadiness {
        state: Arc::new(HostedReadinessState {
            expected: 0,
            observed: AtomicUsize::new(0),
            notify: tokio::sync::Notify::new(),
        }),
    };
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    assert!(zero.wait(shutdown_rx).await);

    let cancelled = HostedReadiness {
        state: Arc::new(HostedReadinessState {
            expected: 1,
            observed: AtomicUsize::new(0),
            notify: tokio::sync::Notify::new(),
        }),
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    shutdown_tx.send(true)?;
    assert!(!cancelled.wait(shutdown_rx).await);
    Ok(())
}

#[sinex_test]
async fn hosted_readiness_reaches_warm_after_each_worker_signal()
-> xtask::sandbox::TestResult<()> {
    let readiness = HostedReadiness {
        state: Arc::new(HostedReadinessState {
            expected: 2,
            observed: AtomicUsize::new(0),
            notify: tokio::sync::Notify::new(),
        }),
    };
    let state = Arc::clone(&readiness.state);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let waiter = tokio::spawn(async move { readiness.wait(shutdown_rx).await });
    state.observed.store(1, std::sync::atomic::Ordering::Release);
    state.notify.notify_waiters();
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    state.observed.store(2, std::sync::atomic::Ordering::Release);
    state.notify.notify_waiters();
    assert!(tokio::time::timeout(Duration::from_secs(1), waiter).await??);
    Ok(())
}

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

    let result: xtask::sandbox::TestResult<()> = async {
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

        Ok(())
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

    let result: xtask::sandbox::TestResult<()> = async {
        let handle = spawn_watchdog("test-component").ok_or_else(|| {
            SinexError::processing("watchdog task should start when env is configured")
        })?;
        let mut buf = [0_u8; 128];
        let msg_len = timeout(Duration::from_secs(1), listener.recv(&mut buf)).await??;
        stop_watchdog(Some(handle), "test-component").await;
        let msg = std::str::from_utf8(&buf[..msg_len])?;
        assert!(msg.contains("WATCHDOG=1"));
        Ok(())
    }
    .await;

    restore_var("NOTIFY_SOCKET", old_notify_socket);
    restore_var("WATCHDOG_USEC", old_watchdog_usec);
    restore_var("WATCHDOG_PID", old_watchdog_pid);
    result?;
    Ok(())
}
