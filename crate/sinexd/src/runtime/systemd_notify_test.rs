// Small inline tests are justified here because they exercise private
// watchdog interval logic and process-global environment handling directly.
use super::{
    HostedReadiness, HostedReadinessStatus, HostedWorkerId, notify_ready, notify_stopping,
    spawn_watchdog, stop_watchdog,
};
use crate::runtime::SinexError;
use std::process;
use std::sync::LazyLock;
use tempfile::tempdir;
use tokio::net::UnixDatagram;
use tokio::time::{Duration, timeout};
use xtask::sandbox::sinex_test;

static ENV_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

#[sinex_test]
async fn hosted_readiness_handles_zero_workers_and_shutdown() -> xtask::sandbox::TestResult<()> {
    let zero = HostedReadiness::configured(std::iter::empty::<HostedWorkerId>())
        .map_err(|error| color_eyre::eyre::eyre!(error))?;
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    assert_eq!(zero.wait(shutdown_rx).await, HostedReadinessStatus::Ready);

    let cancelled = HostedReadiness::configured([HostedWorkerId::from("worker")])
        .map_err(|error| color_eyre::eyre::eyre!(error))?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    shutdown_tx.send(true)?;
    assert_eq!(
        cancelled.wait(shutdown_rx).await,
        HostedReadinessStatus::Cancelled
    );
    Ok(())
}

#[sinex_test]
async fn hosted_readiness_requires_each_explicit_worker_identity()
-> xtask::sandbox::TestResult<()> {
    let readiness =
        HostedReadiness::configured([HostedWorkerId::from("automaton:a"), HostedWorkerId::from(
            "source:b",
        )])
        .map_err(|error| color_eyre::eyre::eyre!(error))?;
    let first = readiness.worker("automaton:a").expect("first worker");
    let second = readiness.worker("source:b").expect("second worker");
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let waiter = tokio::spawn(async move { readiness.wait(shutdown_rx).await });
    first.mark_failure("test failure");
    tokio::task::yield_now().await;
    assert_eq!(
        timeout(Duration::from_secs(1), waiter).await??,
        HostedReadinessStatus::Failed {
            worker_id: HostedWorkerId::from("automaton:a"),
            reason: "test failure".to_string(),
        }
    );
    assert!(second.id().as_str() == "source:b");
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
async fn hosted_worker_failure_cancels_pre_ready_wait_without_retry_window()
-> xtask::sandbox::TestResult<()> {
    let readiness = HostedReadiness::configured([HostedWorkerId::from("worker")])
        .map_err(|error| color_eyre::eyre::eyre!(error))?;
    let worker = readiness.worker("worker").expect("worker");
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let wait = tokio::spawn(async move { readiness.wait(shutdown_rx).await });
    assert!(worker.mark_failure("startup failed"));
    assert_eq!(
        timeout(Duration::from_secs(1), wait).await??,
        HostedReadinessStatus::Failed {
            worker_id: HostedWorkerId::from("worker"),
            reason: "startup failed".to_string(),
        }
    );
    Ok(())
}

#[sinex_test]
async fn hosted_worker_shutdown_cancels_worker_scope() -> xtask::sandbox::TestResult<()> {
    let readiness = HostedReadiness::configured([HostedWorkerId::from("worker")])
        .map_err(|error| color_eyre::eyre::eyre!(error))?;
    let worker = readiness.worker("worker").expect("worker");
    let task = tokio::spawn(worker.run(async {
        tokio::time::sleep(Duration::from_secs(30)).await;
    }));
    readiness.cancel();
    timeout(Duration::from_secs(1), task).await??;
    Ok(())
}

#[sinex_test]
async fn hosted_worker_failure_after_ready_reports_degraded_status()
-> xtask::sandbox::TestResult<()> {
    let readiness = HostedReadiness::configured([HostedWorkerId::from("worker")])
        .map_err(|error| color_eyre::eyre::eyre!(error))?;
    let worker = readiness.worker("worker").expect("worker");
    readiness.mark_ready(worker.id());
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let waiter_readiness = readiness.clone();
    let waiter = tokio::spawn(async move { waiter_readiness.wait(shutdown_rx).await });
    let status = timeout(Duration::from_secs(1), waiter).await??;
    assert_eq!(status, HostedReadinessStatus::Ready);
    assert!(!worker.mark_failure("runtime failed"));
    assert_eq!(
        readiness.subscribe().borrow().clone(),
        HostedReadinessStatus::Degraded {
            worker_id: HostedWorkerId::from("worker"),
            reason: "runtime failed".to_string(),
        }
    );
    Ok(())
}

#[sinex_test]
async fn notify_preserves_socket_for_followup_messages() -> xtask::sandbox::TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    let dir = tempdir()?;
    let socket_path = dir.path().join("notify.sock");
    let listener = UnixDatagram::bind(&socket_path)?;
    let old_notify_socket = std::env::var_os("NOTIFY_SOCKET");
    let old_hosted_mode = std::env::var_os("SINEX_SD_NOTIFY_HOSTED");

    unsafe {
        std::env::set_var("NOTIFY_SOCKET", &socket_path);
        std::env::remove_var("SINEX_SD_NOTIFY_HOSTED");
    }

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
    restore_var("SINEX_SD_NOTIFY_HOSTED", old_hosted_mode);
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
    let old_hosted_mode = std::env::var_os("SINEX_SD_NOTIFY_HOSTED");

    unsafe {
        std::env::set_var("NOTIFY_SOCKET", &socket_path);
        std::env::set_var("WATCHDOG_USEC", "50000");
        std::env::set_var("WATCHDOG_PID", process::id().to_string());
        std::env::remove_var("SINEX_SD_NOTIFY_HOSTED");
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
    restore_var("SINEX_SD_NOTIFY_HOSTED", old_hosted_mode);
    result?;
    Ok(())
}
