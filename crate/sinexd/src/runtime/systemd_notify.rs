use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle as ThreadJoinHandle;
use std::time::Duration;

use futures::FutureExt as _;
use sd_notify::NotifyState;
use tokio::sync::{Notify, watch};
use tracing::{debug, warn};

tokio::task_local! {
    static CURRENT_HOSTED_WORKER: HostedWorker;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct HostedWorkerId(String);

impl HostedWorkerId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for HostedWorkerId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

impl From<String> for HostedWorkerId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl std::fmt::Display for HostedWorkerId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostedReadinessStatus {
    Warming,
    Ready,
    Failed {
        worker_id: HostedWorkerId,
        reason: String,
    },
    Degraded {
        worker_id: HostedWorkerId,
        reason: String,
    },
    Cancelled,
}

struct HostedWorkerRecord {
    ready: bool,
}

struct HostedReadinessState {
    workers: Mutex<HashMap<HostedWorkerId, HostedWorkerRecord>>,
    configured: bool,
    cancelled: AtomicBool,
    cancel_notify: Notify,
    state_notify: Notify,
    status_tx: watch::Sender<HostedReadinessStatus>,
}

/// Supervisor-owned lifecycle barrier for in-process workers.
#[derive(Clone)]
pub struct HostedReadiness {
    state: Arc<HostedReadinessState>,
}

impl HostedReadiness {
    /// Create a barrier with the complete worker identity set before any
    /// hosted worker is spawned.
    pub fn configured<I>(worker_ids: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = HostedWorkerId>,
    {
        let mut workers = HashMap::new();
        for worker_id in worker_ids {
            if workers
                .insert(worker_id.clone(), HostedWorkerRecord { ready: false })
                .is_some()
            {
                return Err(format!("duplicate hosted worker identity: {worker_id}"));
            }
        }
        let (status_tx, _status_rx) = watch::channel(HostedReadinessStatus::Warming);
        Ok(Self {
            state: Arc::new(HostedReadinessState {
                workers: Mutex::new(workers),
                configured: true,
                cancelled: AtomicBool::new(false),
                cancel_notify: Notify::new(),
                state_notify: Notify::new(),
                status_tx,
            }),
        })
    }

    pub fn worker(&self, worker_id: impl Into<HostedWorkerId>) -> Option<HostedWorker> {
        let worker_id = worker_id.into();
        self.state
            .workers
            .lock()
            .expect("hosted readiness worker lock poisoned")
            .contains_key(&worker_id)
            .then(|| HostedWorker {
                readiness: self.clone(),
                worker_id,
            })
    }

    pub fn subscribe(&self) -> watch::Receiver<HostedReadinessStatus> {
        self.state.status_tx.subscribe()
    }

    /// Cancel all hosted workers. Cancellation is distinct from worker
    /// failure, so shutdown cannot accidentally announce READY.
    pub fn cancel(&self) {
        if !self.state.cancelled.swap(true, Ordering::AcqRel) {
            self.state
                .status_tx
                .send_replace(HostedReadinessStatus::Cancelled);
            self.state.cancel_notify.notify_waiters();
            self.state.state_notify.notify_waiters();
        }
    }

    fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    fn status(&self) -> HostedReadinessStatus {
        if self.is_cancelled() {
            return HostedReadinessStatus::Cancelled;
        }

        let workers = self
            .state
            .workers
            .lock()
            .expect("hosted readiness worker lock poisoned");
        if workers.values().all(|worker| worker.ready) && self.state.configured {
            HostedReadinessStatus::Ready
        } else {
            HostedReadinessStatus::Warming
        }
    }

    /// Wait until every worker expected by the supervisor has reached its
    /// runner-level startup barrier, or until startup failure/shutdown occurs.
    pub async fn wait(&self, mut shutdown_rx: watch::Receiver<bool>) -> HostedReadinessStatus {
        loop {
            if let Some(status) = self.failure_status() {
                return status;
            }
            match self.status() {
                HostedReadinessStatus::Ready => return HostedReadinessStatus::Ready,
                HostedReadinessStatus::Cancelled => return HostedReadinessStatus::Cancelled,
                HostedReadinessStatus::Warming => {}
                HostedReadinessStatus::Failed { .. } | HostedReadinessStatus::Degraded { .. } => {
                    unreachable!("failure statuses are returned above")
                }
            }

            if *shutdown_rx.borrow() {
                self.cancel();
                return HostedReadinessStatus::Cancelled;
            }
            let state_notified = self.state.state_notify.notified();
            tokio::select! {
                _ = state_notified => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        self.cancel();
                        return HostedReadinessStatus::Cancelled;
                    }
                }
            }
        }
    }

    fn failure_status(&self) -> Option<HostedReadinessStatus> {
        self.state.status_tx.borrow().clone().into_failure_status()
    }

    fn mark_ready(&self, worker_id: &HostedWorkerId) {
        if self.is_cancelled() {
            return;
        }
        if matches!(
            &*self.state.status_tx.borrow(),
            HostedReadinessStatus::Failed { .. } | HostedReadinessStatus::Cancelled
        ) {
            return;
        }
        let mut workers = self
            .state
            .workers
            .lock()
            .expect("hosted readiness worker lock poisoned");
        if let Some(worker) = workers.get_mut(worker_id) {
            worker.ready = true;
        }
        drop(workers);
        let status = self.status();
        self.state.status_tx.send_replace(status);
        self.state.state_notify.notify_waiters();
    }

    /// Returns true when the failure happened before this worker became ready.
    fn mark_failure(&self, worker_id: &HostedWorkerId, reason: String) -> bool {
        if self.is_cancelled() {
            return false;
        }
        let ready = self
            .state
            .workers
            .lock()
            .expect("hosted readiness worker lock poisoned")
            .get(worker_id)
            .is_some_and(|worker| worker.ready);
        let status = if ready {
            HostedReadinessStatus::Degraded {
                worker_id: worker_id.clone(),
                reason,
            }
        } else {
            HostedReadinessStatus::Failed {
                worker_id: worker_id.clone(),
                reason,
            }
        };
        let pre_ready = matches!(status, HostedReadinessStatus::Failed { .. });
        self.state.status_tx.send_replace(status);
        self.state.state_notify.notify_waiters();
        pre_ready
    }

    fn mark_exited(&self, worker_id: &HostedWorkerId) {
        if matches!(
            &*self.state.status_tx.borrow(),
            HostedReadinessStatus::Failed { .. }
                | HostedReadinessStatus::Degraded { .. }
                | HostedReadinessStatus::Cancelled
        ) {
            return;
        }
        let _ = self.mark_failure(
            worker_id,
            "worker exited without remaining healthy".to_string(),
        );
    }
}

impl HostedReadinessStatus {
    fn into_failure_status(self) -> Option<Self> {
        match self {
            Self::Failed { .. } | Self::Degraded { .. } => Some(self),
            Self::Warming | Self::Ready | Self::Cancelled => None,
        }
    }
}

#[derive(Clone)]
pub struct HostedWorker {
    readiness: HostedReadiness,
    worker_id: HostedWorkerId,
}

impl HostedWorker {
    pub fn id(&self) -> &HostedWorkerId {
        &self.worker_id
    }

    pub fn mark_failure(&self, reason: impl Into<String>) -> bool {
        self.readiness.mark_failure(&self.worker_id, reason.into())
    }

    pub async fn run<F>(self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let worker = self.clone();
        CURRENT_HOSTED_WORKER
            .scope(self, async move {
                let completed = tokio::select! {
                    result = std::panic::AssertUnwindSafe(future).catch_unwind() => Some(result),
                    _ = worker.cancelled() => None,
                };
                match completed {
                    Some(Ok(())) => worker.readiness.mark_exited(&worker.worker_id),
                    Some(Err(_)) => {
                        worker.mark_failure("worker task panicked");
                    }
                    None => {}
                }
            })
            .await;
    }

    async fn cancelled(&self) {
        let notified = self.readiness.state.cancel_notify.notified();
        if self.readiness.is_cancelled() {
            return;
        }
        notified.await;
    }
}

fn hosted_worker_ready() {
    let _ = CURRENT_HOSTED_WORKER.try_with(|worker| {
        worker.readiness.mark_ready(&worker.worker_id);
    });
}

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
        hosted_worker_ready();
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

pub fn notify_status_unhosted(component: &str, status: &str) {
    if let Err(error) = sd_notify::notify(false, &[NotifyState::Status(status)]) {
        warn!(component, error = %error, "Failed to notify systemd status");
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
