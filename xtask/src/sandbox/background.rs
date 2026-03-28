use futures::future::BoxFuture;
use std::mem;
use std::sync::Mutex;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::warn;

/// Global registry for cleanup task handles spawned during Drop.
///
/// Uses `std::sync::Mutex` (not tokio) so that synchronous `Drop` impls can
/// always register handles via `.lock()` — no `try_lock()` failures. The
/// critical section is only `Vec::push()`, so contention is negligible.
pub(crate) static CLEANUP_HANDLES: std::sync::LazyLock<Mutex<Vec<tokio::task::JoinHandle<()>>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

pub(crate) const CLEANUP_AWAIT_SECS: u64 = 2;
pub(crate) const BACKGROUND_TIMEOUT_SECS: u64 = 10;

pub(crate) async fn await_pending_cleanups() {
    let timeout = Duration::from_secs(CLEANUP_AWAIT_SECS);

    let pending = {
        let mut guard = CLEANUP_HANDLES
            .lock()
            .expect("CLEANUP_HANDLES lock poisoned");
        mem::take(&mut *guard)
    };

    for mut handle in pending {
        match tokio::time::timeout(timeout, &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                warn!("Background cleanup task failed: {}", err);
            }
            Err(_) => {
                handle.abort();
                warn!(
                    "Background cleanup task exceeded {:?}; aborting to avoid cross-test deadlocks",
                    timeout
                );
            }
        }
    }
}

#[derive(Default)]
pub struct BackgroundRegistry {
    tasks: Vec<(String, JoinHandle<()>)>,
    shutdown_hooks: Vec<(String, BoxFuture<'static, ()>)>,
}

impl BackgroundRegistry {
    fn background_timeout_secs() -> u64 {
        BACKGROUND_TIMEOUT_SECS
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.tasks.len() + self.shutdown_hooks.len()
    }

    pub fn add_task(&mut self, label: impl Into<String>, handle: JoinHandle<()>) {
        self.tasks.push((label.into(), handle));
    }

    pub fn add_hook(&mut self, label: impl Into<String>, hook: BoxFuture<'static, ()>) {
        self.shutdown_hooks.push((label.into(), hook));
    }

    #[must_use]
    pub fn labels(&self) -> Vec<String> {
        self.tasks
            .iter()
            .map(|(l, _)| l.clone())
            .chain(self.shutdown_hooks.iter().map(|(l, _)| l.clone()))
            .collect()
    }

    async fn run_shutdown_hooks(&mut self, timeout_secs: u64) {
        // Run shutdown hooks first so tasks can observe the signal.
        let hooks = std::mem::take(&mut self.shutdown_hooks);
        for (label, hook) in hooks {
            if let Err(err) = tokio::time::timeout(Duration::from_secs(timeout_secs), hook).await {
                warn!(%label, ?err, "Timeout waiting for shutdown hook");
            }
        }
    }

    async fn wait_for_tasks(&mut self, timeout_secs: u64) {
        // Wait for tracked background tasks to finish, aborting on timeout.
        let tasks = std::mem::take(&mut self.tasks);
        for (label, handle) in tasks {
            let mut handle = handle;
            let timeout_sleep = tokio::time::sleep(Duration::from_secs(timeout_secs));
            tokio::pin!(timeout_sleep);

            tokio::select! {
                result = &mut handle => {
                    match result {
                        Ok(()) => {}
                        Err(join_err) => warn!(%label, error = %join_err, "Background task join failed"),
                    }
                }
                () = &mut timeout_sleep => {
                    warn!(%label, "Background task did not finish within timeout; aborting");
                    handle.abort();
                    if let Err(join_err) = handle.await {
                        warn!(%label, error = %join_err, "Background task join failed after abort");
                    }
                }
            };
        }
    }

    pub async fn quiesce(&mut self) {
        let timeout_secs = Self::background_timeout_secs();
        self.run_shutdown_hooks(timeout_secs).await;
        self.wait_for_tasks(timeout_secs).await;
    }

    pub async fn quiesce_tasks_only(&mut self) {
        let timeout_secs = Self::background_timeout_secs();
        self.wait_for_tasks(timeout_secs).await;
    }
}
