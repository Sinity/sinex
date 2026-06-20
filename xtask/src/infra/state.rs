//! Per-checkout state management with cross-checkout locking.
//!
//! This module manages the `.sinex/` state directory for each checkout,
//! ensuring only one dev stack can be active at a time across all checkouts.

use color_eyre::eyre::{Result, WrapErr, bail};
use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::{Timestamp, format_rfc3339};
use std::fs;
use std::path::{Path, PathBuf};

/// Information stored in the lock file
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::unsafe_derive_deserialize)] // is_alive() uses kill(pid, 0) which is safe for any PID
pub struct LockInfo {
    /// PID of the process holding the lock
    pub pid: u32,
    /// Absolute path to the checkout holding the lock
    pub checkout_path: PathBuf,
    /// Timestamp when lock was acquired
    pub acquired_at: Timestamp,
    /// Optional description of what's running
    pub description: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CheckoutInventoryRoot {
    pub cache_root: PathBuf,
    pub dev_state_dir: PathBuf,
    pub checkout_path: Option<PathBuf>,
    pub lock: LockInspection,
}

#[derive(Debug, Clone)]
pub enum LockInspection {
    Missing,
    Live(LockInfo),
    Stale(LockInfo),
    Malformed(String),
}

impl LockInfo {
    /// Create a new lock info for the current process
    #[must_use]
    pub fn current(checkout_path: PathBuf, description: Option<String>) -> Self {
        Self {
            pid: std::process::id(),
            checkout_path,
            acquired_at: Timestamp::now(),
            description,
        }
    }

    /// Check if the process holding this lock is still alive
    #[must_use]
    pub fn is_alive(&self) -> bool {
        // Use kill(pid, 0) to check if process exists
        unsafe { libc::kill(self.pid as i32, 0) == 0 }
    }
}

fn remove_lock_file(path: &Path, context: &str) -> Result<()> {
    fs::remove_file(path).wrap_err_with(|| format!("failed to remove {context} {}", path.display()))
}

/// Manages per-checkout state directory and cross-checkout locking
pub struct CheckoutState {
    /// Path to the checkout root (where .sinex lives)
    checkout_root: PathBuf,
    /// Path to the state directory (.sinex/)
    state_dir: PathBuf,
}

impl CheckoutState {
    /// State directory name
    const STATE_DIR_NAME: &'static str = ".sinex";
    /// Lock file name within state directory
    const LOCK_FILE_NAME: &'static str = ".lock";

    /// Create a `CheckoutState` for the current working directory's checkout.
    ///
    /// Honors `SINEX_DEV_STATE_DIR` exported by the sinex dev shell, which
    /// relocates the infra state directory (postgres data, run socket, nats)
    /// onto NVMe. When unset — CI, a bare `nix develop`, or any non-direnv
    /// invocation — this falls back to the in-checkout `.sinex/`, so behavior
    /// is unchanged outside the relocating dev shell.
    ///
    /// This is what makes xtask's computed `DATABASE_URL` socket path agree
    /// with the `DATABASE_URL`/`PGHOST` the dev shell exports
    /// (`$SINEX_DEV_STATE_DIR/run`). Without it, xtask starts postgres under
    /// `.sinex/run` while the environment advertises the relocated socket, so
    /// rust-analyzer / sqlx / psql hammer a dead socket.
    pub fn for_current_checkout() -> Result<Self> {
        let checkout_root = Self::find_checkout_root()?;
        let state_dir = Self::resolve_state_dir(&checkout_root);
        Ok(Self {
            checkout_root,
            state_dir,
        })
    }

    /// Resolve the infra state directory for a checkout, honoring the dev
    /// shell's `SINEX_DEV_STATE_DIR` relocation while ignoring values that
    /// belong to a different checkout (worktree isolation). Falls back to the
    /// in-checkout `.sinex/`.
    fn resolve_state_dir(checkout_root: &Path) -> PathBuf {
        crate::config::workspace_pinned_env_path("SINEX_DEV_STATE_DIR", checkout_root, || {
            checkout_root.join(Self::STATE_DIR_NAME)
        })
    }

    /// Create a `CheckoutState` rooted at a specific checkout path.
    ///
    /// Always uses the in-checkout `.sinex/` for that path. Unlike
    /// [`Self::for_current_checkout`], this does not consult
    /// `SINEX_DEV_STATE_DIR`: the current shell's relocation env belongs to the
    /// *current* checkout, not an arbitrary one passed in here.
    pub fn new(checkout_root: PathBuf) -> Result<Self> {
        let state_dir = checkout_root.join(Self::STATE_DIR_NAME);
        Ok(Self {
            checkout_root,
            state_dir,
        })
    }

    /// Find the checkout root by looking for .git directory
    fn find_checkout_root() -> Result<PathBuf> {
        let cwd = std::env::current_dir().context("Failed to get current directory")?;

        // Walk up looking for .git
        let mut current = cwd.as_path();
        loop {
            if current.join(".git").exists() {
                return Ok(current.to_path_buf());
            }
            match current.parent() {
                Some(parent) => current = parent,
                None => bail!("Not in a git repository. Run from within the sinex checkout."),
            }
        }
    }

    /// Get the checkout root path
    #[must_use]
    pub fn checkout_root(&self) -> &Path {
        &self.checkout_root
    }

    /// Get the state directory path (.sinex/)
    #[must_use]
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    /// Get the lock file path
    #[must_use]
    pub fn lock_file(&self) -> PathBuf {
        self.state_dir.join(Self::LOCK_FILE_NAME)
    }

    /// Derived paths within the state directory
    #[must_use]
    pub fn data_dir(&self) -> PathBuf {
        self.state_dir.join("data")
    }

    #[must_use]
    pub fn run_dir(&self) -> PathBuf {
        self.state_dir.join("run")
    }

    #[must_use]
    pub fn logs_dir(&self) -> PathBuf {
        self.run_dir().join("logs")
    }

    #[must_use]
    pub fn snapshots_dir(&self) -> PathBuf {
        self.state_dir.join("snapshots")
    }

    #[must_use]
    pub fn config_dir(&self) -> PathBuf {
        self.state_dir.join("config")
    }

    #[must_use]
    pub fn pg_data(&self) -> PathBuf {
        self.data_dir().join("postgres")
    }

    #[must_use]
    pub fn nats_data(&self) -> PathBuf {
        self.data_dir().join("nats")
    }

    #[must_use]
    pub fn annex_data(&self) -> PathBuf {
        self.data_dir().join("annex")
    }

    /// Ensure all directories exist
    pub fn ensure_directories(&self) -> Result<()> {
        fs::create_dir_all(self.config_dir().join("nats"))?;
        fs::create_dir_all(self.pg_data())?;
        fs::create_dir_all(self.nats_data().join("jetstream"))?;
        fs::create_dir_all(self.annex_data())?;
        fs::create_dir_all(self.run_dir())?;
        fs::create_dir_all(self.logs_dir())?;
        fs::create_dir_all(self.snapshots_dir())?;
        Ok(())
    }

    /// Check if another checkout has an active lock
    ///
    /// Returns Some(LockInfo) if locked by another process, None if available.
    pub fn is_locked_by_other(&self) -> Result<Option<LockInfo>> {
        let lock_file = self.lock_file();
        if !lock_file.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&lock_file).context("Failed to read lock file")?;

        let lock_info: LockInfo =
            serde_json::from_str(&content).context("Failed to parse lock file")?;

        // Check if the locking process is still alive
        if !lock_info.is_alive() {
            // Process is dead, clean up stale lock
            remove_lock_file(&lock_file, "stale lock file")?;
            return Ok(None);
        }

        // Check if it's this process (reentrant lock)
        if lock_info.pid == std::process::id() {
            return Ok(None);
        }

        // Check if it's a different checkout
        if lock_info.checkout_path == self.checkout_root {
            // Same checkout, different PID - another process in this checkout
            Ok(Some(lock_info))
        } else {
            // Different checkout has the lock
            Ok(Some(lock_info))
        }
    }

    /// Acquire the lock for this checkout
    ///
    /// Returns error if locked by another process.
    pub fn acquire_lock(&self, description: Option<String>) -> Result<LockGuard> {
        // Ensure state directory exists
        fs::create_dir_all(&self.state_dir)?;

        // Check for existing lock
        if let Some(lock_info) = self.is_locked_by_other()? {
            bail!(
                "Another dev stack is already running:\n\
                 \n\
                 Checkout: {}\n\
                 PID: {}\n\
                 Started: {}\n\
                 {}\n\
                 \n\
                 Stop it first: xtask infra stop",
                lock_info.checkout_path.display(),
                lock_info.pid,
                format_rfc3339(lock_info.acquired_at),
                lock_info.description.as_deref().unwrap_or("")
            );
        }

        // Write lock file
        let lock_info = LockInfo::current(self.checkout_root.clone(), description);
        let content = serde_json::to_string_pretty(&lock_info)?;
        fs::write(self.lock_file(), content)?;

        Ok(LockGuard {
            lock_file: self.lock_file(),
        })
    }

    /// Release the lock (if held by this process)
    pub fn release_lock(&self) -> Result<()> {
        let lock_file = self.lock_file();
        if !lock_file.exists() {
            return Ok(());
        }

        let content =
            fs::read_to_string(&lock_file).context("Failed to read lock file during release")?;
        let lock_info: LockInfo =
            serde_json::from_str(&content).context("Failed to parse lock file during release")?;

        // Only remove if we own it
        if lock_info.pid == std::process::id() {
            remove_lock_file(&lock_file, "owned lock file")?;
        }

        Ok(())
    }

    /// Discover every current-user dev-state root under a cache base directory.
    ///
    /// This is read-only: it classifies locks and paths without removing stale
    /// files or starting/stopping services.
    pub fn inventory_roots_under(base_dir: &Path) -> Result<Vec<CheckoutInventoryRoot>> {
        if !base_dir.exists() {
            return Ok(Vec::new());
        }

        let mut roots = Vec::new();
        for entry in fs::read_dir(base_dir)
            .wrap_err_with(|| format!("failed to read dev cache base {}", base_dir.display()))?
        {
            let entry = entry.wrap_err_with(|| {
                format!("failed to read dev cache entry in {}", base_dir.display())
            })?;
            let cache_root = entry.path();
            if !cache_root.is_dir() {
                continue;
            }
            let dev_state_dir = cache_root.join("dev-state");
            if !dev_state_dir.is_dir() {
                continue;
            }

            let lock = inspect_lock_file(&dev_state_dir.join(Self::LOCK_FILE_NAME));
            let checkout_path = match &lock {
                LockInspection::Live(info) | LockInspection::Stale(info) => {
                    Some(info.checkout_path.clone())
                }
                LockInspection::Missing | LockInspection::Malformed(_) => None,
            };

            roots.push(CheckoutInventoryRoot {
                cache_root,
                dev_state_dir,
                checkout_path,
                lock,
            });
        }

        Ok(roots)
    }

    pub fn default_inventory_base_dir() -> PathBuf {
        let user = std::env::var("USER").unwrap_or_else(|_| "sinity".to_string());
        PathBuf::from("/var/cache/sinex").join(user)
    }
}

fn inspect_lock_file(lock_file: &Path) -> LockInspection {
    let Ok(content) = fs::read_to_string(lock_file) else {
        return if lock_file.exists() {
            LockInspection::Malformed(format!("failed to read {}", lock_file.display()))
        } else {
            LockInspection::Missing
        };
    };

    match serde_json::from_str::<LockInfo>(&content) {
        Ok(lock) if lock.is_alive() => LockInspection::Live(lock),
        Ok(lock) => LockInspection::Stale(lock),
        Err(error) => LockInspection::Malformed(format!(
            "failed to parse lock file {}: {error}",
            lock_file.display()
        )),
    }
}

/// RAII guard that releases the lock when dropped
pub struct LockGuard {
    lock_file: PathBuf,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Err(error) = remove_lock_file(&self.lock_file, "lock file during guard drop")
            && error
                .downcast_ref::<std::io::Error>()
                .is_none_or(|io_error| io_error.kind() != std::io::ErrorKind::NotFound)
        {
            tracing::warn!(path = %self.lock_file.display(), error = %error, "failed to remove lock file during drop");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::EnvGuard;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn resolve_state_dir_honors_dev_state_relocation() -> TestResult<()> {
        // The dev shell relocates infra state onto NVMe via SINEX_DEV_STATE_DIR.
        // A relocated path outside any checkout must be honored verbatim so the
        // postgres socket lands where DATABASE_URL/PGHOST advertise it.
        let relocated = tempfile::tempdir()?;
        let checkout = tempfile::tempdir()?;
        let mut env = EnvGuard::with_keys(&["SINEX_DEV_STATE_DIR"]);
        env.set("SINEX_DEV_STATE_DIR", relocated.path());

        let resolved = CheckoutState::resolve_state_dir(checkout.path());
        assert_eq!(resolved, relocated.path());
        Ok(())
    }

    #[sinex_test]
    async fn resolve_state_dir_falls_back_to_in_checkout() -> TestResult<()> {
        // Unset env (CI, bare `nix develop`) → in-checkout `.sinex/`, preserving
        // pre-relocation behavior.
        let checkout = tempfile::tempdir()?;
        let mut env = EnvGuard::with_keys(&["SINEX_DEV_STATE_DIR"]);
        env.clear("SINEX_DEV_STATE_DIR");

        let resolved = CheckoutState::resolve_state_dir(checkout.path());
        assert_eq!(resolved, checkout.path().join(".sinex"));
        Ok(())
    }

    #[sinex_test]
    async fn test_lock_info_current() -> TestResult<()> {
        let info = LockInfo::current(PathBuf::from("/test/path"), Some("test".to_string()));
        assert_eq!(info.pid, std::process::id());
        assert_eq!(info.checkout_path, PathBuf::from("/test/path"));
        assert!(info.is_alive()); // Current process should be alive
        Ok(())
    }

    #[sinex_test]
    async fn test_lock_info_dead_process() -> TestResult<()> {
        let info = LockInfo {
            pid: 99_999_999, // Very unlikely to exist
            checkout_path: PathBuf::from("/test"),
            acquired_at: Timestamp::now(),
            description: None,
        };
        // This might actually be alive in rare cases, but usually not
        // Just test that is_alive() doesn't crash
        let _ = info.is_alive();
        Ok(())
    }

    #[sinex_test]
    async fn test_remove_lock_file_reports_remove_failures() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let error = remove_lock_file(temp.path(), "test lock").unwrap_err();
        assert!(format!("{error:#}").contains("failed to remove test lock"));
        Ok(())
    }

    #[sinex_test]
    async fn test_release_lock_reports_malformed_lock_file() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let state = CheckoutState::new(temp.path().to_path_buf())?;
        fs::create_dir_all(state.state_dir())?;
        fs::write(state.lock_file(), "not json")?;

        let error = state.release_lock().unwrap_err();
        assert!(format!("{error:#}").contains("Failed to parse lock file during release"));
        Ok(())
    }

    #[sinex_test]
    async fn inventory_roots_under_maps_dev_state_locks_without_cleanup() -> TestResult<()> {
        let base = tempfile::tempdir()?;
        let checkout = tempfile::tempdir()?;
        let dev_state = base.path().join("hash123/dev-state");
        fs::create_dir_all(&dev_state)?;
        let lock = LockInfo::current(checkout.path().to_path_buf(), Some("infra".to_string()));
        fs::write(
            dev_state.join(".lock"),
            serde_json::to_string_pretty(&lock)?,
        )?;

        let roots = CheckoutState::inventory_roots_under(base.path())?;

        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].dev_state_dir, dev_state);
        assert_eq!(roots[0].checkout_path.as_deref(), Some(checkout.path()));
        assert!(matches!(roots[0].lock, LockInspection::Live(_)));
        assert!(roots[0].dev_state_dir.join(".lock").exists());
        Ok(())
    }

    #[sinex_test]
    async fn inventory_roots_under_reports_malformed_locks() -> TestResult<()> {
        let base = tempfile::tempdir()?;
        let dev_state = base.path().join("hash123/dev-state");
        fs::create_dir_all(&dev_state)?;
        fs::write(dev_state.join(".lock"), "not json")?;

        let roots = CheckoutState::inventory_roots_under(base.path())?;

        assert_eq!(roots.len(), 1);
        assert!(roots[0].checkout_path.is_none());
        assert!(matches!(roots[0].lock, LockInspection::Malformed(_)));
        Ok(())
    }
}
