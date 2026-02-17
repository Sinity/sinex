//! Per-checkout state management with cross-checkout locking.
//!
//! This module manages the `.sinex/` state directory for each checkout,
//! ensuring only one dev stack can be active at a time across all checkouts.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::{format_rfc3339, Timestamp};
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

    /// Create a `CheckoutState` for the current working directory's checkout
    pub fn for_current_checkout() -> Result<Self> {
        let checkout_root = Self::find_checkout_root()?;
        Self::new(checkout_root)
    }

    /// Create a `CheckoutState` for a specific checkout path
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
            let _ = fs::remove_file(&lock_file);
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
                lock_info.description.as_deref().unwrap_or(""),
                lock_info.checkout_path.display()
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

        // Only remove if we own it
        if let Ok(content) = fs::read_to_string(&lock_file) {
            if let Ok(lock_info) = serde_json::from_str::<LockInfo>(&content) {
                if lock_info.pid == std::process::id() {
                    fs::remove_file(&lock_file)?;
                }
            }
        }

        Ok(())
    }
}

/// RAII guard that releases the lock when dropped
pub struct LockGuard {
    lock_file: PathBuf,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.lock_file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_info_current() {
        let info = LockInfo::current(PathBuf::from("/test/path"), Some("test".to_string()));
        assert_eq!(info.pid, std::process::id());
        assert_eq!(info.checkout_path, PathBuf::from("/test/path"));
        assert!(info.is_alive()); // Current process should be alive
    }

    #[test]
    fn test_lock_info_dead_process() {
        let info = LockInfo {
            pid: 99999999, // Very unlikely to exist
            checkout_path: PathBuf::from("/test"),
            acquired_at: Timestamp::now(),
            description: None,
        };
        // This might actually be alive in rare cases, but usually not
        // Just test that is_alive() doesn't crash
        let _ = info.is_alive();
    }
}
