//! Per-checkout development state paths.
//!
//! AgentCTL owns service lifetime and cross-checkout isolation. This module
//! only resolves the state directory used by a leased foreground job.

use color_eyre::eyre::{Result, WrapErr};
use std::fs;
use std::path::{Path, PathBuf};

pub struct CheckoutState {
    checkout_root: PathBuf,
    state_dir: PathBuf,
}

impl CheckoutState {
    const STATE_DIR_NAME: &'static str = ".sinex";

    pub fn for_current_checkout() -> Result<Self> {
        let checkout_root = Self::find_checkout_root()?;
        let state_dir = Self::resolve_state_dir(&checkout_root);
        Ok(Self { checkout_root, state_dir })
    }

    fn resolve_state_dir(checkout_root: &Path) -> PathBuf {
        crate::config::workspace_pinned_env_path("SINEX_DEV_STATE_DIR", checkout_root, || {
            checkout_root.join(Self::STATE_DIR_NAME)
        })
    }

    fn find_checkout_root() -> Result<PathBuf> {
        let cwd = std::env::current_dir().context("failed to get current directory")?;
        let mut current = cwd.as_path();
        loop {
            if current.join(".git").exists() {
                return Ok(current.to_path_buf());
            }
            current = current.parent().ok_or_else(|| {
                color_eyre::eyre::eyre!("not in a git repository; run from within the sinex checkout")
            })?;
        }
    }

    #[must_use]
    pub fn checkout_root(&self) -> &Path { &self.checkout_root }

    #[must_use]
    pub fn state_dir(&self) -> &Path { &self.state_dir }

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

    #[must_use]
    pub fn data_dir(&self) -> PathBuf { self.state_dir.join("data") }
    #[must_use]
    pub fn run_dir(&self) -> PathBuf { self.state_dir.join("run") }
    #[must_use]
    pub fn logs_dir(&self) -> PathBuf { self.run_dir().join("logs") }
    #[must_use]
    pub fn snapshots_dir(&self) -> PathBuf { self.state_dir.join("snapshots") }
    #[must_use]
    pub fn config_dir(&self) -> PathBuf { self.state_dir.join("config") }
    #[must_use]
    pub fn pg_data(&self) -> PathBuf { self.data_dir().join("postgres") }
    #[must_use]
    pub fn nats_data(&self) -> PathBuf { self.data_dir().join("nats") }
    #[must_use]
    pub fn annex_data(&self) -> PathBuf { self.data_dir().join("annex") }
}
