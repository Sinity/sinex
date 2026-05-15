//! Staging-directory RAII wrapper.
//!
//! Creates a temporary working tree `<output_parent>/.sinex-snapshot-staging-{id}/`
//! and removes it automatically when the guard is dropped or on explicit cleanup.

use color_eyre::eyre::{Context, Result};
use std::path::{Path, PathBuf};

/// RAII guard that owns the staging directory lifetime.
///
/// The directory is removed when the guard is dropped.  Call [`keep`] to
/// prevent removal (use only after a successful archive creation).
pub struct StagingDir {
    path: PathBuf,
    keep: bool,
}

impl StagingDir {
    /// Create the staging directory under `parent/.sinex-snapshot-staging-{id}`.
    pub fn create(parent: &Path, snapshot_id: &str) -> Result<Self> {
        let path = parent.join(format!(".sinex-snapshot-staging-{snapshot_id}"));
        std::fs::create_dir_all(&path)
            .with_context(|| format!("create staging directory {}", path.display()))?;
        Ok(Self { path, keep: false })
    }

    /// Return the path to the staging directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Create a component subdirectory inside staging and return its path.
    pub fn component_dir(&self, component: &str) -> Result<PathBuf> {
        let dir = self.path.join(component);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create component staging dir {}", dir.display()))?;
        Ok(dir)
    }

    /// Persist the staging directory instead of removing it on drop.
    ///
    /// Call this only after the archive has been successfully created and
    /// verified.
    pub fn keep(&mut self) {
        self.keep = true;
    }

    /// Explicitly remove the staging directory.  Idempotent.
    pub fn cleanup(&mut self) -> Result<()> {
        if self.path.exists() {
            std::fs::remove_dir_all(&self.path)
                .with_context(|| format!("remove staging directory {}", self.path.display()))?;
        }
        self.keep = true; // Don't try again in drop.
        Ok(())
    }
}

impl Drop for StagingDir {
    fn drop(&mut self) {
        if !self.keep && self.path.exists() {
            // Best-effort cleanup; don't panic in a destructor.
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
