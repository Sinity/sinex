//! Directory management utilities
//!
//! This module provides utilities for managing directories with
//! consistent error handling and permissions.

use crate::error::{Result, SinexError};
use crate::filesystem;
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::{debug, info};

/// Configuration for directory operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryConfig {
    /// Base directory for operations
    pub base_path: Utf8PathBuf,
    /// Default permissions for created directories
    pub default_permissions: u32,
    /// Whether to create parent directories automatically
    pub create_parents: bool,
}

impl Default for DirectoryConfig {
    fn default() -> Self {
        Self {
            base_path: Utf8PathBuf::from("."),
            default_permissions: filesystem::DEFAULT_DIR_PERMISSIONS,
            create_parents: true,
        }
    }
}

/// Directory management operations
pub struct DirectoryManager {
    config: DirectoryConfig,
}

impl DirectoryManager {
    /// Create a new directory manager
    #[must_use]
    pub fn new(config: DirectoryConfig) -> Self {
        Self { config }
    }

    /// Create a directory with default permissions
    pub async fn create_directory<P: AsRef<Utf8Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let full_path = self.config.base_path.join(path);

        if self.config.create_parents {
            fs::create_dir_all(&full_path).await.map_err(|e| {
                SinexError::io(format!("Failed to create directory: {e}"))
                    .with_path(&full_path)
                    .with_operation("create_directory_all")
                    .with_context("create_parents", true)
            })?;
        } else {
            fs::create_dir(&full_path).await.map_err(|e| {
                SinexError::io(format!("Failed to create directory: {e}"))
                    .with_path(&full_path)
                    .with_operation("create_directory")
                    .with_context("create_parents", false)
            })?;
        }

        debug!("Created directory: {:?}", full_path);
        Ok(())
    }

    /// Remove a directory and all its contents
    pub async fn remove_directory<P: AsRef<Utf8Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let full_path = self.config.base_path.join(path);

        fs::remove_dir_all(&full_path).await.map_err(|e| {
            SinexError::io(format!("Failed to remove directory: {e}"))
                .with_path(&full_path)
                .with_operation("remove_directory")
        })?;

        debug!("Removed directory: {:?}", full_path);
        Ok(())
    }

    /// Check if a directory exists
    pub async fn directory_exists<P: AsRef<Utf8Path>>(&self, path: P) -> Result<bool> {
        let path = path.as_ref();
        let full_path = self.config.base_path.join(path);

        match fs::metadata(&full_path).await {
            Ok(metadata) => Ok(metadata.is_dir()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(SinexError::io(format!(
                "Failed to check directory {} (operation: directory_exists): {}",
                full_path.as_str(),
                e
            ))),
        }
    }

    /// List directory contents
    pub async fn list_directory<P: AsRef<Utf8Path>>(&self, path: P) -> Result<Vec<Utf8PathBuf>> {
        let path = path.as_ref();
        let full_path = self.config.base_path.join(path);

        let mut entries = Vec::new();
        let mut dir_entries = fs::read_dir(&full_path).await.map_err(|e| {
            SinexError::io(format!(
                "Failed to read directory {} (operation: list_directory): {}",
                full_path.as_str(),
                e
            ))
        })?;

        while let Some(entry) = dir_entries.next_entry().await.map_err(|e| {
            SinexError::io(format!(
                "Failed to read directory entry in {} (operation: read_directory_entry): {}",
                full_path.as_str(),
                e
            ))
        })? {
            let path = entry.path();
            if let Ok(utf8_path) = Utf8PathBuf::from_path_buf(path) {
                entries.push(utf8_path);
            }
        }

        Ok(entries)
    }

    /// Ensure a directory exists, creating it if necessary
    pub async fn ensure_directory<P: AsRef<Utf8Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();

        if !self.directory_exists(path).await? {
            self.create_directory(path).await?;
            info!("Created directory: {:?}", path);
        }

        Ok(())
    }

    /// Get the base path
    #[must_use]
    pub fn base_path(&self) -> &Utf8Path {
        &self.config.base_path
    }

    /// Get the configuration
    #[must_use]
    pub fn config(&self) -> &DirectoryConfig {
        &self.config
    }
}
