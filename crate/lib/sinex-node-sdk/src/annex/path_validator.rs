//! Path validation utilities for blob manager operations
//!
//! This module provides secure path validation to prevent directory traversal
//! attacks and ensure all file paths are properly sanitized before use.

use std::ops::Deref;

use crate::{NodeResult, SinexError};
use camino::{Utf8Path, Utf8PathBuf};
use sinex_primitives::validation::validate_path;

/// Path that has passed security validation.
#[derive(Debug, Clone)]
pub struct VerifiedPath(Utf8PathBuf);

impl VerifiedPath {
    /// Parse and validate a string path into a [`VerifiedPath`].
    pub fn parse(path: &str) -> NodeResult<Self> {
        validate_and_convert_path(path).map(Self)
    }

    /// Validate an existing [`Utf8Path`] reference and wrap it as [`VerifiedPath`].
    pub fn from_utf8_path(path: &Utf8Path) -> NodeResult<Self> {
        Self::parse(path.as_str())
    }

    /// Access the inner [`Utf8Path`].
    pub fn as_path(&self) -> &Utf8Path {
        &self.0
    }

    /// Consume the wrapper and return the owned [`Utf8PathBuf`].
    pub fn into_path_buf(self) -> Utf8PathBuf {
        self.0
    }
}

impl Deref for VerifiedPath {
    type Target = Utf8Path;

    fn deref(&self) -> &Self::Target {
        self.as_path()
    }
}

impl AsRef<Utf8Path> for VerifiedPath {
    fn as_ref(&self) -> &Utf8Path {
        self.as_path()
    }
}

/// Validates and converts a string path to a secure Utf8PathBuf
pub fn validate_and_convert_path(path: &str) -> NodeResult<Utf8PathBuf> {
    // First validate the path for security
    let validated_path = validate_path(path)
        .map_err(|e| SinexError::validation(format!("Path validation failed for {path}: {e}")))?;

    Ok(validated_path)
}

/// Validates a path exists and is accessible
pub fn validate_path_exists(path: &Utf8Path) -> NodeResult<()> {
    if !path.exists() {
        return Err(SinexError::validation(format!(
            "Path does not exist: {path}"
        )));
    }

    Ok(())
}

/// Creates a secure temporary file path with validation
pub fn create_secure_temp_path(prefix: &str, extension: &str) -> NodeResult<Utf8PathBuf> {
    let temp_dir = std::env::temp_dir();

    // Validate temp directory path
    let temp_dir_str = temp_dir.to_string_lossy();
    let validated_temp_dir = validate_path(&temp_dir_str).map_err(|e| {
        SinexError::validation(format!("Failed to validate temp directory path: {e}"))
    })?;

    let filename = format!("{}_{}.{}", prefix, uuid::Uuid::new_v4(), extension);
    let temp_path = validated_temp_dir.join(filename);

    Ok(temp_path)
}
