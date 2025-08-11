//! Path validation utilities for blob manager operations
//!
//! This module provides secure path validation to prevent directory traversal
//! attacks and ensure all file paths are properly sanitized before use.

use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::eyre::{Context, Result};
use sinex_core::types::validate_path;

/// Validates and converts a string path to a secure Utf8PathBuf
pub fn validate_and_convert_path(path: &str) -> Result<Utf8PathBuf> {
    // First validate the path for security
    let validated_path =
        validate_path(path).with_context(|| format!("Path validation failed for: {}", path))?;

    Ok(validated_path)
}

/// Validates a path exists and is accessible
pub fn validate_path_exists(path: &Utf8Path) -> Result<()> {
    if !path.exists() {
        return Err(color_eyre::eyre::eyre!("Path does not exist: {}", path));
    }

    Ok(())
}

/// Creates a secure temporary file path with validation
pub fn create_secure_temp_path(prefix: &str, extension: &str) -> Result<Utf8PathBuf> {
    let temp_dir = std::env::temp_dir();

    // Validate temp directory path
    let temp_dir_str = temp_dir.to_string_lossy();
    let validated_temp_dir =
        validate_path(&temp_dir_str).context("Failed to validate temp directory path")?;

    let filename = format!("{}_{}.{}", prefix, uuid::Uuid::new_v4(), extension);
    let temp_path = validated_temp_dir.join(filename);

    Ok(temp_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    fn test_validate_and_convert_path() -> color_eyre::eyre::Result<()> {
        // Valid paths should work
        let valid_path = validate_and_convert_path("/tmp/test.txt")?;
        assert!(valid_path.to_string().contains("test.txt"));

        // Directory traversal should be rejected
        assert!(validate_and_convert_path("../../../etc/passwd").is_err());
        assert!(validate_and_convert_path("/path/../../../etc/passwd").is_err());

        Ok(())
    }

    #[sinex_test]
    fn test_create_secure_temp_path() -> color_eyre::eyre::Result<()> {
        let temp_path = create_secure_temp_path("sinex_blob", "tmp")?;

        // Should be in temp directory
        assert!(temp_path.to_string().contains("sinex_blob"));
        assert!(temp_path.extension().unwrap_or("") == "tmp");

        // Should not exist yet
        assert!(!temp_path.exists());

        Ok(())
    }
}
