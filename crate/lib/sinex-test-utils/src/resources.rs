//! Test resource management utilities
//!
//! This module provides secure utilities for creating temporary files and directories
//! in test environments. All functions use validated paths and safe temporary directory
//! practices.

#![allow(clippy::result_large_err)]

use crate::{
    path_validation::{create_test_temp_dir, validate_test_path},
    Result,
};
use camino::{Utf8Path, Utf8PathBuf};
use sinex_core::types::error::SinexError;
use std::path::Path;
use tempfile::TempDir;

/// Create a secure temporary directory for test operations
///
/// This function creates a temporary directory using the system's temporary
/// directory with proper validation and cleanup.
pub fn temp_dir() -> Result<TempDir> {
    tempfile::tempdir()
        .map_err(|e| SinexError::io(format!("Failed to create temporary directory: {e}")))
}

/// Create a test file with validated path and content
///
/// This function creates a file within a validated directory structure.
/// The parent directory must exist and be validated.
pub fn create_test_file(parent_dir: &Path, filename: &str, content: &str) -> Result<Utf8PathBuf> {
    // Convert std::path::Path to camino::Utf8Path
    let utf8_parent = Utf8Path::from_path(parent_dir)
        .ok_or_else(|| SinexError::validation("Parent directory path is not valid UTF-8"))?;

    // Validate the parent directory path
    validate_test_path(utf8_parent.as_str())?;

    // Create the file path
    let file_path = utf8_parent.join(filename);

    // Validate the final file path
    validate_test_path(file_path.as_str())?;

    // Write the content to the file
    std::fs::write(&file_path, content)
        .map_err(|e| SinexError::io(format!("Failed to write test file: {e}")))?;

    Ok(file_path)
}

/// Create a secure test directory with a specific name
///
/// This creates a subdirectory within the system temporary directory
/// with proper validation and security checks.
pub fn create_secure_test_dir(test_name: &str) -> Result<Utf8PathBuf> {
    create_test_temp_dir(test_name)
}

/// Create a test file with auto-generated safe filename
///
/// This function creates a test file with a filename that's automatically
/// sanitized and placed in a secure temporary directory.
pub fn create_temp_test_file(test_name: &str, content: &str) -> Result<Utf8PathBuf> {
    let temp_dir = create_test_temp_dir(test_name)?;
    let filename = format!("{test_name}.txt");
    let file_path = temp_dir.join(filename);

    // Write the content
    std::fs::write(&file_path, content)
        .map_err(|e| SinexError::io(format!("Failed to write temporary test file: {e}")))?;

    Ok(file_path)
}

/// Create a test binary file with specific content
///
/// Similar to create_test_file but for binary content.
pub fn create_test_binary_file(
    parent_dir: &Path,
    filename: &str,
    content: &[u8],
) -> Result<Utf8PathBuf> {
    // Convert std::path::Path to camino::Utf8Path
    let utf8_parent = Utf8Path::from_path(parent_dir)
        .ok_or_else(|| SinexError::validation("Parent directory path is not valid UTF-8"))?;

    // Validate the parent directory path
    validate_test_path(utf8_parent.as_str())?;

    // Create the file path
    let file_path = utf8_parent.join(filename);

    // Validate the final file path
    validate_test_path(file_path.as_str())?;

    // Write the binary content to the file
    std::fs::write(&file_path, content)
        .map_err(|e| SinexError::io(format!("Failed to write test binary file: {e}")))?;

    Ok(file_path)
}

/// Verify that a path is safe for test file operations
///
/// This is a convenience function that wraps the path validation
/// for use in test resource management.
pub fn verify_test_path_safety(path: &str) -> Result<()> {
    validate_test_path(path).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;

    #[sinex_test]
    async fn test_temp_dir_creation() -> color_eyre::eyre::Result<()> {
        let temp_dir = temp_dir()?;

        // Directory should exist and be accessible
        assert!(temp_dir.path().exists());
        assert!(temp_dir.path().is_dir());

        // Should be in system temp directory
        let system_temp = std::env::temp_dir();
        assert!(temp_dir.path().starts_with(&system_temp));

        // Directory should be automatically cleaned up when dropped
        let _temp_path = temp_dir.path().to_path_buf();
        drop(temp_dir);

        // Note: Cleanup happens when temp_dir is dropped
        // In some systems, it might take a moment to clean up

        Ok(())
    }

    #[sinex_test]
    async fn test_create_test_file() -> color_eyre::eyre::Result<()> {
        let temp_dir = temp_dir()?;
        let content = "Test file content for validation";

        let file_path = create_test_file(temp_dir.path(), "test.txt", content)?;

        // File should exist
        assert!(file_path.exists());

        // Content should match
        let read_content = std::fs::read_to_string(&file_path)?;
        assert_eq!(read_content, content);

        // Path should be validated
        assert!(verify_test_path_safety(file_path.as_str()).is_ok());

        Ok(())
    }

    #[sinex_test]
    async fn test_create_secure_test_dir() -> color_eyre::eyre::Result<()> {
        let test_dir = create_secure_test_dir("resources_test")?;

        // Directory should exist
        assert!(test_dir.exists());
        assert!(test_dir.is_dir());

        // Should be in a secure temp location
        assert!(test_dir.as_str().contains("sinex-test"));

        // Should be validated
        assert!(verify_test_path_safety(test_dir.as_str()).is_ok());

        Ok(())
    }

    #[sinex_test]
    async fn test_create_temp_test_file() -> color_eyre::eyre::Result<()> {
        let content = "Temporary test file content";
        let file_path = create_temp_test_file("temp_file_test", content)?;

        // File should exist
        assert!(file_path.exists());

        // Content should match
        let read_content = std::fs::read_to_string(&file_path)?;
        assert_eq!(read_content, content);

        // Should have expected structure
        assert!(file_path.as_str().contains("temp_file_test"));

        Ok(())
    }

    #[sinex_test]
    async fn test_create_test_binary_file() -> color_eyre::eyre::Result<()> {
        let temp_dir = temp_dir()?;
        let binary_content = b"Binary test content\x00\x01\x02\xFF";

        let file_path =
            create_test_binary_file(temp_dir.path(), "binary_test.bin", binary_content)?;

        // File should exist
        assert!(file_path.exists());

        // Content should match exactly
        let read_content = std::fs::read(&file_path)?;
        assert_eq!(read_content, binary_content);

        Ok(())
    }

    #[sinex_test]
    async fn test_path_validation_rejection() -> color_eyre::eyre::Result<()> {
        // These should be rejected by the validation
        let dangerous_paths = ["/etc/passwd", "../../../etc/shadow", "/bin/sh", ""];

        for path in &dangerous_paths {
            let result = verify_test_path_safety(path);
            assert!(result.is_err(), "Should reject dangerous path: {path}");
        }

        Ok(())
    }
}
