//! Test resource management utilities
//!
//! This module provides secure utilities for creating temporary files and directories
//! in test environments. All functions use validated paths and safe temporary directory
//! practices.

#![allow(clippy::result_large_err)]

use super::path_validation::{create_test_temp_dir, validate_test_path};
use crate::sandbox::prelude::*;
use camino::{Utf8Path, Utf8PathBuf};

use std::path::Path;
use tempfile::TempDir;

/// Per-test temporary directory redirect.
///
/// Keeps `tempfile`, `std::env::temp_dir()`, and subprocesses on a
/// checkout-backed `.sinex/test-tmp` tree instead of the host `/tmp`.
pub struct TestTempEnv {
    _env: super::EnvGuard,
    _dir: TempDir,
}

/// Redirect temporary-directory environment variables for the duration of a test.
pub fn prepare_test_temp_env(test_name: &str) -> TestResult<TestTempEnv> {
    let temp_root = workspace_test_temp_root()?;
    let safe_name = sanitize_filename(test_name);
    let dir = tempfile::Builder::new()
        .prefix(&format!("sinex-test-{safe_name}-"))
        .tempdir_in(temp_root.as_std_path())
        .map_err(|e| eyre!(format!("Failed to create workspace-backed temp directory: {e}")))?;

    let mut env = super::EnvGuard::with_keys(&["TMPDIR", "TMP", "TEMP"]);
    env.set("TMPDIR", dir.path());
    env.set("TMP", dir.path());
    env.set("TEMP", dir.path());

    Ok(TestTempEnv {
        _env: env,
        _dir: dir,
    })
}

/// Create a secure temporary directory for test operations
///
/// This function creates a temporary directory using the system's temporary
/// directory with proper validation and cleanup.
pub fn temp_dir() -> TestResult<TempDir> {
    tempfile::tempdir().map_err(|e| eyre!(format!("Failed to create temporary directory: {e}")))
}

/// Create a test file with validated path and content
///
/// This function creates a file within a validated directory structure.
/// The parent directory must exist and be validated.
pub fn create_test_file(
    parent_dir: &Path,
    filename: &str,
    content: &str,
) -> TestResult<Utf8PathBuf> {
    // Convert std::path::Path to camino::Utf8Path
    let utf8_parent = Utf8Path::from_path(parent_dir)
        .ok_or_else(|| eyre!("Parent directory path is not valid UTF-8"))?;

    // Validate the parent directory path
    validate_test_path(utf8_parent.as_str())?;

    // Create the file path
    let file_path = utf8_parent.join(filename);

    // Validate the final file path
    validate_test_path(file_path.as_str())?;

    // Write the content to the file
    std::fs::write(&file_path, content)
        .map_err(|e| eyre!(format!("Failed to write test file: {e}")))?;

    Ok(file_path)
}

/// Create a secure test directory with a specific name
///
/// This creates a subdirectory within the system temporary directory
/// with proper validation and security checks.
pub fn create_secure_test_dir(test_name: &str) -> TestResult<Utf8PathBuf> {
    create_test_temp_dir(test_name).map_err(Error::from)
}

/// Create a test file with auto-generated safe filename
///
/// This function creates a test file with a filename that's automatically
/// sanitized and placed in a secure temporary directory.
pub fn create_temp_test_file(test_name: &str, content: &str) -> TestResult<Utf8PathBuf> {
    let temp_dir = create_test_temp_dir(test_name)?;
    let filename = format!("{test_name}.txt");
    let file_path = temp_dir.join(filename);

    // Write the content
    std::fs::write(&file_path, content)
        .map_err(|e| eyre!(format!("Failed to write temporary test file: {e}")))?;

    Ok(file_path)
}

/// Create a test binary file with specific content
///
/// Similar to `create_test_file` but for binary content.
pub fn create_test_binary_file(
    parent_dir: &Path,
    filename: &str,
    content: &[u8],
) -> TestResult<Utf8PathBuf> {
    // Convert std::path::Path to camino::Utf8Path
    let utf8_parent = Utf8Path::from_path(parent_dir)
        .ok_or_else(|| eyre!("Parent directory path is not valid UTF-8"))?;

    // Validate the parent directory path
    validate_test_path(utf8_parent.as_str())?;

    // Create the file path
    let file_path = utf8_parent.join(filename);

    // Validate the final file path
    validate_test_path(file_path.as_str())?;

    // Write the binary content to the file
    std::fs::write(&file_path, content)
        .map_err(|e| eyre!(format!("Failed to write test binary file: {e}")))?;

    Ok(file_path)
}

/// Verify that a path is safe for test file operations
///
/// This is a convenience function that wraps the path validation
/// for use in test resource management.
pub fn verify_test_path_safety(path: &str) -> TestResult<()> {
    validate_test_path(path).map(|_| ()).map_err(Error::from)
}

fn workspace_test_temp_root() -> TestResult<Utf8PathBuf> {
    let manifest_dir = Utf8Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .ok_or_else(|| eyre!("xtask manifest directory has no workspace parent"))?;
    let temp_root = workspace_root.join(".sinex/test-tmp");
    std::fs::create_dir_all(temp_root.as_std_path()).map_err(|e| {
        eyre!(format!(
            "Failed to create workspace-backed test temp root {}: {e}",
            temp_root
        ))
    })?;
    Ok(temp_root)
}

fn sanitize_filename(filename: &str) -> String {
    filename
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect::<String>()
        .trim_matches('.')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_temp_dir_creation() -> ::xtask::sandbox::TestResult<()> {
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

        Ok(())
    }

    #[sinex_test]
    async fn test_create_test_file() -> ::xtask::sandbox::TestResult<()> {
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
    async fn test_create_secure_test_dir() -> ::xtask::sandbox::TestResult<()> {
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
    async fn test_create_temp_test_file() -> ::xtask::sandbox::TestResult<()> {
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
    async fn test_create_test_binary_file() -> ::xtask::sandbox::TestResult<()> {
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
    async fn test_path_validation_rejection() -> ::xtask::sandbox::TestResult<()> {
        // These should be rejected by the validation
        let dangerous_paths = ["/etc/passwd", "../../../etc/shadow", "/bin/sh", ""];

        for path in &dangerous_paths {
            let result = verify_test_path_safety(path);
            assert!(result.is_err(), "Should reject dangerous path: {path}");
        }

        Ok(())
    }
}
