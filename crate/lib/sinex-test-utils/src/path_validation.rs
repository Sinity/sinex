//! Path validation utilities for test infrastructure
//!
//! This module provides secure path validation for test utilities to prevent:
//! - Path traversal attacks in test files
//! - Creation of files outside safe test directories
//! - Unsafe temporary file operations
//!
//! Key principles:
//! - All test file operations should use validated paths
//! - Temporary files should be created in secure, bounded locations
//! - Test cleanup should only delete files within safe boundaries

use camino::{Utf8Path, Utf8PathBuf};
use sinex_core::types::domain::SanitizedPath;
use sinex_core::types::error::SinexError;
use std::env;
use std::str::FromStr;

/// Result type for path validation operations
pub type PathValidationResult<T> = Result<T, SinexError>;

/// Validates a path for use in test operations
///
/// This provides additional validation on top of SanitizedPath specifically
/// for test environments, including:
/// - Ensuring paths are within safe test directories
/// - Preventing access to system-critical paths
/// - Validating temporary directory usage
pub fn validate_test_path(path: &str) -> PathValidationResult<SanitizedPath> {
    // First, use the core SanitizedPath validation
    let sanitized = SanitizedPath::from_str(path)
        .map_err(|e| SinexError::validation(format!("Path validation failed: {}", e)))?;

    let utf8_path = Utf8Path::new(path);

    // Additional test-specific validations
    validate_not_system_critical(utf8_path)?;
    validate_not_root_directory(utf8_path)?;
    validate_reasonable_depth(utf8_path)?;

    Ok(sanitized)
}

/// Creates a validated temporary directory for test operations
///
/// Returns a path that is:
/// - Within the system temporary directory
/// - Prefixed with 'sinex-test-' for easy identification
/// - Safe for test file operations
pub fn create_test_temp_dir(test_name: &str) -> PathValidationResult<Utf8PathBuf> {
    let temp_base = get_safe_temp_base()?;
    let test_dir_name = format!("sinex-test-{}-{}", test_name, uuid::Uuid::new_v4());
    let test_temp_dir = temp_base.join(test_dir_name);

    // Create the directory
    std::fs::create_dir_all(&test_temp_dir)
        .map_err(|e| SinexError::io(format!("Failed to create test temp dir: {}", e)))?;

    Ok(test_temp_dir)
}

/// Creates a validated temporary file path for test operations
///
/// The file path will be within a safe temporary directory and can be
/// used for test file creation without security concerns.
pub fn create_test_temp_file(test_name: &str, filename: &str) -> PathValidationResult<Utf8PathBuf> {
    let test_dir = create_test_temp_dir(test_name)?;
    let file_path = test_dir.join(sanitize_filename(filename));

    // Validate the final path
    validate_test_path(file_path.as_str())?;

    Ok(file_path)
}

/// Safely removes a test directory and all its contents
///
/// This function includes additional safety checks to ensure we only
/// remove directories that are clearly test-related and within safe boundaries.
pub fn remove_test_dir(dir_path: &Utf8Path) -> PathValidationResult<()> {
    // Validate this looks like a test directory
    validate_is_test_directory(dir_path)?;

    // Remove the directory
    std::fs::remove_dir_all(dir_path)
        .map_err(|e| SinexError::io(format!("Failed to remove test directory: {}", e)))?;

    Ok(())
}

/// Validates that a path represents a test directory that's safe to remove
fn validate_is_test_directory(path: &Utf8Path) -> PathValidationResult<()> {
    let path_str = path.as_str();

    // Must contain 'sinex-test' or be in a temp directory
    let temp_base = get_safe_temp_base()?;
    let is_in_temp = path.starts_with(&temp_base);
    let is_test_named = path_str.contains("sinex-test") || path_str.contains("test-");

    if !is_in_temp || !is_test_named {
        return Err(SinexError::validation(
            "Directory does not appear to be a safe test directory",
        ));
    }

    // Additional safety: check path depth
    if path.components().count() > 10 {
        return Err(SinexError::validation(
            "Test directory path is suspiciously deep",
        ));
    }

    Ok(())
}

/// Gets a safe base directory for temporary test files
fn get_safe_temp_base() -> PathValidationResult<Utf8PathBuf> {
    // Try to get system temp directory
    let temp_dir = env::temp_dir();
    let utf8_temp = Utf8PathBuf::from_path_buf(temp_dir)
        .map_err(|_| SinexError::validation("System temp directory is not valid UTF-8"))?;

    // Create sinex-specific temp subdirectory
    let sinex_temp = utf8_temp.join("sinex-tests");

    // Ensure it exists
    std::fs::create_dir_all(&sinex_temp)
        .map_err(|e| SinexError::io(format!("Failed to create sinex temp directory: {}", e)))?;

    Ok(sinex_temp)
}

/// Validates that a path is not pointing to system-critical directories
fn validate_not_system_critical(path: &Utf8Path) -> PathValidationResult<()> {
    let path_str = path.as_str();

    // List of critical system paths that tests should never access
    let forbidden_paths = [
        "/etc",
        "/bin",
        "/sbin",
        "/usr/bin",
        "/usr/sbin",
        "/boot",
        "/dev",
        "/proc",
        "/sys",
        "/run",
        "/var/lib",
        "/var/log",
        "/opt",
        "/root",
        "/home", // Don't allow direct access to user homes
    ];

    for forbidden in &forbidden_paths {
        if path_str.starts_with(forbidden) {
            return Err(SinexError::validation(format!(
                "Test paths cannot access system directory: {}",
                forbidden
            )));
        }
    }

    Ok(())
}

/// Validates that a path is not the root directory
fn validate_not_root_directory(path: &Utf8Path) -> PathValidationResult<()> {
    if path.as_str() == "/" || path.as_str().is_empty() {
        return Err(SinexError::validation(
            "Test paths cannot be root directory or empty",
        ));
    }

    Ok(())
}

/// Validates that a path doesn't have excessive depth (potential indicator of malicious path)
fn validate_reasonable_depth(path: &Utf8Path) -> PathValidationResult<()> {
    let component_count = path.components().count();

    if component_count > 20 {
        return Err(SinexError::validation(
            "Path depth exceeds reasonable limits for test operations",
        ));
    }

    Ok(())
}

/// Sanitizes a filename for safe usage
fn sanitize_filename(filename: &str) -> String {
    // Remove or replace potentially dangerous characters
    filename
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect::<String>()
        .trim_matches('.') // Remove leading/trailing dots
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;

    #[sinex_test]
    async fn test_validate_test_path_accepts_safe_paths() -> color_eyre::eyre::Result<()> {
        // These should be accepted (assuming /tmp exists)
        let temp_path = format!("{}/test-file.txt", env::temp_dir().to_string_lossy());
        assert!(validate_test_path(&temp_path).is_ok());

        Ok(())
    }

    #[sinex_test]
    async fn test_validate_test_path_rejects_dangerous_paths() -> color_eyre::eyre::Result<()> {
        // These should be rejected
        let dangerous_paths = [
            "/etc/passwd",
            "/bin/sh",
            "/root/.ssh/authorized_keys",
            "/var/log/system.log",
            "../../../etc/passwd",
            "",
            "/",
        ];

        for path in &dangerous_paths {
            let result = validate_test_path(path);
            assert!(result.is_err(), "Path should be rejected: {}", path);
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_create_test_temp_dir() -> color_eyre::eyre::Result<()> {
        let temp_dir = create_test_temp_dir("path_validation_test")?;

        // Directory should exist
        assert!(temp_dir.exists());
        assert!(temp_dir.is_dir());

        // Should be in temp directory
        let system_temp = env::temp_dir();
        assert!(temp_dir.starts_with(&system_temp));

        // Should contain test identifier
        assert!(temp_dir.as_str().contains("sinex-test"));
        assert!(temp_dir.as_str().contains("path_validation_test"));

        // Clean up
        remove_test_dir(&temp_dir)?;
        assert!(!temp_dir.exists());

        Ok(())
    }

    #[sinex_test]
    async fn test_create_test_temp_file() -> color_eyre::eyre::Result<()> {
        let temp_file = create_test_temp_file("file_test", "test-data.txt")?;

        // File path should be valid
        assert!(validate_test_path(temp_file.as_str()).is_ok());

        // Parent directory should exist
        assert!(temp_file.parent().unwrap().exists());

        // Should contain sanitized filename
        assert!(temp_file.file_name().unwrap().contains("test-data"));

        // Clean up directory
        if let Some(parent) = temp_file.parent() {
            remove_test_dir(parent)?;
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_sanitize_filename() -> color_eyre::eyre::Result<()> {
        let test_cases = [
            ("normal_file.txt", "normal_file.txt"),
            ("file/with/slashes.txt", "file_with_slashes.txt"),
            ("file:with:colons.txt", "file_with_colons.txt"),
            ("file\"with\"quotes.txt", "file_with_quotes.txt"),
            ("..dangerous", "_dangerous"),
            ("also_dangerous..", "also_dangerous"),
        ];

        for (input, expected) in &test_cases {
            let result = sanitize_filename(input);
            assert_eq!(&result, expected, "Failed to sanitize: {}", input);
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_remove_test_dir_safety() -> color_eyre::eyre::Result<()> {
        // Create a legitimate test directory
        let test_dir = create_test_temp_dir("removal_test")?;

        // Should allow removal of test directory
        assert!(remove_test_dir(&test_dir).is_ok());

        // Should reject removal of system directories
        let system_paths = [
            Utf8Path::new("/etc"),
            Utf8Path::new("/home"),
            Utf8Path::new("/usr"),
        ];

        for path in &system_paths {
            let result = remove_test_dir(path);
            assert!(result.is_err(), "Should reject removal of: {}", path);
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_path_depth_validation() -> color_eyre::eyre::Result<()> {
        // Reasonable depth should be fine
        let reasonable_path = "/tmp/sinex-test/sub1/sub2/sub3/file.txt";
        assert!(validate_test_path(reasonable_path).is_ok());

        // Excessive depth should be rejected
        let deep_path = "/tmp/".to_string() + &"deep/".repeat(25) + "file.txt";
        assert!(validate_test_path(&deep_path).is_err());

        Ok(())
    }
}
