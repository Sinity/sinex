//! Test resource management utilities
//!
//! This module provides secure utilities for creating temporary files and directories
//! in test environments. All functions use validated paths and safe temporary directory
//! practices.

#![allow(clippy::result_large_err)]

use super::path_validation::{create_test_temp_dir, validate_test_path};
use crate::sandbox::prelude::*;
use camino::{Utf8Path, Utf8PathBuf};
use std::collections::hash_map::DefaultHasher;
use std::ffi::OsString;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use tempfile::TempDir;

static TEMP_ENV_MUTEX: std::sync::LazyLock<Mutex<()>> = std::sync::LazyLock::new(|| Mutex::new(()));

/// Per-test temporary directory redirect.
///
/// Keeps `tempfile`, `std::env::temp_dir()`, and subprocesses on the
/// packaging-selected test temp root instead of the host `/tmp`.
pub struct TestTempEnv {
    lock: Option<MutexGuard<'static, ()>>,
    original: [(&'static str, Option<OsString>); 3],
    _dir: TempDir,
}

/// Redirect temporary-directory environment variables for the duration of a test.
pub fn prepare_test_temp_env(test_name: &str) -> TestResult<TestTempEnv> {
    let lock = TEMP_ENV_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp_root = workspace_test_temp_root()?;
    let dir = tempfile::Builder::new()
        .prefix(&short_test_temp_prefix(test_name))
        .tempdir_in(temp_root.as_std_path())
        .map_err(|e| eyre!("Failed to create test temp directory: {e}"))?;

    // `TMPDIR` / `TMP` / `TEMP` are process-global, so `cargo test` can race
    // parallel cases into each other's deleted temp roots. Hold a dedicated
    // temp-env mutex for the lifetime of the redirect while keeping the normal
    // EnvGuard mutex free for other per-test environment overrides.
    let original = [
        ("TMPDIR", std::env::var_os("TMPDIR")),
        ("TMP", std::env::var_os("TMP")),
        ("TEMP", std::env::var_os("TEMP")),
    ];

    unsafe {
        std::env::set_var("TMPDIR", dir.path());
        std::env::set_var("TMP", dir.path());
        std::env::set_var("TEMP", dir.path());
    }

    Ok(TestTempEnv {
        lock: Some(lock),
        original,
        _dir: dir,
    })
}

impl Drop for TestTempEnv {
    fn drop(&mut self) {
        unsafe {
            for (key, previous) in &self.original {
                if let Some(value) = previous {
                    std::env::set_var(key, value);
                } else {
                    std::env::remove_var(key);
                }
            }
        }
        self.lock.take();
    }
}

/// Create a secure temporary directory for test operations
///
/// This function creates a temporary directory using the system's temporary
/// directory with proper validation and cleanup.
pub fn temp_dir() -> TestResult<TempDir> {
    let root = workspace_test_temp_root()?;
    tempfile::Builder::new()
        .prefix("sinex-test-")
        .tempdir_in(root.as_std_path())
        .map_err(|e| eyre!("Failed to create temporary directory: {e}"))
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
    std::fs::write(&file_path, content).map_err(|e| eyre!("Failed to write test file: {e}"))?;

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
        .map_err(|e| eyre!("Failed to write temporary test file: {e}"))?;

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
        .map_err(|e| eyre!("Failed to write test binary file: {e}"))?;

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
    if let Some(configured) = std::env::var_os("SINEX_TEST_TMPDIR") {
        let temp_root = Utf8PathBuf::from_path_buf(configured.into())
            .map_err(|path| eyre!("SINEX_TEST_TMPDIR is not valid UTF-8: {path:?}"))?;
        std::fs::create_dir_all(temp_root.as_std_path())
            .map_err(|e| eyre!("Failed to create configured test temp root {temp_root}: {e}"))?;
        return Ok(temp_root);
    }

    let manifest_dir = Utf8Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .ok_or_else(|| eyre!("xtask manifest directory has no workspace parent"))?;
    let temp_root = workspace_root.join(".sinex/test-tmp");
    std::fs::create_dir_all(temp_root.as_std_path())
        .map_err(|e| eyre!("Failed to create workspace-backed test temp root {temp_root}: {e}"))?;
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

fn short_test_temp_prefix(test_name: &str) -> String {
    let mut hasher = DefaultHasher::new();
    test_name.hash(&mut hasher);
    let digest = hasher.finish();
    let slug = sanitize_filename(test_name)
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(12)
        .collect::<String>();
    if slug.is_empty() {
        format!("st-{digest:016x}-")
    } else {
        format!("st-{slug}-{digest:016x}-")
    }
}

#[cfg(test)]
#[path = "resources_test.rs"]
mod tests;
