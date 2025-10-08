//! File watching security extensions for the validation module
//!
//! This module provides specialized security validation for file watching operations,
//! including path validation, security policies, and symlink protection.

use super::{validate_path, validate_path_within_root, Result, ValidationError};
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use std::collections::HashSet;

// ===== File Watching Security Module =====

/// Security policy for file watching operations
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileWatchingSecurityPolicy {
    /// Maximum depth for recursive watching (None = unlimited)
    pub max_watch_depth: Option<usize>,
    /// Paths that are completely forbidden to watch
    pub forbidden_paths: HashSet<PathBuf>,
    /// Path prefixes that are forbidden to watch
    pub forbidden_prefixes: HashSet<PathBuf>,
    /// Whether to follow symlinks
    pub follow_symlinks: bool,
    /// Maximum number of files to watch
    pub max_watched_files: Option<usize>,
    /// Whether to allow watching system directories
    pub allow_system_directories: bool,
}

impl Default for FileWatchingSecurityPolicy {
    fn default() -> Self {
        let mut forbidden_paths = HashSet::new();
        let mut forbidden_prefixes = HashSet::new();

        // Add common dangerous paths
        forbidden_paths.insert(PathBuf::from("/etc/shadow"));
        forbidden_paths.insert(PathBuf::from("/etc/passwd"));
        forbidden_paths.insert(PathBuf::from("/root"));

        // Add dangerous prefixes
        forbidden_prefixes.insert(PathBuf::from("/proc"));
        forbidden_prefixes.insert(PathBuf::from("/sys"));
        forbidden_prefixes.insert(PathBuf::from("/dev"));
        forbidden_prefixes.insert(PathBuf::from("/var/log"));
        forbidden_prefixes.insert(PathBuf::from("/etc"));

        Self {
            max_watch_depth: Some(10),
            forbidden_paths,
            forbidden_prefixes,
            follow_symlinks: false,
            max_watched_files: Some(100_000),
            allow_system_directories: true,
        }
    }
}

impl FileWatchingSecurityPolicy {
    /// Create a permissive policy for testing
    pub fn permissive() -> Self {
        Self {
            max_watch_depth: None,
            forbidden_paths: HashSet::new(),
            forbidden_prefixes: HashSet::new(),
            follow_symlinks: true,
            max_watched_files: None,
            allow_system_directories: true,
        }
    }

    /// Create a restrictive policy for production
    pub fn restrictive() -> Self {
        Self {
            max_watch_depth: Some(5),
            max_watched_files: Some(10_000),
            ..Self::default()
        }
    }
}

/// Validate a path for file watching with security policy
pub fn validate_watch_path(path: &str, policy: &FileWatchingSecurityPolicy) -> Result<PathBuf> {
    // First do basic path validation
    let cleaned_path = validate_path(path)?;

    // Check against forbidden exact paths
    for forbidden in &policy.forbidden_paths {
        if cleaned_path == *forbidden {
            return Err(ValidationError::Path(format!(
                "Path '{path}' is explicitly forbidden for watching"
            )));
        }
    }

    // Check against forbidden prefixes
    for prefix in &policy.forbidden_prefixes {
        if cleaned_path.starts_with(prefix) {
            return Err(ValidationError::Path(format!(
                "Path '{path}' is under forbidden prefix '{prefix}'"
            )));
        }
    }

    // Check system directory restrictions
    if !policy.allow_system_directories {
        let system_prefixes = [
            "/boot", "/proc", "/sys", "/dev", "/run", "/var/run", "/tmp", "/var/tmp",
        ];

        for sys_prefix in &system_prefixes {
            let sys_path = PathBuf::from(sys_prefix);
            if cleaned_path.starts_with(&sys_path) {
                return Err(ValidationError::Path(format!(
                    "System directory '{sys_prefix}' is not allowed for watching"
                )));
            }
        }
    }

    // Check for symlink concerns if path exists
    if cleaned_path.exists() && !policy.follow_symlinks {
        if let Ok(metadata) = std::fs::symlink_metadata(&cleaned_path) {
            if metadata.is_symlink() {
                return Err(ValidationError::Path(format!(
                    "Symlink '{path}' detected but policy forbids following symlinks"
                )));
            }
        }
    }

    Ok(cleaned_path)
}

/// Validate multiple watch paths with security policy
pub fn validate_watch_paths(
    paths: &[String],
    policy: &FileWatchingSecurityPolicy,
) -> Result<Vec<PathBuf>> {
    let mut validated_paths = Vec::new();

    for path in paths {
        let validated = validate_watch_path(path, policy)?;
        validated_paths.push(validated);
    }

    // Check total file count if policy specifies a limit
    if let Some(max_files) = policy.max_watched_files {
        let mut total_estimated_files = 0;

        for path in &validated_paths {
            if path.exists() {
                // Quick estimate of file count
                total_estimated_files += estimate_file_count(path, policy.max_watch_depth)?;

                if total_estimated_files > max_files {
                    return Err(ValidationError::Path(format!(
                        "Estimated file count {total_estimated_files} exceeds policy limit {max_files}"
                    )));
                }
            }
        }
    }

    Ok(validated_paths)
}

/// Estimate file count in a directory for security checking
fn estimate_file_count(path: &Path, max_depth: Option<usize>) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }

    // Count files using a bounded recursive traversal

    // Simple directory traversal for estimation
    fn count_files_recursive(path: &Path, depth: usize, max_depth: Option<usize>) -> usize {
        if let Some(max) = max_depth {
            if depth >= max {
                return 0;
            }
        }

        let mut count = 0;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.take(1000).flatten() {
                // Limit for performance
                let path = entry.path();
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() {
                        count += 1;
                    } else if metadata.is_dir() {
                        if let Ok(utf8_path) = camino::Utf8PathBuf::from_path_buf(path) {
                            count += count_files_recursive(&utf8_path, depth + 1, max_depth);
                        }
                    }
                }
            }
        }
        count
    }

    let count = count_files_recursive(path, 0, max_depth);
    Ok(count)
}

/// Validate that a discovered file path is safe for processing
pub fn validate_discovered_file(
    file_path: &str,
    watch_root: &str,
    policy: &FileWatchingSecurityPolicy,
) -> Result<PathBuf> {
    // Basic validation first
    let _file_path_buf = validate_path(file_path)?;

    // Ensure file stays within watch root
    let validated_within_root = validate_path_within_root(file_path, watch_root)?;

    // Apply symlink policy
    if !policy.follow_symlinks && validated_within_root.exists() {
        if let Ok(metadata) = std::fs::symlink_metadata(&validated_within_root) {
            if metadata.is_symlink() {
                return Err(ValidationError::Path(format!(
                    "Discovered symlink '{file_path}' but policy forbids following symlinks"
                )));
            }
        }
    }

    Ok(validated_within_root)
}

/// Check if a path depth exceeds policy limits
pub fn check_path_depth(path: &Path, max_depth: Option<usize>) -> Result<()> {
    if let Some(max) = max_depth {
        let depth = path.components().count();
        if depth > max {
            return Err(ValidationError::Path(format!(
                "Path depth {depth} exceeds maximum allowed depth {max}"
            )));
        }
    }
    Ok(())
}
