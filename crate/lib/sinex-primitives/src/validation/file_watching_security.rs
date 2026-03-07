//! File watching security extensions for the validation module
//!
//! This module provides specialized security validation for file watching operations,
//! including path validation, security policies, and symlink protection.

use super::{validate_path, validate_path_within_root};
use crate::error::{Result, SinexError};
use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use std::collections::HashSet;

// ===== File Watching Security Module =====

// Default security policy values
const DEFAULT_MAX_WATCH_DEPTH: usize = 10;
const DEFAULT_MAX_WATCHED_FILES: usize = 100_000;
const RESTRICTIVE_MAX_WATCH_DEPTH: usize = 5;
const RESTRICTIVE_MAX_WATCHED_FILES: usize = 10_000;

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
            max_watch_depth: Some(DEFAULT_MAX_WATCH_DEPTH),
            forbidden_paths,
            forbidden_prefixes,
            follow_symlinks: false,
            max_watched_files: Some(DEFAULT_MAX_WATCHED_FILES),
            allow_system_directories: true,
        }
    }
}

impl FileWatchingSecurityPolicy {
    /// Create a permissive policy for testing
    #[must_use]
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
    #[must_use]
    pub fn restrictive() -> Self {
        Self {
            max_watch_depth: Some(RESTRICTIVE_MAX_WATCH_DEPTH),
            max_watched_files: Some(RESTRICTIVE_MAX_WATCHED_FILES),
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
            return Err(SinexError::validation(format!(
                "Path '{path}' is explicitly forbidden for watching"
            )));
        }
    }

    // Check against forbidden prefixes
    for prefix in &policy.forbidden_prefixes {
        if cleaned_path.starts_with(prefix) {
            return Err(SinexError::validation(format!(
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
                return Err(SinexError::validation(format!(
                    "System directory '{sys_prefix}' is not allowed for watching"
                )));
            }
        }
    }

    // Check for symlink concerns if path exists
    if cleaned_path.exists()
        && !policy.follow_symlinks
        && let Ok(metadata) = std::fs::symlink_metadata(&cleaned_path)
        && metadata.is_symlink()
    {
        return Err(SinexError::validation(format!(
            "Symlink '{path}' detected but policy forbids following symlinks"
        )));
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
                    return Err(SinexError::validation(format!(
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
        if let Some(max) = max_depth
            && depth >= max
        {
            return 0;
        }

        let mut count = 0;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.take(1000).flatten() {
                // Limit for performance
                let path = entry.path();
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() {
                        count += 1;
                    } else if metadata.is_dir()
                        && let Ok(utf8_path) = camino::Utf8PathBuf::from_path_buf(path)
                    {
                        count += count_files_recursive(&utf8_path, depth + 1, max_depth);
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
    if !policy.follow_symlinks
        && validated_within_root.exists()
        && let Ok(metadata) = std::fs::symlink_metadata(&validated_within_root)
        && metadata.is_symlink()
    {
        return Err(SinexError::validation(format!(
            "Discovered symlink '{file_path}' but policy forbids following symlinks"
        )));
    }

    Ok(validated_within_root)
}

/// Directory components that indicate sensitive content.
/// Files under these directories should not be ingested to avoid leaking credentials.
const SENSITIVE_DIR_COMPONENTS: &[&str] = &[
    ".ssh",
    ".gnupg",
    ".gpg",
    ".pki",
    ".password-store",
    ".mozilla", // Firefox profiles contain session tokens
    ".config/chromium",
    ".aws",
    ".docker",
    ".kube",
    ".helm",
    ".terraform",
    ".vault-token",
];

/// File name patterns that indicate sensitive content regardless of directory.
const SENSITIVE_FILE_NAMES: &[&str] = &[
    ".env",
    ".env.local",
    ".env.production",
    ".netrc",
    ".npmrc",
    ".pypirc",
    "credentials",
    "credentials.json",
    "token.json",
    "service-account.json",
    "known_hosts",     // not secret, but privacy-relevant
    "authorized_keys", // not secret, but privacy-relevant
];

/// File extensions that indicate sensitive content.
const SENSITIVE_EXTENSIONS: &[&str] = &["pem", "key", "p12", "pfx", "jks", "keystore"];

/// Check if a file path points to potentially sensitive content.
///
/// Returns `Some(reason)` if the path matches a sensitive pattern, `None` otherwise.
/// This is used by ingestors to skip files that could contain credentials or private keys.
#[must_use]
pub fn check_sensitive_path(path: &Path) -> Option<&'static str> {
    let path_str = path.as_str();

    // Check directory components
    for component in SENSITIVE_DIR_COMPONENTS {
        // Match as exact path component (e.g. "/.ssh/" or ends with "/.ssh")
        let with_slashes = format!("/{component}/");
        if path_str.contains(&with_slashes) || path_str.ends_with(&format!("/{component}")) {
            return Some("path contains sensitive directory");
        }
    }

    // Check file name
    if let Some(file_name) = path.file_name() {
        for name in SENSITIVE_FILE_NAMES {
            if file_name == *name {
                return Some("file name matches sensitive pattern");
            }
        }

        // Check if file starts with id_ (SSH key pattern: id_rsa, id_ed25519, etc.)
        if file_name.starts_with("id_") && !file_name.ends_with(".pub") {
            return Some("file matches SSH private key pattern");
        }
    }

    // Check extension
    if let Some(ext) = path.extension() {
        for sensitive_ext in SENSITIVE_EXTENSIONS {
            if ext == *sensitive_ext {
                return Some("file extension indicates cryptographic material");
            }
        }
    }

    None
}

/// Check if a path depth exceeds policy limits
pub fn check_path_depth(path: &Path, max_depth: Option<usize>) -> Result<()> {
    if let Some(max) = max_depth {
        let depth = path.components().count();
        if depth > max {
            return Err(SinexError::validation(format!(
                "Path depth {depth} exceeds maximum allowed depth {max}"
            )));
        }
    }
    Ok(())
}
