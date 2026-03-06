//! Node version information and utilities
//!
//! This module provides access to compile-time version information generated
//! by shadow-rs, including semantic versioning, git metadata, and
//! build information for node coordination and handoff.

use semver::Version;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;
use std::time::SystemTime;

// Import shadow-rs generated build information
shadow_rs::shadow!(build);

/// Complete node version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeVersion {
    /// Semantic version (major.minor.patch)
    pub version: Version,
    /// Full version with build metadata (`version+commit_hash`)
    pub full_version: String,
    /// Git commit hash (8 characters)
    pub commit_hash: String,
    /// Git commit count (used for ordering when versions match)
    pub commit_count: u32,
    /// Git branch name
    pub branch: String,
    /// Build timestamp (RFC3339)
    pub build_timestamp: String,
    /// Whether working directory was dirty during build
    pub is_dirty: bool,
}

impl NodeVersion {
    /// Get the current node version information
    ///
    /// # Errors
    /// Returns `SinexError::configuration` if any version information is invalid
    pub fn current() -> crate::NodeResult<Self> {
        Ok(Self {
            version: node_version()?,
            full_version: node_full_version(),
            commit_hash: node_commit_hash(),
            commit_count: node_commit_count(),
            branch: node_branch(),
            build_timestamp: node_build_timestamp(),
            is_dirty: node_is_dirty(),
        })
    }

    /// Get the current node version information with fallback to defaults on error
    ///
    /// This provides a non-panicking alternative for cases where you need version info
    /// but can tolerate fallback values if the build metadata is corrupted.
    #[must_use]
    pub fn current_or_default() -> Self {
        Self::current().unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to get node version info, using defaults");
            Self {
                version: Version::new(0, 1, 0), // Fallback version
                full_version: "0.1.0-unknown".to_string(),
                commit_hash: "unknown".to_string(),
                commit_count: 0,
                branch: "unknown".to_string(),
                build_timestamp: sinex_primitives::temporal::now().format_rfc3339(),
                is_dirty: true, // Conservative assumption
            }
        })
    }

    /// Compare versions for leadership election (newer version wins)
    #[must_use]
    pub fn is_newer_than(&self, other: &NodeVersion) -> bool {
        self.version > other.version
    }

    /// Check if this is a production build (not dirty, not on dev branch)
    #[must_use]
    pub fn is_production_build(&self) -> bool {
        !self.is_dirty && !self.branch.starts_with("dev") && self.branch != "HEAD"
    }

    /// Get age of this build in seconds
    #[must_use]
    pub fn build_age_seconds(&self) -> Option<u64> {
        let build_time =
            sinex_primitives::temporal::Timestamp::parse_rfc3339(&self.build_timestamp).ok()?;
        let now = sinex_primitives::temporal::Timestamp::now();
        let duration = now - build_time;
        Some(duration.whole_seconds().max(0) as u64)
    }

    /// Create version summary for logging
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{} ({}@{}, built {})",
            self.version, self.commit_hash, self.branch, self.build_timestamp
        )
    }
}

impl PartialEq for NodeVersion {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
    }
}

impl Eq for NodeVersion {}

impl PartialOrd for NodeVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        // Primary: compare semantic versions
        match self.version.cmp(&other.version) {
            Ordering::Equal => {
                // Secondary: if same version, prefer non-dirty builds
                match (self.is_dirty, other.is_dirty) {
                    (false, true) => Ordering::Greater,
                    (true, false) => Ordering::Less,
                    _ => {
                        // Both dirty or both clean - compare commit count (more commits = newer)
                        match self.commit_count.cmp(&other.commit_count) {
                            Ordering::Equal => {
                                // Same commit count - use build timestamp (development rebuilds)
                                // This enables hot reload with same semver to prefer newer builds
                                self.build_timestamp.cmp(&other.build_timestamp)
                            }
                            other => other,
                        }
                    }
                }
            }
            other => other,
        }
    }
}

impl fmt::Display for NodeVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.full_version)
    }
}

impl FromStr for NodeVersion {
    type Err = crate::SinexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse version string (expected format: "major.minor.patch" or "major.minor.patch+metadata")
        let version = Version::from_str(s.split('+').next().unwrap_or(s)).map_err(|e| {
            crate::SinexError::configuration(format!("Invalid version string '{s}': {e}"))
        })?;

        Ok(Self {
            version,
            full_version: s.to_string(),
            commit_hash: "unknown".to_string(),
            commit_count: 0,
            branch: "unknown".to_string(),
            build_timestamp: sinex_primitives::temporal::now().format_rfc3339(),
            is_dirty: false,
        })
    }
}

/// Instance information for coordination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInstance {
    pub instance_id: String,
    pub version: NodeVersion,
    pub start_time: SystemTime,
    pub service_name: String,
    pub host_name: String,
}

impl NodeInstance {
    /// Create a new node instance
    ///
    /// # Errors
    /// Returns `SinexError::configuration` if version information is invalid
    pub fn new(instance_id: String, service_name: String) -> crate::NodeResult<Self> {
        let host_name = gethostname::gethostname().to_string_lossy().to_string();

        Ok(Self {
            instance_id,
            version: NodeVersion::current()?,
            start_time: SystemTime::now(),
            service_name,
            host_name,
        })
    }

    /// Create a new node instance with fallback version on error
    ///
    /// This provides a non-panicking alternative that uses default version info
    /// if the build metadata is corrupted.
    #[must_use]
    pub fn new_or_default(instance_id: String, service_name: String) -> Self {
        let host_name = gethostname::gethostname().to_string_lossy().to_string();

        Self {
            instance_id,
            version: NodeVersion::current_or_default(),
            start_time: SystemTime::now(),
            service_name,
            host_name,
        }
    }

    /// Get instance uptime in seconds
    #[must_use]
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().unwrap_or_default().as_secs()
    }

    /// Check if this instance should be leader over another
    #[must_use]
    pub fn should_be_leader_over(&self, other: &NodeInstance) -> bool {
        match self.version.cmp(&other.version) {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => {
                // Same version - earlier start time wins (stability)
                self.start_time < other.start_time
            }
        }
    }

    /// Create instance summary for logging
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{} v{} on {} (up {}s)",
            self.service_name,
            self.version.version,
            self.host_name,
            self.uptime_seconds()
        )
    }
}

// Version accessor functions using shadow-rs compile-time constants

/// Get semantic version of the node
///
/// # Errors
/// Returns `SinexError::configuration` if the node version is invalid
pub fn node_version() -> crate::NodeResult<Version> {
    Version::from_str(build::PKG_VERSION)
        .map_err(|e| crate::SinexError::configuration(format!("Invalid node version: {e}")))
}

/// Get full version string with build metadata
#[must_use]
pub fn node_full_version() -> String {
    let commit = build::SHORT_COMMIT;
    if commit.is_empty() || commit == "unknown" {
        build::PKG_VERSION.to_string()
    } else {
        format!("{}+{}", build::PKG_VERSION, commit)
    }
}

/// Get git commit hash (8 characters)
#[must_use]
pub fn node_commit_hash() -> String {
    let commit = build::SHORT_COMMIT;
    if commit.is_empty() {
        "unknown".to_string()
    } else {
        commit.to_string()
    }
}

/// Get git commit count
///
/// Note: shadow-rs doesn't provide commit count directly.
/// We use a placeholder of 0 since the ordering logic primarily uses
/// semver and build timestamp for tiebreaking anyway.
#[must_use]
pub fn node_commit_count() -> u32 {
    // shadow-rs doesn't provide total commit count
    // The ordering logic uses build timestamp as final tiebreaker,
    // so this is only used for initial ordering of same-semver builds
    0
}

/// Get git branch name
#[must_use]
pub fn node_branch() -> String {
    let branch = build::BRANCH;
    if branch.is_empty() {
        "unknown".to_string()
    } else {
        branch.to_string()
    }
}

/// Get build timestamp (RFC3339 format)
#[must_use]
pub fn node_build_timestamp() -> String {
    // shadow-rs provides BUILD_TIME_3339 in RFC3339 format
    build::BUILD_TIME_3339.to_string()
}

/// Check if working directory was dirty during build
#[must_use]
pub fn node_is_dirty() -> bool {
    // shadow-rs GIT_CLEAN is a bool: true when clean, false when dirty
    !build::GIT_CLEAN
}

/// Print version information to stdout (for --version flags)
pub fn print_version_info() {
    match NodeVersion::current() {
        Ok(version) => {
            println!("{}", version.full_version);
            println!("commit: {}", version.commit_hash);
            println!("branch: {}", version.branch);
            println!("built: {}", version.build_timestamp);
            if version.is_dirty {
                println!("status: dirty");
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "Error reading version information");
            let fallback = NodeVersion::current_or_default();
            println!("{}", fallback.full_version);
            println!("status: version info corrupted, using fallback");
        }
    }
}
