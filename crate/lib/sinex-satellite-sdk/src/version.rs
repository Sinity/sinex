//! Satellite version information and utilities
//!
//! This module provides access to compile-time version information generated
//! by the build script, including semantic versioning, git metadata, and
//! build information for satellite coordination and handoff.

use semver::Version;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;
use std::time::SystemTime;

/// Complete satellite version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SatelliteVersion {
    /// Semantic version (major.minor.patch)
    pub version: Version,
    /// Full version with build metadata (version+commit_hash)
    pub full_version: String,
    /// Git commit hash (8 characters)
    pub commit_hash: String,
    /// Git commit count (used as patch version)
    pub commit_count: u32,
    /// Git branch name
    pub branch: String,
    /// Build timestamp (RFC3339)
    pub build_timestamp: String,
    /// Whether working directory was dirty during build
    pub is_dirty: bool,
}

impl SatelliteVersion {
    /// Get the current satellite version information
    ///
    /// # Errors
    /// Returns `SatelliteError::Configuration` if any version information is invalid
    #[must_use]
    pub fn current() -> crate::SatelliteResult<Self> {
        Ok(Self {
            version: satellite_version()?,
            full_version: satellite_full_version(),
            commit_hash: satellite_commit_hash(),
            commit_count: satellite_commit_count()?,
            branch: satellite_branch(),
            build_timestamp: satellite_build_timestamp(),
            is_dirty: satellite_is_dirty()?,
        })
    }

    /// Get the current satellite version information with fallback to defaults on error
    ///
    /// This provides a non-panicking alternative for cases where you need version info
    /// but can tolerate fallback values if the build metadata is corrupted.
    pub fn current_or_default() -> Self {
        Self::current().unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to get satellite version info, using defaults");
            Self {
                version: Version::new(0, 1, 0), // Fallback version
                full_version: "0.1.0-unknown".to_string(),
                commit_hash: "unknown".to_string(),
                commit_count: 0,
                branch: "unknown".to_string(),
                build_timestamp: chrono::Utc::now().to_rfc3339(),
                is_dirty: true, // Conservative assumption
            }
        })
    }

    /// Compare versions for leadership election (newer version wins)
    pub fn is_newer_than(&self, other: &SatelliteVersion) -> bool {
        self.version > other.version
    }

    /// Check if this is a production build (not dirty, not on dev branch)
    pub fn is_production_build(&self) -> bool {
        !self.is_dirty && !self.branch.starts_with("dev") && self.branch != "HEAD"
    }

    /// Get age of this build in seconds
    pub fn build_age_seconds(&self) -> Option<u64> {
        let build_time = chrono::DateTime::parse_from_rfc3339(&self.build_timestamp).ok()?;
        let now = chrono::Utc::now();
        let duration = now.signed_duration_since(build_time.with_timezone(&chrono::Utc));
        Some(duration.num_seconds().max(0) as u64)
    }

    /// Create version summary for logging
    pub fn summary(&self) -> String {
        format!(
            "{} ({}@{}, built {})",
            self.version, self.commit_hash, self.branch, self.build_timestamp
        )
    }
}

impl PartialEq for SatelliteVersion {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
    }
}

impl Eq for SatelliteVersion {}

impl PartialOrd for SatelliteVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SatelliteVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        // Primary: compare semantic versions
        match self.version.cmp(&other.version) {
            Ordering::Equal => {
                // Secondary: if same version, prefer non-dirty builds
                match (self.is_dirty, other.is_dirty) {
                    (false, true) => Ordering::Greater,
                    (true, false) => Ordering::Less,
                    _ => Ordering::Equal,
                }
            }
            other => other,
        }
    }
}

impl fmt::Display for SatelliteVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.full_version)
    }
}

/// Instance information for coordination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SatelliteInstance {
    pub instance_id: String,
    pub version: SatelliteVersion,
    pub start_time: SystemTime,
    pub service_name: String,
    pub host_name: String,
}

impl SatelliteInstance {
    pub fn new(instance_id: String, service_name: String) -> Self {
        let host_name = gethostname::gethostname().to_string_lossy().to_string();

        Self {
            instance_id,
            version: SatelliteVersion::current(),
            start_time: SystemTime::now(),
            service_name,
            host_name,
        }
    }

    /// Get instance uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().unwrap_or_default().as_secs()
    }

    /// Check if this instance should be leader over another
    pub fn should_be_leader_over(&self, other: &SatelliteInstance) -> bool {
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

// Version accessor functions using compile-time environment variables

/// Get semantic version of the satellite
///
/// # Errors
/// Returns `SatelliteError::Configuration` if the satellite version is invalid
#[must_use]
pub fn satellite_version() -> crate::SatelliteResult<Version> {
    Version::from_str(env!("SATELLITE_VERSION")).map_err(|e| {
        crate::SatelliteError::Configuration(format!("Invalid satellite version: {}", e))
    })
}

/// Get full version string with build metadata
pub fn satellite_full_version() -> String {
    env!("SATELLITE_FULL_VERSION").to_string()
}

/// Get git commit hash (8 characters)
pub fn satellite_commit_hash() -> String {
    env!("SATELLITE_COMMIT_HASH").to_string()
}

/// Get git commit count (used as patch version)
///
/// # Errors
/// Returns `SatelliteError::Configuration` if the commit count is invalid
#[must_use]
pub fn satellite_commit_count() -> crate::SatelliteResult<u32> {
    env!("SATELLITE_COMMIT_COUNT")
        .parse()
        .map_err(|e| crate::SatelliteError::Configuration(format!("Invalid commit count: {}", e)))
}

/// Get git branch name
pub fn satellite_branch() -> String {
    env!("SATELLITE_BRANCH").to_string()
}

/// Get build timestamp
pub fn satellite_build_timestamp() -> String {
    env!("SATELLITE_BUILD_TIMESTAMP").to_string()
}

/// Check if working directory was dirty during build
///
/// # Errors
/// Returns `SatelliteError::Configuration` if the dirty flag is invalid
#[must_use]
pub fn satellite_is_dirty() -> crate::SatelliteResult<bool> {
    env!("SATELLITE_IS_DIRTY")
        .parse()
        .map_err(|e| crate::SatelliteError::Configuration(format!("Invalid dirty flag: {}", e)))
}

/// Print version information to stdout (for --version flags)
pub fn print_version_info() {
    let version = SatelliteVersion::current();
    println!("{}", version.full_version);
    println!("commit: {}", version.commit_hash);
    println!("branch: {}", version.branch);
    println!("built: {}", version.build_timestamp);
    if version.is_dirty {
        println!("status: dirty");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    fn test_version_parsing() -> color_eyre::eyre::Result<()> {
        let version = satellite_version();
        assert!(version.major >= 1);
        assert!(version.patch > 0); // Should have some commits
        Ok(())
    }

    #[sinex_test]
    fn test_version_comparison() -> color_eyre::eyre::Result<()> {
        let v1 = SatelliteVersion {
            version: Version::new(1, 0, 100),
            full_version: "1.0.100".to_string(),
            commit_hash: "abc12345".to_string(),
            commit_count: 100,
            branch: "main".to_string(),
            build_timestamp: "2023-01-01T00:00:00Z".to_string(),
            is_dirty: false,
        };

        let v2 = SatelliteVersion {
            version: Version::new(1, 0, 101),
            full_version: "1.0.101".to_string(),
            commit_hash: "def67890".to_string(),
            commit_count: 101,
            branch: "main".to_string(),
            build_timestamp: "2023-01-01T01:00:00Z".to_string(),
            is_dirty: false,
        };

        assert!(v2.is_newer_than(&v1));
        assert!(!v1.is_newer_than(&v2));
        assert!(v2 > v1);
        Ok(())
    }

    #[sinex_test]
    fn test_dirty_build_preference() -> color_eyre::eyre::Result<()> {
        let clean = SatelliteVersion {
            version: Version::new(1, 0, 100),
            full_version: "1.0.100".to_string(),
            commit_hash: "abc12345".to_string(),
            commit_count: 100,
            branch: "main".to_string(),
            build_timestamp: "2023-01-01T00:00:00Z".to_string(),
            is_dirty: false,
        };

        let dirty = SatelliteVersion {
            version: Version::new(1, 0, 100),
            full_version: "1.0.100+abc12345.dirty".to_string(),
            commit_hash: "abc12345".to_string(),
            commit_count: 100,
            branch: "main".to_string(),
            build_timestamp: "2023-01-01T00:00:00Z".to_string(),
            is_dirty: true,
        };

        // Same version, but clean build should be preferred
        assert!(clean > dirty);
        Ok(())
    }
}
