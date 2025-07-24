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
    pub fn current() -> Self {
        Self {
            version: satellite_version(),
            full_version: satellite_full_version(),
            commit_hash: satellite_commit_hash(),
            commit_count: satellite_commit_count(),
            branch: satellite_branch(),
            build_timestamp: satellite_build_timestamp(),
            is_dirty: satellite_is_dirty(),
        }
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
        format!("{} ({}@{}, built {})", 
                self.version, 
                self.commit_hash, 
                self.branch,
                self.build_timestamp)
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
        let host_name = gethostname::gethostname()
            .to_string_lossy()
            .to_string();
        
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
        self.start_time
            .elapsed()
            .unwrap_or_default()
            .as_secs()
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
        format!("{} v{} on {} (up {}s)", 
                self.service_name,
                self.version.version,
                self.host_name,
                self.uptime_seconds())
    }
}

// Version accessor functions using compile-time environment variables

/// Get semantic version of the satellite
pub fn satellite_version() -> Version {
    Version::from_str(env!("SATELLITE_VERSION"))
        .expect("Invalid satellite version")
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
pub fn satellite_commit_count() -> u32 {
    env!("SATELLITE_COMMIT_COUNT").parse()
        .expect("Invalid commit count")
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
pub fn satellite_is_dirty() -> bool {
    env!("SATELLITE_IS_DIRTY").parse()
        .expect("Invalid dirty flag")
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

    #[test]
    fn test_version_parsing() {
        let version = satellite_version();
        assert!(version.major >= 1);
        assert!(version.patch > 0); // Should have some commits
    }

    #[test]
    fn test_version_comparison() {
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
    }

    #[test]
    fn test_dirty_build_preference() {
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
    }
}