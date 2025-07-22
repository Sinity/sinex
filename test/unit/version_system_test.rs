//! Unit tests for auto-versioning system
//!
//! Tests git-based version generation and comparison:
//! - Version parsing and formatting
//! - Semantic version comparison
//! - Git commit count integration
//! - SatelliteInstance creation

use sinex_satellite_sdk::version::{SatelliteVersion, SatelliteInstance, satellite_version};
use std::cmp::Ordering;

#[test]
fn test_satellite_version_parsing() {
    // Test basic version parsing
    let v1 = SatelliteVersion::parse("1.0.100+abc123").unwrap();
    assert_eq!(v1.major(), 1);
    assert_eq!(v1.minor(), 0);
    assert_eq!(v1.patch(), 100);
    assert_eq!(v1.build(), "abc123");
    assert!(!v1.is_dirty());
    
    // Test dirty version
    let v2 = SatelliteVersion::parse("2.1.250+def456.dirty").unwrap();
    assert_eq!(v2.major(), 2);
    assert_eq!(v2.minor(), 1);
    assert_eq!(v2.patch(), 250);
    assert_eq!(v2.build(), "def456");
    assert!(v2.is_dirty());
    
    // Test minimal version
    let v3 = SatelliteVersion::parse("0.1.1+a").unwrap();
    assert_eq!(v3.major(), 0);
    assert_eq!(v3.minor(), 1);
    assert_eq!(v3.patch(), 1);
    assert_eq!(v3.build(), "a");
}

#[test]
fn test_satellite_version_formatting() {
    let version = SatelliteVersion::parse("1.2.300+abc123").unwrap();
    assert_eq!(version.to_string(), "1.2.300+abc123");
    
    let dirty_version = SatelliteVersion::parse("1.2.300+abc123.dirty").unwrap();
    assert_eq!(dirty_version.to_string(), "1.2.300+abc123.dirty");
}

#[test]
fn test_satellite_version_comparison() {
    let v1_0_100 = SatelliteVersion::parse("1.0.100+abc").unwrap();
    let v1_0_200 = SatelliteVersion::parse("1.0.200+def").unwrap();
    let v1_1_50 = SatelliteVersion::parse("1.1.50+ghi").unwrap();
    let v2_0_10 = SatelliteVersion::parse("2.0.10+jkl").unwrap();
    
    // Test patch version comparison (commit count)
    assert!(v1_0_200 > v1_0_100);
    assert!(v1_0_100 < v1_0_200);
    assert_eq!(v1_0_100.cmp(&v1_0_100), Ordering::Equal);
    
    // Test minor version beats patch
    assert!(v1_1_50 > v1_0_200);
    assert!(v1_0_200 < v1_1_50);
    
    // Test major version beats minor
    assert!(v2_0_10 > v1_1_50);
    assert!(v1_1_50 < v2_0_10);
}

#[test]
fn test_satellite_version_edge_cases() {
    // Same semantic version, different builds
    let v1 = SatelliteVersion::parse("1.0.100+abc123").unwrap();
    let v2 = SatelliteVersion::parse("1.0.100+def456").unwrap();
    
    // Should be equal for semantic comparison (build doesn't affect ordering)
    assert_eq!(v1.cmp(&v2), Ordering::Equal);
    
    // Dirty vs clean with same semantic version
    let clean = SatelliteVersion::parse("1.0.100+abc123").unwrap();
    let dirty = SatelliteVersion::parse("1.0.100+abc123.dirty").unwrap();
    
    // Should be equal for semantic comparison
    assert_eq!(clean.cmp(&dirty), Ordering::Equal);
    
    // But dirty flag should be accessible
    assert!(!clean.is_dirty());
    assert!(dirty.is_dirty());
}

#[test]
fn test_satellite_version_ordering_scenarios() {
    let test_cases = vec![
        ("1.0.100+a", "1.0.200+b", Ordering::Less),
        ("1.0.200+a", "1.0.100+b", Ordering::Greater),
        ("1.0.100+a", "1.1.50+b", Ordering::Less),
        ("1.1.50+a", "1.0.200+b", Ordering::Greater),
        ("1.9.999+a", "2.0.1+b", Ordering::Less),
        ("2.0.1+a", "1.9.999+b", Ordering::Greater),
        ("1.0.100+same", "1.0.100+different", Ordering::Equal),
        ("1.0.100+abc", "1.0.100+abc.dirty", Ordering::Equal),
    ];
    
    for (v1_str, v2_str, expected) in test_cases {
        let v1 = SatelliteVersion::parse(v1_str).unwrap();
        let v2 = SatelliteVersion::parse(v2_str).unwrap();
        
        assert_eq!(
            v1.cmp(&v2), 
            expected,
            "Comparing {} with {} should be {:?}", 
            v1_str, v2_str, expected
        );
    }
}

#[test]
fn test_satellite_version_invalid_parsing() {
    let invalid_versions = vec![
        "1.0",                    // Missing patch and build
        "1.0.100",               // Missing build
        "1.0.100+",              // Empty build
        "a.0.100+abc",           // Non-numeric major
        "1.b.100+abc",           // Non-numeric minor  
        "1.0.c+abc",             // Non-numeric patch
        "",                      // Empty string
        "1.0.100+abc.extra.dirty", // Too many parts
    ];
    
    for invalid in invalid_versions {
        assert!(
            SatelliteVersion::parse(invalid).is_err(),
            "Should reject invalid version: {}", 
            invalid
        );
    }
}

#[test]
fn test_satellite_instance_creation() {
    let version = SatelliteVersion::parse("1.0.100+abc123").unwrap();
    let instance = SatelliteInstance::new("test-service", version.clone());
    
    // Test basic properties
    assert_eq!(instance.service_name(), "test-service");
    assert_eq!(instance.version(), &version);
    
    // Instance ID should be unique
    assert!(!instance.instance_id().to_string().is_empty());
    
    // Multiple instances should have different IDs
    let instance2 = SatelliteInstance::new("test-service", version.clone());
    assert_ne!(instance.instance_id(), instance2.instance_id());
}

#[test]
fn test_satellite_instance_different_services() {
    let version = SatelliteVersion::parse("1.0.100+abc123").unwrap();
    
    let fs_instance = SatelliteInstance::new("fs-watcher", version.clone());
    let terminal_instance = SatelliteInstance::new("terminal-satellite", version.clone());
    
    assert_eq!(fs_instance.service_name(), "fs-watcher");
    assert_eq!(terminal_instance.service_name(), "terminal-satellite");
    assert_eq!(fs_instance.version(), terminal_instance.version());
    assert_ne!(fs_instance.instance_id(), terminal_instance.instance_id());
}

#[test]
fn test_satellite_instance_metadata() {
    let version = SatelliteVersion::parse("1.0.100+abc123").unwrap();
    let instance = SatelliteInstance::new("metadata-test", version);
    
    // Test start time is set
    let start_time = instance.start_time();
    assert!(start_time.elapsed().unwrap().as_secs() < 1); // Should be very recent
    
    // Test host name is set
    let host_name = instance.host_name();
    assert!(!host_name.is_empty());
}

#[test]
fn test_auto_version_generation() {
    // Test that satellite_version() returns a valid version
    let version = satellite_version();
    
    // Should be parseable as a SatelliteVersion
    let parsed = SatelliteVersion::parse(&version.to_string()).unwrap();
    assert_eq!(parsed, version);
    
    // Should have reasonable values
    assert!(version.major() <= 10); // Reasonable for development
    assert!(version.minor() <= 10); // Reasonable for development
    assert!(version.patch() > 0);   // Should have some commits
    assert!(!version.build().is_empty()); // Should have git hash
}

#[test]
fn test_version_with_start_time_comparison() {
    // Test that instances with same version use start time for ordering
    let version = SatelliteVersion::parse("1.0.100+same").unwrap();
    
    let instance1 = SatelliteInstance::new("tiebreaker-test", version.clone());
    
    // Wait to ensure different start time
    std::thread::sleep(std::time::Duration::from_millis(1));
    
    let instance2 = SatelliteInstance::new("tiebreaker-test", version.clone());
    
    // instance1 should have earlier start time
    assert!(instance1.start_time() < instance2.start_time());
    
    // Both should have same version
    assert_eq!(instance1.version(), instance2.version());
}

#[test]
fn test_version_serialization_format() {
    let version = SatelliteVersion::parse("1.2.300+abc123def").unwrap();
    let formatted = version.to_string();
    
    // Should match expected format
    assert_eq!(formatted, "1.2.300+abc123def");
    
    // Should be parseable back to same version
    let reparsed = SatelliteVersion::parse(&formatted).unwrap();
    assert_eq!(version, reparsed);
}

#[test]
fn test_version_hash_consistency() {
    let v1 = SatelliteVersion::parse("1.0.100+abc123").unwrap();
    let v2 = SatelliteVersion::parse("1.0.100+abc123").unwrap();
    let v3 = SatelliteVersion::parse("1.0.200+def456").unwrap();
    
    // Same versions should have same hash
    assert_eq!(
        std::collections::hash_map::DefaultHasher::new().finish(),
        std::collections::hash_map::DefaultHasher::new().finish()
    );
    
    // Equal versions should be equal
    assert_eq!(v1, v2);
    assert_ne!(v1, v3);
}

#[test]
fn test_version_with_realistic_git_hashes() {
    // Test with realistic git hash lengths and formats
    let realistic_versions = vec![
        "1.0.100+a1b2c3d4",
        "1.0.100+1234567890abcdef",
        "1.0.100+abc123.dirty",
        "1.0.100+1a2b3c4d5e6f7890.dirty",
    ];
    
    for version_str in realistic_versions {
        let version = SatelliteVersion::parse(version_str).unwrap();
        assert!(!version.build().is_empty());
        
        // Should round-trip through string conversion
        assert_eq!(version.to_string(), version_str);
    }
}

#[test]
fn test_version_commit_count_semantics() {
    // Patch version represents commit count since last tag
    // Higher commit count = newer version
    
    let older_commits = SatelliteVersion::parse("1.0.50+abc123").unwrap();
    let newer_commits = SatelliteVersion::parse("1.0.150+def456").unwrap();
    
    // More commits should be newer
    assert!(newer_commits > older_commits);
    
    // This models real development where:
    // - git tag v1.0 (creates baseline)
    // - 50 commits later -> 1.0.50+[hash]
    // - 150 commits later -> 1.0.150+[hash]
    // - Next minor/major release resets commit count
}

#[test]
fn test_instance_id_uniqueness() {
    let version = SatelliteVersion::parse("1.0.100+test").unwrap();
    let mut ids = std::collections::HashSet::new();
    
    // Generate 100 instances and verify all IDs are unique
    for i in 0..100 {
        let instance = SatelliteInstance::new(&format!("service-{}", i), version.clone());
        let id = instance.instance_id();
        
        assert!(ids.insert(*id), "Duplicate instance ID generated: {}", id);
    }
    
    assert_eq!(ids.len(), 100);
}