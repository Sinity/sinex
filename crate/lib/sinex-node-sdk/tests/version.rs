use semver::Version;
use sinex_node_sdk::VersionInfo;
use sinex_node_sdk::version::{NodeVersion, node_version};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn node_version_env_is_valid() -> TestResult<()> {
    let version = node_version()?;
    let minimum_supported = Version::new(0, 1, 0);
    assert!(
        version >= minimum_supported,
        "node version must be at least {minimum_supported}, got {version}"
    );
    assert!(version.patch > 0);
    Ok(())
}

#[sinex_test]
async fn node_version_comparison_prefers_newer_semver() -> TestResult<()> {
    let v1 = NodeVersion {
        version: semver::Version::new(1, 0, 100),
        full_version: "1.0.100".to_string(),
        commit_hash: "abc12345".to_string(),
        commit_count: 100,
        branch: "main".to_string(),
        build_timestamp: "2023-01-01T00:00:00Z".to_string(),
        is_dirty: false,
    };

    let v2 = NodeVersion {
        version: semver::Version::new(1, 0, 101),
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
async fn clean_build_is_preferred_over_dirty() -> TestResult<()> {
    let clean = NodeVersion {
        version: semver::Version::new(1, 0, 100),
        full_version: "1.0.100".to_string(),
        commit_hash: "abc12345".to_string(),
        commit_count: 100,
        branch: "main".to_string(),
        build_timestamp: "2023-01-01T00:00:00Z".to_string(),
        is_dirty: false,
    };

    let dirty = NodeVersion {
        version: semver::Version::new(1, 0, 100),
        full_version: "1.0.100+abc12345.dirty".to_string(),
        commit_hash: "abc12345".to_string(),
        commit_count: 100,
        branch: "main".to_string(),
        build_timestamp: "2023-01-01T00:00:00Z".to_string(),
        is_dirty: true,
    };

    assert!(clean > dirty);
    Ok(())
}

#[sinex_test]
async fn version_info_has_build_stamp() -> TestResult<()> {
    let info = VersionInfo::current("build-stamp-check");
    assert!(!info.git_revision.is_empty());
    assert!(!info.binary_hash.is_empty());

    if !cfg!(debug_assertions) {
        assert_ne!(info.git_revision, "unknown");
        assert_ne!(info.binary_hash, "unknown");
    }

    Ok(())
}
