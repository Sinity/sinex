use semver::Version;
use sinex_node_sdk::NodeVersion;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_build_age_seconds() -> TestResult<()> {
    let now = sinex_primitives::temporal::Timestamp::now();
    let one_hour_ago = now - time::Duration::seconds(3600);
    let timestamp_str = one_hour_ago.format_rfc3339();

    let version = NodeVersion {
        full_version: "0.0.0".to_string(),
        version: Version::new(0, 0, 0),
        commit_hash: "test".to_string(),
        branch: "test".to_string(),
        build_timestamp: timestamp_str,
        is_dirty: false,
    };

    let age = version.build_age_seconds().expect("Should return age");
    assert!(
        (3599..=3605).contains(&age),
        "Age {age} should be close to 3600"
    );
    Ok(())
}

#[sinex_test]
async fn test_build_age_future() -> TestResult<()> {
    let now = sinex_primitives::temporal::Timestamp::now();
    let one_hour_future = now + time::Duration::seconds(3600);
    let timestamp_str = one_hour_future.format_rfc3339();

    let version = NodeVersion {
        full_version: "0.0.0".to_string(),
        version: Version::new(0, 0, 0),
        commit_hash: "test".to_string(),
        branch: "test".to_string(),
        build_timestamp: timestamp_str,
        is_dirty: false,
    };

    let age = version.build_age_seconds().expect("Should return age");
    assert_eq!(age, 0, "Future build should return 0 age");
    Ok(())
}

#[sinex_test]
async fn test_build_age_invalid_timestamp() -> TestResult<()> {
    let version = NodeVersion {
        full_version: "0.0.0".to_string(),
        version: Version::new(0, 0, 0),
        commit_hash: "test".to_string(),
        commit_count: 0,
        branch: "test".to_string(),
        build_timestamp: "not-a-timestamp".to_string(),
        is_dirty: false,
    };

    assert!(version.build_age_seconds().is_none());
    Ok(())
}
