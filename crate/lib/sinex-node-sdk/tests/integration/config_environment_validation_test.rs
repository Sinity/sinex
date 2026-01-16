//! Satellite configuration environment validation tests.

use sinex_node_sdk::NodeConfig;
use sinex_test_utils::{TestResult, sinex_test, TestContext};

#[sinex_test]
async fn test_satellite_environment_path_validation() -> TestResult<()> {
    std::env::set_var("SINEX_WORK_DIR", "../../../etc");

    let config = NodeConfig::load_from_env("test-satellite");
    assert!(!config.work_dir.as_str().contains("../../"));
    assert!(config.work_dir.is_absolute());

    std::env::remove_var("SINEX_WORK_DIR");
    Ok(())
}

#[sinex_test]
async fn test_satellite_default_work_dir_is_secure() -> TestResult<()> {
    let config = NodeConfig::load_from_env("test-service");
    assert!(config.work_dir.is_absolute());
    assert!(!config.work_dir.as_str().contains(".."));
    Ok(())
}
