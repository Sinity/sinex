//! Node configuration environment validation tests.

use sinex_node_sdk::NodeConfig;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_node_environment_path_validation() -> TestResult<()> {
    unsafe { std::env::set_var("SINEX_WORK_DIR", "../../../etc") };

    let config = NodeConfig::load_from_env("test-node")?;
    assert!(!config.work_dir.as_str().contains("../../"));
    assert!(config.work_dir.is_absolute());

    unsafe { std::env::remove_var("SINEX_WORK_DIR") };
    Ok(())
}

#[sinex_test]
async fn test_node_default_work_dir_is_secure() -> TestResult<()> {
    let config = NodeConfig::load_from_env("test-service")?;
    assert!(config.work_dir.is_absolute());
    assert!(!config.work_dir.as_str().contains(".."));
    Ok(())
}
