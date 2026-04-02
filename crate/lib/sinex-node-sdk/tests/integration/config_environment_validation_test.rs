//! Node configuration environment validation tests.

use sinex_node_sdk::NodeConfig;
use xtask::sandbox::{EnvGuard, sinex_test};

#[sinex_test]
async fn test_node_environment_path_validation() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_WORK_DIR", "../../../etc");

    let error =
        NodeConfig::load_from_env("test-node").expect_err("invalid work dir override must fail");
    let message = error.to_string();
    assert!(message.contains("SINEX_WORK_DIR"));
    assert!(message.contains("invalid path value"));
    Ok(())
}

#[sinex_test]
async fn test_node_default_work_dir_is_secure() -> TestResult<()> {
    let config = NodeConfig::load_from_env("test-service")?;
    assert!(config.work_dir.is_absolute());
    assert!(!config.work_dir.as_str().contains(".."));
    Ok(())
}
