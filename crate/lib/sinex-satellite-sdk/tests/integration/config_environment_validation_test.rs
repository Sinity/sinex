//! Satellite configuration environment validation tests.

use sinex_satellite_sdk::SatelliteConfig;
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn test_satellite_environment_path_validation() -> color_eyre::eyre::Result<()> {
    std::env::set_var("SINEX_WORK_DIR", "../../../etc");

    let config = SatelliteConfig::load_from_env("test-satellite");
    assert!(!config.work_dir.as_str().contains("../../"));
    assert!(config.work_dir.is_absolute());

    std::env::remove_var("SINEX_WORK_DIR");
    Ok(())
}

#[sinex_test]
async fn test_satellite_default_work_dir_is_secure() -> color_eyre::eyre::Result<()> {
    let config = SatelliteConfig::load_from_env("test-service");
    assert!(config.work_dir.is_absolute());
    assert!(!config.work_dir.as_str().contains(".."));
    Ok(())
}
