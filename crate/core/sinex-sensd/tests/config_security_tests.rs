//! Sensd configuration security regressions migrated from the workspace harness.

use serde_json::json;
use sinex_sensd::config::SensdConfig;
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn test_sensd_config_deserialization_security(
    _ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let malicious_sensd_config = json!({
        "database_url": "postgresql://localhost/test",
        "grpc_port": 50052,
        "material_storage_path": "../../../../tmp/evil"
    });

    let result: Result<SensdConfig, _> = serde_json::from_value(malicious_sensd_config);
    assert!(
        result.is_err(),
        "Malicious material_storage_path should be rejected"
    );
    Ok(())
}

#[sinex_test]
async fn test_sensd_default_path_security(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let config = SensdConfig::default();
    assert!(!config.material_storage_path.contains(".."));
    Ok(())
}
