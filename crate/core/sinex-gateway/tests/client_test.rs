use sinex_gateway::client::GatewayClient;
use xtask::sandbox::sinex_test;
use std::time::Duration;

#[sinex_test]
async fn test_client_builder_defaults() -> TestResult<()> {
    let _client = GatewayClient::builder()
        .build()
        .expect("Failed to build default client");
    // If it builds, defaults are valid
    Ok(())
}

#[sinex_test]
async fn test_client_builder_custom_config() -> TestResult<()> {
    let _client = GatewayClient::builder()
        .base_url("https://api.internal:4000")
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build custom client");
    Ok(())
}

#[sinex_test]
async fn test_mtls_identity_loading_missing_file() -> TestResult<()> {
    let result = GatewayClient::builder()
        .load_pem_identity("non_existent_file.pem")
        .await;

    assert!(result.is_err(), "Should fail for missing identity file");
    Ok(())
}
