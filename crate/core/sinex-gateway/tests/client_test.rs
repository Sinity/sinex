use sinex_gateway::client::GatewayClient;
use std::time::Duration;

#[tokio::test]
async fn test_client_builder_defaults() {
    let _client = GatewayClient::builder()
        .build()
        .expect("Failed to build default client");
    // If it builds, defaults are valid
}

#[tokio::test]
async fn test_client_builder_custom_config() {
    let _client = GatewayClient::builder()
        .base_url("https://api.internal:4000")
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build custom client");
}

#[tokio::test]
async fn test_mtls_identity_loading_missing_file() {
    let result = GatewayClient::builder()
        .load_pem_identity("non_existent_file.pem")
        .await;

    assert!(result.is_err(), "Should fail for missing identity file");
}
