//! Integration tests for `MockGatewayClient`

mod common;

use common::{MockGatewayClient, MockResponse};

#[tokio::test]
async fn test_mock_client_ping() {
    let client = MockGatewayClient::new();
    let result = client.ping().await.unwrap();
    assert_eq!(result, "pong");

    let calls = client.get_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "ping");
}

#[tokio::test]
async fn test_mock_client_custom_response() {
    let client = MockGatewayClient::new();
    client.set_response("ping", MockResponse::String("custom_pong".to_string()));

    let result = client.ping().await.unwrap();
    assert_eq!(result, "custom_pong");
}

#[tokio::test]
async fn test_mock_client_records_calls() {
    let client = MockGatewayClient::new();

    client.ping().await.unwrap();
    client.version().await.unwrap();
    client.health().await.unwrap();

    let calls = client.get_calls();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0].0, "ping");
    assert_eq!(calls[1].0, "version");
    assert_eq!(calls[2].0, "health");
}

#[tokio::test]
async fn test_mock_client_clear_calls() {
    let client = MockGatewayClient::new();

    client.ping().await.unwrap();
    assert_eq!(client.get_calls().len(), 1);

    client.clear_calls();
    assert_eq!(client.get_calls().len(), 0);
}

#[tokio::test]
async fn test_mock_client_node_operations() {
    let client = MockGatewayClient::new();

    client
        .drain_node("node-1", Some("maintenance"))
        .await
        .unwrap();
    client.resume_node("node-1").await.unwrap();
    client
        .set_node_horizon("node-1", "2024-01-01T00:00:00Z")
        .await
        .unwrap();

    let calls = client.get_calls();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0].0, "drain_node");
    assert_eq!(calls[1].0, "resume_node");
    assert_eq!(calls[2].0, "set_node_horizon");
}
