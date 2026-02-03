use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use sinexctl::client::{ClientConfig, GatewayClient, RetryConfig};

#[tokio::test]
async fn test_retry_on_connection_failure() {
    // Use an invalid URL that will fail to connect
    let url = "http://127.0.0.1:1".to_string(); // Port 1 is typically closed

    let config = ClientConfig {
        url,
        token: Some("test-token".to_string()),
        insecure: true,
        timeout: 1, // Short timeout
        retry_config: RetryConfig::builder()
            .max_attempts(3)
            .initial_delay(Duration::from_millis(10))
            .build(),
        ..Default::default()
    };

    let client = GatewayClient::new(config).expect("Failed to create client");

    // This should fail after retries
    let result = client.ping().await;
    assert!(result.is_err(), "Expected error but got success");
}

#[tokio::test]
async fn test_retry_on_server_error_then_success() {
    let mock_server = MockServer::start().await;
    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count_clone = attempt_count.clone();

    // First 2 attempts: return 500 error
    // Third attempt: return success
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |_req: &wiremock::Request| {
            let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst);
            if count < 2 {
                ResponseTemplate::new(500).set_body_string("Internal Server Error")
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "result": "pong",
                    "id": 1
                }))
            }
        })
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        retry_config: RetryConfig::builder()
            .max_attempts(3)
            .initial_delay(Duration::from_millis(10))
            .build(),
        ..Default::default()
    };

    let client = GatewayClient::new(config).expect("Failed to create client");

    // Should succeed after retries
    let result = client.ping().await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "pong");

    // Verify it retried 3 times
    assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_no_retry_on_client_error() {
    let mock_server = MockServer::start().await;
    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count_clone = attempt_count.clone();

    // Always return 404 (client error - should not retry)
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |_req: &wiremock::Request| {
            attempt_count_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(404).set_body_string("Not Found")
        })
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        retry_config: RetryConfig::builder()
            .max_attempts(3)
            .initial_delay(Duration::from_millis(10))
            .build(),
        ..Default::default()
    };

    let client = GatewayClient::new(config).expect("Failed to create client");

    // Should fail immediately without retry
    let result = client.ping().await;
    assert!(result.is_err());

    // Should only attempt once (no retry on 404)
    assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_no_retry_on_auth_error() {
    let mock_server = MockServer::start().await;
    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count_clone = attempt_count.clone();

    // Always return 401 (auth error - should not retry)
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |_req: &wiremock::Request| {
            attempt_count_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(401).set_body_string("Unauthorized")
        })
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        retry_config: RetryConfig::builder()
            .max_attempts(3)
            .initial_delay(Duration::from_millis(10))
            .build(),
        ..Default::default()
    };

    let client = GatewayClient::new(config).expect("Failed to create client");

    // Should fail immediately without retry
    let result = client.ping().await;
    assert!(result.is_err());

    // Should only attempt once (no retry on 401)
    assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_exponential_backoff_timing() {
    let retry_config = RetryConfig::builder()
        .initial_delay(Duration::from_millis(100))
        .multiplier(2.0)
        .build();

    // Attempt 0: 100ms
    let backoff0 = retry_config.backoff_for_attempt(0);
    assert!(
        (backoff0.as_millis() as i64 - 100).abs() < 2,
        "Expected ~100ms, got {backoff0:?}"
    );

    // Attempt 1: 100ms * 2^0 = 100ms
    let backoff1 = retry_config.backoff_for_attempt(1);
    assert!(
        (backoff1.as_millis() as i64 - 100).abs() < 2,
        "Expected ~100ms, got {backoff1:?}"
    );

    // Attempt 2: 100ms * 2^1 = 200ms
    let backoff2 = retry_config.backoff_for_attempt(2);
    assert!(
        (backoff2.as_millis() as i64 - 200).abs() < 2,
        "Expected ~200ms, got {backoff2:?}"
    );

    // Attempt 3: 100ms * 2^2 = 400ms
    let backoff3 = retry_config.backoff_for_attempt(3);
    assert!(
        (backoff3.as_millis() as i64 - 400).abs() < 2,
        "Expected ~400ms, got {backoff3:?}"
    );
}

#[tokio::test]
async fn test_backoff_capped_at_max() {
    let retry_config = RetryConfig::builder()
        .initial_delay(Duration::from_secs(1))
        .max_delay(Duration::from_secs(5))
        .multiplier(10.0)
        .build();

    // Should be capped at 5 seconds
    let backoff = retry_config.backoff_for_attempt(10);
    assert!(backoff <= Duration::from_secs(5));
}
