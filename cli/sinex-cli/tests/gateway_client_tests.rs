//! Tests for the gateway client module
//!
//! Uses `MockGatewayClient` for unit tests and wiremock for integration tests.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use common::{MockGatewayClient, MockResponse, TestDir, TokenFixture};
use sinex_cli::client::{ClientConfig, GatewayClient, RetryConfig};
use sinex_primitives::rpc::dlq::DlqListResponse;
use sinex_primitives::rpc::system::{
    ComponentHealth, ComponentsHealth, ReplayControlHealth, SystemHealthResponse,
};

// ============================================================================
// MockGatewayClient Tests
// ============================================================================

#[tokio::test]
async fn test_mock_client_default_responses() {
    let client = MockGatewayClient::new();

    // Default ping response
    assert_eq!(client.ping().await.unwrap(), "pong");

    // Default version response
    assert_eq!(client.version().await.unwrap(), "0.4.2");

    // Default health response
    let health = client.health().await.unwrap();
    assert_eq!(health.status, "healthy");
    assert!(health.components.database.connected);
    assert!(health.components.nats.connected);
}

#[tokio::test]
async fn test_mock_client_custom_health_response() {
    let client = MockGatewayClient::new();

    let custom_health = SystemHealthResponse {
        status: "degraded".to_string(),
        components: ComponentsHealth {
            database: ComponentHealth {
                status: "healthy".to_string(),
                connected: true,
            },
            nats: ComponentHealth {
                status: "unhealthy".to_string(),
                connected: false,
            },
            replay_control: ReplayControlHealth {
                status: "healthy".to_string(),
                enabled: true,
                bypass_allowed: false,
                bypass_active: false,
                connected: true,
                last_error: None,
            },
        },
    };

    client.set_response("health", MockResponse::Health(custom_health.clone()));

    let health = client.health().await.unwrap();
    assert_eq!(health.status, "degraded");
    assert!(!health.components.nats.connected);
}

#[tokio::test]
async fn test_mock_client_dlq_operations() {
    let client = MockGatewayClient::new();

    // Set custom DLQ list response
    client.set_response(
        "dlq_list",
        MockResponse::DlqList(DlqListResponse {
            total_messages: 42,
            total_bytes: 1024,
            first_seq: 1,
            last_seq: 42,
        }),
    );

    let list = client.dlq_list().await.unwrap();
    assert_eq!(list.total_messages, 42);
    assert_eq!(list.total_bytes, 1024);

    // Verify call was recorded
    let calls = client.get_calls();
    assert!(calls.iter().any(|(method, _)| method == "dlq_list"));
}

#[tokio::test]
async fn test_mock_client_dlq_peek() {
    let client = MockGatewayClient::new();

    let peek_result = client.dlq_peek(Some(5)).await.unwrap();
    assert!(peek_result.messages.is_empty()); // Default response

    // Verify call recorded with args
    let calls = client.get_calls();
    assert_eq!(calls.last().unwrap().0, "dlq_peek");
    assert!(calls.last().unwrap().1[0].contains('5'));
}

#[tokio::test]
async fn test_mock_client_dlq_requeue() {
    let client = MockGatewayClient::new();

    let event_ids = vec!["event-1".to_string(), "event-2".to_string()];
    let result = client.dlq_requeue(event_ids.clone()).await.unwrap();
    assert_eq!(result.status, "success");

    // Verify call recorded with event IDs
    let calls = client.get_calls();
    let (method, args) = calls.last().unwrap();
    assert_eq!(method, "dlq_requeue");
    assert_eq!(args, &event_ids);
}

#[tokio::test]
async fn test_mock_client_replay_operations() {
    let client = MockGatewayClient::new();

    // List replay operations
    let ops = client.replay_list().await.unwrap();
    assert!(ops.is_empty()); // Default response

    // Get replay status
    let status = client.replay_status("op-123").await.unwrap();
    assert_eq!(status.operation_id, "op-123");
    assert_eq!(status.scope.processor_id, "test-processor");

    // Verify calls
    let calls = client.get_calls();
    assert!(calls.iter().any(|(m, _)| m == "replay_list"));
    assert!(calls
        .iter()
        .any(|(m, args)| m == "replay_status" && args[0] == "op-123"));
}

#[tokio::test]
async fn test_mock_client_search() {
    use sinex_cli::model::search::SearchQuery;

    let client = MockGatewayClient::new();

    let query = SearchQuery {
        text: Some("error".to_string()),
        sources: vec!["shell".to_string()],
        ..Default::default()
    };

    let results = client.search_events(query).await.unwrap();
    assert!(results.is_empty()); // Default response

    // Verify call recorded
    let calls = client.get_calls();
    assert!(calls.iter().any(|(m, _)| m == "search_events"));
}

#[tokio::test]
async fn test_mock_client_thread_safe() {
    let client = MockGatewayClient::new();
    let client_clone = client.clone();

    // Call from multiple tasks
    let handle1 = tokio::spawn({
        let c = client.clone();
        async move {
            for _ in 0..10 {
                c.ping().await.unwrap();
            }
        }
    });

    let handle2 = tokio::spawn({
        let c = client_clone;
        async move {
            for _ in 0..10 {
                c.version().await.unwrap();
            }
        }
    });

    handle1.await.unwrap();
    handle2.await.unwrap();

    let calls = client.get_calls();
    assert_eq!(calls.len(), 20);
}

// ============================================================================
// GatewayClient Creation Tests
// ============================================================================

#[tokio::test]
async fn test_gateway_client_creation_with_token() {
    let dir = TestDir::new();
    let token_path = dir.create_file("token", TokenFixture::valid());

    let config = ClientConfig {
        url: "https://localhost:9999".to_string(),
        token_file: Some(token_path.to_string_lossy().to_string()),
        insecure: true,
        ..Default::default()
    };

    let result = GatewayClient::new(config);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_gateway_client_creation_without_token_fails() {
    // Clear environment
    std::env::remove_var("SINEX_RPC_TOKEN");

    let config = ClientConfig {
        url: "https://localhost:9999".to_string(),
        token: None,
        token_file: None,
        insecure: true,
        ..Default::default()
    };

    let result = GatewayClient::new(config);
    assert!(result.is_err());
    let err = result.err().unwrap().to_string();
    assert!(err.contains("No authentication token"));
}

#[tokio::test]
async fn test_gateway_client_with_explicit_token() {
    let config = ClientConfig {
        url: "https://localhost:9999".to_string(),
        token: Some("explicit-token".to_string()),
        insecure: true,
        ..Default::default()
    };

    let result = GatewayClient::new(config);
    assert!(result.is_ok());
}

// ============================================================================
// HTTP Error Handling Tests (with wiremock)
// ============================================================================

#[tokio::test]
async fn test_gateway_client_handles_http_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        retry_config: RetryConfig::builder().max_attempts(1).build(),
        ..Default::default()
    };

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("HTTP 500"));
}

#[tokio::test]
async fn test_gateway_client_handles_401_unauthorized() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("invalid-token".to_string()),
        insecure: true,
        retry_config: RetryConfig::builder().max_attempts(1).build(),
        ..Default::default()
    };

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("401"));
}

#[tokio::test]
async fn test_gateway_client_handles_rpc_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "error": {
                "code": -32601,
                "message": "Method not found"
            },
            "id": 1
        })))
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        ..Default::default()
    };

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("-32601"));
    assert!(err.contains("Method not found"));
}

#[tokio::test]
async fn test_gateway_client_handles_rpc_error_with_data() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "error": {
                "code": -32602,
                "message": "Invalid params",
                "data": {"field": "processor_id", "reason": "required"}
            },
            "id": 1
        })))
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        ..Default::default()
    };

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Invalid params"));
    assert!(err.contains("processor_id"));
}

#[tokio::test]
async fn test_gateway_client_handles_malformed_response() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        ..Default::default()
    };

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_gateway_client_handles_missing_result() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1
            // Missing both result and error
        })))
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        ..Default::default()
    };

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("missing result"));
}

// ============================================================================
// Timeout Tests
// ============================================================================

#[tokio::test]
async fn test_gateway_client_timeout() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(10)))
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        timeout: 1, // 1 second timeout
        retry_config: RetryConfig::builder().max_attempts(1).build(),
        ..Default::default()
    };

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string().to_lowercase();
    // The error may contain various timeout-related messages depending on the platform
    // reqwest may show "error sending request" when the request times out
    assert!(
        err.contains("timeout")
            || err.contains("timed out")
            || err.contains("deadline")
            || err.contains("operation was canceled")
            || err.contains("error sending request"),
        "Expected timeout error, got: {err}"
    );
}

// ============================================================================
// Retry Logic Tests
// ============================================================================

#[tokio::test]
async fn test_gateway_client_retry_on_5xx() {
    let mock_server = MockServer::start().await;
    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count_clone = attempt_count.clone();

    // Fail twice with 500, then succeed
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |_req: &wiremock::Request| {
            let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst);
            if count < 2 {
                ResponseTemplate::new(500).set_body_string("Server Error")
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

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await;

    assert!(result.is_ok());
    assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_gateway_client_no_retry_on_4xx() {
    let mock_server = MockServer::start().await;
    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count_clone = attempt_count.clone();

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |_req: &wiremock::Request| {
            attempt_count_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(400).set_body_string("Bad Request")
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

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await;

    assert!(result.is_err());
    // Should only attempt once for client errors
    assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_gateway_client_retry_exhausted() {
    let mock_server = MockServer::start().await;
    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count_clone = attempt_count.clone();

    // Always fail with 500
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |_req: &wiremock::Request| {
            attempt_count_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(500).set_body_string("Permanent Error")
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

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await;

    assert!(result.is_err());
    // Should attempt exactly max_attempts times
    assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_gateway_client_retry_on_rate_limit() {
    let mock_server = MockServer::start().await;
    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count_clone = attempt_count.clone();

    // Rate limit once, then succeed
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |_req: &wiremock::Request| {
            let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst);
            if count < 1 {
                ResponseTemplate::new(429).set_body_string("Rate Limited")
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

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await;

    assert!(result.is_ok());
    assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
}

// ============================================================================
// ClientConfig Tests
// ============================================================================

#[test]
fn test_client_config_default() {
    let config = ClientConfig::default();

    assert!(config.url.starts_with("https://"));
    assert!(config.token.is_none());
    assert!(config.token_file.is_none());
    assert!(config.ca_cert.is_none());
    assert!(!config.insecure);
    assert_eq!(config.timeout, 30);
}

#[test]
fn test_client_config_from_app_config() {
    use sinex_cli::config::Config;

    let mut app_config = Config::default();
    app_config.rpc_url = "https://custom:8080".to_string();
    app_config.token = Some("config-token".to_string());
    app_config.insecure = true;
    app_config.timeout = 60;

    let client_config: ClientConfig = (&app_config).into();

    assert_eq!(client_config.url, "https://custom:8080");
    assert_eq!(client_config.token, Some("config-token".to_string()));
    assert!(client_config.insecure);
    assert_eq!(client_config.timeout, 60);
}

// ============================================================================
// Successful RPC Call Tests
// ============================================================================

#[tokio::test]
async fn test_gateway_client_successful_ping() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "result": "pong",
            "id": 1
        })))
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        ..Default::default()
    };

    let client = GatewayClient::new(config).unwrap();
    let result = client.ping().await.unwrap();

    assert_eq!(result, "pong");
}

#[tokio::test]
async fn test_gateway_client_successful_version() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "result": "1.2.3",
            "id": 1
        })))
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        ..Default::default()
    };

    let client = GatewayClient::new(config).unwrap();
    let result = client.version().await.unwrap();

    assert_eq!(result, "1.2.3");
}

#[tokio::test]
async fn test_gateway_client_successful_health() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "result": {
                "status": "healthy",
                "components": {
                    "database": {
                        "status": "healthy",
                        "connected": true
                    },
                    "nats": {
                        "status": "healthy",
                        "connected": true
                    },
                    "replay_control": {
                        "status": "healthy",
                        "enabled": true,
                        "bypass_allowed": false,
                        "bypass_active": false,
                        "connected": true,
                        "last_error": null
                    }
                }
            },
            "id": 1
        })))
        .mount(&mock_server)
        .await;

    let config = ClientConfig {
        url: mock_server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        ..Default::default()
    };

    let client = GatewayClient::new(config).unwrap();
    let health = client.health().await.unwrap();

    assert_eq!(health.status, "healthy");
    assert!(health.components.database.connected);
    assert!(health.components.nats.connected);
}
