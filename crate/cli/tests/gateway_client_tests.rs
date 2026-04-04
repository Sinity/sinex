//! Tests for the gateway client module
//!
//! Uses `MockGatewayClient` for unit tests and wiremock for integration tests.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use xtask::sandbox::prelude::*;

use common::{MockGatewayClient, MockResponse, TestDir, TokenFixture};
use sinex_primitives::domain::HealthStatus;
use sinex_primitives::rpc::dlq::DlqListResponse;
use sinex_primitives::rpc::system::{
    ComponentHealth, ComponentsHealth, ReplayControlHealth, SystemHealthResponse,
};
use sinexctl::client::{ClientConfig, GatewayClient, RetryConfig};

// ============================================================================
// MockGatewayClient Tests
// ============================================================================

#[sinex_test]
async fn test_mock_client_default_responses() -> TestResult<()> {
    let client = MockGatewayClient::new();

    // Default ping response
    assert_eq!(client.ping().await.unwrap(), "pong");

    // Default version response
    assert_eq!(client.version().await.unwrap(), "0.4.2");

    // Default health response
    let health = client.health().await.unwrap();
    assert_eq!(health.status.to_string(), "healthy");
    assert!(health.components.database.connected);
    assert!(health.components.nats.connected);
    Ok(())
}

#[sinex_test]
async fn test_mock_client_custom_health_response() -> TestResult<()> {
    let client = MockGatewayClient::new();

    let custom_health = SystemHealthResponse {
        status: HealthStatus::Degraded,
        healthy: false,
        serving: false,
        degradation_reasons: vec!["nats offline".to_string()],
        components: ComponentsHealth {
            database: ComponentHealth {
                status: HealthStatus::Healthy,
                connected: true,
                latency_ms: None,
                detail: None,
            },
            nats: ComponentHealth {
                status: HealthStatus::Unhealthy,
                connected: false,
                latency_ms: Some(125.0),
                detail: Some("nats unavailable".to_string()),
            },
            replay_control: ReplayControlHealth {
                status: HealthStatus::Healthy,
                enabled: true,
                connected: true,
                last_error: None,
            },
        },
    };

    client.set_response("health", MockResponse::Health(custom_health.clone()));

    let health = client.health().await.unwrap();
    assert_eq!(health.status.to_string(), "degraded");
    assert!(!health.components.nats.connected);
    Ok(())
}

#[sinex_test]
async fn test_mock_client_dlq_operations() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_mock_client_dlq_peek() -> TestResult<()> {
    let client = MockGatewayClient::new();

    let peek_result = client.dlq_peek(Some(5)).await.unwrap();
    assert!(peek_result.messages.is_empty()); // Default response

    // Verify call recorded with args
    let calls = client.get_calls();
    assert_eq!(calls.last().unwrap().0, "dlq_peek");
    assert!(calls.last().unwrap().1[0].contains('5'));
    Ok(())
}

#[sinex_test]
async fn test_mock_client_dlq_requeue() -> TestResult<()> {
    let client = MockGatewayClient::new();

    let event_ids = vec!["event-1".to_string(), "event-2".to_string()];
    let result = client.dlq_requeue(event_ids.clone()).await.unwrap();
    assert_eq!(result.status, "success");

    // Verify call recorded with event IDs
    let calls = client.get_calls();
    let (method, args) = calls.last().unwrap();
    assert_eq!(method, "dlq_requeue");
    assert_eq!(args, &event_ids);
    Ok(())
}

#[sinex_test]
async fn test_mock_client_replay_operations() -> TestResult<()> {
    let client = MockGatewayClient::new();

    // List replay operations
    let ops = client.replay_list().await.unwrap();
    assert!(ops.is_empty()); // Default response

    // Get replay status
    let status = client.replay_status("op-123").await.unwrap();
    assert_eq!(status.operation_id, "op-123");
    assert_eq!(status.scope.node_id, "test-node");

    // Verify calls
    let calls = client.get_calls();
    assert!(calls.iter().any(|(m, _)| m == "replay_list"));
    assert!(
        calls
            .iter()
            .any(|(m, args)| m == "replay_status" && args[0] == "op-123")
    );
    Ok(())
}

#[sinex_test]
async fn test_mock_client_query_events() -> TestResult<()> {
    use sinex_primitives::query::{EventQuery, EventQueryResult, PayloadFilter};

    let client = MockGatewayClient::new();

    let query = EventQuery {
        sources: vec!["shell".into()],
        payload: Some(PayloadFilter::TextSearch {
            text: "error".to_string(),
        }),
        ..Default::default()
    };

    let result = client.query_events(query).await.unwrap();
    match result {
        EventQueryResult::Events { events, .. } => assert!(events.is_empty()),
        _ => panic!("expected Events variant"),
    }

    // Verify call recorded
    let calls = client.get_calls();
    assert!(calls.iter().any(|(m, _)| m == "query_events"));
    Ok(())
}

#[sinex_test]
async fn test_mock_client_thread_safe() -> TestResult<()> {
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
    Ok(())
}

// ============================================================================
// GatewayClient Creation Tests
// ============================================================================

#[sinex_test]
async fn test_gateway_client_creation_with_token() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_creation_without_token_fails() -> TestResult<()> {
    // Clear environment
    unsafe { std::env::remove_var("SINEX_RPC_TOKEN") };

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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_with_explicit_token() -> TestResult<()> {
    let config = ClientConfig {
        url: "https://localhost:9999".to_string(),
        token: Some("explicit-token".to_string()),
        insecure: true,
        ..Default::default()
    };

    let result = GatewayClient::new(config);
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_creation_with_mtls_material() -> TestResult<()> {
    use xtask::tls::{CertConfig, generate_dev_certs};

    let dir = TestDir::new();
    let certs = CertConfig {
        output_dir: dir.path().to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Gateway Client TLS Test CA".to_string(),
        validity_days: 30,
        force: false,
    };
    generate_dev_certs(&certs)?;

    let result = GatewayClient::new(ClientConfig {
        url: "https://localhost:9999".to_string(),
        token: Some("explicit-token".to_string()),
        ca_cert: Some(dir.path().join("ca.pem").display().to_string()),
        client_cert: Some(dir.path().join("client.pem").display().to_string()),
        client_key: Some(dir.path().join("client-key.pem").display().to_string()),
        insecure: false,
        ..Default::default()
    });

    assert!(result.is_ok(), "mTLS client configuration should build");
    Ok(())
}

// ============================================================================
// HTTP Error Handling Tests (with wiremock)
// ============================================================================

#[sinex_test]
async fn test_gateway_client_handles_http_error() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_handles_401_unauthorized() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_handles_rpc_error() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_handles_rpc_error_with_data() -> TestResult<()> {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "error": {
                "code": -32602,
                "message": "Invalid params",
                "data": {"field": "node_id", "reason": "required"}
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
    assert!(err.contains("node_id"));
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_handles_malformed_response() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_handles_missing_result() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_rejects_jsonrpc_version_mismatch() -> TestResult<()> {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "1.0",
            "result": "pong",
            "id": 1
        })))
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
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expected jsonrpc=2.0")
    );
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_rejects_jsonrpc_id_mismatch() -> TestResult<()> {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "result": "pong",
            "id": 99
        })))
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
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expected response id 1, got 99")
    );
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_rejects_non_string_ping_result() -> TestResult<()> {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "result": { "pong": true },
            "id": 1
        })))
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
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("ping returned non-string result")
    );
    Ok(())
}

// ============================================================================
// Timeout Tests
// ============================================================================

#[sinex_test]
async fn test_gateway_client_timeout() -> TestResult<()> {
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
    Ok(())
}

// ============================================================================
// Retry Logic Tests
// ============================================================================

#[sinex_test]
async fn test_gateway_client_retry_on_5xx() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_no_retry_on_4xx() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_retry_exhausted() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_retry_on_rate_limit() -> TestResult<()> {
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
    Ok(())
}

// ============================================================================
// ClientConfig Tests
// ============================================================================

#[sinex_test]
async fn test_client_config_default() -> TestResult<()> {
    let config = ClientConfig::default();

    assert!(config.url.starts_with("https://"));
    assert!(config.token.is_none());
    assert!(config.token_file.is_none());
    assert!(config.ca_cert.is_none());
    assert!(!config.insecure);
    assert_eq!(config.timeout, 30);
    Ok(())
}

#[sinex_test]
async fn test_client_config_from_app_config() -> TestResult<()> {
    use sinexctl::config::Config;

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
    Ok(())
}

// ============================================================================
// Successful RPC Call Tests
// ============================================================================

#[sinex_test]
async fn test_gateway_client_successful_ping() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_successful_version() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_successful_health() -> TestResult<()> {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "result": {
                "status": "healthy",
                "healthy": true,
                "serving": true,
                "degradation_reasons": [],
                "components": {
                    "database": {
                        "status": "healthy",
                        "connected": true,
                        "latency_ms": null,
                        "detail": null
                    },
                    "nats": {
                        "status": "healthy",
                        "connected": true,
                        "latency_ms": 2.5,
                        "detail": "jetstream responsive"
                    },
                    "replay_control": {
                        "status": "healthy",
                        "enabled": true,
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

    assert_eq!(health.status.to_string(), "healthy");
    assert!(health.healthy);
    assert!(health.serving);
    assert!(health.components.database.connected);
    assert!(health.components.nats.connected);
    assert_eq!(health.components.nats.latency_ms, Some(2.5));
    assert_eq!(
        health.components.nats.detail.as_deref(),
        Some("jetstream responsive")
    );
    Ok(())
}

#[sinex_test]
async fn test_gateway_client_replay_submit_previews_before_execute() -> TestResult<()> {
    fn replay_operation_json(
        state: &str,
        preview_summary: Option<serde_json::Value>,
    ) -> serde_json::Value {
        json!({
            "operation_id": "00000000-0000-0000-0000-000000000123",
            "state": state,
            "scope": {
                "node_id": "test-node",
                "time_window": null,
                "material_filter": null,
                "filters": {}
            },
            "preview_summary": preview_summary,
            "checkpoint": {
                "processed_events": 0,
                "total_events": 1,
                "last_event_id": null,
                "batch_number": 0,
                "savepoint_id": null,
                "updated_at": "2026-04-02T00:00:00Z"
            },
            "actor": "service:test",
            "created_at": "2026-04-02T00:00:00Z",
            "approved_by": "service:test",
            "approved_at": "2026-04-02T00:00:01Z",
            "executor_node": null,
            "started_at": null,
            "finished_at": null,
            "outcome": null,
            "error_details": null
        })
    }

    let mock_server = MockServer::start().await;
    let seen_methods = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_methods_clone = Arc::clone(&seen_methods);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &wiremock::Request| {
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).expect("valid rpc body");
            let method = body["method"].as_str().unwrap_or_default().to_string();
            seen_methods_clone
                .lock()
                .expect("record methods")
                .push(method.clone());

            let response = match method.as_str() {
                "replay.operation_status" => json!({
                    "jsonrpc": "2.0",
                    "result": {
                        "operation": replay_operation_json("Planning", None)
                    },
                    "id": 1
                }),
                "replay.preview_operation" => json!({
                    "jsonrpc": "2.0",
                    "result": {
                        "operation": replay_operation_json("Previewed", Some(json!({
                            "total_events": 1,
                            "time_window": {
                                "start": "2026-04-01T00:00:00Z",
                                "end": "2026-04-02T00:00:00Z"
                            }
                        }))),
                        "preview": {
                            "total_events": 1,
                            "time_window": {
                                "start": "2026-04-01T00:00:00Z",
                                "end": "2026-04-02T00:00:00Z"
                            }
                        }
                    },
                    "id": 1
                }),
                "replay.submit_operation" => json!({
                    "jsonrpc": "2.0",
                    "result": {
                        "operation": replay_operation_json("Executing", Some(json!({
                            "total_events": 1,
                            "time_window": {
                                "start": "2026-04-01T00:00:00Z",
                                "end": "2026-04-02T00:00:00Z"
                            }
                        })))
                    },
                    "id": 1
                }),
                other => panic!("unexpected replay method {other}"),
            };

            ResponseTemplate::new(200).set_body_json(response)
        })
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
    let operation = client
        .replay_submit("00000000-0000-0000-0000-000000000123")
        .await?;

    assert_eq!(
        operation.state,
        sinex_primitives::rpc::replay::ReplayState::Executing
    );
    assert_eq!(
        seen_methods.lock().expect("read methods").as_slice(),
        &[
            "replay.operation_status".to_string(),
            "replay.preview_operation".to_string(),
            "replay.submit_operation".to_string()
        ]
    );
    Ok(())
}
