use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_format_http_failure_body_includes_non_empty_body()
-> ::xtask::sandbox::TestResult<()> {
    let message = format_http_failure_body(
        reqwest::StatusCode::BAD_REQUEST,
        Ok::<String, &str>("bad request details".to_string()),
    );
    assert_eq!(
        message,
        "RPC request failed with status 400 Bad Request: bad request details"
    );
    Ok(())
}

#[sinex_test]
async fn test_format_http_failure_body_surfaces_body_read_failures()
-> ::xtask::sandbox::TestResult<()> {
    let message = format_http_failure_body(reqwest::StatusCode::BAD_GATEWAY, Err("boom"));
    assert!(message.contains("RPC request failed with status 502 Bad Gateway"));
    assert!(message.contains("failed to read error body"));
    assert!(message.contains("boom"));
    Ok(())
}

#[sinex_test]
async fn test_consumer_name_format() -> ::xtask::sandbox::TestResult<()> {
    let config = TetherConfig {
        target: "prod".to_string(),
        gateway_url: "https://localhost:9999".to_string(),
        auth_token: "test-token".to_string(),
        subject_filter: None,
        consumer_prefix: "dev-testuser".to_string(),
        from_beginning: false,
        from_sequence: None,
        nats_url: "nats://localhost:4222".to_string(),
        nats_creds: None,
        nats_ca: None,
        nats_cert: None,
        nats_key: None,
    };

    let name = config.consumer_name();
    assert!(name.starts_with("dev-testuser-"));
    // Should have timestamp suffix
    // Compact format is 15 chars (YYYYMMDDTHHMMSS)
    let suffix = name.trim_start_matches("dev-testuser-");
    assert_eq!(suffix.len(), 15);
    assert!(suffix.chars().all(|c| c.is_ascii_digit() || c == 'T'));
    Ok(())
}

#[sinex_test]
async fn test_shadow_create_params_include_from_sequence() -> ::xtask::sandbox::TestResult<()> {
    let config = TetherConfig {
        target: "prod".to_string(),
        gateway_url: "https://localhost:9999".to_string(),
        auth_token: "test-token".to_string(),
        subject_filter: Some("events.>".to_string()),
        consumer_prefix: "dev-testuser".to_string(),
        from_beginning: false,
        from_sequence: Some(42),
        nats_url: "nats://localhost:4222".to_string(),
        nats_creds: None,
        nats_ca: None,
        nats_cert: None,
        nats_key: None,
    };
    let client = TetherClient::new(config)?;

    let params = client.shadow_create_params("dev-testuser-20260329T094800");
    assert_eq!(
        params["consumer_name"],
        serde_json::json!("dev-testuser-20260329T094800")
    );
    assert_eq!(params["subject_filter"], serde_json::json!("events.>"));
    assert_eq!(params["from_beginning"], serde_json::json!(false));
    assert_eq!(params["from_sequence"], serde_json::json!(42));
    Ok(())
}

#[sinex_test]
async fn test_invalid_gateway_certs_only_allowed_for_loopback()
-> ::xtask::sandbox::TestResult<()> {
    assert!(should_accept_invalid_gateway_certs(
        "https://localhost:9999"
    ));
    assert!(should_accept_invalid_gateway_certs(
        "https://127.0.0.1:9999"
    ));
    assert!(should_accept_invalid_gateway_certs("https://[::1]:9999"));
    assert!(!should_accept_invalid_gateway_certs(
        "https://gateway.prod.sinex.io:9999"
    ));
    assert!(!should_accept_invalid_gateway_certs("not-a-url"));
    Ok(())
}
