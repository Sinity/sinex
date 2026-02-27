use reqwest::Client;
use sinex_gateway::{rpc_server, ServiceContainer};
use std::env;
use tokio::sync::watch;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn rpc_server_enforces_auth_token(ctx: TestContext) -> Result<()> {
    // Set up ephemeral NATS (required for ServiceContainer initialization)
    let ctx = ctx.with_nats().shared().await?;

    // Generate self-signed TLS certificates (rpc_server requires TLS for TCP bindings)
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert = rcgen::generate_simple_self_signed(subject_alt_names)?;
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();

    let cert_file = tempfile::NamedTempFile::new()?;
    let key_file = tempfile::NamedTempFile::new()?;
    tokio::fs::write(cert_file.path(), &cert_pem).await?;
    tokio::fs::write(key_file.path(), &key_pem).await?;

    // Configure environment for gateway
    let token = "test-secret-token-123";
    unsafe {
        env::set_var("SINEX_RPC_TOKEN", token);
        env::set_var(
            "SINEX_GATEWAY_TLS_CERT",
            cert_file.path().to_string_lossy().to_string(),
        );
        env::set_var(
            "SINEX_GATEWAY_TLS_KEY",
            key_file.path().to_string_lossy().to_string(),
        );
        env::remove_var("SINEX_GATEWAY_TLS_CLIENT_CA");

        let nats_url = ctx.nats_handle()?.client_url().to_string();
        env::set_var("SINEX_NATS_URL", &nats_url);
        // Disable rate limiting — this test validates auth behavior, not rate limits.
        // Shared NATS KV may have stale counters from parallel tests.
        env::set_var("SINEX_RPC_RATE_LIMIT_ENABLED", "false");
    }

    // Initialize ServiceContainer
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::new(Some(db_url)).await?;

    // Start RPC Server on a random port
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (addr, handle) =
        rpc_server::spawn(Some("127.0.0.1:0"), services, vec![], shutdown_rx).await?;

    let base_url = format!("https://{}/rpc", addr);

    // Client that accepts self-signed certificates
    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;

    // 1. Request without token → 401 Unauthorized
    let resp = client
        .post(&base_url)
        .header("content-type", "application/json")
        .body(r#"{"jsonrpc":"2.0", "method":"system.ping", "id":1}"#)
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "Should reject request without token"
    );

    // 2. Request with invalid token → 401 Unauthorized
    let resp = client
        .post(&base_url)
        .header("content-type", "application/json")
        .header("Authorization", "Bearer invalid-token")
        .body(r#"{"jsonrpc":"2.0", "method":"system.ping", "id":1}"#)
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "Should reject invalid token"
    );

    // 3. Request with valid token → 200 OK (auth passes; method may not exist, but that's 200)
    let resp = client
        .post(&base_url)
        .header("content-type", "application/json")
        .header("Authorization", format!("Bearer {}", token))
        .body(r#"{"jsonrpc":"2.0", "method":"system.ping", "id":1}"#)
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "Should accept valid token"
    );

    // Cleanup
    let _ = shutdown_tx.send(true);
    let _ = handle.await;
    unsafe {
        env::remove_var("SINEX_RPC_TOKEN");
        env::remove_var("SINEX_GATEWAY_TLS_CERT");
        env::remove_var("SINEX_GATEWAY_TLS_KEY");
        env::remove_var("SINEX_NATS_URL");
        env::remove_var("SINEX_RPC_RATE_LIMIT_ENABLED");
    }

    Ok(())
}
