use sinex_gateway::{rpc_server, ServiceContainer};
use std::time::Duration;
use tokio::time::sleep;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_gateway_tcp_tls_handshake(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    // 1. Generate self-signed certs for testing
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert = rcgen::generate_simple_self_signed(subject_alt_names)?;
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();

    // Write to temp files
    let cert_file = tempfile::NamedTempFile::new()?;
    let key_file = tempfile::NamedTempFile::new()?;
    tokio::fs::write(cert_file.path(), &cert_pem).await?;
    tokio::fs::write(key_file.path(), &key_pem).await?;

    // 2. Configure Gateway with TLS
    // We need a random free port. Binding to port 0 usually works.
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener); // Close it so gateway can bind

    // Setup minimal environment
    // We rely on GatewayConfig loading from env or args, but GatewayConfig::load() is env-based.
    // rpc_server::run takes a Config object? Let's check rpc_server signature.
    // Assuming we can construct config or pass env vars.

    // Let's assume we can construct minimal config or partial mock if needed.
    // Note: Since we are in the gateway crate integration tests, we can use internal APIs or just setting env vars.
    std::env::set_var(
        "SINEX_GATEWAY_TLS_CERT",
        cert_file.path().to_string_lossy().to_string(),
    );
    std::env::set_var(
        "SINEX_GATEWAY_TLS_KEY",
        key_file.path().to_string_lossy().to_string(),
    );
    std::env::set_var("SINEX_RPC_TOKEN", "test-token");
    // Ensure host environment CA settings don't bleed into the test
    std::env::remove_var("SINEX_GATEWAY_TLS_CLIENT_CA");

    // Ensure ServiceContainer can connect to NATS
    let nats_url = ctx.nats_handle()?.client_url().to_string();
    std::env::set_var("SINEX_NATS_URL", &nats_url);

    // Initialize ServiceContainer
    let services = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;

    // Create shutdown channel
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn gateway in background
    let gateway_handle = tokio::spawn(async move {
        // Pass the explicit TCP listen address override
        let tcp_listen = format!("127.0.0.1:{port}");
        rpc_server::run(Some(&tcp_listen), services, vec![], shutdown_rx)
            .await
            .expect("Gateway failed");
    });

    // Wait for startup
    sleep(Duration::from_millis(500)).await;

    // 3. Positive Test: Valid Client Handshake
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(cert_pem.as_bytes())?)
        .danger_accept_invalid_certs(true) // Self-signed needs this or root cert add
        .build()?;

    let url = format!("https://127.0.0.1:{port}/health");
    let resp = client.get(&url).send().await;

    // Even if /health doesn't exist or returns 404/401, the *Transport Error* (handshake failure) is what we care about.
    // If handshake succeeds, we get an HTTP response (even 404 Is OK for this test).
    match resp {
        Ok(response) => {
            tracing::info!("TLS Handshake succeeded, got status: {}", response.status());
        }
        Err(e) => {
            if e.is_connect() {
                panic!("Failed to connect: {e}");
            } else if e.is_body() || e.is_request() {
                // Handshake passed, strictly speaking
            } else {
                panic!("Request failed (likely TLS): {e}");
            }
        }
    }

    // 4. Negative Test: Plaintext Client
    let plain_client = reqwest::Client::new();
    let plain_url = format!("http://127.0.0.1:{port}/health"); // HTTP schem against HTTPS port
    let plain_resp = plain_client
        .get(&plain_url)
        .timeout(Duration::from_millis(500))
        .send()
        .await;

    assert!(
        plain_resp.is_err(),
        "Plaintext request to HTTPS port should fail"
    );

    // Cleanup
    gateway_handle.abort();
    Ok(())
}
