//! TLS Integration Test
//!
//! Verifies that the test infrastructure properly propagates TLS configuration
//! through all components: EphemeralNats → TestIngestdConfig → IngestService.

use serde_json::json;
use sinex_test_utils::{
    nats::{shared_ephemeral_nats, SharedNatsProfile},
    prelude::*,
    sinex_test, start_test_ingestd_with_config,
    timing_utils::WaitHelpers,
    TestContext, TestIngestdConfig, TestNodePublisher,
};
use tokio_stream::StreamExt;

/// Verify that TLS configuration is properly propagated from EphemeralNats through
/// the ingestd pipeline. This test exercises the full TLS path:
/// 1. Start NATS with mTLS enabled
/// 2. Start ingestd using the TLS connection config
/// 3. Publish events over TLS
/// 4. Verify events are persisted
#[sinex_test]
async fn tls_enabled_event_pipeline(ctx: TestContext) -> TestResult<()> {
    // Get the shared secure NATS server with TLS enabled
    let nats = shared_ephemeral_nats(SharedNatsProfile::SecureTls).await?;

    // Verify the URL uses tls:// scheme
    let client_url = nats.client_url();
    assert!(
        client_url.starts_with("tls://"),
        "Expected TLS URL, got: {client_url}"
    );

    // Get connection config that includes TLS certificates
    let conn_config = nats.connection_config();
    assert!(conn_config.require_tls, "TLS should be required");
    assert!(conn_config.ca_cert.is_some(), "CA cert should be set");
    assert!(
        conn_config.client_cert.is_some(),
        "Client cert should be set"
    );
    assert!(conn_config.client_key.is_some(), "Client key should be set");

    // Start ingestd with TLS configuration
    let ingest_config = TestIngestdConfig {
        nats: conn_config.clone(),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

    // Connect directly using TLS config to publish events
    let nats_client = conn_config.connect().await?;
    let publisher = TestNodePublisher::new(nats_client, "tls-test-source");

    // Publish a test event
    let event_id = publisher
        .publish_event(
            "tls.test.event",
            json!({
                "message": "Hello over TLS",
                "secure": true
            }),
        )
        .await?;

    // Wait for the event to be persisted
    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), 10).await?;

    // Verify the event exists in the database
    let event = ctx
        .pool
        .events()
        .get_by_id(event_id.into())
        .await?
        .expect("Event should be persisted");

    assert_eq!(event.source.as_str(), "tls-test-source");
    assert_eq!(event.event_type.as_str(), "tls.test.event");

    // Cleanup
    ingest_handle.stop().await?;

    Ok(())
}

/// Verify that connecting with TLS config properly authenticates with the server.
#[sinex_test]
async fn tls_connection_authenticates_properly(ctx: TestContext) -> TestResult<()> {
    let nats = shared_ephemeral_nats(SharedNatsProfile::SecureTls).await?;

    // Connection config should have all TLS fields populated
    let config = nats.connection_config();
    assert!(config.require_tls);

    // Should be able to connect with proper TLS credentials
    let client = config.connect().await?;

    // Verify we can publish/subscribe over the connection
    let mut sub = client.subscribe("tls.test.topic".to_string()).await?;
    client
        .publish("tls.test.topic".to_string(), "test message".into())
        .await?;
    client.flush().await?;

    // Verify message is received
    let msg = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next())
        .await?
        .expect("Should receive message");

    assert_eq!(msg.payload.as_ref(), b"test message");

    Ok(())
}
