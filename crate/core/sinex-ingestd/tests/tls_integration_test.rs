//! TLS Integration Test
//!
//! Verifies that the test infrastructure properly propagates TLS configuration
//! through all components: `EphemeralNats` → `TestIngestdConfig` → `IngestService`.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::{Id, SourceMaterial, Ulid};
use std::time::Duration;
use xtask::sandbox::{
    nats::{shared_ephemeral_nats, SharedNatsProfile},
    prelude::*,
    sinex_test, start_test_ingestd_with_config,
    timing::{Timeouts, WaitHelpers},
    TestIngestdConfig,
};

/// Helper to publish a test event directly to JetStream.
///
/// The caller must pre-register `material_id` in the database before calling this
/// (and before starting ingestd, so that the MaterialReadySet is seeded from DB).
async fn publish_test_event(
    nats_client: &async_nats::Client,
    material_id: Id<SourceMaterial>,
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
) -> TestResult<Ulid> {
    let env = sinex_primitives::environment();
    let event_id = Ulid::new();
    let ts_orig = sinex_primitives::temporal::now().format_rfc3339();

    let event = json!({
        "id": event_id.to_string(),
        "source": source,
        "event_type": event_type,
        "payload": payload,
        "ts_orig": ts_orig,
        "host": "test-host",
        "node_version": "test",
        "source_material_id": material_id.as_ulid().to_string(),
    });

    let subject = env.nats_subject(&format!(
        "events.raw.{}.{}",
        source.replace('.', "_"),
        event_type.replace('.', "_")
    ));
    nats_client
        .publish(subject, serde_json::to_vec(&event)?.into())
        .await?;
    nats_client.flush().await?;

    Ok(event_id)
}

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

    // Pre-register source material BEFORE starting ingestd.
    //
    // ingestd seeds its MaterialReadySet from the database at startup. If we register
    // the material after ingestd is running, it won't be in the ReadySet and the event
    // will be NAK'd indefinitely (never persisted). Pre-registration ensures the material
    // is visible to ingestd's startup seed query.
    let material_id = Id::<SourceMaterial>::new();
    let run_suffix = Ulid::new();
    sqlx::query!(
        r#"
        INSERT INTO raw.source_material_registry
            (id, material_kind, source_identifier, status, timing_info_type)
        VALUES ($1::uuid::ulid, 'annex', $2, 'completed', 'realtime')
        ON CONFLICT (id) DO NOTHING
        "#,
        material_id.to_uuid(),
        format!("tls-test-{run_suffix}"),
    )
    .execute(&ctx.pool)
    .await?;

    // Start ingestd with TLS configuration
    let work_dir = tempfile::tempdir()?;
    let ingest_config = TestIngestdConfig {
        nats: conn_config.clone(),
        database_url: ctx.database_url().to_string(),
        work_dir: Some(work_dir.path().to_path_buf()),
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

    // Connect directly using TLS config to publish events
    let nats_client = conn_config.connect().await?;

    // Wait for ingestd's JetStream stream + consumer to be ready.
    // start_test_ingestd_with_config skipped its readiness check because ctx has
    // no NATS handle (TLS NATS is obtained independently). Without this wait,
    // events published via NATS Core are silently lost (no JetStream stream yet).
    let js = async_nats::jetstream::new(nats_client.clone());
    nats.wait_for_stream(
        &js,
        &ingest_handle.stream_name,
        Duration::from_secs(Timeouts::SHORT),
    )
    .await?;
    nats.wait_for_consumer_on_stream(
        &js,
        &ingest_handle.stream_name,
        Duration::from_secs(Timeouts::SHORT),
    )
    .await?;

    // Publish a test event referencing the pre-registered material
    let event_id = publish_test_event(
        &nats_client,
        material_id,
        "tls-test-source",
        "tls.test.event",
        json!({
            "message": "Hello over TLS",
            "secure": true
        }),
    )
    .await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), Timeouts::STANDARD).await?;

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
