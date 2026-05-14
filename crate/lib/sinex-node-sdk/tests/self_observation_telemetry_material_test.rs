//! Integration tests for the self-observation telemetry-material fix.
//!
//! These tests verify that calling `SelfObserver::prime()` before emitting
//! `metric.gauge` events ensures the source material is registered in
//! `raw.source_material_registry` before the event arrives at ingestd,
//! preventing the "Source material not registered" DLQ failure (issue #1241
//! prong 2).

#![cfg(feature = "messaging")]

use sinex_node_sdk::{
    AcquisitionManager, SelfObserver, SelfObserverConfig, SOURCE_MATERIAL_STREAM,
};
use sinex_primitives::environment::environment;
use std::sync::Arc;
use std::time::Duration;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{DEFAULT_WAIT_SECS, Timeouts, WaitHelpers};
use xtask::sandbox::{EphemeralNats, TestIngestdConfig, start_test_ingestd_with_config};

async fn wait_for_material_assembler_ready(
    nats: &Arc<EphemeralNats>,
    nats_client: &async_nats::Client,
    namespace: &str,
) -> Result<()> {
    let env = environment();
    let js_check = nats.jetstream_with_client(nats_client.clone());
    let stream = env.nats_stream_name_with_namespace(Some(namespace), SOURCE_MATERIAL_STREAM);
    nats.wait_for_consumer_on_stream(&js_check, &stream, Duration::from_secs(Timeouts::STANDARD))
        .await?;
    Ok(())
}

/// Core AC for #1241 prong 2: after `prime()`, a `metric.gauge` event emitted
/// by `SelfObserver` must land in `core.events` instead of the DLQ.
///
/// Without the fix, the source-material BEGIN frame arrives at ingestd at the
/// same time as the event (or later), causing a "Source material not
/// registered" rejection and DLQ routing.
///
/// With the fix, `prime()` synchronously commits the BEGIN frame to JetStream
/// before any events are published, so ingestd's `MaterialReadySet` already
/// has the material registered when the event arrives.
#[sinex_test]
async fn telemetry_material_registered_before_gauge_event(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let pool = ctx.pool().clone();

    let ingest_config = TestIngestdConfig {
        nats: nats.connection_config(),
        database_url: ctx.database_url().to_string(),
        namespace: Some(namespace.clone()),
        ..Default::default()
    };
    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

    // Bootstrap SOURCE_MATERIAL stream and wait for the material assembler
    // consumer to be ready before sending any material frames.
    AcquisitionManager::bootstrap_streams_with_namespace(&nats_client, Some(&namespace)).await?;
    wait_for_material_assembler_ready(&nats, &nats_client, &namespace).await?;

    let observer = SelfObserver::new(
        nats_client,
        SelfObserverConfig {
            component: "test-telemetry-material".to_string(),
            namespace: Some(namespace.clone()),
            enabled: true,
            // No rate-limit so the gauge fires immediately.
            min_emission_interval: Duration::ZERO,
        },
    );

    // prime() must commit the source-material BEGIN frame to JetStream and
    // return before any event referencing that material_id is published.
    // This is the structural fix for the DLQ race.
    observer.prime().await.map_err(|e| {
        color_eyre::eyre::eyre!("SelfObserver::prime() failed: {e}")
    })?;

    // Now emit a metric.gauge — this event carries the same source_material_id
    // whose BEGIN frame was committed by prime().
    observer
        .emit_gauge("derived.event_lag_ms", 42.0, None)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("emit_gauge failed: {e}"))?;

    // The event must land in core.events. If the material wasn't registered,
    // ingestd would NAK the event 10 times and then route it to the DLQ.
    // We assert count > 0 in core.events and 0 in the DLQ.
    WaitHelpers::wait_for_condition(
        || {
            let pool = pool.clone();
            async move {
                let count: Option<i64> = sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*)
                    FROM core.events
                    WHERE source = 'sinex'
                      AND event_type = 'metric.gauge'
                    "#
                )
                .fetch_one(&pool)
                .await
                .map_err(|e| sinex_primitives::error::SinexError::database(e.to_string()))?;
                Ok::<bool, sinex_primitives::error::SinexError>(count.unwrap_or(0) >= 1)
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await
    .map_err(|e| {
        color_eyre::eyre::eyre!(
            "metric.gauge event did not appear in core.events within the wait window \
             (likely DLQ'd due to unregistered source material): {e}"
        )
    })?;

    // Verify the DLQ did not accumulate any metric.gauge events. The DLQ stream
    // may not exist at all if nothing was ever DLQ'd — treat a missing stream
    // as 0 messages (same semantics as an empty stream).
    let js = nats.jetstream_with_client(ctx.nats_client());
    let dlq_stream_name = environment()
        .nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS_DLQ");
    let dlq_messages = js
        .get_stream(&dlq_stream_name)
        .await
        .map(|stream| stream.cached_info().state.messages)
        .unwrap_or(0);

    assert_eq!(
        dlq_messages, 0,
        "metric.gauge events must not accumulate in the DLQ after prime() (issue #1241 prong 2)"
    );

    ingest_handle.stop().await?;
    Ok(())
}

/// Regression guard: `prime()` on a disabled `SelfObserver` must not error.
#[sinex_test]
async fn telemetry_material_prime_is_noop_when_disabled() -> TestResult<()> {
    let observer = SelfObserver::disabled();
    observer.prime().await.map_err(|e| {
        color_eyre::eyre::eyre!("prime() on disabled SelfObserver should be a no-op: {e}")
    })?;
    Ok(())
}
