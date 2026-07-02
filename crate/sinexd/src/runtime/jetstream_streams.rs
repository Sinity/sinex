//! Shared `JetStream` stream bootstrap helpers for runtime producers.

use crate::runtime::{RuntimeResult, SinexError};
use async_nats::{Client as NatsClient, jetstream};
use sinex_primitives::{
    constants::env_vars,
    environment::{SinexEnvironment, environment},
    nats::JetStreamTopology,
};
use std::time::Duration;
use tokio::time::sleep;

// Keep runtime-created stream caps aligned with the Nix bootstrap path. The current
// nats CLI rejects --max-bytes values above signed 32-bit range.
pub const JETSTREAM_BOOTSTRAP_MAX_BYTES: i64 = 2_147_483_647;

/// Ensure the raw-events stream exists for source and automaton producers.
pub async fn bootstrap_raw_events_stream(
    nats_client: &NatsClient,
    namespace: Option<&str>,
) -> RuntimeResult<()> {
    let env = environment().clone();
    let js = jetstream::new(nats_client.clone());

    let mut attempt = 0;
    loop {
        match ensure_raw_events_stream_once(&js, &env, namespace).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                attempt += 1;
                if attempt >= 5 {
                    return Err(err);
                }
                sleep(Duration::from_millis(100 * attempt as u64)).await;
            }
        }
    }
}

/// Create or converge only the raw-events stream from the canonical topology.
pub async fn ensure_raw_events_stream_once(
    js: &jetstream::Context,
    env: &SinexEnvironment,
    namespace: Option<&str>,
) -> RuntimeResult<()> {
    // When SINEX_NATS_STREAMS_MANAGED_EXTERNALLY=true, the NixOS module owns
    // stream configuration. Producers should not rewrite externally-managed
    // topology; a missing stream remains a deployment error in that mode.
    if std::env::var(env_vars::NATS_STREAMS_MANAGED_EXTERNALLY).as_deref() == Ok("true") {
        return Ok(());
    }

    let base_stream = env.nats_stream_name_with_namespace(namespace, "SINEX_RAW_EVENTS");
    let topology = JetStreamTopology::new(env, base_stream, "event-engine".to_string(), namespace);
    ensure_raw_events_stream_for_topology(js, &topology).await
}

/// Create or converge the raw-events stream for an already-resolved topology.
pub async fn ensure_raw_events_stream_for_topology(
    js: &jetstream::Context,
    topology: &JetStreamTopology,
) -> RuntimeResult<()> {
    js.create_or_update_stream(jetstream::stream::Config {
        name: topology.events_stream.to_string(),
        subjects: vec![topology.events_subject.to_string()],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 2_000_000,
        max_bytes: JETSTREAM_BOOTSTRAP_MAX_BYTES,
        max_age: Duration::from_secs(72 * 60 * 60),
        storage: jetstream::stream::StorageType::File,
        discard: jetstream::stream::DiscardPolicy::Old,
        ..Default::default()
    })
    .await
    .map_err(|e| SinexError::network("Failed to create events stream").with_source(e))?;

    Ok(())
}
