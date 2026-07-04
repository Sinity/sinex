//! Shared `JetStream` stream bootstrap helpers for runtime producers.

use crate::runtime::{RuntimeResult, SinexError};
use async_nats::{Client as NatsClient, jetstream};
use futures::StreamExt;
use sinex_primitives::{
    constants::env_vars,
    environment::{SinexEnvironment, environment},
    nats::JetStreamTopology,
};
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

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
    recreate_raw_stream_for_workqueue_if_safe(js, topology).await?;

    js.create_or_update_stream(raw_events_stream_config(topology))
        .await
        .map_err(|e| SinexError::network("Failed to create events stream").with_source(e))?;

    Ok(())
}

fn raw_events_stream_config(topology: &JetStreamTopology) -> jetstream::stream::Config {
    jetstream::stream::Config {
        name: topology.events_stream.to_string(),
        subjects: vec![topology.events_subject.to_string()],
        retention: jetstream::stream::RetentionPolicy::WorkQueue,
        max_messages: 2_000_000,
        max_bytes: JETSTREAM_BOOTSTRAP_MAX_BYTES,
        max_age: Duration::from_secs(72 * 60 * 60),
        storage: jetstream::stream::StorageType::File,
        discard: jetstream::stream::DiscardPolicy::Old,
        ..Default::default()
    }
}

async fn recreate_raw_stream_for_workqueue_if_safe(
    js: &jetstream::Context,
    topology: &JetStreamTopology,
) -> RuntimeResult<()> {
    let mut stream = match js.get_stream(&topology.events_stream).await {
        Ok(stream) => stream,
        Err(_) => return Ok(()),
    };
    let info = stream.info().await.cloned().map_err(|e| {
        SinexError::network(format!(
            "Failed to inspect raw events stream {} before WorkQueue migration",
            topology.events_stream
        ))
        .with_source(e)
    })?;

    if matches!(
        info.config.retention,
        jetstream::stream::RetentionPolicy::WorkQueue
    ) {
        return Ok(());
    }

    let consumers = raw_stream_consumer_states(&stream).await?;
    match raw_stream_workqueue_recreation_decision(
        info.state.messages,
        info.state.last_sequence,
        &topology.consumer_durable,
        &consumers,
    ) {
        RawStreamWorkQueueRecreationDecision::Recreate => {
            warn!(
                stream = %topology.events_stream,
                messages = info.state.messages,
                bytes = info.state.bytes,
                last_sequence = info.state.last_sequence,
                consumer = %topology.consumer_durable,
                "Recreating drained raw events stream with WorkQueue retention"
            );
            js.delete_stream(&topology.events_stream).await.map_err(|e| {
                SinexError::network(format!(
                    "Failed to delete drained raw events stream {} for WorkQueue migration",
                    topology.events_stream
                ))
                .with_source(e)
            })?;
            Ok(())
        }
        RawStreamWorkQueueRecreationDecision::AlreadyWorkQueueOrEmpty => Ok(()),
        RawStreamWorkQueueRecreationDecision::Reject { reason } => Err(SinexError::processing(
            format!(
                "Raw events stream {} cannot be recreated for WorkQueue retention: {reason}",
                topology.events_stream
            ),
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawStreamConsumerState {
    name: String,
    pending: u64,
    ack_pending: usize,
    redelivered: usize,
    ack_floor_sequence: u64,
}

async fn raw_stream_consumer_states(
    stream: &jetstream::stream::Stream,
) -> RuntimeResult<Vec<RawStreamConsumerState>> {
    let mut consumer_list = stream.consumers();
    let mut consumers = Vec::new();
    while let Some(result) = consumer_list.next().await {
        let info = result.map_err(|err| {
            SinexError::processing(format!(
                "Failed to list raw stream consumers before WorkQueue migration: {err}"
            ))
        })?;
        consumers.push(RawStreamConsumerState {
            name: info.name,
            pending: info.num_pending,
            ack_pending: info.num_ack_pending,
            redelivered: info.num_redelivered,
            ack_floor_sequence: info.ack_floor.stream_sequence,
        });
    }
    consumers.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(consumers)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RawStreamWorkQueueRecreationDecision {
    AlreadyWorkQueueOrEmpty,
    Recreate,
    Reject { reason: String },
}

fn raw_stream_workqueue_recreation_decision(
    stream_messages: u64,
    stream_last_sequence: u64,
    expected_consumer_name: &str,
    consumers: &[RawStreamConsumerState],
) -> RawStreamWorkQueueRecreationDecision {
    if stream_messages == 0 {
        return RawStreamWorkQueueRecreationDecision::AlreadyWorkQueueOrEmpty;
    }

    let unexpected = consumers
        .iter()
        .filter(|consumer| consumer.name != expected_consumer_name)
        .map(|consumer| consumer.name.as_str())
        .collect::<Vec<_>>();
    if !unexpected.is_empty() {
        return RawStreamWorkQueueRecreationDecision::Reject {
            reason: format!(
                "unexpected raw consumer(s) still exist: {}",
                unexpected.join(", ")
            ),
        };
    }

    let Some(consumer) = consumers
        .iter()
        .find(|consumer| consumer.name == expected_consumer_name)
    else {
        return RawStreamWorkQueueRecreationDecision::Reject {
            reason: format!(
                "expected raw consumer {expected_consumer_name} is missing while {stream_messages} message(s) remain"
            ),
        };
    };

    if consumer.pending == 0
        && consumer.ack_pending == 0
        && consumer.redelivered == 0
        && consumer.ack_floor_sequence >= stream_last_sequence
    {
        RawStreamWorkQueueRecreationDecision::Recreate
    } else {
        RawStreamWorkQueueRecreationDecision::Reject {
            reason: format!(
                "consumer {} is not fully drained: pending={}, ack_pending={}, redelivered={}, ack_floor={}, stream_last={}",
                consumer.name,
                consumer.pending,
                consumer.ack_pending,
                consumer.redelivered,
                consumer.ack_floor_sequence,
                stream_last_sequence
            ),
        }
    }
}

#[cfg(test)]
#[path = "jetstream_streams_test.rs"]
mod tests;
