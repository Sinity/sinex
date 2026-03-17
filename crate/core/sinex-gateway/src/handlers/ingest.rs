//! Event ingest handler
//!
//! Provides the `events.ingest` RPC endpoint, which accepts a raw event and
//! publishes it directly to the NATS JetStream raw event stream.  This is the
//! thin gateway entry-point for clients that don't run a full node SDK.

use color_eyre::eyre::{Result, WrapErr};
use serde_json::{Value, json};
use sinex_primitives::{
    Uuid,
    environment::SinexEnvironment,
    rpc::ingest::{EventIngestRequest, EventIngestResponse},
    temporal,
};
use std::time::Duration;

const PUBLISH_ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// Handle `events.ingest`
///
/// Validates that `source` and `event_type` are non-empty, assigns a fresh
/// UUIDv7 event ID, then publishes the envelope to JetStream on the subject
/// `events.raw.<source>.<event_type>` (dots replaced with underscores per the
/// NATS subject convention).  Returns the assigned event ID and JetStream
/// sequence number.
pub async fn handle_events_ingest(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    let req: EventIngestRequest =
        serde_json::from_value(params).wrap_err("failed to parse events.ingest request")?;

    // Basic validation — source and event_type must be non-empty
    if req.source.trim().is_empty() {
        color_eyre::eyre::bail!("`source` must not be empty");
    }
    if req.event_type.trim().is_empty() {
        color_eyre::eyre::bail!("`event_type` must not be empty");
    }

    let event_id = Uuid::now_v7();
    let ts_orig = req
        .ts_orig
        .unwrap_or_else(|| temporal::format_rfc3339(temporal::now()));
    let host = req
        .host
        .unwrap_or_else(|| sinex_primitives::events::builder::get_hostname().to_string());

    // Assemble the envelope that ingestd expects on the raw event stream
    let envelope = json!({
        "id": event_id.to_string(),
        "source": req.source,
        "event_type": req.event_type,
        "ts_orig": ts_orig,
        "host": host,
        "payload": req.payload,
    });

    let payload_bytes =
        serde_json::to_vec(&envelope).wrap_err("failed to serialize event envelope")?;

    // Subject: events.raw.<source>.<event_type>  (dots → underscores)
    let subject = env.nats_subject_with_namespace(
        None,
        &format!(
            "events.raw.{}.{}",
            req.source.replace('.', "_"),
            req.event_type.replace('.', "_")
        ),
    );

    let js = async_nats::jetstream::new(nats_client.clone());

    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", event_id.to_string().as_str());

    let ack_future = js
        .publish_with_headers(subject, headers, payload_bytes.into())
        .await
        .wrap_err("failed to publish event to JetStream")?;

    let ack = tokio::time::timeout(PUBLISH_ACK_TIMEOUT, ack_future)
        .await
        .wrap_err("timed out waiting for JetStream publish ack")?
        .wrap_err("JetStream publish ack returned an error")?;

    let resp = EventIngestResponse {
        event_id: event_id.to_string(),
        sequence: ack.sequence,
    };

    Ok(serde_json::to_value(resp)?)
}
