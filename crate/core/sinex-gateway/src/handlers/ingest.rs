//! Event ingest handler
//!
//! Provides the `events.ingest` RPC endpoint, which accepts a raw event and
//! publishes it directly to the NATS JetStream raw event stream. This is the
//! thin gateway entry-point for clients that don't run a full node SDK.

use crate::service_container::ServiceContainer;
use color_eyre::eyre::{Result, WrapErr};
use serde_json::{Value, Value as JsonValue, json};
use sinex_db::{DbPoolExt, SourceMaterialRecord};
use sinex_db::repositories::source_materials::{SourceMaterial, TemporalLedgerEntry};
use sinex_primitives::{
    Id, Uuid,
    domain::{EventSource, EventType, HostName},
    environment::SinexEnvironment,
    events::{Event, SourceMaterial as EventSourceMaterial, builder::Provenance},
    rpc::ingest::{EventIngestRequest, EventIngestResponse},
    temporal,
};
use std::time::Duration;
use tracing::warn;

const PUBLISH_ACK_TIMEOUT: Duration = Duration::from_secs(10);

async fn publish_event_envelope(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    source: &str,
    event_type: &str,
    event_id: Uuid,
    envelope: Event<JsonValue>,
) -> Result<u64> {
    let payload_bytes =
        serde_json::to_vec(&envelope).wrap_err("failed to serialize event envelope")?;

    let subject = env.nats_subject_with_namespace(
        None,
        &format!(
            "events.raw.{}.{}",
            source.replace('.', "_"),
            event_type.replace('.', "_")
        ),
    );

    let js = async_nats::jetstream::new(nats_client.clone());
    let mut headers = async_nats::HeaderMap::new();
    let event_id_header = event_id.to_string();
    headers.insert("Nats-Msg-Id", event_id_header.as_str());

    let ack_future = js
        .publish_with_headers(subject, headers, payload_bytes.into())
        .await
        .wrap_err("failed to publish event to JetStream")?;

    let ack = tokio::time::timeout(PUBLISH_ACK_TIMEOUT, ack_future)
        .await
        .wrap_err("timed out waiting for JetStream publish ack")?
        .wrap_err("JetStream publish ack returned an error")?;

    Ok(ack.sequence)
}

/// Handle `events.ingest`
///
/// Validates the request, registers a backing source-material row so the
/// published event satisfies provenance/FK invariants, then publishes the
/// full event envelope to JetStream on `events.raw.<source>.<event_type>`.
pub async fn handle_events_ingest(services: &ServiceContainer, params: Value) -> Result<Value> {
    let req: EventIngestRequest =
        serde_json::from_value(params).wrap_err("failed to parse events.ingest request")?;

    if req.source.trim().is_empty() {
        color_eyre::eyre::bail!("`source` must not be empty");
    }
    if req.event_type.trim().is_empty() {
        color_eyre::eyre::bail!("`event_type` must not be empty");
    }
    if req.host.as_deref().is_some_and(|host| host.trim().is_empty()) {
        color_eyre::eyre::bail!("`host` must not be empty when provided");
    }

    let event_id = Uuid::now_v7();
    let material_id = Uuid::now_v7();
    let ts_orig = req
        .ts_orig
        .as_deref()
        .map(temporal::parse_rfc3339)
        .transpose()
        .wrap_err("invalid `ts_orig`; expected RFC3339 timestamp")?
        .unwrap_or_else(temporal::now);
    let gateway_host = sinex_primitives::events::builder::get_hostname().to_string();
    let event_host = req.host.unwrap_or_else(|| gateway_host.clone());
    let payload_size_bytes = serde_json::to_vec(&req.payload)
        .wrap_err("failed to measure request payload size")?
        .len() as i64;
    let source = EventSource::new(req.source).wrap_err("invalid `source` value")?;
    let event_type = EventType::new(req.event_type).wrap_err("invalid `event_type` value")?;
    let event_host = HostName::new(event_host);
    let payload = req.payload;

    let material = SourceMaterial::stream(format!("gateway://events.ingest/{event_id}"))
        .with_metadata(json!({
            "gateway_surface": "events.ingest",
            "event_source": source.as_str(),
            "event_type": event_type.as_str(),
            "payload_bytes": payload_size_bytes,
            "inline_payload": true,
        }))
        .with_start_time(ts_orig)
        .with_end_time(ts_orig)
        .with_staged_by("sinex-gateway")
        .with_staged_on_host(gateway_host);

    let material_record = services
        .pool()
        .source_materials()
        .register_external_material(material_id, material)
        .await
        .wrap_err("failed to register inline source material for events.ingest")?;
    services
        .pool()
        .source_materials()
        .append_temporal_ledger(TemporalLedgerEntry::staged_at(
            material_id,
            payload_size_bytes,
            ts_orig,
        ))
        .await
        .wrap_err("failed to append temporal ledger for events.ingest material")?;

    let mut envelope = Event::new_json(
        source.as_str(),
        event_type.as_str(),
        payload,
        Provenance::from_material(Id::<EventSourceMaterial>::from_uuid(material_id), 0, None, None),
    )
    .with_timestamp(ts_orig)
    .with_host(event_host);
    envelope.id = Some(Id::from_uuid(event_id));

    let publish_result = publish_event_envelope(
        services
            .nats_client()
            .ok_or_else(|| color_eyre::eyre::eyre!("NATS client is not available"))?,
        services.environment(),
        source.as_str(),
        event_type.as_str(),
        event_id,
        envelope,
    )
    .await;

    let sequence = match publish_result {
        Ok(sequence) => sequence,
        Err(err) => {
            if let Err(mark_err) = services
                .pool()
                .source_materials()
                .mark_as_failed(
                    Id::<SourceMaterialRecord>::from_uuid(material_record.id),
                    &err.to_string(),
                )
                .await
            {
                warn!(
                    material_id = %material_record.id,
                    error = %mark_err,
                    "Failed to mark gateway-ingested source material as failed after publish error"
                );
            }
            return Err(err);
        }
    };

    Ok(serde_json::to_value(EventIngestResponse {
        event_id: event_id.to_string(),
        sequence,
    })?)
}
