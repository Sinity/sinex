//! NATS JetStream event publisher

use serde::Serialize;
use sinex_core::{db::models::Event, environment::SinexEnvironment, OffsetKind, Provenance};
use std::{future::IntoFuture, io, time::Duration};

const DEFAULT_PUBLISH_ACK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct NatsPublisher {
    nats_client: async_nats::Client,
    env: SinexEnvironment,
    namespace: Option<String>,
}

#[derive(Serialize)]
struct PublishEvent<'a> {
    id: String,
    source: &'a str,
    event_type: &'a str,
    ts_orig: String,
    host: &'a str,
    payload: &'a sinex_core::JsonValue,
    ingestor_version: Option<String>,
    payload_schema_id: Option<String>,
    associated_blob_ids: Option<Vec<String>>,
    source_material_id: Option<String>,
    anchor_byte: Option<i64>,
    offset_start: Option<i64>,
    offset_end: Option<i64>,
    offset_kind: Option<String>,
    source_event_ids: Option<Vec<String>>,
}

impl NatsPublisher {
    pub fn new(nats_client: async_nats::Client) -> Self {
        Self::with_namespace(nats_client, None)
    }

    pub fn with_namespace(nats_client: async_nats::Client, namespace: Option<String>) -> Self {
        let env = sinex_core::environment().clone();
        Self {
            nats_client,
            env,
            namespace,
        }
    }

    /// Get the underlying NATS client
    pub fn nats_client(&self) -> &async_nats::Client {
        &self.nats_client
    }

    pub async fn publish(
        &self,
        event: &Event,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let js = async_nats::jetstream::new(self.nats_client.clone());

        let (
            source_material_id,
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind,
            source_event_ids,
        ) = match &event.provenance {
            Provenance::Material {
                id,
                anchor_byte,
                offset_start,
                offset_end,
                offset_kind,
            } => (
                Some(id.to_string()),
                Some(*anchor_byte),
                *offset_start,
                *offset_end,
                Some(offset_kind_label(*offset_kind).to_string()),
                None,
            ),
            Provenance::Synthesis {
                source_event_ids, ..
            } => (
                None,
                None,
                None,
                None,
                None,
                Some(
                    source_event_ids
                        .iter()
                        .map(|id| id.as_ulid().to_string())
                        .collect::<Vec<_>>(),
                ),
            ),
        };

        let (event_id_str, payload) = build_publish_payload(
            event,
            source_material_id,
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind,
            source_event_ids,
        )?;

        let subject = self.env.nats_subject_with_namespace(
            self.namespace.as_deref(),
            &format!(
                "events.raw.{}.{}",
                event.source.as_str().replace('.', "_"),
                event.event_type.as_str().replace('.', "_")
            ),
        );

        // Add idempotency header
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id_str.as_str());

        // Publish to JetStream, then wait for acknowledgment (bounded by timeout).
        let ack_future = js
            .publish_with_headers(subject, headers, payload.into())
            .await?;
        let ack = wait_for_publish_ack(ack_future, DEFAULT_PUBLISH_ACK_TIMEOUT).await?;

        tracing::debug!(
            event_id = %event_id_str,
            sequence = ack.sequence,
            stream = %ack.stream,
            "Event published to JetStream"
        );

        Ok(())
    }
}

fn build_publish_payload(
    event: &Event,
    source_material_id: Option<String>,
    anchor_byte: Option<i64>,
    offset_start: Option<i64>,
    offset_end: Option<i64>,
    offset_kind: Option<String>,
    source_event_ids: Option<Vec<String>>,
) -> Result<(String, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
    let event_id = event.id.as_ref().ok_or("Event ID is required")?;
    let ts_orig = event.ts_orig.ok_or("Event ts_orig is required")?;
    let event_id_str = event_id.to_string();

    let payload_schema_id = event.payload_schema_id.map(|id| id.to_string());
    let associated_blob_ids = event
        .associated_blob_ids
        .as_ref()
        .map(|ids| ids.iter().map(|id| id.to_string()).collect::<Vec<_>>());

    let payload = PublishEvent {
        id: event_id_str.clone(),
        source: event.source.as_str(),
        event_type: event.event_type.as_str(),
        ts_orig: ts_orig.to_rfc3339(),
        host: event.host.as_str(),
        payload: &event.payload,
        ingestor_version: event.ingestor_version.clone(),
        payload_schema_id,
        associated_blob_ids,
        source_material_id,
        anchor_byte,
        offset_start,
        offset_end,
        offset_kind,
        source_event_ids,
    };

    Ok((event_id_str, serde_json::to_vec(&payload)?))
}

async fn wait_for_publish_ack<T, E, F>(
    future: F,
    timeout: Duration,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    F: IntoFuture<Output = Result<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    match tokio::time::timeout(timeout, future.into_future()).await {
        Ok(result) => result.map_err(|err| Box::new(err) as _),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("Timed out waiting for JetStream publish ack after {timeout:?}"),
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::{build_publish_payload, wait_for_publish_ack};
    use sinex_core::{Event, EventId, Provenance, Ulid};
    use sinex_test_utils::{sinex_test, TestResult};
    use std::{future, io, time::Duration};

    #[sinex_test]
    async fn publish_ack_timeout_is_reported() -> TestResult<()> {
        let result =
            wait_for_publish_ack::<(), io::Error, _>(future::pending(), Duration::from_millis(10))
                .await;
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn publish_payload_serializes_json_once() -> TestResult<()> {
        let mut event = Event::create(
            "publisher.test",
            "payload.check",
            serde_json::json!({"nested": {"a": 1}}),
            Provenance::from_synthesis_safe(EventId::from_ulid(Ulid::new()), Vec::new()),
        );
        event.id = Some(sinex_core::Id::from_ulid(Ulid::new()));

        let (event_id, payload) = build_publish_payload(&event, None, None, None, None, None, None)
            .map_err(|err| color_eyre::eyre::eyre!(err))?;
        let value: serde_json::Value = serde_json::from_slice(&payload)?;

        assert_eq!(value["id"], event_id);
        assert!(value["payload"].is_object());
        assert_eq!(value["payload"]["nested"]["a"], 1);
        Ok(())
    }
}

fn offset_kind_label(kind: OffsetKind) -> &'static str {
    match kind {
        OffsetKind::Byte => "byte",
        OffsetKind::Line => "line",
        OffsetKind::Record => "rowid",
        OffsetKind::Character => "logical",
    }
}
