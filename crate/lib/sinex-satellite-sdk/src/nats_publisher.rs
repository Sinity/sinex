//! NATS JetStream event publisher

use sinex_core::{db::models::Event, environment::SinexEnvironment, OffsetKind, Provenance};

#[derive(Debug, Clone)]
pub struct NatsPublisher {
    nats_client: async_nats::Client,
    env: SinexEnvironment,
}

impl NatsPublisher {
    pub fn new(nats_client: async_nats::Client) -> Self {
        let env = sinex_core::environment().clone();
        Self { nats_client, env }
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

        let event_id = event.id.as_ref().ok_or("Event ID is required")?;
        let ts_orig = event.ts_orig.ok_or("Event ts_orig is required")?;

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

        let payload_schema_id = event.payload_schema_id.map(|id| id.to_string());
        let associated_blob_ids = event
            .associated_blob_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| id.to_string()).collect::<Vec<_>>());

        let payload = serde_json::json!({
            "id": event_id.to_string(),
            "source": event.source.as_str(),
            "event_type": event.event_type.as_str(),
            "ts_orig": ts_orig.to_rfc3339(),
            "host": event.host.as_str(),
            "payload": event.payload,
            "ingestor_version": event.ingestor_version,
            "payload_schema_id": payload_schema_id,
            "associated_blob_ids": associated_blob_ids,
            "source_material_id": source_material_id,
            "anchor_byte": anchor_byte,
            "offset_start": offset_start,
            "offset_end": offset_end,
            "offset_kind": offset_kind,
            "source_event_ids": source_event_ids,
        });

        let subject = self.env.nats_subject(&format!(
            "events.raw.{}.{}",
            event.source.as_str().replace('.', "_"),
            event.event_type.as_str().replace('.', "_")
        ));

        // Add idempotency header
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id.to_string().as_str());

        // Publish to JetStream and wait for acknowledgment
        // First await: send the publish request
        // Second await: wait for PublishAck confirmation from JetStream
        let ack = js
            .publish_with_headers(subject, headers, serde_json::to_vec(&payload)?.into())
            .await?
            .await?;

        tracing::debug!(
            event_id = %event_id,
            sequence = ack.sequence,
            stream = %ack.stream,
            "Event published to JetStream"
        );

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
