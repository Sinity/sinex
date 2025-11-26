use std::time::Duration;

use async_nats::{jetstream, jetstream::Context, Client, HeaderMap};
use blake3::Hasher;
use chrono::Utc;
use color_eyre::eyre::{eyre, Result};
use gethostname::gethostname;
use serde_json::json;
use sinex_core::{environment::SinexEnvironment, types::ulid::Ulid};
use tokio::time::timeout;
use tokio_stream::StreamExt;

/// Helper that mimics satellite publishing semantics for tests.
#[derive(Clone)]
pub struct TestSatellitePublisher {
    client: Client,
    js: Context,
    env: SinexEnvironment,
    source: String,
}

impl TestSatellitePublisher {
    /// Create a publisher from a raw NATS client and logical source name.
    pub fn new(client: Client, source: impl Into<String>) -> Self {
        let js = jetstream::new(client.clone());
        Self {
            client,
            js,
            env: sinex_core::environment().clone(),
            source: source.into(),
        }
    }

    /// Helper to connect via EphemeralNats with a given source label.
    pub async fn from_ephemeral(nats: &crate::EphemeralNats, source: impl Into<String>) -> Result<Self> {
        let client = nats.connect().await?;
        Ok(Self::new(client, source))
    }

    /// Publish an event to the standard `events.raw.<source>.<event_type>` subject.
    pub async fn publish_event(
        &self,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<Ulid> {
        let event_id = Ulid::new();
        let now = Utc::now().to_rfc3339();
        let host = gethostname().to_string_lossy().to_string();

        let message = json!({
            "id": event_id.to_string(),
            "source": self.source.as_str(),
            "event_type": event_type,
            "ts_orig": now,
            "host": host,
            "payload": payload,
            "ingestor_version": "test-satellite",
            "payload_schema_id": null,
            "associated_blob_ids": null,
            "source_material_id": null,
            "anchor_byte": null,
            "offset_start": null,
            "offset_end": null,
            "offset_kind": null,
            "source_event_ids": null,
        });

        let subject = self.env.nats_subject(&format!(
            "events.raw.{}.{}",
            self.source.replace('.', "_"),
            event_type.replace('.', "_")
        ));

        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id.to_string().as_str());

        self.js
            .publish_with_headers(subject, headers, serde_json::to_vec(&message)?.into())
            .await?
            .await
            .map_err(|err| eyre!("failed to publish event: {err}"))?;

        Ok(event_id)
    }

    /// Publish a synthetic material stream (begin, slices, end) and return the material ULID.
    pub async fn publish_material_stream<I, S>(&self, slices: I) -> Result<Ulid>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let material_id = Ulid::new();
        let material_kind = self.source.clone();
        let begin_payload = json!({
            "material_id": material_id.to_string(),
            "material_kind": material_kind,
            "source_identifier": format!("test://{}", self.source),
            "metadata": json!({"helper": "test_satellite_publisher"}),
            "started_at": Utc::now().to_rfc3339(),
        });

        self.js
            .publish(
                self.env.nats_subject("source_material.begin"),
                serde_json::to_vec(&begin_payload)?.into(),
            )
            .await?
            .await
            .map_err(|err| eyre!("failed to publish material begin: {err}"))?;

        let mut offset: i64 = 0;
        let mut slice_index = 0usize;
        let mut hasher = Hasher::new();

        for slice in slices.into_iter() {
            let data = slice.as_ref();
            let subject = self
                .env
                .nats_subject(&format!("source_material.slices.{}", material_id));

            let mut headers = HeaderMap::new();
            headers.insert(
                "Nats-Msg-Id",
                format!("{}-{}", material_id, slice_index).as_str(),
            );
            headers.insert("Slice-Index", slice_index.to_string().as_str());
            headers.insert("Offset", offset.to_string().as_str());
            headers.insert("Chunk-Hash", blake3::hash(data).to_hex().as_str());

            self.js
                .publish_with_headers(subject, headers, data.to_vec().into())
                .await?
                .await
                .map_err(|err| eyre!("failed to publish slice: {err}"))?;

            hasher.update(data);
            offset += data.len() as i64;
            slice_index += 1;
        }

        let end_payload = json!({
            "material_id": material_id.to_string(),
            "ended_at": Utc::now().to_rfc3339(),
            "content_hash": hasher.finalize().to_hex().to_string(),
            "total_slices": slice_index,
            "total_size_bytes": offset,
        });

        self.js
            .publish(
                self.env.nats_subject("source_material.end"),
                serde_json::to_vec(&end_payload)?.into(),
            )
            .await?
            .await
            .map_err(|err| eyre!("failed to publish material end: {err}"))?;

        Ok(material_id)
    }

    /// Wait for a confirmation message for the given published event.
    pub async fn wait_confirmation(
        &self,
        event_id: &Ulid,
        timeout_duration: Duration,
    ) -> Result<()> {
        let subject = format!(
            "{}.{}",
            self.env.nats_subject("events.confirmations"),
            event_id
        );

        let mut subscription = self
            .client
            .subscribe(subject.clone())
            .await
            .map_err(|err| eyre!("failed to subscribe to confirmations {subject}: {err}"))?;

        let next = timeout(timeout_duration, subscription.next())
            .await
            .map_err(|_| eyre!("timed out waiting for confirmation on {subject}"))?;
        next.ok_or_else(|| eyre!("confirmation stream closed for {subject}"))?;
        Ok(())
    }

    /// Access the underlying JetStream context.
    pub fn jetstream(&self) -> &Context {
        &self.js
    }

    /// Access the raw NATS client (useful for custom subscriptions).
    pub fn client(&self) -> &Client {
        &self.client
    }
}
