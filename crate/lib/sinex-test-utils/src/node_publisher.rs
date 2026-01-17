use std::time::Duration;

use async_nats::{jetstream, jetstream::Context, Client, HeaderMap};
use blake3::Hasher;
use chrono::Utc;
use color_eyre::eyre::{eyre, Result};
use gethostname::gethostname;
use serde_json::json;
use sinex_core::{environment::SinexEnvironment, types::ulid::Ulid};
use sinex_node_sdk::acquisition_manager::{AcquisitionManager, RotationPolicy};
use tokio::time::timeout;
use tokio_stream::StreamExt;

/// Helper that mimics node publishing semantics for tests.
#[derive(Clone)]
pub struct TestNodePublisher {
    client: Client,
    js: Context,
    env: SinexEnvironment,
    source: String,
    namespace: Option<String>,
}

/// Backward compatibility alias for TestNodePublisher.
#[deprecated(since = "0.5.0", note = "Use TestNodePublisher instead")]
pub type TestSatellitePublisher = TestNodePublisher;

#[derive(Clone, Debug, Default)]
pub struct EventOverrides {
    pub id: Option<Ulid>,
    pub ts_orig: Option<String>,
    pub host: Option<String>,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub source_event_ids: Option<Vec<Ulid>>,
    pub source_material_id: Option<Ulid>,
    pub anchor_byte: Option<i64>,
    pub offset_start: Option<i64>,
    pub offset_end: Option<i64>,
    pub offset_kind: Option<String>,
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

impl TestNodePublisher {
    /// Create a publisher from a raw NATS client and logical source name.
    pub fn new(client: Client, source: impl Into<String>) -> Self {
        Self::with_namespace(client, source, None)
    }

    /// Create a publisher with an explicit subject namespace.
    pub fn with_namespace(
        client: Client,
        source: impl Into<String>,
        namespace: Option<String>,
    ) -> Self {
        let js = jetstream::new(client.clone());
        Self {
            client,
            js,
            env: sinex_core::environment().clone(),
            source: source.into(),
            namespace,
        }
    }

    /// Helper to connect via EphemeralNats with a given source label.
    pub async fn from_ephemeral(
        nats: &crate::EphemeralNats,
        source: impl Into<String>,
    ) -> Result<Self> {
        let client = nats.connect().await?;
        Ok(Self::with_namespace(client, source, None))
    }

    /// Publish an event to the standard `events.raw.<source>.<event_type>` subject.
    pub async fn publish_event(
        &self,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<Ulid> {
        self.publish_event_with_overrides(event_type, payload, EventOverrides::default())
            .await
    }

    /// Publish an event with optional envelope overrides.
    pub async fn publish_event_with_overrides(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        overrides: EventOverrides,
    ) -> Result<Ulid> {
        let event_id = overrides.id.unwrap_or_else(Ulid::new);
        let now = overrides.ts_orig.unwrap_or_else(|| Utc::now().to_rfc3339());
        let host = overrides
            .host
            .unwrap_or_else(|| gethostname().to_string_lossy().to_string());

        let message = json!({
            "id": event_id.to_string(),
            "source": self.source.as_str(),
            "event_type": event_type,
            "ts_orig": now,
            "host": host,
            "payload": payload,
            "ingestor_version": overrides
                .ingestor_version
                .unwrap_or_else(|| "test-node".to_string()),
            "payload_schema_id": overrides
                .payload_schema_id
                .map(|id| id.to_string()),
            "associated_blob_ids": overrides
                .associated_blob_ids
                .map(|ids| ids.into_iter().map(|id| id.to_string()).collect::<Vec<_>>()),
            "source_material_id": overrides
                .source_material_id
                .map(|id| id.to_string()),
            "anchor_byte": overrides.anchor_byte,
            "offset_start": overrides.offset_start,
            "offset_end": overrides.offset_end,
            "offset_kind": overrides.offset_kind,
            "source_event_ids": overrides
                .source_event_ids
                .map(|ids| ids.into_iter().map(|id| id.to_string()).collect::<Vec<_>>()),
        });

        let subject = self.event_subject(event_type);

        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id.to_string().as_str());

        self.js
            .publish_with_headers(subject, headers, serde_json::to_vec(&message)?.into())
            .await?
            .await
            .map_err(|err| eyre!("failed to publish event: {err}"))?;

        Ok(event_id)
    }

    /// Publish raw bytes on the standard subject with a stable message id.
    pub async fn publish_raw_event_bytes(
        &self,
        event_type: &str,
        raw_payload: impl AsRef<[u8]>,
        event_id: Option<Ulid>,
    ) -> Result<Ulid> {
        let event_id = event_id.unwrap_or_else(Ulid::new);
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id.to_string().as_str());

        self.js
            .publish_with_headers(
                self.event_subject(event_type),
                headers,
                raw_payload.as_ref().to_vec().into(),
            )
            .await?
            .await
            .map_err(|err| eyre!("failed to publish raw event bytes: {err}"))?;

        Ok(event_id)
    }

    /// Publish a synthetic material stream (begin, slices, end) and return the material ULID.
    pub async fn publish_material_stream<I, S>(&self, slices: I) -> Result<Ulid>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let material_id = Ulid::new();
        self.publish_material_stream_with_id(material_id, slices)
            .await
    }

    /// Publish a material stream via the AcquisitionManager (Stage-as-You-Go path).
    pub async fn publish_material_stream_via_acquisition_manager<I, S>(
        &self,
        slices: I,
    ) -> Result<Ulid>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let source_identifier = format!("test://{}", self.source);
        let manager = AcquisitionManager::new_with_namespace(
            self.client.clone(),
            RotationPolicy::default(),
            self.source.clone(),
            source_identifier.clone(),
            self.namespace.clone(),
        );

        let mut handle = manager.begin_material(&source_identifier).await?;
        for slice in slices.into_iter() {
            manager.append_slice(&mut handle, slice.as_ref()).await?;
        }
        let material_id = handle.material_id;
        manager.finalize(handle, "test").await?;
        Ok(material_id)
    }

    /// Publish a synthetic material stream with a fixed material ULID.
    pub async fn publish_material_stream_with_id<I, S>(
        &self,
        material_id: Ulid,
        slices: I,
    ) -> Result<Ulid>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let material_kind = self.source.clone();
        let begin_payload = json!({
            "material_id": material_id.to_string(),
            "material_kind": material_kind,
            "source_identifier": format!("test://{}", self.source),
            "metadata": json!({"helper": "test_node_publisher"}),
            "started_at": Utc::now().to_rfc3339(),
        });

        self.js
            .publish(
                self.namespaced_subject("source_material.begin"),
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
            let subject =
                self.namespaced_subject(&format!("source_material.slices.{}", material_id));

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
                self.namespaced_subject("source_material.end"),
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
            self.namespaced_subject("events.confirmations"),
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

    fn event_subject(&self, event_type: &str) -> String {
        self.namespaced_subject(&format!(
            "events.raw.{}.{}",
            self.source.replace('.', "_"),
            event_type.replace('.', "_")
        ))
    }

    fn namespaced_subject(&self, base: &str) -> String {
        self.env
            .nats_subject_with_namespace(self.namespace.as_deref(), base)
    }
}
