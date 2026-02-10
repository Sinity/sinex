use crate::sandbox::context::Sandbox;
use crate::sandbox::prelude::TestResult;
use crate::sandbox::timing::{WaitHelpers, DEFAULT_WAIT_SECS};
use color_eyre::eyre::eyre;
use serde_json::Value as JsonValue;
use sinex_db::DbPool;
use sinex_db::DbPoolExt;
use sinex_primitives::events::Publishable;
use sinex_primitives::{Event, HostName, Id, OffsetKind, Provenance, SourceMaterial, Timestamp};
use sinex_primitives::{EventSource, EventType, Ulid};
use std::collections::HashSet;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct CreatedEventInfo {
    pub event_id: Ulid,
    pub material_id: Option<Ulid>,
}

pub async fn cleanup_created_records(
    pool: DbPool,
    records: Vec<CreatedEventInfo>,
) -> TestResult<()> {
    if records.is_empty() {
        return Ok(());
    }

    let event_ids: Vec<Uuid> = records.iter().map(|info| info.event_id.to_uuid()).collect();

    if !event_ids.is_empty() {
        sqlx::query!(
            "DELETE FROM core.events WHERE id = ANY(($1::uuid[])::ulid[])",
            &event_ids
        )
        .execute(&pool)
        .await?;
    }

    let material_set: HashSet<Uuid> = records
        .iter()
        .filter_map(|info| info.material_id.map(|id| id.to_uuid()))
        .collect();
    let material_ids: Vec<Uuid> = material_set.into_iter().collect();

    if !material_ids.is_empty() {
        sqlx::query!(
            "DELETE FROM raw.source_material_registry WHERE id = ANY(($1::uuid[])::ulid[])",
            &material_ids
        )
        .execute(&pool)
        .await?;
    }

    Ok(())
}

/// Extension trait for publishing events in Sandbox tests.
#[allow(async_fn_in_trait)]
pub trait EventPublisher {
    /// Publish a test event through the ingestion pipeline.
    async fn publish<P: Publishable>(&self, payload: P) -> TestResult<Event<JsonValue>>;

    /// Internal implementation for event publishing.
    async fn publish_event_internal(
        &self,
        source: EventSource,
        event_type: EventType,
        payload: JsonValue,
        timestamp_override: Option<Timestamp>,
    ) -> TestResult<Event<JsonValue>>;

    /// Publish a pre-built event to the ingestion pipeline via NATS.
    async fn publish_prebuilt_event(&self, event: &Event<JsonValue>) -> TestResult<Ulid>;
}

impl EventPublisher for Sandbox {
    async fn publish<P: Publishable>(&self, payload: P) -> TestResult<Event<JsonValue>> {
        self.publish_event_internal(
            payload.source(),
            payload.event_type(),
            payload.to_json_value()?,
            None,
        )
        .await
    }

    async fn publish_event_internal(
        &self,
        source: EventSource,
        event_type: EventType,
        payload: JsonValue,
        timestamp_override: Option<Timestamp>,
    ) -> TestResult<Event<JsonValue>> {
        // Ensure NATS is available (lazy initialization for property tests)
        let _client = self.ensure_nats().await?;

        let mut sanitized_payload = payload;
        Sandbox::sanitize_payload(&mut sanitized_payload);

        // Create real source material first
        let material_id = Id::<SourceMaterial>::new();
        self.ensure_source_material(material_id, Some(source.as_str()))
            .await?;
        let material_ulid = *material_id.as_ulid();

        // Build event with real provenance from the start
        let event = Event::<JsonValue> {
            id: Some(Id::new()),
            source,
            event_type,
            payload: sanitized_payload,
            ts_orig: Some(timestamp_override.unwrap_or_else(sinex_primitives::Timestamp::now)),
            host: HostName::new(gethostname::gethostname().to_string_lossy().to_string()),
            ingestor_version: Some("test-ingestor".to_string()),
            payload_schema_id: None,
            provenance: Provenance::Material {
                id: material_id,
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: OffsetKind::Byte,
            },
            associated_blob_ids: None,
        };

        // Use the trait method recursion or self method?
        // Since we are implementing the trait for Sandbox, we can call methods on self.
        let persisted_id = self.publish_prebuilt_event(&event).await?;
        let published_event_id = Id::<Event<JsonValue>>::from_ulid(persisted_id);
        WaitHelpers::wait_for_event_id(self.pool(), published_event_id, DEFAULT_WAIT_SECS).await?;

        let stored = self
            .pool()
            .events()
            .get_by_id(published_event_id)
            .await?
            .ok_or_else(|| {
                eyre!(
                    "Event {} not found after pipeline publish",
                    published_event_id
                )
            })?;

        let cleanup_material = match &stored.provenance() {
            Provenance::Material { id, .. } => Some(*id.as_ulid()),
            _ => Some(material_ulid),
        };
        self.record_created_event(*published_event_id.as_ulid(), cleanup_material);

        Ok(stored)
    }

    async fn publish_prebuilt_event(&self, event: &Event<JsonValue>) -> TestResult<Ulid> {
        // Just publish to NATS - caller (PipelineScope) is responsible for ingestd
        let client = self.nats_client();
        let mut envelope = event.clone();

        // Assign an ID if the event doesn't have one
        let event_id = if let Some(id) = &envelope.id {
            *id.as_ulid()
        } else {
            let new_id = Id::new();
            let ulid = *new_id.as_ulid();
            envelope.id = Some(new_id);
            ulid
        };

        if envelope.ingestor_version.is_none() {
            envelope.ingestor_version = Some("test-ingestd".to_string());
        }
        let payload = serde_json::to_vec(&envelope)?;

        let base_subject = format!("events.raw.{}", event.source);
        let subject = self.pipeline_namespace().subject(&base_subject);

        client.publish(subject, payload.into()).await?;

        Ok(event_id)
    }
}
