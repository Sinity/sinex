use crate::sandbox::context::Sandbox;
use crate::sandbox::prelude::TestResult;
use crate::sandbox::timing::{DEFAULT_WAIT_SECS, WaitHelpers};
use color_eyre::eyre::eyre;
use serde_json::Value as JsonValue;
use sinex_db::DbPool;
use sinex_db::DbPoolExt;
use sinex_primitives::events::Publishable;
use sinex_primitives::{Event, HostName, Id, OffsetKind, Provenance, SourceMaterial, Timestamp};
use sinex_primitives::{EventSource, EventType, Uuid};
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct CreatedEventInfo {
    pub event_id: Uuid,
    pub material_id: Option<Uuid>,
}

pub async fn cleanup_created_records(
    pool: DbPool,
    records: Vec<CreatedEventInfo>,
) -> TestResult<()> {
    if records.is_empty() {
        return Ok(());
    }

    let event_ids: Vec<Uuid> = records.iter().map(|info| info.event_id).collect();

    if !event_ids.is_empty() {
        sqlx::query("DELETE FROM core.events WHERE id = ANY(($1::uuid[])::uuid[])")
            .bind(&event_ids)
            .execute(&pool)
            .await?;
    }

    let material_set: HashSet<Uuid> = records
        .iter()
        .filter_map(|info| info.material_id)
        .collect();
    let material_ids: Vec<Uuid> = material_set.into_iter().collect();

    if !material_ids.is_empty() {
        sqlx::query(
            "DELETE FROM raw.source_material_registry WHERE id = ANY(($1::uuid[])::uuid[])",
        )
        .bind(&material_ids)
        .execute(&pool)
        .await?;
    }

    Ok(())
}

/// Extension trait for publishing events in Sandbox tests.
pub trait EventPublisher {
    /// Publish a test event through the ingestion pipeline.
    async fn publish<P: Publishable>(&self, payload: P) -> TestResult<Event<JsonValue>>;

    /// Publish a test event with a specific timestamp override.
    ///
    /// Used by dataset seeding to create events with deterministic, ordered timestamps
    /// rather than defaulting to `Timestamp::now()`.
    async fn publish_at<P: Publishable>(
        &self,
        payload: P,
        timestamp: Timestamp,
    ) -> TestResult<Event<JsonValue>> {
        self.publish_event_internal(
            payload.source(),
            payload.event_type(),
            payload.to_json_value()?,
            Some(timestamp),
        )
        .await
    }

    /// Internal implementation for event publishing.
    async fn publish_event_internal(
        &self,
        source: EventSource,
        event_type: EventType,
        payload: JsonValue,
        timestamp_override: Option<Timestamp>,
    ) -> TestResult<Event<JsonValue>>;

    /// Publish a pre-built event to the ingestion pipeline via NATS.
    async fn publish_prebuilt_event(&self, event: &Event<JsonValue>) -> TestResult<Uuid>;
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
        let material_uuid = *material_id.as_uuid();

        // Build event with real provenance from the start
        let event = Event::<JsonValue> {
            id: Some(Id::new()),
            source,
            event_type,
            payload: sanitized_payload,
            ts_orig: Some(timestamp_override.unwrap_or_else(sinex_primitives::Timestamp::now)),
            host: HostName::new(gethostname::gethostname().to_string_lossy().to_string()),
            node_version: Some("test-ingestor".to_string()),
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
        let published_event_id = Id::<Event<JsonValue>>::from_uuid(persisted_id);
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
            Provenance::Material { id, .. } => Some(*id.as_uuid()),
            _ => Some(material_uuid),
        };
        self.record_created_event(*published_event_id.as_uuid(), cleanup_material);

        Ok(stored)
    }

    async fn publish_prebuilt_event(&self, event: &Event<JsonValue>) -> TestResult<Uuid> {
        // Just publish to NATS - caller (PipelineScope) is responsible for ingestd
        let client = self.nats_client();
        let mut envelope = event.clone();

        // Assign an ID if the event doesn't have one
        let event_id = if let Some(id) = &envelope.id {
            *id.as_uuid()
        } else {
            let new_id = Id::new();
            let uuid = *new_id.as_uuid();
            envelope.id = Some(new_id);
            uuid
        };

        if envelope.node_version.is_none() {
            envelope.node_version = Some("test-ingestd".to_string());
        }
        let payload = serde_json::to_vec(&envelope)?;

        let base_subject = format!("events.raw.{}", event.source);
        let subject = self.pipeline_namespace().subject(&base_subject);

        client.publish(subject.clone(), payload.into()).await?;
        client
            .flush()
            .await
            .map_err(|e| eyre!("NATS flush failed: {e}"))?;

        Ok(event_id)
    }
}

impl Sandbox {
    /// Build test events in memory without publishing to the pipeline.
    ///
    /// This is the fast path for property tests that only need to verify event
    /// construction properties (ordering, batching, counts) without requiring
    /// DB persistence or pipeline processing. No NATS or ingestd needed.
    pub fn build_test_events<P: Publishable>(
        &self,
        payloads: impl IntoIterator<Item = P>,
    ) -> TestResult<Vec<Event<JsonValue>>> {
        let mut events = Vec::new();
        for payload in payloads {
            let source = payload.source();
            let event_type = payload.event_type();
            let mut sanitized_payload = payload.to_json_value()?;
            Sandbox::sanitize_payload(&mut sanitized_payload);

            let material_id = Id::<SourceMaterial>::new();

            let event = Event::<JsonValue> {
                id: Some(Id::new()),
                source,
                event_type,
                payload: sanitized_payload,
                ts_orig: Some(Timestamp::now()),
                host: HostName::new(gethostname::gethostname().to_string_lossy().to_string()),
                node_version: Some("test-ingestor".to_string()),
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
            events.push(event);
        }
        Ok(events)
    }

    /// Publish multiple events through the pipeline as a batch.
    ///
    /// **Much faster than calling `publish()` in a loop**: all events are published
    /// to NATS first (O(N) NATS publishes, ~1ms each), then a single wait confirms
    /// the last event is persisted in the database.
    ///
    /// Returns the pre-built events (as submitted to NATS). For 100 events this
    /// completes in ~2-5 seconds vs ~50+ seconds with sequential `publish()`.
    pub async fn publish_many<P: Publishable>(
        &self,
        payloads: impl IntoIterator<Item = P>,
    ) -> TestResult<Vec<Event<JsonValue>>> {
        let _client = self.ensure_nats().await?;

        let mut events = Vec::new();
        let mut cleanup_records: Vec<(Uuid, Uuid)> = Vec::new();

        // Phase 1: Publish all events to NATS without waiting for DB persistence.
        // Each NATS publish takes ~1ms, so 100 events ≈ 100ms.
        for payload in payloads {
            let source = payload.source();
            let event_type = payload.event_type();
            let mut sanitized_payload = payload.to_json_value()?;
            Sandbox::sanitize_payload(&mut sanitized_payload);

            let material_id = Id::<SourceMaterial>::new();
            self.ensure_source_material(material_id, Some(source.as_str()))
                .await?;
            let material_uuid = *material_id.as_uuid();

            let event = Event::<JsonValue> {
                id: Some(Id::new()),
                source,
                event_type,
                payload: sanitized_payload,
                ts_orig: Some(Timestamp::now()),
                host: HostName::new(gethostname::gethostname().to_string_lossy().to_string()),
                node_version: Some("test-ingestor".to_string()),
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

            let event_uuid = self.publish_prebuilt_event(&event).await?;
            cleanup_records.push((event_uuid, material_uuid));
            events.push(event);
        }

        if events.is_empty() {
            return Ok(vec![]);
        }

        // Phase 2: Wait for the last event to be persisted.
        // Since ingestd processes events in JetStream order (single consumer),
        // once the last event is in DB, all preceding events are guaranteed to be there.
        // Safety: `cleanup_records` is non-empty because we checked `events.is_empty()` above.
        let last_event_uuid = cleanup_records
            .last()
            .expect("non-empty after is_empty check")
            .0;
        let last_event_id = Id::<Event<JsonValue>>::from_uuid(last_event_uuid);
        WaitHelpers::wait_for_event_id(self.pool(), last_event_id, DEFAULT_WAIT_SECS).await?;

        // Phase 3: Record all events for cleanup
        for (event_uuid, material_uuid) in &cleanup_records {
            self.record_created_event(*event_uuid, Some(*material_uuid));
        }

        Ok(events)
    }
}
