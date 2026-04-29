//! NATS `JetStream` event publisher

use crate::{NodeResult, error_helpers::env_parse_with_default};
use serde::Serialize;
use sinex_primitives::{
    JsonValue,
    environment::{SinexEnvironment, environment},
    events::{Event, OffsetKind, Provenance},
    nats::{NatsTrafficClass, insert_traffic_class_header},
};
use std::{
    future::IntoFuture,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;

const DEFAULT_PUBLISH_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RAW_EVENT_PUBLISH_CONCURRENCY: usize = 100;
const DEFAULT_TELEMETRY_PUBLISH_CONCURRENCY: usize = 16;
const DEFAULT_RAW_INGEST_DLQ_PUBLISH_CONCURRENCY: usize = 16;
const DEFAULT_PROCESSING_FAILURE_PUBLISH_CONCURRENCY: usize = 16;

#[derive(Debug, Clone)]
struct PublishSemaphores {
    raw_event: Arc<Semaphore>,
    telemetry: Arc<Semaphore>,
    raw_ingest_dlq: Arc<Semaphore>,
    processing_failure: Arc<Semaphore>,
}

impl PublishSemaphores {
    fn new(
        raw_event: usize,
        telemetry: usize,
        raw_ingest_dlq: usize,
        processing_failure: usize,
    ) -> Self {
        Self {
            raw_event: Arc::new(Semaphore::new(raw_event)),
            telemetry: Arc::new(Semaphore::new(telemetry)),
            raw_ingest_dlq: Arc::new(Semaphore::new(raw_ingest_dlq)),
            processing_failure: Arc::new(Semaphore::new(processing_failure)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NatsPublisher {
    nats_client: async_nats::Client,
    js: async_nats::jetstream::Context,
    env: SinexEnvironment,
    namespace: Option<String>,
    semaphores: PublishSemaphores,
    processing_failure_log_count: Arc<AtomicU64>,
}

/// Destructured provenance fields for publish payloads.
struct ProvenanceFields {
    source_material_id: Option<String>,
    anchor_byte: Option<i64>,
    offset_start: Option<i64>,
    offset_end: Option<i64>,
    offset_kind: Option<String>,
    source_event_ids: Option<Vec<String>>,
}

fn destructure_provenance(provenance: &Provenance) -> ProvenanceFields {
    match provenance {
        Provenance::Material {
            id,
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind,
        } => {
            let include_offsets = offset_start.is_some() && offset_end.is_some();
            ProvenanceFields {
                source_material_id: Some(id.to_string()),
                anchor_byte: Some(*anchor_byte),
                offset_start: *offset_start,
                offset_end: *offset_end,
                offset_kind: include_offsets.then(|| offset_kind_label(*offset_kind).to_string()),
                source_event_ids: None,
            }
        }
        Provenance::Synthesis {
            source_event_ids, ..
        } => ProvenanceFields {
            source_material_id: None,
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: Some(
                source_event_ids
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect(),
            ),
        },
    }
}

#[derive(Serialize)]
struct PublishEvent<'a> {
    id: String,
    source: &'a str,
    event_type: &'a str,
    ts_orig: String,
    host: &'a str,
    payload: &'a JsonValue,
    node_run_id: Option<String>,
    payload_schema_id: Option<String>,
    associated_blob_ids: Option<Vec<String>>,
    source_material_id: Option<String>,
    anchor_byte: Option<i64>,
    offset_start: Option<i64>,
    offset_end: Option<i64>,
    offset_kind: Option<String>,
    source_event_ids: Option<Vec<String>>,
    temporal_policy: Option<String>,
    semantics_version: Option<&'a str>,
    scope_key: Option<&'a str>,
    equivalence_key: Option<&'a str>,
    created_by_operation_id: Option<String>,
    node_model: Option<String>,
}

impl NatsPublisher {
    #[must_use]
    pub fn new(nats_client: async_nats::Client) -> Self {
        Self::with_namespace(nats_client, None)
    }

    #[must_use]
    pub fn with_namespace(nats_client: async_nats::Client, namespace: Option<String>) -> Self {
        let env = environment().clone();
        let raw_event_concurrency = env_parse_with_default(
            "SINEX_PUBLISH_CONCURRENCY",
            DEFAULT_RAW_EVENT_PUBLISH_CONCURRENCY,
            "nats raw-event publisher concurrency",
        );
        let telemetry_concurrency = env_parse_with_default(
            "SINEX_TELEMETRY_PUBLISH_CONCURRENCY",
            DEFAULT_TELEMETRY_PUBLISH_CONCURRENCY,
            "nats telemetry publisher concurrency",
        );
        let raw_ingest_dlq_concurrency = env_parse_with_default(
            "SINEX_RAW_INGEST_DLQ_PUBLISH_CONCURRENCY",
            DEFAULT_RAW_INGEST_DLQ_PUBLISH_CONCURRENCY,
            "nats raw-ingest DLQ publisher concurrency",
        );
        let processing_failure_concurrency = env_parse_with_default(
            "SINEX_PROCESSING_FAILURE_PUBLISH_CONCURRENCY",
            DEFAULT_PROCESSING_FAILURE_PUBLISH_CONCURRENCY,
            "nats processing-failure publisher concurrency",
        );
        let js = async_nats::jetstream::new(nats_client.clone());
        Self {
            nats_client,
            js,
            env,
            namespace,
            semaphores: PublishSemaphores::new(
                raw_event_concurrency,
                telemetry_concurrency,
                raw_ingest_dlq_concurrency,
                processing_failure_concurrency,
            ),
            processing_failure_log_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get the underlying NATS client
    #[must_use]
    pub fn nats_client(&self) -> &async_nats::Client {
        &self.nats_client
    }

    /// Publish an event to the raw-ingest DLQ.
    ///
    /// transport::Class::Critical (DLQ routing) — operator-facing raw DLQ;
    /// retry tooling available via `sinexctl dlq retry`. Derived/runtime
    /// processing failures must use `publish_processing_failure`.
    pub async fn publish_to_raw_ingest_dlq(
        &self,
        event: &Event,
        error: &str,
        node_name: &str,
    ) -> NodeResult<()> {
        let _permit = acquire_lane_permit(
            &self.semaphores.raw_ingest_dlq,
            "raw-ingest DLQ",
        )
        .await?;
        let prov = destructure_provenance(event.provenance());

        let (event_id, original_event_bytes) = build_publish_payload(
            event,
            prov.source_material_id,
            prov.anchor_byte,
            prov.offset_start,
            prov.offset_end,
            prov.offset_kind,
            prov.source_event_ids,
        )?;
        let original_event = serde_json::from_slice::<JsonValue>(&original_event_bytes)
            .map_err(sinex_primitives::SinexError::from)?;
        let original_subject = self.env.nats_raw_event_subject_with_namespace(
            self.namespace.as_deref(),
            event.source.as_str(),
            event.event_type.as_str(),
        );

        // Build DLQ entry with error context
        let dlq_entry = serde_json::json!({
            "event_id": event_id,
            "nats_msg_id": event_id,
            "source": event.source.as_str(),
            "event_type": event.event_type.as_str(),
            "error": error,
            "node": node_name,
            "original_event": original_event,
            "failed_at": sinex_primitives::temporal::format_rfc3339(sinex_primitives::temporal::now()),
        });

        let payload = serde_json::to_vec(&dlq_entry).map_err(sinex_primitives::SinexError::from)?;

        // DLQ subject format: events.dlq.{node_name}.{event_id}
        let subject = self.env.nats_subject_with_namespace(
            self.namespace.as_deref(),
            &format!("events.dlq.{}.{}", node_name.replace('.', "_"), event_id),
        );

        // Add headers for retry tracking
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", format!("dlq-{event_id}").as_str());
        headers.insert("Event-Id", event_id.as_str());
        headers.insert("Original-Subject", original_subject.as_str());
        headers.insert("Retry-Count", "0");
        insert_traffic_class_header(&mut headers, NatsTrafficClass::RawIngestDlq);

        let ack_future = self
            .publish_with_headers(
            subject,
            headers,
            payload,
            "Failed to publish raw-ingest DLQ message",
        )
            .await?;
        let ack = wait_for_publish_ack(ack_future, DEFAULT_PUBLISH_ACK_TIMEOUT).await?;

        tracing::warn!(
            event_id = %event_id,
            node = %node_name,
            error = %error,
            sequence = ack.sequence,
            "Event sent to raw-ingest DLQ"
        );

        Ok(())
    }

    /// Publish a raw ingestor event.
    ///
    /// transport::Class::Critical — provenance-bearing; failure routes to
    /// local recovery spool. Drain: wait for in-flight ACKs.
    pub async fn publish(&self, event: &Event) -> NodeResult<()> {
        self.publish_event_with_class(
            event,
            NatsTrafficClass::RawEvent,
            &self.semaphores.raw_event,
            "raw event",
        )
        .await
    }

    /// Publish a self-observation telemetry event.
    ///
    /// transport::Class::Telemetry — loss acceptable; drop with warn on
    /// failure. Drain: best-effort flush.
    pub async fn publish_telemetry(&self, event: &Event) -> NodeResult<()> {
        self.publish_event_with_class(
            event,
            NatsTrafficClass::Telemetry,
            &self.semaphores.telemetry,
            "telemetry event",
        )
        .await
    }

    /// Publish a derived-node processing failure envelope.
    ///
    /// transport::Class::Derived (failure routing) — routes to the
    /// processing-failure stream (`events.processing_failures.*`), not the
    /// raw-ingest DLQ. Re-runnable via automaton replay.
    pub async fn publish_processing_failure(
        &self,
        event: &Event,
        error: &str,
        node_name: &str,
    ) -> NodeResult<()> {
        let _permit = acquire_lane_permit(
            &self.semaphores.processing_failure,
            "processing failure",
        )
        .await?;
        let prov = destructure_provenance(event.provenance());

        let (event_id, original_event_bytes) = build_publish_payload(
            event,
            prov.source_material_id,
            prov.anchor_byte,
            prov.offset_start,
            prov.offset_end,
            prov.offset_kind,
            prov.source_event_ids,
        )?;
        let original_event = serde_json::from_slice::<JsonValue>(&original_event_bytes)
            .map_err(sinex_primitives::SinexError::from)?;
        let original_subject = self.env.nats_raw_event_subject_with_namespace(
            self.namespace.as_deref(),
            event.source.as_str(),
            event.event_type.as_str(),
        );

        let failure_entry = serde_json::json!({
            "event_id": event_id,
            "source": event.source.as_str(),
            "event_type": event.event_type.as_str(),
            "error": error,
            "node": node_name,
            "original_event": original_event,
            "failed_at": sinex_primitives::temporal::format_rfc3339(sinex_primitives::temporal::now()),
        });

        let payload =
            serde_json::to_vec(&failure_entry).map_err(sinex_primitives::SinexError::from)?;
        let subject = self.env.nats_subject_with_namespace(
            self.namespace.as_deref(),
            &format!(
                "events.processing_failures.{}.{}",
                node_name.replace('.', "_"),
                event_id
            ),
        );

        let mut headers = async_nats::HeaderMap::new();
        headers.insert(
            "Nats-Msg-Id",
            format!("processing-failure-{event_id}").as_str(),
        );
        headers.insert("Event-Id", event_id.as_str());
        headers.insert("Original-Subject", original_subject.as_str());
        insert_traffic_class_header(&mut headers, NatsTrafficClass::ProcessingFailure);

        let ack_future = self
            .publish_with_headers(
            subject,
            headers,
            payload,
            "Failed to publish processing-failure message",
        )
            .await?;
        let ack = wait_for_publish_ack(ack_future, DEFAULT_PUBLISH_ACK_TIMEOUT).await?;

        let count = self
            .processing_failure_log_count
            .fetch_add(1, Ordering::Relaxed);
        if count % 100 == 0 {
            tracing::warn!(
                event_id = %event_id,
                node = %node_name,
                error = %error,
                sequence = ack.sequence,
                skipped = count,
                "Event sent to processing-failure stream (rate-limited, logging every 100th)"
            );
        }

        Ok(())
    }

    async fn publish_event_with_class(
        &self,
        event: &Event,
        traffic_class: NatsTrafficClass,
        semaphore: &Arc<Semaphore>,
        lane_label: &'static str,
    ) -> NodeResult<()> {
        let _permit = acquire_lane_permit(semaphore, lane_label).await?;

        let prov = destructure_provenance(event.provenance());

        let (event_id_str, payload) = build_publish_payload(
            event,
            prov.source_material_id,
            prov.anchor_byte,
            prov.offset_start,
            prov.offset_end,
            prov.offset_kind,
            prov.source_event_ids,
        )?;

        let subject = self.env.nats_raw_event_subject_with_namespace(
            self.namespace.as_deref(),
            event.source.as_str(),
            event.event_type.as_str(),
        );

        // Add idempotency header
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id_str.as_str());
        insert_traffic_class_header(&mut headers, traffic_class);

        // Publish to JetStream, then wait for acknowledgment (bounded by timeout).
        let ack_future = self
            .publish_with_headers(subject, headers, payload, "Failed to publish event")
            .await?;
        let ack = wait_for_publish_ack(ack_future, DEFAULT_PUBLISH_ACK_TIMEOUT).await?;

        tracing::debug!(
            event_id = %event_id_str,
            traffic_class = traffic_class.as_header_value(),
            sequence = ack.sequence,
            stream = %ack.stream,
            "Event published to JetStream"
        );

        Ok(())
    }

    async fn publish_with_headers(
        &self,
        subject: String,
        headers: async_nats::HeaderMap,
        payload: Vec<u8>,
        error_message: &'static str,
    ) -> NodeResult<async_nats::jetstream::context::PublishAckFuture> {
        self.js
            .publish_with_headers(subject, headers, payload.into())
            .await
            .map_err(|error| {
                sinex_primitives::SinexError::processing(error_message).with_source(error)
            })
    }
}

async fn acquire_lane_permit(
    semaphore: &Arc<Semaphore>,
    lane_label: &'static str,
) -> NodeResult<tokio::sync::OwnedSemaphorePermit> {
    semaphore.clone().acquire_owned().await.map_err(|error| {
        sinex_primitives::SinexError::processing(format!(
            "{lane_label} publish semaphore closed"
        ))
        .with_source(error)
    })
}

fn build_publish_payload(
    event: &Event,
    source_material_id: Option<String>,
    anchor_byte: Option<i64>,
    offset_start: Option<i64>,
    offset_end: Option<i64>,
    offset_kind: Option<String>,
    source_event_ids: Option<Vec<String>>,
) -> NodeResult<(String, Vec<u8>)> {
    let event_id = event.id.as_ref().ok_or_else(|| {
        sinex_primitives::SinexError::processing("Event ID is required".to_string())
    })?;
    let ts_orig = event.ts_orig.ok_or_else(|| {
        sinex_primitives::SinexError::processing("Event ts_orig is required".to_string())
    })?;
    let event_id_str = event_id.to_string();

    let payload_schema_id = event.payload_schema_id.map(|id| id.to_string());
    let associated_blob_ids = event.associated_blob_ids.as_ref().map(|ids| {
        ids.iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
    });

    let payload = PublishEvent {
        id: event_id_str.clone(),
        source: event.source.as_str(),
        event_type: event.event_type.as_str(),
        ts_orig: ts_orig.format_rfc3339(),
        host: event.host.as_str(),
        payload: &event.payload,
        node_run_id: event.node_run_id.map(|id| id.to_string()),
        payload_schema_id,
        associated_blob_ids,
        source_material_id,
        anchor_byte,
        offset_start,
        offset_end,
        offset_kind,
        source_event_ids,
        temporal_policy: event.temporal_policy.map(|policy| policy.to_string()),
        semantics_version: event.semantics_version.as_deref(),
        scope_key: event.scope_key.as_deref(),
        equivalence_key: event.equivalence_key.as_deref(),
        created_by_operation_id: event.created_by_operation_id.map(|id| id.to_string()),
        node_model: event.node_model.map(|model| model.to_string()),
    };

    let encoded = serde_json::to_vec(&payload).map_err(sinex_primitives::SinexError::from)?;
    Ok((event_id_str, encoded))
}

async fn wait_for_publish_ack<T, E, F>(future: F, timeout: Duration) -> NodeResult<T>
where
    F: IntoFuture<Output = Result<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    match tokio::time::timeout(timeout, future.into_future()).await {
        Ok(result) => result.map_err(|err| {
            sinex_primitives::SinexError::processing("Failed waiting for JetStream publish ack")
                .with_source(err)
        }),
        Err(_) => Err(sinex_primitives::SinexError::processing(format!(
            "Timed out waiting for JetStream publish ack after {timeout:?}"
        ))),
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

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_RAW_EVENT_PUBLISH_CONCURRENCY, NatsPublisher, build_publish_payload,
        destructure_provenance, wait_for_publish_ack,
    };
    use sinex_primitives::{
        DynamicPayload, Id, Uuid,
        domain::{DerivedNodeModel, SyntheticTemporalPolicy},
        events::Event,
    };
    use std::{future, io, time::Duration};
    use xtask::sandbox::sinex_test;

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
        let mut event = DynamicPayload::new(
            "publisher.test",
            "payload.check",
            serde_json::json!({"nested": {"a": 1}}),
        )
        .from_parents([Id::from_uuid(Uuid::now_v7())])?
        .build()
        .expect("infallible: test provenance set");
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        let (event_id, payload) =
            build_publish_payload(&event, None, None, None, None, None, None)?;
        let value: serde_json::Value = serde_json::from_slice(&payload)?;

        assert_eq!(value["id"], event_id);
        assert!(value["payload"].is_object());
        assert_eq!(value["payload"]["nested"]["a"], 1);
        Ok(())
    }

    #[sinex_test]
    async fn publish_payload_preserves_replay_and_synthetic_metadata() -> TestResult<()> {
        let source_material_id = Id::from_uuid(Uuid::now_v7());
        let mut event = DynamicPayload::new(
            "publisher.test",
            "payload.replay",
            serde_json::json!({"path": "/tmp/replay.txt"}),
        )
        .from_material(source_material_id)
        .build()
        .expect("infallible: test provenance set");
        let operation_id = Uuid::now_v7();
        event.id = Some(Id::from_uuid(Uuid::now_v7()));
        event.temporal_policy = Some(SyntheticTemporalPolicy::LatestInput);
        event.semantics_version = Some("2026-04-13".to_string());
        event.scope_key = Some("scope:publisher".to_string());
        event.equivalence_key = Some("publisher-slot".to_string());
        event.created_by_operation_id = Some(operation_id);
        event.node_model = Some(DerivedNodeModel::Windowed);

        let prov = destructure_provenance(event.provenance());
        let (_, payload) = build_publish_payload(
            &event,
            prov.source_material_id,
            prov.anchor_byte,
            prov.offset_start,
            prov.offset_end,
            prov.offset_kind,
            prov.source_event_ids,
        )?;
        let decoded: Event<serde_json::Value> = serde_json::from_slice(&payload)?;

        assert_eq!(
            decoded.temporal_policy,
            Some(SyntheticTemporalPolicy::LatestInput)
        );
        assert_eq!(decoded.semantics_version.as_deref(), Some("2026-04-13"));
        assert_eq!(decoded.scope_key.as_deref(), Some("scope:publisher"));
        assert_eq!(decoded.equivalence_key.as_deref(), Some("publisher-slot"));
        assert_eq!(decoded.created_by_operation_id, Some(operation_id));
        assert_eq!(decoded.node_model, Some(DerivedNodeModel::Windowed));
        Ok(())
    }

    #[sinex_test]
    async fn invalid_publish_concurrency_override_falls_back_to_default(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let previous = std::env::var("SINEX_PUBLISH_CONCURRENCY").ok();
        unsafe { std::env::set_var("SINEX_PUBLISH_CONCURRENCY", "bogus") };

        let publisher = NatsPublisher::new(ctx.nats_client());

        unsafe {
            match previous {
                Some(value) => std::env::set_var("SINEX_PUBLISH_CONCURRENCY", value),
                None => std::env::remove_var("SINEX_PUBLISH_CONCURRENCY"),
            }
        }

        assert_eq!(
            publisher.semaphores.raw_event.available_permits(),
            DEFAULT_RAW_EVENT_PUBLISH_CONCURRENCY
        );
        Ok(())
    }
}
