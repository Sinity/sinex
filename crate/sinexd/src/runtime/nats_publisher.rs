//! NATS `JetStream` event publisher

use crate::runtime::RuntimeResult;
use crate::runtime::nats_payload::ensure_nats_payload_fits;
use async_nats::jetstream::context::ConsumerInfoErrorKind;
use serde::Serialize;
use sinex_primitives::env as shared_env;
use sinex_primitives::{
    JsonValue,
    environment::{SinexEnvironment, environment},
    events::{Event, OffsetKind, Provenance, admission::EventIntent},
    nats::{NatsTrafficClass, insert_traffic_class_header},
    transport,
};
use std::{
    future::IntoFuture,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

const DEFAULT_PUBLISH_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RAW_EVENT_PUBLISH_CONCURRENCY: usize = 100;
const DEFAULT_TELEMETRY_PUBLISH_CONCURRENCY: usize = 16;
const DEFAULT_RAW_INGEST_DLQ_PUBLISH_CONCURRENCY: usize = 16;
const DEFAULT_PROCESSING_FAILURE_PUBLISH_CONCURRENCY: usize = 16;
const RAW_STREAM_BACKPRESSURE_HIGH_PENDING: u64 = 10_000;
const RAW_STREAM_BACKPRESSURE_LOW_PENDING: u64 = 2_000;
const RAW_STREAM_BACKPRESSURE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const RAW_STREAM_BACKPRESSURE_LOG_EVERY: u64 = 1_200;

#[derive(Debug, Default)]
struct RawStreamPressureState {
    last_capacity_check: Option<Instant>,
    pressure_checks: u64,
}

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
    raw_events_stream_ready: Arc<AtomicBool>,
    reflection_events_stream_ready: Arc<AtomicBool>,
    raw_events_stream_bootstrap_lock: Arc<Mutex<()>>,
    reflection_events_stream_bootstrap_lock: Arc<Mutex<()>>,
    raw_stream_pressure: Arc<Mutex<RawStreamPressureState>>,
    processing_failure_log_count: Arc<AtomicU64>,
    publish_ack_timeout: Duration,
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
        Provenance::Derived {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    ts_orig: Option<String>,
    host: &'a str,
    payload: &'a JsonValue,
    module_run_id: Option<String>,
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
    automaton_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ts_quality: Option<String>,
}

impl NatsPublisher {
    #[must_use]
    pub fn new(nats_client: async_nats::Client) -> Self {
        Self::with_namespace(nats_client, None)
    }

    #[must_use]
    pub fn with_namespace(nats_client: async_nats::Client, namespace: Option<String>) -> Self {
        let env = environment().clone();
        let raw_event_concurrency = shared_env::parse_or(
            "SINEX_PUBLISH_CONCURRENCY",
            DEFAULT_RAW_EVENT_PUBLISH_CONCURRENCY,
            "nats raw-event publisher concurrency",
        );
        let telemetry_concurrency = shared_env::parse_or(
            "SINEX_TELEMETRY_PUBLISH_CONCURRENCY",
            DEFAULT_TELEMETRY_PUBLISH_CONCURRENCY,
            "nats telemetry publisher concurrency",
        );
        let raw_ingest_dlq_concurrency = shared_env::parse_or(
            "SINEX_RAW_INGEST_DLQ_PUBLISH_CONCURRENCY",
            DEFAULT_RAW_INGEST_DLQ_PUBLISH_CONCURRENCY,
            "nats raw-ingest DLQ publisher concurrency",
        );
        let processing_failure_concurrency = shared_env::parse_or(
            "SINEX_PROCESSING_FAILURE_PUBLISH_CONCURRENCY",
            DEFAULT_PROCESSING_FAILURE_PUBLISH_CONCURRENCY,
            "nats processing-failure publisher concurrency",
        );
        let js = async_nats::jetstream::new(nats_client.clone());
        let publish_ack_timeout: u64 = shared_env::parse_or(
            "SINEX_PUBLISH_ACK_TIMEOUT_MS",
            DEFAULT_PUBLISH_ACK_TIMEOUT.as_millis() as u64,
            "nats publish ack timeout (ms)",
        );
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
            raw_events_stream_ready: Arc::new(AtomicBool::new(false)),
            reflection_events_stream_ready: Arc::new(AtomicBool::new(false)),
            raw_events_stream_bootstrap_lock: Arc::new(Mutex::new(())),
            reflection_events_stream_bootstrap_lock: Arc::new(Mutex::new(())),
            raw_stream_pressure: Arc::new(Mutex::new(RawStreamPressureState::default())),
            processing_failure_log_count: Arc::new(AtomicU64::new(0)),
            publish_ack_timeout: Duration::from_millis(publish_ack_timeout),
        }
    }

    /// Set the publish ack timeout (e.g. from `RetryConfig::publish_ack_timeout`).
    #[must_use]
    pub fn with_publish_ack_timeout(mut self, timeout: Duration) -> Self {
        self.publish_ack_timeout = timeout;
        self
    }

    /// Get the underlying NATS client
    #[must_use]
    pub fn nats_client(&self) -> &async_nats::Client {
        &self.nats_client
    }

    /// Namespace used for raw event, telemetry, DLQ, and material subjects.
    #[must_use]
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// Publish an event to the raw-ingest DLQ.
    ///
    /// `transport::Class::Critical` (DLQ routing) — operator-facing raw DLQ;
    /// retry tooling available via `sinexctl dlq retry`. Derived/runtime
    /// processing failures must use `publish_processing_failure`.
    pub async fn publish_to_raw_ingest_dlq(
        &self,
        event: &Event,
        error: &str,
        module_name: &str,
        class: transport::Class,
    ) -> RuntimeResult<()> {
        debug_assert!(
            matches!(class, transport::Class::Critical),
            "publish_to_raw_ingest_dlq is exclusively for Class::Critical; got {class:?}"
        );
        let _permit =
            acquire_lane_permit(&self.semaphores.raw_ingest_dlq, "raw-ingest DLQ").await?;
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
            "module": module_name,
            "original_event": original_event,
            "failed_at": sinex_primitives::temporal::format_rfc3339(sinex_primitives::temporal::now()),
        });

        let payload = serde_json::to_vec(&dlq_entry).map_err(sinex_primitives::SinexError::from)?;

        // DLQ subject format: events.dlq.{module_name}.{event_id}
        let subject = self.env.nats_subject_with_namespace(
            self.namespace.as_deref(),
            &format!("events.dlq.{}.{}", module_name.replace('.', "_"), event_id),
        );

        // Add headers for retry tracking
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", format!("dlq-{event_id}").as_str());
        headers.insert("Event-Id", event_id.as_str());
        headers.insert("Original-Subject", original_subject.as_str());
        headers.insert("Retry-Count", "0");
        insert_traffic_class_header(&mut headers, NatsTrafficClass::RawIngestDlq);
        transport::insert_semantic_transport_class_header(&mut headers, class);

        let ack_future = self
            .publish_with_headers(
                subject,
                headers,
                payload,
                "Failed to publish raw-ingest DLQ message",
            )
            .await?;
        let ack = wait_for_publish_ack(ack_future, self.publish_ack_timeout).await?;

        tracing::warn!(
            event_id = %event_id,
            module = %module_name,
            error = %error,
            sequence = ack.sequence,
            "Event sent to raw-ingest DLQ"
        );

        Ok(())
    }

    /// Publish an admitted event intent envelope to durable transport.
    ///
    /// This is the NORMAL path for provenance-bearing events. Producers construct
    /// an [`EventIntent`] to declare "I've done my admission checks" and
    /// the envelope is serialized as a single NATS message.
    ///
    /// Both `Class::Critical` (source-bearing events) and
    /// `Class::Derived` (automaton derived outputs) ride the same raw-events
    /// lane on the wire.
    ///
    /// Drain semantics: wait for in-flight ACKs (both Critical and Derived).
    /// Failure routing differs — see `transport::Class` docs.
    pub async fn publish_intent(
        &self,
        intent: &EventIntent,
        class: transport::Class,
    ) -> RuntimeResult<()> {
        debug_assert!(
            matches!(
                class,
                transport::Class::Critical | transport::Class::Derived
            ),
            "NatsPublisher::publish_intent accepts Critical or Derived; got {class:?}. \
             Use publish_telemetry for Class::Telemetry. \
             Confirmation/Invalidation/Control are not raw-events publishers."
        );

        if !intent.is_version_accepted() {
            return Err(sinex_primitives::SinexError::processing(format!(
                "envelope version {} is not accepted by this publisher (accepted: {:?})",
                intent.envelope_version,
                sinex_primitives::events::admission::ACCEPTED_ENVELOPE_VERSIONS,
            )));
        }

        let _permit = acquire_lane_permit(&self.semaphores.raw_event, "raw event intent").await?;
        self.ensure_raw_events_stream_ready().await?;
        self.wait_for_raw_events_stream_capacity().await?;

        let payload = serde_json::to_vec(intent).map_err(sinex_primitives::SinexError::from)?;

        // Use the first event's source/type for subject routing
        let first_event = intent.events.first().ok_or_else(|| {
            sinex_primitives::SinexError::processing("admitted event intent has no events")
        })?;
        let subject = self.env.nats_raw_event_subject_with_namespace(
            self.namespace.as_deref(),
            first_event.source.as_str(),
            first_event.event_type.as_str(),
        );

        // Idempotency header uses a composite: first event ID + count
        let msg_id = if let Some(first_id) = first_event.id.as_ref() {
            format!("intent-{}-{}", first_id, intent.event_count())
        } else {
            format!(
                "intent-{}-{}",
                sinex_primitives::Uuid::now_v7(),
                intent.event_count()
            )
        };

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", msg_id.as_str());
        headers.insert("Sinex-Envelope-Version", intent.envelope_version.as_str());
        transport::insert_transport_class_headers(&mut headers, class);

        let ack_future = self
            .publish_with_headers(subject, headers, payload, "Failed to publish event intent")
            .await?;
        let ack = wait_for_publish_ack(ack_future, self.publish_ack_timeout).await?;

        tracing::debug!(
            event_count = intent.event_count(),
            envelope_version = %intent.envelope_version,
            source = %intent.source_id,
            traffic_class = class.traffic_class().as_header_value(),
            sequence = ack.sequence,
            stream = %ack.stream,
            "Event intent envelope published to JetStream"
        );

        Ok(())
    }

    /// LOW-LEVEL ESCAPE HATCH: publish raw events without an admission envelope.
    ///
    /// ONLY for tests, fixtures, and bootstrap. Never in production producer code.
    /// Grep for this name (`publish_raw_event_batch`) to audit call sites.
    ///
    /// Each event is serialized individually and published to the appropriate
    /// NATS subject. This bypasses the `EventIntent` envelope that normal
    /// producers must use.
    pub async fn publish_raw_event_batch(
        &self,
        events: &[&Event],
        class: transport::Class,
    ) -> RuntimeResult<()> {
        debug_assert!(
            matches!(
                class,
                transport::Class::Critical | transport::Class::Derived
            ),
            "publish_raw_event_batch accepts Critical or Derived; got {class:?}"
        );

        for event in events {
            self.publish_raw_event(event, class).await?;
        }
        Ok(())
    }

    /// Publish a single raw event (internal helper for the escape hatch).
    async fn publish_raw_event(&self, event: &Event, class: transport::Class) -> RuntimeResult<()> {
        let _permit =
            acquire_lane_permit(&self.semaphores.raw_event, "raw event (escape hatch)").await?;
        self.ensure_raw_events_stream_ready().await?;
        self.wait_for_raw_events_stream_capacity().await?;

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

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id_str.as_str());
        transport::insert_transport_class_headers(&mut headers, class);

        let ack_future = self
            .publish_with_headers(subject, headers, payload, "Failed to publish raw event")
            .await?;
        let ack = wait_for_publish_ack(ack_future, self.publish_ack_timeout).await?;

        tracing::debug!(
            event_id = %event_id_str,
            traffic_class = class.traffic_class().as_header_value(),
            sequence = ack.sequence,
            stream = %ack.stream,
            "Raw event published to JetStream (escape hatch)"
        );

        Ok(())
    }

    /// Publish a self-observation telemetry event.
    ///
    /// `transport::Class::Telemetry` is implicit (the method name pins the
    /// semantic class); the parameter remains positional for symmetry with
    /// `publish_intent` and is asserted internally to catch accidental misuse.
    /// Loss acceptable; drop with warn on failure. Drain: best-effort flush.
    pub async fn publish_telemetry(
        &self,
        event: &Event,
        class: transport::Class,
    ) -> RuntimeResult<()> {
        debug_assert!(
            matches!(class, transport::Class::Telemetry),
            "publish_telemetry is exclusively for Class::Telemetry; got {class:?}"
        );
        let _permit = acquire_lane_permit(&self.semaphores.telemetry, "telemetry event").await?;
        self.ensure_reflection_events_stream_ready().await?;
        // Telemetry is lossy and may be emitted by the event-engine consumer.
        // Do not wait for backlog capacity here; the consumer that emits this
        // signal may be the same component that must drain the stream.

        let event_id_str = event
            .id
            .as_ref()
            .ok_or_else(|| {
                sinex_primitives::SinexError::processing("Telemetry event ID is required")
            })?
            .to_string();

        // Wrap in EventIntent so the event engine can parse it through the
        // standard admission pipeline. Raw-event bypass caused DLQ entries
        // with "missing field `envelope_version`".
        let intent = EventIntent::new(
            format!("sinex.self-telemetry.{}", event.source.as_str()),
            "self-observer",
            "1.0.0",
            vec![event.clone()],
            sinex_primitives::events::builder::get_hostname(),
        );
        let payload = serde_json::to_vec(&intent).map_err(sinex_primitives::SinexError::from)?;

        let subject = self.env.nats_reflection_event_subject_with_namespace(
            self.namespace.as_deref(),
            event.source.as_str(),
            event.event_type.as_str(),
        );

        let msg_id = format!("intent-{}-1", event_id_str);
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", msg_id.as_str());
        headers.insert(
            "Sinex-Envelope-Version",
            sinex_primitives::events::admission::CURRENT_ENVELOPE_VERSION,
        );
        transport::insert_transport_class_headers(&mut headers, class);

        let ack_future = self
            .publish_with_headers(
                subject,
                headers,
                payload,
                "Failed to publish telemetry event",
            )
            .await?;
        let ack = wait_for_publish_ack(ack_future, self.publish_ack_timeout).await?;

        tracing::debug!(
            event_id = %event_id_str,
            sequence = ack.sequence,
            stream = %ack.stream,
            "Telemetry event published to JetStream"
        );

        Ok(())
    }

    async fn wait_for_raw_events_stream_capacity(&self) -> RuntimeResult<()> {
        let mut state = self.raw_stream_pressure.lock().await;
        if state
            .last_capacity_check
            .is_some_and(|last_check| last_check.elapsed() < RAW_STREAM_BACKPRESSURE_POLL_INTERVAL)
        {
            return Ok(());
        }

        let stream_name = self
            .env
            .nats_stream_name_with_namespace(self.namespace.as_deref(), "SINEX_RAW_EVENTS");
        let consumer_name = std::env::var("SINEX_EVENT_ENGINE_CONSUMER_NAME")
            .unwrap_or_else(|_| format!("event-engine-{}", self.env.name()));

        loop {
            state.last_capacity_check = Some(Instant::now());
            let mut stream = self.js.get_stream(&stream_name).await.map_err(|error| {
                sinex_primitives::SinexError::network("Failed to inspect raw-events stream")
                    .with_source(error)
            })?;
            let consumer_info = match stream.consumer_info(&consumer_name).await {
                Ok(info) => info,
                Err(error) if error.kind() == ConsumerInfoErrorKind::NotFound => {
                    return Ok(());
                }
                Err(error) => {
                    return Err(sinex_primitives::SinexError::network(
                        "Failed to inspect event-engine raw consumer",
                    )
                    .with_source(error));
                }
            };
            let info = stream.info().await.map_err(|error| {
                sinex_primitives::SinexError::network("Failed to inspect raw-events stream info")
                    .with_source(error)
            })?;
            let bytes = info.state.bytes;
            let pending = consumer_info.num_pending;
            let ack_pending = consumer_info.num_ack_pending;
            let target_watermark = if state.pressure_checks > 0 {
                RAW_STREAM_BACKPRESSURE_LOW_PENDING
            } else {
                RAW_STREAM_BACKPRESSURE_HIGH_PENDING
            };
            if pending <= target_watermark {
                if state.pressure_checks > 0 && pending <= RAW_STREAM_BACKPRESSURE_LOW_PENDING {
                    tracing::info!(
                        stream = %stream_name,
                        consumer = %consumer_name,
                        pending,
                        ack_pending,
                        bytes,
                        pressure_checks = state.pressure_checks,
                        "Raw-events consumer backlog pressure cleared"
                    );
                    state.pressure_checks = 0;
                }
                return Ok(());
            }

            state.pressure_checks = state.pressure_checks.saturating_add(1);
            if state.pressure_checks == 1
                || state
                    .pressure_checks
                    .is_multiple_of(RAW_STREAM_BACKPRESSURE_LOG_EVERY)
            {
                tracing::warn!(
                    stream = %stream_name,
                    consumer = %consumer_name,
                    pending,
                    ack_pending,
                    bytes,
                    high_pending = RAW_STREAM_BACKPRESSURE_HIGH_PENDING,
                    low_pending = RAW_STREAM_BACKPRESSURE_LOW_PENDING,
                    pressure_checks = state.pressure_checks,
                    "Raw-events consumer backlog high; delaying publishers until the event engine drains"
                );
            }
            tokio::time::sleep(RAW_STREAM_BACKPRESSURE_POLL_INTERVAL).await;
        }
    }

    /// Publish a automaton processing failure envelope.
    ///
    /// `transport::Class::Derived` (failure routing) — routes to the
    /// processing-failure stream (`events.processing_failures.*`), not the
    /// raw-ingest DLQ. Re-runnable via automaton replay.
    pub async fn publish_processing_failure(
        &self,
        event: &Event,
        error: &str,
        module_name: &str,
        class: transport::Class,
    ) -> RuntimeResult<()> {
        debug_assert!(
            matches!(class, transport::Class::Derived),
            "publish_processing_failure is exclusively for Class::Derived; got {class:?}"
        );
        let _permit =
            acquire_lane_permit(&self.semaphores.processing_failure, "processing failure").await?;
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
            "module": module_name,
            "original_event": original_event,
            "failed_at": sinex_primitives::temporal::format_rfc3339(sinex_primitives::temporal::now()),
        });

        let payload =
            serde_json::to_vec(&failure_entry).map_err(sinex_primitives::SinexError::from)?;
        let subject = self.env.nats_subject_with_namespace(
            self.namespace.as_deref(),
            &format!(
                "events.processing_failures.{}.{}",
                module_name.replace('.', "_"),
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
        transport::insert_semantic_transport_class_header(&mut headers, class);

        let ack_future = self
            .publish_with_headers(
                subject,
                headers,
                payload,
                "Failed to publish processing-failure message",
            )
            .await?;
        let ack = wait_for_publish_ack(ack_future, self.publish_ack_timeout).await?;

        let count = self
            .processing_failure_log_count
            .fetch_add(1, Ordering::Relaxed);
        if count.is_multiple_of(100) {
            tracing::warn!(
                event_id = %event_id,
                module = %module_name,
                error = %error,
                sequence = ack.sequence,
                skipped = count,
                "Event sent to processing-failure stream (rate-limited, logging every 100th)"
            );
        }

        Ok(())
    }

    async fn publish_with_headers(
        &self,
        subject: String,
        headers: async_nats::HeaderMap,
        payload: Vec<u8>,
        error_message: &'static str,
    ) -> RuntimeResult<async_nats::jetstream::context::PublishAckFuture> {
        ensure_nats_payload_fits(error_message, &subject, payload.len())?;

        self.js
            .publish_with_headers(subject, headers, payload.into())
            .await
            .map_err(|error| {
                sinex_primitives::SinexError::processing(error_message).with_source(error)
            })
    }

    async fn ensure_raw_events_stream_ready(&self) -> RuntimeResult<()> {
        if self.raw_events_stream_ready.load(Ordering::Acquire) {
            return Ok(());
        }

        let _bootstrap_guard = self.raw_events_stream_bootstrap_lock.lock().await;
        if self.raw_events_stream_ready.load(Ordering::Acquire) {
            return Ok(());
        }

        crate::runtime::jetstream_streams::bootstrap_raw_events_stream(
            &self.nats_client,
            self.namespace.as_deref(),
        )
        .await?;
        self.raw_events_stream_ready.store(true, Ordering::Release);
        Ok(())
    }

    async fn ensure_reflection_events_stream_ready(&self) -> RuntimeResult<()> {
        if self.reflection_events_stream_ready.load(Ordering::Acquire) {
            return Ok(());
        }

        let _bootstrap_guard = self.reflection_events_stream_bootstrap_lock.lock().await;
        if self.reflection_events_stream_ready.load(Ordering::Acquire) {
            return Ok(());
        }

        crate::runtime::jetstream_streams::bootstrap_reflection_events_stream(
            &self.nats_client,
            self.namespace.as_deref(),
        )
        .await?;
        self.reflection_events_stream_ready
            .store(true, Ordering::Release);
        Ok(())
    }
}

async fn acquire_lane_permit(
    semaphore: &Arc<Semaphore>,
    lane_label: &'static str,
) -> RuntimeResult<tokio::sync::OwnedSemaphorePermit> {
    semaphore.clone().acquire_owned().await.map_err(|error| {
        sinex_primitives::SinexError::processing(format!("{lane_label} publish semaphore closed"))
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
) -> RuntimeResult<(String, Vec<u8>)> {
    let event_id = event.id.as_ref().ok_or_else(|| {
        sinex_primitives::SinexError::processing("Event ID is required".to_string())
    })?;
    // #1570 Prong B: a material event may publish with `ts_orig = None` (deferred
    // to the event_engine admission stage, which resolves it from the source-material
    // timing tier). The wire format omits ts_orig in that case.
    let ts_orig = event.ts_orig.map(|ts| ts.format_rfc3339());
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
        ts_orig,
        host: event.host.as_str(),
        payload: &event.payload,
        module_run_id: event.module_run_id.map(|id| id.to_string()),
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
        automaton_model: event.automaton_model.map(|model| model.to_string()),
        ts_quality: event.ts_quality.map(|quality| quality.to_string()),
    };

    let encoded = serde_json::to_vec(&payload).map_err(sinex_primitives::SinexError::from)?;
    Ok((event_id_str, encoded))
}

async fn wait_for_publish_ack<T, E, F>(future: F, timeout: Duration) -> RuntimeResult<T>
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
#[path = "nats_publisher_test.rs"]
mod tests;
