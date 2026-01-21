//! JetStream event consumer with confirmations and DLQ support

use async_nats::{jetstream, Client as NatsClient};
use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sinex_core::{db::DbPool, environment::SinexEnvironment, types::ulid::Ulid, JsonValue};
use sqlx::postgres::PgPoolCopyExt;
use sqlx::QueryBuilder;
use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    validator::{EventValidator, ValidationResult},
    IngestdResult, SinexError,
};
use tokio::sync::RwLock;

#[derive(Debug, Deserialize)]
struct RawEvent {
    id: String,
    source: String,
    event_type: String,
    ts_orig: String,
    host: String,
    payload: JsonValue,
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

#[derive(Debug, Serialize)]
struct Confirmation {
    event_id: String,
    persisted: bool,
    ts_ingest: String,
}

#[derive(Debug, Serialize)]
struct DlqEntry {
    event_id: String,
    error: String,
    original_payload: JsonValue,
    failed_at: String,
}

pub struct JetStreamConsumer {
    js: jetstream::Context,
    pool: DbPool,
    validator: Arc<RwLock<EventValidator>>,
    topology: JetStreamTopology,
    ack_wait: Duration,
    max_ack_pending: i64,
    fail_once: Option<Arc<AtomicBool>>,
    post_persist_fail_once: Option<Arc<AtomicBool>>,
    confirmation_failures_remaining: Option<Arc<AtomicUsize>>,
    processing_delay: Option<Duration>,
    delivery_observer: Option<Arc<AtomicU64>>,
    stats: ConsumerStats,
    route_db_errors_to_dlq: bool,
    recent_id_cache: Mutex<RecentIdCache>,
    batch_fetch_max_messages: usize,
    batch_fetch_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct JetStreamTopology {
    pub events_stream: String,
    pub events_subject: String,
    pub confirmations_stream: String,
    pub confirmations_subject: String,
    pub confirmations_prefix: String,
    pub dlq_stream: String,
    pub dlq_subject: String,
    pub dlq_publish_subject: String,
    pub consumer_durable: String,
}

impl JetStreamTopology {
    pub fn new(
        env: &SinexEnvironment,
        base_stream: String,
        consumer_durable: String,
        namespace: Option<&str>,
    ) -> Self {
        let confirmations_stream = format!("{base_stream}_CONFIRMATIONS");
        let dlq_stream = format!("{base_stream}_DLQ");
        let namespaced = |subject: &str| env.nats_subject_with_namespace(namespace, subject);
        let confirmations_prefix = format!("{}.", namespaced("events.confirmations"));

        Self {
            events_stream: base_stream.clone(),
            events_subject: namespaced("events.raw.>"),
            confirmations_stream,
            confirmations_subject: namespaced("events.confirmations.>"),
            confirmations_prefix,
            dlq_stream,
            dlq_subject: namespaced("events.dlq.>"),
            dlq_publish_subject: namespaced("events.dlq.ingestd"),
            consumer_durable,
        }
    }
}

const DB_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const RECENT_ID_CACHE_SIZE: usize = 50_000;
const DEFAULT_BATCH_FETCH_MAX_MESSAGES: usize = 100;
const DEFAULT_BATCH_FETCH_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_MAX_ACK_PENDING: i64 = 100;
const DLQ_PUBLISH_MAX_ATTEMPTS: usize = 3;
const DLQ_PUBLISH_BACKOFF_BASE: Duration = Duration::from_millis(200);
const DLQ_PUBLISH_BACKOFF_MAX: Duration = Duration::from_secs(2);
const DLQ_RETRY_DELAY: Duration = Duration::from_secs(1);
const CONFIRM_PUBLISH_MAX_ATTEMPTS: usize = 3;
const CONFIRM_PUBLISH_BACKOFF_BASE: Duration = Duration::from_millis(200);
const CONFIRM_PUBLISH_BACKOFF_MAX: Duration = Duration::from_secs(2);
const CONFIRM_RETRY_DELAY: Duration = Duration::from_secs(1);
const STREAM_CAPACITY_WARNING_THRESHOLD: f64 = 0.8; // Alert at 80% capacity
const STREAM_CAPACITY_CHECK_INTERVAL: Duration = Duration::from_secs(300); // Check every 5 minutes

#[derive(Debug)]
struct PersistBatchResult {
    inserted_ids: Option<Vec<Ulid>>,
}

#[derive(Debug)]
struct RecentIdCache {
    capacity: usize,
    order: VecDeque<Ulid>,
    set: HashSet<Ulid>,
}

impl RecentIdCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            set: HashSet::with_capacity(capacity),
        }
    }

    fn contains(&self, id: &Ulid) -> bool {
        if self.capacity == 0 {
            return false;
        }
        self.set.contains(id)
    }

    fn insert(&mut self, id: Ulid) {
        if self.capacity == 0 {
            return;
        }
        if self.set.insert(id) {
            self.order.push_back(id);
            while self.order.len() > self.capacity {
                if let Some(evicted) = self.order.pop_front() {
                    self.set.remove(&evicted);
                }
            }
        }
    }
}

struct PreparedEvent {
    raw: RawEvent,
    parsed_id: sinex_core::types::ulid::Ulid,
    parsed_ts: DateTime<Utc>,
    source_material_id: Option<Uuid>,
    anchor_byte: Option<i64>,
    offset_start: Option<i64>,
    offset_end: Option<i64>,
    offset_kind: Option<String>,
    source_event_ids: Option<Vec<Uuid>>,
    payload_schema_id: Option<Uuid>,
    ingestor_version: Option<String>,
    associated_blob_ids: Option<Vec<Uuid>>,
    message: jetstream::Message,
}

enum PreparedProvenance {
    Material {
        source_material_id: Uuid,
        anchor_byte: i64,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
        offset_kind: String,
    },
    Synthesis {
        source_event_ids: Vec<Uuid>,
    },
}

#[derive(Debug, Default)]
struct ConsumerStats {
    events_processed: AtomicU64,
    events_failed: AtomicU64,
    validation_failures: AtomicU64,
    dlq_routed: AtomicU64,
    confirmation_failures: AtomicU64,
    dlq_publish_failures: AtomicU64,
    nack_failures: AtomicU64,
}

impl ConsumerStats {
    fn log(&self) {
        info!(
            events_processed = self.events_processed.load(Ordering::Relaxed),
            events_failed = self.events_failed.load(Ordering::Relaxed),
            validation_failures = self.validation_failures.load(Ordering::Relaxed),
            dlq_routed = self.dlq_routed.load(Ordering::Relaxed),
            confirmation_failures = self.confirmation_failures.load(Ordering::Relaxed),
            dlq_publish_failures = self.dlq_publish_failures.load(Ordering::Relaxed),
            nack_failures = self.nack_failures.load(Ordering::Relaxed),
            "JetStream consumer stats"
        );
    }
}

impl JetStreamConsumer {
    pub fn new(
        nats_client: NatsClient,
        pool: DbPool,
        validator: Arc<RwLock<EventValidator>>,
        topology: JetStreamTopology,
    ) -> Self {
        let js = jetstream::new(nats_client);

        Self {
            js,
            pool,
            validator,
            topology,
            ack_wait: Duration::from_secs(30),
            max_ack_pending: DEFAULT_MAX_ACK_PENDING,
            fail_once: None,
            post_persist_fail_once: None,
            confirmation_failures_remaining: None,
            processing_delay: None,
            delivery_observer: None,
            stats: ConsumerStats::default(),
            route_db_errors_to_dlq: false,
            recent_id_cache: Mutex::new(RecentIdCache::new(RECENT_ID_CACHE_SIZE)),
            batch_fetch_max_messages: DEFAULT_BATCH_FETCH_MAX_MESSAGES,
            batch_fetch_timeout: DEFAULT_BATCH_FETCH_TIMEOUT,
        }
    }

    /// Build a consumer with a custom AckWait (primarily for tests).
    pub fn with_ack_wait(
        nats_client: NatsClient,
        pool: DbPool,
        validator: Arc<RwLock<EventValidator>>,
        topology: JetStreamTopology,
        ack_wait: Duration,
    ) -> Self {
        let mut consumer = Self::new(nats_client, pool, validator, topology);
        consumer.ack_wait = ack_wait;
        consumer
    }

    /// Override the JetStream batch fetch behavior (max messages per pull and expiration timeout).
    pub fn with_batch_fetch_config(mut self, max_messages: usize, timeout: Duration) -> Self {
        self.batch_fetch_max_messages = max_messages.max(1);
        self.batch_fetch_timeout = timeout;
        self
    }

    /// Override the maximum unacknowledged messages for the consumer.
    pub fn with_max_ack_pending(mut self, max_ack_pending: i64) -> Self {
        self.max_ack_pending = max_ack_pending.max(1);
        self
    }

    /// Build a consumer that will fail once before proceeding (test-only hook).
    pub fn with_ack_wait_and_fail_once(
        nats_client: NatsClient,
        pool: DbPool,
        validator: Arc<RwLock<EventValidator>>,
        topology: JetStreamTopology,
        ack_wait: Duration,
        fail_once: Arc<AtomicBool>,
    ) -> Self {
        Self::with_test_hooks(
            nats_client,
            pool,
            validator,
            topology,
            ack_wait,
            Some(fail_once),
            None,
            None,
            false,
            None,
        )
    }

    /// Build a consumer with optional test-only hooks.
    pub fn with_test_hooks(
        nats_client: NatsClient,
        pool: DbPool,
        validator: Arc<RwLock<EventValidator>>,
        topology: JetStreamTopology,
        ack_wait: Duration,
        fail_once: Option<Arc<AtomicBool>>,
        processing_delay: Option<Duration>,
        delivery_observer: Option<Arc<AtomicU64>>,
        route_db_errors_to_dlq: bool,
        confirmation_failures_remaining: Option<Arc<AtomicUsize>>,
    ) -> Self {
        let mut consumer = Self::with_ack_wait(nats_client, pool, validator, topology, ack_wait);
        consumer.fail_once = fail_once;
        consumer.processing_delay = processing_delay;
        consumer.delivery_observer = delivery_observer;
        consumer.route_db_errors_to_dlq = route_db_errors_to_dlq;
        consumer.confirmation_failures_remaining = confirmation_failures_remaining;
        consumer
    }

    /// Bootstrap all required JetStream streams
    async fn bootstrap_streams(&self) -> IngestdResult<()> {
        info!("Bootstrapping JetStream streams");

        // Events stream - durable event log for automata replay
        // 90 days retention to support full operational history replay
        let events_stream = self.topology.events_stream.clone();
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: events_stream.clone(),
                subjects: vec![self.topology.events_subject.clone()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 10_000_000,
                max_age: Duration::from_secs(90 * 24 * 60 * 60), // 90 days (operational history)
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create events stream: {}", e)))?;

        // Confirmations stream with compaction - only keep latest per event
        // Short retention since confirmations are ephemeral operational state
        let confirmations_stream = self.topology.confirmations_stream.clone();
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: confirmations_stream.clone(),
                subjects: vec![self.topology.confirmations_subject.clone()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages_per_subject: 1, // Compaction: only keep latest confirmation
                max_age: Duration::from_secs(7 * 24 * 60 * 60), // 7 days (operational buffer)
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network(format!("Failed to create confirmations stream: {}", e))
            })?;

        // DLQ stream
        let dlq_stream = self.topology.dlq_stream.clone();
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: dlq_stream.clone(),
                subjects: vec![self.topology.dlq_subject.clone()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 1_000_000,
                max_age: Duration::from_secs(30 * 24 * 60 * 60), // 30 days
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create DLQ stream: {}", e)))?;

        info!("JetStream streams bootstrapped successfully");
        Ok(())
    }

    pub async fn run(self) -> IngestdResult<()> {
        info!("Starting JetStream consumer");

        // Bootstrap streams
        self.bootstrap_streams().await?;

        // Get events stream
        let stream_name = self.topology.events_stream.clone();
        let stream = self
            .js
            .get_stream(&stream_name)
            .await
            .map_err(|e| SinexError::network(format!("Failed to get stream: {}", e)))?;

        // Create durable consumer
        let consumer = stream
            .get_or_create_consumer(
                &self.topology.consumer_durable,
                jetstream::consumer::pull::Config {
                    durable_name: Some(self.topology.consumer_durable.clone()),
                    deliver_policy: jetstream::consumer::DeliverPolicy::All,
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: self.ack_wait,
                    max_deliver: 10,
                    max_ack_pending: self.max_ack_pending,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| SinexError::network(format!("Failed to create consumer: {}", e)))?;

        // Stats logging interval
        let mut stats_interval = tokio::time::interval(Duration::from_secs(60));
        // Stream capacity monitoring interval
        let mut capacity_check_interval = tokio::time::interval(STREAM_CAPACITY_CHECK_INTERVAL);

        loop {
            tokio::select! {
                _ = stats_interval.tick() => {
                    self.stats.log();
                }
                _ = capacity_check_interval.tick() => {
                    self.check_stream_capacity(&stream_name).await;
                }
                batch_result = self.process_batch(&consumer) => {
                    if let Err(e) = batch_result {
                        error!("Batch processing error: {}", e);
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip(self, consumer), fields(consumer_name = %self.topology.consumer_durable))]
    async fn process_batch(
        &self,
        consumer: &jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
    ) -> IngestdResult<()> {
        let mut messages = consumer
            .batch()
            .max_messages(self.batch_fetch_max_messages)
            .expires(self.batch_fetch_timeout)
            .messages()
            .await
            .map_err(|e| SinexError::network(format!("Failed to fetch messages: {}", e)))?;
        let mut batch = Vec::new();

        while let Some(msg) = messages.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    error!("Error receiving message: {}", e);
                    continue;
                }
            };

            if let Some(counter) = &self.delivery_observer {
                counter.fetch_add(1, Ordering::Relaxed);
            }

            if let Some(prepared) = self.prepare_event(msg).await? {
                batch.push(prepared);
            }
        }

        if batch.is_empty() {
            return Ok(());
        }

        self.persist_and_confirm_batch(&batch).await
    }

    async fn prepare_event(&self, msg: jetstream::Message) -> IngestdResult<Option<PreparedEvent>> {
        // Parse event
        let raw_event: RawEvent = match serde_json::from_slice(&msg.payload) {
            Ok(e) => e,
            Err(e) => {
                error!(event_id = ?msg.headers, "Failed to parse event: {}", e);
                self.route_validation_failure(&msg, format!("Parse error: {}", e))
                    .await?;
                return Ok(None);
            }
        };

        // Validate event using EventValidator
        if let Err(e) = self.validate_event(&raw_event).await {
            warn!(event_id = %raw_event.id, "Event validation failed: {}", e);
            self.route_validation_failure(&msg, format!("Validation failed: {}", e))
                .await?;
            return Ok(None);
        }

        // Parse timestamp early to isolate poison messages
        let parsed_ts = match raw_event.ts_orig.parse::<DateTime<Utc>>() {
            Ok(ts) => ts,
            Err(e) => {
                error!(
                    event_id = %raw_event.id,
                    ts = %raw_event.ts_orig,
                    "Invalid timestamp; routing to DLQ: {}",
                    e
                );
                self.route_validation_failure(&msg, format!("Invalid timestamp: {}", e))
                    .await?;
                return Ok(None);
            }
        };

        let parsed_id = match Ulid::from_str(&raw_event.id) {
            Ok(id) => id,
            Err(e) => {
                error!(event_id = %raw_event.id, "Invalid ULID; routing to DLQ: {}", e);
                self.route_validation_failure(&msg, format!("Invalid ULID: {}", e))
                    .await?;
                return Ok(None);
            }
        };

        let payload_schema_id = match parse_optional_uuid(
            raw_event.payload_schema_id.as_deref(),
            "payload_schema_id",
        ) {
            Ok(value) => value,
            Err(error) => {
                self.route_validation_failure(&msg, error).await?;
                return Ok(None);
            }
        };

        let associated_blob_ids = match parse_uuid_list(
            raw_event.associated_blob_ids.as_ref(),
            "associated_blob_ids",
        ) {
            Ok(value) => value,
            Err(error) => {
                self.route_validation_failure(&msg, error).await?;
                return Ok(None);
            }
        };

        let provenance = match (
            raw_event.source_material_id.as_deref(),
            raw_event.source_event_ids.as_ref(),
        ) {
            (Some(_), Some(_)) => {
                Err("Event cannot provide both source_material_id and source_event_ids".to_string())
            }
            (Some(material_id), None) => {
                match parse_ulid_value(material_id, "source_material_id") {
                    Ok(uuid) => match normalize_offset_kind(raw_event.offset_kind.as_deref()) {
                        Ok(offset_kind) => Ok(PreparedProvenance::Material {
                            source_material_id: uuid,
                            anchor_byte: raw_event.anchor_byte.unwrap_or(0),
                            offset_start: raw_event.offset_start,
                            offset_end: raw_event.offset_end,
                            offset_kind,
                        }),
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                }
            }
            (None, Some(source_ids)) if !source_ids.is_empty() => {
                match parse_ulid_slice(source_ids, "source_event_ids") {
                    Ok(parsed) => Ok(PreparedProvenance::Synthesis {
                        source_event_ids: parsed,
                    }),
                    Err(error) => Err(error),
                }
            }
            (None, Some(_)) => Err("source_event_ids must contain at least one entry".to_string()),
            (None, None) => {
                warn!(event_id = %raw_event.id, "Event missing provenance metadata; assuming self-referential synthesis");
                Ok(PreparedProvenance::Synthesis {
                    source_event_ids: vec![parsed_id.as_uuid()],
                })
            }
        };

        let provenance = match provenance {
            Ok(provenance) => provenance,
            Err(error) => {
                self.route_validation_failure(&msg, error).await?;
                return Ok(None);
            }
        };

        let (
            source_material_id,
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind,
            source_event_ids,
        ) = match provenance {
            PreparedProvenance::Material {
                source_material_id,
                anchor_byte,
                offset_start,
                offset_end,
                offset_kind,
            } => (
                Some(source_material_id),
                Some(anchor_byte),
                offset_start,
                offset_end,
                Some(offset_kind),
                None,
            ),
            PreparedProvenance::Synthesis { source_event_ids } => {
                (None, None, None, None, None, Some(source_event_ids))
            }
        };

        let ingestor_version = raw_event.ingestor_version.clone();

        Ok(Some(PreparedEvent {
            raw: raw_event,
            parsed_id,
            parsed_ts,
            source_material_id,
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind,
            source_event_ids,
            payload_schema_id,
            ingestor_version,
            associated_blob_ids,
            message: msg,
        }))
    }

    #[tracing::instrument(skip(self, batch), fields(batch_size = batch.len()))]
    async fn persist_and_confirm_batch(&self, batch: &[PreparedEvent]) -> IngestdResult<()> {
        match self.persist_batch_optimized(batch).await {
            Ok(persisted) => {
                let persisted_set = persisted
                    .inserted_ids
                    .as_ref()
                    .map(|ids| ids.iter().cloned().collect::<HashSet<_>>());
                if let Some(fail_flag) = &self.post_persist_fail_once {
                    if fail_flag.swap(false, Ordering::SeqCst) {
                        return Err(SinexError::database("forced post-persist failure"));
                    }
                }
                if let Some(delay) = self.processing_delay {
                    tokio::time::sleep(delay).await;
                }
                // Publish confirmations for every message in the batch to guarantee downstream delivery
                let mut confirmation_error: Option<SinexError> = None;
                for prepared in batch {
                    match self
                        .publish_confirmation_with_retry(&prepared.parsed_id)
                        .await
                    {
                        Ok(()) => {
                            if let Some(set) = &persisted_set {
                                if !set.contains(&prepared.parsed_id) {
                                    debug!(
                                        event_id = %prepared.parsed_id,
                                        "Re-published confirmation for already persisted event"
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            warn!(
                                event_id = %prepared.parsed_id,
                                error = %err,
                                "Failed to publish confirmation after retries"
                            );
                            self.stats
                                .confirmation_failures
                                .fetch_add(1, Ordering::Relaxed);
                            confirmation_error = Some(err);
                            break;
                        }
                    }
                }

                if confirmation_error.is_some() {
                    for prepared in batch {
                        if let Err(err) = prepared
                            .message
                            .ack_with(jetstream::AckKind::Nak(Some(CONFIRM_RETRY_DELAY)))
                            .await
                        {
                            warn!(
                                event_id = %prepared.parsed_id,
                                error = %err,
                                "Failed to NAK after confirmation publish failure"
                            );
                            self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    self.stats
                        .events_failed
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                    return Ok(());
                }

                // ACK all messages
                for prepared in batch {
                    prepared
                        .message
                        .ack()
                        .await
                        .map_err(|e| SinexError::network(format!("Failed to ack: {}", e)))?;
                }

                self.stats
                    .events_processed
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
                info!("Processed and confirmed {} events", batch.len());
            }
            Err(e) => {
                error!("Failed to persist batch: {}", e);
                if self.route_db_errors_to_dlq {
                    for prepared in batch {
                        if let Err(err) = self
                            .route_to_dlq_and_ack(
                                &prepared.message,
                                format!("Persistence error: {}", e),
                            )
                            .await
                        {
                            warn!(
                                event_id = %prepared.parsed_id,
                                error = %err,
                                "Failed to route persistence error to DLQ"
                            );
                        }
                    }
                } else {
                    for prepared in batch {
                        if let Err(err) = prepared
                            .message
                            .ack_with(jetstream::AckKind::Nak(None))
                            .await
                        {
                            warn!(
                                event_id = %prepared.parsed_id,
                                error = %err,
                                "Failed to NAK after persistence failure"
                            );
                            self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                self.stats
                    .events_failed
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    async fn route_validation_failure(
        &self,
        msg: &jetstream::Message,
        error: String,
    ) -> IngestdResult<()> {
        self.route_to_dlq_and_ack(msg, error).await?;
        self.stats
            .validation_failures
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Validate event against JSON schema
    async fn validate_event(&self, event: &RawEvent) -> IngestdResult<()> {
        if event.id.is_empty() {
            return Err(SinexError::validation("Event ID cannot be empty"));
        }
        if event.source.is_empty() {
            return Err(SinexError::validation("Event source cannot be empty"));
        }
        if event.event_type.is_empty() {
            return Err(SinexError::validation("Event type cannot be empty"));
        }

        let guard = self.validator.read().await;
        let validation =
            guard.validate_payload_for(&event.source, &event.event_type, &event.payload);

        match validation {
            ValidationResult::Valid | ValidationResult::Skipped | ValidationResult::NoSchema => {
                Ok(())
            }
            ValidationResult::SchemaNotFound { schema_id } => {
                warn!(
                    source = %event.source,
                    event_type = %event.event_type,
                    schema = %schema_id,
                    "Schema referenced in lookup was not found; accepting event"
                );
                Ok(())
            }
            ValidationResult::Invalid { errors } => Err(SinexError::validation(format!(
                "Schema validation failed: {}",
                errors.join(", ")
            ))
            .with_operation("jetstream_consumer.validate_event")),
        }
    }

    /// Persist batch using COPY with an ON CONFLICT fallback for duplicates.
    #[tracing::instrument(skip(self, batch), fields(batch_size = batch.len()))]
    async fn persist_batch_optimized(
        &self,
        batch: &[PreparedEvent],
    ) -> IngestdResult<PersistBatchResult> {
        if batch.is_empty() {
            return Ok(PersistBatchResult { inserted_ids: None });
        }

        if let Some(fail_flag) = &self.fail_once {
            if fail_flag.swap(false, Ordering::SeqCst) {
                return Err(SinexError::database("forced transient failure"));
            }
        }

        let to_persist = self.filter_cached_batch(batch);
        if to_persist.is_empty() {
            return Ok(PersistBatchResult { inserted_ids: None });
        }

        let copy_attempt = timeout(DB_WRITE_TIMEOUT, self.persist_batch_copy(&to_persist)).await;
        match copy_attempt {
            Ok(Ok(_rows)) => {
                self.remember_batch(batch);
                Ok(PersistBatchResult { inserted_ids: None })
            }
            Ok(Err(err)) => {
                if is_unique_violation(&err) {
                    warn!(
                        batch_size = to_persist.len(),
                        "COPY insert hit duplicate IDs; falling back to INSERT ... ON CONFLICT"
                    );
                    let inserted_ids = self.persist_batch_insert_on_conflict(&to_persist).await?;
                    self.remember_batch(batch);
                    Ok(PersistBatchResult {
                        inserted_ids: Some(inserted_ids),
                    })
                } else {
                    error!("Failed to persist events batch: {}", err);
                    Err(SinexError::database(format!(
                        "Failed to persist events batch: {}",
                        err
                    )))
                }
            }
            Err(_) => {
                error!(
                    batch_size = to_persist.len(),
                    timeout_seconds = DB_WRITE_TIMEOUT.as_secs(),
                    "Timed out waiting for batch insert to complete"
                );
                Err(SinexError::database(format!(
                    "Persisting batch timed out after {:?}",
                    DB_WRITE_TIMEOUT
                )))
            }
        }
    }

    fn filter_cached_batch<'a>(&self, batch: &'a [PreparedEvent]) -> Vec<&'a PreparedEvent> {
        let cache = self.recent_id_cache.lock().unwrap_or_else(|poisoned| {
            warn!(
                "Recent ID cache mutex was poisoned; recovering with potentially inconsistent data"
            );
            poisoned.into_inner()
        });
        let mut seen = HashSet::new();
        batch
            .iter()
            .filter(|event| {
                if cache.contains(&event.parsed_id) {
                    return false;
                }
                seen.insert(event.parsed_id)
            })
            .collect()
    }

    fn remember_batch(&self, batch: &[PreparedEvent]) {
        let mut cache = self.recent_id_cache.lock().unwrap_or_else(|poisoned| {
            warn!(
                "Recent ID cache mutex was poisoned; recovering with potentially inconsistent data"
            );
            poisoned.into_inner()
        });
        for event in batch {
            cache.insert(event.parsed_id);
        }
    }

    async fn persist_batch_copy(&self, batch: &[&PreparedEvent]) -> Result<u64, sqlx::Error> {
        let mut copy = self
            .pool
            .copy_in_raw(
                "COPY core.events \
                (id, source, event_type, ts_orig, ts_orig_subnano, host, payload, source_material_id, \
                 anchor_byte, offset_start, offset_end, offset_kind, source_event_ids, \
                 payload_schema_id, ingestor_version, associated_blob_ids) \
                 FROM STDIN WITH (FORMAT text)",
            )
            .await?;

        let mut row_buf = Vec::with_capacity(1024);
        for prepared in batch {
            row_buf.clear();
            if let Err(err) = write_copy_row(&mut row_buf, prepared) {
                let _ = copy.abort("COPY row encoding failed").await;
                return Err(err);
            }
            if let Err(err) = copy.send(row_buf.as_slice()).await {
                let _ = copy.abort("COPY row send failed").await;
                return Err(err);
            }
        }

        copy.finish().await
    }

    async fn persist_batch_insert_on_conflict(
        &self,
        batch: &[&PreparedEvent],
    ) -> IngestdResult<Vec<Ulid>> {
        let mut builder = QueryBuilder::new(
            "INSERT INTO core.events (id, source, event_type, ts_orig, ts_orig_subnano, host, payload, \
             source_material_id, anchor_byte, offset_start, offset_end, offset_kind, \
             source_event_ids, payload_schema_id, ingestor_version, associated_blob_ids) ",
        );
        builder.push("VALUES ");
        for (idx, prepared) in batch.iter().enumerate() {
            let prepared = *prepared;
            if idx > 0 {
                builder.push(", ");
            }
            builder.push("(");
            let ts_orig_subnano = (prepared.parsed_ts.nanosecond() % 1_000) as i32;
            builder.push_bind(prepared.parsed_id.as_uuid());
            builder.push(", ");
            builder.push_bind(prepared.raw.source.as_str());
            builder.push(", ");
            builder.push_bind(prepared.raw.event_type.as_str());
            builder.push(", ");
            builder.push_bind(prepared.parsed_ts);
            builder.push(", ");
            builder.push_bind(ts_orig_subnano);
            builder.push(", ");
            builder.push_bind(prepared.raw.host.as_str());
            builder.push(", ");
            builder.push_bind(prepared.raw.payload.clone());
            builder.push(", ");
            builder.push_bind(prepared.source_material_id);
            builder.push(", ");
            builder.push_bind(prepared.anchor_byte);
            builder.push(", ");
            builder.push_bind(prepared.offset_start);
            builder.push(", ");
            builder.push_bind(prepared.offset_end);
            builder.push(", ");
            builder.push_bind(prepared.offset_kind.as_deref());
            builder.push(", ");
            builder.push_bind(prepared.source_event_ids.as_deref());
            builder.push(", ");
            builder.push_bind(prepared.payload_schema_id);
            builder.push(", ");
            builder.push_bind(prepared.ingestor_version.as_deref());
            builder.push(", ");
            builder.push_bind(prepared.associated_blob_ids.as_deref());
            builder.push(")");
        }

        builder.push(" ON CONFLICT (id) DO NOTHING RETURNING id::uuid as \"id!\"");

        let rows = timeout(
            DB_WRITE_TIMEOUT,
            builder.build_query_as::<(Uuid,)>().fetch_all(&self.pool),
        )
        .await
        .map_err(|_| {
            error!(
                batch_size = batch.len(),
                timeout_seconds = DB_WRITE_TIMEOUT.as_secs(),
                "Timed out waiting for batch insert to complete"
            );
            SinexError::database(format!(
                "Persisting batch timed out after {:?}",
                DB_WRITE_TIMEOUT
            ))
        })?
        .map_err(|err| {
            error!("Failed to persist events batch: {}", err);
            err
        })?;

        Ok(rows.into_iter().map(|row| Ulid::from(row.0)).collect())
    }

    /// Publish confirmation to NATS
    async fn publish_confirmation(&self, event_id: &Ulid) -> IngestdResult<()> {
        if let Some(failures) = &self.confirmation_failures_remaining {
            if failures.load(Ordering::SeqCst) > 0 {
                failures.fetch_sub(1, Ordering::SeqCst);
                return Err(SinexError::network("forced confirmation publish failure"));
            }
        }

        let event_id_str = event_id.to_string();
        let confirmation = Confirmation {
            event_id: event_id_str.clone(),
            persisted: true,
            ts_ingest: Utc::now().to_rfc3339(),
        };

        let subject = format!("{}{}", self.topology.confirmations_prefix, event_id_str);
        let payload = serde_json::to_vec(&confirmation)?;

        // Add idempotency header
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id_str.as_str());

        self.js
            .publish_with_headers(subject, headers, payload.into())
            .await
            .map_err(|e| SinexError::network(format!("Failed to publish confirmation: {}", e)))?
            .await
            .map_err(|e| SinexError::network(format!("Confirmation ack failed: {}", e)))?;

        debug!(event_id = %event_id, "Published confirmation");
        Ok(())
    }

    async fn publish_confirmation_with_retry(&self, event_id: &Ulid) -> IngestdResult<()> {
        let mut backoff = CONFIRM_PUBLISH_BACKOFF_BASE;
        let mut last_error: Option<SinexError> = None;

        for attempt in 1..=CONFIRM_PUBLISH_MAX_ATTEMPTS {
            match self.publish_confirmation(event_id).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    warn!(
                        attempt,
                        event_id = %event_id,
                        error = %err,
                        "Confirmation publish attempt failed"
                    );
                    last_error = Some(err);
                }
            }

            if attempt < CONFIRM_PUBLISH_MAX_ATTEMPTS {
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff.saturating_mul(2), CONFIRM_PUBLISH_BACKOFF_MAX);
            }
        }

        Err(last_error
            .unwrap_or_else(|| SinexError::network("Failed to publish confirmation after retries")))
    }

    /// Route failed message to DLQ. Returns true when publish + ack succeeds.
    #[tracing::instrument(skip(self, msg), fields(error = %error))]
    async fn route_to_dlq(&self, msg: &jetstream::Message, error: String) -> bool {
        let original_payload = match serde_json::from_slice(&msg.payload) {
            Ok(json) => json,
            Err(parse_err) => {
                warn!(
                    error = %parse_err,
                    payload_len = msg.payload.len(),
                    "Failed to parse original payload for DLQ entry; preserving raw bytes as base64"
                );
                serde_json::json!({
                    "_parse_error": parse_err.to_string(),
                    "_raw_bytes_base64": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &msg.payload)
                })
            }
        };

        let dlq_entry = DlqEntry {
            event_id: msg
                .headers
                .as_ref()
                .and_then(|h| h.get("Nats-Msg-Id"))
                .map(|v| v.as_str().to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            error,
            original_payload,
            failed_at: Utc::now().to_rfc3339(),
        };

        let payload = match serde_json::to_vec(&dlq_entry) {
            Ok(payload) => payload,
            Err(err) => {
                error!("Failed to serialize DLQ entry: {}", err);
                return false;
            }
        };

        let mut backoff = DLQ_PUBLISH_BACKOFF_BASE;
        for attempt in 1..=DLQ_PUBLISH_MAX_ATTEMPTS {
            match self
                .js
                .publish(
                    self.topology.dlq_publish_subject.clone(),
                    payload.clone().into(),
                )
                .await
            {
                Ok(ack) => match ack.await {
                    Ok(_) => {
                        debug!(event_id = %dlq_entry.event_id, "Routed to DLQ");
                        return true;
                    }
                    Err(err) => {
                        error!(attempt, error = %err, "Failed to confirm DLQ publish");
                    }
                },
                Err(err) => {
                    error!(attempt, error = %err, "Failed to route to DLQ");
                }
            }

            if attempt < DLQ_PUBLISH_MAX_ATTEMPTS {
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff.saturating_mul(2), DLQ_PUBLISH_BACKOFF_MAX);
            }
        }

        false
    }

    async fn route_to_dlq_and_ack(
        &self,
        msg: &jetstream::Message,
        error: String,
    ) -> IngestdResult<()> {
        if self.route_to_dlq(msg, error).await {
            msg.ack()
                .await
                .map_err(|e| SinexError::network(format!("Failed to ack: {}", e)))?;
            self.stats.dlq_routed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .dlq_publish_failures
                .fetch_add(1, Ordering::Relaxed);
            msg.ack_with(jetstream::AckKind::Nak(Some(DLQ_RETRY_DELAY)))
                .await
                .map_err(|e| {
                    self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                    SinexError::network(format!("Failed to NAK after DLQ failure: {}", e))
                })?;
        }
        Ok(())
    }

    /// Check stream capacity and log warnings if approaching limits
    async fn check_stream_capacity(&self, stream_name: &str) {
        match self.js.get_stream(stream_name).await {
            Ok(mut stream) => {
                if let Ok(info) = stream.info().await {
                    let state = info.state.clone();
                    let config = info.config.clone();

                    // Check message count capacity
                    if config.max_messages > 0 {
                        let usage_ratio = state.messages as f64 / config.max_messages as f64;
                        if usage_ratio >= STREAM_CAPACITY_WARNING_THRESHOLD {
                            warn!(
                                stream = %stream_name,
                                messages = state.messages,
                                max_messages = config.max_messages,
                                usage_percent = format!("{:.1}%", usage_ratio * 100.0),
                                "Stream approaching message capacity limit"
                            );
                        }
                    }

                    // Check byte capacity if configured
                    if config.max_bytes > 0 {
                        let bytes_ratio = state.bytes as f64 / config.max_bytes as f64;
                        if bytes_ratio >= STREAM_CAPACITY_WARNING_THRESHOLD {
                            warn!(
                                stream = %stream_name,
                                bytes = state.bytes,
                                max_bytes = config.max_bytes,
                                usage_percent = format!("{:.1}%", bytes_ratio * 100.0),
                                "Stream approaching byte capacity limit"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                debug!("Failed to check stream capacity for {}: {}", stream_name, e);
            }
        }
    }
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().as_deref() == Some("23505"),
        _ => false,
    }
}

fn write_copy_row(buf: &mut Vec<u8>, prepared: &PreparedEvent) -> Result<(), sqlx::Error> {
    let id = prepared.parsed_id.to_string();
    let ts_orig = prepared
        .parsed_ts
        .to_rfc3339_opts(SecondsFormat::Micros, true);
    let ts_orig_subnano = (prepared.parsed_ts.nanosecond() % 1_000) as i32;
    let payload = serde_json::to_string(&prepared.raw.payload).map_err(|err| {
        sqlx::Error::Protocol(format!("Failed to serialize payload for COPY: {err}"))
    })?;
    let source_material_id = format_ulid_optional(prepared.source_material_id);
    let payload_schema_id = format_ulid_optional(prepared.payload_schema_id);
    let source_event_ids = format_ulid_array(prepared.source_event_ids.as_deref());
    let associated_blob_ids = format_ulid_array(prepared.associated_blob_ids.as_deref());

    push_copy_field(buf, Some(&id));
    buf.push(b'\t');
    push_copy_field(buf, Some(prepared.raw.source.as_str()));
    buf.push(b'\t');
    push_copy_field(buf, Some(prepared.raw.event_type.as_str()));
    buf.push(b'\t');
    push_copy_field(buf, Some(&ts_orig));
    buf.push(b'\t');
    push_copy_i64_field(buf, Some(ts_orig_subnano as i64));
    buf.push(b'\t');
    push_copy_field(buf, Some(prepared.raw.host.as_str()));
    buf.push(b'\t');
    push_copy_field(buf, Some(&payload));
    buf.push(b'\t');
    push_copy_field(buf, source_material_id.as_deref());
    buf.push(b'\t');
    push_copy_i64_field(buf, prepared.anchor_byte);
    buf.push(b'\t');
    push_copy_i64_field(buf, prepared.offset_start);
    buf.push(b'\t');
    push_copy_i64_field(buf, prepared.offset_end);
    buf.push(b'\t');
    push_copy_field(buf, prepared.offset_kind.as_deref());
    buf.push(b'\t');
    push_copy_field(buf, source_event_ids.as_deref());
    buf.push(b'\t');
    push_copy_field(buf, payload_schema_id.as_deref());
    buf.push(b'\t');
    push_copy_field(buf, prepared.ingestor_version.as_deref());
    buf.push(b'\t');
    push_copy_field(buf, associated_blob_ids.as_deref());
    buf.push(b'\n');

    Ok(())
}

fn push_copy_field(buf: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => escape_copy_text(buf, value),
        None => buf.extend_from_slice(b"\\N"),
    }
}

fn push_copy_i64_field(buf: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => buf.extend_from_slice(value.to_string().as_bytes()),
        None => buf.extend_from_slice(b"\\N"),
    }
}

fn escape_copy_text(buf: &mut Vec<u8>, value: &str) {
    for byte in value.as_bytes() {
        match byte {
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            0x08 => buf.extend_from_slice(b"\\b"),
            0x0c => buf.extend_from_slice(b"\\f"),
            _ => buf.push(*byte),
        }
    }
}

fn format_ulid_optional(value: Option<Uuid>) -> Option<String> {
    value.map(|id| Ulid::from_uuid(id).to_string())
}

fn format_ulid_array(values: Option<&[Uuid]>) -> Option<String> {
    let values = values?;
    if values.is_empty() {
        return Some("{}".to_string());
    }
    let mut out = String::new();
    out.push('{');
    for (idx, id) in values.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&Ulid::from_uuid(*id).to_string());
    }
    out.push('}');
    Some(out)
}

fn parse_optional_uuid(value: Option<&str>, field: &str) -> Result<Option<Uuid>, String> {
    match value {
        Some(raw) => parse_ulid_value(raw, field).map(Some),
        None => Ok(None),
    }
}

fn parse_uuid_list(values: Option<&Vec<String>>, field: &str) -> Result<Option<Vec<Uuid>>, String> {
    match values {
        Some(list) => {
            if list.is_empty() {
                return Ok(Some(Vec::new()));
            }
            parse_ulid_slice(list, field).map(Some)
        }
        None => Ok(None),
    }
}

fn parse_ulid_slice(values: &[String], field: &str) -> Result<Vec<Uuid>, String> {
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        parsed.push(parse_ulid_value(value, field)?);
    }
    Ok(parsed)
}

fn parse_ulid_value(value: &str, field: &str) -> Result<Uuid, String> {
    Ulid::from_str(value)
        .map(|id| id.to_uuid())
        .map_err(|e| format!("Invalid {} '{}': {}", field, value, e))
}

fn normalize_offset_kind(value: Option<&str>) -> Result<String, String> {
    let normalized = value
        .map(|kind| kind.to_ascii_lowercase())
        .unwrap_or_else(|| "byte".to_string());
    match normalized.as_str() {
        "byte" | "line" | "rowid" | "logical" => Ok(normalized),
        other => Err(format!("Invalid offset_kind: {}", other)),
    }
}
