//! `JetStream` event consumer with confirmations and DLQ support
//!
//! See `crate::docs::ingestion_pipeline` for architectural details.

use async_nats::{jetstream, Client as NatsClient};
use futures::StreamExt;
use serde::Serialize;
use sinex_db::repositories::StreamBatchRow;
use sinex_db::{repositories::DbPoolExt, DbPool};
use sinex_primitives::Timestamp;
use sinex_primitives::{environment::SinexEnvironment, ulid::Ulid, JsonValue};
use sqlx::postgres::PgPoolCopyExt;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, warn};

use crate::{
    validator::{EventValidator, ValidationResult},
    IngestdResult, SinexError,
};
use sinex_db::postgres_copy::ToPostgresCopy;
use sinex_primitives::events::Event;
use tokio::sync::RwLock;

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
    #[must_use]
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
            events_stream: base_stream,
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
const STREAM_CAPACITY_CHECK_INTERVAL: Duration = Duration::from_mins(5); // Check every 5 minutes

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
    event: Event<JsonValue>,
    parsed_id: Ulid,
    message: jetstream::Message,
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
    nats_errors: AtomicU64,
}

impl ConsumerStats {
    fn log(&self) {
        info!(
            events_processed = self.events_processed.load(Ordering::Relaxed),
            events_failed = self.events_failed.load(Ordering::Relaxed),
            validation_failures = self.validation_failures.load(Ordering::Relaxed),
            nats_errors = self.nats_errors.load(Ordering::Relaxed),
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

    /// Build a consumer with a custom `AckWait` (primarily for tests).
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

    /// Override the `JetStream` batch fetch behavior (max messages per pull and expiration timeout).
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

    /// Bootstrap all required `JetStream` streams
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
                max_age: Duration::from_hours(2160), // 90 days (operational history)
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create events stream: {e}")))?;

        // Confirmations stream with compaction - only keep latest per event
        // Short retention since confirmations are ephemeral operational state
        let confirmations_stream = self.topology.confirmations_stream.clone();
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: confirmations_stream.clone(),
                subjects: vec![self.topology.confirmations_subject.clone()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages_per_subject: 1, // Compaction: only keep latest confirmation
                max_age: Duration::from_hours(168), // 7 days (operational buffer)
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network(format!("Failed to create confirmations stream: {e}"))
            })?;

        // DLQ stream
        let dlq_stream = self.topology.dlq_stream.clone();
        self.js
            .get_or_create_stream(jetstream::stream::Config {
                name: dlq_stream.clone(),
                subjects: vec![self.topology.dlq_subject.clone()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 1_000_000,
                max_age: Duration::from_hours(720), // 30 days
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network(format!("Failed to create DLQ stream: {e}")))?;

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
            .map_err(|e| SinexError::network(format!("Failed to get stream: {e}")))?;

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
            .map_err(|e| SinexError::network(format!("Failed to create consumer: {e}")))?;

        // Stats logging interval
        let mut stats_interval = tokio::time::interval(Duration::from_mins(1));
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
            .map_err(|e| SinexError::network(format!("Failed to fetch messages: {e}")))?;
        let mut batch = Vec::new();

        while let Some(msg) = messages.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    self.stats.nats_errors.fetch_add(1, Ordering::Relaxed);
                    error!(
                        nats_errors = self.stats.nats_errors.load(Ordering::Relaxed),
                        "Error receiving message: {}", e
                    );
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
        // Parse event using unified Event model
        let event: Event<JsonValue> = match serde_json::from_slice(&msg.payload) {
            Ok(e) => e,
            Err(e) => {
                error!(event_id = ?msg.headers, "Failed to parse event: {}", e);
                self.route_validation_failure(&msg, format!("Parse error: {e}"))
                    .await?;
                return Ok(None);
            }
        };

        // Validate event using EventValidator
        if let Err(e) = self.validate_event(&event).await {
            warn!(event_id = ?event.id, "Event validation failed: {}", e);
            self.route_validation_failure(&msg, format!("Validation failed: {e}"))
                .await?;
            return Ok(None);
        }

        // The ID MUST be present for events coming from Ingestors
        let parsed_id = if let Some(id) = event.id {
            *id.as_ulid()
        } else {
            error!("Event missing required ID; routing to DLQ");
            self.route_validation_failure(&msg, "Missing event ID".to_string())
                .await?;
            return Ok(None);
        };

        Ok(Some(PreparedEvent {
            event,
            parsed_id,
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
                    .map(|ids| ids.iter().copied().collect::<HashSet<_>>());
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
                        .map_err(|e| SinexError::network(format!("Failed to ack: {e}")))?;
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
                                format!("Persistence error: {e}"),
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
    async fn validate_event(&self, event: &Event<JsonValue>) -> IngestdResult<()> {
        let guard = self.validator.read().await;
        let validation =
            guard.validate_payload_for(&event.source, &event.event_type, &event.payload);
        let strict_mode = guard.is_strict_mode();

        match validation {
            ValidationResult::Valid | ValidationResult::Skipped => Ok(()),
            ValidationResult::NoSchema => {
                if strict_mode {
                    Err(SinexError::validation(format!(
                        "Strict validation enabled: event has no registered schema (source={}, event_type={})",
                        event.source, event.event_type
                    ))
                    .with_operation("jetstream_consumer.validate_event")
                    .with_context("strict_mode", "enabled"))
                } else {
                    Ok(())
                }
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
                        "Failed to persist events batch: {err}"
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
                    "Persisting batch timed out after {DB_WRITE_TIMEOUT:?}"
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
            if let Err(err) = prepared.event.write_copy_row(&mut row_buf) {
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
        // Warning: This batch method bypasses `ensure_no_synthesis_cycles`.
        // While efficient, it risks introducing circular synthesis dependencies.
        // Consider implementing a batched cycle check or ensuring upstream validation.

        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let rows: Vec<StreamBatchRow> = batch
            .iter()
            .map(|prepared| {
                let event = &prepared.event;
                let (
                    source_event_ids,
                    source_material_id,
                    offset_start,
                    offset_end,
                    offset_kind,
                    anchor_byte,
                ) = sinex_db::repositories::events::conversions::extract_provenance(event);

                // Re-map extracted provenance to match StreamBatchRow expectations
                // extract_provenance returns Option<Vec<Ulid>> for source_event_ids
                // StreamBatchRow expects Option<Vec<Uuid>>.
                let source_event_ids = source_event_ids
                    .map(|ids| ids.iter().map(sinex_primitives::Ulid::as_uuid).collect());
                let source_material_id = source_material_id.map(|id| id.as_uuid());

                StreamBatchRow {
                    id: prepared.parsed_id,
                    source: event.source.as_str().to_string(),
                    event_type: event.event_type.as_str().to_string(),
                    ts_orig: event
                        .ts_orig
                        .unwrap_or_else(sinex_primitives::Timestamp::now),
                    host: event.host.as_str().to_string(),
                    payload: event.payload.clone(),
                    source_material_id,
                    anchor_byte,
                    offset_start,
                    offset_end,
                    offset_kind,
                    source_event_ids,
                    payload_schema_id: event.payload_schema_id.map(|id| id.as_uuid()),
                    ingestor_version: event.ingestor_version.clone(),
                    associated_blob_ids: event
                        .associated_blob_ids
                        .as_ref()
                        .map(|ids| ids.iter().map(sinex_primitives::Ulid::as_uuid).collect()),
                }
            })
            .collect();

        let result = timeout(
            DB_WRITE_TIMEOUT,
            self.pool.events().insert_stream_batch(&rows),
        )
        .await
        .map_err(|_| {
            SinexError::database(format!(
                "Persisting batch timed out after {DB_WRITE_TIMEOUT:?}"
            ))
        })?
        .map_err(|err| {
            error!("Failed to persist events batch: {}", err);
            SinexError::database(err.to_string())
        })?;

        Ok(result.inserted_ids.unwrap_or_default())
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
            ts_ingest: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
        };

        let subject = format!("{}{}", self.topology.confirmations_prefix, event_id_str);
        let payload = serde_json::to_vec(&confirmation)?;

        // Add idempotency header
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id_str.as_str());

        self.js
            .publish_with_headers(subject, headers, payload.into())
            .await
            .map_err(|e| SinexError::network(format!("Failed to publish confirmation: {e}")))?
            .await
            .map_err(|e| SinexError::network(format!("Confirmation ack failed: {e}")))?;

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
                .map_or_else(|| "unknown".to_string(), |v| v.as_str().to_string()),
            error,
            original_payload,
            failed_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
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
                .map_err(|e| SinexError::network(format!("Failed to ack: {e}")))?;
            self.stats.dlq_routed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .dlq_publish_failures
                .fetch_add(1, Ordering::Relaxed);
            msg.ack_with(jetstream::AckKind::Nak(Some(DLQ_RETRY_DELAY)))
                .await
                .map_err(|e| {
                    self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                    SinexError::network(format!("Failed to NAK after DLQ failure: {e}"))
                })?;
        }
        Ok(())
    }

    /// Check stream capacity and log warnings if approaching limits
    async fn check_stream_capacity(&self, stream_name: &str) {
        match self.js.get_stream(stream_name).await {
            Ok(mut stream) => {
                if let Ok(info) = stream.info().await {
                    let state = info.state;
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
