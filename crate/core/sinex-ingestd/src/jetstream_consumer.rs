//! `JetStream` event consumer with confirmations and DLQ support
//!
//! See `crate::docs::ingestion_pipeline` for architectural details.

use async_nats::{jetstream, Client as NatsClient};
use futures::{future::join_all, StreamExt};
use serde::Serialize;
use sinex_db::repositories::StreamBatchRow;
use sinex_db::{repositories::DbPoolExt, DbPool};
use sinex_node_sdk::SelfObserver;
use sinex_primitives::Timestamp;
use sinex_primitives::{environment::SinexEnvironment, ulid::Ulid, JsonValue};
use sqlx::{Connection, PgConnection};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, warn};

use crate::{
    material_ready_set::MaterialReadySet,
    validator::{EventValidator, ValidationResult},
    IngestdResult, SinexError,
};
use sinex_primitives::events::builder::Provenance;
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
    database_url: String,
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
    /// Shared coordination set: when present, events whose `source_material_id` hasn't
    /// been registered yet are NAK'd with a short delay instead of attempting a DB insert
    /// that would hit an FK violation.
    ready_set: Option<MaterialReadySet>,
    /// Self-observer for emitting internal metrics
    observer: Option<Arc<SelfObserver>>,
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
/// Retry delay for deferred events whose source material isn't registered yet.
/// Short delay (200ms) allows the MaterialAssembler to process the BEGIN message
/// before JetStream redelivers the event. Used by both the proactive ready-set
/// pre-filter and the reactive FK violation safety net.
const FK_VIOLATION_RETRY_DELAY: Duration = Duration::from_millis(200);
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
    events_deferred: AtomicU64,
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
            events_deferred = self.events_deferred.load(Ordering::Relaxed),
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

        // Get DATABASE_URL for non-pooled COPY connections
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex?host=/run/postgresql".to_string());

        Self {
            js,
            pool,
            database_url,
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
            ready_set: None,
            observer: None,
        }
    }

    /// Set self-observer for emitting metrics (stream stats, processing stats)
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<SelfObserver>) -> Self {
        self.observer = Some(observer);
        self
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

    /// Attach a `MaterialReadySet` for proactive FK-violation prevention.
    ///
    /// When set, events whose `source_material_id` is not yet registered will be
    /// NAK'd with a short delay instead of hitting a database FK constraint error.
    pub fn with_ready_set(mut self, ready_set: MaterialReadySet) -> Self {
        self.ready_set = Some(ready_set);
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
            .map_err(|e| SinexError::network("Failed to create events stream").with_source(e))?;

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
                SinexError::network("Failed to create confirmations stream").with_source(e)
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
            .map_err(|e| SinexError::network("Failed to create DLQ stream").with_source(e))?;

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
            .map_err(|e| SinexError::network("Failed to get stream").with_source(e))?;

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
            .map_err(|e| SinexError::network("Failed to create consumer").with_source(e))?;

        // Stats logging interval
        let mut stats_interval = tokio::time::interval(Duration::from_mins(1));
        // Stream capacity monitoring interval
        let mut capacity_check_interval = tokio::time::interval(STREAM_CAPACITY_CHECK_INTERVAL);

        loop {
            tokio::select! {
                _ = stats_interval.tick() => {
                    self.stats.log();
                    // Emit processing stats via self-observer
                    if let Some(ref observer) = self.observer {
                        let processed = self.stats.events_processed.load(std::sync::atomic::Ordering::Relaxed);
                        let failed = self.stats.events_failed.load(std::sync::atomic::Ordering::Relaxed);
                        let deferred = self.stats.events_deferred.load(std::sync::atomic::Ordering::Relaxed);
                        let dlq_routed = self.stats.dlq_routed.load(std::sync::atomic::Ordering::Relaxed);
                        if let Err(e) = observer.emit_node_processing_stats(
                            "jetstream-consumer",
                            processed,
                            deferred + dlq_routed, // events_dropped = deferred + routed to DLQ
                            None, // avg_latency_ms - not tracked yet
                            0,    // queue_depth - would need consumer info
                            failed,
                        ).await {
                            warn!("Failed to emit processing stats: {}", e);
                        }
                    }
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
            .map_err(|e| SinexError::network("Failed to fetch messages").with_source(e))?;
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
        // Parse event using unified Event model.
        // Distinguish pure JSON syntax errors from typed deserialization failures
        // (e.g. an invalid timestamp string in a Timestamp field).
        let event: Event<JsonValue> = match serde_json::from_slice(&msg.payload) {
            Ok(e) => e,
            Err(e) => {
                let reason = if serde_json::from_slice::<serde_json::Value>(&msg.payload).is_ok() {
                    // Valid JSON but typed fields didn't match (e.g. bad timestamp format)
                    error!(event_id = ?msg.headers, "Invalid timestamp or field format: {}", e);
                    format!("Invalid timestamp or field format: {e}")
                } else {
                    error!(event_id = ?msg.headers, "Failed to parse event: {}", e);
                    format!("Parse error: {e}")
                };
                self.route_validation_failure(&msg, reason).await?;
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
        // Pre-filter: defer events whose source material isn't registered yet.
        // This prevents FK violations without relying on database error handling.
        let batch = if let Some(ref ready_set) = self.ready_set {
            let (ready, not_ready): (Vec<&PreparedEvent>, Vec<&PreparedEvent>) =
                batch.iter().partition(|prepared| {
                    match &prepared.event.provenance {
                        // Material provenance: check if the referenced material is registered
                        Provenance::Material { id, .. } => ready_set.is_ready(id.as_ulid()),
                        // Synthesis provenance (and any future variants) have no material FK
                        _ => true,
                    }
                });

            if !not_ready.is_empty() {
                debug!(
                    deferred = not_ready.len(),
                    ready = ready.len(),
                    "Deferring events whose source material is not yet registered"
                );
                for prepared in &not_ready {
                    if let Err(err) = prepared
                        .message
                        .ack_with(jetstream::AckKind::Nak(Some(FK_VIOLATION_RETRY_DELAY)))
                        .await
                    {
                        warn!(
                            event_id = %prepared.parsed_id,
                            error = %err,
                            "Failed to NAK deferred event"
                        );
                        self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
                self.stats
                    .events_deferred
                    .fetch_add(not_ready.len() as u64, Ordering::Relaxed);
            }

            if ready.is_empty() {
                return Ok(());
            }
            ready
        } else {
            batch.iter().collect()
        };

        let persist_result = self.persist_batch_optimized(&batch).await;
        match persist_result {
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
                // Publish confirmations concurrently for the entire batch.
                // This is the primary throughput optimization: O(1) wall-clock time
                // instead of O(n) serial NATS round-trips per batch.
                let confirmation_futs: Vec<_> = batch
                    .iter()
                    .map(|prepared| self.publish_confirmation_with_retry(&prepared.parsed_id))
                    .collect();
                let confirmation_results = join_all(confirmation_futs).await;

                let mut has_confirmation_failure = false;
                for (result, prepared) in confirmation_results.iter().zip(batch.iter()) {
                    match result {
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
                            has_confirmation_failure = true;
                        }
                    }
                }

                if has_confirmation_failure {
                    let nak_futs: Vec<_> = batch
                        .iter()
                        .map(|prepared| {
                            prepared
                                .message
                                .ack_with(jetstream::AckKind::Nak(Some(CONFIRM_RETRY_DELAY)))
                        })
                        .collect();
                    let nak_results = join_all(nak_futs).await;
                    for (result, prepared) in nak_results.iter().zip(batch.iter()) {
                        if let Err(err) = result {
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

                // ACK all messages concurrently
                let ack_futs: Vec<_> = batch
                    .iter()
                    .map(|prepared| prepared.message.ack())
                    .collect();
                let ack_results = join_all(ack_futs).await;
                for result in &ack_results {
                    if let Err(e) = result {
                        return Err(SinexError::network(format!("Failed to ack: {e}")));
                    }
                }

                self.stats
                    .events_processed
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
                info!("Processed and confirmed {} events", batch.len());
            }
            Err(e) => {
                // Check if this is a transient FK violation (source material not yet registered).
                // Safety net: the ready set should prevent most FK violations, but races are
                // possible (e.g. material registered between ready-set check and DB insert).
                let is_fk_error = e.to_string().contains("FK_VIOLATION");
                if is_fk_error {
                    debug!(
                        batch_size = batch.len(),
                        "FK violation on batch - source material likely still registering; NAKing with delay"
                    );
                    for prepared in &batch {
                        if let Err(err) = prepared
                            .message
                            .ack_with(jetstream::AckKind::Nak(Some(FK_VIOLATION_RETRY_DELAY)))
                            .await
                        {
                            warn!(
                                event_id = %prepared.parsed_id,
                                error = %err,
                                "Failed to NAK after FK violation"
                            );
                            self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    // Don't count as failed - this is a transient condition
                    return Ok(());
                }

                error!("Failed to persist batch: {}", e);
                if self.route_db_errors_to_dlq {
                    for prepared in &batch {
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
                    for prepared in &batch {
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
        // Validate domain type formats before payload validation
        if let Err(reason) = event.source.validate() {
            return Err(SinexError::validation(format!(
                "Invalid event source '{}': {reason}",
                event.source
            ))
            .with_operation("jetstream_consumer.validate_event")
            .with_context("source", event.source.to_string()));
        }
        if let Err(reason) = event.event_type.validate() {
            return Err(SinexError::validation(format!(
                "Invalid event type '{}': {reason}",
                event.event_type
            ))
            .with_operation("jetstream_consumer.validate_event")
            .with_context("event_type", event.event_type.to_string()));
        }

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
                // Fail closed: reject events when their schema cannot be found.
                // Previously this accepted with a warning, which allowed invalid payloads
                // to be ingested silently.
                Err(SinexError::validation(format!(
                    "Schema '{}' not found for {}.{} — rejecting event (fail-closed)",
                    schema_id, event.source, event.event_type
                ))
                .with_operation("jetstream_consumer.validate_event")
                .with_context("schema_id", schema_id.to_string()))
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
        batch: &[&PreparedEvent],
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

        // Use COPY with non-pooled connection (avoids sqlx 0.8.x pool corruption bug).
        // Falls back to INSERT ON CONFLICT if COPY fails.
        let insert_result = timeout(DB_WRITE_TIMEOUT, self.persist_batch_copy(&to_persist)).await;
        match insert_result {
            Ok(Ok(inserted_ids)) => {
                self.remember_batch(batch);
                Ok(PersistBatchResult {
                    inserted_ids: Some(inserted_ids),
                })
            }
            Ok(Err(err)) => {
                let err_str = err.to_string();
                if err_str.contains("FK_VIOLATION")
                    || err_str.contains("violates foreign key constraint")
                {
                    warn!(
                        batch_size = to_persist.len(),
                        "INSERT hit FK violation (source_material not yet registered); will retry"
                    );
                    Err(SinexError::service(
                        "FK_VIOLATION: source material not yet registered".to_string(),
                    ))
                } else {
                    error!("Failed to persist events batch: {}", err);
                    Err(err)
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

    fn filter_cached_batch<'a>(&self, batch: &[&'a PreparedEvent]) -> Vec<&'a PreparedEvent> {
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
            .copied()
            .collect()
    }

    fn remember_batch(&self, batch: &[&PreparedEvent]) {
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

    /// Persist batch using COPY protocol with non-pooled connection.
    ///
    /// Uses a dedicated connection (not from pool) to avoid sqlx 0.8.x corruption bug.
    /// Falls back to INSERT ON CONFLICT if COPY fails.
    async fn persist_batch_copy(&self, batch: &[&PreparedEvent]) -> IngestdResult<Vec<Ulid>> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        // Only use COPY for larger batches (overhead not worth it for small batches)
        if batch.len() < 10 {
            return self.persist_batch_insert_on_conflict(batch).await;
        }

        // Try COPY with non-pooled connection
        match self.try_persist_batch_copy_internal(batch).await {
            Ok(ids) => Ok(ids),
            Err(e) => {
                warn!(
                    batch_size = batch.len(),
                    error = %e,
                    "COPY failed, falling back to INSERT ON CONFLICT"
                );
                self.persist_batch_insert_on_conflict(batch).await
            }
        }
    }

    /// Internal COPY implementation using non-pooled connection
    async fn try_persist_batch_copy_internal(
        &self,
        batch: &[&PreparedEvent],
    ) -> IngestdResult<Vec<Ulid>> {
        // Create non-pooled connection (bypasses pool entirely)
        let mut conn = PgConnection::connect(&self.database_url)
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to create non-pooled connection: {e}"))
            })?;

        // Prepare data rows
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
                ) = sinex_db::repositories::events::conversions::extract_provenance(event)?;

                let source_event_ids = source_event_ids
                    .map(|ids| ids.iter().map(sinex_primitives::Ulid::as_uuid).collect());
                let source_material_id = source_material_id.map(|id| id.as_uuid());

                Ok(StreamBatchRow {
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
                })
            })
            .collect::<IngestdResult<Vec<_>>>()?;

        // INSERT via non-pooled connection using QueryBuilder (same pattern as
        // EventRepository::execute_batch_insert). This avoids the sqlx 0.8.x pool
        // corruption bug by never returning this connection to a pool.
        // TODO: Replace with proper COPY BINARY protocol for 5-10x throughput.
        use sqlx::QueryBuilder;

        let mut builder = QueryBuilder::new(
            "INSERT INTO core.events (
                id, source, event_type, ts_orig, ts_orig_subnano, host, payload,
                source_material_id, anchor_byte, offset_start, offset_end, offset_kind,
                source_event_ids, payload_schema_id, ingestor_version, associated_blob_ids
            ) ",
        );

        builder.push_values(rows.iter().enumerate(), |mut b, (_idx, row)| {
            let (ts_truncated, ts_subnano) = row.ts_orig.to_postgres_parts();
            b.push_bind(row.id.as_uuid())
                .push_unseparated("::uuid::ulid");
            b.push_bind(row.source.clone());
            b.push_bind(row.event_type.clone());
            b.push_bind(ts_truncated);
            b.push_bind(ts_subnano);
            b.push_bind(row.host.clone());
            b.push_bind(row.payload.clone());
            b.push_bind(row.source_material_id);
            b.push_bind(row.anchor_byte);
            b.push_bind(row.offset_start);
            b.push_bind(row.offset_end);
            b.push_bind(row.offset_kind.clone());
            b.push_bind(row.source_event_ids.clone());
            b.push_bind(row.payload_schema_id);
            b.push_bind(row.ingestor_version.clone());
            b.push_bind(row.associated_blob_ids.clone());
        });

        builder.push(" ON CONFLICT (id) DO NOTHING");

        builder
            .build()
            .execute(&mut conn)
            .await
            .map_err(|e| SinexError::database("Non-pooled INSERT failed").with_source(e))?;

        let inserted_ids: Vec<Ulid> = rows.iter().map(|r| r.id).collect();

        // Connection is dropped here, not returned to any pool
        Ok(inserted_ids)
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
                ) = sinex_db::repositories::events::conversions::extract_provenance(event)?;

                // Re-map extracted provenance to match StreamBatchRow expectations
                // extract_provenance returns Option<Vec<Ulid>> for source_event_ids
                // StreamBatchRow expects Option<Vec<Uuid>>.
                let source_event_ids = source_event_ids
                    .map(|ids| ids.iter().map(sinex_primitives::Ulid::as_uuid).collect());
                let source_material_id = source_material_id.map(|id| id.as_uuid());

                Ok(StreamBatchRow {
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
                })
            })
            .collect::<IngestdResult<Vec<_>>>()?;

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
            .map_err(|e| SinexError::network("Failed to publish confirmation").with_source(e))?
            .await
            .map_err(|e| SinexError::network("Confirmation ack failed").with_source(e))?;

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
                .map_err(|e| SinexError::network("Failed to ack").with_source(e))?;
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

                    // Emit stream stats via self-observer
                    if let Some(ref observer) = self.observer {
                        if let Err(e) = observer
                            .emit_stream_stats(
                                stream_name,
                                state.messages,
                                config.max_messages as u64,
                                state.bytes,
                                config.max_bytes as u64,
                                state.consumer_count as u32,
                                state.first_sequence,
                                state.last_sequence,
                            )
                            .await
                        {
                            debug!("Failed to emit stream stats: {}", e);
                        }
                    }

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
