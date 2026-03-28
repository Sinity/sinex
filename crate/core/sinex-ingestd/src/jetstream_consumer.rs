//! `JetStream` event consumer with confirmations and DLQ support
//!
//! See `crate::docs::ingestion_pipeline` for architectural details.

use async_nats::{Client as NatsClient, jetstream};
use futures::future::{BoxFuture, join_all};
use serde::{Deserialize, Serialize};
use sinex_db::repositories::{COPY_BATCH_THRESHOLD, StreamBatchRow};
use sinex_db::{DbPool, repositories::DbPoolExt};
use sinex_node_sdk::SelfObserver;
use sinex_node_sdk::heartbeat::HeartbeatCounterHandle;
use sinex_node_sdk::runtime::stream::{PullConsumerSpec, ensure_pull_consumer, pull_batch};
use sinex_primitives::Timestamp;
use sinex_primitives::{JsonValue, Uuid, environment::SinexEnvironment};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::{Duration, timeout};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    IngestdResult, SinexError,
    material_ready_set::MaterialReadySet,
    validator::{EventValidator, ValidationResult},
};
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::Provenance;
use tokio::sync::RwLock;

#[derive(Debug, Serialize)]
struct Confirmation {
    event_id: String,
    persisted: bool,
    ts_ingest: Timestamp,
}

#[derive(Debug, Serialize, Deserialize)]
struct ConfirmationRetryRequest {
    event_id: String,
}

fn signal_ready(ready_tx: Option<tokio::sync::oneshot::Sender<()>>, component: &str) -> bool {
    match ready_tx {
        Some(tx) => {
            if tx.send(()).is_err() {
                warn!(component, "Readiness receiver dropped before ready signal");
                false
            } else {
                true
            }
        }
        None => true,
    }
}

#[derive(Debug, Serialize)]
struct DlqEntry {
    /// NATS Msg-Id header value (not a Sinex event `UUIDv7`).
    #[serde(skip_serializing_if = "Option::is_none")]
    nats_msg_id: Option<String>,
    error: String,
    original_payload: JsonValue,
    failed_at: Timestamp,
}

pub struct JetStreamConsumer {
    js: jetstream::Context,
    pool: DbPool,
    validator: Arc<RwLock<EventValidator>>,
    topology: JetStreamTopology,
    ack_wait: Duration,
    max_ack_pending: i64,
    fail_once: Option<Arc<AtomicBool>>,
    db_failures_remaining: Option<Arc<AtomicUsize>>,
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
    /// How often to log processing stats
    stats_log_interval: Duration,
    /// Heartbeat counter handle — feeds batch counts into health status determination
    heartbeat_handle: Option<HeartbeatCounterHandle>,
}

#[derive(Debug, Clone)]
pub struct JetStreamTopology {
    pub events_stream: String,
    pub events_subject: String,
    pub confirmations_stream: String,
    pub confirmations_subject: String,
    pub confirmations_prefix: String,
    pub confirmation_retry_stream: String,
    pub confirmation_retry_subject: String,
    pub confirmation_retry_prefix: String,
    pub confirmation_retry_consumer: String,
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
        let confirmation_retry_stream = format!("{base_stream}_CONFIRMATION_RETRIES");
        let dlq_stream = format!("{base_stream}_DLQ");
        let namespaced = |subject: &str| env.nats_subject_with_namespace(namespace, subject);
        let confirmations_prefix = format!("{}.", namespaced("events.confirmations"));
        let confirmation_retry_prefix =
            format!("{}.", namespaced("events.confirmation_retries"));

        Self {
            events_stream: base_stream,
            events_subject: namespaced("events.raw.>"),
            confirmations_stream,
            confirmations_subject: namespaced("events.confirmations.>"),
            confirmations_prefix,
            confirmation_retry_stream,
            confirmation_retry_subject: namespaced("events.confirmation_retries.>"),
            confirmation_retry_prefix,
            confirmation_retry_consumer: format!("{consumer_durable}_confirm_retries"),
            dlq_stream,
            dlq_subject: namespaced("events.dlq.>"),
            dlq_publish_subject: namespaced("events.dlq.ingestd"),
            consumer_durable,
        }
    }
}

const DB_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// SQLSTATE for foreign-key violation.
const SQLSTATE_DATA_EXCEPTION_CLASS: &str = "22";
const SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION_CLASS: &str = "23";

/// Error-class marker for deferred source-material FK violations.
const ERROR_CLASS_SOURCE_MATERIAL_FK: &str = "source_material_fk_violation";
const EVENTS_SOURCE_MATERIAL_ID_FKEY: &str = "events_source_material_id_fkey";

fn is_foreign_key_violation(err: &SinexError) -> bool {
    err.context_map()
        .get("sqlstate")
        .is_some_and(|value| value == "23503")
        || err
            .to_string()
            .contains("Foreign key constraint violation")
}

fn has_explicit_source_material_fk_marker(err: &SinexError) -> bool {
    err.context_map()
        .get("error_class")
        .is_some_and(|value| value == ERROR_CLASS_SOURCE_MATERIAL_FK)
        || err
            .context_map()
            .get("constraint")
            .is_some_and(|value| value == EVENTS_SOURCE_MATERIAL_ID_FKEY)
}

fn batch_depends_only_on_source_material_fk(batch: &[&PreparedEvent]) -> bool {
    batch.iter().all(|prepared| {
        matches!(prepared.event.provenance, Provenance::Material { .. })
            && prepared.event.payload_schema_id.is_none()
            && prepared.event.node_run_id.is_none()
    })
}

fn rows_depend_only_on_source_material_fk(batch: &[StreamBatchRow]) -> bool {
    batch.iter().all(|row| {
        row.source_material_id.is_some()
            && row
                .source_event_ids
                .as_ref()
                .is_none_or(|source_ids| source_ids.is_empty())
            && row.payload_schema_id.is_none()
            && row.node_run_id.is_none()
    })
}

fn is_source_material_fk_violation_for_prepared_batch(
    err: &SinexError,
    batch: &[&PreparedEvent],
) -> bool {
    has_explicit_source_material_fk_marker(err)
        || (is_foreign_key_violation(err) && batch_depends_only_on_source_material_fk(batch))
}

fn is_source_material_fk_violation_for_stream_batch(
    err: &SinexError,
    batch: &[StreamBatchRow],
) -> bool {
    has_explicit_source_material_fk_marker(err)
        || (is_foreign_key_violation(err) && rows_depend_only_on_source_material_fk(batch))
}

fn is_isolatable_batch_persistence_failure(err: &SinexError) -> bool {
    if has_explicit_source_material_fk_marker(err)
        || sinex_db::query_helpers::is_retryable_db_error(err)
    {
        return false;
    }

    if is_foreign_key_violation(err) {
        return true;
    }

    err.context_map()
        .get("sqlstate")
        .is_some_and(|value| {
            value.starts_with(SQLSTATE_DATA_EXCEPTION_CLASS)
                || value.starts_with(SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION_CLASS)
        })
}
const RECENT_ID_CACHE_SIZE: usize = 50_000;
const DEFAULT_BATCH_FETCH_MAX_MESSAGES: usize = 100;
const DEFAULT_BATCH_FETCH_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_MAX_ACK_PENDING: i64 = 100;
const MAIN_CONSUMER_MAX_DELIVER: i64 = 10;
const DLQ_PUBLISH_MAX_ATTEMPTS: usize = 3;
const DLQ_PUBLISH_BACKOFF_BASE: Duration = Duration::from_millis(200);
const DLQ_PUBLISH_BACKOFF_MAX: Duration = Duration::from_secs(2);
const DLQ_DUPLICATE_WINDOW: Duration = Duration::from_hours(1);
const DLQ_RETRY_DELAY: Duration = Duration::from_secs(1);
const CONFIRM_PUBLISH_MAX_ATTEMPTS: usize = 3;
const CONFIRM_PUBLISH_BACKOFF_BASE: Duration = Duration::from_millis(200);
const CONFIRM_PUBLISH_BACKOFF_MAX: Duration = Duration::from_secs(2);
const CONFIRM_RETRY_DELAY: Duration = Duration::from_secs(1);
const CONFIRM_RETRY_POLL_INTERVAL: Duration = Duration::from_secs(1);
const CONFIRM_RETRY_BATCH_MAX_MESSAGES: usize = 32;
const CONFIRM_RETRY_BATCH_TIMEOUT: Duration = Duration::from_millis(100);
const SUSPICIOUS_TS_ORIG_FUTURE_SKEW: time::Duration = time::Duration::hours(1);
/// Retry delay for deferred events whose source material isn't registered yet.
/// Short delay (200ms) allows the `MaterialAssembler` to process the BEGIN message
/// before `JetStream` redelivers the event. Used by both the proactive ready-set
/// pre-filter and the reactive FK violation safety net.
const FK_VIOLATION_RETRY_DELAY: Duration = Duration::from_millis(200);
const STREAM_CAPACITY_WARNING_THRESHOLD: f64 = 0.8; // Alert at 80% capacity
const STREAM_CAPACITY_CHECK_INTERVAL: Duration = Duration::from_mins(5); // Check every 5 minutes

fn is_suspicious_future_ts_orig(ts_orig: Timestamp, now: Timestamp) -> bool {
    ts_orig > now + SUSPICIOUS_TS_ORIG_FUTURE_SKEW
}

#[derive(Debug)]
struct PersistBatchResult {
    inserted_ids: Option<Vec<Uuid>>,
}

#[derive(Debug, Clone)]
struct RecentIdCache {
    capacity: usize,
    order: VecDeque<Uuid>,
    set: HashSet<Uuid>,
}

impl RecentIdCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            set: HashSet::with_capacity(capacity),
        }
    }

    fn contains(&self, id: &Uuid) -> bool {
        if self.capacity == 0 {
            return false;
        }
        self.set.contains(id)
    }

    fn insert(&mut self, id: Uuid) {
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
    parsed_id: Uuid,
    message: jetstream::Message,
}

fn dlq_publish_msg_id(
    msg: &jetstream::Message,
    original_nats_msg_id: Option<&str>,
    original_payload: &JsonValue,
) -> String {
    if let Some(event_id) = original_payload.get("id").and_then(|value| value.as_str()) {
        return format!("dlq.{event_id}");
    }

    match original_nats_msg_id {
        Some(original_id) => format!("dlq.msg.{original_id}"),
        None => {
            let mut hasher = blake3::Hasher::new();
            hasher.update(msg.subject.as_str().as_bytes());
            hasher.update(&msg.payload);
            format!("dlq.hash.{}", hasher.finalize().to_hex())
        }
    }
}

#[derive(Debug, Default)]
struct ConsumerStats {
    events_processed: AtomicU64,
    events_failed: AtomicU64,
    events_deferred: AtomicU64,
    suspicious_future_ts_orig: AtomicU64,
    validation_failures: AtomicU64,
    dlq_routed: AtomicU64,
    confirmation_failures: AtomicU64,
    confirmation_retries_enqueued: AtomicU64,
    confirmation_retry_failures: AtomicU64,
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
            suspicious_future_ts_orig = self.suspicious_future_ts_orig.load(Ordering::Relaxed),
            validation_failures = self.validation_failures.load(Ordering::Relaxed),
            nats_errors = self.nats_errors.load(Ordering::Relaxed),
            dlq_routed = self.dlq_routed.load(Ordering::Relaxed),
            confirmation_failures = self.confirmation_failures.load(Ordering::Relaxed),
            confirmation_retries_enqueued = self.confirmation_retries_enqueued.load(Ordering::Relaxed),
            confirmation_retry_failures = self.confirmation_retry_failures.load(Ordering::Relaxed),
            dlq_publish_failures = self.dlq_publish_failures.load(Ordering::Relaxed),
            nack_failures = self.nack_failures.load(Ordering::Relaxed),
            "JetStream consumer stats"
        );
    }
}

impl JetStreamConsumer {
    fn log_observer_error(metric: &'static str, error: &sinex_node_sdk::SelfObservationError) {
        warn!(metric, error = %error, "Failed to emit ingestd telemetry");
    }

    async fn emit_observer_gauge(
        &self,
        metric: &'static str,
        value: f64,
        labels: Option<HashMap<String, String>>,
    ) {
        if let Some(ref observer) = self.observer
            && let Err(error) = observer.emit_gauge(metric, value, labels).await
        {
            Self::log_observer_error(metric, &error);
        }
    }

    async fn record_suspicious_ts_orig(&self, event: &Event<JsonValue>) {
        let Some(ts_orig) = event.ts_orig else {
            return;
        };

        let now = Timestamp::now();
        if !is_suspicious_future_ts_orig(ts_orig, now) {
            return;
        }
        let latest_expected = now + SUSPICIOUS_TS_ORIG_FUTURE_SKEW;

        self.stats
            .suspicious_future_ts_orig
            .fetch_add(1, Ordering::Relaxed);

        warn!(
            event_id = ?event.id,
            source = %event.source,
            event_type = %event.event_type,
            ts_orig = %ts_orig,
            latest_expected = %latest_expected,
            skew_seconds = (ts_orig - now).whole_seconds(),
            "Event ts_orig is implausibly far in the future"
        );

        if let Some(ref observer) = self.observer
            && let Err(error) = observer
                .emit_counter("suspicious_future_ts_orig_total", 1, None)
                .await
        {
            Self::log_observer_error("ingestd.suspicious_ts_orig", &error);
        }
    }

    fn require_inserted_ids(
        inserted_ids: Option<Vec<Uuid>>,
        attempted_rows: usize,
    ) -> IngestdResult<Vec<Uuid>> {
        inserted_ids.ok_or_else(|| {
            SinexError::invalid_state(format!(
                "Event repository omitted inserted_ids for a non-empty stream batch of {attempted_rows} row(s)"
            ))
        })
    }

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
            db_failures_remaining: None,
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
            stats_log_interval: Duration::from_mins(1),
            heartbeat_handle: None,
        }
    }

    /// Set stats logging interval.
    #[must_use]
    pub fn with_stats_log_interval(mut self, interval: Duration) -> Self {
        self.stats_log_interval = interval;
        self
    }

    /// Set self-observer for emitting metrics (stream stats, processing stats)
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<SelfObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Set heartbeat counter handle for health status tracking.
    /// Batch success/failure counts are forwarded to the heartbeat emitter.
    #[must_use]
    pub fn with_heartbeat_handle(mut self, handle: HeartbeatCounterHandle) -> Self {
        self.heartbeat_handle = Some(handle);
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
        db_failures_remaining: Option<Arc<AtomicUsize>>,
        processing_delay: Option<Duration>,
        delivery_observer: Option<Arc<AtomicU64>>,
        route_db_errors_to_dlq: bool,
        confirmation_failures_remaining: Option<Arc<AtomicUsize>>,
    ) -> Self {
        let mut consumer = Self::with_ack_wait(nats_client, pool, validator, topology, ack_wait);
        consumer.fail_once = fail_once;
        consumer.db_failures_remaining = db_failures_remaining;
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
            .create_or_update_stream(jetstream::stream::Config {
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
            .create_or_update_stream(jetstream::stream::Config {
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

        let confirmation_retry_stream = self.topology.confirmation_retry_stream.clone();
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: confirmation_retry_stream.clone(),
                subjects: vec![self.topology.confirmation_retry_subject.clone()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages_per_subject: 1,
                max_age: Duration::from_hours(168),
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network("Failed to create confirmation retry stream").with_source(e)
            })?;

        // DLQ stream
        let dlq_stream = self.topology.dlq_stream.clone();
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: dlq_stream.clone(),
                subjects: vec![self.topology.dlq_subject.clone()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 1_000_000,
                max_age: Duration::from_hours(720), // 30 days
                storage: jetstream::stream::StorageType::File,
                duplicate_window: DLQ_DUPLICATE_WINDOW,
                allow_direct: true,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network("Failed to create DLQ stream").with_source(e))?;

        info!("JetStream streams bootstrapped successfully");
        Ok(())
    }

    pub async fn run(self) -> IngestdResult<()> {
        self.run_with_ready_signal(None).await
    }

    /// Run the consumer, optionally signalling readiness after streams are bound.
    ///
    /// `ready_tx` is sent on after the durable consumer has been created and
    /// the pull loop is about to start. Callers can await the corresponding
    /// receiver before emitting `sd_notify(READY)` to systemd.
    #[instrument(skip(self, ready_tx), fields(consumer = %self.topology.consumer_durable))]
    pub async fn run_with_ready_signal(
        self,
        ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> IngestdResult<()> {
        info!("Starting JetStream consumer");

        // Bootstrap streams
        self.bootstrap_streams().await?;

        // Get events stream and create durable consumer through shared kernel.
        let stream_name = self.topology.events_stream.clone();
        let mut consumer_spec =
            PullConsumerSpec::new(stream_name.clone(), self.topology.consumer_durable.clone());
        consumer_spec.filter_subject = Some(self.topology.events_subject.clone());
        consumer_spec.deliver_policy = jetstream::consumer::DeliverPolicy::All;
        consumer_spec.ack_wait = self.ack_wait;
        consumer_spec.max_ack_pending = self.max_ack_pending;
        consumer_spec.max_deliver = MAIN_CONSUMER_MAX_DELIVER;
        let consumer = ensure_pull_consumer(&self.js, &consumer_spec)
            .await
            .map_err(|e| SinexError::network("Failed to create consumer").with_source(e))?;
        let mut lag_consumer = consumer.clone();
        let mut confirmation_retry_spec = PullConsumerSpec::new(
            self.topology.confirmation_retry_stream.clone(),
            self.topology.confirmation_retry_consumer.clone(),
        );
        confirmation_retry_spec.filter_subject = Some(self.topology.confirmation_retry_subject.clone());
        confirmation_retry_spec.deliver_policy = jetstream::consumer::DeliverPolicy::All;
        confirmation_retry_spec.ack_wait = self.ack_wait;
        confirmation_retry_spec.max_ack_pending = self.max_ack_pending;
        confirmation_retry_spec.max_deliver = -1;
        let confirmation_retry_consumer = ensure_pull_consumer(&self.js, &confirmation_retry_spec)
            .await
            .map_err(|e| {
                SinexError::network("Failed to create confirmation retry consumer").with_source(e)
            })?;

        // Signal readiness: consumer is bound and the pull loop is about to start.
        // This allows callers to delay sd_notify(READY) until the subscription is live.
        signal_ready(ready_tx, "jetstream-consumer");

        // Stats logging interval
        let mut stats_interval = tokio::time::interval(self.stats_log_interval);
        // Stream capacity monitoring interval
        let mut capacity_check_interval = tokio::time::interval(STREAM_CAPACITY_CHECK_INTERVAL);
        // Consumer lag check interval (30s)
        let mut lag_check_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let mut confirmation_retry_interval = tokio::time::interval(CONFIRM_RETRY_POLL_INTERVAL);
        let mut batch_future: BoxFuture<'_, IngestdResult<()>> =
            Box::pin(self.process_batch(&consumer));

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
                _ = lag_check_interval.tick() => {
                    if self.observer.is_some() {
                        match lag_consumer.info().await {
                            Ok(info) => {
                                let mut labels = HashMap::new();
                                labels.insert("consumer".to_string(), self.topology.consumer_durable.clone());
                                self.emit_observer_gauge(
                                    "ingestd.consumer.lag.pending",
                                    info.num_pending as f64,
                                    Some(labels.clone()),
                                ).await;
                                self.emit_observer_gauge(
                                    "ingestd.consumer.lag.ack_pending",
                                    info.num_ack_pending as f64,
                                    Some(labels),
                                ).await;
                            }
                            Err(e) => {
                                debug!("Consumer lag check failed: {e}");
                            }
                        }
                    }
                }
                _ = confirmation_retry_interval.tick() => {
                    if let Err(e) = self.process_confirmation_retry_batch(&confirmation_retry_consumer).await {
                        error!("Confirmation retry processing error: {}", e);
                    }
                }
                batch_result = &mut batch_future => {
                    if let Err(e) = batch_result {
                        error!("Batch processing error: {}", e);
                    }
                    batch_future = Box::pin(self.process_batch(&consumer));
                }
            }
        }
    }

    #[tracing::instrument(skip(self, consumer), fields(consumer_name = %self.topology.consumer_durable))]
    async fn process_batch(
        &self,
        consumer: &jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
    ) -> IngestdResult<()> {
        let batch_start = std::time::Instant::now();
        let mut batch = Vec::new();
        let messages = pull_batch(
            consumer,
            self.batch_fetch_max_messages,
            self.batch_fetch_timeout,
        )
        .await
        .map_err(|e| SinexError::network("Failed to fetch messages").with_source(e))?;
        for msg in messages {
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

        let batch_size = batch.len() as u32;
        let had_synthesis = batch.iter().any(|p| {
            matches!(
                p.event.provenance,
                sinex_primitives::events::Provenance::Synthesis { .. }
            )
        });

        // Snapshot cumulative counters before persist so we can compute per-batch deltas
        let deferred_before = self.stats.events_deferred.load(Ordering::Relaxed);
        let failed_before = self.stats.events_failed.load(Ordering::Relaxed);

        let result = self.persist_and_confirm_batch(&batch).await;

        // Emit batch stats on success
        if result.is_ok()
            && let Some(ref observer) = self.observer
        {
            let fetch_to_ack_ms = batch_start.elapsed().as_millis() as u64;
            let events_deferred =
                (self.stats.events_deferred.load(Ordering::Relaxed) - deferred_before) as u32;
            let events_failed =
                (self.stats.events_failed.load(Ordering::Relaxed) - failed_before) as u32;
            let insert_path = if had_synthesis {
                "query_builder"
            } else if batch_size as usize >= COPY_BATCH_THRESHOLD {
                "copy"
            } else {
                "query_builder"
            };
            let val_stats = self.validator.read().await.stats();
            let suspicious_future_ts_orig = self
                .stats
                .suspicious_future_ts_orig
                .load(Ordering::Relaxed);
            if let Err(error) = observer
                .emit_ingestd_batch_stats(
                    batch_size,
                    fetch_to_ack_ms,
                    events_deferred,
                    events_failed,
                    had_synthesis,
                    insert_path,
                    val_stats.valid,
                    val_stats.skipped,
                    val_stats.no_schema,
                    val_stats.schema_not_found,
                    val_stats.invalid,
                    val_stats.coverage_pct(),
                    suspicious_future_ts_orig,
                )
                .await
            {
                Self::log_observer_error("ingestd.batch", &error);
            }
        }

        result
    }

    #[instrument(skip(self, msg), fields(event_id, source, event_type))]
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

        if event.ts_orig.is_none() {
            warn!(event_id = ?event.id, "Event validation failed: missing ts_orig");
            self.route_validation_failure(&msg, "Validation failed: missing ts_orig".to_string())
                .await?;
            return Ok(None);
        }

        self.record_suspicious_ts_orig(&event).await;

        // Validate event using EventValidator; capture the matched schema_id for persistence.
        let validated_schema_id = match self.validate_event(&event).await {
            Ok(schema_id) => schema_id,
            Err(e) => {
                warn!(event_id = ?event.id, "Event validation failed: {}", e);
                self.route_validation_failure(&msg, format!("Validation failed: {e}"))
                    .await?;
                return Ok(None);
            }
        };

        // Stamp the matched schema_id so it is persisted with the event row.
        // Only overwrite if validation actually matched a schema (None means no schema / disabled).
        let mut event = event;
        if let Some(sid) = validated_schema_id {
            event.payload_schema_id = Some(sid);
        }

        // The ID MUST be present for events coming from Ingestors
        let parsed_id = if let Some(id) = event.id {
            *id.as_uuid()
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
            let mut ready = Vec::with_capacity(batch.len());
            let mut not_ready = Vec::new();

            for prepared in batch {
                let is_ready = match &prepared.event.provenance {
                    // Material provenance: first consult the in-memory set, then fall back
                    // to the registry so externally-registered materials are not deferred forever.
                    Provenance::Material { id, .. } => {
                        ready_set.ensure_ready(&self.pool, *id.as_uuid()).await?
                    }
                    // Synthesis provenance has no material FK — always ready.
                    Provenance::Synthesis { .. } => true,
                };

                if is_ready {
                    ready.push(prepared);
                } else {
                    not_ready.push(prepared);
                }
            }

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

        self.persist_and_confirm_prepared_batch(&batch).await
    }

    #[tracing::instrument(skip(self, batch), fields(batch_size = batch.len()))]
    async fn persist_and_confirm_prepared_batch(
        &self,
        batch: &[&PreparedEvent],
    ) -> IngestdResult<()> {
        let mut pending_batches = vec![batch.to_vec()];

        while let Some(batch) = pending_batches.pop() {
            let persist_result = self.persist_batch_optimized(&batch).await;
            match persist_result {
                Ok(persisted) => {
                    let persisted_set = persisted
                        .inserted_ids
                        .as_ref()
                        .map(|ids| ids.iter().copied().collect::<HashSet<_>>());
                    if let Some(fail_flag) = &self.post_persist_fail_once
                        && fail_flag.swap(false, Ordering::SeqCst)
                    {
                        return Err(SinexError::database("forced post-persist failure"));
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

                    let mut ack_messages = Vec::with_capacity(batch.len());
                    let mut nack_prepared = Vec::new();
                    for (result, prepared) in confirmation_results.iter().zip(batch.iter()) {
                        match result {
                            Ok(()) => {
                                if let Some(set) = &persisted_set
                                    && !set.contains(&prepared.parsed_id)
                                {
                                    debug!(
                                        event_id = %prepared.parsed_id,
                                        "Re-published confirmation for already persisted event"
                                    );
                                }
                                ack_messages.push(&prepared.message);
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
                                match self.enqueue_confirmation_retry(&prepared.parsed_id).await {
                                    Ok(()) => {
                                        info!(
                                            event_id = %prepared.parsed_id,
                                            "Queued durable confirmation retry after publish failure"
                                        );
                                        self.stats
                                            .confirmation_retries_enqueued
                                            .fetch_add(1, Ordering::Relaxed);
                                        ack_messages.push(&prepared.message);
                                    }
                                    Err(retry_err) => {
                                        error!(
                                            event_id = %prepared.parsed_id,
                                            error = %retry_err,
                                            "Failed to queue durable confirmation retry; falling back to raw message redelivery"
                                        );
                                        self.stats
                                            .confirmation_retry_failures
                                            .fetch_add(1, Ordering::Relaxed);
                                        nack_prepared.push(prepared);
                                    }
                                }
                            }
                        }
                    }

                    let ack_futs: Vec<_> =
                        ack_messages.iter().map(|message| message.ack()).collect();
                    let ack_results = join_all(ack_futs).await;
                    for result in &ack_results {
                        if let Err(e) = result {
                            return Err(SinexError::network(format!("Failed to ack: {e}")));
                        }
                    }

                    if !nack_prepared.is_empty() {
                        let nak_futs: Vec<_> = nack_prepared
                            .iter()
                            .map(|prepared| {
                                prepared
                                    .message
                                    .ack_with(jetstream::AckKind::Nak(Some(CONFIRM_RETRY_DELAY)))
                            })
                            .collect();
                        let nak_results = join_all(nak_futs).await;
                        let mut settlement_errors = Vec::new();
                        for (result, prepared) in nak_results.iter().zip(nack_prepared.iter()) {
                            if let Err(err) = result {
                                warn!(
                                    event_id = %prepared.parsed_id,
                                    error = %err,
                                    "Failed to NAK after confirmation publish failure"
                                );
                                self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                                settlement_errors.push((
                                    prepared.parsed_id,
                                    Self::message_settlement_failure(
                                        "failed to NAK after confirmation publish failure",
                                        prepared.parsed_id,
                                        err,
                                    ),
                                ));
                            }
                        }
                        let processed_count = ack_messages.len() as u64;
                        if processed_count > 0 {
                            self.stats
                                .events_processed
                                .fetch_add(processed_count, Ordering::Relaxed);
                            if let Some(ref handle) = self.heartbeat_handle {
                                handle.increment_events_processed(processed_count);
                            }
                        }
                        let failed_count = nack_prepared.len() as u64;
                        self.stats
                            .events_failed
                            .fetch_add(failed_count, Ordering::Relaxed);
                        if let Some(ref handle) = self.heartbeat_handle {
                            handle.record_error("confirmation publish failure");
                        }
                        Self::collapse_settlement_errors(
                            "confirmation publish retry settlement",
                            settlement_errors,
                        )?;
                        continue;
                    }

                    let count = ack_messages.len() as u64;
                    self.stats
                        .events_processed
                        .fetch_add(count, Ordering::Relaxed);
                    if let Some(ref handle) = self.heartbeat_handle {
                        handle.increment_events_processed(count);
                    }
                    info!("Processed and confirmed {} events", batch.len());
                }
                Err(e) => {
                    // Check if this is a transient FK violation (source material not yet registered).
                    // Safety net: the ready set should prevent most FK violations, but races are
                    // possible (e.g. material registered between ready-set check and DB insert).
                let is_fk_error = is_source_material_fk_violation_for_prepared_batch(&e, &batch);
                    if is_fk_error {
                        let mut settlement_errors = Vec::new();
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
                                settlement_errors.push((
                                    prepared.parsed_id,
                                    Self::message_settlement_failure(
                                        "failed to NAK after FK violation",
                                        prepared.parsed_id,
                                        &err,
                                    ),
                                ));
                            }
                        }
                        Self::collapse_settlement_errors("FK violation retry settlement", settlement_errors)?;
                        // Don't count as failed - this is a transient condition
                        continue;
                    }

                    if is_isolatable_batch_persistence_failure(&e) {
                        if batch.len() > 1 {
                            let split_at = batch.len() / 2;
                            warn!(
                                batch_size = batch.len(),
                                split_at,
                                sqlstate = ?e.context_map().get("sqlstate"),
                                constraint = ?e.context_map().get("constraint"),
                                "Splitting batch to isolate non-retryable persistence failure"
                            );
                            pending_batches.push(batch[split_at..].to_vec());
                            pending_batches.push(batch[..split_at].to_vec());
                            continue;
                        }

                        let prepared = batch[0];
                        warn!(
                            event_id = %prepared.parsed_id,
                            sqlstate = ?e.context_map().get("sqlstate"),
                            constraint = ?e.context_map().get("constraint"),
                            "Routing isolated non-retryable persistence failure to DLQ"
                        );
                        self.route_to_dlq_and_ack(
                            &prepared.message,
                            format!("Persistence error: {e}"),
                        )
                        .await?;
                        self.stats.events_failed.fetch_add(1, Ordering::Relaxed);
                        if let Some(ref handle) = self.heartbeat_handle {
                            handle.record_error("isolated persistence failure");
                        }
                        continue;
                    }

                    error!("Failed to persist batch: {}", e);
                    let mut settlement_errors = Vec::new();
                    for prepared in &batch {
                        if self.should_route_terminal_persistence_failure(&prepared.message) {
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
                                settlement_errors.push((
                                    prepared.parsed_id,
                                    Self::message_settlement_failure(
                                        "failed to route persistence error to DLQ",
                                        prepared.parsed_id,
                                        &err,
                                    ),
                                ));
                            }
                        } else if let Err(err) = prepared
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
                            settlement_errors.push((
                                prepared.parsed_id,
                                Self::message_settlement_failure(
                                    "failed to NAK after persistence failure",
                                    prepared.parsed_id,
                                    &err,
                                ),
                            ));
                        }
                    }
                    let failed_count = batch.len() as u64;
                    self.stats
                        .events_failed
                        .fetch_add(failed_count, Ordering::Relaxed);
                    if let Some(ref handle) = self.heartbeat_handle {
                        handle.record_error("batch persistence failure");
                    }
                    Self::collapse_settlement_errors("persistence failure settlement", settlement_errors)?;
                }
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

    /// Validate event against JSON schema.
    ///
    /// Returns the matched schema UUID on success (`None` when validation is disabled/no schema
    /// registered), so the caller can stamp `payload_schema_id` on the event before persistence.
    async fn validate_event(
        &self,
        event: &Event<JsonValue>,
    ) -> IngestdResult<Option<Uuid>> {
        // Domain type formats (EventSource, EventType) are validated at deserialization
        // time — if we hold them here, they're already valid.

        let guard = self.validator.read().await;
        let validation =
            guard.validate_payload_for(&event.source, &event.event_type, &event.payload);
        let strict_mode = guard.is_strict_mode();
        Self::resolve_validation_result(validation, strict_mode, &event.source, &event.event_type)
    }

    fn resolve_validation_result(
        validation: ValidationResult,
        strict_mode: bool,
        source: &sinex_primitives::domain::EventSource,
        event_type: &sinex_primitives::domain::EventType,
    ) -> IngestdResult<Option<Uuid>> {
        match validation {
            ValidationResult::Valid { schema_id } => Ok(Some(schema_id)),
            ValidationResult::Skipped => Ok(None),
            ValidationResult::NoSchema => {
                if strict_mode {
                    Err(SinexError::validation(format!(
                        "Strict validation enabled: event has no registered schema (source={}, event_type={})",
                        source, event_type
                    ))
                    .with_operation("jetstream_consumer.validate_event")
                    .with_context("strict_mode", "enabled"))
                } else {
                    Ok(None)
                }
            }
            ValidationResult::SchemaNotFound { schema_id } => {
                warn!(
                    schema_id = %schema_id,
                    source = %source,
                    event_type = %event_type,
                    "Schema referenced by validator lookup is missing from cache; accepting event without payload schema id"
                );
                Ok(None)
            }
            ValidationResult::Invalid { errors } => Err(SinexError::validation(format!(
                "Schema validation failed: {}",
                errors.join(", ")
            ))
            .with_operation("jetstream_consumer.validate_event")),
        }
    }

    /// Persist batch through `EventRepository::insert_stream_batch()`.
    ///
    /// The repository owns all routing decisions (`QueryBuilder` for small batches,
    /// COPY for large material-only batches, REPEATABLE READ for synthesis batches).
    /// The recent-ID cache acts as a prefilter only.
    #[tracing::instrument(skip(self, batch), fields(batch_size = batch.len()))]
    async fn persist_batch_optimized(
        &self,
        batch: &[&PreparedEvent],
    ) -> IngestdResult<PersistBatchResult> {
        if batch.is_empty() {
            return Ok(PersistBatchResult { inserted_ids: None });
        }

        if let Some(fail_flag) = &self.fail_once
            && fail_flag.swap(false, Ordering::SeqCst)
        {
            return Err(SinexError::database("forced transient failure"));
        }

        if let Some(remaining) = &self.db_failures_remaining
            && remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current.checked_sub(1)
                })
                .is_ok()
        {
            return Err(SinexError::database("forced persistent failure"));
        }

        let to_persist = self.filter_cached_batch(batch);
        if to_persist.is_empty() {
            return Ok(PersistBatchResult { inserted_ids: None });
        }

        let rows: Vec<StreamBatchRow> = to_persist
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

                Ok(StreamBatchRow {
                    id: prepared.parsed_id,
                    source: event.source.clone(),
                    event_type: event.event_type.clone(),
                    ts_orig: event.ts_orig.ok_or_else(|| {
                        SinexError::validation("validated event missing ts_orig")
                            .with_context("event_id", prepared.parsed_id.to_string())
                            .with_context("source", event.source.as_str().to_string())
                            .with_context("event_type", event.event_type.as_str().to_string())
                    })?,
                    host: event.host.clone(),
                    payload: event.payload.clone(),
                    source_material_id,
                    anchor_byte,
                    offset_start,
                    offset_end,
                    offset_kind,
                    source_event_ids,
                    payload_schema_id: event.payload_schema_id,
                    node_run_id: event.node_run_id,
                    associated_blob_ids: event.associated_blob_ids.clone(),
                    temporal_policy: event.temporal_policy.map(|p| p.to_string()),
                    semantics_version: event.semantics_version.clone(),
                    scope_key: event.scope_key.clone(),
                    equivalence_key: event.equivalence_key.clone(),
                    created_by_operation_id: event.created_by_operation_id,
                    node_model: event.node_model.map(|m| m.to_string()),
                })
            })
            .collect::<IngestdResult<Vec<_>>>()?;

        let result = timeout(
            DB_WRITE_TIMEOUT,
            self.pool.events().insert_stream_batch(&rows),
        )
        .await
        .map_err(|_| {
            error!(
                batch_size = to_persist.len(),
                timeout_seconds = DB_WRITE_TIMEOUT.as_secs(),
                "Timed out waiting for batch insert to complete"
            );
            SinexError::database(format!(
                "Persisting batch timed out after {DB_WRITE_TIMEOUT:?}"
            ))
        })?
        .map_err(|err| {
            if is_source_material_fk_violation_for_stream_batch(&err, &rows) {
                warn!(
                    batch_size = to_persist.len(),
                    "INSERT hit FK violation (source_material not yet registered); will retry"
                );
            } else {
                error!("Failed to persist events batch: {}", err);
            }
            err
        })?;

        let inserted_ids = Self::require_inserted_ids(result.inserted_ids, to_persist.len())?;
        self.remember_batch(batch);
        Ok(PersistBatchResult {
            inserted_ids: Some(inserted_ids),
        })
    }

    fn should_route_terminal_persistence_failure(&self, msg: &jetstream::Message) -> bool {
        if self.route_db_errors_to_dlq {
            return true;
        }

        match msg.info() {
            Ok(info) => info.delivered >= MAIN_CONSUMER_MAX_DELIVER,
            Err(error) => {
                warn!(
                    error = %error,
                    "Failed to inspect JetStream delivery metadata for persistence failure"
                );
                false
            }
        }
    }

    fn message_settlement_failure(
        operation: &'static str,
        event_id: Uuid,
        error: impl std::fmt::Display,
    ) -> SinexError {
        SinexError::network(operation)
            .with_context("event_id", event_id.to_string())
            .with_source(error.to_string())
    }

    fn collapse_settlement_errors(
        stage: &'static str,
        mut errors: Vec<(Uuid, SinexError)>,
    ) -> IngestdResult<()> {
        if errors.is_empty() {
            return Ok(());
        }

        let (event_id, error) = errors.remove(0);
        let mut combined = error
            .with_context("settlement_stage", stage)
            .with_context("event_id", event_id.to_string());
        for (index, (event_id, extra)) in errors.into_iter().enumerate() {
            combined = combined
                .with_context(
                    format!("additional_settlement_event_id_{}", index + 1),
                    event_id.to_string(),
                )
                .with_context(
                    format!("additional_settlement_error_{}", index + 1),
                    extra.to_string(),
                );
        }
        Err(combined)
    }

    fn filter_cached_batch<'a>(&self, batch: &[&'a PreparedEvent]) -> Vec<&'a PreparedEvent> {
        // Clone cache snapshot then release the lock immediately — don't hold it
        // across the entire batch scan, which would block the writer.
        let cached_ids = {
            let cache = self.recent_id_cache.lock().unwrap_or_else(|poisoned| {
                warn!(
                    "Recent ID cache mutex was poisoned; recovering with potentially inconsistent data"
                );
                poisoned.into_inner()
            });
            cache.clone()
        };
        let mut seen = HashSet::new();
        batch
            .iter()
            .filter(|event| {
                if cached_ids.contains(&event.parsed_id) {
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

    /// Publish confirmation to NATS
    async fn publish_confirmation(&self, event_id: &Uuid) -> IngestdResult<()> {
        if let Some(failures) = &self.confirmation_failures_remaining
            && failures.load(Ordering::SeqCst) > 0
        {
            failures.fetch_sub(1, Ordering::SeqCst);
            return Err(SinexError::network("forced confirmation publish failure"));
        }

        let event_id_str = event_id.to_string();
        let confirmation = Confirmation {
            event_id: event_id_str.clone(),
            persisted: true,
            ts_ingest: Timestamp::now(),
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

    async fn publish_confirmation_with_retry(&self, event_id: &Uuid) -> IngestdResult<()> {
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

    async fn enqueue_confirmation_retry(&self, event_id: &Uuid) -> IngestdResult<()> {
        let event_id_str = event_id.to_string();
        let subject = format!("{}{}", self.topology.confirmation_retry_prefix, event_id_str);
        let payload = serde_json::to_vec(&ConfirmationRetryRequest {
            event_id: event_id_str.clone(),
        })?;

        let mut headers = async_nats::HeaderMap::new();
        let retry_msg_id = format!("confirm-retry.{event_id_str}");
        headers.insert("Nats-Msg-Id", retry_msg_id.as_str());

        self.js
            .publish_with_headers(subject, headers, payload.into())
            .await
            .map_err(|e| {
                SinexError::network("Failed to enqueue confirmation retry").with_source(e)
            })?
            .await
            .map_err(|e| {
                SinexError::network("Confirmation retry enqueue ack failed").with_source(e)
            })?;

        Ok(())
    }

    async fn process_confirmation_retry_batch(
        &self,
        consumer: &jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
    ) -> IngestdResult<()> {
        let messages = pull_batch(
            consumer,
            CONFIRM_RETRY_BATCH_MAX_MESSAGES,
            CONFIRM_RETRY_BATCH_TIMEOUT,
        )
        .await
        .map_err(|e| {
            SinexError::network("Failed to fetch confirmation retry messages").with_source(e)
        })?;

        for message in messages {
            let retry = match serde_json::from_slice::<ConfirmationRetryRequest>(&message.payload) {
                Ok(retry) => retry,
                Err(err) => {
                    warn!(
                        error = %err,
                        "Failed to parse confirmation retry payload; acknowledging corrupt retry message"
                    );
                    if let Err(ack_err) = message.ack().await {
                        warn!(error = %ack_err, "Failed to ack corrupt confirmation retry message");
                        self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
            };

            let event_id = match Uuid::parse_str(&retry.event_id) {
                Ok(event_id) => event_id,
                Err(err) => {
                    warn!(
                        event_id = %retry.event_id,
                        error = %err,
                        "Confirmation retry payload contained an invalid event id; acknowledging corrupt retry message"
                    );
                    if let Err(ack_err) = message.ack().await {
                        warn!(error = %ack_err, "Failed to ack invalid confirmation retry message");
                        self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
            };

            match self.publish_confirmation_with_retry(&event_id).await {
                Ok(()) => {
                    if let Err(err) = message.ack().await {
                        return Err(SinexError::network(format!(
                            "Failed to ack confirmation retry message: {err}"
                        )));
                    }
                }
                Err(err) => {
                    warn!(
                        event_id = %event_id,
                        error = %err,
                        "Failed to publish confirmation from durable retry queue"
                    );
                    self.stats
                        .confirmation_retry_failures
                        .fetch_add(1, Ordering::Relaxed);
                    if let Some(ref handle) = self.heartbeat_handle {
                        handle.record_error("confirmation retry failure");
                    }
                    if let Err(nak_err) =
                        message.ack_with(jetstream::AckKind::Nak(Some(CONFIRM_RETRY_DELAY))).await
                    {
                        return Err(SinexError::network(format!(
                            "Failed to NAK confirmation retry message: {nak_err}"
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Route failed message to DLQ and return Ok(()) on success.
    ///
    /// Errors indicate the DLQ publish itself failed after all retries. The caller
    /// is responsible for deciding whether to NAK the original message in that case.
    #[tracing::instrument(skip(self, msg), fields(error = %error))]
    async fn route_to_dlq(&self, msg: &jetstream::Message, error: String) -> IngestdResult<()> {
        let original_nats_msg_id = msg
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Msg-Id"))
            .map(|v| v.as_str().to_string());

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
        let dlq_publish_msg_id =
            dlq_publish_msg_id(msg, original_nats_msg_id.as_deref(), &original_payload);
        let original_event_id = original_payload
            .get("id")
            .and_then(|value| value.as_str())
            .map(str::to_owned);

        let dlq_entry = DlqEntry {
            nats_msg_id: original_nats_msg_id,
            error,
            original_payload,
            failed_at: Timestamp::now(),
        };

        let payload = serde_json::to_vec(&dlq_entry).map_err(|e| {
            SinexError::serialization(format!("Failed to serialize DLQ entry: {e}"))
        })?;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", dlq_publish_msg_id.as_str());
        headers.insert("Original-Subject", msg.subject.as_str());
        headers.insert("Retry-Count", "0");
        if let Some(event_id) = original_event_id.as_deref() {
            headers.insert("Event-Id", event_id);
        }

        let mut backoff = DLQ_PUBLISH_BACKOFF_BASE;
        let mut last_error: Option<SinexError> = None;
        for attempt in 1..=DLQ_PUBLISH_MAX_ATTEMPTS {
            match self
                .js
                .publish_with_headers(
                    self.topology.dlq_publish_subject.clone(),
                    headers.clone(),
                    payload.clone().into(),
                )
                .await
            {
                Ok(ack) => match ack.await {
                    Ok(_) => {
                        debug!(nats_msg_id = ?dlq_entry.nats_msg_id, "Routed to DLQ");
                        return Ok(());
                    }
                    Err(err) => {
                        error!(attempt, error = %err, "Failed to confirm DLQ publish");
                        last_error =
                            Some(SinexError::network("DLQ publish ack failed").with_source(err));
                    }
                },
                Err(err) => {
                    error!(attempt, error = %err, "Failed to route to DLQ");
                    last_error = Some(SinexError::network("DLQ publish failed").with_source(err));
                }
            }

            if attempt < DLQ_PUBLISH_MAX_ATTEMPTS {
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff.saturating_mul(2), DLQ_PUBLISH_BACKOFF_MAX);
            }
        }

        Err(last_error
            .unwrap_or_else(|| SinexError::network("Failed to route to DLQ after retries")))
    }

    async fn route_to_dlq_and_ack(
        &self,
        msg: &jetstream::Message,
        error: String,
    ) -> IngestdResult<()> {
        match self.route_to_dlq(msg, error).await {
            Ok(()) => {
                msg.ack().await.map_err(|e| {
                    SinexError::network("Failed to ack after DLQ route").with_source(e)
                })?;
                self.stats.dlq_routed.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                warn!(error = %e, "Failed to route to DLQ after retries; NAKing for retry");
                self.stats
                    .dlq_publish_failures
                    .fetch_add(1, Ordering::Relaxed);
                msg.ack_with(jetstream::AckKind::Nak(Some(DLQ_RETRY_DELAY)))
                    .await
                    .map_err(|nak_err| {
                        self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                        SinexError::network(format!("Failed to NAK after DLQ failure: {nak_err}"))
                    })?;
            }
        }
        Ok(())
    }

    /// Check stream capacity and log warnings if approaching limits
    async fn check_stream_capacity(&self, stream_name: &str) {
        match self.js.get_stream(stream_name).await {
            Ok(mut stream) => {
                match stream.info().await {
                    Ok(info) => {
                        let state = info.state.clone();
                        let config = info.config.clone();

                        // Emit stream stats via self-observer
                        if let Some(ref observer) = self.observer
                            && let Err(error) = observer
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
                            Self::log_observer_error("ingestd.stream", &error);
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
                    Err(error) => {
                        debug!(
                            stream = %stream_name,
                            error = %error,
                            "Failed to inspect stream capacity"
                        );
                    }
                }
            }
            Err(e) => {
                debug!("Failed to check stream capacity for {}: {}", stream_name, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Inline because the behavior under test is a private validation-mapping helper.
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn schema_not_found_is_accepted_leniently() -> TestResult<()> {
        let accepted = JetStreamConsumer::resolve_validation_result(
            ValidationResult::SchemaNotFound {
                schema_id: Uuid::now_v7(),
            },
            false,
            &sinex_primitives::domain::EventSource::from_static("test"),
            &sinex_primitives::domain::EventType::from_static("schema.missing"),
        )?;
        assert!(accepted.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn missing_schema_binding_is_accepted_leniently() -> TestResult<()> {
        let accepted = JetStreamConsumer::resolve_validation_result(
            ValidationResult::NoSchema,
            false,
            &sinex_primitives::domain::EventSource::from_static("test"),
            &sinex_primitives::domain::EventType::from_static("schema.missing"),
        )?;
        assert!(accepted.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn strict_mode_still_rejects_missing_schema_bindings() -> TestResult<()> {
        let err = JetStreamConsumer::resolve_validation_result(
            ValidationResult::NoSchema,
            true,
            &sinex_primitives::domain::EventSource::from_static("test"),
            &sinex_primitives::domain::EventType::from_static("schema.missing"),
        )
        .expect_err("strict mode must reject events without schema bindings");

        assert!(err.to_string().contains("Strict validation enabled"));
        Ok(())
    }

    #[sinex_test]
    async fn require_inserted_ids_accepts_present_repository_ids() -> TestResult<()> {
        let ids = vec![Uuid::now_v7()];
        let accepted = JetStreamConsumer::require_inserted_ids(Some(ids.clone()), 1)?;
        assert_eq!(accepted, ids);
        Ok(())
    }

    #[sinex_test]
    async fn require_inserted_ids_rejects_missing_repository_ids() -> TestResult<()> {
        let err = JetStreamConsumer::require_inserted_ids(None, 2)
            .expect_err("missing inserted_ids must surface as an invalid repository contract");
        assert!(
            err.to_string().contains("Event repository omitted inserted_ids"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn suspicious_future_ts_orig_has_one_hour_soft_threshold() -> TestResult<()> {
        let now = Timestamp::now();
        assert!(!is_suspicious_future_ts_orig(
            now + time::Duration::minutes(59),
            now
        ));
        assert!(is_suspicious_future_ts_orig(
            now + time::Duration::minutes(61),
            now
        ));
        Ok(())
    }

    #[sinex_test]
    async fn ready_signal_reports_dropped_receiver() -> TestResult<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        drop(rx);

        assert!(!signal_ready(Some(tx), "jetstream-consumer"));
        Ok(())
    }

    #[sinex_test]
    async fn collapse_settlement_errors_preserves_additional_failures() -> TestResult<()> {
        let first = Uuid::now_v7();
        let second = Uuid::now_v7();

        let error = JetStreamConsumer::collapse_settlement_errors(
            "persistence failure settlement",
            vec![
                (
                    first,
                    JetStreamConsumer::message_settlement_failure(
                        "failed to NAK after persistence failure",
                        first,
                        "first boom",
                    ),
                ),
                (
                    second,
                    JetStreamConsumer::message_settlement_failure(
                        "failed to route persistence error to DLQ",
                        second,
                        "second boom",
                    ),
                ),
            ],
        )
        .expect_err("multiple settlement failures must stay visible");

        let rendered = error.to_string();
        assert!(rendered.contains("failed to NAK after persistence failure"));
        let second_id = second.to_string();
        assert_eq!(
            error
                .context_map()
                .get("additional_settlement_event_id_1")
                .map(String::as_str),
            Some(second_id.as_str())
        );
        let extra = error
            .context_map()
            .get("additional_settlement_error_1")
            .expect("extra settlement error should stay attached");
        assert!(extra.contains("failed to route persistence error to DLQ"));
        Ok(())
    }
}
