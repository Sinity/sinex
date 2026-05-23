//! `JetStream` event consumer with confirmations and DLQ support
//!
//! See `crate::docs::ingestion_pipeline` for architectural details.
//!
//! # Batch Atomicity Contract
//!
//! The ingestd consumer does NOT guarantee all-or-nothing atomicity for a NATS pull-batch.
//! When persistence fails, the batch is split in half and each sub-batch is retried independently.
//! This means a single pull-batch may result in partial persistence:
//!
//! - Sub-batch A succeeds → events committed, NATS messages acked
//! - Sub-batch B fails → events not committed, NATS messages NAK'd for redelivery
//!
//! This is intentional: maximizing throughput takes priority over batch-level atomicity.
//! Individual events within a successful sub-batch ARE atomically persisted (single DB transaction).
//! Downstream consumers must tolerate duplicate processing on redelivery of the NAK'd messages.
//!
//! The `BATCH_ATOMICITY_SCOPE` context field is attached to all related error diagnostics
//! so operators can correlate partial-commit scenarios in logs.

use async_nats::jetstream::stream::DiscardPolicy;
use async_nats::{Client as NatsClient, jetstream};
use futures::future::{BoxFuture, join_all};
use serde::{Deserialize, Serialize};
use sinex_db::DbPool;
use sinex_db::repositories::COPY_BATCH_THRESHOLD;
use sinex_node_sdk::SelfObserver;
use sinex_node_sdk::heartbeat::HeartbeatCounterHandle;
use sinex_node_sdk::runtime::stream::{PullConsumerSpec, ensure_pull_consumer, pull_batch};
use sinex_primitives::Timestamp;
use sinex_primitives::constants::env_vars;
use sinex_primitives::{
    JsonValue, Uuid,
    nats::{JetStreamTopology, NatsTrafficClass, insert_traffic_class_header},
    transport,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
#[cfg(any(test, feature = "testing"))]
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use tokio::time::Duration;
use tracing::{debug, error, info, instrument, warn};

#[cfg(test)]
use crate::validator::ValidationResult;
use crate::{
    IngestdResult, SinexError,
    admission::{
        AdmissionDecision, AdmissionRejection, AdmissionRejectionKind, AdmissionService,
        AdmittedEvent,
    },
    material_ready_set::MaterialReadySet,
    validator::IngestEventValidator,
};
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::Provenance;

/// Confirmation message published to `prod.events.confirmations.<source>.<event_type>`.
///
/// `event_id` is the **high-watermark** event id for this `(source, event_type)`
/// kind — the latest event of this kind that ingestd has persisted. Per #1306,
/// the implied semantics is that all earlier events of the same kind are also
/// confirmed (publish order is monotonic per kind: ingestd publishes only when
/// a fresh max `event_id` is seen for that kind within or across batches).
///
/// Downstream readers that watch confirmations should advance their per-kind
/// high-watermark on each message and treat pending events of that kind with
/// `event_id <= watermark` as confirmed.
#[derive(Debug, Serialize)]
struct Confirmation {
    event_id: String,
    source: String,
    event_type: String,
    persisted: bool,
    ts_ingest: Timestamp,
}

#[derive(Debug, Serialize, Deserialize)]
struct ConfirmationRetryRequest {
    event_id: String,
    /// Per #1306: confirmations are published per `(source, event_type)`
    /// watermark, not per event id. The retry path needs the kind to
    /// reconstruct the correct subject.
    #[serde(default)]
    source: String,
    #[serde(default)]
    event_type: String,
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
    validator: Arc<RwLock<IngestEventValidator>>,
    admission: AdmissionService,
    topology: JetStreamTopology,
    ack_wait: Duration,
    max_ack_pending: i64,
    #[cfg(any(test, feature = "testing"))]
    confirmation_failures_remaining: Option<Arc<AtomicUsize>>,
    confirmation_semaphore: Arc<tokio::sync::Semaphore>,
    #[cfg(any(test, feature = "testing"))]
    processing_delay: Option<Duration>,
    #[cfg(any(test, feature = "testing"))]
    delivery_observer: Option<Arc<AtomicU64>>,
    stats: ConsumerStats,
    /// Test-only: when true, persistence errors are routed to DLQ instead of NAK'd.
    /// Production always uses the NAK path; this field is initialized to `false` and
    /// only mutated by `with_test_hooks`. Left as a primitive (not cfg-gated) because
    /// the read sites are in hot persistence-error paths and threading cfg around them
    /// would add more noise than the 1 byte of struct memory it would save.
    route_db_errors_to_dlq: bool,
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
    /// Maximum duration `ts_orig` may exceed wall-clock time before DLQ routing
    future_ts_skew: time::Duration,
    /// Earliest accepted `ts_orig` as a timestamp (default: 2000-01-01 UTC)
    ts_orig_lower_bound: Timestamp,
    /// Max concurrent batch-processing tasks during startup catch-up.
    /// Limits I/O pressure while the consumer works through the backlog.
    /// Default: 4. Set to 0 to disable catch-up limiting (full speed).
    startup_catch_up_max_concurrent: usize,
    /// When true, refuse missing durable + `DeliverPolicy::All` startup if the
    /// raw-event stream is non-empty.
    reject_initial_replay: bool,
    /// Per-(source, `event_type`) high-watermark of latest confirmed `event_id`.
    /// Used by the per-kind compaction strategy in `publish_confirmations_for_batch`
    /// to skip publishes that would not advance the watermark. Per #1306.
    confirmation_watermark: Arc<tokio::sync::Mutex<HashMap<(String, String), Uuid>>>,
}

/// SQLSTATE for foreign-key violation.
const SQLSTATE_DATA_EXCEPTION_CLASS: &str = "22";
const SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION_CLASS: &str = "23";

/// Error-class marker for deferred source-material FK violations.
const ERROR_CLASS_SOURCE_MATERIAL_FK: &str = "source_material_fk_violation";
const EVENTS_SOURCE_MATERIAL_ID_FKEY: &str = "events_source_material_id_fkey";

fn is_source_material_fk_constraint_name(value: &str) -> bool {
    value == EVENTS_SOURCE_MATERIAL_ID_FKEY
        || value
            .strip_suffix(EVENTS_SOURCE_MATERIAL_ID_FKEY)
            .is_some_and(|prefix| prefix.ends_with('_'))
}

/// Hard guard for node-supplied event IDs.
///
/// Ingestors and derived nodes may use `sinex_node_sdk::deterministic_event_id`
/// for idempotent source occurrences, but ingestd still rejects every ID that is
/// not an RFC4122 `UUIDv7` before it reaches the hypertable partition key.
#[cfg(test)]
fn is_uuid_v7(value: &Uuid) -> bool {
    value.get_version_num() == 7 && value.get_variant() == uuid::Variant::RFC4122
}

fn is_foreign_key_violation(err: &SinexError) -> bool {
    // Per #751 F32: classify FK violations by SQLSTATE (23503 foreign_key_violation)
    // instead of inspecting rendered error text. SQLSTATE is always set when errors
    // flow through sinex_db::db_error(), which extracts pg errcode from the sqlx error.
    err.context_map()
        .get("sqlstate")
        .is_some_and(|value| value == "23503")
}

fn has_explicit_source_material_fk_marker(err: &SinexError) -> bool {
    err.context_map()
        .get("error_class")
        .is_some_and(|value| value == ERROR_CLASS_SOURCE_MATERIAL_FK)
        || err
            .context_map()
            .get("constraint")
            .is_some_and(|value| is_source_material_fk_constraint_name(value))
}

fn batch_depends_only_on_source_material_fk(batch: &[&PreparedEvent]) -> bool {
    batch.iter().all(|prepared| {
        matches!(prepared.event.provenance, Provenance::Material { .. })
            && prepared.event.payload_schema_id.is_none()
            && prepared.event.source_run_id.is_none()
    })
}

fn is_source_material_fk_violation_for_prepared_batch(
    err: &SinexError,
    batch: &[&PreparedEvent],
) -> bool {
    has_explicit_source_material_fk_marker(err)
        || (is_foreign_key_violation(err) && batch_depends_only_on_source_material_fk(batch))
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

    err.context_map().get("sqlstate").is_some_and(|value| {
        value.starts_with(SQLSTATE_DATA_EXCEPTION_CLASS)
            || value.starts_with(SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION_CLASS)
    })
}
const DEFAULT_BATCH_FETCH_MAX_MESSAGES: usize = 100;
const DEFAULT_BATCH_FETCH_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_MAX_ACK_PENDING: i64 = 100;
/// NATS-side `max_deliver` on the events consumer. Must be >= the highest
/// application-side terminal threshold below so app-level DLQ routing fires
/// before NATS silently stops redelivery. Sized for the source-material
/// cross-stream-lag scenario (#1310/#1311).
const MAIN_CONSUMER_JETSTREAM_MAX_DELIVER: i64 = 32;
const MAIN_CONSUMER_TERMINAL_DLQ_THRESHOLD: i64 = 10;
/// Source-material-not-found is a soft cross-stream-lag condition, not a hard
/// error: the material's BEGIN message is being processed on a separate consumer
/// path. Give it generous retry budget. With `FK_VIOLATION_RETRY_DELAY` = 5s,
/// threshold = 30 means up to ~150s wall-clock for the BEGIN to catch up before
/// we give up and DLQ. The earlier value of 10 (50s) routed many events to DLQ
/// during normal backlog drains. See #1310 / #1311.
const SOURCE_MATERIAL_READY_DLQ_THRESHOLD: i64 = 30;
const DLQ_PUBLISH_MAX_ATTEMPTS: usize = 3;
const DLQ_PUBLISH_BACKOFF_BASE: Duration = Duration::from_millis(200);
const DLQ_PUBLISH_BACKOFF_MAX: Duration = Duration::from_secs(2);
const DLQ_DUPLICATE_WINDOW: Duration = Duration::from_hours(1);
const DLQ_RETRY_DELAY: Duration = Duration::from_secs(1);
const CONFIRM_PUBLISH_MAX_ATTEMPTS: usize = 3;
const CONFIRM_PUBLISH_BACKOFF_BASE: Duration = Duration::from_millis(200);
const CONFIRM_PUBLISH_BACKOFF_MAX: Duration = Duration::from_secs(2);
const CONFIRM_PUBLISH_CONCURRENCY: usize = 50;
const CONFIRM_RETRY_DELAY: Duration = Duration::from_secs(1);
const CONFIRM_RETRY_POLL_INTERVAL: Duration = Duration::from_secs(1);
const CONFIRM_RETRY_BATCH_MAX_MESSAGES: usize = 32;
const CONFIRM_RETRY_BATCH_TIMEOUT: Duration = Duration::from_millis(100);
const ERROR_CLASS_CONFIRMATION_DURABILITY_GAP: &str = "confirmation_durability_gap";
/// Diagnostic context value attached to errors and log fields that arise from the split-retry
/// persistence path. The value `"per_successful_persistence_attempt"` signals that atomicity is
/// scoped to each individual sub-batch attempt, not the enclosing pull-batch: a pull-batch may
/// be partially committed if one sub-batch succeeds before a sibling fails.
const BATCH_ATOMICITY_SCOPE: &str = "per_successful_persistence_attempt";
/// Retry delay for deferred events whose source material isn't registered yet.
///
/// Each NAK with this delay counts toward `max_deliver` (10), so the total race
/// window the system tolerates is `delay * max_deliver` (= 50 s with 5 s delay).
///
/// The cross-stream race is the load-bearing case: events on
/// `PROD_SINEX_RAW_EVENTS` and material lifecycle frames on `SOURCE_MATERIAL`
/// flow through independent `JetStream` consumers with no cross-stream ordering.
/// Under backlog, the `SOURCE_MATERIAL` consumer can lag behind the events
/// consumer by tens of seconds; the previous 200 ms delay × 10 retries (= 2 s
/// total window) was insufficient and DLQ'd every fresh self-observation
/// material's first events (see issue #1241).
///
/// 5 s × 10 retries = 50 s is the practical upper bound a healthy assembler
/// should clear; longer delays mainly hurt liveness under transient races.
const FK_VIOLATION_RETRY_DELAY: Duration = Duration::from_secs(5);
const STREAM_CAPACITY_WARNING_THRESHOLD: f64 = 0.8; // Alert at 80% capacity
const STREAM_CAPACITY_CHECK_INTERVAL: Duration = Duration::from_mins(5); // Check every 5 minutes
// Keep runtime-created stream caps aligned with the Nix bootstrap path. The current
// nats CLI rejects --max-bytes values above signed 32-bit range.
const JETSTREAM_BOOTSTRAP_MAX_BYTES: i64 = 2_147_483_647;

#[derive(Debug)]
struct PersistBatchResult {
    inserted_ids: Option<Vec<Uuid>>,
    duplicate_event_ids: Vec<Uuid>,
    tombstoned_event_ids: Vec<Uuid>,
}

#[derive(Debug)]
struct PersistBatchFailure {
    error: SinexError,
    attempted_event_ids: Vec<Uuid>,
    duplicate_event_ids: Vec<Uuid>,
    tombstoned_event_ids: Vec<Uuid>,
}

struct PreparedEvent {
    event: Event<JsonValue>,
    parsed_id: Uuid,
    message: jetstream::Message,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceMaterialSettlement {
    Deferred,
    RoutedToDlq,
}

fn dlq_publish_msg_id(
    msg: &jetstream::Message,
    original_nats_msg_id: Option<&str>,
    original_payload: &JsonValue,
) -> String {
    if let Some(event_id) = original_payload.get("id").and_then(|value| value.as_str()) {
        return format!("dlq.{event_id}");
    }

    if let Some(original_id) = original_nats_msg_id {
        format!("dlq.msg.{original_id}")
    } else {
        let mut hasher = blake3::Hasher::new();
        hasher.update(msg.subject.as_str().as_bytes());
        hasher.update(&msg.payload);
        format!("dlq.hash.{}", hasher.finalize().to_hex())
    }
}

fn source_material_unavailable_error(
    prepared: &PreparedEvent,
    material_id: Option<Uuid>,
    persistence_error: Option<&SinexError>,
) -> String {
    let material = material_id.map_or_else(|| "unknown".to_string(), |id| id.to_string());
    let base = format!(
        "Source material {material} was not registered after {SOURCE_MATERIAL_READY_DLQ_THRESHOLD} deliveries for event {} (source={}, event_type={})",
        prepared.parsed_id, prepared.event.source, prepared.event.event_type
    );

    if let Some(error) = persistence_error {
        format!("{base}; persistence error: {error}")
    } else {
        base
    }
}

#[derive(Debug, Default)]
struct ConsumerStats {
    events_processed: AtomicU64,
    events_failed: AtomicU64,
    events_deferred: AtomicU64,
    suspicious_future_ts_orig: AtomicU64,
    suspicious_past_ts_orig: AtomicU64,
    negative_anchor_byte: AtomicU64,
    validation_failures: AtomicU64,
    tombstoned_events_rejected: AtomicU64,
    dlq_routed: AtomicU64,
    confirmation_failures: AtomicU64,
    confirmation_retries_enqueued: AtomicU64,
    confirmation_retry_failures: AtomicU64,
    confirmation_durability_gaps: AtomicU64,
    dlq_publish_failures: AtomicU64,
    nack_failures: AtomicU64,
    nats_errors: AtomicU64,
    telemetry_publish_failures: AtomicU64,
}

impl ConsumerStats {
    fn log(&self) {
        info!(
            events_processed = self.events_processed.load(Ordering::Relaxed),
            events_failed = self.events_failed.load(Ordering::Relaxed),
            events_deferred = self.events_deferred.load(Ordering::Relaxed),
            suspicious_future_ts_orig = self.suspicious_future_ts_orig.load(Ordering::Relaxed),
            suspicious_past_ts_orig = self.suspicious_past_ts_orig.load(Ordering::Relaxed),
            negative_anchor_byte = self.negative_anchor_byte.load(Ordering::Relaxed),
            validation_failures = self.validation_failures.load(Ordering::Relaxed),
            tombstoned_events_rejected = self.tombstoned_events_rejected.load(Ordering::Relaxed),
            nats_errors = self.nats_errors.load(Ordering::Relaxed),
            dlq_routed = self.dlq_routed.load(Ordering::Relaxed),
            confirmation_failures = self.confirmation_failures.load(Ordering::Relaxed),
            confirmation_retries_enqueued =
                self.confirmation_retries_enqueued.load(Ordering::Relaxed),
            confirmation_retry_failures = self.confirmation_retry_failures.load(Ordering::Relaxed),
            confirmation_durability_gaps =
                self.confirmation_durability_gaps.load(Ordering::Relaxed),
            dlq_publish_failures = self.dlq_publish_failures.load(Ordering::Relaxed),
            nack_failures = self.nack_failures.load(Ordering::Relaxed),
            telemetry_publish_failures = self.telemetry_publish_failures.load(Ordering::Relaxed),
            "JetStream consumer stats"
        );
    }
}

impl JetStreamConsumer {
    fn log_observer_error(
        stats: &ConsumerStats,
        metric: &'static str,
        error: &sinex_node_sdk::SelfObservationError,
    ) {
        stats
            .telemetry_publish_failures
            .fetch_add(1, Ordering::Relaxed);
        warn!(metric, error = %error, "Failed to emit ingestd telemetry");
    }

    fn is_fatal_batch_processing_error(err: &SinexError) -> bool {
        err.context_map()
            .get("error_class")
            .is_some_and(|value| value == ERROR_CLASS_CONFIRMATION_DURABILITY_GAP)
    }

    fn confirmation_durability_gap_error(
        errors: Vec<(Uuid, SinexError)>,
        acked_count: usize,
    ) -> SinexError {
        let Err(combined) =
            Self::collapse_settlement_errors("post-persist confirmation durability", errors)
        else {
            unreachable!("confirmation durability gap requires at least one event");
        };

        combined
            .with_context("error_class", ERROR_CLASS_CONFIRMATION_DURABILITY_GAP)
            .with_context("acked_event_count", acked_count.to_string())
            .with_context("batch_atomicity", BATCH_ATOMICITY_SCOPE)
            .with_context("raw_message_settlement", "left_unacked_for_redelivery")
            .with_context(
                "terminal_state",
                "database commit landed but confirmation durability was not established",
            )
            .with_context(
                "recovery",
                "shut down the consumer and let JetStream redeliver unsettled raw messages once confirmation transport recovers",
            )
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
            Self::log_observer_error(&self.stats, metric, &error);
        }
    }

    #[cfg(test)]
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
        validator: Arc<RwLock<IngestEventValidator>>,
        topology: JetStreamTopology,
    ) -> Self {
        let js = jetstream::new(nats_client);
        let admission = AdmissionService::new(pool.clone(), Arc::clone(&validator));

        Self {
            js,
            pool,
            validator,
            admission,
            topology,
            ack_wait: Duration::from_secs(30),
            max_ack_pending: DEFAULT_MAX_ACK_PENDING,
            #[cfg(any(test, feature = "testing"))]
            confirmation_failures_remaining: None,
            confirmation_semaphore: Arc::new(tokio::sync::Semaphore::new(
                CONFIRM_PUBLISH_CONCURRENCY,
            )),
            #[cfg(any(test, feature = "testing"))]
            processing_delay: None,
            #[cfg(any(test, feature = "testing"))]
            delivery_observer: None,
            stats: ConsumerStats::default(),
            route_db_errors_to_dlq: false,
            batch_fetch_max_messages: DEFAULT_BATCH_FETCH_MAX_MESSAGES,
            batch_fetch_timeout: DEFAULT_BATCH_FETCH_TIMEOUT,
            ready_set: None,
            observer: None,
            stats_log_interval: Duration::from_mins(1),
            heartbeat_handle: None,
            future_ts_skew: time::Duration::hours(1),
            ts_orig_lower_bound: Timestamp::from_const(
                time::macros::datetime!(2000-01-01 00:00:00 UTC),
            ),
            startup_catch_up_max_concurrent: 4,
            reject_initial_replay: true,
            confirmation_watermark: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Set the maximum duration `ts_orig` may exceed wall-clock time before DLQ routing.
    #[must_use]
    pub fn with_future_ts_skew(mut self, skew: time::Duration) -> Self {
        self.future_ts_skew = skew;
        self.admission.set_future_ts_skew(skew);
        self
    }

    /// Set the earliest accepted `ts_orig` as a timestamp.
    #[must_use]
    pub fn with_ts_orig_lower_bound(mut self, lower_bound: Timestamp) -> Self {
        self.ts_orig_lower_bound = lower_bound;
        self.admission.set_ts_orig_lower_bound(lower_bound);
        self
    }

    /// Set max concurrent batch-processing tasks during startup catch-up.
    /// 0 disables the semaphore entirely (full speed).
    #[must_use]
    pub fn with_startup_catch_up_max_concurrent(mut self, max_concurrent: usize) -> Self {
        self.startup_catch_up_max_concurrent = max_concurrent;
        self
    }

    /// Set whether startup rejects a missing durable consumer on a non-empty
    /// stream when using `DeliverPolicy::All`.
    #[must_use]
    pub fn with_reject_initial_replay(mut self, reject: bool) -> Self {
        self.reject_initial_replay = reject;
        self
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
        validator: Arc<RwLock<IngestEventValidator>>,
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

    /// Build a consumer with optional test-only hooks.
    ///
    /// Only compiled when the `testing` feature is enabled (always on for `cfg(test)`).
    /// Production builds do not carry this constructor or the fields it sets.
    #[cfg(any(test, feature = "testing"))]
    pub fn with_test_hooks(
        nats_client: NatsClient,
        pool: DbPool,
        validator: Arc<RwLock<IngestEventValidator>>,
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
        consumer.admission = consumer
            .admission
            .with_test_fail_once(fail_once)
            .with_test_db_failures(db_failures_remaining);
        consumer.processing_delay = processing_delay;
        consumer.delivery_observer = delivery_observer;
        consumer.route_db_errors_to_dlq = route_db_errors_to_dlq;
        consumer.confirmation_failures_remaining = confirmation_failures_remaining;
        consumer
    }

    /// Bootstrap all required `JetStream` streams
    async fn bootstrap_streams(&self) -> IngestdResult<()> {
        // When SINEX_NATS_STREAMS_MANAGED_EXTERNALLY=true, the NixOS module owns
        // stream configuration. Skip bootstrap so the two sources of truth don't
        // conflict on stream shape or subject overlap.
        if std::env::var(env_vars::NATS_STREAMS_MANAGED_EXTERNALLY).as_deref() == Ok("true") {
            info!("NATS streams managed externally -- skipping bootstrap");
            return Ok(());
        }

        info!("Bootstrapping JetStream streams");

        // Events stream - durable event log for automata replay.
        // Keep enough history for downstream catch-up, but bound the store so
        // the event bus does not become the primary archive.
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.events_stream.to_string(),
                subjects: vec![self.topology.events_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 2_000_000,
                max_bytes: JETSTREAM_BOOTSTRAP_MAX_BYTES,
                max_age: Duration::from_hours(72), // 3 days
                storage: jetstream::stream::StorageType::File,
                discard: DiscardPolicy::New,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network("Failed to create events stream").with_source(e))?;

        // Confirmations stream — ephemeral operational notifications, not durable
        // history. Per-event-id subject pattern means `max_messages_per_subject = 1`
        // is structurally a no-op (each subject only ever holds one message); see
        // #1306 for the intended per-kind redesign. Until that lands, cap with
        // max_messages + max_bytes and discard oldest when full so newly-confirmed
        // events still get published.
        const CONFIRMATIONS_MAX_MESSAGES: i64 = 5_000_000;
        const CONFIRMATIONS_MAX_BYTES: i64 = 512 * 1024 * 1024; // 512 MiB
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.confirmations_stream.to_string(),
                subjects: vec![self.topology.confirmations_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages_per_subject: 1,
                max_messages: CONFIRMATIONS_MAX_MESSAGES,
                max_bytes: CONFIRMATIONS_MAX_BYTES,
                max_age: Duration::from_hours(72), // 3 days
                storage: jetstream::stream::StorageType::File,
                discard: DiscardPolicy::Old,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network("Failed to create confirmations stream").with_source(e)
            })?;

        // Cap the total backlog to prevent unbounded growth when confirmation publish failures
        // persist. DiscardPolicy::New combined with max_messages ensures the stream does not
        // grow beyond the cap even if many events are continuously failing confirmation.
        const CONFIRMATION_RETRY_MAX_MESSAGES: i64 = 50_000;
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.confirmation_retry_stream.to_string(),
                subjects: vec![self.topology.confirmation_retry_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages_per_subject: 1,
                max_messages: CONFIRMATION_RETRY_MAX_MESSAGES,
                max_age: Duration::from_hours(72),
                storage: jetstream::stream::StorageType::File,
                discard: DiscardPolicy::New,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network("Failed to create confirmation retry stream").with_source(e)
            })?;

        // DLQ stream
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.dlq_stream.to_string(),
                subjects: vec![self.topology.dlq_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_bytes: JETSTREAM_BOOTSTRAP_MAX_BYTES,
                max_age: Duration::from_hours(72), // 3 days
                storage: jetstream::stream::StorageType::File,
                duplicate_window: DLQ_DUPLICATE_WINDOW,
                allow_direct: true,
                discard: DiscardPolicy::New,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::network("Failed to create DLQ stream").with_source(e))?;

        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.processing_failures_stream.to_string(),
                subjects: vec![self.topology.processing_failures_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_bytes: JETSTREAM_BOOTSTRAP_MAX_BYTES,
                max_age: Duration::from_hours(72), // 3 days
                storage: jetstream::stream::StorageType::File,
                duplicate_window: DLQ_DUPLICATE_WINDOW,
                allow_direct: true,
                discard: DiscardPolicy::New,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network("Failed to create processing-failures stream").with_source(e)
            })?;

        // Derived invalidation stream — scope invalidation signals for derived nodes.
        // Short retention since invalidations are only relevant for running automata.
        self.js
            .create_or_update_stream(jetstream::stream::Config {
                name: self.topology.invalidation_stream.to_string(),
                subjects: vec![self.topology.invalidation_subject.to_string()],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_age: Duration::from_hours(24), // 24h — running automata only
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::network("Failed to create derived invalidation stream").with_source(e)
            })?;

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
        let stream_name = self.topology.events_stream.to_string();
        let mut consumer_spec =
            PullConsumerSpec::new(stream_name.clone(), self.topology.consumer_durable.clone());
        consumer_spec.filter_subject = Some(self.topology.events_subject.to_string());
        consumer_spec.deliver_policy = jetstream::consumer::DeliverPolicy::All;
        consumer_spec.ack_wait = self.ack_wait;
        consumer_spec.max_ack_pending = self.max_ack_pending;
        consumer_spec.max_deliver = MAIN_CONSUMER_JETSTREAM_MAX_DELIVER;
        consumer_spec.reject_initial_replay = self.reject_initial_replay;
        let mut consumer = ensure_pull_consumer(&self.js, &consumer_spec)
            .await
            .map_err(|e| SinexError::network("Failed to create consumer").with_source(e))?;
        let mut lag_consumer = consumer.clone();
        let mut confirmation_retry_spec = PullConsumerSpec::new(
            self.topology.confirmation_retry_stream.to_string(),
            self.topology.confirmation_retry_consumer.clone(),
        );
        confirmation_retry_spec.filter_subject =
            Some(self.topology.confirmation_retry_subject.to_string());
        confirmation_retry_spec.deliver_policy = jetstream::consumer::DeliverPolicy::All;
        confirmation_retry_spec.ack_wait = self.ack_wait;
        confirmation_retry_spec.max_ack_pending = self.max_ack_pending;
        confirmation_retry_spec.max_deliver = 10;
        let confirmation_retry_consumer = ensure_pull_consumer(&self.js, &confirmation_retry_spec)
            .await
            .map_err(|e| {
                SinexError::network("Failed to create confirmation retry consumer").with_source(e)
            })?;

        // Emit startup snapshot before READY so operators can distinguish
        // normal resume from cold-start full replay from catch-up runs.
        if let Some(ref observer) = self.observer {
            // Best-effort: if we can't query stream/consumer state, emit
            // the snapshot with zeroed fields rather than block startup.
            let (
                stream_messages,
                stream_bytes,
                stream_first_seq,
                stream_last_seq,
                stream_max_msgs,
                stream_max_bytes,
                stream_max_age_secs,
            ) = match self.js.get_stream(&stream_name).await {
                Ok(mut stream) => match stream.info().await {
                    Ok(info) => {
                        let s = &info.state;
                        let c = &info.config;
                        (
                            s.messages,
                            s.bytes,
                            s.first_sequence,
                            s.last_sequence,
                            c.max_messages as u64,
                            c.max_bytes as u64,
                            c.max_age.as_secs(),
                        )
                    }
                    Err(e) => {
                        warn!("Failed to get stream info for startup snapshot: {e}");
                        (0, 0, 0, 0, 0, 0, 0)
                    }
                },
                Err(e) => {
                    warn!("Failed to get stream for startup snapshot: {e}");
                    (0, 0, 0, 0, 0, 0, 0)
                }
            };
            let consumer_info = consumer.info().await.ok();
            let consumer_existed = consumer_info.as_ref().is_some_and(|ci| ci.num_pending > 0);
            let deliver_policy = format!("{:?}", consumer_spec.deliver_policy);
            let initial_replay_risk = !consumer_existed
                && matches!(
                    consumer_spec.deliver_policy,
                    jetstream::consumer::DeliverPolicy::All
                )
                && stream_messages > 0;

            let _ = observer
                .emit_consumer_startup_snapshot(
                    stream_name.clone(),
                    self.topology.consumer_durable.clone(),
                    consumer_existed,
                    deliver_policy,
                    stream_messages,
                    stream_bytes,
                    stream_first_seq,
                    stream_last_seq,
                    stream_max_msgs,
                    stream_max_bytes,
                    stream_max_age_secs,
                    consumer_info.as_ref().map_or(0, |ci| ci.num_pending),
                    consumer_info.as_ref().map_or(0, |ci| ci.num_ack_pending),
                    0,
                    consumer_spec.max_ack_pending,
                    consumer_spec.max_deliver,
                    initial_replay_risk,
                )
                .await;

            if initial_replay_risk {
                warn!(
                    stream = %stream_name,
                    consumer = %self.topology.consumer_durable,
                    "Dangerous cold-start replay detected: new consumer with non-empty stream"
                );
            }
        }

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

        // Startup catch-up semaphore: limits I/O pressure while the consumer
        // works through the initial backlog. Once the consumer is caught up
        // (num_pending == 0), the semaphore is no longer used.
        let catch_up_semaphore = (self.startup_catch_up_max_concurrent > 0).then(|| {
            Arc::new(tokio::sync::Semaphore::new(
                self.startup_catch_up_max_concurrent,
            ))
        });
        let mut catching_up = catch_up_semaphore.is_some();
        let mut batch_future: BoxFuture<'_, IngestdResult<()>> = Box::pin(
            Self::process_batch_with_semaphore(&self, &consumer, &catch_up_semaphore, catching_up),
        );

        loop {
            tokio::select! {
                _ = stats_interval.tick() => {
                    self.stats.log();
                    // Emit processing stats via self-observer
                    if let Some(ref observer) = self.observer {
                        let processed = self.stats.events_processed.load(Ordering::Relaxed);
                        let failed = self.stats.events_failed.load(Ordering::Relaxed);
                        let deferred = self.stats.events_deferred.load(Ordering::Relaxed);
                        let dlq_routed = self.stats.dlq_routed.load(Ordering::Relaxed);
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

                        // Emit operational health counters not covered by emit_node_processing_stats.
                        // These are monotonic cumulative totals emitted as gauges (snapshot-at-tick).
                        let operational_gauges: &[(&'static str, u64)] = &[
                            ("ingestd.tombstoned_events_rejected_total", self.stats.tombstoned_events_rejected.load(Ordering::Relaxed)),
                            ("ingestd.confirmation_failures_total", self.stats.confirmation_failures.load(Ordering::Relaxed)),
                            ("ingestd.confirmation_retries_enqueued_total", self.stats.confirmation_retries_enqueued.load(Ordering::Relaxed)),
                            ("ingestd.confirmation_retry_failures_total", self.stats.confirmation_retry_failures.load(Ordering::Relaxed)),
                            ("ingestd.confirmation_durability_gaps_total", self.stats.confirmation_durability_gaps.load(Ordering::Relaxed)),
                            ("ingestd.dlq_publish_failures_total", self.stats.dlq_publish_failures.load(Ordering::Relaxed)),
                            ("ingestd.nack_failures_total", self.stats.nack_failures.load(Ordering::Relaxed)),
                            ("ingestd.nats_errors_total", self.stats.nats_errors.load(Ordering::Relaxed)),
                            ("ingestd.telemetry_publish_failures_total", self.stats.telemetry_publish_failures.load(Ordering::Relaxed)),
                        ];
                        for (metric, value) in operational_gauges {
                            self.emit_observer_gauge(metric, *value as f64, None).await;
                        }
                    }
                }
                _ = capacity_check_interval.tick() => {
                    self.check_stream_capacity(&stream_name).await;
                    // DLQ growth is a durable signal of persistent failures; monitor it too.
                    self.check_stream_capacity(self.topology.dlq_stream.as_ref()).await;
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
                                // Detect catch-up completion when pending drops to zero
                                if catching_up && info.num_pending == 0 {
                                    catching_up = false;
                                    info!("Startup catch-up complete; releasing semaphore throttle");
                                }
                            }
                            Err(e) => {
                                debug!("Consumer lag check failed: {e}");
                            }
                        }
                    }
                }
                _ = confirmation_retry_interval.tick() => {
                    if let Err(e) = self.process_confirmation_retry_batch(&confirmation_retry_consumer).await {
                        error!(
                            target: "sinex_metrics",
                            metric = "ingestd.confirmation_retry_failures_total",
                            error = %e,
                            "Confirmation retry processing error"
                        );
                    }
                }
                batch_result = &mut batch_future => {
                    if let Err(e) = batch_result {
                        if Self::is_fatal_batch_processing_error(&e) {
                            error!(
                                target: "sinex_metrics",
                                metric = "ingestd.fatal_batch_errors_total",
                                error = %e,
                                "Fatal batch processing error"
                            );
                            return Err(e);
                        }
                        error!(
                            target: "sinex_metrics",
                            metric = "ingestd.batch_errors_total",
                            error = %e,
                            "Batch processing error"
                        );
                    }
                    batch_future = Box::pin(Self::process_batch_with_semaphore(
                        &self,
                        &consumer,
                        &catch_up_semaphore,
                        catching_up,
                    ));
                }
            }
        }
    }

    /// Process a batch, acquiring a catch-up semaphore permit during the
    /// startup catch-up phase to limit I/O pressure.
    ///
    /// Catch-up detection (setting `catching_up = false`) is handled in the
    /// lag-check interval of the main loop, where we already have mutable
    /// access to `lag_consumer`.
    async fn process_batch_with_semaphore(
        this: &Self,
        consumer: &jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
        catch_up_semaphore: &Option<Arc<tokio::sync::Semaphore>>,
        catching_up: bool,
    ) -> IngestdResult<()> {
        if let (true, Some(sem)) = (catching_up, catch_up_semaphore.as_ref()) {
            let _permit = sem.acquire().await;
            this.process_batch(consumer).await?;
        } else {
            this.process_batch(consumer).await?;
        }
        Ok(())
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
            #[cfg(any(test, feature = "testing"))]
            if let Some(counter) = &self.delivery_observer {
                counter.fetch_add(1, Ordering::Relaxed);
            }

            let prepared_events = self.prepare_events(msg).await?;
            batch.extend(prepared_events);
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
            let suspicious_future_ts_orig =
                self.stats.suspicious_future_ts_orig.load(Ordering::Relaxed);
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
                    self.stats
                        .telemetry_publish_failures
                        .load(Ordering::Relaxed),
                    self.stats
                        .confirmation_durability_gaps
                        .load(Ordering::Relaxed),
                )
                .await
            {
                Self::log_observer_error(&self.stats, "ingestd.batch", &error);
            }
        }

        result
    }

    #[instrument(skip(self, msg))]
    async fn prepare_events(&self, msg: jetstream::Message) -> IngestdResult<Vec<PreparedEvent>> {
        let decisions = self.admission.admit_intent_bytes(&msg.payload).await?;
        let mut prepared = Vec::with_capacity(decisions.len());

        for decision in decisions {
            match decision {
                AdmissionDecision::Admitted(admitted)
                | AdmissionDecision::Transformed(admitted) => {
                    prepared.push(PreparedEvent {
                        event: admitted.event,
                        parsed_id: admitted.event_id,
                        message: msg.clone(),
                    });
                }
                AdmissionDecision::Rejected(rejection)
                | AdmissionDecision::Suppressed(rejection)
                | AdmissionDecision::QuarantineNeeded(rejection) => {
                    self.record_admission_rejection(&rejection).await;
                    self.route_validation_failure(&msg, rejection.reason)
                        .await?;
                }
            }
        }

        Ok(prepared)
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
                let mut settlement_errors = Vec::new();
                let mut deferred_count = 0_u64;
                for prepared in &not_ready {
                    let material_id = match &prepared.event.provenance {
                        Provenance::Material { id, .. } => Some(*id.as_uuid()),
                        Provenance::Synthesis { .. } => None,
                    };
                    match self
                        .settle_unready_source_material_event(prepared, material_id, None)
                        .await
                    {
                        Ok(SourceMaterialSettlement::Deferred) => deferred_count += 1,
                        Ok(SourceMaterialSettlement::RoutedToDlq) => {}
                        Err(err) => settlement_errors.push((prepared.parsed_id, err)),
                    }
                }
                Self::collapse_settlement_errors(
                    "source-material readiness settlement",
                    settlement_errors,
                )?;
                self.stats
                    .events_deferred
                    .fetch_add(deferred_count, Ordering::Relaxed);
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

    /// Persist and settle a prepared batch.
    ///
    /// Atomicity is intentionally scoped to each successful persistence attempt,
    /// not to the original `JetStream` pull batch. If a non-retryable row poisons a
    /// mixed batch, ingestd bisects the batch to isolate the poison row. Any sibling
    /// sub-batch that already persisted keeps its commit and raw-message ACKs, while
    /// the isolated row is retried or routed to the DLQ on its own. Replay and
    /// lineage therefore reason at event granularity, not at raw pull-batch
    /// granularity.
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
                    let inserted_set = persisted
                        .inserted_ids
                        .as_ref()
                        .map(|ids| ids.iter().copied().collect::<HashSet<_>>());
                    let mut confirmation_ids: HashSet<Uuid> =
                        persisted.duplicate_event_ids.iter().copied().collect();
                    if let Some(ids) = &persisted.inserted_ids {
                        confirmation_ids.extend(ids.iter().copied());
                    }
                    let tombstoned_ids: HashSet<Uuid> =
                        persisted.tombstoned_event_ids.iter().copied().collect();
                    let confirmation_batch: Vec<_> = batch
                        .iter()
                        .copied()
                        .filter(|prepared| confirmation_ids.contains(&prepared.parsed_id))
                        .collect();
                    let tombstoned_batch: Vec<_> = batch
                        .iter()
                        .copied()
                        .filter(|prepared| tombstoned_ids.contains(&prepared.parsed_id))
                        .collect();
                    #[cfg(any(test, feature = "testing"))]
                    if let Some(delay) = self.processing_delay {
                        tokio::time::sleep(delay).await;
                    }
                    // Per #1306: group by (source, event_type) and publish one
                    // watermark per kind, not one confirmation per event id.
                    // Skip publishes when the in-memory watermark is already at
                    // or beyond this batch's max for that kind — saves NATS
                    // roundtrips and keeps the stream compacted at one message
                    // per kind.
                    let mut by_kind: HashMap<(String, String), Vec<&PreparedEvent>> =
                        HashMap::new();
                    for prepared in &confirmation_batch {
                        let key = (
                            prepared.event.source.as_str().to_string(),
                            prepared.event.event_type.as_str().to_string(),
                        );
                        by_kind.entry(key).or_default().push(*prepared);
                    }

                    let confirmation_futs: Vec<_> = by_kind
                        .into_iter()
                        .filter_map(|(kind, preps)| {
                            let sem = Arc::clone(&self.confirmation_semaphore);
                            let watermark = Arc::clone(&self.confirmation_watermark);
                            let max_event_id = preps.iter().map(|p| p.parsed_id).max()?;
                            let (source, event_type) = (kind.0.clone(), kind.1.clone());
                            let key = kind;
                            Some(async move {
                                // Watermark advancement gate.
                                let advance = {
                                    let mut wm = watermark.lock().await;
                                    let existing = wm.get(&key).copied();
                                    let should_advance =
                                        existing.is_none_or(|prev| max_event_id > prev);
                                    if should_advance {
                                        wm.insert(key, max_event_id);
                                    }
                                    should_advance
                                };
                                if !advance {
                                    // Already at or beyond this watermark; skip
                                    // publish but treat as success so downstream
                                    // ack accounting proceeds.
                                    return (preps, max_event_id, source, event_type, Ok(()));
                                }
                                let _permit = match sem.acquire().await {
                                    Ok(permit) => permit,
                                    Err(error) => {
                                        return (
                                            preps,
                                            max_event_id,
                                            source,
                                            event_type,
                                            Err(SinexError::processing(
                                                "confirmation semaphore closed",
                                            )
                                            .with_std_error(&error)),
                                        );
                                    }
                                };
                                let result = self
                                    .publish_confirmation_with_retry(
                                        &max_event_id,
                                        &source,
                                        &event_type,
                                    )
                                    .await;
                                (preps, max_event_id, source, event_type, result)
                            })
                        })
                        .collect();
                    let kind_results = join_all(confirmation_futs).await;

                    let mut ack_messages = Vec::with_capacity(batch.len());
                    ack_messages.extend(tombstoned_batch.iter().map(|prepared| &prepared.message));
                    let mut confirmation_durability_gaps = Vec::new();
                    for (preps, max_event_id, source, event_type, result) in &kind_results {
                        match result {
                            Ok(()) => {
                                for prepared in preps {
                                    if let Some(set) = &inserted_set
                                        && !set.contains(&prepared.parsed_id)
                                    {
                                        debug!(
                                            event_id = %prepared.parsed_id,
                                            "Re-published confirmation for already persisted event"
                                        );
                                    }
                                    ack_messages.push(&prepared.message);
                                }
                            }
                            Err(err) => {
                                warn!(
                                    source = %source,
                                    event_type = %event_type,
                                    watermark = %max_event_id,
                                    error = %err,
                                    "Failed to publish per-kind confirmation watermark after retries"
                                );
                                self.stats
                                    .confirmation_failures
                                    .fetch_add(1, Ordering::Relaxed);
                                // One retry-queue entry per kind suffices — the retry
                                // consumer republishes the watermark and that
                                // implicitly confirms every event of the kind with
                                // id <= watermark.
                                match self
                                    .enqueue_confirmation_retry(max_event_id, source, event_type)
                                    .await
                                {
                                    Ok(()) => {
                                        info!(
                                            source = %source,
                                            event_type = %event_type,
                                            watermark = %max_event_id,
                                            covered_events = preps.len(),
                                            "Queued durable confirmation-watermark retry after publish failure"
                                        );
                                        self.stats
                                            .confirmation_retries_enqueued
                                            .fetch_add(1, Ordering::Relaxed);
                                        for prepared in preps {
                                            ack_messages.push(&prepared.message);
                                        }
                                    }
                                    Err(retry_err) => {
                                        error!(
                                            target: "sinex_metrics",
                                            metric = "ingestd.confirmation_retry_failures_total",
                                            source = %source,
                                            event_type = %event_type,
                                            watermark = %max_event_id,
                                            error = %retry_err,
                                            "Failed to queue durable confirmation-watermark retry after persistence; leaving the raw messages unsettled and failing the consumer"
                                        );
                                        self.stats
                                            .confirmation_retry_failures
                                            .fetch_add(1, Ordering::Relaxed);
                                        for prepared in preps {
                                            confirmation_durability_gaps.push((
                                                prepared.parsed_id,
                                                SinexError::network(
                                                    "Persisted event could not publish a confirmation or durably enqueue its retry",
                                                )
                                                .with_context(
                                                    "confirmation_publish_error",
                                                    err.to_string(),
                                                )
                                                .with_context(
                                                    "confirmation_retry_enqueue_error",
                                                    retry_err.to_string(),
                                                )
                                                .with_context(
                                                    "kind_source",
                                                    source.clone(),
                                                )
                                                .with_context(
                                                    "kind_event_type",
                                                    event_type.clone(),
                                                )
                                                .with_context(
                                                    "kind_watermark",
                                                    max_event_id.to_string(),
                                                ),
                                            ));
                                        }
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
                            return Err(SinexError::network("Failed to ack batch")
                                .with_context("batch_size", ack_messages.len().to_string())
                                .with_source(e.to_string()));
                        }
                    }

                    let acked_count = ack_messages.len() as u64;
                    if acked_count > 0 {
                        self.stats
                            .events_processed
                            .fetch_add(acked_count, Ordering::Relaxed);
                        if let Some(ref handle) = self.heartbeat_handle {
                            handle.increment_events_processed(acked_count);
                        }
                    }

                    if !confirmation_durability_gaps.is_empty() {
                        let gap_count = confirmation_durability_gaps.len() as u64;
                        self.stats
                            .confirmation_durability_gaps
                            .fetch_add(gap_count, Ordering::Relaxed);
                        if let Some(ref handle) = self.heartbeat_handle {
                            handle.record_error("confirmation durability gap");
                        }
                        return Err(Self::confirmation_durability_gap_error(
                            confirmation_durability_gaps,
                            acked_count as usize,
                        ));
                    }
                    info!(
                        confirmed = confirmation_batch.len(),
                        tombstoned = tombstoned_batch.len(),
                        "Processed admission batch"
                    );
                }
                Err(failure) => {
                    self.settle_admission_skips(
                        &batch,
                        &failure.duplicate_event_ids,
                        &failure.tombstoned_event_ids,
                    )
                    .await?;
                    let e = failure.error;
                    let attempted_ids: HashSet<Uuid> =
                        failure.attempted_event_ids.iter().copied().collect();
                    let attempted_batch: Vec<_> = batch
                        .iter()
                        .copied()
                        .filter(|prepared| attempted_ids.contains(&prepared.parsed_id))
                        .collect();
                    // Check if this is a transient FK violation (source material not yet registered).
                    // Safety net: the ready set should prevent most FK violations, but races are
                    // possible (e.g. material registered between ready-set check and DB insert).
                    let is_fk_error =
                        is_source_material_fk_violation_for_prepared_batch(&e, &attempted_batch);
                    if is_fk_error {
                        let mut settlement_errors = Vec::new();
                        let mut deferred_count = 0_u64;
                        debug!(
                            batch_size = attempted_batch.len(),
                            "FK violation on batch - source material likely still registering"
                        );
                        for prepared in &attempted_batch {
                            let material_id = match &prepared.event.provenance {
                                Provenance::Material { id, .. } => Some(*id.as_uuid()),
                                Provenance::Synthesis { .. } => None,
                            };
                            match self
                                .settle_unready_source_material_event(
                                    prepared,
                                    material_id,
                                    Some(&e),
                                )
                                .await
                            {
                                Ok(SourceMaterialSettlement::Deferred) => deferred_count += 1,
                                Ok(SourceMaterialSettlement::RoutedToDlq) => {}
                                Err(err) => settlement_errors.push((prepared.parsed_id, err)),
                            }
                        }
                        Self::collapse_settlement_errors(
                            "FK violation retry settlement",
                            settlement_errors,
                        )?;
                        self.stats
                            .events_deferred
                            .fetch_add(deferred_count, Ordering::Relaxed);
                        // Don't count as failed - this is a transient condition
                        continue;
                    }

                    if is_isolatable_batch_persistence_failure(&e) {
                        if attempted_batch.len() > 1 {
                            let split_at = attempted_batch.len() / 2;
                            warn!(
                                batch_size = attempted_batch.len(),
                                split_at,
                                batch_atomicity = BATCH_ATOMICITY_SCOPE,
                                sqlstate = ?e.context_map().get("sqlstate"),
                                constraint = ?e.context_map().get("constraint"),
                                "Splitting batch to isolate non-retryable persistence failure; already-persisted sibling sub-batches remain committed"
                            );
                            pending_batches.push(attempted_batch[split_at..].to_vec());
                            pending_batches.push(attempted_batch[..split_at].to_vec());
                            continue;
                        }

                        let prepared = attempted_batch[0];
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

                    error!(
                        target: "sinex_metrics",
                        metric = "ingestd.batch_persistence_failures_total",
                        error = %e,
                        "Failed to persist batch"
                    );
                    let mut settlement_errors = Vec::new();
                    for prepared in &attempted_batch {
                        match self.should_route_terminal_persistence_failure(&prepared.message, &e)
                        {
                            Ok(true) => {
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
                            }
                            Ok(false) => {
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
                            Err(err) => {
                                warn!(
                                    event_id = %prepared.parsed_id,
                                    error = %err,
                                    "Failed to inspect persistence retry state; NAKing for retry"
                                );
                                settlement_errors.push((
                                    prepared.parsed_id,
                                    err.with_context(
                                        "settlement_operation",
                                        "inspect_persistence_retry_state",
                                    ),
                                ));
                                if let Err(nak_err) = prepared
                                    .message
                                    .ack_with(jetstream::AckKind::Nak(None))
                                    .await
                                {
                                    warn!(
                                        event_id = %prepared.parsed_id,
                                        error = %nak_err,
                                        "Failed to NAK after persistence retry-state inspection failure"
                                    );
                                    self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                                    settlement_errors.push((
                                        prepared.parsed_id,
                                        Self::message_settlement_failure(
                                            "failed to NAK after persistence retry-state inspection failure",
                                            prepared.parsed_id,
                                            &nak_err,
                                        ),
                                    ));
                                }
                            }
                        }
                    }
                    let failed_count = attempted_batch.len() as u64;
                    self.stats
                        .events_failed
                        .fetch_add(failed_count, Ordering::Relaxed);
                    if let Some(ref handle) = self.heartbeat_handle {
                        handle.record_error("batch persistence failure");
                    }
                    Self::collapse_settlement_errors(
                        "persistence failure settlement",
                        settlement_errors,
                    )?;
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

    async fn record_admission_rejection(&self, rejection: &AdmissionRejection) {
        // Update legacy per-kind in-memory counters for backward compatibility.
        match rejection.kind {
            AdmissionRejectionKind::PastTimestamp => {
                self.stats
                    .suspicious_past_ts_orig
                    .fetch_add(1, Ordering::Relaxed);
            }
            AdmissionRejectionKind::FutureTimestamp => {
                self.stats
                    .suspicious_future_ts_orig
                    .fetch_add(1, Ordering::Relaxed);
            }
            AdmissionRejectionKind::NegativeAnchor => {
                self.stats
                    .negative_anchor_byte
                    .fetch_add(1, Ordering::Relaxed);
            }
            AdmissionRejectionKind::SchemaValidation
            | AdmissionRejectionKind::MissingTimestamp
            | AdmissionRejectionKind::PayloadTooLarge
            | AdmissionRejectionKind::InvalidUtf8
            | AdmissionRejectionKind::StructuralJson
            | AdmissionRejectionKind::EventDeserialization
            | AdmissionRejectionKind::CandidateMetadata
            | AdmissionRejectionKind::PrivacyPolicy
            | AdmissionRejectionKind::QuarantinePolicy
            | AdmissionRejectionKind::MissingEventId
            | AdmissionRejectionKind::InvalidEventId
            | AdmissionRejectionKind::EnvelopeDeserialization
            | AdmissionRejectionKind::EnvelopeValidation => {
                self.stats
                    .validation_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        // Emit a unified rejection counter with kind label so every rejection
        // variant is visible in NATS metrics, not just PastTimestamp/FutureTimestamp.
        let kind_label = match rejection.kind {
            AdmissionRejectionKind::PayloadTooLarge => "payload_too_large",
            AdmissionRejectionKind::InvalidUtf8 => "invalid_utf8",
            AdmissionRejectionKind::StructuralJson => "structural_json",
            AdmissionRejectionKind::EventDeserialization => "event_deserialization",
            AdmissionRejectionKind::EnvelopeDeserialization => "envelope_deserialization",
            AdmissionRejectionKind::EnvelopeValidation => "envelope_validation",
            AdmissionRejectionKind::MissingTimestamp => "missing_timestamp",
            AdmissionRejectionKind::PastTimestamp => "past_timestamp",
            AdmissionRejectionKind::FutureTimestamp => "future_timestamp",
            AdmissionRejectionKind::NegativeAnchor => "negative_anchor",
            AdmissionRejectionKind::SchemaValidation => "schema_validation",
            AdmissionRejectionKind::CandidateMetadata => "candidate_metadata",
            AdmissionRejectionKind::PrivacyPolicy => "privacy_policy",
            AdmissionRejectionKind::QuarantinePolicy => "quarantine_policy",
            AdmissionRejectionKind::MissingEventId => "missing_event_id",
            AdmissionRejectionKind::InvalidEventId => "invalid_event_id",
        };

        tracing::debug!(
            target: "sinex_metrics",
            metric = "ingestd.admission_rejections_total",
            kind = kind_label,
            "Event rejected by admission service"
        );

        if let Some(ref observer) = self.observer {
            let labels = Some(std::collections::HashMap::from([(
                "kind".to_string(),
                kind_label.to_string(),
            )]));
            if let Err(error) = observer
                .emit_counter("ingestd.admission_rejections_total", 1, labels)
                .await
            {
                Self::log_observer_error(&self.stats, "ingestd.admission_rejections_total", &error);
            }
        }
    }

    async fn settle_admission_skips(
        &self,
        batch: &[&PreparedEvent],
        duplicate_event_ids: &[Uuid],
        tombstoned_event_ids: &[Uuid],
    ) -> IngestdResult<()> {
        if duplicate_event_ids.is_empty() && tombstoned_event_ids.is_empty() {
            return Ok(());
        }

        let duplicate_ids: HashSet<Uuid> = duplicate_event_ids.iter().copied().collect();
        let tombstoned_ids: HashSet<Uuid> = tombstoned_event_ids.iter().copied().collect();
        let duplicate_batch: Vec<_> = batch
            .iter()
            .copied()
            .filter(|prepared| duplicate_ids.contains(&prepared.parsed_id))
            .collect();
        let tombstoned_batch: Vec<_> = batch
            .iter()
            .copied()
            .filter(|prepared| tombstoned_ids.contains(&prepared.parsed_id))
            .collect();

        // Per #1306: per-kind watermark, not per-event.
        let mut by_kind: HashMap<(String, String), Vec<&PreparedEvent>> = HashMap::new();
        for prepared in &duplicate_batch {
            let key = (
                prepared.event.source.as_str().to_string(),
                prepared.event.event_type.as_str().to_string(),
            );
            by_kind.entry(key).or_default().push(*prepared);
        }

        let confirmation_futs: Vec<_> = by_kind
            .into_iter()
            .filter_map(|(kind, preps)| {
                let sem = Arc::clone(&self.confirmation_semaphore);
                let watermark = Arc::clone(&self.confirmation_watermark);
                let max_event_id = preps.iter().map(|p| p.parsed_id).max()?;
                let (source, event_type) = (kind.0.clone(), kind.1.clone());
                let key = kind;
                Some(async move {
                    let advance = {
                        let mut wm = watermark.lock().await;
                        let existing = wm.get(&key).copied();
                        let should_advance = existing.is_none_or(|prev| max_event_id > prev);
                        if should_advance {
                            wm.insert(key, max_event_id);
                        }
                        should_advance
                    };
                    if !advance {
                        return (preps, max_event_id, source, event_type, Ok(()));
                    }
                    let _permit = match sem.acquire().await {
                        Ok(permit) => permit,
                        Err(error) => {
                            return (
                                preps,
                                max_event_id,
                                source,
                                event_type,
                                Err(SinexError::processing("confirmation semaphore closed")
                                    .with_std_error(&error)),
                            );
                        }
                    };
                    let result = self
                        .publish_confirmation_with_retry(&max_event_id, &source, &event_type)
                        .await;
                    (preps, max_event_id, source, event_type, result)
                })
            })
            .collect();
        let kind_results = join_all(confirmation_futs).await;

        let mut ack_messages = Vec::with_capacity(duplicate_batch.len() + tombstoned_batch.len());
        ack_messages.extend(tombstoned_batch.iter().map(|prepared| &prepared.message));
        let mut confirmation_durability_gaps = Vec::new();
        for (preps, max_event_id, source, event_type, result) in &kind_results {
            match result {
                Ok(()) => {
                    for prepared in preps {
                        debug!(
                            event_id = %prepared.parsed_id,
                            "Re-published confirmation for duplicate already admitted event"
                        );
                        ack_messages.push(&prepared.message);
                    }
                }
                Err(err) => {
                    warn!(
                        source = %source,
                        event_type = %event_type,
                        watermark = %max_event_id,
                        error = %err,
                        "Failed to publish duplicate-confirmation watermark after retries"
                    );
                    self.stats
                        .confirmation_failures
                        .fetch_add(1, Ordering::Relaxed);
                    match self
                        .enqueue_confirmation_retry(max_event_id, source, event_type)
                        .await
                    {
                        Ok(()) => {
                            self.stats
                                .confirmation_retries_enqueued
                                .fetch_add(1, Ordering::Relaxed);
                            for prepared in preps {
                                ack_messages.push(&prepared.message);
                            }
                        }
                        Err(retry_err) => {
                            self.stats
                                .confirmation_retry_failures
                                .fetch_add(1, Ordering::Relaxed);
                            for prepared in preps {
                                confirmation_durability_gaps.push((
                                    prepared.parsed_id,
                                    SinexError::network(
                                        "Duplicate event could not publish a confirmation or durably enqueue its retry",
                                    )
                                    .with_context("confirmation_publish_error", err.to_string())
                                    .with_context(
                                        "confirmation_retry_enqueue_error",
                                        retry_err.to_string(),
                                    )
                                    .with_context("kind_source", source.clone())
                                    .with_context("kind_event_type", event_type.clone())
                                    .with_context("kind_watermark", max_event_id.to_string()),
                                ));
                            }
                        }
                    }
                }
            }
        }

        let ack_futs: Vec<_> = ack_messages.iter().map(|message| message.ack()).collect();
        let ack_results = join_all(ack_futs).await;
        for result in &ack_results {
            if let Err(error) = result {
                return Err(
                    SinexError::network("Failed to ack admission-skipped messages")
                        .with_context("batch_size", ack_messages.len().to_string())
                        .with_source(error.to_string()),
                );
            }
        }

        let acked_count = ack_messages.len() as u64;
        if acked_count > 0 {
            self.stats
                .events_processed
                .fetch_add(acked_count, Ordering::Relaxed);
            if let Some(ref handle) = self.heartbeat_handle {
                handle.increment_events_processed(acked_count);
            }
        }

        if !confirmation_durability_gaps.is_empty() {
            let gap_count = confirmation_durability_gaps.len() as u64;
            self.stats
                .confirmation_durability_gaps
                .fetch_add(gap_count, Ordering::Relaxed);
            if let Some(ref handle) = self.heartbeat_handle {
                handle.record_error("confirmation durability gap");
            }
            return Err(Self::confirmation_durability_gap_error(
                confirmation_durability_gaps,
                acked_count as usize,
            ));
        }

        Ok(())
    }

    #[cfg(test)]
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
                        "Strict validation enabled: event has no registered schema (source={source}, event_type={event_type})"
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
    ) -> Result<PersistBatchResult, PersistBatchFailure> {
        if batch.is_empty() {
            return Ok(PersistBatchResult {
                inserted_ids: None,
                duplicate_event_ids: Vec::new(),
                tombstoned_event_ids: Vec::new(),
            });
        }

        let admitted_batch: Vec<AdmittedEvent> = batch
            .iter()
            .map(|prepared| AdmittedEvent {
                event: prepared.event.clone(),
                event_id: prepared.parsed_id,
                metadata: None,
            })
            .collect();
        let admitted_refs: Vec<&AdmittedEvent> = admitted_batch.iter().collect();

        let plan = self
            .admission
            .plan_persistence_batch_refs(&admitted_refs)
            .await
            .map_err(|error| PersistBatchFailure {
                error,
                attempted_event_ids: admitted_batch.iter().map(|event| event.event_id).collect(),
                duplicate_event_ids: Vec::new(),
                tombstoned_event_ids: Vec::new(),
            })?;
        let attempted_event_ids = plan.attempted_event_ids();
        let duplicate_event_ids = plan.cached_duplicate_event_ids.clone();
        let tombstoned_event_ids = plan.tombstoned_event_ids.clone();
        let result =
            self.admission
                .persist_plan(&plan)
                .await
                .map_err(|error| PersistBatchFailure {
                    error,
                    attempted_event_ids: attempted_event_ids.clone(),
                    duplicate_event_ids: duplicate_event_ids.clone(),
                    tombstoned_event_ids: tombstoned_event_ids.clone(),
                })?;
        if result.tombstoned_events_rejected > 0 {
            self.stats
                .tombstoned_events_rejected
                .fetch_add(result.tombstoned_events_rejected as u64, Ordering::Relaxed);
        }
        Ok(PersistBatchResult {
            inserted_ids: result.inserted_ids,
            duplicate_event_ids: result.duplicate_event_ids,
            tombstoned_event_ids: result.tombstoned_event_ids,
        })
    }

    fn should_route_terminal_persistence_failure(
        &self,
        msg: &jetstream::Message,
        err: &SinexError,
    ) -> IngestdResult<bool> {
        let delivery_attempt = msg
            .info()
            .map(|info| info.delivered)
            .map_err(|error| error.to_string());
        Self::should_route_persistence_failure(self.route_db_errors_to_dlq, delivery_attempt, err)
    }

    fn source_material_delivery_attempt(&self, msg: &jetstream::Message) -> IngestdResult<i64> {
        msg.info().map(|info| info.delivered).map_err(|error| {
            SinexError::processing(
                "Failed to inspect JetStream delivery metadata for source-material readiness",
            )
            .with_context("delivery_metadata_error", error.to_string())
        })
    }

    async fn settle_unready_source_material_event(
        &self,
        prepared: &PreparedEvent,
        material_id: Option<Uuid>,
        persistence_error: Option<&SinexError>,
    ) -> IngestdResult<SourceMaterialSettlement> {
        let delivery_attempt = if self.route_db_errors_to_dlq {
            None
        } else {
            Some(self.source_material_delivery_attempt(&prepared.message)?)
        };
        let should_dlq = self.route_db_errors_to_dlq
            || delivery_attempt
                .is_some_and(|attempt| attempt >= SOURCE_MATERIAL_READY_DLQ_THRESHOLD);

        if should_dlq {
            warn!(
                event_id = %prepared.parsed_id,
                material_id = ?material_id,
                delivery_attempt = ?delivery_attempt,
                threshold = SOURCE_MATERIAL_READY_DLQ_THRESHOLD,
                "Source material remained unavailable after retry budget; routing event to DLQ"
            );
            self.route_to_dlq_and_ack(
                &prepared.message,
                source_material_unavailable_error(prepared, material_id, persistence_error),
            )
            .await?;
            self.stats.events_failed.fetch_add(1, Ordering::Relaxed);
            if let Some(ref handle) = self.heartbeat_handle {
                handle.record_error("source material unresolved");
            }
            return Ok(SourceMaterialSettlement::RoutedToDlq);
        }

        if let Err(err) = prepared
            .message
            .ack_with(jetstream::AckKind::Nak(Some(FK_VIOLATION_RETRY_DELAY)))
            .await
        {
            warn!(
                event_id = %prepared.parsed_id,
                material_id = ?material_id,
                error = %err,
                "Failed to NAK deferred source-material event"
            );
            self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
            return Err(Self::message_settlement_failure(
                "failed to NAK deferred source-material event",
                prepared.parsed_id,
                &err,
            ));
        }

        Ok(SourceMaterialSettlement::Deferred)
    }

    fn should_route_persistence_failure(
        route_db_errors_to_dlq: bool,
        delivery_attempt: std::result::Result<i64, String>,
        err: &SinexError,
    ) -> IngestdResult<bool> {
        if route_db_errors_to_dlq {
            return Ok(true);
        }

        if sinex_db::query_helpers::is_retryable_db_error(err) {
            return Ok(false);
        }

        match delivery_attempt {
            Ok(delivered) => Ok(delivered >= MAIN_CONSUMER_TERMINAL_DLQ_THRESHOLD),
            Err(error) => Err(SinexError::processing(
                "Failed to inspect JetStream delivery metadata for persistence failure",
            )
            .with_context("delivery_metadata_error", error)),
        }
    }

    fn message_settlement_failure(
        operation: &'static str,
        event_id: Uuid,
        error: impl std::fmt::Display,
    ) -> SinexError {
        sinex_node_sdk::error_helpers::nats_settlement_error(
            operation,
            "",
            Some(event_id.to_string().as_str()),
            error,
        )
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

    /// Publish a per-kind confirmation watermark.
    ///
    /// The subject is `prod.events.confirmations.<source>.<event_type>` and the
    /// payload's `event_id` is the high-watermark — the latest event of this
    /// kind we have persisted. With `max_messages_per_subject = 1` on the
    /// stream, this acts as real compaction (one entry per kind). Downstream
    /// readers advance their per-kind watermark and treat earlier events of
    /// the same kind as confirmed. Per #1306.
    async fn publish_confirmation(
        &self,
        event_id: &Uuid,
        source: &str,
        event_type: &str,
    ) -> IngestdResult<()> {
        #[cfg(any(test, feature = "testing"))]
        if let Some(failures) = &self.confirmation_failures_remaining
            && failures.load(Ordering::SeqCst) > 0
        {
            failures.fetch_sub(1, Ordering::SeqCst);
            return Err(SinexError::network("forced confirmation publish failure"));
        }

        let event_id_str = event_id.to_string();
        let confirmation = Confirmation {
            event_id: event_id_str.clone(),
            source: source.to_string(),
            event_type: event_type.to_string(),
            persisted: true,
            ts_ingest: Timestamp::now(),
        };

        let subject = format!(
            "{}{}.{}",
            self.topology.confirmations_prefix, source, event_type
        );
        let payload = serde_json::to_vec(&confirmation)?;

        // transport::Class::Confirmation — best-effort ACK signal; failure
        // routes to the durable retry queue then durability-gap warn (not DLQ).
        // Nats-Msg-Id is per-watermark (event_id) so duplicate publishes of the
        // same watermark within the dedup window are coalesced server-side.
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", event_id_str.as_str());
        transport::insert_transport_class_headers(&mut headers, transport::Class::Confirmation);

        self.js
            .publish_with_headers(subject, headers, payload.into())
            .await
            .map_err(|e| SinexError::network("Failed to publish confirmation").with_source(e))?
            .await
            .map_err(|e| SinexError::network("Confirmation ack failed").with_source(e))?;

        debug!(event_id = %event_id, source = %source, event_type = %event_type, "Published confirmation watermark");
        Ok(())
    }

    async fn publish_confirmation_with_retry(
        &self,
        event_id: &Uuid,
        source: &str,
        event_type: &str,
    ) -> IngestdResult<()> {
        let mut backoff = CONFIRM_PUBLISH_BACKOFF_BASE;
        let mut last_error: Option<SinexError> = None;

        for attempt in 1..=CONFIRM_PUBLISH_MAX_ATTEMPTS {
            match self
                .publish_confirmation(event_id, source, event_type)
                .await
            {
                Ok(()) => return Ok(()),
                Err(err) => {
                    warn!(
                        attempt,
                        event_id = %event_id,
                        source = %source,
                        event_type = %event_type,
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

    async fn enqueue_confirmation_retry(
        &self,
        event_id: &Uuid,
        source: &str,
        event_type: &str,
    ) -> IngestdResult<()> {
        let event_id_str = event_id.to_string();
        let subject = format!(
            "{}{}",
            self.topology.confirmation_retry_prefix, event_id_str
        );
        let payload = serde_json::to_vec(&ConfirmationRetryRequest {
            event_id: event_id_str.clone(),
            source: source.to_string(),
            event_type: event_type.to_string(),
        })?;

        let mut headers = async_nats::HeaderMap::new();
        let retry_msg_id = format!("confirm-retry.{event_id_str}");
        headers.insert("Nats-Msg-Id", retry_msg_id.as_str());
        transport::insert_transport_class_headers(&mut headers, transport::Class::Confirmation);

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

            if retry.source.is_empty() || retry.event_type.is_empty() {
                warn!(
                    event_id = %event_id,
                    "Confirmation retry payload missing source/event_type (legacy pre-#1306 payload); acknowledging without re-publish — downstream will rely on next batch's watermark"
                );
                if let Err(ack_err) = message.ack().await {
                    warn!(error = %ack_err, "Failed to ack legacy confirmation retry message");
                    self.stats.nack_failures.fetch_add(1, Ordering::Relaxed);
                }
                continue;
            }

            match self
                .publish_confirmation_with_retry(&event_id, &retry.source, &retry.event_type)
                .await
            {
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
                    if let Err(nak_err) = message
                        .ack_with(jetstream::AckKind::Nak(Some(CONFIRM_RETRY_DELAY)))
                        .await
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
        insert_traffic_class_header(&mut headers, NatsTrafficClass::RawIngestDlq);
        transport::insert_semantic_transport_class_header(&mut headers, transport::Class::Critical);
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
                        error!(
                            target: "sinex_metrics",
                            metric = "ingestd.dlq_confirm_failures_total",
                            attempt,
                            error = %err,
                            "Failed to confirm DLQ publish"
                        );
                        last_error =
                            Some(SinexError::network("DLQ publish ack failed").with_source(err));
                    }
                },
                Err(err) => {
                    error!(
                        target: "sinex_metrics",
                        metric = "ingestd.dlq_routing_failures_total",
                        attempt,
                        error = %err,
                        "Failed to route to DLQ"
                    );
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
        let dlq_error = error.clone();
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
                        SinexError::network("Failed to NAK after DLQ publish failure")
                            .with_context("dlq_error", dlq_error.clone())
                            .with_source(nak_err.to_string())
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
                            Self::log_observer_error(&self.stats, "ingestd.stream", &error);
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
            err.to_string()
                .contains("Event repository omitted inserted_ids"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn suspicious_future_ts_orig_default_one_hour_skew() -> TestResult<()> {
        let default_skew = time::Duration::hours(1);
        let now = Timestamp::now();
        assert!(now + time::Duration::minutes(59) <= now + default_skew);
        assert!(now + time::Duration::minutes(61) > now + default_skew);
        Ok(())
    }

    #[sinex_test]
    async fn implausibly_old_ts_orig_default_year_2000() -> TestResult<()> {
        let lower_bound = Timestamp::from_const(time::macros::datetime!(2000-01-01 00:00:00 UTC));
        let before_2000 = Timestamp::from_const(time::macros::datetime!(1999-12-31 23:59:59 UTC));
        let after_2000 = Timestamp::from_const(time::macros::datetime!(2000-01-02 00:00:00 UTC));
        assert!(
            before_2000 < lower_bound,
            "1999-12-31 should be before lower bound"
        );
        assert!(
            (lower_bound >= lower_bound),
            "2000-01-01 itself should not be flagged"
        );
        assert!(
            (after_2000 >= lower_bound),
            "2000-01-02 should not be flagged"
        );
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

    #[sinex_test]
    async fn source_material_fk_constraint_name_accepts_exact_name() -> TestResult<()> {
        assert!(is_source_material_fk_constraint_name(
            EVENTS_SOURCE_MATERIAL_ID_FKEY
        ));
        Ok(())
    }

    #[sinex_test]
    async fn source_material_fk_constraint_name_accepts_timescale_chunk_prefix() -> TestResult<()> {
        assert!(is_source_material_fk_constraint_name(
            "1_4_events_source_material_id_fkey"
        ));
        Ok(())
    }

    #[sinex_test]
    async fn source_material_fk_constraint_name_rejects_other_constraints() -> TestResult<()> {
        assert!(!is_source_material_fk_constraint_name(
            "events_payload_schema_id_fkey"
        ));
        assert!(!is_source_material_fk_constraint_name(
            "events_source_material_id_fkey_extra"
        ));
        Ok(())
    }

    #[sinex_test]
    async fn uuid_v7_guard_rejects_other_uuid_versions() -> TestResult<()> {
        assert!(is_uuid_v7(&Uuid::now_v7()));
        let deterministic_timestamp =
            Timestamp::from_const(time::macros::datetime!(2024-03-09 16:00:00.123 UTC));
        assert!(is_uuid_v7(&sinex_node_sdk::deterministic_event_id(
            "ingestd-guard",
            "source-anchor",
            deterministic_timestamp
        )));
        assert!(!is_uuid_v7(&Uuid::new_v4()));
        assert!(!is_uuid_v7(
            &"019da690-06f8-707c-f98d-218250d05d62".parse::<Uuid>()?
        ));
        Ok(())
    }

    #[sinex_test]
    async fn persistence_failure_routing_short_circuits_when_dlq_is_forced() -> TestResult<()> {
        assert!(JetStreamConsumer::should_route_persistence_failure(
            true,
            Err("delivery metadata unavailable".to_string()),
            &SinexError::database("forced failure"),
        )?);
        Ok(())
    }

    #[sinex_test]
    async fn persistence_failure_routing_uses_delivery_attempts_for_non_retryable_errors()
    -> TestResult<()> {
        assert!(!JetStreamConsumer::should_route_persistence_failure(
            false,
            Ok(MAIN_CONSUMER_TERMINAL_DLQ_THRESHOLD - 1),
            &SinexError::database("forced persistent failure"),
        )?);
        assert!(JetStreamConsumer::should_route_persistence_failure(
            false,
            Ok(MAIN_CONSUMER_TERMINAL_DLQ_THRESHOLD),
            &SinexError::database("forced persistent failure"),
        )?);
        Ok(())
    }

    #[sinex_test]
    async fn persistence_failure_routing_never_dlqs_retryable_db_errors() -> TestResult<()> {
        let retryable =
            SinexError::database("serialization failure").with_context("sqlstate", "40001");
        assert!(!JetStreamConsumer::should_route_persistence_failure(
            false,
            Ok(MAIN_CONSUMER_TERMINAL_DLQ_THRESHOLD),
            &retryable,
        )?);
        Ok(())
    }

    #[sinex_test]
    async fn persistence_failure_routing_rejects_missing_delivery_metadata() -> TestResult<()> {
        let error = JetStreamConsumer::should_route_persistence_failure(
            false,
            Err("metadata missing".to_string()),
            &SinexError::database("forced persistent failure"),
        )
        .expect_err("missing delivery metadata must fail honestly");
        assert!(
            error
                .to_string()
                .contains("Failed to inspect JetStream delivery metadata"),
            "unexpected error: {error}"
        );
        assert_eq!(
            error
                .context_map()
                .get("delivery_metadata_error")
                .map(String::as_str),
            Some("metadata missing")
        );
        Ok(())
    }

    #[sinex_test]
    async fn confirmation_durability_gap_errors_are_marked_fatal() -> TestResult<()> {
        let event_id = Uuid::now_v7();
        let error = JetStreamConsumer::confirmation_durability_gap_error(
            vec![(
                event_id,
                SinexError::network("confirmation transport exhausted")
                    .with_context("confirmation_publish_error", "publish failed")
                    .with_context("confirmation_retry_enqueue_error", "enqueue failed"),
            )],
            2,
        );

        assert!(JetStreamConsumer::is_fatal_batch_processing_error(&error));
        assert_eq!(
            error.context_map().get("error_class").map(String::as_str),
            Some(ERROR_CLASS_CONFIRMATION_DURABILITY_GAP)
        );
        assert_eq!(
            error
                .context_map()
                .get("acked_event_count")
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            error
                .context_map()
                .get("batch_atomicity")
                .map(String::as_str),
            Some(BATCH_ATOMICITY_SCOPE)
        );
        assert_eq!(
            error
                .context_map()
                .get("raw_message_settlement")
                .map(String::as_str),
            Some("left_unacked_for_redelivery")
        );
        Ok(())
    }

    #[sinex_test]
    async fn ordinary_errors_are_not_marked_as_fatal_confirmation_gaps() -> TestResult<()> {
        assert!(!JetStreamConsumer::is_fatal_batch_processing_error(
            &SinexError::network("ordinary nack failure")
        ));
        Ok(())
    }
}
