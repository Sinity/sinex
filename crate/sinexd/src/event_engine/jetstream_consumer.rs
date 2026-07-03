//! `JetStream` event consumer with confirmations and DLQ support
//!
//! See `crate::event_engine::docs::ingestion_pipeline` for architectural details.
//!
//! # Batch Atomicity Contract
//!
//! The event_engine consumer does NOT guarantee all-or-nothing atomicity for a NATS pull-batch.
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
//!
//! The physical implementation is split by consumer phase: root state/config lives here,
//! while bootstrap, run-loop, prepare, persist, confirmation, DLQ, pressure, telemetry,
//! and support vocabulary live under `jetstream_consumer/*`.

use crate::runtime::SelfObserver;
use crate::runtime::heartbeat::HeartbeatCounterHandle;
use crate::runtime::stream::{
    PullConsumerSpec, ensure_pull_consumer, pull_batch_bounded,
};
use async_nats::jetstream::stream::DiscardPolicy;
use async_nats::{Client as NatsClient, jetstream};
use futures::future::{BoxFuture, join_all};
use sinex_db::DbPool;
use sinex_db::DbPoolExt;
use sinex_db::repositories::COPY_BATCH_THRESHOLD;
use sinex_db::schema::defs::records::SourceMaterialRecord;
use sinex_primitives::Timestamp;
use sinex_primitives::constants::env_vars;
use sinex_primitives::events::payloads::{
    StreamPressureSnapshot, StreamPressureWarningState, record_stream_pressure_warning_sample,
};
use sinex_primitives::{
    JsonValue, Uuid,
    nats::{JetStreamTopology, NatsTrafficClass, insert_traffic_class_header},
    transport,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
#[cfg(any(test, feature = "testing"))]
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use tokio::sync::RwLock;
use tokio::time::Duration;
use tracing::{debug, error, info, instrument, warn};

#[cfg(test)]
use crate::event_engine::validator::ValidationResult;
use crate::event_engine::{
    EventEngineResult, SinexError,
    admission::{
        AdmissionDecision, AdmissionRejection, AdmissionRejectionKind, AdmissionService,
        AdmittedEvent,
    },
    material_ready_set::MaterialReadySet,
    validator::IngestEventValidator,
};
use crate::runtime::ingestion_helpers::{LedgerEntry, LedgerReader, MaterialTiming};
use crate::runtime::nats_payload::ensure_nats_payload_fits;
use sinex_primitives::Id;
use sinex_primitives::domain::SourceMaterialTimingInfoType;
use sinex_primitives::events::builder::Provenance;

mod bootstrap;
mod config;
mod confirmation;
mod dlq;
mod persist;
mod persistence_support;
mod prepare;
mod pressure;
mod readiness;
mod run_loop;
mod settings;
mod stats;
mod telemetry;

use persistence_support::*;
use readiness::signal_ready;
use settings::*;
use stats::ConsumerStats;

pub struct JetStreamConsumer {
    js: jetstream::Context,
    pool: DbPool,
    validator: Arc<RwLock<IngestEventValidator>>,
    admission: AdmissionService,
    topology: JetStreamTopology,
    /// DB-backed privacy policy engine (#1042). Applied at the persistence
    /// chokepoint in `persist_batch_optimized` before any DB write, covering
    /// both source (material) and derived events.
    policy_engine: Arc<crate::event_engine::policy::PolicyEngine>,
    ack_wait: Duration,
    max_ack_pending: i64,
    #[cfg(any(test, feature = "testing"))]
    confirmation_failures_remaining: Option<Arc<AtomicUsize>>,
    confirmation_semaphore: Arc<tokio::sync::Semaphore>,
    #[cfg(any(test, feature = "testing"))]
    processing_delay: Option<Duration>,
    #[cfg(any(test, feature = "testing"))]
    delivery_observer: Option<Arc<AtomicU64>>,
    #[cfg(any(test, feature = "testing"))]
    source_material_ready_dlq_threshold: Option<i64>,
    #[cfg(any(test, feature = "testing"))]
    source_material_ready_retry_delay: Option<Duration>,
    stats: ConsumerStats,
    /// Test-only: when true, persistence errors are routed to DLQ instead of NAK'd.
    /// Production always uses the NAK path; this field is initialized to `false` and
    /// only mutated by `with_test_hooks`. Left as a primitive (not cfg-gated) because
    /// the read sites are in hot persistence-error paths and threading cfg around them
    /// would add more noise than the 1 byte of struct memory it would save.
    route_db_errors_to_dlq: bool,
    batch_fetch_max_messages: usize,
    /// Cumulative payload-byte budget per fetch; caps the decode high-watermark
    /// independent of per-message size. See `DEFAULT_BATCH_FETCH_MAX_BYTES`.
    batch_fetch_max_bytes: usize,
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
    /// Per-stream pressure warning counters used to keep saturated RAW/DLQ
    /// capacity samples from becoming their own journald feedback stream.
    stream_pressure_warning_state:
        Arc<tokio::sync::Mutex<HashMap<String, StreamPressureWarningState>>>,
}

#[cfg(test)]
#[path = "jetstream_consumer_test.rs"]
mod tests;
