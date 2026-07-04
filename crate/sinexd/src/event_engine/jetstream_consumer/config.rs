//! Constructor, policy-engine loading, and test-hook configuration for `JetStreamConsumer`.

use super::confirmation::CONFIRM_PUBLISH_CONCURRENCY;
use super::*;

impl JetStreamConsumer {
    pub fn new(
        nats_client: NatsClient,
        pool: DbPool,
        validator: Arc<RwLock<IngestEventValidator>>,
        topology: JetStreamTopology,
    ) -> Self {
        let js = jetstream::new(nats_client);
        let admission = AdmissionService::new(pool.clone(), Arc::clone(&validator));

        // Initialize with an explicit noop for construction/tests. Production
        // startup must call `.with_policy_engine()` and fail if DB policy cannot
        // be loaded.
        let pool_clone = pool.clone();
        Self {
            js,
            pool,
            validator,
            admission,
            topology,
            policy_engine: Arc::new(crate::event_engine::policy::PolicyEngine::noop(pool_clone)),
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
            #[cfg(any(test, feature = "testing"))]
            source_material_ready_dlq_threshold: None,
            #[cfg(any(test, feature = "testing"))]
            source_material_ready_retry_delay: None,
            stats: ConsumerStats::default(),
            route_db_errors_to_dlq: false,
            batch_fetch_max_messages: DEFAULT_BATCH_FETCH_MAX_MESSAGES,
            batch_fetch_max_bytes: DEFAULT_BATCH_FETCH_MAX_BYTES,
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
            stream_pressure_warning_state: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Load DB-backed privacy policy and attach it to this consumer.
    pub async fn with_policy_engine(mut self) -> EventEngineResult<Self> {
        let engine = crate::event_engine::policy::PolicyEngine::load(self.pool.clone())
            .await
            .map_err(|e| {
                SinexError::configuration("failed to load DB privacy policy at admission")
                    .with_std_error(&e)
            })?;
        self.policy_engine = Arc::new(engine);
        Ok(self)
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

    /// Set the physical stream/storage lane for this consumer.
    #[must_use]
    pub fn with_event_lane(mut self, lane: JetStreamEventLane) -> Self {
        self.admission.set_storage_lane(match lane {
            JetStreamEventLane::Activity => EventStorageLane::Activity,
            JetStreamEventLane::Reflection => EventStorageLane::Reflection,
        });
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
        source_material_ready_dlq_threshold: Option<i64>,
        source_material_ready_retry_delay: Option<Duration>,
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
        consumer.source_material_ready_dlq_threshold = source_material_ready_dlq_threshold;
        consumer.source_material_ready_retry_delay = source_material_ready_retry_delay;
        consumer
    }
}
