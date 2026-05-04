#![doc = include_str!("../docs/config.md")]

//! Configuration helpers for the ingestion daemon.

use crate::{IngestdResult, SinexError, material_assembler::DurabilityThresholds};
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sinex_db::{PoolConfig, PoolSessionPolicy, create_pool_with_config_and_session_policy};
use sinex_primitives::{
    env as shared_env,
    environment::environment,
    units::{Bytes, Milliseconds, Seconds},
    utils::wait_helpers::RetryConfig,
    validation::deserialize_validated_utf8_path,
};
use std::time::Duration;
use tracing::{debug, error, info, warn};
use validator::{Validate, ValidationError};

/// Configuration for the ingestion daemon
#[derive(Debug, Clone, Serialize, Deserialize, Validate, bon::Builder)]
#[builder(on(String, into))]
pub struct IngestdConfig {
    /// Database URL for `PostgreSQL` connection
    #[validate(url(message = "Invalid database URL"))]
    #[validate(custom(
        function = "validate_postgres_url",
        message = "Must be a PostgreSQL URL"
    ))]
    #[builder(default = default_database_url())]
    pub database_url: String,

    /// Database connection pool size
    #[validate(range(min = 1, max = 1000, message = "Pool size must be between 1 and 1000"))]
    #[builder(default = 50)]
    pub database_pool_size: u32,

    /// Connection acquisition timeout in seconds
    ///
    /// Set via: `SINEX_INGESTD_POOL_ACQUIRE_TIMEOUT_SECS=30`
    #[serde(default = "default_pool_acquire_timeout_secs")]
    #[builder(default = default_pool_acquire_timeout_secs())]
    #[validate(range(min = 1, max = 300))]
    pub pool_acquire_timeout_secs: u64,

    /// Idle connection timeout in seconds
    ///
    /// Set via: `SINEX_INGESTD_POOL_IDLE_TIMEOUT_SECS=600`
    #[serde(default = "default_pool_idle_timeout_secs")]
    #[builder(default = default_pool_idle_timeout_secs())]
    #[validate(range(min = 10, max = 3600))]
    pub pool_idle_timeout_secs: u64,

    /// NATS connection configuration
    #[validate(custom(function = "validate_nats_config"))]
    #[builder(default)]
    pub nats: sinex_primitives::nats::NatsConnectionConfig,

    /// Maximum messages to fetch per `JetStream` pull batch
    #[builder(default = default_consumer_fetch_max_messages())]
    #[validate(range(
        min = 1,
        max = 10_000,
        message = "Fetch batch size must be between 1 and 10000"
    ))]
    pub consumer_fetch_max_messages: usize,
    /// `JetStream` pull expiration timeout in milliseconds
    #[serde(default = "default_consumer_fetch_timeout_ms")]
    #[builder(default = default_consumer_fetch_timeout_ms())]
    #[validate(custom(function = "validate_fetch_timeout"))]
    pub consumer_fetch_timeout_ms: Milliseconds,
    /// Maximum unacknowledged messages for the main `JetStream` consumer
    #[builder(default = default_consumer_max_ack_pending())]
    #[validate(range(
        min = 1,
        max = 10_000,
        message = "Consumer max_ack_pending must be between 1 and 10000"
    ))]
    pub consumer_max_ack_pending: i64,
    /// Maximum unacknowledged messages for the material slices consumer
    #[builder(default = default_material_slices_max_ack_pending())]
    #[validate(range(
        min = 1,
        max = 100_000,
        message = "Material slices max_ack_pending must be between 1 and 100000"
    ))]
    pub material_slices_max_ack_pending: i64,

    /// Enable dry-run mode (no database writes)
    #[builder(default = false)]
    pub dry_run: bool,

    /// Enable schema validation
    #[builder(default = true)]
    pub validate_schemas: bool,

    /// Skip schema synchronization on startup (useful for tests)
    #[builder(default = false)]
    pub skip_schema_sync: bool,

    /// Working directory for temporary files
    #[serde(deserialize_with = "deserialize_validated_utf8_path")]
    #[validate(custom(function = "validate_work_dir", message = "Invalid work directory"))]
    #[builder(default = default_work_dir())]
    pub work_dir: Utf8PathBuf,

    /// Maximum message size in bytes
    #[serde(default = "default_max_message_size")]
    #[builder(default = default_max_message_size())]
    #[validate(custom(function = "validate_max_message_size"))]
    pub max_message_size: Bytes,

    /// NATS stream name for events
    #[validate(length(min = 1, message = "NATS stream name cannot be empty"))]
    #[builder(default = default_nats_stream_name())]
    pub nats_stream_name: String,

    /// NATS consumer durable name
    #[validate(length(min = 1, message = "NATS consumer name cannot be empty"))]
    #[builder(default = String::from("ingestd"))]
    pub nats_consumer_name: String,

    /// Optional namespace appended to all `JetStream` subjects/streams (used by tests).
    #[serde(default)]
    pub nats_namespace: Option<String>,

    /// Content-store root path for assembled materials
    #[serde(deserialize_with = "deserialize_validated_utf8_path")]
    #[validate(custom(
        function = "validate_content_store_path",
        message = "Invalid content-store path"
    ))]
    #[builder(default = default_content_store_path())]
    pub content_store_path: Utf8PathBuf,

    /// Directory used to persist in-flight assembler state between restarts
    #[serde(deserialize_with = "deserialize_validated_utf8_path")]
    #[validate(custom(
        function = "validate_state_dir",
        message = "Invalid assembler state directory"
    ))]
    #[builder(default = default_assembler_state_dir())]
    pub assembler_state_dir: Utf8PathBuf,

    /// Strict validation mode: reject events without registered schemas
    ///
    /// When enabled, ingestd will reject any event that doesn't have a registered schema.
    /// When disabled (default), events without schemas are allowed but won't be validated.
    ///
    /// Set via: `SINEX_INGESTD_STRICT_VALIDATION=true`
    #[serde(default)]
    #[builder(default = false)]
    pub strict_validation: bool,

    /// Maximum number of seconds `ts_orig` may exceed wall-clock time before the event
    /// is considered implausibly future-dated and routed to the DLQ.
    ///
    /// Set via: `SINEX_INGESTD_TS_ORIG_FUTURE_SKEW_SECS=3600`
    #[serde(default = "default_ts_orig_future_skew_secs")]
    #[builder(default = default_ts_orig_future_skew_secs())]
    #[validate(range(min = 1, max = 86400))]
    pub ts_orig_future_skew_secs: u64,

    /// Earliest accepted `ts_orig` expressed as a Unix timestamp (seconds since epoch).
    /// Events with `ts_orig` before this date are considered implausibly old and routed to the DLQ.
    ///
    /// Default: `946684800` (2000-01-01 00:00:00 UTC).
    ///
    /// Set via: `SINEX_INGESTD_TS_ORIG_LOWER_BOUND_UNIX=946684800`
    #[serde(default = "default_ts_orig_lower_bound_unix")]
    #[builder(default = default_ts_orig_lower_bound_unix())]
    #[validate(range(min = 0))]
    pub ts_orig_lower_bound_unix: i64,

    /// Maximum buffered out-of-order slices per material assembly
    ///
    /// Set via: `SINEX_INGESTD_MAX_BUFFERED_SLICES=100`
    #[serde(default = "default_max_buffered_slices")]
    #[builder(default = default_max_buffered_slices())]
    #[validate(range(min = 1, max = 10000))]
    pub max_buffered_slices: usize,

    /// Slice arrival timeout in seconds
    ///
    /// Set via: `SINEX_INGESTD_SLICE_TIMEOUT_SECS=300`
    #[serde(default = "default_slice_timeout_secs")]
    #[builder(default = default_slice_timeout_secs())]
    #[validate(range(min = 10, max = 86400))]
    pub slice_timeout_secs: u64,

    /// Orphaned file age threshold in seconds
    ///
    /// Set via: `SINEX_INGESTD_ORPHAN_THRESHOLD_SECS=3600`
    #[serde(default = "default_orphan_threshold_secs")]
    #[builder(default = default_orphan_threshold_secs())]
    #[validate(range(min = 60, max = 604800))]
    pub orphan_threshold_secs: u64,

    /// Disk usage threshold percentage at which the assembler starts refusing new
    /// assemblies to prevent filling the filesystem.
    ///
    /// Set via: `SINEX_INGESTD_DISK_THRESHOLD_PERCENT=90`
    #[serde(default = "default_disk_threshold_percent")]
    #[builder(default = default_disk_threshold_percent())]
    #[validate(range(min = 50, max = 99))]
    pub disk_threshold_percent: u8,

    /// Maximum total bytes a single material assembly may accumulate before it is
    /// rejected and routed to the DLQ.
    ///
    /// Set via: `SINEX_INGESTD_MAX_MATERIAL_SIZE_BYTES=536870912`
    #[serde(default = "default_max_material_size_bytes")]
    #[builder(default = default_max_material_size_bytes())]
    #[validate(custom(function = "validate_material_size_limit"))]
    pub max_material_size_bytes: Bytes,

    /// Bytes of staged material writes buffered before the assembler forces
    /// `flush + fsync` on the staged file.
    ///
    /// Set via: `SINEX_INGESTD_MATERIAL_STAGED_SYNC_BYTES=1048576`
    #[serde(default = "default_material_staged_sync_bytes")]
    #[builder(default = default_material_staged_sync_bytes())]
    #[validate(custom(function = "validate_positive_bytes"))]
    pub material_staged_sync_bytes: Bytes,

    /// Maximum elapsed time between staged-file `flush + fsync` operations.
    ///
    /// Set via: `SINEX_INGESTD_MATERIAL_STAGED_SYNC_INTERVAL_MS=1000`
    #[serde(default = "default_material_staged_sync_interval_ms")]
    #[builder(default = default_material_staged_sync_interval_ms())]
    #[validate(custom(function = "validate_positive_milliseconds"))]
    pub material_staged_sync_interval_ms: Milliseconds,

    /// Bytes of WAL writes buffered before the assembler forces WAL fsync.
    ///
    /// Set via: `SINEX_INGESTD_MATERIAL_WAL_SYNC_BYTES=262144`
    #[serde(default = "default_material_wal_sync_bytes")]
    #[builder(default = default_material_wal_sync_bytes())]
    #[validate(custom(function = "validate_positive_bytes"))]
    pub material_wal_sync_bytes: Bytes,

    /// WAL entries buffered before the assembler forces WAL fsync.
    ///
    /// Set via: `SINEX_INGESTD_MATERIAL_WAL_SYNC_ENTRIES=128`
    #[serde(default = "default_material_wal_sync_entries")]
    #[builder(default = default_material_wal_sync_entries())]
    #[validate(range(min = 1, max = 100000))]
    pub material_wal_sync_entries: u32,

    /// Maximum elapsed time between WAL fsync operations.
    ///
    /// Set via: `SINEX_INGESTD_MATERIAL_WAL_SYNC_INTERVAL_MS=1000`
    #[serde(default = "default_material_wal_sync_interval_ms")]
    #[builder(default = default_material_wal_sync_interval_ms())]
    #[validate(custom(function = "validate_positive_milliseconds"))]
    pub material_wal_sync_interval_ms: Milliseconds,

    /// Enable `GitOps` schema sync service
    ///
    /// When enabled, ingestd periodically fetches configured Git repositories
    /// and discovers JSON schema files to register in the database.
    ///
    /// Set via: `SINEX_INGESTD_GITOPS_ENABLED=true`
    #[serde(default)]
    #[builder(default = false)]
    pub gitops_enabled: bool,

    /// Working directory for `GitOps` repository clones
    ///
    /// Set via: `SINEX_INGESTD_GITOPS_WORK_DIR=/path/to/dir`
    #[serde(default = "default_gitops_work_dir")]
    #[builder(default = default_gitops_work_dir())]
    pub gitops_work_dir: Utf8PathBuf,

    /// Schema reload interval in seconds
    ///
    /// How often ingestd reloads JSON schemas from the database.
    /// Lower values make schema updates take effect faster at the cost of more DB queries.
    ///
    /// Set via: `SINEX_INGESTD_SCHEMA_RELOAD_INTERVAL_SECS=300`
    #[serde(default = "default_schema_reload_interval_secs")]
    #[builder(default = default_schema_reload_interval_secs())]
    #[validate(range(min = 10, max = 3600))]
    pub schema_reload_interval_secs: u64,

    /// Stats logging interval in seconds
    ///
    /// How often ingestd logs processing statistics (events processed, failed, etc.).
    ///
    /// Set via: `SINEX_INGESTD_STATS_LOG_INTERVAL_SECS=60`
    #[serde(default = "default_stats_log_interval_secs")]
    #[builder(default = default_stats_log_interval_secs())]
    #[validate(range(min = 5, max = 3600))]
    pub stats_log_interval_secs: u64,

    /// Core retry configuration (max attempts, delays, backoff, jitter).
    ///
    /// Controls retry behavior for transient failures (DLQ, DB, NATS).
    /// Individual env-var overrides are available via `SINEX_INGESTD_RETRY_MAX_ATTEMPTS` etc.
    #[serde(skip)]
    #[builder(default = RetryConfig::default())]
    pub retry_config: RetryConfig,

    /// Max concurrent batch-processing tasks during startup catch-up.
    /// Limits I/O pressure while the consumer works through the backlog.
    /// Default: 4. Set to 0 to disable catch-up limiting (full speed).
    ///
    /// Set via: `SINEX_INGESTD_STARTUP_CATCH_UP_MAX_CONCURRENT=4`
    #[serde(default = "default_startup_catch_up_max_concurrent")]
    #[builder(default = default_startup_catch_up_max_concurrent())]
    #[validate(range(min = 0, max = 256))]
    pub startup_catch_up_max_concurrent: usize,

    /// Interval between automatic blob garbage collection sweeps. None = disabled.
    ///
    /// When set, ingestd periodically sweeps content-store keys that are unused
    /// in git-annex AND have no matching `core.blobs` row, dropping them from
    /// the large-object backend. The same logic backs `sinexctl blob sweep-orphans`.
    ///
    /// Set via: `SINEX_INGESTD_BLOB_GC_INTERVAL_SECS`
    #[serde(default = "default_blob_gc_interval_secs")]
    #[validate(custom(function = "validate_blob_gc_interval"))]
    pub blob_gc_interval_secs: Option<u64>,
}

impl IngestdConfig {
    /// Create configuration from command line arguments using the builder
    pub fn from_args(
        database_url: Option<String>,
        nats_url: String,
        nats_require_tls: bool,
        pool_size: u32,
        consumer_fetch_max_messages: Option<usize>,
        consumer_fetch_timeout_ms: Option<u64>,
        consumer_max_ack_pending: Option<i64>,
        material_slices_max_ack_pending: Option<i64>,
        dry_run: bool,
        content_store_path: Option<String>,
        assembler_state_dir: Option<String>,
        namespace: Option<String>,
    ) -> IngestdResult<Self> {
        let work_dir_override =
            strict_env_validated_path("SINEX_INGESTD_WORK_DIR", "ingestd work dir")?;
        let content_store_env_override =
            strict_env_validated_path("SINEX_CONTENT_STORE_PATH", "content-store path")?;
        let assembler_state_dir_env_override =
            strict_env_validated_path("SINEX_ASSEMBLER_STATE_DIR", "assembler state directory")?;
        let gitops_work_dir_override =
            strict_env_validated_path("SINEX_INGESTD_GITOPS_WORK_DIR", "gitops work directory")?;
        let skip_schema_sync = shared_env::strict_flag("SINEX_SKIP_SCHEMA_SYNC")?.unwrap_or(false);
        let validate_schemas = shared_env::strict_flag("SINEX_VALIDATE_SCHEMAS")?.unwrap_or(true);
        let strict_validation = shared_env::strict_flag("SINEX_INGESTD_STRICT_VALIDATION")?.unwrap_or(false);
        let gitops_enabled = shared_env::strict_flag("SINEX_INGESTD_GITOPS_ENABLED")?.unwrap_or(false);
        let consumer_fetch_max_messages_env =
            shared_env::strict_parsed("SINEX_INGESTD_CONSUMER_FETCH_MAX_MESSAGES")?;
        let consumer_fetch_timeout_ms_env = shared_env::strict_parsed("SINEX_INGESTD_CONSUMER_FETCH_TIMEOUT_MS")?;
        let consumer_max_ack_pending_env = shared_env::strict_parsed("SINEX_INGESTD_CONSUMER_MAX_ACK_PENDING")?;
        let material_slices_max_ack_pending_env =
            shared_env::strict_parsed("SINEX_INGESTD_MATERIAL_SLICES_MAX_ACK_PENDING")?;
        let schema_reload_interval_secs: u64 =
            shared_env::strict_parsed("SINEX_INGESTD_SCHEMA_RELOAD_INTERVAL_SECS")?
                .unwrap_or_else(default_schema_reload_interval_secs);
        let stats_log_interval_secs: u64 = shared_env::strict_parsed("SINEX_INGESTD_STATS_LOG_INTERVAL_SECS")?
            .unwrap_or_else(default_stats_log_interval_secs);
        let blob_gc_interval_secs: Option<u64> =
            shared_env::strict_parsed("SINEX_INGESTD_BLOB_GC_INTERVAL_SECS")?;
        let pool_acquire_timeout_secs: u64 = shared_env::strict_parsed("SINEX_INGESTD_POOL_ACQUIRE_TIMEOUT_SECS")?
            .unwrap_or_else(default_pool_acquire_timeout_secs);
        let pool_idle_timeout_secs: u64 = shared_env::strict_parsed("SINEX_INGESTD_POOL_IDLE_TIMEOUT_SECS")?
            .unwrap_or_else(default_pool_idle_timeout_secs);
        let ts_orig_future_skew_secs: u64 =
            shared_env::strict_parsed("SINEX_INGESTD_TS_ORIG_FUTURE_SKEW_SECS")?
                .unwrap_or_else(default_ts_orig_future_skew_secs);
        let ts_orig_lower_bound_unix: i64 =
            shared_env::strict_parsed("SINEX_INGESTD_TS_ORIG_LOWER_BOUND_UNIX")?
                .unwrap_or_else(default_ts_orig_lower_bound_unix);

        // Construct NatsConnectionConfig from args/environment.
        // Full auth/TLS detail is still supplied via the shared env-first NATS config.
        let mut nats_config = sinex_primitives::nats::NatsConnectionConfig::from_env();
        nats_config.url = nats_url;
        nats_config.require_tls = nats_require_tls;
        let nats_config_clone = nats_config;

        let db_url = match database_url {
            Some(url) => url,
            None => shared_env::strict_var("DATABASE_URL")?.unwrap_or_else(default_database_url_fallback),
        };
        let mut config = Self::default();
        config.database_url = db_url;
        config.database_pool_size = pool_size;
        config.pool_acquire_timeout_secs = pool_acquire_timeout_secs;
        config.pool_idle_timeout_secs = pool_idle_timeout_secs;
        config.dry_run = dry_run;
        config.skip_schema_sync = skip_schema_sync;
        config.validate_schemas = validate_schemas;
        config.strict_validation = strict_validation;
        config.gitops_enabled = gitops_enabled;
        config.schema_reload_interval_secs = schema_reload_interval_secs;
        config.stats_log_interval_secs = stats_log_interval_secs;
        config.blob_gc_interval_secs = blob_gc_interval_secs;
        config.ts_orig_future_skew_secs = ts_orig_future_skew_secs;
        config.ts_orig_lower_bound_unix = ts_orig_lower_bound_unix;
        config.nats = nats_config_clone;
        config.nats_namespace = namespace;
        if let Some(path) = work_dir_override {
            config.work_dir = path;
        }
        if let Some(path) = content_store_env_override {
            config.content_store_path = path;
        }
        if let Some(path) = gitops_work_dir_override {
            config.gitops_work_dir = path;
        }
        if let Some(path) = assembler_state_dir_env_override {
            config.assembler_state_dir = path;
        }

        // Override fetch config if specified
        if let Some(max_msgs) = consumer_fetch_max_messages.or(consumer_fetch_max_messages_env) {
            config.consumer_fetch_max_messages = max_msgs;
        }
        if let Some(timeout_ms) = consumer_fetch_timeout_ms.or(consumer_fetch_timeout_ms_env) {
            config.consumer_fetch_timeout_ms = Milliseconds::from_millis(timeout_ms);
        }
        if let Some(pending) = consumer_max_ack_pending.or(consumer_max_ack_pending_env) {
            config.consumer_max_ack_pending = pending;
        }
        if let Some(pending) =
            material_slices_max_ack_pending.or(material_slices_max_ack_pending_env)
        {
            config.material_slices_max_ack_pending = pending;
        }

        if let Some(path) = content_store_path {
            config.content_store_path = validated_path_override(&path, "content-store path")?;
        }

        if let Some(path) = assembler_state_dir {
            config.assembler_state_dir =
                validated_path_override(&path, "assembler state directory")?;
        }

        // Retry config overrides from env vars
        if let Some(value) = shared_env::strict_parsed("SINEX_INGESTD_RETRY_MAX_ATTEMPTS")? {
            config.retry_config.max_attempts = value;
        }
        if let Some(value) =
            shared_env::strict_parsed::<u64>("SINEX_INGESTD_RETRY_INITIAL_DELAY_MS")?
        {
            config.retry_config.initial_delay = Duration::from_millis(value);
        }
        if let Some(value) = shared_env::strict_parsed::<u64>("SINEX_INGESTD_RETRY_MAX_DELAY_MS")? {
            config.retry_config.max_delay = Duration::from_millis(value);
        }
        if let Some(value) = shared_env::strict_parsed("SINEX_INGESTD_RETRY_MULTIPLIER")? {
            config.retry_config.multiplier = value;
        }
        if let Some(value) = shared_env::strict_flag("SINEX_INGESTD_RETRY_JITTER")? {
            config.retry_config.jitter = value;
        }
        if let Some(value) =
            shared_env::strict_parsed::<u64>("SINEX_INGESTD_RETRY_PUBLISH_ACK_TIMEOUT_MS")?
        {
            config.retry_config.publish_ack_timeout = Duration::from_millis(value);
        }
        // Startup catch-up concurrency override from env
        if let Some(value) =
            shared_env::strict_parsed("SINEX_INGESTD_STARTUP_CATCH_UP_MAX_CONCURRENT")?
        {
            config.startup_catch_up_max_concurrent = value;
        }

        Ok(config.normalize())
    }

    fn normalize(mut self) -> Self {
        // When a namespace is set (test isolation), apply it to the stream name
        // so each test gets its own JetStream stream.
        if let Some(ref ns) = self.nats_namespace {
            let env = environment();
            self.nats_stream_name =
                env.nats_stream_name_with_namespace(Some(ns), &self.nats_stream_name);
        }

        self
    }

    pub(crate) fn material_durability_thresholds(&self) -> IngestdResult<DurabilityThresholds> {
        let staged_bytes =
            i64::try_from(self.material_staged_sync_bytes.as_u64()).map_err(|_| {
                SinexError::configuration("staged material sync byte threshold exceeds i64 range")
                    .with_context(
                        "material_staged_sync_bytes",
                        self.material_staged_sync_bytes.as_u64().to_string(),
                    )
            })?;
        let wal_bytes = usize::try_from(self.material_wal_sync_bytes.as_u64()).map_err(|_| {
            SinexError::configuration("WAL sync byte threshold exceeds usize range").with_context(
                "material_wal_sync_bytes",
                self.material_wal_sync_bytes.as_u64().to_string(),
            )
        })?;

        DurabilityThresholds::try_new(
            staged_bytes,
            Duration::from_millis(self.material_staged_sync_interval_ms.as_millis()),
            wal_bytes,
            self.material_wal_sync_entries,
            Duration::from_millis(self.material_wal_sync_interval_ms.as_millis()),
        )
    }

    /// Validate the configuration
    pub async fn validate(&self) -> IngestdResult<()> {
        use validator::Validate as ValidateTrait;

        // Fail fast on NATS TLS policy before running other validators.
        self.nats.validate().map_err(|e| {
            SinexError::configuration(e.to_string()).with_operation("config.validate_nats")
        })?;

        // Run validator crate validation for the rest of the fields.
        ValidateTrait::validate(self).map_err(|e| {
            SinexError::configuration(format!("Validation failed: {e}"))
                .with_operation("config.validate_connection_strings")
        })?;

        // Ensure work directory exists using atomic create_dir_all
        match tokio::fs::create_dir_all(&self.work_dir).await {
            Ok(()) => {
                debug!("Ensured work directory exists: {}", self.work_dir.as_str());
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Directory already exists, this is fine
                debug!("Work directory already exists: {}", self.work_dir.as_str());
            }
            Err(e) => {
                return Err(SinexError::configuration(format!(
                    "Cannot create work directory {}: {}",
                    self.work_dir.as_str(),
                    e
                )));
            }
        }

        if tokio::fs::metadata(&self.content_store_path).await.is_err() {
            warn!(
                path = %self.content_store_path,
                "Content-store path does not exist; git-annex backend will attempt initialization"
            );
        }

        if let Err(e) = tokio::fs::create_dir_all(&self.assembler_state_dir).await {
            return Err(SinexError::configuration(format!(
                "Cannot create assembler state directory {}: {}",
                self.assembler_state_dir.as_str(),
                e
            )));
        }

        // Test database connection
        self.test_database_connection().await?;

        // Test NATS connection
        self.test_nats_connection().await?;

        info!("Configuration validation passed");
        Ok(())
    }

    /// Test database connection
    async fn test_database_connection(&self) -> IngestdResult<()> {
        let mut config = self.pool_config();
        config.max_connections = 1;

        let pool = create_pool_with_config_and_session_policy(
            &self.database_url,
            &config,
            PoolSessionPolicy::SqlxDefaults,
        )
        .await?;

        // Test basic query with a simple SELECT 1
        sqlx::query!("SELECT 1 as one")
            .fetch_one(&pool)
            .await
            .map_err(|e| {
                SinexError::configuration(format!("Database connection test failed: {e}"))
                    .with_operation("config.test_database_connection")
                    .with_context("database_url", sinex_primitives::utils::redact_url_password_for_diagnostics(&self.database_url, sinex_primitives::utils::InvalidUrlPolicy::RedactedMarker))
            })?;

        pool.close().await;
        info!("Database connection test passed");
        Ok(())
    }

    /// Test NATS connection
    async fn test_nats_connection(&self) -> IngestdResult<()> {
        let client = self.nats.connect().await?;

        // Connection successful logic is implicit in connect() success
        // But we can check status if needed. connect() returns a connected client.

        info!("NATS connection test passed");
        drop(client);
        Ok(())
    }

    /// Get database connection options
    #[must_use]
    pub fn pool_config(&self) -> PoolConfig {
        PoolConfig {
            max_connections: self.database_pool_size,
            min_connections: 0,
            acquire_timeout_secs: Seconds::from_secs(self.pool_acquire_timeout_secs),
            idle_timeout_secs: Seconds::from_secs(self.pool_idle_timeout_secs),
            statement_timeout_secs: Seconds::from_secs(0),
            max_lifetime_secs: Some(Seconds::from_secs(30 * 60)),
            validate_against_postgres_max: false,
        }
        // Intentionally rely on sqlx's built-in connection health checks.
        // A custom before_acquire rollback/reset hook can deadlock after
        // specific protocol exchanges (for example COPY IN).
        // See: https://github.com/launchbadge/sqlx/issues/3117
    }

    pub async fn create_db_pool(&self) -> IngestdResult<sinex_db::DbPool> {
        create_pool_with_config_and_session_policy(
            &self.database_url,
            &self.pool_config(),
            PoolSessionPolicy::SqlxDefaults,
        )
        .await
        .map_err(|e| {
            SinexError::database(format!("Failed to connect to database: {e}"))
                .with_operation("service.init_db_pool")
        })
    }
}

fn env_validated_path(name: &str, context: &str) -> Option<Utf8PathBuf> {
    use sinex_primitives::validation::validate_path;

    let raw = shared_env::var_optional(name, context)?;
    match validate_path(&raw) {
        Ok(validated) => Some(validated),
        Err(error) => {
            warn!(
                env = name,
                value = %raw,
                %error,
                "Invalid path override for {context}; ignoring override"
            );
            None
        }
    }
}

fn strict_env_validated_path(name: &str, context: &str) -> IngestdResult<Option<Utf8PathBuf>> {
    let Some(raw) = shared_env::strict_var(name)? else {
        return Ok(None);
    };

    validated_path_override(&raw, context)
        .map(Some)
        .map_err(|error| {
            error
                .with_context("environment_variable", name)
                .with_context("raw_value", raw)
        })
}

fn validated_path_override(raw: &str, context: &str) -> IngestdResult<Utf8PathBuf> {
    sinex_primitives::validation::validate_path(raw).map_err(|error| {
        SinexError::configuration(format!("invalid path value for {context}"))
            .with_context("context", context)
            .with_std_error(&error)
    })
}

fn default_path_base_dir() -> Utf8PathBuf {
    if let Some(path) = dirs::cache_dir() {
        match Utf8PathBuf::from_path_buf(path) {
            Ok(path) => path,
            Err(path) => {
                warn!(
                    path = %path.display(),
                    "Cache directory path is not valid UTF-8; falling back to /tmp"
                );
                Utf8PathBuf::from("/tmp")
            }
        }
    } else {
        warn!("Cache directory unavailable; falling back to /tmp");
        Utf8PathBuf::from("/tmp")
    }
}

fn validated_path_or_fallback(
    candidate: &Utf8PathBuf,
    fallback: Utf8PathBuf,
    context: &str,
) -> Utf8PathBuf {
    use sinex_primitives::validation::validate_path;

    match validate_path(candidate.as_str()) {
        Ok(validated) => validated,
        Err(error) => {
            warn!(
                path = %candidate,
                fallback = %fallback,
                %error,
                "Derived default path for {context} is invalid; using fallback"
            );
            fallback
        }
    }
}

impl Default for IngestdConfig {
    fn default() -> Self {
        let env = environment();
        Self {
            database_url: default_database_url(),
            database_pool_size: 50,
            pool_acquire_timeout_secs: default_pool_acquire_timeout_secs(),
            pool_idle_timeout_secs: default_pool_idle_timeout_secs(),
            nats: sinex_primitives::nats::NatsConnectionConfig::from_env(),
            consumer_fetch_max_messages: default_consumer_fetch_max_messages(),
            consumer_fetch_timeout_ms: default_consumer_fetch_timeout_ms(),
            consumer_max_ack_pending: default_consumer_max_ack_pending(),
            material_slices_max_ack_pending: default_material_slices_max_ack_pending(),
            dry_run: false,
            validate_schemas: true,
            skip_schema_sync: false,
            work_dir: default_work_dir(),
            max_message_size: default_max_message_size(),
            nats_stream_name: default_nats_stream_name(),
            nats_consumer_name: format!("ingestd-{}", env.name()),
            nats_namespace: None,
            content_store_path: default_content_store_path(),
            assembler_state_dir: default_assembler_state_dir(),
            strict_validation: false,
            ts_orig_future_skew_secs: default_ts_orig_future_skew_secs(),
            ts_orig_lower_bound_unix: default_ts_orig_lower_bound_unix(),
            max_buffered_slices: default_max_buffered_slices(),
            slice_timeout_secs: default_slice_timeout_secs(),
            orphan_threshold_secs: default_orphan_threshold_secs(),
            disk_threshold_percent: default_disk_threshold_percent(),
            max_material_size_bytes: default_max_material_size_bytes(),
            material_staged_sync_bytes: default_material_staged_sync_bytes(),
            material_staged_sync_interval_ms: default_material_staged_sync_interval_ms(),
            material_wal_sync_bytes: default_material_wal_sync_bytes(),
            material_wal_sync_entries: default_material_wal_sync_entries(),
            material_wal_sync_interval_ms: default_material_wal_sync_interval_ms(),
            gitops_enabled: false,
            gitops_work_dir: default_gitops_work_dir(),
            schema_reload_interval_secs: default_schema_reload_interval_secs(),
            stats_log_interval_secs: default_stats_log_interval_secs(),
            retry_config: RetryConfig::default(),
            startup_catch_up_max_concurrent: default_startup_catch_up_max_concurrent(),
            blob_gc_interval_secs: default_blob_gc_interval_secs(),
        }
    }
}

// Helper functions

/// Default database URL with environment namespacing.
///
/// Explicit `DATABASE_URL` values are treated as exact operator input and are
/// no longer rewritten through the ambient Sinex environment.
fn default_database_url() -> String {
    match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(std::env::VarError::NotPresent) => default_database_url_fallback(),
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                "DATABASE_URL is not valid UTF-8; falling back to the namespaced local database URL"
            );
            default_database_url_fallback()
        }
    }
}

fn default_database_url_fallback() -> String {
    let env = environment();
    let base_name = env.database_name("sinex");
    format!("postgresql:///{base_name}?host=/run/postgresql")
}

/// Default work directory for ingestd with environment namespacing
fn default_work_dir() -> Utf8PathBuf {
    if let Some(validated) = env_validated_path("SINEX_INGESTD_WORK_DIR", "ingestd work dir") {
        return validated;
    }

    let env = environment();
    let work_dir = Utf8PathBuf::from_path_buf(
        env.work_directory(default_path_base_dir().join("sinex").join("ingestd")),
    )
    .unwrap_or_else(|path| {
        warn!(
            path = %path.display(),
            "Derived ingestd work directory is not valid UTF-8; using fallback"
        );
        Utf8PathBuf::from("/tmp/sinex/ingestd")
    });
    let fallback = Utf8PathBuf::from_path_buf(env.work_directory("/tmp/sinex/ingestd"))
        .unwrap_or_else(|path| {
            warn!(
                path = %path.display(),
                "Fallback ingestd work directory is not valid UTF-8; using literal /tmp path"
            );
            Utf8PathBuf::from("/tmp/sinex/ingestd")
        });
    validated_path_or_fallback(&work_dir, fallback, "ingestd work dir")
}

fn default_pool_acquire_timeout_secs() -> u64 {
    30
}

fn default_pool_idle_timeout_secs() -> u64 {
    600
}

fn default_consumer_fetch_max_messages() -> usize {
    match shared_env::strict_parsed("SINEX_INGESTD_CONSUMER_FETCH_MAX_MESSAGES") {
        Ok(Some(value)) => value,
        Ok(None) => 100,
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_CONSUMER_FETCH_MAX_MESSAGES",
                %error,
                "Invalid env override for consumer fetch max messages; using default"
            );
            100
        }
    }
}

fn default_consumer_fetch_timeout_ms() -> Milliseconds {
    match shared_env::strict_parsed("SINEX_INGESTD_CONSUMER_FETCH_TIMEOUT_MS") {
        Ok(Some(value)) => Milliseconds::from_millis(value),
        Ok(None) => Milliseconds::from_millis(100),
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_CONSUMER_FETCH_TIMEOUT_MS",
                %error,
                "Invalid env override for consumer fetch timeout; using default"
            );
            Milliseconds::from_millis(100)
        }
    }
}

fn default_consumer_max_ack_pending() -> i64 {
    match shared_env::strict_parsed("SINEX_INGESTD_CONSUMER_MAX_ACK_PENDING") {
        Ok(Some(value)) => value,
        Ok(None) => 100,
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_CONSUMER_MAX_ACK_PENDING",
                %error,
                "Invalid env override for consumer max_ack_pending; using default"
            );
            100
        }
    }
}

fn default_material_slices_max_ack_pending() -> i64 {
    match shared_env::strict_parsed("SINEX_INGESTD_MATERIAL_SLICES_MAX_ACK_PENDING") {
        Ok(Some(value)) => value,
        Ok(None) => 1_000,
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_MATERIAL_SLICES_MAX_ACK_PENDING",
                %error,
                "Invalid env override for material slices max_ack_pending; using default"
            );
            1_000
        }
    }
}

fn default_max_message_size() -> Bytes {
    Bytes::from_mebibytes(16)
}

/// Default NATS stream name with environment namespacing
fn default_nats_stream_name() -> String {
    let env = environment();
    env.nats_stream_name("SINEX_RAW_EVENTS")
}

// Custom validator functions

fn validate_postgres_url(url: &str) -> Result<(), validator::ValidationError> {
    match url::Url::parse(url) {
        Ok(parsed_url) => {
            if matches!(parsed_url.scheme(), "postgresql" | "postgres") {
                Ok(())
            } else {
                Err(validator::ValidationError::new("not_postgres_url"))
            }
        }
        Err(_) => Err(validator::ValidationError::new("invalid_url")),
    }
}

fn validate_nats_config(
    config: &sinex_primitives::nats::NatsConnectionConfig,
) -> Result<(), ValidationError> {
    if config.url.trim().is_empty() {
        return Err(ValidationError::new("nats_url_empty"));
    }
    if config.require_tls && !config.url.starts_with("tls://") && !config.url.starts_with("wss://")
    {
        return Err(ValidationError::new("nats_tls_required"));
    }
    Ok(())
}

fn validate_work_dir(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    use sinex_primitives::validation::validate_path;

    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_work_dir"))
}

fn default_content_store_path() -> Utf8PathBuf {
    if let Some(validated) = env_validated_path("SINEX_CONTENT_STORE_PATH", "content-store path") {
        return validated;
    }

    let content_store = default_work_dir().join("content-store");
    validated_path_or_fallback(
        &content_store,
        Utf8PathBuf::from("/tmp/sinex/ingestd/content-store"),
        "content-store path",
    )
}

fn default_assembler_state_dir() -> Utf8PathBuf {
    if let Some(validated) =
        env_validated_path("SINEX_ASSEMBLER_STATE_DIR", "assembler state directory")
    {
        return validated;
    }

    let state_dir = default_work_dir().join("assembler_state");
    validated_path_or_fallback(
        &state_dir,
        Utf8PathBuf::from("/tmp/sinex/ingestd/assembler_state"),
        "assembler state directory",
    )
}

fn validate_content_store_path(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    use sinex_primitives::validation::validate_path;
    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_content_store_path"))
}

fn validate_state_dir(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    use sinex_primitives::validation::validate_path;
    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_state_dir"))
}

fn validate_max_message_size(value: &Bytes) -> Result<(), ValidationError> {
    let bytes = value.as_u64();
    if !(1024..=1_073_741_824).contains(&bytes) {
        return Err(ValidationError::new("range"));
    }
    Ok(())
}

fn validate_material_size_limit(value: &Bytes) -> Result<(), ValidationError> {
    let bytes = value.as_u64();
    if !(1024..=1_073_741_824).contains(&bytes) {
        return Err(ValidationError::new("range"));
    }
    Ok(())
}

fn validate_positive_bytes(value: &Bytes) -> Result<(), ValidationError> {
    if value.as_u64() == 0 {
        return Err(ValidationError::new("range"));
    }
    Ok(())
}

fn validate_positive_milliseconds(value: &Milliseconds) -> Result<(), ValidationError> {
    if value.as_millis() == 0 {
        return Err(ValidationError::new("range"));
    }
    Ok(())
}

fn validate_fetch_timeout(value: &Milliseconds) -> Result<(), ValidationError> {
    let ms = value.as_millis();
    if !(1..=60_000).contains(&ms) {
        return Err(ValidationError::new(
            "Fetch timeout must be between 1 and 60000 ms",
        ));
    }
    Ok(())
}

fn default_max_buffered_slices() -> usize {
    match shared_env::strict_parsed("SINEX_INGESTD_MAX_BUFFERED_SLICES") {
        Ok(Some(value)) => value,
        Ok(None) => 100,
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_MAX_BUFFERED_SLICES",
                %error,
                "Invalid env override for max buffered slices; using default"
            );
            100
        }
    }
}

fn default_slice_timeout_secs() -> u64 {
    match shared_env::strict_parsed("SINEX_INGESTD_SLICE_TIMEOUT_SECS") {
        Ok(Some(value)) => value,
        Ok(None) => 300,
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_SLICE_TIMEOUT_SECS",
                %error,
                "Invalid env override for slice timeout; using default"
            );
            300
        }
    }
}

fn default_orphan_threshold_secs() -> u64 {
    match shared_env::strict_parsed("SINEX_INGESTD_ORPHAN_THRESHOLD_SECS") {
        Ok(Some(value)) => value,
        Ok(None) => 3600,
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_ORPHAN_THRESHOLD_SECS",
                %error,
                "Invalid env override for orphan threshold; using default"
            );
            3600
        }
    }
}

fn default_disk_threshold_percent() -> u8 {
    match shared_env::strict_parsed("SINEX_INGESTD_DISK_THRESHOLD_PERCENT") {
        Ok(Some(value)) => value,
        Ok(None) => 90,
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_DISK_THRESHOLD_PERCENT",
                %error,
                "Invalid env override for disk threshold percent; using default"
            );
            90
        }
    }
}

fn default_max_material_size_bytes() -> Bytes {
    match shared_env::strict_parsed("SINEX_INGESTD_MAX_MATERIAL_SIZE_BYTES") {
        Ok(Some(value)) => Bytes::from_bytes(value),
        Ok(None) => Bytes::from_mebibytes(512),
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_MAX_MATERIAL_SIZE_BYTES",
                %error,
                "Invalid env override for max material size; using default"
            );
            Bytes::from_mebibytes(512)
        }
    }
}

fn default_material_staged_sync_bytes() -> Bytes {
    match shared_env::strict_parsed("SINEX_INGESTD_MATERIAL_STAGED_SYNC_BYTES") {
        Ok(Some(value)) => Bytes::from_bytes(value),
        Ok(None) => Bytes::from_mebibytes(1),
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_MATERIAL_STAGED_SYNC_BYTES",
                %error,
                "Invalid env override for staged material sync bytes; using default"
            );
            Bytes::from_mebibytes(1)
        }
    }
}

fn default_material_staged_sync_interval_ms() -> Milliseconds {
    match shared_env::strict_parsed("SINEX_INGESTD_MATERIAL_STAGED_SYNC_INTERVAL_MS") {
        Ok(Some(value)) => Milliseconds::from_millis(value),
        Ok(None) => Milliseconds::from_millis(1000),
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_MATERIAL_STAGED_SYNC_INTERVAL_MS",
                %error,
                "Invalid env override for staged material sync interval; using default"
            );
            Milliseconds::from_millis(1000)
        }
    }
}

fn default_material_wal_sync_bytes() -> Bytes {
    match shared_env::strict_parsed("SINEX_INGESTD_MATERIAL_WAL_SYNC_BYTES") {
        Ok(Some(value)) => Bytes::from_bytes(value),
        Ok(None) => Bytes::from_kibibytes(256),
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_MATERIAL_WAL_SYNC_BYTES",
                %error,
                "Invalid env override for material WAL sync bytes; using default"
            );
            Bytes::from_kibibytes(256)
        }
    }
}

fn default_material_wal_sync_entries() -> u32 {
    match shared_env::strict_parsed("SINEX_INGESTD_MATERIAL_WAL_SYNC_ENTRIES") {
        Ok(Some(value)) => value,
        Ok(None) => 128,
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_MATERIAL_WAL_SYNC_ENTRIES",
                %error,
                "Invalid env override for material WAL sync entries; using default"
            );
            128
        }
    }
}

fn default_material_wal_sync_interval_ms() -> Milliseconds {
    match shared_env::strict_parsed("SINEX_INGESTD_MATERIAL_WAL_SYNC_INTERVAL_MS") {
        Ok(Some(value)) => Milliseconds::from_millis(value),
        Ok(None) => Milliseconds::from_millis(1000),
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_MATERIAL_WAL_SYNC_INTERVAL_MS",
                %error,
                "Invalid env override for material WAL sync interval; using default"
            );
            Milliseconds::from_millis(1000)
        }
    }
}

fn default_gitops_work_dir() -> Utf8PathBuf {
    if let Some(validated) =
        env_validated_path("SINEX_INGESTD_GITOPS_WORK_DIR", "gitops work directory")
    {
        return validated;
    }

    let gitops = default_work_dir().join("gitops");
    validated_path_or_fallback(
        &gitops,
        Utf8PathBuf::from("/tmp/sinex/ingestd/gitops"),
        "gitops work directory",
    )
}

fn default_schema_reload_interval_secs() -> u64 {
    300 // 5 minutes
}

fn default_stats_log_interval_secs() -> u64 {
    60 // 1 minute
}

fn default_startup_catch_up_max_concurrent() -> usize {
    match shared_env::strict_parsed("SINEX_INGESTD_STARTUP_CATCH_UP_MAX_CONCURRENT") {
        Ok(Some(value)) => value,
        Ok(None) => 4,
        Err(error) => {
            error!(
                env = "SINEX_INGESTD_STARTUP_CATCH_UP_MAX_CONCURRENT",
                %error,
                "Invalid env override for startup catch-up max concurrent; using default"
            );
            4
        }
    }
}

fn default_blob_gc_interval_secs() -> Option<u64> {
    None
}

fn validate_blob_gc_interval(value: u64) -> Result<(), ValidationError> {
    if value < 60 {
        return Err(ValidationError::new("blob_gc_interval_too_small"));
    }
    Ok(())
}

fn default_ts_orig_future_skew_secs() -> u64 {
    3600 // 1 hour
}

fn default_ts_orig_lower_bound_unix() -> i64 {
    946_684_800 // 2000-01-01 00:00:00 UTC
}

#[cfg(test)]
mod tests {
    use super::{
        DurabilityThresholds, IngestdConfig, default_assembler_state_dir,
        default_content_store_path, default_gitops_work_dir, default_path_base_dir,
        default_work_dir, env_validated_path,
    };
    use camino::Utf8PathBuf;
    use sinex_primitives::environment::environment;
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use xtask::sandbox::sinex_serial_test;

    use xtask::sandbox::EnvGuard;

    #[sinex_serial_test]
    async fn material_durability_thresholds_match_policy_defaults() -> xtask::sandbox::TestResult<()>
    {
        let mut env = EnvGuard::new();
        env.set("SINEX_INGESTD_MATERIAL_STAGED_SYNC_BYTES", "1048576");
        env.set("SINEX_INGESTD_MATERIAL_STAGED_SYNC_INTERVAL_MS", "1000");
        env.set("SINEX_INGESTD_MATERIAL_WAL_SYNC_BYTES", "262144");
        env.set("SINEX_INGESTD_MATERIAL_WAL_SYNC_ENTRIES", "128");
        env.set("SINEX_INGESTD_MATERIAL_WAL_SYNC_INTERVAL_MS", "1000");

        let config = IngestdConfig::default();

        assert_eq!(
            config.material_durability_thresholds()?,
            DurabilityThresholds::default_checked()?
        );
        Ok(())
    }

    #[sinex_serial_test]
    async fn pool_config_preserves_ingestd_pool_policy() -> xtask::sandbox::TestResult<()> {
        let config = IngestdConfig {
            database_pool_size: 7,
            pool_acquire_timeout_secs: 11,
            pool_idle_timeout_secs: 29,
            ..IngestdConfig::default()
        };

        let pool = config.pool_config();

        assert_eq!(pool.max_connections, 7);
        assert_eq!(pool.min_connections, 0);
        assert_eq!(pool.acquire_timeout_secs.as_secs(), 11);
        assert_eq!(pool.idle_timeout_secs.as_secs(), 29);
        assert_eq!(pool.statement_timeout_secs.as_secs(), 0);
        assert_eq!(
            pool.max_lifetime_secs.map(|value| value.as_secs()),
            Some(30 * 60)
        );
        assert!(!pool.validate_against_postgres_max);
        Ok(())
    }

    #[sinex_serial_test]
    async fn default_work_dir_ignores_invalid_override() -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_INGESTD_WORK_DIR", "../../etc");
        env.set("XDG_CACHE_HOME", "/tmp/sinex-ingestd-config-cache");

        let expected = Utf8PathBuf::from_path_buf(
            environment().work_directory(default_path_base_dir().join("sinex").join("ingestd")),
        )
        .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex/ingestd"));

        assert_eq!(default_work_dir(), expected);
        Ok(())
    }

    #[sinex_serial_test]
    async fn derived_default_paths_ignore_invalid_overrides() -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_INGESTD_WORK_DIR", "/tmp/sinex-ingestd-config-root");
        env.set("SINEX_CONTENT_STORE_PATH", "../../bad-content-store");
        env.set("SINEX_ASSEMBLER_STATE_DIR", "../../bad-state-dir");
        env.set("SINEX_INGESTD_GITOPS_WORK_DIR", "../../bad-gitops");

        assert_eq!(
            default_content_store_path(),
            Utf8PathBuf::from("/tmp/sinex-ingestd-config-root/content-store")
        );
        assert_eq!(
            default_assembler_state_dir(),
            Utf8PathBuf::from("/tmp/sinex-ingestd-config-root/assembler_state")
        );
        assert_eq!(
            default_gitops_work_dir(),
            Utf8PathBuf::from("/tmp/sinex-ingestd-config-root/gitops")
        );
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn env_validated_path_rejects_non_utf8_override() -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(
            "SINEX_CONFIG_PATH_OVERRIDE",
            OsString::from_vec(vec![0x2f, 0x74, 0x6d, 0x70, 0x80]),
        );

        assert_eq!(
            env_validated_path("SINEX_CONFIG_PATH_OVERRIDE", "test"),
            None
        );
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn from_args_rejects_non_utf8_database_url_override() -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("DATABASE_URL", OsString::from_vec(vec![0x70, 0x80]));

        let error = IngestdConfig::from_args(
            None,
            "nats://localhost:4222".to_string(),
            false,
            16,
            None,
            None,
            None,
            None,
            false,
            None,
            None,
            None,
        )
        .expect_err("non-UTF8 DATABASE_URL should fail ingestd config construction");

        let message = error.to_string();
        assert!(message.contains("DATABASE_URL"));
        assert!(message.contains("not valid UTF-8"));
        Ok(())
    }
}
