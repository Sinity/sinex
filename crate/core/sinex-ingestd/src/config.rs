#![doc = include_str!("../docs/config.md")]

//! Configuration helpers for the ingestion daemon.

use crate::{IngestdResult, SinexError};
use camino::Utf8PathBuf;
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use sinex_primitives::{
    environment::environment,
    units::{Bytes, Milliseconds},
    validation::{deserialize_validated_utf8_path, validate_path},
};
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

    /// Maximum concurrent material assemblies (semaphore limit)
    ///
    /// Controls how many materials can be assembled simultaneously.
    /// Higher values increase throughput but consume more memory.
    /// Set via: `SINEX_INGESTD_MAX_CONCURRENT_ASSEMBLIES=50`
    #[builder(default = default_max_concurrent_assemblies())]
    #[validate(range(
        min = 1,
        max = 500,
        message = "max_concurrent_assemblies must be between 1 and 500"
    ))]
    pub max_concurrent_assemblies: usize,

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

    /// git-annex repository path for assembled materials
    #[serde(deserialize_with = "deserialize_validated_utf8_path")]
    #[validate(custom(
        function = "validate_annex_path",
        message = "Invalid annex repository path"
    ))]
    #[builder(default = default_annex_repo_path())]
    pub annex_repo_path: Utf8PathBuf,

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

    /// Enable GitOps schema sync service
    ///
    /// When enabled, ingestd periodically fetches configured Git repositories
    /// and discovers JSON schema files to register in the database.
    ///
    /// Set via: `SINEX_INGESTD_GITOPS_ENABLED=true`
    #[serde(default)]
    #[builder(default = false)]
    pub gitops_enabled: bool,

    /// Working directory for GitOps repository clones
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
}

impl IngestdConfig {
    /// Build a Figment instance with defaults, config files, and environment overrides.
    fn build_figment_base() -> Figment {
        let figment = Figment::from(Serialized::defaults(Self::default()));
        let figment = Self::merge_config_file(figment, "ingestd.toml");
        Self::merge_config_file(figment, "/etc/sinex/ingestd.toml")
    }

    /// Add shared environment variable layers for ingestd configuration.
    fn add_env(figment: Figment) -> Figment {
        figment
            .merge(Env::prefixed("SINEX_INGESTD_").split('_'))
            .merge(Env::raw().only(&["DATABASE_URL", "SINEX_NATS_REQUIRE_TLS"]))
    }

    /// Load configuration from defaults, files, and environment overrides.
    pub fn load() -> Result<Self, figment::Error> {
        Self::add_env(Self::build_figment_base())
            .extract()
            .map(Self::normalize)
    }

    /// Load configuration including a specific config file.
    pub fn load_from_path(path: impl AsRef<str>) -> Result<Self, figment::Error> {
        let figment = Self::merge_config_file(Self::build_figment_base(), path.as_ref());
        Self::add_env(figment).extract().map(Self::normalize)
    }

    /// Load configuration from an existing Figment instance.
    pub fn from_figment(figment: Figment) -> Result<Self, figment::Error> {
        Self::add_env(figment).extract().map(Self::normalize)
    }

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
        annex_repo_path: Option<String>,
        assembler_state_dir: Option<String>,
        namespace: Option<String>,
    ) -> Self {
        let skip_schema_sync = env_flag("SINEX_SKIP_SCHEMA_SYNC").unwrap_or(false);
        let validate_schemas = env_flag("SINEX_VALIDATE_SCHEMAS").unwrap_or(true);
        let pool_acquire_timeout_secs: u64 =
            std::env::var("SINEX_INGESTD_POOL_ACQUIRE_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_pool_acquire_timeout_secs);
        let pool_idle_timeout_secs: u64 = std::env::var("SINEX_INGESTD_POOL_IDLE_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(default_pool_idle_timeout_secs);

        // Construct NatsConnectionConfig from args
        // Note: CLI args for certs are not yet exposed in this helper, users should use env vars or config file for full TLS.
        // We only map the basic URL and require_tls flag here as they are common CLI args.
        let mut nats_config = sinex_primitives::nats::NatsConnectionConfig::from_env();
        nats_config.url = nats_url;
        nats_config.require_tls = nats_require_tls;
        let nats_config_clone = nats_config;

        let db_url = database_url.unwrap_or_else(default_database_url);
        let mut config = Self::default();
        config.database_url = db_url;
        config.database_pool_size = pool_size;
        config.pool_acquire_timeout_secs = pool_acquire_timeout_secs;
        config.pool_idle_timeout_secs = pool_idle_timeout_secs;
        config.dry_run = dry_run;
        config.skip_schema_sync = skip_schema_sync;
        config.validate_schemas = validate_schemas;
        config.nats = nats_config_clone;
        config.nats_namespace = namespace;

        // Override work_dir if set via environment (prevents fallback to ephemeral /tmp
        // when ProtectHome=true blocks ~/.cache in the systemd unit).
        if let Ok(dir) = std::env::var("SINEX_INGESTD_WORK_DIR") {
            config.work_dir = Utf8PathBuf::from(dir);
            // Only re-derive gitops_work_dir from the new work_dir if it hasn't been
            // explicitly set via SINEX_INGESTD_GITOPS_WORK_DIR (checked below).
            config.gitops_work_dir = config.work_dir.join("gitops");
        }
        if let Ok(dir) = std::env::var("SINEX_INGESTD_GITOPS_WORK_DIR") {
            config.gitops_work_dir = Utf8PathBuf::from(dir);
        }

        // Override fetch config if specified
        if let Some(max_msgs) = consumer_fetch_max_messages {
            config.consumer_fetch_max_messages = max_msgs;
        }
        if let Some(timeout_ms) = consumer_fetch_timeout_ms {
            config.consumer_fetch_timeout_ms = Milliseconds::from_millis(timeout_ms);
        }
        if let Some(pending) = consumer_max_ack_pending {
            config.consumer_max_ack_pending = pending;
        }
        if let Some(pending) = material_slices_max_ack_pending {
            config.material_slices_max_ack_pending = pending;
        }

        if let Some(path) = annex_repo_path {
            config.annex_repo_path = Utf8PathBuf::from(path);
        }

        if let Some(path) = assembler_state_dir {
            config.assembler_state_dir = Utf8PathBuf::from(path);
        }

        config.normalize()
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

    /// Validate configuration and exit with appropriate status code
    pub async fn validate_and_exit(&self) -> ! {
        info!("Validating configuration...");
        match self.validate().await {
            Ok(()) => {
                info!("✅ Configuration is valid");
                std::process::exit(0);
            }
            Err(e) => {
                error!("❌ Configuration validation failed: {}", e);
                std::process::exit(1);
            }
        }
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

        if tokio::fs::metadata(&self.annex_repo_path).await.is_err() {
            warn!(
                path = %self.annex_repo_path,
                "Annex repository path does not exist; git-annex will attempt initialization"
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
        use sqlx::postgres::PgPoolOptions;

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.database_url)
            .await?;

        // Test basic query with a simple SELECT 1
        sqlx::query!("SELECT 1 as one")
            .fetch_one(&pool)
            .await
            .map_err(|e| {
                SinexError::configuration(format!("Database connection test failed: {e}"))
                    .with_operation("config.test_database_connection")
                    .with_context("database_url", self.database_url.clone())
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
    pub fn get_db_options(&self) -> sqlx::postgres::PgPoolOptions {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(self.database_pool_size)
            .acquire_timeout(std::time::Duration::from_secs(
                self.pool_acquire_timeout_secs,
            ))
            .idle_timeout(std::time::Duration::from_secs(self.pool_idle_timeout_secs))
            .max_lifetime(std::time::Duration::from_mins(30))
        // Intentionally rely on sqlx's built-in connection health checks.
        // A custom before_acquire rollback/reset hook can deadlock after
        // specific protocol exchanges (for example COPY IN).
        // See: https://github.com/launchbadge/sqlx/issues/3117
    }
}

impl IngestdConfig {
    fn merge_config_file(figment: Figment, path: &str) -> Figment {
        figment
            .merge(Toml::file(path).nested())
            .merge(Figment::from(Toml::file(path)).select("ingestd"))
    }
}

fn env_flag(name: &str) -> Option<bool> {
    match std::env::var(name) {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            Some(matches!(normalized.as_str(), "1" | "true" | "yes" | "on"))
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                env = name,
                "Environment variable is not valid UTF-8; ignoring"
            );
            None
        }
        Err(std::env::VarError::NotPresent) => None,
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
            max_concurrent_assemblies: default_max_concurrent_assemblies(),
            dry_run: false,
            validate_schemas: true,
            skip_schema_sync: false,
            work_dir: default_work_dir(),
            max_message_size: default_max_message_size(),
            nats_stream_name: default_nats_stream_name(),
            nats_consumer_name: format!("ingestd-{}", env.name()),
            nats_namespace: None,
            annex_repo_path: default_annex_repo_path(),
            assembler_state_dir: default_assembler_state_dir(),
            strict_validation: false,
            max_buffered_slices: default_max_buffered_slices(),
            slice_timeout_secs: default_slice_timeout_secs(),
            orphan_threshold_secs: default_orphan_threshold_secs(),
            disk_threshold_percent: default_disk_threshold_percent(),
            gitops_enabled: false,
            gitops_work_dir: default_gitops_work_dir(),
            schema_reload_interval_secs: default_schema_reload_interval_secs(),
            stats_log_interval_secs: default_stats_log_interval_secs(),
        }
    }
}

// Helper functions

/// Default database URL with environment namespacing
fn default_database_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        environment().database_url(&url).unwrap_or(url)
    } else {
        let env = environment();
        let base_name = env.database_name("sinex");
        format!("postgresql:///{base_name}?host=/run/postgresql")
    }
}

/// Default work directory for ingestd with environment namespacing
fn default_work_dir() -> Utf8PathBuf {
    let env = environment();
    let base_dir = dirs::cache_dir()
        .and_then(|p| Utf8PathBuf::from_path_buf(p).ok())
        .unwrap_or_else(|| Utf8PathBuf::from("/tmp"));

    let work_dir = env.work_directory(base_dir.join("sinex").join("ingestd"));

    // Validate the default path
    match validate_path(work_dir.to_str().unwrap_or("/tmp/sinex/ingestd")) {
        Ok(validated) => validated,
        Err(_) => {
            // Fallback to a safe default if validation fails
            Utf8PathBuf::from_path_buf(env.work_directory("/tmp/sinex/ingestd"))
                .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex/ingestd"))
        }
    }
}

fn default_pool_acquire_timeout_secs() -> u64 {
    30
}

fn default_pool_idle_timeout_secs() -> u64 {
    600
}

fn default_consumer_fetch_max_messages() -> usize {
    100
}

fn default_consumer_fetch_timeout_ms() -> Milliseconds {
    Milliseconds::from_millis(100)
}

fn default_consumer_max_ack_pending() -> i64 {
    100
}

fn default_material_slices_max_ack_pending() -> i64 {
    1_000
}

fn default_max_concurrent_assemblies() -> usize {
    50
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

fn default_annex_repo_path() -> Utf8PathBuf {
    use sinex_primitives::validation::validate_path;

    if let Ok(path) = std::env::var("SINEX_ANNEX_PATH") {
        if let Ok(validated) = validate_path(&path) {
            return validated;
        }
    }

    let annex = default_work_dir().join("annex");
    validate_path(annex.as_str()).unwrap_or(annex)
}

fn default_assembler_state_dir() -> Utf8PathBuf {
    use sinex_primitives::validation::validate_path;

    let state_dir = default_work_dir().join("assembler_state");
    validate_path(state_dir.as_str()).unwrap_or(state_dir)
}

fn validate_annex_path(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    use sinex_primitives::validation::validate_path;
    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_annex_path"))
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
    100
}

fn default_slice_timeout_secs() -> u64 {
    300 // 5 minutes
}

fn default_orphan_threshold_secs() -> u64 {
    3600 // 1 hour
}

fn default_disk_threshold_percent() -> u8 {
    90
}

fn default_gitops_work_dir() -> Utf8PathBuf {
    default_work_dir().join("gitops")
}

fn default_schema_reload_interval_secs() -> u64 {
    300 // 5 minutes
}

fn default_stats_log_interval_secs() -> u64 {
    60 // 1 minute
}
