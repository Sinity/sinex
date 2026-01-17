#![doc = include_str!("../docs/config.md")]

//! Configuration helpers for the ingestion daemon.

use crate::{IngestdResult, SinexError};
use camino::Utf8PathBuf;
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use sinex_core::{
    environment::environment,
    types::{deserialize_validated_utf8_path, validate_path, Bytes, Milliseconds, Seconds},
};
use tracing::{debug, error, info, warn};
use validator::{Validate, ValidationError};

/// Configuration for the ingestion daemon
#[derive(Debug, Clone, Serialize, Deserialize, Validate, bon::Builder)]
#[builder(on(String, into))]
pub struct IngestdConfig {
    /// Database URL for PostgreSQL connection
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

    /// NATS connection configuration
    #[validate(custom(function = "validate_nats_config"))]
    #[builder(default)]
    pub nats: sinex_core::nats::NatsConnectionConfig,

    /// Batch size for database writes
    #[validate(range(min = 1, message = "Batch size must be greater than 0"))]
    #[builder(default = 1000)]
    pub batch_size: usize,

    /// Batch timeout in seconds
    #[serde(default = "default_batch_timeout_secs")]
    #[builder(default = default_batch_timeout_secs())]
    #[validate(custom(function = "validate_batch_timeout_secs"))]
    pub batch_timeout_secs: Seconds,
    /// Maximum messages to fetch per JetStream pull batch
    #[builder(default = default_consumer_fetch_max_messages())]
    #[validate(range(
        min = 1,
        max = 10_000,
        message = "Fetch batch size must be between 1 and 10000"
    ))]
    pub consumer_fetch_max_messages: usize,
    /// JetStream pull expiration timeout in milliseconds
    #[serde(default = "default_consumer_fetch_timeout_ms")]
    #[builder(default = default_consumer_fetch_timeout_ms())]
    #[validate(custom(function = "validate_fetch_timeout"))]
    pub consumer_fetch_timeout_ms: Milliseconds,
    /// Maximum unacknowledged messages for the main JetStream consumer
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

    /// Optional namespace appended to all JetStream subjects/streams (used by tests).
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
        batch_size: usize,
        batch_timeout_secs: Seconds,
        dry_run: bool,
        annex_repo_path: Option<String>,
        assembler_state_dir: Option<String>,
    ) -> Self {
        let skip_schema_sync = env_flag("SINEX_SKIP_SCHEMA_SYNC").unwrap_or(false);
        let validate_schemas = env_flag("SINEX_VALIDATE_SCHEMAS").unwrap_or(true);

        // Construct NatsConnectionConfig from args
        // Note: CLI args for certs are not yet exposed in this helper, users should use env vars or config file for full TLS.
        // We only map the basic URL and require_tls flag here as they are common CLI args.
        let mut nats_config = sinex_core::nats::NatsConnectionConfig::from_env();
        nats_config.url = nats_url.clone();
        nats_config.require_tls = nats_require_tls;
        let nats_config_clone = nats_config.clone();

        let db_url = database_url.unwrap_or_else(default_database_url);
        let mut config = Self::default();
        config.database_url = db_url;
        config.database_pool_size = pool_size;
        config.batch_size = batch_size;
        config.batch_timeout_secs = batch_timeout_secs;
        config.dry_run = dry_run;
        config.skip_schema_sync = skip_schema_sync;
        config.validate_schemas = validate_schemas;
        config.nats = nats_config_clone;

        if let Some(path) = annex_repo_path {
            config.annex_repo_path = Utf8PathBuf::from(path);
        }

        if let Some(path) = assembler_state_dir {
            config.assembler_state_dir = Utf8PathBuf::from(path);
        }

        config.normalize()
    }

    fn normalize(mut self) -> Self {
        let default_batch_size = Self::default().batch_size;
        if self.consumer_fetch_max_messages == default_consumer_fetch_max_messages()
            && self.batch_size != default_batch_size
        {
            self.consumer_fetch_max_messages = self.batch_size;
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
    pub fn get_db_options(&self) -> sqlx::postgres::PgPoolOptions {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(self.database_pool_size)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .idle_timeout(std::time::Duration::from_secs(600))
            .max_lifetime(std::time::Duration::from_secs(1800))
            .before_acquire(|conn, _meta| {
                Box::pin(async move {
                    // Clean up any lingering transaction state before returning to caller.
                    // This handles cases where connections are returned to the pool with
                    // aborted transactions (e.g., after a failed query or lost connection).
                    // Note: Must execute as separate queries because prepared statements
                    // can't contain multiple commands.
                    if let Err(e) = sqlx::query("ROLLBACK").execute(&mut *conn).await {
                        // If ROLLBACK fails, the connection is truly broken.
                        warn!("Connection ROLLBACK failed, discarding: {e}");
                        return Ok(false);
                    }
                    // Reset session state to defaults (timeout, search_path, etc.)
                    if let Err(e) = sqlx::query("RESET ALL").execute(&mut *conn).await {
                        warn!("Connection RESET ALL failed, discarding: {e}");
                        return Ok(false);
                    }
                    Ok(true)
                })
            })
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
            nats: sinex_core::nats::NatsConnectionConfig::from_env(),
            batch_size: default_batch_size(),
            batch_timeout_secs: default_batch_timeout_secs(),
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
            annex_repo_path: default_annex_repo_path(),
            assembler_state_dir: default_assembler_state_dir(),
        }
    }
}

// Helper functions

/// Default database URL with environment namespacing
fn default_database_url() -> String {
    match std::env::var("DATABASE_URL") {
        Ok(url) => environment().database_url(&url).unwrap_or(url),
        Err(_) => {
            let env = environment();
            let base_name = env.database_name("sinex");
            format!("postgresql:///{base_name}?host=/run/postgresql")
        }
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

fn default_batch_timeout_secs() -> Seconds {
    Seconds::from_secs(5)
}

fn default_batch_size() -> usize {
    1000
}

fn default_consumer_fetch_max_messages() -> usize {
    100
}

fn default_consumer_fetch_timeout_ms() -> Milliseconds {
    Milliseconds::from_millis(1_000)
}

fn default_consumer_max_ack_pending() -> i64 {
    100
}

fn default_material_slices_max_ack_pending() -> i64 {
    1_000
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
    config: &sinex_core::nats::NatsConnectionConfig,
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
    use sinex_core::types::validate_path;

    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_work_dir"))
}

fn default_annex_repo_path() -> Utf8PathBuf {
    use sinex_core::types::validate_path;

    if let Ok(path) = std::env::var("SINEX_ANNEX_PATH") {
        if let Ok(validated) = validate_path(&path) {
            return validated;
        }
    }

    let annex = default_work_dir().join("annex");
    validate_path(annex.as_str()).unwrap_or(annex)
}

fn default_assembler_state_dir() -> Utf8PathBuf {
    use sinex_core::types::validate_path;

    let state_dir = default_work_dir().join("assembler_state");
    validate_path(state_dir.as_str()).unwrap_or(state_dir)
}

fn validate_annex_path(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    use sinex_core::types::validate_path;
    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_annex_path"))
}

fn validate_state_dir(path: &Utf8PathBuf) -> Result<(), validator::ValidationError> {
    use sinex_core::types::validate_path;
    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_state_dir"))
}

fn validate_batch_timeout_secs(value: &Seconds) -> Result<(), ValidationError> {
    if value.as_secs() == 0 {
        return Err(ValidationError::new("min"));
    }
    Ok(())
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
