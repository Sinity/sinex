//! Gateway configuration via a typed env-first loader.

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sinex_db::PoolConfig;
use sinex_primitives::domain::SanitizedPath;
use sinex_primitives::error::SinexError;
use sinex_primitives::nats::NatsConnectionConfig;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::Duration;

/// Gateway configuration.
///
/// Loaded as: struct defaults → environment variables → CLI args.
/// Environment variables use the `SINEX_GATEWAY_` prefix for gateway-owned fields
/// (for example, `SINEX_GATEWAY_POOL_MAX_CONNECTIONS=20`) plus a small number of
/// shared `SINEX_*` variables for cross-cutting transport/auth settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Database URL for `PostgreSQL` connection.
    #[serde(default = "default_database_url")]
    pub database_url: String,

    /// TCP listen address for the RPC server.
    #[serde(default = "default_tcp_listen")]
    pub tcp_listen: String,

    /// Comma-separated CORS origins. Empty means localhost only.
    #[serde(default)]
    pub cors_origins: String,

    /// Maximum database connections per service pool.
    #[serde(default = "default_pool_max_connections")]
    pub pool_max_connections: u32,

    /// Minimum database connections per service pool.
    #[serde(default = "default_pool_min_connections")]
    pub pool_min_connections: u32,

    /// Connection acquisition timeout in seconds.
    #[serde(default = "default_pool_acquire_timeout_secs")]
    pub pool_acquire_timeout_secs: u64,

    /// git-annex storage path.
    #[serde(default = "default_annex_path")]
    pub annex_path: String,

    /// Bearer token used for RPC authentication.
    #[serde(default)]
    pub rpc_token: Option<String>,

    /// File containing the bearer token used for RPC authentication.
    #[serde(default)]
    pub rpc_token_file: Option<String>,

    /// Higher-priority file containing the gateway admin token.
    #[serde(default)]
    pub admin_token_file: Option<String>,

    /// Shared NATS connection configuration used by replay control and coordination.
    #[serde(default)]
    pub nats: NatsConnectionConfig,

    /// TLS certificate path for the RPC server.
    #[serde(default)]
    pub tls_cert: Option<String>,

    /// TLS private key path for the RPC server.
    #[serde(default)]
    pub tls_key: Option<String>,

    /// Client CA bundle path for mTLS.
    #[serde(default)]
    pub tls_client_ca: Option<String>,

    /// Require mTLS even on loopback bindings.
    #[serde(default)]
    pub require_client_tls: bool,

    /// Maximum concurrent RPC requests.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,

    /// RPC request timeout in seconds.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Maximum JSON-RPC request body size in bytes.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: u64,

    /// Maximum decoded blob payload size in bytes.
    #[serde(default = "default_max_blob_bytes")]
    pub max_blob_bytes: usize,

    /// Replay-control request timeout in seconds.
    #[serde(default = "default_replay_control_timeout_secs")]
    pub replay_control_timeout_secs: u64,

    /// NATS consumer creation timeout in seconds for RPC handlers.
    #[serde(default = "default_nats_consumer_create_timeout_secs")]
    pub nats_consumer_create_timeout_secs: u64,

    /// Trusted extension allow-list for native messaging.
    #[serde(default)]
    pub native_messaging_trusted_extensions: Option<String>,

    /// Trusted native-messaging hosts.
    #[serde(default)]
    pub native_messaging_trusted_hosts: Option<String>,

    /// Enforced native-messaging protocol version.
    #[serde(default)]
    pub native_messaging_protocol_version: Option<String>,

    /// Capability map for native messaging extensions as JSON.
    #[serde(default)]
    pub native_messaging_capabilities: Option<String>,

    /// Per-extension role map for native messaging as JSON.
    #[serde(default)]
    pub native_messaging_extension_roles: Option<String>,

    /// Native-messaging read timeout in seconds.
    #[serde(default = "default_native_messaging_read_timeout_secs")]
    pub native_messaging_read_timeout_secs: u64,

    /// Maximum native-messaging payload size in bytes.
    #[serde(default = "default_native_messaging_max_size_bytes")]
    pub native_messaging_max_size_bytes: usize,

    /// Whether RPC rate limiting is enabled.
    #[serde(default = "default_rate_limit_enabled")]
    pub rpc_rate_limit_enabled: bool,

    /// In-memory token bucket refill rate.
    #[serde(default = "default_rate_limit_requests_per_second")]
    pub rpc_rate_limit_requests_per_sec: u32,

    /// In-memory token bucket burst capacity.
    #[serde(default = "default_rate_limit_burst")]
    pub rpc_rate_limit_burst: u32,

    /// How long to retain idle in-memory token buckets.
    #[serde(default = "default_rate_limit_idle_timeout_secs")]
    pub rpc_rate_limit_idle_timeout_secs: u64,

    /// Distributed rate-limit window in seconds.
    #[serde(default = "default_rate_limit_window_secs")]
    pub rpc_rate_limit_window_secs: u64,

    /// Distributed rate-limit allowance per minute.
    #[serde(default = "default_rate_limit_per_minute")]
    pub rpc_rate_limit_per_minute: u32,
}

fn default_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_default()
}

fn default_tcp_listen() -> String {
    crate::rpc_server::DEFAULT_TCP_LISTEN.to_string()
}

fn default_annex_path() -> String {
    std::env::var("SINEX_ANNEX_PATH").unwrap_or_else(|_| {
        std::env::var("HOME").map_or_else(
            |_| {
                sinex_primitives::environment::environment()
                    .work_directory("annex")
                    .to_string_lossy()
                    .into_owned()
            },
            |home| format!("{home}/.local/share/sinex/annex"),
        )
    })
}

fn default_pool_max_connections() -> u32 {
    PoolConfig::default().max_connections
}

fn default_pool_min_connections() -> u32 {
    PoolConfig::default().min_connections
}

fn default_pool_acquire_timeout_secs() -> u64 {
    PoolConfig::default().acquire_timeout_secs.as_secs()
}

fn default_max_concurrency() -> usize {
    100
}

fn default_request_timeout_secs() -> u64 {
    30
}

fn default_max_body_bytes() -> u64 {
    2 * 1024 * 1024
}

fn default_max_blob_bytes() -> usize {
    5 * 1024 * 1024
}

fn default_replay_control_timeout_secs() -> u64 {
    30
}

fn default_nats_consumer_create_timeout_secs() -> u64 {
    10
}

fn default_rate_limit_enabled() -> bool {
    true
}

fn default_rate_limit_requests_per_second() -> u32 {
    100
}

fn default_rate_limit_burst() -> u32 {
    50
}

fn default_rate_limit_idle_timeout_secs() -> u64 {
    3600
}

fn default_rate_limit_window_secs() -> u64 {
    60
}

fn default_rate_limit_per_minute() -> u32 {
    6000
}

fn default_native_messaging_read_timeout_secs() -> u64 {
    30
}

fn default_native_messaging_max_size_bytes() -> usize {
    1024 * 1024
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            database_url: default_database_url(),
            tcp_listen: default_tcp_listen(),
            cors_origins: String::new(),
            pool_max_connections: default_pool_max_connections(),
            pool_min_connections: default_pool_min_connections(),
            pool_acquire_timeout_secs: default_pool_acquire_timeout_secs(),
            annex_path: default_annex_path(),
            rpc_token: None,
            rpc_token_file: None,
            admin_token_file: None,
            nats: NatsConnectionConfig::default(),
            tls_cert: None,
            tls_key: None,
            tls_client_ca: None,
            require_client_tls: false,
            max_concurrency: default_max_concurrency(),
            request_timeout_secs: default_request_timeout_secs(),
            max_body_bytes: default_max_body_bytes(),
            max_blob_bytes: default_max_blob_bytes(),
            replay_control_timeout_secs: default_replay_control_timeout_secs(),
            nats_consumer_create_timeout_secs: default_nats_consumer_create_timeout_secs(),
            native_messaging_trusted_extensions: None,
            native_messaging_trusted_hosts: None,
            native_messaging_protocol_version: None,
            native_messaging_capabilities: None,
            native_messaging_extension_roles: None,
            native_messaging_read_timeout_secs: default_native_messaging_read_timeout_secs(),
            native_messaging_max_size_bytes: default_native_messaging_max_size_bytes(),
            rpc_rate_limit_enabled: default_rate_limit_enabled(),
            rpc_rate_limit_requests_per_sec: default_rate_limit_requests_per_second(),
            rpc_rate_limit_burst: default_rate_limit_burst(),
            rpc_rate_limit_idle_timeout_secs: default_rate_limit_idle_timeout_secs(),
            rpc_rate_limit_window_secs: default_rate_limit_window_secs(),
            rpc_rate_limit_per_minute: default_rate_limit_per_minute(),
        }
    }
}

impl GatewayConfig {
    /// Load configuration from defaults and environment variables.
    #[must_use]
    pub fn load() -> Self {
        let mut config = Self {
            nats: NatsConnectionConfig::from_env(),
            ..Self::default()
        };
        config.apply_gateway_env_overrides();
        config.apply_manual_env_overrides();
        config
    }

    /// Apply CLI overrides on top of loaded config.
    #[must_use]
    pub fn with_cli_overrides(
        mut self,
        database_url: Option<String>,
        tcp_listen: Option<String>,
        cors_origins: Option<String>,
    ) -> Self {
        if let Some(url) = database_url {
            self.database_url = url;
        }
        if let Some(listen) = tcp_listen {
            self.tcp_listen = listen;
        }
        if let Some(origins) = cors_origins {
            self.cors_origins = origins;
        }
        self
    }

    /// Build a sinex-db `PoolConfig` from the flattened pool fields.
    #[must_use]
    pub fn pool_config(&self) -> PoolConfig {
        let mut config = PoolConfig::default();
        config.max_connections = self.pool_max_connections;
        config.min_connections = self.pool_min_connections;
        config.acquire_timeout_secs =
            sinex_primitives::units::Seconds::from_secs(self.pool_acquire_timeout_secs);
        config
    }

    /// Resolve and validate the annex path.
    pub fn resolve_annex_path(&self) -> Result<Utf8PathBuf, SinexError> {
        let sanitized = SanitizedPath::from_str_validated(&self.annex_path)
            .map_err(|e| SinexError::validation(format!("Invalid annex_path: {e}")))?;
        Ok(Utf8PathBuf::from(sanitized.as_str()))
    }

    /// Parse CORS origins from the comma-separated string.
    #[must_use]
    pub fn cors_origins_list(&self) -> Vec<String> {
        if self.cors_origins.is_empty() {
            Vec::new()
        } else {
            self.cors_origins
                .split(',')
                .map(|o| o.trim().to_string())
                .collect()
        }
    }

    #[must_use]
    pub fn rpc_token_path(&self) -> Option<PathBuf> {
        self.rpc_token_file
            .as_ref()
            .map(PathBuf::from)
            .or_else(|| self.admin_token_file.as_ref().map(PathBuf::from))
    }

    pub fn auth_token_from_config(&self) -> Result<(Option<String>, Option<PathBuf>), SinexError> {
        if let Some(path_str) = &self.admin_token_file {
            let path = PathBuf::from(path_str);
            let contents = std::fs::read_to_string(&path).map_err(|e| {
                SinexError::configuration("Failed to read admin token file")
                    .with_path(path.display().to_string())
                    .with_source(e.to_string())
            })?;
            return Ok((Some(contents.trim().to_string()), Some(path)));
        }

        if let Some(path_str) = &self.rpc_token_file {
            let path = PathBuf::from(path_str);
            let contents = std::fs::read_to_string(&path).map_err(|e| {
                SinexError::configuration("Failed to read RPC token file")
                    .with_path(path.display().to_string())
                    .with_source(e.to_string())
            })?;
            return Ok((Some(contents.trim().to_string()), Some(path)));
        }

        Ok((
            self.rpc_token
                .as_ref()
                .map(|token| token.trim().to_string()),
            None,
        ))
    }

    #[must_use]
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }

    #[must_use]
    pub fn replay_control_timeout(&self) -> Duration {
        Duration::from_secs(self.replay_control_timeout_secs)
    }

    #[must_use]
    pub fn nats_consumer_create_timeout(&self) -> Duration {
        Duration::from_secs(self.nats_consumer_create_timeout_secs)
    }

    // SAFETY: these are known non-zero compile-time constants.
    const DEFAULT_RATE_RPS: NonZeroU32 = NonZeroU32::new(100).unwrap();
    const DEFAULT_RATE_BURST: NonZeroU32 = NonZeroU32::new(50).unwrap();
    const DEFAULT_RATE_PER_MIN: NonZeroU32 = NonZeroU32::new(6000).unwrap();

    #[must_use]
    pub fn rate_limit_requests_per_second(&self) -> NonZeroU32 {
        NonZeroU32::new(self.rpc_rate_limit_requests_per_sec).unwrap_or(Self::DEFAULT_RATE_RPS)
    }

    #[must_use]
    pub fn rate_limit_burst(&self) -> NonZeroU32 {
        NonZeroU32::new(self.rpc_rate_limit_burst).unwrap_or(Self::DEFAULT_RATE_BURST)
    }

    #[must_use]
    pub fn rate_limit_per_minute(&self) -> NonZeroU32 {
        NonZeroU32::new(self.rpc_rate_limit_per_minute).unwrap_or(Self::DEFAULT_RATE_PER_MIN)
    }

    #[must_use]
    pub fn nats_connection_config(&self) -> NatsConnectionConfig {
        self.nats.clone()
    }

    fn apply_gateway_env_overrides(&mut self) {
        self.tcp_listen = env_string_override("SINEX_GATEWAY_TCP_LISTEN", self.tcp_listen.clone());
        self.cors_origins =
            env_string_override("SINEX_GATEWAY_CORS_ORIGINS", self.cors_origins.clone());
        self.pool_max_connections = env_u32_override(
            "SINEX_GATEWAY_POOL_MAX_CONNECTIONS",
            self.pool_max_connections,
        );
        self.pool_min_connections = env_u32_override(
            "SINEX_GATEWAY_POOL_MIN_CONNECTIONS",
            self.pool_min_connections,
        );
        self.pool_acquire_timeout_secs = env_u64_override(
            "SINEX_GATEWAY_POOL_ACQUIRE_TIMEOUT_SECS",
            self.pool_acquire_timeout_secs,
        );
        self.annex_path = env_string_override("SINEX_GATEWAY_ANNEX_PATH", self.annex_path.clone());
        self.tls_cert = env_option_override("SINEX_GATEWAY_TLS_CERT", self.tls_cert.take());
        self.tls_key = env_option_override("SINEX_GATEWAY_TLS_KEY", self.tls_key.take());
        self.tls_client_ca =
            env_option_override("SINEX_GATEWAY_TLS_CLIENT_CA", self.tls_client_ca.take());
        self.require_client_tls =
            env_bool_override("SINEX_GATEWAY_REQUIRE_CLIENT_TLS", self.require_client_tls);
        self.max_concurrency =
            env_usize_override("SINEX_GATEWAY_MAX_CONCURRENCY", self.max_concurrency);
        self.request_timeout_secs = env_u64_override(
            "SINEX_GATEWAY_REQUEST_TIMEOUT_SECS",
            self.request_timeout_secs,
        );
        self.max_body_bytes = env_u64_override("SINEX_GATEWAY_MAX_BODY_BYTES", self.max_body_bytes);
        self.max_blob_bytes =
            env_usize_override("SINEX_GATEWAY_MAX_BLOB_BYTES", self.max_blob_bytes);
    }

    fn apply_manual_env_overrides(&mut self) {
        self.rpc_token = std::env::var("SINEX_RPC_TOKEN")
            .ok()
            .map(|v| v.trim().to_string())
            .or(self.rpc_token.take());
        self.rpc_token_file = std::env::var("SINEX_RPC_TOKEN_FILE")
            .ok()
            .or(self.rpc_token_file.take());
        self.admin_token_file = std::env::var("SINEX_GATEWAY_ADMIN_TOKEN_FILE")
            .ok()
            .or(self.admin_token_file.take());
        self.nats.url = std::env::var("SINEX_NATS_URL").unwrap_or_else(|_| self.nats.url.clone());
        self.nats.name = std::env::var("SINEX_NATS_NAME")
            .ok()
            .or(self.nats.name.take());
        self.nats.require_tls = env_bool_override("SINEX_NATS_REQUIRE_TLS", self.nats.require_tls);
        self.nats.ca_cert = std::env::var("SINEX_NATS_CA_CERT")
            .ok()
            .map(PathBuf::from)
            .or(self.nats.ca_cert.take());
        self.nats.client_cert = std::env::var("SINEX_NATS_CLIENT_CERT")
            .ok()
            .map(PathBuf::from)
            .or(self.nats.client_cert.take());
        self.nats.client_key = std::env::var("SINEX_NATS_CLIENT_KEY")
            .ok()
            .map(PathBuf::from)
            .or(self.nats.client_key.take());
        self.nats.creds_file = std::env::var("SINEX_NATS_CREDS_FILE")
            .ok()
            .map(PathBuf::from)
            .or(self.nats.creds_file.take());
        self.nats.nkey_seed_file = std::env::var("SINEX_NATS_NKEY_SEED_FILE")
            .ok()
            .map(PathBuf::from)
            .or(self.nats.nkey_seed_file.take());
        self.nats.token = std::env::var("SINEX_NATS_TOKEN")
            .ok()
            .or(self.nats.token.take());
        self.nats.token_file = std::env::var("SINEX_NATS_TOKEN_FILE")
            .ok()
            .map(PathBuf::from)
            .or(self.nats.token_file.take());

        self.rpc_rate_limit_enabled =
            env_bool_override("SINEX_RPC_RATE_LIMIT_ENABLED", self.rpc_rate_limit_enabled);
        self.rpc_rate_limit_requests_per_sec = env_u32_override(
            "SINEX_RPC_RATE_LIMIT_REQUESTS_PER_SEC",
            self.rpc_rate_limit_requests_per_sec,
        );
        self.rpc_rate_limit_burst =
            env_u32_override("SINEX_RPC_RATE_LIMIT_BURST", self.rpc_rate_limit_burst);
        self.rpc_rate_limit_idle_timeout_secs = env_u64_override(
            "SINEX_RPC_RATE_LIMIT_IDLE_TIMEOUT_SECS",
            self.rpc_rate_limit_idle_timeout_secs,
        );
        self.rpc_rate_limit_window_secs = env_u64_override(
            "SINEX_RPC_RATE_LIMIT_WINDOW_SECS",
            self.rpc_rate_limit_window_secs,
        );
        self.rpc_rate_limit_per_minute = env_u32_override(
            "SINEX_RPC_RATE_LIMIT_PER_MINUTE",
            self.rpc_rate_limit_per_minute,
        );
        self.replay_control_timeout_secs = env_u64_override(
            "SINEX_REPLAY_CONTROL_TIMEOUT_SECS",
            self.replay_control_timeout_secs,
        );
        self.nats_consumer_create_timeout_secs = env_u64_override(
            "SINEX_NATS_CONSUMER_CREATE_TIMEOUT_SECS",
            self.nats_consumer_create_timeout_secs,
        );
        self.native_messaging_trusted_extensions =
            std::env::var("SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS")
                .ok()
                .or(self.native_messaging_trusted_extensions.take());
        self.native_messaging_trusted_hosts = std::env::var("SINEX_NATIVE_MESSAGING_TRUSTED_HOSTS")
            .ok()
            .or(self.native_messaging_trusted_hosts.take());
        self.native_messaging_protocol_version =
            std::env::var("SINEX_NATIVE_MESSAGING_PROTOCOL_VERSION")
                .ok()
                .or(self.native_messaging_protocol_version.take());
        self.native_messaging_capabilities = std::env::var("SINEX_NATIVE_MESSAGING_CAPABILITIES")
            .ok()
            .or(self.native_messaging_capabilities.take());
        self.native_messaging_extension_roles =
            std::env::var("SINEX_NATIVE_MESSAGING_EXTENSION_ROLES")
                .ok()
                .or(self.native_messaging_extension_roles.take());
        self.native_messaging_read_timeout_secs = env_u64_override(
            "SINEX_NATIVE_MESSAGING_READ_TIMEOUT_SECS",
            self.native_messaging_read_timeout_secs,
        );
        self.native_messaging_max_size_bytes = env_usize_override(
            "SINEX_NATIVE_MESSAGING_MAX_SIZE_BYTES",
            self.native_messaging_max_size_bytes,
        );
    }
}

fn env_string_override(name: &str, current: String) -> String {
    std::env::var(name).unwrap_or(current)
}

fn env_option_override(name: &str, current: Option<String>) -> Option<String> {
    std::env::var(name).ok().or(current)
}

fn env_u32_override(name: &str, current: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
        .unwrap_or(current)
}

fn env_u64_override(name: &str, current: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(current)
}

fn env_usize_override(name: &str, current: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(current)
}

fn env_bool_override(name: &str, current: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|raw| match raw.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Some(true),
            "0" | "false" | "no" => Some(false),
            _ => None,
        })
        .unwrap_or(current)
}
