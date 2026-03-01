//! Gateway configuration via Figment (hierarchical: defaults → file → env → CLI).

use camino::Utf8PathBuf;
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use sinex_db::PoolConfig;
use sinex_primitives::domain::SanitizedPath;
use sinex_primitives::error::SinexError;

/// Gateway configuration.
///
/// Loaded hierarchically: struct defaults → `gateway.toml` → env vars → CLI args.
/// Environment variables use the `SINEX_GATEWAY_` prefix with `_` splitting
/// (e.g., `SINEX_GATEWAY_POOL_MAX_CONNECTIONS=20`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Database URL for PostgreSQL connection.
    #[serde(default = "default_database_url")]
    pub database_url: String,

    /// TCP listen address for the RPC server.
    #[serde(default = "default_tcp_listen")]
    pub tcp_listen: String,

    /// Comma-separated CORS origins. Empty means localhost only.
    #[serde(default)]
    pub cors_origins: String,

    /// Pool configuration.
    #[serde(default)]
    pub pool: PoolConfigFields,

    /// git-annex storage path.
    #[serde(default = "default_annex_path")]
    pub annex_path: String,

    /// Whether replay control is optional (degraded mode without NATS).
    #[serde(default)]
    pub replay_control_optional: bool,
}

/// Pool-specific config fields (flattened from env as `SINEX_GATEWAY_POOL_*`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfigFields {
    #[serde(default = "default_pool_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_pool_min_connections")]
    pub min_connections: u32,
    #[serde(default = "default_pool_acquire_timeout_secs")]
    pub acquire_timeout_secs: u64,
}

impl Default for PoolConfigFields {
    fn default() -> Self {
        Self {
            max_connections: default_pool_max_connections(),
            min_connections: default_pool_min_connections(),
            acquire_timeout_secs: default_pool_acquire_timeout_secs(),
        }
    }
}

impl PoolConfigFields {
    /// Convert to sinex-db PoolConfig.
    pub fn to_pool_config(&self) -> PoolConfig {
        let mut config = PoolConfig::default();
        config.max_connections = self.max_connections;
        config.min_connections = self.min_connections;
        config.acquire_timeout_secs =
            sinex_primitives::units::Seconds::from_secs(self.acquire_timeout_secs);
        config
    }
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

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            database_url: default_database_url(),
            tcp_listen: default_tcp_listen(),
            cors_origins: String::new(),
            pool: PoolConfigFields::default(),
            annex_path: default_annex_path(),
            replay_control_optional: false,
        }
    }
}

impl GatewayConfig {
    /// Load configuration from defaults → config file → environment variables.
    pub fn load() -> Result<Self, figment::Error> {
        let figment = Figment::from(Serialized::defaults(Self::default()))
            .merge(Toml::file("gateway.toml"))
            .merge(Toml::file("/etc/sinex/gateway.toml"))
            .merge(Env::prefixed("SINEX_GATEWAY_").split('_'))
            .merge(Env::raw().only(&["DATABASE_URL"]));

        figment.extract()
    }

    /// Apply CLI overrides on top of loaded config.
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

    /// Resolve and validate the annex path.
    pub fn resolve_annex_path(&self) -> Result<Utf8PathBuf, SinexError> {
        let sanitized = SanitizedPath::from_str_validated(&self.annex_path)
            .map_err(|e| SinexError::validation(format!("Invalid annex_path: {e}")))?;
        Ok(Utf8PathBuf::from(sanitized.as_str()))
    }

    /// Parse CORS origins from the comma-separated string.
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
}
