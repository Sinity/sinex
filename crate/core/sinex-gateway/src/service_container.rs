//! Service container that holds all service instances

use crate::replay_control::{spawn_replay_control, ReplayControlClient, ReplayControlError};
use crate::replay_state_machine::ReplayStateMachine;
use camino::Utf8PathBuf;
use color_eyre::eyre::Result;
use sinex_core::{
    coordination::CoordinationKvClient,
    db::{create_pool_with_config, PoolConfig},
    environment as sinex_environment,
    types::{domain::SanitizedPath, error::SinexError},
};
use sinex_node_sdk::annex::BlobManager;
use sinex_services::{AnalyticsService, ContentService, PkmService, SearchService};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// Container holding all service instances
#[derive(Clone)]
pub struct ServiceContainer {
    pool_max_connections: usize,
    pub analytics: Arc<AnalyticsService>,
    pub content: Arc<ContentService>,
    pub pkm: Arc<PkmService>,
    pub search: Arc<SearchService>,
    pub replay_control: Option<ReplayControlClient>,
    pub coordination: Option<Arc<CoordinationKvClient>>,
    nats_client: Option<async_nats::Client>,
    env: sinex_core::environment::SinexEnvironment,
    replay_control_bypass: bool,
    replay_control_init_error: Option<ReplayControlError>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayControlStatus {
    pub enabled: bool,
    pub bypass_allowed: bool,
    pub bypass_active: bool,
    pub connected: bool,
    pub last_error: Option<ReplayControlError>,
}

const REPLAY_CONTROL_CONNECT_ATTEMPTS: usize = 3;
const REPLAY_CONTROL_CONNECT_BACKOFF_BASE: Duration = Duration::from_millis(100);
const REPLAY_CONTROL_CONNECT_BACKOFF_MAX: Duration = Duration::from_secs(1);

impl ServiceContainer {
    /// Create a new service container with the given database URL
    pub async fn new(database_url: Option<String>) -> Result<Self> {
        // Get database URL from parameter or environment
        let (db_url, namespace_url) = match database_url {
            Some(url) => (url, false),
            None => (
                std::env::var("DATABASE_URL").map_err(|_| {
                    SinexError::configuration("Database URL not provided and DATABASE_URL not set")
                })?,
                true,
            ),
        };
        let db_url = if namespace_url {
            sinex_environment::environment()
                .database_url(&db_url)
                .map_err(|e| SinexError::configuration(format!("Invalid database URL: {e}")))?
        } else {
            db_url
        };

        // Issue 129: Expose pool configuration via environment variables
        // Issue 150 (LOW): Connection pool health checks
        //
        // SQLx PgPool does not expose a test_before_acquire option. Connection health is
        // managed through:
        //
        // - idle_timeout: Closes connections idle for too long (default: 10 minutes)
        // - max_lifetime: Not exposed by sinex PoolConfig but could be added if needed
        // - Connection errors trigger automatic retry via SQLx internals
        //
        // For the gateway's workload:
        // - Read queries dominate (analytics, search)
        // - Connection lifetime is managed by pgbouncer in production
        // - idle_timeout is sufficient for preventing stale connections
        //
        // To enable additional health monitoring, consider wrapping queries with
        // `acquire_with_timeout` which includes latency warnings.
        let mut base_config = PoolConfig::default();

        if let Ok(max_conn_str) = std::env::var("SINEX_GATEWAY_POOL_MAX_CONNECTIONS") {
            if let Ok(max_conn) = max_conn_str.parse::<u32>() {
                base_config.max_connections = max_conn;
            } else {
                warn!(
                    "Invalid SINEX_GATEWAY_POOL_MAX_CONNECTIONS value: {}, using default",
                    max_conn_str
                );
            }
        }

        if let Ok(min_conn_str) = std::env::var("SINEX_GATEWAY_POOL_MIN_CONNECTIONS") {
            if let Ok(min_conn) = min_conn_str.parse::<u32>() {
                base_config.min_connections = min_conn;
            } else {
                warn!(
                    "Invalid SINEX_GATEWAY_POOL_MIN_CONNECTIONS value: {}, using default",
                    min_conn_str
                );
            }
        }

        if let Ok(timeout_str) = std::env::var("SINEX_GATEWAY_POOL_ACQUIRE_TIMEOUT_SECS") {
            if let Ok(timeout_secs) = timeout_str.parse::<u64>() {
                base_config.acquire_timeout_secs = timeout_secs.into();
            } else {
                warn!(
                    "Invalid SINEX_GATEWAY_POOL_ACQUIRE_TIMEOUT_SECS value: {}, using default",
                    timeout_str
                );
            }
        }

        let service_config = per_service_pool_config(&base_config, 4);

        let analytics_pool = create_pool_with_config(&db_url, &service_config)
            .await
            .map_err(|e| {
                SinexError::service("Failed to create database pool")
                    .with_operation("gateway.create_pool.analytics")
                    .with_source(e.to_string())
            })?;
        let content_pool = create_pool_with_config(&db_url, &service_config)
            .await
            .map_err(|e| {
                SinexError::service("Failed to create database pool")
                    .with_operation("gateway.create_pool.content")
                    .with_source(e.to_string())
            })?;
        let pkm_pool = create_pool_with_config(&db_url, &service_config)
            .await
            .map_err(|e| {
                SinexError::service("Failed to create database pool")
                    .with_operation("gateway.create_pool.pkm")
                    .with_source(e.to_string())
            })?;
        let search_pool = create_pool_with_config(&db_url, &service_config)
            .await
            .map_err(|e| {
                SinexError::service("Failed to create database pool")
                    .with_operation("gateway.create_pool.search")
                    .with_source(e.to_string())
            })?;

        // Create blob manager for content service
        // Issue 130: Use persistent default path instead of /tmp
        let annex_path_str = match std::env::var("SINEX_ANNEX_PATH") {
            Ok(value) => value,
            Err(_) => {
                // Use ~/.local/share/sinex/annex as persistent default
                let default_path = if let Ok(home) = std::env::var("HOME") {
                    format!("{}/.local/share/sinex/annex", home)
                } else {
                    // Fallback to work_directory if HOME is not set
                    let work_dir = sinex_environment::environment().work_directory("annex");
                    work_dir.to_string_lossy().into_owned()
                };
                default_path
            }
        };
        let annex_path = SanitizedPath::from_str_validated(&annex_path_str)
            .map_err(|e| SinexError::validation(format!("Invalid SINEX_ANNEX_PATH: {}", e)))?;
        let annex_path = Utf8PathBuf::from(annex_path.as_str());

        // Ensure the annex directory exists
        tokio::fs::create_dir_all(&annex_path).await.map_err(|e| {
            SinexError::io("Failed to create annex directory")
                .with_path(&annex_path)
                .with_source(e.to_string())
        })?;

        let annex_config = sinex_node_sdk::annex::AnnexConfig {
            repo_path: annex_path,
            num_copies: None,
            large_files: None,
        };
        let blob_manager = Arc::new(
            BlobManager::new(annex_config, content_pool.clone(), None).map_err(|e| {
                SinexError::service("Failed to create blob manager").with_source(e.to_string())
            })?,
        );

        // Initialize all services
        let allow_replay_bypass =
            std::env::var("SINEX_ALLOW_REPLAY_CONTROL_BYPASS").map_or(false, |value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes"
                )
            });

        let replay = Arc::new(ReplayStateMachine::new(content_pool.clone()));
        let nats_config = sinex_core::nats::NatsConnectionConfig::from_env();
        let mut replay_control_init_error = None;

        // Connect to NATS for replay control and coordination
        let control_client = if allow_replay_bypass {
            match connect_replay_control_with_backoff(&nats_config, replay.clone()).await {
                Ok(client) => Some(client),
                Err(err) => {
                    warn!(
                        error = %err,
                        "Replay control bus disabled (SINEX_ALLOW_REPLAY_CONTROL_BYPASS=1)"
                    );
                    replay_control_init_error = Some(ReplayControlError::new(err.to_string()));
                    None
                }
            }
        } else {
            Some(connect_replay_control_with_backoff(&nats_config, replay.clone()).await?)
        };

        // Initialize coordination client (best-effort, optional)
        let (nats_client, coordination_client) = match nats_config.connect().await {
            Ok(client) => {
                let js = async_nats::jetstream::new(client.clone());
                // Use "sinex-gateway" as the service name for coordination queries
                let coord = Some(Arc::new(CoordinationKvClient::new(
                    js,
                    "sinex-gateway".to_string(),
                )));
                (Some(client), coord)
            }
            Err(err) => {
                warn!(error = %err, "Coordination client disabled (NATS connection failed)");
                (None, None)
            }
        };

        // Get environment for handler operations
        let env = sinex_environment::environment();

        Ok(Self {
            pool_max_connections: [
                analytics_pool.options().get_max_connections(),
                content_pool.options().get_max_connections(),
                pkm_pool.options().get_max_connections(),
                search_pool.options().get_max_connections(),
            ]
            .iter()
            .map(|value| *value as usize)
            .sum(),
            analytics: Arc::new(AnalyticsService::new(analytics_pool)),
            content: Arc::new(ContentService::new(content_pool, blob_manager)),
            pkm: Arc::new(PkmService::new(pkm_pool)),
            search: Arc::new(SearchService::new(search_pool)),
            replay_control: control_client,
            coordination: coordination_client,
            nats_client,
            env,
            replay_control_bypass: allow_replay_bypass,
            replay_control_init_error,
        })
    }

    /// Get NATS client if available
    pub fn nats_client(&self) -> Option<&async_nats::Client> {
        self.nats_client.as_ref()
    }

    /// Get Sinex environment
    pub fn environment(&self) -> &sinex_core::environment::SinexEnvironment {
        &self.env
    }

    /// Get a database pool for general operations
    /// Uses the content service pool as it's already used for system operations
    pub fn pool(&self) -> &sqlx::PgPool {
        self.content.pool()
    }

    pub fn pool_max_connections(&self) -> usize {
        self.pool_max_connections
    }

    pub fn replay_control_status(&self) -> ReplayControlStatus {
        let enabled = self.replay_control.is_some();
        let bypass_active = self.replay_control_bypass && !enabled;
        let (connected, last_error) = match &self.replay_control {
            Some(client) => {
                let snapshot = client.health_snapshot();
                let last_error = snapshot
                    .last_error
                    .or_else(|| self.replay_control_init_error.clone());
                (snapshot.connected, last_error)
            }
            None => (false, self.replay_control_init_error.clone()),
        };

        ReplayControlStatus {
            enabled,
            bypass_allowed: self.replay_control_bypass,
            bypass_active,
            connected,
            last_error,
        }
    }
}

async fn connect_replay_control_with_backoff(
    nats_config: &sinex_core::nats::NatsConnectionConfig,
    replay: Arc<ReplayStateMachine>,
) -> Result<ReplayControlClient> {
    let mut attempt = 0usize;
    let mut backoff = REPLAY_CONTROL_CONNECT_BACKOFF_BASE;

    loop {
        attempt += 1;
        let result = async {
            let nats_client = nats_config.connect().await.map_err(|err| {
                SinexError::service("Failed to connect to NATS")
                    .with_operation("gateway.connect_nats")
                    .with_source(err.to_string())
            })?;
            spawn_replay_control(replay.clone(), nats_client)
                .await
                .map_err(|err| {
                    SinexError::service("Failed to initialize replay control")
                        .with_operation("gateway.spawn_replay_control")
                        .with_source(err.to_string())
                })
        }
        .await;

        match result {
            Ok(client) => return Ok(client),
            Err(err) => {
                if attempt >= REPLAY_CONTROL_CONNECT_ATTEMPTS {
                    return Err(err.into());
                }
                warn!(
                    attempt,
                    backoff_ms = backoff.as_millis(),
                    error = %err,
                    "Replay control startup failed; retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(
                    backoff.saturating_mul(2),
                    REPLAY_CONTROL_CONNECT_BACKOFF_MAX,
                );
            }
        }
    }
}

fn per_service_pool_config(base: &PoolConfig, service_count: u32) -> PoolConfig {
    let mut config = base.clone();
    let divisor = service_count.max(1);
    config.max_connections = (base.max_connections / divisor).max(1);
    config.min_connections = (base.min_connections / divisor).min(config.max_connections);
    config
}
