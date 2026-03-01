//! Service container that holds all service instances

use crate::replay_control::{ReplayControlClient, ReplayControlError, spawn_replay_control};
use crate::replay_state_machine::ReplayStateMachine;
use camino::Utf8PathBuf;
use color_eyre::eyre::Result;
use sinex_db::{PoolConfig, create_pool_with_config};
use sinex_node_sdk::annex::BlobManager;
use sinex_primitives::domain::SanitizedPath;
use sinex_primitives::{
    coordination::CoordinationKvClient, environment as sinex_environment, error::SinexError,
};
use sinex_services::{ContentService, PkmService};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// Container holding all service instances
#[derive(Clone)]
pub struct ServiceContainer {
    pool_max_connections: usize,
    pub content: Arc<ContentService>,
    pub pkm: Arc<PkmService>,
    pub replay_control: Option<ReplayControlClient>,
    pub coordination: Option<Arc<CoordinationKvClient>>,
    nats_client: Option<async_nats::Client>,
    env: sinex_primitives::environment::SinexEnvironment,
    replay_control_optional: bool,
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
        let db_url = match database_url {
            Some(url) => url,
            None => std::env::var("DATABASE_URL").map_err(|_| {
                SinexError::configuration("Database URL not provided and DATABASE_URL not set")
            })?,
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
        apply_env_pool_overrides(&mut base_config);

        let service_config = per_service_pool_config(&base_config, 2);

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

        // Create blob manager for content service
        let annex_path = resolve_annex_path()?;

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
        let replay_control_optional =
            std::env::var("SINEX_REPLAY_CONTROL_OPTIONAL").is_ok_and(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes"
                )
            });

        let replay = Arc::new(ReplayStateMachine::new(content_pool.clone()));
        let nats_config = sinex_primitives::nats::NatsConnectionConfig::from_env();
        let mut replay_control_init_error = None;

        // Connect to NATS for replay control and coordination
        let control_client = if replay_control_optional {
            match connect_replay_control_with_backoff(&nats_config, replay.clone()).await {
                Ok(client) => Some(client),
                Err(err) => {
                    warn!(
                        error = %err,
                        "Replay control bus disabled (SINEX_REPLAY_CONTROL_OPTIONAL=1)"
                    );
                    replay_control_init_error = Some(ReplayControlError::new(err.to_string()));
                    None
                }
            }
        } else {
            Some(connect_replay_control_with_backoff(&nats_config, replay.clone()).await?)
        };

        // Two NATS connections are established intentionally:
        // 1. The replay-control connection (above) handles time-critical command traffic and
        //    JetStream subscriptions; keeping it isolated prevents coordination traffic from
        //    interfering with replay operations.
        // 2. This second connection is used solely for coordination (KV store, service
        //    discovery). Separating them prevents a slow replay command from starving
        //    coordination queries on the shared connection.
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
                content_pool.options().get_max_connections(),
                pkm_pool.options().get_max_connections(),
            ]
            .iter()
            .map(|value| *value as usize)
            .sum(),
            content: Arc::new(ContentService::new(content_pool, blob_manager)),
            pkm: Arc::new(PkmService::new(pkm_pool)),
            replay_control: control_client,
            coordination: coordination_client,
            nats_client,
            env,
            replay_control_optional,
            replay_control_init_error,
        })
    }

    /// Get NATS client if available
    #[must_use]
    pub fn nats_client(&self) -> Option<&async_nats::Client> {
        self.nats_client.as_ref()
    }

    /// Get Sinex environment
    #[must_use]
    pub fn environment(&self) -> &sinex_primitives::environment::SinexEnvironment {
        &self.env
    }

    /// Get a database pool for general operations
    /// Uses the content service pool as it's already used for system operations
    #[must_use]
    pub fn pool(&self) -> &sqlx::PgPool {
        self.content.pool()
    }

    #[must_use]
    pub fn pool_max_connections(&self) -> usize {
        self.pool_max_connections
    }

    #[must_use]
    pub fn replay_control_status(&self) -> ReplayControlStatus {
        let enabled = self.replay_control.is_some();
        let bypass_active = self.replay_control_optional && !enabled;
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
            bypass_allowed: self.replay_control_optional,
            bypass_active,
            connected,
            last_error,
        }
    }

    /// Perform an active NATS connectivity probe.
    ///
    /// Unlike `nats_client().connection_state()`, which reports a cached in-process state,
    /// this issues a real request to the broker (via JetStream info) and times out if
    /// the broker is unreachable. Use this in health checks to catch stale connections.
    pub async fn probe_nats_active(&self) -> NatsHealthProbe {
        let Some(client) = self.nats_client.as_ref() else {
            return NatsHealthProbe {
                connected: false,
                latency_ms: None,
                detail: "NATS client not configured".to_string(),
            };
        };

        // Fast state check first — avoid the async round-trip if already known disconnected
        if !matches!(
            client.connection_state(),
            async_nats::connection::State::Connected
        ) {
            return NatsHealthProbe {
                connected: false,
                latency_ms: None,
                detail: "NATS connection state: not connected".to_string(),
            };
        }

        // Active probe: flush() sends PING and waits for PONG — a genuine broker round-trip.
        // This catches stale connections that still report Connected in-process state.
        let start = std::time::Instant::now();
        match tokio::time::timeout(Duration::from_millis(500), client.flush()).await {
            Ok(Ok(())) => NatsHealthProbe {
                connected: true,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                detail: "ok".to_string(),
            },
            Ok(Err(e)) => NatsHealthProbe {
                connected: false,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                detail: format!("NATS flush failed: {e}"),
            },
            Err(_timeout) => NatsHealthProbe {
                connected: false,
                latency_ms: Some(500),
                detail: "NATS active probe timed out (>500ms)".to_string(),
            },
        }
    }

    /// Produce a unified health report for the gateway.
    ///
    /// Covers:
    /// - Database reachability (ping via any pool)
    /// - NATS broker active probe (round-trip, not just cached state)
    /// - Replay control bus connectivity and bypass status
    pub async fn health_report(&self) -> GatewayHealthReport {
        // Database ping — use the content pool (shared system pool).
        // Bounded by a 5-second timeout so a stalled DB doesn't hang the health endpoint.
        let db_ok = tokio::time::timeout(
            Duration::from_secs(5),
            sqlx::query("SELECT 1").execute(self.pool()),
        )
        .await
        .is_ok_and(|r| r.is_ok());

        let nats = self.probe_nats_active().await;
        let replay = self.replay_control_status();

        // NATS is required unless the gateway was started with replay-control optional
        // (SINEX_REPLAY_CONTROL_OPTIONAL=1), which signals that a NATS-free degraded
        // mode is acceptable. In that case a NATS outage does not flip healthy to false.
        let nats_ok = nats.connected || self.replay_control_optional;
        GatewayHealthReport {
            db_ok,
            nats,
            replay,
            healthy: db_ok && nats_ok,
        }
    }
}

/// Result of an active NATS connectivity probe.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NatsHealthProbe {
    /// Whether the probe succeeded
    pub connected: bool,
    /// Round-trip latency in milliseconds (None if probe skipped)
    pub latency_ms: Option<u64>,
    /// Human-readable detail or error message
    pub detail: String,
}

/// Unified health report for the gateway.
#[derive(Debug, Clone, serde::Serialize)]
pub struct GatewayHealthReport {
    /// Database is reachable
    pub db_ok: bool,
    /// NATS active probe result
    pub nats: NatsHealthProbe,
    /// Replay control bus status
    pub replay: ReplayControlStatus,
    /// Overall health: db_ok is always required; NATS is required unless
    /// SINEX_REPLAY_CONTROL_OPTIONAL=1 was set (degraded/read-only mode).
    pub healthy: bool,
}

async fn connect_replay_control_with_backoff(
    nats_config: &sinex_primitives::nats::NatsConnectionConfig,
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

/// Apply environment variable overrides to pool configuration.
fn apply_env_pool_overrides(config: &mut PoolConfig) {
    fn try_parse_env_u32(var: &str, target: &mut u32) {
        if let Ok(raw) = std::env::var(var) {
            if let Ok(val) = raw.parse::<u32>() {
                *target = val;
            } else {
                warn!("Invalid {var} value: {raw}, using default");
            }
        }
    }

    try_parse_env_u32(
        "SINEX_GATEWAY_POOL_MAX_CONNECTIONS",
        &mut config.max_connections,
    );
    try_parse_env_u32(
        "SINEX_GATEWAY_POOL_MIN_CONNECTIONS",
        &mut config.min_connections,
    );
    if let Ok(raw) = std::env::var("SINEX_GATEWAY_POOL_ACQUIRE_TIMEOUT_SECS") {
        match raw.parse::<u64>() {
            Ok(secs) => {
                config.acquire_timeout_secs = sinex_primitives::units::Seconds::from_secs(secs);
            }
            Err(_) => {
                warn!(
                    "Invalid SINEX_GATEWAY_POOL_ACQUIRE_TIMEOUT_SECS value: {raw}, using default"
                );
            }
        }
    }
}

/// Resolve the git-annex storage path from environment or defaults.
fn resolve_annex_path() -> Result<Utf8PathBuf> {
    let raw = std::env::var("SINEX_ANNEX_PATH").unwrap_or_else(|_| {
        std::env::var("HOME").map_or_else(
            |_| {
                sinex_environment::environment()
                    .work_directory("annex")
                    .to_string_lossy()
                    .into_owned()
            },
            |home| format!("{home}/.local/share/sinex/annex"),
        )
    });
    let sanitized = SanitizedPath::from_str_validated(&raw)
        .map_err(|e| SinexError::validation(format!("Invalid SINEX_ANNEX_PATH: {e}")))?;
    Ok(Utf8PathBuf::from(sanitized.as_str()))
}

fn per_service_pool_config(base: &PoolConfig, service_count: u32) -> PoolConfig {
    let mut config = base.clone();
    let divisor = service_count.max(1);
    config.max_connections = (base.max_connections / divisor).max(1);
    config.min_connections = (base.min_connections / divisor).min(config.max_connections);
    config
}
