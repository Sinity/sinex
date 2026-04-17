//! Service container that holds all service instances

use crate::config::GatewayConfig;
use crate::replay_control::{ReplayControlClient, ReplayControlError, spawn_replay_control};
use color_eyre::eyre::Result;
use sinex_db::create_pool_with_config;
use sinex_db::replay::state_machine::ReplayStateMachine;
use sinex_node_sdk::annex::BlobManager;
use sinex_primitives::{
    coordination::CoordinationKvClient, environment as sinex_environment, error::SinexError,
};
use sinex_services::{ContentService, PkmService};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// Container holding all service instances
#[derive(Clone)]
pub struct ServiceContainer {
    config: Arc<GatewayConfig>,
    pool_max_connections: usize,
    pub content: Arc<ContentService>,
    pub pkm: Arc<PkmService>,
    pub replay_control: Option<ReplayControlClient>,
    pub coordination: Option<Arc<CoordinationKvClient>>,
    nats_client: Option<async_nats::Client>,
    env: sinex_primitives::environment::SinexEnvironment,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayControlStatus {
    pub enabled: bool,
    pub connected: bool,
    pub last_error: Option<ReplayControlError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayHealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

const REPLAY_CONTROL_CONNECT_ATTEMPTS: usize = 3;
const REPLAY_CONTROL_CONNECT_BACKOFF_BASE: Duration = Duration::from_millis(100);
const REPLAY_CONTROL_CONNECT_BACKOFF_MAX: Duration = Duration::from_secs(1);

async fn recover_stale_replay_operations(replay: &ReplayStateMachine) -> Result<()> {
    const STALE_EXECUTING_THRESHOLD: Duration = Duration::from_mins(10);

    match replay
        .recover_stale_executing(STALE_EXECUTING_THRESHOLD)
        .await
    {
        Ok(0) => Ok(()),
        Ok(recovered) => {
            info!(
                recovered,
                "Recovered stale executing replay operations on startup"
            );
            Ok(())
        }
        Err(error) => Err(SinexError::service(
            "Failed to recover stale replay operations on startup",
        )
        .with_operation("gateway.recover_stale_replay_operations")
        .with_source(error.to_string())
        .into()),
    }
}

impl ServiceContainer {
    /// Create a service container from a database URL (test convenience).
    ///
    /// Loads the normal environment-backed gateway configuration, then forces the
    /// provided database URL on top. For production use, prefer `new()` with a full
    /// `GatewayConfig` loaded by the process entrypoint.
    pub async fn from_database_url(database_url: impl Into<String>) -> Result<Self> {
        let config = GatewayConfig::load_with_database_url(database_url.into())?;
        Self::new(&config).await
    }

    /// Create a new service container from gateway configuration.
    pub async fn new(config: &GatewayConfig) -> Result<Self> {
        let db_url = if config.database_url.trim().is_empty() {
            return Err(SinexError::configuration(
                "Database URL not provided — set DATABASE_URL or the NixOS module option that exports it",
            )
            .into());
        } else {
            config.database_url.clone()
        };

        let base_config = config.pool_config();
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
        let annex_path = config.resolve_annex_path()?;

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

        let replay = Arc::new(ReplayStateMachine::new(content_pool.clone()));

        recover_stale_replay_operations(&replay).await?;

        let nats_config = config.nats_connection_config();

        let control_client = Some(
            connect_replay_control_with_backoff(
                &nats_config,
                replay.clone(),
                config.replay_control_timeout(),
            )
            .await?,
        );

        // Two NATS connections are established intentionally:
        // 1. The replay-control connection (above) handles time-critical command traffic and
        //    JetStream subscriptions; keeping it isolated prevents coordination traffic from
        //    interfering with replay operations.
        // 2. This second connection is used solely for coordination (KV store, service
        //    discovery). Separating them prevents a slow replay command from starving
        //    coordination queries on the shared connection.
        let coordination_nats = nats_config.connect().await.map_err(|err| {
            SinexError::service("Failed to connect to NATS for coordination")
                .with_operation("gateway.connect_nats.coordination")
                .with_source(err.to_string())
        })?;
        let js = async_nats::jetstream::new(coordination_nats.clone());
        let coordination_client = Some(Arc::new(CoordinationKvClient::new(
            js,
            "sinex-gateway".to_string(),
        )));
        let nats_client = Some(coordination_nats);

        // Get environment for handler operations
        let env = sinex_environment::environment();

        Ok(Self {
            config: Arc::new(config.clone()),
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
    pub fn config(&self) -> &GatewayConfig {
        self.config.as_ref()
    }

    #[must_use]
    pub fn replay_control_status(&self) -> ReplayControlStatus {
        let (enabled, connected, last_error) = match &self.replay_control {
            Some(client) => {
                let snapshot = client.health_snapshot();
                (true, snapshot.connected, snapshot.last_error)
            }
            None => (
                false,
                false,
                Some(ReplayControlError::new("replay control not initialized")),
            ),
        };

        ReplayControlStatus {
            enabled,
            connected,
            last_error,
        }
    }

    /// Perform an active NATS connectivity probe.
    ///
    /// Unlike `nats_client().connection_state()`, which reports a cached in-process state,
    /// this issues a real request to the broker (via `JetStream` info) and times out if
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
    /// - Replay control bus connectivity
    pub async fn health_report(&self) -> GatewayHealthReport {
        // Database ping — use the content pool (shared system pool).
        // Bounded by a 5-second timeout so a stalled DB doesn't hang the health endpoint.
        let db_start = std::time::Instant::now();
        let (db_ok, db_latency_ms, db_detail) = match tokio::time::timeout(
            Duration::from_secs(5),
            sqlx::query_scalar!("SELECT 1").fetch_one(self.pool()),
        )
        .await
        {
            Ok(Ok(_)) => (
                true,
                Some(db_start.elapsed().as_millis() as u64),
                "ok".to_string(),
            ),
            Ok(Err(error)) => (
                false,
                Some(db_start.elapsed().as_millis() as u64),
                format!("Database ping failed: {error}"),
            ),
            Err(_timeout) => (
                false,
                Some(5_000),
                "Database ping timed out (>5s)".to_string(),
            ),
        };

        let nats = self.probe_nats_active().await;
        let replay = self.replay_control_status();
        let mut degradation_reasons = Vec::new();

        if !db_ok {
            degradation_reasons.push("database unreachable".to_string());
        }
        if !nats.connected {
            degradation_reasons.push("NATS unavailable".to_string());
        }
        if !replay.connected {
            degradation_reasons.push(if replay.enabled {
                "replay control disconnected".to_string()
            } else {
                "replay control unavailable".to_string()
            });
        }

        let healthy = db_ok && nats.connected && replay.connected;
        let serving = db_ok;
        let status = if !db_ok {
            GatewayHealthStatus::Unhealthy
        } else if healthy {
            GatewayHealthStatus::Healthy
        } else {
            GatewayHealthStatus::Degraded
        };
        GatewayHealthReport {
            status,
            db_ok,
            db_latency_ms,
            db_detail,
            nats,
            replay,
            healthy,
            serving,
            degradation_reasons,
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
    /// Overall status for operators: healthy, degraded, or unhealthy.
    pub status: GatewayHealthStatus,
    /// Database is reachable
    pub db_ok: bool,
    /// Database ping latency in milliseconds.
    pub db_latency_ms: Option<u64>,
    /// Human-readable database probe detail or error message.
    pub db_detail: String,
    /// NATS active probe result
    pub nats: NatsHealthProbe,
    /// Replay control bus status
    pub replay: ReplayControlStatus,
    /// True only when the gateway and its coordination dependencies are fully healthy.
    pub healthy: bool,
    /// Whether the gateway is ready to serve end-to-end RPC traffic.
    pub serving: bool,
    /// Reasons the gateway is not fully healthy.
    pub degradation_reasons: Vec<String>,
}

async fn connect_replay_control_with_backoff(
    nats_config: &sinex_primitives::nats::NatsConnectionConfig,
    replay: Arc<ReplayStateMachine>,
    request_timeout: Duration,
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
            spawn_replay_control(replay.clone(), nats_client, request_timeout)
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

fn per_service_pool_config(
    base: &sinex_db::PoolConfig,
    service_count: u32,
) -> sinex_db::PoolConfig {
    let mut config = base.clone();
    let divisor = service_count.max(1);
    config.max_connections = (base.max_connections / divisor).max(1);
    config.min_connections = (base.min_connections / divisor).min(config.max_connections);
    config
}

#[cfg(test)]
mod tests {
    use super::recover_stale_replay_operations;
    use sqlx::postgres::PgPoolOptions;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn stale_replay_recovery_accepts_clean_state(ctx: TestContext) -> TestResult<()> {
        let replay = sinex_db::replay::state_machine::ReplayStateMachine::new(ctx.pool.clone());
        recover_stale_replay_operations(&replay).await?;
        Ok(())
    }

    #[sinex_test]
    async fn stale_replay_recovery_surfaces_startup_failures() -> TestResult<()> {
        let pool = PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(10))
            .connect_lazy("postgresql://127.0.0.1:1/sinex_test")?;
        let replay = sinex_db::replay::state_machine::ReplayStateMachine::new(pool);

        let error = recover_stale_replay_operations(&replay)
            .await
            .expect_err("startup recovery should fail honestly when the pool is unusable");

        let message = error.to_string();
        assert!(message.contains("Failed to recover stale replay operations on startup"));
        assert!(message.contains("gateway.recover_stale_replay_operations"));
        Ok(())
    }
}
