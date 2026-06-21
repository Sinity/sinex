//! Service container that holds all service instances

use crate::api::config::GatewayConfig;
use crate::api::content_service::ContentService;
use crate::api::replay_control::{ReplayControlClient, ReplayControlError, spawn_replay_control};
use crate::event_engine::policy::PolicyEngine;
use crate::runtime::content_store::{ContentStoreConfig, ContentStoreManager};
use sinex_db::create_pool_with_config;
use sinex_db::pkm::PkmService;
use sinex_db::replay::state_machine::ReplayStateMachine;
use sinex_primitives::{
    Result as SinexResult, coordination::CoordinationKvClient, environment as sinex_environment,
    error::SinexError, runtime_pressure::RuntimePressureAction,
};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
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
    privacy_policy: Arc<PolicyEngine>,
    nats_client: Option<async_nats::Client>,
    env: sinex_primitives::environment::SinexEnvironment,
    sse_bus: Arc<OnceLock<Arc<crate::api::sse_bus::SubscriptionBus>>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayControlStatus {
    pub enabled: bool,
    pub connected: bool,
    pub last_error: Option<ReplayControlError>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SseConfirmationStatus {
    pub running: bool,
    pub degraded: bool,
    pub detail: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RawIngestDlqHealth {
    pub status: GatewayHealthStatus,
    pub connected: bool,
    pub pending_messages: Option<u64>,
    pub pending_sequence_span: Option<u64>,
    pub detail: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ConfirmationBufferHealth {
    pub status: GatewayHealthStatus,
    pub connected: bool,
    pub memory_owner: ConfirmationBufferMemoryOwner,
    pub pressure_level: String,
    pub runtime_action: RuntimePressureAction,
    pub observed_buffers: usize,
    pub pending_count: usize,
    pub timed_out_retained_count: usize,
    pub rejected_count: u64,
    pub late_confirmation_count: u64,
    pub retained_payload_bytes: usize,
    pub approximate_payload_bytes: usize,
    pub active_payload_bytes: usize,
    pub timed_out_retained_payload_bytes: usize,
    pub approximate_payload_bytes_by_kind: BTreeMap<String, usize>,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfirmationBufferMemoryOwner {
    NotObserved,
    None,
    ActivePendingPayloads,
    TimedOutGracePayloads,
    CountersOnly,
}

impl ConfirmationBufferMemoryOwner {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotObserved => "not_observed",
            Self::None => "none",
            Self::ActivePendingPayloads => "active_pending_payloads",
            Self::TimedOutGracePayloads => "timed_out_grace_payloads",
            Self::CountersOnly => "counters_only",
        }
    }
}

const CONFIRMATION_BUFFER_DEGRADED_BYTES: usize = 64 * 1024 * 1024;

/// Type alias — gateway uses the canonical `HealthStatus` domain enum.
pub type GatewayHealthStatus = sinex_primitives::domain::HealthStatus;

const REPLAY_CONTROL_CONNECT_ATTEMPTS: usize = 3;
const REPLAY_CONTROL_CONNECT_BACKOFF_BASE: Duration = Duration::from_millis(100);
const REPLAY_CONTROL_CONNECT_BACKOFF_MAX: Duration = Duration::from_secs(1);

async fn recover_stale_replay_operations(replay: &ReplayStateMachine) -> SinexResult<()> {
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
        .with_source(error.to_string())),
    }
}

impl ServiceContainer {
    /// Create a service container from a database URL (test convenience).
    ///
    /// Loads the normal environment-backed gateway configuration, then forces the
    /// provided database URL on top. For production use, prefer `new()` with a full
    /// `GatewayConfig` loaded by the process entrypoint.
    pub async fn from_database_url(database_url: impl Into<String>) -> SinexResult<Self> {
        let config = GatewayConfig::load_with_database_url(database_url.into())?;
        Self::new(&config).await
    }

    /// Create a new service container from gateway configuration.
    pub async fn new(config: &GatewayConfig) -> SinexResult<Self> {
        let db_url = if config.database_url.trim().is_empty() {
            return Err(SinexError::configuration(
                "Database URL not provided — set DATABASE_URL or the NixOS module option that exports it",
            ));
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

        // Check binary-schema version compatibility
        verify_binary_schema_version(&content_pool).await?;

        // Create content store for content service
        let content_store_path = config.resolve_content_store_path()?;

        // Ensure the content-store root exists
        tokio::fs::create_dir_all(&content_store_path)
            .await
            .map_err(|e| {
                SinexError::io("Failed to create content-store root")
                    .with_path(&content_store_path)
                    .with_source(e.to_string())
            })?;

        let content_store_config = ContentStoreConfig {
            root_path: content_store_path,
            num_copies: None,
            large_files: None,
            ..Default::default()
        };
        let content_store = Arc::new(
            ContentStoreManager::new(content_store_config, content_pool.clone(), None).map_err(
                |e| {
                    SinexError::service("Failed to create content store").with_source(e.to_string())
                },
            )?,
        );

        let replay = Arc::new(ReplayStateMachine::new(content_pool.clone()));
        let privacy_policy = Arc::new(PolicyEngine::load(content_pool.clone()).await?);

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
            "sinexd".to_string(),
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
            content: Arc::new(ContentService::new(content_pool, content_store)),
            pkm: Arc::new(PkmService::new(pkm_pool)),
            replay_control: control_client,
            coordination: coordination_client,
            privacy_policy,
            nats_client,
            env,
            sse_bus: Arc::new(OnceLock::new()),
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

    /// Get the runtime state directory used by local gateway state files.
    #[must_use]
    pub fn state_dir(&self) -> &std::path::Path {
        &self.config.state_dir
    }

    /// Attach the SSE confirmation bus after runtime startup has constructed it.
    pub(crate) fn attach_sse_bus(&self, bus: Arc<crate::api::sse_bus::SubscriptionBus>) {
        let _ = self.sse_bus.set(bus);
    }

    /// Inspect confirmation fan-out health.
    #[must_use]
    pub fn sse_confirmation_status(&self) -> SseConfirmationStatus {
        let Some(bus) = self.sse_bus.get() else {
            return SseConfirmationStatus {
                running: false,
                degraded: true,
                detail: "SSE confirmation bus not running".to_string(),
            };
        };
        let snapshot = bus.health_snapshot();
        let degraded = snapshot.pending_retry_confirmations > 0
            || snapshot.dropped_confirmations_total > 0
            || snapshot.db_fetch_failures_total > 0
            || snapshot.malformed_confirmations_total > 0;
        SseConfirmationStatus {
            running: true,
            degraded,
            detail: format!(
                "active_subscriptions={}, pending_retries={}, dropped_confirmations={}, db_fetch_failures={}, malformed_confirmations={}, reconnects={}",
                snapshot.active_subscriptions,
                snapshot.pending_retry_confirmations,
                snapshot.dropped_confirmations_total,
                snapshot.db_fetch_failures_total,
                snapshot.malformed_confirmations_total,
                snapshot.subscription_reconnects_total,
            ),
        }
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
    pub fn privacy_policy(&self) -> &Arc<PolicyEngine> {
        &self.privacy_policy
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

    /// Inspect raw-ingest DLQ pressure through JetStream stream state.
    pub async fn probe_raw_ingest_dlq_pressure(&self) -> RawIngestDlqHealth {
        let Some(client) = self.nats_client.as_ref() else {
            return RawIngestDlqHealth {
                status: GatewayHealthStatus::Unknown,
                connected: false,
                pending_messages: None,
                pending_sequence_span: None,
                detail: "NATS client not configured; raw-ingest DLQ pressure unknown".to_string(),
            };
        };

        if !matches!(
            client.connection_state(),
            async_nats::connection::State::Connected
        ) {
            return RawIngestDlqHealth {
                status: GatewayHealthStatus::Unknown,
                connected: false,
                pending_messages: None,
                pending_sequence_span: None,
                detail: "NATS connection state is not connected; raw-ingest DLQ pressure unknown"
                    .to_string(),
            };
        }

        let js = async_nats::jetstream::new(client.clone());
        let dlq_stream = self.env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");
        let mut stream = match tokio::time::timeout(
            Duration::from_millis(500),
            js.get_stream(&dlq_stream),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                return RawIngestDlqHealth {
                    status: GatewayHealthStatus::Unknown,
                    connected: true,
                    pending_messages: None,
                    pending_sequence_span: None,
                    detail: format!("raw-ingest DLQ stream {dlq_stream} unavailable: {error}"),
                };
            }
            Err(_timeout) => {
                return RawIngestDlqHealth {
                    status: GatewayHealthStatus::Unknown,
                    connected: true,
                    pending_messages: None,
                    pending_sequence_span: None,
                    detail: "raw-ingest DLQ stream lookup timed out (>500ms)".to_string(),
                };
            }
        };

        let state = match tokio::time::timeout(Duration::from_millis(500), stream.info()).await {
            Ok(Ok(info)) => info.state.clone(),
            Ok(Err(error)) => {
                return RawIngestDlqHealth {
                    status: GatewayHealthStatus::Unknown,
                    connected: true,
                    pending_messages: None,
                    pending_sequence_span: None,
                    detail: format!("raw-ingest DLQ stream inspection failed: {error}"),
                };
            }
            Err(_timeout) => {
                return RawIngestDlqHealth {
                    status: GatewayHealthStatus::Unknown,
                    connected: true,
                    pending_messages: None,
                    pending_sequence_span: None,
                    detail: "raw-ingest DLQ stream inspection timed out (>500ms)".to_string(),
                };
            }
        };

        let pending_sequence_span = if state.messages == 0
            || state.first_sequence == 0
            || state.last_sequence < state.first_sequence
        {
            0
        } else {
            state.last_sequence - state.first_sequence + 1
        };
        if state.messages == 0 {
            RawIngestDlqHealth {
                status: GatewayHealthStatus::Healthy,
                connected: true,
                pending_messages: Some(0),
                pending_sequence_span: Some(0),
                detail: "raw-ingest DLQ empty".to_string(),
            }
        } else {
            RawIngestDlqHealth {
                status: GatewayHealthStatus::Degraded,
                connected: true,
                pending_messages: Some(state.messages),
                pending_sequence_span: Some(pending_sequence_span),
                detail: format!(
                    "raw-ingest DLQ pressure: {} pending message(s), sequence span {}",
                    state.messages, pending_sequence_span
                ),
            }
        }
    }

    /// Inspect registered confirmation buffers without extending their lifetime.
    pub async fn probe_confirmation_buffer_pressure(&self) -> ConfirmationBufferHealth {
        let snapshots = crate::runtime::registered_confirmation_buffer_snapshots().await;
        if snapshots.is_empty() {
            return ConfirmationBufferHealth {
                status: GatewayHealthStatus::Unknown,
                connected: false,
                memory_owner: ConfirmationBufferMemoryOwner::NotObserved,
                pressure_level: "unknown".to_string(),
                runtime_action: RuntimePressureAction::None,
                observed_buffers: 0,
                pending_count: 0,
                timed_out_retained_count: 0,
                rejected_count: 0,
                late_confirmation_count: 0,
                retained_payload_bytes: 0,
                approximate_payload_bytes: 0,
                active_payload_bytes: 0,
                timed_out_retained_payload_bytes: 0,
                approximate_payload_bytes_by_kind: BTreeMap::new(),
                detail: "confirmation buffers not registered".to_string(),
            };
        }

        let mut pending_count = 0usize;
        let mut timed_out_retained_count = 0usize;
        let mut rejected_count = 0u64;
        let mut late_confirmation_count = 0u64;
        let mut retained_payload_bytes = 0usize;
        let mut approximate_payload_bytes = 0usize;
        let mut active_payload_bytes = 0usize;
        let mut timed_out_retained_payload_bytes = 0usize;
        let mut approximate_payload_bytes_by_kind = BTreeMap::new();
        let mut pressure_level = crate::runtime::ConfirmationBufferPressureLevel::Nominal;
        let mut runtime_action = RuntimePressureAction::Admit;

        for snapshot in &snapshots {
            pending_count += snapshot.pending_count;
            timed_out_retained_count += snapshot.timed_out_retained_count;
            rejected_count += snapshot.rejected_count;
            late_confirmation_count += snapshot.late_confirmation_count;
            retained_payload_bytes += snapshot.retained_payload_bytes;
            approximate_payload_bytes += snapshot.approximate_payload_bytes;
            active_payload_bytes += snapshot.active_payload_bytes;
            timed_out_retained_payload_bytes += snapshot.timed_out_retained_payload_bytes;
            pressure_level =
                strongest_confirmation_pressure(pressure_level, snapshot.pressure_level);
            runtime_action = runtime_action.strongest(snapshot.runtime_action);
            for (kind, bytes) in &snapshot.approximate_payload_bytes_by_kind {
                *approximate_payload_bytes_by_kind
                    .entry(kind.clone())
                    .or_insert(0) += *bytes;
            }
        }

        let status = if timed_out_retained_count > 0
            || rejected_count > 0
            || approximate_payload_bytes >= CONFIRMATION_BUFFER_DEGRADED_BYTES
        {
            GatewayHealthStatus::Degraded
        } else {
            GatewayHealthStatus::Healthy
        };
        let memory_owner = confirmation_buffer_memory_owner(
            active_payload_bytes,
            timed_out_retained_payload_bytes,
            rejected_count,
            late_confirmation_count,
        );
        let top_kind = approximate_payload_bytes_by_kind
            .iter()
            .max_by_key(|(_, bytes)| *bytes)
            .map(|(kind, bytes)| format!(", top_kind={kind} ({bytes} bytes)"))
            .unwrap_or_default();
        let detail = format!(
            "confirmation buffers: observed={}, pending={}, timed_out_retained={}, rejected={}, late_confirmations={}, pressure_level={}, runtime_action={}, retained_payload_bytes={}, approximate_payload_bytes={}, active_payload_bytes={}, timed_out_retained_payload_bytes={}, memory_owner={}{}",
            snapshots.len(),
            pending_count,
            timed_out_retained_count,
            rejected_count,
            late_confirmation_count,
            pressure_level.as_str(),
            runtime_action,
            retained_payload_bytes,
            approximate_payload_bytes,
            active_payload_bytes,
            timed_out_retained_payload_bytes,
            memory_owner.as_str(),
            top_kind
        );

        ConfirmationBufferHealth {
            status,
            connected: true,
            memory_owner,
            pressure_level: pressure_level.as_str().to_string(),
            runtime_action,
            observed_buffers: snapshots.len(),
            pending_count,
            timed_out_retained_count,
            rejected_count,
            late_confirmation_count,
            retained_payload_bytes,
            approximate_payload_bytes,
            active_payload_bytes,
            timed_out_retained_payload_bytes,
            approximate_payload_bytes_by_kind,
            detail,
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
        let raw_ingest_dlq = self.probe_raw_ingest_dlq_pressure().await;
        let confirmation_buffer = self.probe_confirmation_buffer_pressure().await;
        let replay = self.replay_control_status();
        let sse_confirmation = self.sse_confirmation_status();
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
        if raw_ingest_dlq.status == GatewayHealthStatus::Degraded {
            degradation_reasons.push(raw_ingest_dlq.detail.clone());
        }
        if confirmation_buffer.status == GatewayHealthStatus::Degraded {
            degradation_reasons.push(confirmation_buffer.detail.clone());
        }
        if !sse_confirmation.running {
            degradation_reasons.push("SSE confirmation bus not running".to_string());
        } else if sse_confirmation.degraded {
            degradation_reasons.push("SSE confirmation fan-out degraded".to_string());
        }

        let healthy = db_ok
            && nats.connected
            && raw_ingest_dlq.status != GatewayHealthStatus::Degraded
            && confirmation_buffer.status != GatewayHealthStatus::Degraded
            && replay.connected
            && !sse_confirmation.degraded;
        // Gateway is ready to serve end-to-end RPC traffic only when both
        // the database (query/write path) and NATS (event publishing path)
        // are reachable. Replay control is coordination-only and does not
        // gate serving.
        let serving = db_ok && nats.connected;
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
            raw_ingest_dlq,
            confirmation_buffer,
            replay,
            sse_confirmation,
            healthy,
            serving,
            degradation_reasons,
        }
    }
}

fn confirmation_buffer_memory_owner(
    active_payload_bytes: usize,
    timed_out_retained_payload_bytes: usize,
    rejected_count: u64,
    late_confirmation_count: u64,
) -> ConfirmationBufferMemoryOwner {
    if timed_out_retained_payload_bytes > 0 {
        ConfirmationBufferMemoryOwner::TimedOutGracePayloads
    } else if active_payload_bytes > 0 {
        ConfirmationBufferMemoryOwner::ActivePendingPayloads
    } else if rejected_count > 0 || late_confirmation_count > 0 {
        ConfirmationBufferMemoryOwner::CountersOnly
    } else {
        ConfirmationBufferMemoryOwner::None
    }
}

fn strongest_confirmation_pressure(
    current: crate::runtime::ConfirmationBufferPressureLevel,
    candidate: crate::runtime::ConfirmationBufferPressureLevel,
) -> crate::runtime::ConfirmationBufferPressureLevel {
    use crate::runtime::ConfirmationBufferPressureLevel::{Critical, Nominal, Warning};
    match (current, candidate) {
        (Critical, _) | (_, Critical) => Critical,
        (Warning, _) | (_, Warning) => Warning,
        (Nominal, Nominal) => Nominal,
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
    /// Raw-ingest DLQ pressure state.
    pub raw_ingest_dlq: RawIngestDlqHealth,
    /// Confirmation-buffer pressure and retained-payload attribution.
    pub confirmation_buffer: ConfirmationBufferHealth,
    /// Replay control bus status
    pub replay: ReplayControlStatus,
    /// SSE confirmation fan-out status
    pub sse_confirmation: SseConfirmationStatus,
    /// True only when the gateway and its coordination dependencies are fully healthy.
    pub healthy: bool,
    /// Whether the gateway is ready to serve end-to-end RPC traffic.
    ///
    /// Requires both database connectivity (query/write path) and NATS
    /// connectivity (event publishing path). Replay control availability is
    /// coordination-only and does not gate this flag.
    pub serving: bool,
    /// Reasons the gateway is not fully healthy.
    pub degradation_reasons: Vec<String>,
}

async fn connect_replay_control_with_backoff(
    nats_config: &sinex_primitives::nats::NatsConnectionConfig,
    replay: Arc<ReplayStateMachine>,
    request_timeout: Duration,
) -> SinexResult<ReplayControlClient> {
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
                    return Err(err);
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

async fn verify_binary_schema_version(pool: &sqlx::PgPool) -> Result<(), SinexError> {
    use sinex_primitives::EXPECTED_BINARY_SCHEMA_VERSION;
    let db_version: Option<String> =
        sqlx::query_scalar("SELECT version FROM sinex_schemas.binary_schema_version WHERE id = 1")
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                SinexError::database("Failed to query binary_schema_version")
                    .with_operation("gateway.verify_binary_schema_version")
                    .with_source(e.to_string())
            })?;
    match db_version {
        Some(v) if v == EXPECTED_BINARY_SCHEMA_VERSION => {
            info!(version = %v, "Binary schema version verified");
            Ok(())
        }
        Some(v) => Err(SinexError::configuration(format!(
            "Schema version mismatch: binary expects '{EXPECTED_BINARY_SCHEMA_VERSION}', database has '{v}'"
        )).with_operation("gateway.verify_binary_schema_version")),
        None => {
            // Use ON CONFLICT DO NOTHING to handle a concurrent insert race: two
            // gateway processes starting simultaneously could both observe NULL and
            // then collide on the unique (id) key. After the upsert, re-read to
            // verify that whoever won wrote our expected version.
            sqlx::query(
                "INSERT INTO sinex_schemas.binary_schema_version (id, version) VALUES (1, $1) ON CONFLICT (id) DO NOTHING",
            )
            .bind(EXPECTED_BINARY_SCHEMA_VERSION)
            .execute(pool)
            .await
            .map_err(|e| {
                SinexError::database("Failed to insert binary_schema_version")
                    .with_operation("gateway.verify_binary_schema_version")
                    .with_source(e.to_string())
            })?;
            // Re-read to verify the winner (could be us or a concurrent process)
            let winner: Option<String> = sqlx::query_scalar(
                "SELECT version FROM sinex_schemas.binary_schema_version WHERE id = 1",
            )
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                SinexError::database("Failed to re-read binary_schema_version after insert")
                    .with_operation("gateway.verify_binary_schema_version")
                    .with_source(e.to_string())
            })?;
            match winner {
                Some(v) if v == EXPECTED_BINARY_SCHEMA_VERSION => {
                    info!(version = %EXPECTED_BINARY_SCHEMA_VERSION, "Initialized binary_schema_version");
                    Ok(())
                }
                Some(v) => Err(SinexError::configuration(format!(
                    "Schema version mismatch after concurrent insert: binary expects '{EXPECTED_BINARY_SCHEMA_VERSION}', database has '{v}'"
                )).with_operation("gateway.verify_binary_schema_version")),
                None => Err(SinexError::database(
                    "binary_schema_version row missing after insert attempt",
                ).with_operation("gateway.verify_binary_schema_version")),
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
