//! System types

use crate::domain::HealthStatus;
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────
// system.ping / system.version
// ─────────────────────────────────────────────────────────────

pub const SYSTEM_PING_METHOD: RpcMethod<SystemPingRequest, String> = RpcMethod::new(
    methods::SYSTEM_PING,
    RpcRole::ReadOnly,
    RpcDomain::System,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const SYSTEM_VERSION_METHOD: RpcMethod<SystemVersionRequest, String> = RpcMethod::new(
    methods::SYSTEM_VERSION,
    RpcRole::ReadOnly,
    RpcDomain::System,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SystemPingRequest {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SystemVersionRequest {}

// ─────────────────────────────────────────────────────────────
// system.health
// ─────────────────────────────────────────────────────────────

pub const SYSTEM_HEALTH_METHOD: RpcMethod<SystemHealthRequest, SystemHealthResponse> =
    RpcMethod::new(
        methods::SYSTEM_HEALTH,
        RpcRole::ReadOnly,
        RpcDomain::System,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

/// Request: system.health (no params)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SystemHealthRequest {}

/// RPC wire projection of a single component's health status.
///
/// Renamed from `ComponentHealth` to `ComponentHealthReport` to distinguish it from
/// the automaton's in-memory `ComponentHealth` state and the event
/// payload shape — see issue #746 (A4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealthReport {
    pub status: HealthStatus,
    pub connected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Replay control component health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayControlHealth {
    pub status: HealthStatus,
    pub enabled: bool,
    pub connected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// All component health statuses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentsHealth {
    pub database: ComponentHealthReport,
    pub nats: ComponentHealthReport,
    pub raw_ingest_dlq: ComponentHealthReport,
    pub confirmation_buffer: ComponentHealthReport,
    pub replay_control: ReplayControlHealth,
    pub sse_confirmation: ComponentHealthReport,
}

/// Response: system.health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealthResponse {
    /// Overall system health status
    pub status: HealthStatus,
    pub healthy: bool,
    pub serving: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub degradation_reasons: Vec<String>,
    pub components: ComponentsHealth,
}
