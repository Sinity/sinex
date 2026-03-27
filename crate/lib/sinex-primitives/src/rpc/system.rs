//! System types

use crate::domain::HealthStatus;
use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────
// system.health
// ─────────────────────────────────────────────────────────────

/// Request: system.health (no params)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SystemHealthRequest {}

/// Component health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
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
    pub database: ComponentHealth,
    pub nats: ComponentHealth,
    pub replay_control: ReplayControlHealth,
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
