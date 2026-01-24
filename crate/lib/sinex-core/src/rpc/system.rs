//! System types

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
    pub status: String,
    pub connected: bool,
}

/// Replay control component health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayControlHealth {
    pub status: String,
    pub enabled: bool,
    pub bypass_allowed: bool,
    pub bypass_active: bool,
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
    /// Overall status: "healthy", "degraded", or "unhealthy"
    pub status: String,
    pub components: ComponentsHealth,
}
