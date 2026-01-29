use crate::events::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fmt::Display;

/// Status indicators for health checks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

impl Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// Service metadata for registration and discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub name: String,
    pub version: String,
    pub kind: ServiceKind,
    pub status: HealthStatus,
    pub started_at: Timestamp,
    pub metadata: HashMap<String, JsonValue>,
}

/// Types of services in the Sinex ecosystem
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceKind {
    Ingestor,
    Automaton,
    Gateway,
    Collector,
}

impl Display for ServiceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceKind::Ingestor => write!(f, "ingestor"),
            ServiceKind::Automaton => write!(f, "automaton"),
            ServiceKind::Gateway => write!(f, "gateway"),
            ServiceKind::Collector => write!(f, "collector"),
        }
    }
}

/// Common trait for components that can be health-checked
#[async_trait::async_trait]
pub trait HealthCheck: Send + Sync {
    async fn check_health(&self) -> Result<HealthStatus, crate::error::SinexError>;
}
