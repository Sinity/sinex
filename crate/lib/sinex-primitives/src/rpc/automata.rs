//! Operator-facing automata status RPC types.

use crate::domain::NodeName;
use crate::{Timestamp, Uuid};
use serde::{Deserialize, Serialize};

fn default_stale_after_secs() -> u64 {
    300
}

fn default_recent_window_secs() -> u64 {
    300
}

/// Request: `automata.status`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomataStatusRequest {
    /// Heartbeats older than this are considered stale.
    #[serde(default = "default_stale_after_secs")]
    pub stale_after_secs: u64,
    /// Time window used for recent output/error-rate context.
    #[serde(default = "default_recent_window_secs")]
    pub recent_window_secs: u64,
}

impl Default for AutomataStatusRequest {
    fn default() -> Self {
        Self {
            stale_after_secs: default_stale_after_secs(),
            recent_window_secs: default_recent_window_secs(),
        }
    }
}

/// Response: `automata.status`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomataStatusResponse {
    pub generated_at: Timestamp,
    pub stale_after_secs: u64,
    pub recent_window_secs: u64,
    pub automata: Vec<AutomatonStatus>,
}

/// Operator-visible state for one registered automaton.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomatonStatus {
    pub node_name: NodeName,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub manifest_status: String,
    pub live: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_run_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events_processed_current_run: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_position: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_revision: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_recorded_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_invalidation_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_rate_5m: Option<f64>,
    pub recent_output_count: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_output_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_replay_at: Option<Timestamp>,
}
