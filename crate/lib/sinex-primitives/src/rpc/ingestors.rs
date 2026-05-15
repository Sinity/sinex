//! Operator-facing ingestor status RPC types.
//!
//! Mirrors `rpc::automata` for the source-side: every registered ingestor (and
//! source-worker source unit) manifest, joined to its latest run, latest
//! `health.status` event, and recent event-emission stats. Distinct from
//! `rpc::nodes` (which carries coordinator-style state — drain/resume/horizon).

use crate::domain::NodeName;
use crate::{Timestamp, Uuid};
use serde::{Deserialize, Serialize};

fn default_stale_after_secs() -> u64 {
    300
}

fn default_recent_window_secs() -> u64 {
    300
}

/// Request: `ingestors.status`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestorsStatusRequest {
    /// Heartbeats older than this make the ingestor non-live.
    #[serde(default = "default_stale_after_secs")]
    pub stale_after_secs: u64,
    /// Window used for recent-event-count context.
    #[serde(default = "default_recent_window_secs")]
    pub recent_window_secs: u64,
}

impl Default for IngestorsStatusRequest {
    fn default() -> Self {
        Self {
            stale_after_secs: default_stale_after_secs(),
            recent_window_secs: default_recent_window_secs(),
        }
    }
}

/// Response: `ingestors.status`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestorsStatusResponse {
    pub generated_at: Timestamp,
    pub stale_after_secs: u64,
    pub recent_window_secs: u64,
    pub ingestors: Vec<IngestorStatus>,
}

/// Operator-visible state for one registered ingestor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestorStatus {
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
    pub source_run_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<Timestamp>,
    /// Current health from the latest `health.status` event for this component.
    /// `None` if the ingestor has never emitted a transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_health: Option<String>,
    /// When the current health was last emitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_changed_at: Option<Timestamp>,
    /// Reason text from the most recent health transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_reason: Option<String>,
    /// Count of events emitted by this ingestor inside the recent window.
    pub recent_output_count: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_output_at: Option<Timestamp>,
}
