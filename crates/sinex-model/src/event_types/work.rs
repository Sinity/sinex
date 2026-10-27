//! Development tools Sinex observes but does not own: AgentCTL's job log.
//! (Beads issues are `task.revised` events, beside the other task lists.)
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One line of AgentCTL's event log: a job queued, started or finished, or
/// something else AgentCTL recorded (a pool paused under memory pressure, a
/// campaign's step). The line is its place in the log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "agentctl.event", key = location, replace = repeat, time = at, quality = intrinsic)]
pub struct AgentctlEvent {
    /// What the line is about, as AgentCTL names it (`queue-task`,
    /// `declared-operation`, `backpressure`, `campaign`, ...).
    pub record_kind: String,
    /// `queued`, `started`, `finished`, `succeeded`, `failed`, ...
    pub phase: Option<String>,
    pub job_id: Option<String>,
    pub project: Option<String>,
    pub operation: Option<String>,
    pub pool: Option<String>,
    pub attempt: Option<u64>,
    /// How a finished job ended (`success`, `failed`, `timeout`, `cancelled`).
    pub outcome: Option<String>,
    pub exit_code: Option<i64>,
    /// The checkout the job ran in.
    pub working_directory: Option<String>,
    /// The checkout's HEAD when the job started and when it ended, as
    /// AgentCTL recorded them.
    pub start_head: Option<String>,
    pub end_head: Option<String>,
    /// When AgentCTL wrote the line (`emitted_at`, or `completed_at` in the
    /// oldest lines).
    pub at: Option<Timestamp>,
    /// The line's other fields. The execution receipt and evidence (file
    /// manifests of the checkout) are left in the stored line.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}
