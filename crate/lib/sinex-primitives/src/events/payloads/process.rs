//! Process lifecycle event payloads

use crate::domain::NodeType;
use crate::events::enums::ShutdownReason;
use crate::units::{ExitCode, ProcessId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;
use std::fmt;

/// Strongly typed status for process heartbeat payloads
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProcessStatus {
    Healthy,
    Degraded,
    Failed,
}

impl fmt::Display for ProcessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            ProcessStatus::Healthy => "healthy",
            ProcessStatus::Degraded => "degraded",
            ProcessStatus::Failed => "failed",
        };

        f.write_str(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "process.started")]
pub struct ProcessStartedPayload {
    pub process_name: String,
    pub process_type: NodeType,
    pub pid: ProcessId,
    pub version: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "process.degraded")]
pub struct ProcessDegradedPayload {
    pub process_name: String,
    pub uptime_seconds: u64,
    pub errors_in_window: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "process.failed")]
pub struct ProcessFailedPayload {
    pub process_name: String,
    pub uptime_seconds: u64,
    pub errors_in_window: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "process.shutdown")]
pub struct ProcessShutdownPayload {
    pub process_name: String,
    pub process_type: NodeType,
    pub pid: ProcessId,
    pub uptime_seconds: u64,
    pub shutdown_reason: ShutdownReason,
    pub exit_code: ExitCode,
}

// Automaton error events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "automaton.error")]
pub struct AutomatonErrorPayload {
    pub automaton_name: String,
    pub error_message: String,
    pub error_code: Option<String>,
    pub stack_trace: Option<String>,
    pub context: Option<serde_json::Value>,
}

// Test helpers for external tests
#[cfg(any(test, feature = "testing"))]
impl ProcessStartedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            process_name: "test-process".into(),
            process_type: NodeType::Ingestor,
            pid: ProcessId::from(0u32),
            version: "0.0.0".into(),
            config: serde_json::json!({}),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl ProcessShutdownPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            process_name: "test-process".into(),
            process_type: NodeType::Ingestor,
            pid: ProcessId::from(0u32),
            uptime_seconds: 0,
            shutdown_reason: ShutdownReason::Requested,
            exit_code: ExitCode::SUCCESS,
        }
    }
}
