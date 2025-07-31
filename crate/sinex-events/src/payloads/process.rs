//! Process lifecycle event payloads

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "process.started")]
pub struct ProcessStartedPayload {
    pub process_name: String,
    pub process_type: String, // satellite, automaton, service
    pub pid: u32,
    pub version: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "process.heartbeat")]
pub struct ProcessHeartbeatPayload {
    /// Name of the service/process emitting the heartbeat
    pub source: String,
    /// Sequence number of this heartbeat (increments each emission)
    pub sequence: u64,
    /// Status of the process - should probably be an enum
    pub status: String, // TODO: Make this an enum (healthy, warning, error)
    /// Optional metrics collected from MetricsProviders
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "process.shutdown")]
pub struct ProcessShutdownPayload {
    pub process_name: String,
    pub process_type: String,
    pub pid: u32,
    pub uptime_seconds: u64,
    pub shutdown_reason: String,
    pub exit_code: i32,
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

// Sensor lifecycle

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "sensor.activated")]
pub struct SensorActivatedPayload {
    pub sensor: String,
    pub satellite: String,
    pub activation_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "sensor.deactivated")]
pub struct SensorDeactivatedPayload {
    pub sensor: String,
    pub satellite: String,
    pub uptime_seconds: u64,
    pub events_generated: u64,
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventPayload;

    #[test]
    fn test_event_payload_constants() {
        // Verify that the EventPayload trait is implemented
        use crate::EventPayload;
        assert_eq!(ProcessHeartbeatPayload::SOURCE.as_str(), "sinex");
        assert_eq!(
            ProcessHeartbeatPayload::EVENT_TYPE.as_str(),
            "process.heartbeat"
        );
    }
}
