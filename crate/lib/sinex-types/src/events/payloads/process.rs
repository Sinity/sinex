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

impl ProcessStartedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(process_name: impl Into<String>) -> Self {
        Self {
            process_name: process_name.into(),
            process_type: "satellite".to_string(),
            pid: 1234,
            version: "1.0.0".to_string(),
            config: serde_json::json!({}),
        }
    }

    /// Builder-style method for process type
    pub fn with_process_type(mut self, process_type: impl Into<String>) -> Self {
        self.process_type = process_type.into();
        self
    }

    /// Builder-style method for PID
    pub fn with_pid(mut self, pid: u32) -> Self {
        self.pid = pid;
        self
    }

    /// Builder-style method for version
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// Builder-style method for config
    pub fn with_config(mut self, config: serde_json::Value) -> Self {
        self.config = config;
        self
    }
}

impl ProcessHeartbeatPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            sequence: 1,
            status: "healthy".to_string(),
            metrics: None,
        }
    }

    /// Builder-style method for sequence
    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = sequence;
        self
    }

    /// Builder-style method for status
    pub fn with_status(mut self, status: impl Into<String>) -> Self {
        self.status = status.into();
        self
    }

    /// Builder-style method for metrics
    pub fn with_metrics(mut self, metrics: serde_json::Value) -> Self {
        self.metrics = Some(metrics);
        self
    }
}

impl ProcessShutdownPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(process_name: impl Into<String>) -> Self {
        Self {
            process_name: process_name.into(),
            process_type: "satellite".to_string(),
            pid: 1234,
            uptime_seconds: 0,
            shutdown_reason: "normal".to_string(),
            exit_code: 0,
        }
    }

    /// Builder-style method for process type
    pub fn with_process_type(mut self, process_type: impl Into<String>) -> Self {
        self.process_type = process_type.into();
        self
    }

    /// Builder-style method for PID
    pub fn with_pid(mut self, pid: u32) -> Self {
        self.pid = pid;
        self
    }

    /// Builder-style method for uptime
    pub fn with_uptime_seconds(mut self, uptime: u64) -> Self {
        self.uptime_seconds = uptime;
        self
    }

    /// Builder-style method for shutdown reason
    pub fn with_shutdown_reason(mut self, reason: impl Into<String>) -> Self {
        self.shutdown_reason = reason.into();
        self
    }

    /// Builder-style method for exit code
    pub fn with_exit_code(mut self, code: i32) -> Self {
        self.exit_code = code;
        self
    }
}

impl AutomatonErrorPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(
        automaton_name: impl Into<String>,
        error_message: impl Into<String>,
    ) -> Self {
        Self {
            automaton_name: automaton_name.into(),
            error_message: error_message.into(),
            error_code: None,
            stack_trace: None,
            context: None,
        }
    }

    /// Builder-style method for error code
    pub fn with_error_code(mut self, code: impl Into<String>) -> Self {
        self.error_code = Some(code.into());
        self
    }

    /// Builder-style method for stack trace
    pub fn with_stack_trace(mut self, trace: impl Into<String>) -> Self {
        self.stack_trace = Some(trace.into());
        self
    }

    /// Builder-style method for context
    pub fn with_context(mut self, context: serde_json::Value) -> Self {
        self.context = Some(context);
        self
    }
}

impl SensorActivatedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(sensor: impl Into<String>, satellite: impl Into<String>) -> Self {
        Self {
            sensor: sensor.into(),
            satellite: satellite.into(),
            activation_time: Utc::now(),
        }
    }

    /// Builder-style method for activation time
    pub fn with_activation_time(mut self, time: DateTime<Utc>) -> Self {
        self.activation_time = time;
        self
    }
}

impl SensorDeactivatedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(sensor: impl Into<String>, satellite: impl Into<String>) -> Self {
        Self {
            sensor: sensor.into(),
            satellite: satellite.into(),
            uptime_seconds: 0,
            events_generated: 0,
            reason: "normal".to_string(),
        }
    }

    /// Builder-style method for uptime
    pub fn with_uptime_seconds(mut self, uptime: u64) -> Self {
        self.uptime_seconds = uptime;
        self
    }

    /// Builder-style method for events generated
    pub fn with_events_generated(mut self, count: u64) -> Self {
        self.events_generated = count;
        self
    }

    /// Builder-style method for reason
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = reason.into();
        self
    }
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
