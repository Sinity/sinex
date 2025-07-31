//! Telemetry and metrics event payloads

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "events.processed")]
pub struct EventsProcessedPayload {
    pub time_range_seconds: u64,
    pub total_events: u64,
    pub events_per_source: HashMap<String, u64>,
    pub events_per_type: HashMap<String, u64>,
    pub processing_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "errors.summary")]
pub struct ErrorsSummaryPayload {
    pub time_range_seconds: u64,
    pub total_errors: u64,
    pub errors_by_severity: HashMap<String, u64>,
    pub errors_by_component: HashMap<String, u64>,
    pub error_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "system.resources")]
pub struct SystemResourcesPayload {
    pub cpu_usage_percent: f64,
    pub memory_usage_bytes: u64,
    pub memory_total_bytes: u64,
    pub disk_usage_bytes: u64,
    pub disk_total_bytes: u64,
    pub open_file_descriptors: u64,
    pub network_bytes_sent: u64,
    pub network_bytes_received: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "operation.performance")]
pub struct OperationPerformancePayload {
    pub operation_name: String,
    pub duration_ms: u64,
    pub items_processed: u64,
    pub success: bool,
    pub error: Option<String>,
    pub metrics: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "sinex", event_type = "resource.usage")]
pub struct ComponentResourceUsagePayload {
    pub component: String,
    pub period_seconds: u64,
    pub memory_mb: serde_json::Value, // Object with current, avg, peak
    pub cpu_percent: serde_json::Value, // Object with avg, peak
}
