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

impl EventsProcessedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default() -> Self {
        Self {
            time_range_seconds: 60,
            total_events: 0,
            events_per_source: HashMap::new(),
            events_per_type: HashMap::new(),
            processing_rate: 0.0,
        }
    }
    
    /// Builder-style method for time range
    pub fn with_time_range_seconds(mut self, seconds: u64) -> Self {
        self.time_range_seconds = seconds;
        self
    }
    
    /// Builder-style method for total events
    pub fn with_total_events(mut self, count: u64) -> Self {
        self.total_events = count;
        self
    }
    
    /// Builder-style method for events per source
    pub fn with_events_per_source(mut self, events: HashMap<String, u64>) -> Self {
        self.events_per_source = events;
        self
    }
    
    /// Builder-style method for events per type
    pub fn with_events_per_type(mut self, events: HashMap<String, u64>) -> Self {
        self.events_per_type = events;
        self
    }
    
    /// Builder-style method for processing rate
    pub fn with_processing_rate(mut self, rate: f64) -> Self {
        self.processing_rate = rate;
        self
    }
}

impl ErrorsSummaryPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default() -> Self {
        Self {
            time_range_seconds: 60,
            total_errors: 0,
            errors_by_severity: HashMap::new(),
            errors_by_component: HashMap::new(),
            error_rate: 0.0,
        }
    }
    
    /// Builder-style method for time range
    pub fn with_time_range_seconds(mut self, seconds: u64) -> Self {
        self.time_range_seconds = seconds;
        self
    }
    
    /// Builder-style method for total errors
    pub fn with_total_errors(mut self, count: u64) -> Self {
        self.total_errors = count;
        self
    }
    
    /// Builder-style method for errors by severity
    pub fn with_errors_by_severity(mut self, errors: HashMap<String, u64>) -> Self {
        self.errors_by_severity = errors;
        self
    }
    
    /// Builder-style method for errors by component
    pub fn with_errors_by_component(mut self, errors: HashMap<String, u64>) -> Self {
        self.errors_by_component = errors;
        self
    }
    
    /// Builder-style method for error rate
    pub fn with_error_rate(mut self, rate: f64) -> Self {
        self.error_rate = rate;
        self
    }
}

impl SystemResourcesPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default() -> Self {
        Self {
            cpu_usage_percent: 0.0,
            memory_usage_bytes: 0,
            memory_total_bytes: 1024 * 1024 * 1024, // 1GB
            disk_usage_bytes: 0,
            disk_total_bytes: 100 * 1024 * 1024 * 1024, // 100GB
            open_file_descriptors: 0,
            network_bytes_sent: 0,
            network_bytes_received: 0,
        }
    }
    
    /// Builder-style method for CPU usage
    pub fn with_cpu_usage_percent(mut self, percent: f64) -> Self {
        self.cpu_usage_percent = percent;
        self
    }
    
    /// Builder-style method for memory usage
    pub fn with_memory_usage_bytes(mut self, bytes: u64) -> Self {
        self.memory_usage_bytes = bytes;
        self
    }
    
    /// Builder-style method for memory total
    pub fn with_memory_total_bytes(mut self, bytes: u64) -> Self {
        self.memory_total_bytes = bytes;
        self
    }
    
    /// Builder-style method for disk usage
    pub fn with_disk_usage_bytes(mut self, bytes: u64) -> Self {
        self.disk_usage_bytes = bytes;
        self
    }
    
    /// Builder-style method for disk total
    pub fn with_disk_total_bytes(mut self, bytes: u64) -> Self {
        self.disk_total_bytes = bytes;
        self
    }
    
    /// Builder-style method for open file descriptors
    pub fn with_open_file_descriptors(mut self, count: u64) -> Self {
        self.open_file_descriptors = count;
        self
    }
    
    /// Builder-style method for network bytes sent
    pub fn with_network_bytes_sent(mut self, bytes: u64) -> Self {
        self.network_bytes_sent = bytes;
        self
    }
    
    /// Builder-style method for network bytes received
    pub fn with_network_bytes_received(mut self, bytes: u64) -> Self {
        self.network_bytes_received = bytes;
        self
    }
}

impl OperationPerformancePayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(operation_name: impl Into<String>) -> Self {
        Self {
            operation_name: operation_name.into(),
            duration_ms: 0,
            items_processed: 0,
            success: true,
            error: None,
            metrics: HashMap::new(),
        }
    }
    
    /// Builder-style method for duration
    pub fn with_duration_ms(mut self, duration: u64) -> Self {
        self.duration_ms = duration;
        self
    }
    
    /// Builder-style method for items processed
    pub fn with_items_processed(mut self, count: u64) -> Self {
        self.items_processed = count;
        self
    }
    
    /// Builder-style method for success
    pub fn with_success(mut self, success: bool) -> Self {
        self.success = success;
        self
    }
    
    /// Builder-style method for error
    pub fn with_error(mut self, error: impl Into<String>) -> Self {
        self.error = Some(error.into());
        self
    }
    
    /// Builder-style method for metrics
    pub fn with_metrics(mut self, metrics: HashMap<String, serde_json::Value>) -> Self {
        self.metrics = metrics;
        self
    }
}

impl ComponentResourceUsagePayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(component: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            period_seconds: 60,
            memory_mb: serde_json::json!({"current": 0, "avg": 0, "peak": 0}),
            cpu_percent: serde_json::json!({"avg": 0.0, "peak": 0.0}),
        }
    }
    
    /// Builder-style method for period
    pub fn with_period_seconds(mut self, seconds: u64) -> Self {
        self.period_seconds = seconds;
        self
    }
    
    /// Builder-style method for memory metrics
    pub fn with_memory_mb(mut self, memory: serde_json::Value) -> Self {
        self.memory_mb = memory;
        self
    }
    
    /// Builder-style method for CPU metrics
    pub fn with_cpu_percent(mut self, cpu: serde_json::Value) -> Self {
        self.cpu_percent = cpu;
        self
    }
}
