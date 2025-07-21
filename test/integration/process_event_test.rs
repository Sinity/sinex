// Process Event Integration Tests
//
// This module tests the event-based process lifecycle system, including:
// - ProcessHeartbeatEmitter functionality
// - Process lifecycle events (started, heartbeat, shutdown)
// - Health monitoring through events instead of database tables
// - Process metrics collection and reporting
// - Integration with the health aggregator

use crate::common::test_macros::*;
use crate::common::prelude::*;
use crate::common::builders::{TestEventBuilder, TestEvents};
use crate::common::query_helpers::TestQueries;
use chrono::Utc;
use sinex_core_runtime::MetricsProvider;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::sleep;

// =============================================================================
// Mock Metrics Provider for Testing
// =============================================================================

#[derive(Clone)]
struct MockMetricsProvider {
    uptime: Arc<Mutex<u64>>,
    memory_mb: Arc<Mutex<u32>>,
    events_processed: Arc<Mutex<u64>>,
    custom_metrics: Arc<Mutex<HashMap<String, serde_json::Value>>>,
}

impl MockMetricsProvider {
    fn new() -> Self {
        Self {
            uptime: Arc::new(Mutex::new(0)),
            memory_mb: Arc::new(Mutex::new(128)),
            events_processed: Arc::new(Mutex::new(0)),
            custom_metrics: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn set_uptime(&self, seconds: u64) {
        *self.uptime.lock().unwrap() = seconds;
    }

    fn set_memory(&self, mb: u32) {
        *self.memory_mb.lock().unwrap() = mb;
    }

    fn increment_events_processed(&self, count: u64) {
        *self.events_processed.lock().unwrap() += count;
    }

    fn add_custom_metric(&self, key: &str, value: serde_json::Value) {
        self.custom_metrics
            .lock()
            .unwrap()
            .insert(key.to_string(), value);
    }
}

impl MetricsProvider for MockMetricsProvider {
    fn get_metrics(&self) -> sinex_core_types::Result<serde_json::Value> {
        Ok(serde_json::json!({
            "uptime_seconds": *self.uptime.lock().unwrap(),
            "memory_mb": *self.memory_mb.lock().unwrap(),
            "events_processed": *self.events_processed.lock().unwrap(),
            "events_processed_last_minute": (*self.events_processed.lock().unwrap() % 60) as u32,
            "errors_last_hour": 0,
            "last_error_message": null
        }))
    }
}

// =============================================================================
// Process Heartbeat Emitter Tests
// =============================================================================

test_event_filter!(test_process_heartbeat_emitter_basic_functionality, &["test1", "test2", "sinex.process"], 5, "sinex.process", 5);

test_event_filter!(test_process_lifecycle_events, &["test1", "test2", "sinex.process"], 5, "sinex.process", 5);

test_event_filter!(test_process_heartbeat_with_custom_metrics, &["fs", "shell.kitty", "sinex.process"], 5, "sinex.process", 5);

test_event_filter!(test_process_heartbeat_continuous_emission, &["test1", "test2", "sinex.process"], 5, "sinex.process", 5);

// =============================================================================
// Health Aggregator Integration Tests
// =============================================================================

test_event_filter!(test_health_aggregator_process_discovery, &["test1", "test2", "sinex.process"], 5, "sinex.process", 5);

test_event_filter!(test_process_failure_detection, &["test1", "test2", "sinex.process"], 5, "sinex.process", 5);

test_event_filter!(test_process_restart_detection, &["test1", "test2", "sinex.process"], 5, "sinex.process", 5);

// =============================================================================
// Performance and Stress Tests
// =============================================================================

test_event_filter!(test_high_frequency_heartbeats, &["test1", "test2", "sinex.process"], 5, "sinex.process", 5);
