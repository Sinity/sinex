use std::hint::black_box;
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::Value;
use sinex_core::types::events::payloads::process::ProcessStatus;
use sinex_satellite_sdk::heartbeat::{HeartbeatEmitter, HeartbeatLogSink};
use sinex_test_utils::{sinex_test, TestContext};

#[derive(Default, Debug)]
struct RecordingSink {
    entries: Mutex<Vec<Value>>,
}

impl HeartbeatLogSink for RecordingSink {
    fn emit(&self, entry: &Value) {
        self.entries.lock().push(entry.clone());
    }
}

fn emitter_with_sink(service: &str) -> (HeartbeatEmitter, Arc<RecordingSink>) {
    let sink = Arc::new(RecordingSink::default());
    let emitter = HeartbeatEmitter::new(service.to_string(), 1).with_log_sink(sink.clone());
    (emitter, sink)
}

#[sinex_test]
async fn heartbeat_metrics_capture_real_cpu_usage() -> color_eyre::Result<()> {
    let emitter = HeartbeatEmitter::new("test-heartbeat-service".to_string(), 1);

    // Generate some CPU activity so any real implementation would register usage.
    let mut accumulator: u64 = 0;
    for i in 0..1_000_000u64 {
        accumulator = accumulator.wrapping_add(i);
    }
    black_box(accumulator);

    let metrics = emitter.create_heartbeat_metrics(None);

    assert!(
        metrics.cpu_usage_percent > 0.0,
        "Heartbeat metrics should report actual CPU usage instead of the current hard-coded 0.0 value"
    );

    Ok(())
}

#[sinex_test]
async fn heartbeat_status_transitions_on_error_volume() -> color_eyre::Result<()> {
    let emitter = HeartbeatEmitter::new("test-heartbeat-service".to_string(), 1);

    for _ in 0..60 {
        emitter.record_error("simulated failure");
    }

    let metrics = emitter.create_heartbeat_metrics(None);

    assert_eq!(metrics.status, ProcessStatus::Failed, "Heartbeat status should transition to failed after repeated errors so operators can alert on degraded satellites");

    Ok(())
}

#[sinex_test]
async fn heartbeat_emits_degraded_alert_on_error_spike() -> color_eyre::Result<()> {
    let (emitter, sink) = emitter_with_sink("degraded-service");

    for _ in 0..15 {
        emitter.record_error("temporary failure");
    }

    emitter.emit_heartbeat(None);

    let entries = sink.entries.lock();
    assert_eq!(
        entries.len(),
        2,
        "Heartbeat emission should log the heartbeat and a degraded alert"
    );
    assert_eq!(entries[0]["fields"]["event_type"], "satellite.heartbeat");
    assert_eq!(entries[1]["fields"]["event_type"], "process.degraded");

    Ok(())
}

#[sinex_test]
async fn heartbeat_emits_failed_alert_only_on_transition() -> color_eyre::Result<()> {
    let (emitter, sink) = emitter_with_sink("failed-service");

    for _ in 0..80 {
        emitter.record_error("catastrophic failure");
    }

    emitter.emit_heartbeat(None);
    emitter.emit_heartbeat(None);

    let entries = sink.entries.lock();
    let failed_alerts = entries
        .iter()
        .filter(|entry| entry["fields"]["event_type"] == "process.failed")
        .count();

    assert_eq!(
        failed_alerts, 1,
        "Process failed alert should only fire on the first transition to failed"
    );

    Ok(())
}
