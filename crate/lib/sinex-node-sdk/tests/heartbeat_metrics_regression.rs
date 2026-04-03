use std::hint::black_box;
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::Value;
use sinex_node_sdk::heartbeat::{HeartbeatEmitter, HeartbeatLogSink};
use sinex_primitives::Seconds;
use sinex_primitives::events::payloads::process::ProcessStatus;
use xtask::sandbox::prelude::*;

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
    let emitter = HeartbeatEmitter::new(service.to_string(), Seconds::from_secs(1))
        .with_log_sink(sink.clone());
    (emitter, sink)
}

#[sinex_test]
async fn heartbeat_metrics_capture_real_cpu_usage() -> color_eyre::Result<()> {
    let emitter =
        HeartbeatEmitter::new("test-heartbeat-service".to_string(), Seconds::from_secs(1));

    // Generate some CPU activity so any real implementation would register usage.
    let mut accumulator: u64 = 0;
    for i in 0..1_000_000u64 {
        accumulator = accumulator.wrapping_add(i);
    }
    black_box(accumulator);

    let metrics = emitter.create_heartbeat_metrics(None).await;

    assert!(
        metrics.cpu_usage_percent > 0.0,
        "Heartbeat metrics should report actual CPU usage instead of the current hard-coded 0.0 value"
    );

    Ok(())
}

#[sinex_test]
async fn heartbeat_status_transitions_on_error_volume() -> color_eyre::Result<()> {
    let emitter =
        HeartbeatEmitter::new("test-heartbeat-service".to_string(), Seconds::from_secs(1));

    for _ in 0..60 {
        emitter.record_error("simulated failure");
    }

    let metrics = emitter.create_heartbeat_metrics(None).await;

    assert_eq!(
        metrics.status,
        ProcessStatus::Failed,
        "Heartbeat status should transition to failed after repeated errors so operators can alert on degraded nodes"
    );

    Ok(())
}

#[sinex_test]
async fn heartbeat_status_transitions_at_exact_thresholds() -> color_eyre::Result<()> {
    let degraded = HeartbeatEmitter::new("threshold-degraded".to_string(), Seconds::from_secs(1));
    for _ in 0..10 {
        degraded.record_error("threshold failure");
    }
    let degraded_metrics = degraded.create_heartbeat_metrics(None).await;
    assert_eq!(
        degraded_metrics.status,
        ProcessStatus::Degraded,
        "Exact degraded threshold should transition to degraded instead of requiring one extra error"
    );

    let failed = HeartbeatEmitter::new("threshold-failed".to_string(), Seconds::from_secs(1));
    for _ in 0..50 {
        failed.record_error("threshold failure");
    }
    let failed_metrics = failed.create_heartbeat_metrics(None).await;
    assert_eq!(
        failed_metrics.status,
        ProcessStatus::Failed,
        "Exact failed threshold should transition to failed instead of requiring one extra error"
    );

    Ok(())
}

#[sinex_test]
async fn heartbeat_emits_degraded_alert_on_error_spike() -> color_eyre::Result<()> {
    let (emitter, sink) = emitter_with_sink("degraded-service");

    for _ in 0..15 {
        emitter.record_error("temporary failure");
    }

    emitter.emit_heartbeat(None).await;

    let entries = sink.entries.lock();
    assert_eq!(
        entries.len(),
        2,
        "Heartbeat emission should log the heartbeat and a degraded alert"
    );
    assert_eq!(entries[0]["fields"]["event_type"], "node.heartbeat");
    assert_eq!(entries[1]["fields"]["event_type"], "process.degraded");

    Ok(())
}

#[sinex_test]
async fn heartbeat_alert_uses_window_error_count() -> color_eyre::Result<()> {
    let (emitter, sink) = emitter_with_sink("window-count-service");

    for _ in 0..5 {
        emitter.record_error("first burst");
    }
    emitter.emit_heartbeat(None).await;

    for _ in 0..6 {
        emitter.record_error("second burst");
    }
    emitter.emit_heartbeat(None).await;

    let entries = sink.entries.lock();
    let degraded_entry = entries
        .iter()
        .find(|entry| entry["fields"]["event_type"] == "process.degraded")
        .expect("degraded transition alert should be emitted");

    assert_eq!(
        degraded_entry["fields"]["errors_count"],
        serde_json::json!(11),
        "Alert metadata should report the full sliding-window error count that triggered the transition"
    );
    assert_eq!(
        degraded_entry["fields"]["payload"]["errors_in_window"],
        serde_json::json!(11),
        "Alert payload should agree with the sliding-window error count used for status transitions"
    );

    Ok(())
}

#[sinex_test]
async fn heartbeat_emits_failed_alert_only_on_transition() -> color_eyre::Result<()> {
    let (emitter, sink) = emitter_with_sink("failed-service");

    for _ in 0..80 {
        emitter.record_error("catastrophic failure");
    }

    emitter.emit_heartbeat(None).await;
    emitter.emit_heartbeat(None).await;

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
