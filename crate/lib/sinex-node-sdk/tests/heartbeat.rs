use sinex_node_sdk::emit_heartbeat;
use sinex_node_sdk::heartbeat::HeartbeatEmitter;
use sinex_primitives::Seconds;
use xtask::sandbox::prelude::*;

struct ScopedEnvGuard {
    keys: Vec<(String, Option<String>)>,
}

impl ScopedEnvGuard {
    fn new(keys: &[&str]) -> Self {
        let previous = keys
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        Self { keys: previous }
    }

    fn set(&mut self, key: &str, value: &str) {
        unsafe { std::env::set_var(key, value) };
    }
}

impl Drop for ScopedEnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.keys.drain(..) {
            unsafe {
                match value {
                    Some(val) => std::env::set_var(key, val),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

#[sinex_test]
async fn heartbeat_emitter_tracks_metadata() -> TestResult<()> {
    let emitter = HeartbeatEmitter::new("test-service".to_string(), Seconds::from_secs(30));
    assert_eq!(emitter.service_name(), "test-service");
    assert_eq!(emitter.interval_seconds(), Seconds::from_secs(30));
    Ok(())
}

#[sinex_test]
async fn counter_handle_updates_metrics() -> TestResult<()> {
    let emitter = HeartbeatEmitter::new("test-service".to_string(), Seconds::from_secs(30));
    let handle = emitter.get_counter_handle();

    handle.increment_events_processed(5);
    handle.record_error("test error");

    assert_eq!(handle.get_events_processed(), 5);
    assert_eq!(handle.get_errors_count(), 1);
    Ok(())
}

#[sinex_test]
async fn heartbeat_metrics_include_latest_state() -> TestResult<()> {
    let emitter = HeartbeatEmitter::new("test-service".to_string(), Seconds::from_secs(30));
    emitter.increment_events_processed(10);
    emitter.record_error("test error");

    let metrics = emitter.create_heartbeat_metrics(None).await;
    assert_eq!(metrics.service_name, "test-service");
    assert_eq!(metrics.errors_count, 1);
    assert!(metrics.last_error_message.is_some());
    Ok(())
}

#[sinex_test]
async fn emit_heartbeat_macro_compiles() -> TestResult<()> {
    emit_heartbeat!("test-service");
    emit_heartbeat!("test-service", events_processed = 5, status = "healthy");
    Ok(())
}

#[sinex_test]
async fn heartbeat_invalid_threshold_overrides_fall_back_to_defaults() -> TestResult<()> {
    let mut env = ScopedEnvGuard::new(&[
        "SINEX_HEARTBEAT_DEGRADED_THRESHOLD",
        "SINEX_HEARTBEAT_FAILED_THRESHOLD",
    ]);
    env.set("SINEX_HEARTBEAT_DEGRADED_THRESHOLD", "bogus");
    env.set("SINEX_HEARTBEAT_FAILED_THRESHOLD", "bogus");

    let emitter = HeartbeatEmitter::new("test-service".to_string(), Seconds::from_secs(30));
    for _ in 0..11 {
        emitter.record_error("test error");
    }

    let metrics = emitter.create_heartbeat_metrics(None).await;
    assert_eq!(metrics.status, sinex_primitives::events::payloads::process::ProcessStatus::Degraded);
    Ok(())
}
