use sinex_core::types::Seconds;
use sinex_node_sdk::emit_heartbeat;
use sinex_node_sdk::heartbeat::HeartbeatEmitter;
use sinex_test_utils::sinex_test;

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
fn emit_heartbeat_macro_compiles() -> TestResult<()> {
    emit_heartbeat!("test-service");
    emit_heartbeat!("test-service", events_processed = 5, status = "healthy");
    Ok(())
}
