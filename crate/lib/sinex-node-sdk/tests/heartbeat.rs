use sinex_node_sdk::emit_heartbeat;
use sinex_node_sdk::heartbeat::HeartbeatEmitter;
use sinex_db::DbPoolExt;
use sinex_primitives::{domain::{NodeName, NodeType}, Seconds};
use xtask::sandbox::prelude::*;

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
    let mut env = EnvGuard::new();
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

#[sinex_test]
async fn heartbeat_emitter_persists_manifest_heartbeat(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let node_name = NodeName::new("test-service");
    pool.state()
        .register_node(&node_name, NodeType::Service, env!("CARGO_PKG_VERSION"), Some("test"))
        .await?;

    let emitter = HeartbeatEmitter::new("test-service".to_string(), Seconds::from_secs(30))
        .with_node_name(node_name.clone())
        .with_db_pool(pool.clone());
    emitter.emit_heartbeat(None).await;

    let manifest = pool
        .state()
        .get_nodes_by_type(NodeType::Service)
        .await?
        .into_iter()
        .find(|manifest| manifest.node_name == node_name)
        .expect("registered node manifest should exist");

    assert_eq!(manifest.status, "active");
    assert!(
        manifest.last_heartbeat_at.is_some(),
        "heartbeat emission should persist last_heartbeat_at"
    );
    Ok(())
}
