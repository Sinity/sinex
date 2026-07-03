use super::*;
use crate::runtime::EventTransport;
use crate::runtime::stream::{EventEmitter, RuntimeHandles, ServiceInfo};
use crate::runtime::{CheckpointManager, NatsPublisher};
use sinex_primitives::domain::HostName;
use sinex_primitives::events::DynamicPayload;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use xtask::sandbox::prelude::*;

/// Verify MonitorPhase variants are Debug + Clone.
#[sinex_test]
async fn test_monitor_phase_clone_all_variants() -> TestResult<()> {
    let start = MonitorPhase::ServiceStart;
    let interval = MonitorPhase::PerInterval {
        period: Duration::from_mins(1),
    };
    let shutdown = MonitorPhase::ServiceShutdown;

    assert!(matches!(start.clone(), MonitorPhase::ServiceStart));
    assert!(
        matches!(interval.clone(), MonitorPhase::PerInterval { period } if period == Duration::from_mins(1))
    );
    assert!(matches!(shutdown.clone(), MonitorPhase::ServiceShutdown));

    Ok(())
}

/// Verify MonitorDriver errors cleanly if `run_continuous` is called
/// without a prior `initialize()`.
#[sinex_test]
async fn test_monitor_driver_missing_runtime_errors() -> TestResult<()> {
    fn noop_emit(
        _runtime: RuntimeContext,
        _material_id: Id<SourceMaterial>,
    ) -> futures::future::BoxFuture<'static, RuntimeResult<Vec<Event<JsonValue>>>> {
        Box::pin(async { Ok(vec![]) })
    }

    let mut source = MonitorDriver::new("test.monitor", MonitorPhase::ServiceStart, noop_emit);

    // run_continuous without prior initialize() should return Err.
    let (_tx, rx) = watch::channel(false);
    let mut state = MonitorState::default();
    let start = ContinuousStart::from_checkpoint(Checkpoint::None);
    let result = source.run_continuous(&mut state, start, rx).await;

    assert!(result.is_err(), "expected Err when runtime not captured");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("runtime not captured"),
        "unexpected error message: {err}"
    );

    Ok(())
}

/// Verify that a MonitorDriver with a noop emit function reflects the
/// correct capabilities: continuous only, no snapshot/historical.
#[sinex_test]
async fn test_monitor_driver_capabilities() -> TestResult<()> {
    fn noop_emit(
        _runtime: RuntimeContext,
        _material_id: Id<SourceMaterial>,
    ) -> futures::future::BoxFuture<'static, RuntimeResult<Vec<Event<JsonValue>>>> {
        Box::pin(async { Ok(vec![]) })
    }

    let source = MonitorDriver::new("test.monitor", MonitorPhase::ServiceStart, noop_emit);
    let caps = source.capabilities();

    assert!(!caps.supports_snapshot, "monitors have no snapshot mode");
    assert!(
        !caps.supports_historical,
        "monitors have no historical mode"
    );
    assert!(caps.supports_continuous, "monitors run in continuous mode");
    assert!(
        caps.manages_own_continuous_loop,
        "monitors manage their own loop"
    );

    Ok(())
}

#[sinex_test]
async fn monitor_fire_once_opens_material_and_emits_event(ctx: TestContext) -> TestResult<()> {
    fn emit_test_monitor(
        _runtime: RuntimeContext,
        material_id: Id<SourceMaterial>,
    ) -> futures::future::BoxFuture<'static, RuntimeResult<Vec<Event<JsonValue>>>> {
        Box::pin(async move {
            let event = DynamicPayload::new(
                "monitor.test",
                "monitor.test.started",
                serde_json::json!({ "ok": true }),
            )
            .from_material(material_id)
            .build()?;
            Ok(vec![event])
        })
    }

    let ctx = ctx.with_nats().shared().await?;
    let (runtime, mut events) = make_monitor_runtime(&ctx).await?;

    fire_monitor_once("test.monitor", emit_test_monitor, &runtime).await?;

    let event = events
        .recv()
        .await
        .ok_or_else(|| SinexError::processing("monitor event channel closed"))?;
    assert_eq!(event.source.as_str(), "monitor.test");
    assert_eq!(event.event_type.as_str(), "monitor.test.started");
    assert!(
        matches!(
            event.provenance,
            sinex_primitives::events::Provenance::Material { .. }
        ),
        "monitor events must use material provenance"
    );
    assert_eq!(event.payload["ok"], true);
    Ok(())
}

#[sinex_test]
async fn service_start_monitor_drain_wait_stays_resident_until_signal() -> TestResult<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut shutdown_rx = shutdown_rx;

    let mut handle =
        tokio::spawn(
            async move { wait_for_monitor_drain("test.monitor", &mut shutdown_rx).await },
        );

    tokio::select! {
        result = &mut handle => {
            result?;
            panic!("ServiceStart monitor returned before drain");
        }
        () = tokio::time::sleep(Duration::from_millis(50)) => {}
    }

    shutdown_tx
        .send(true)
        .map_err(|_| SinexError::processing("failed to signal monitor drain"))?;
    tokio::time::timeout(Duration::from_secs(1), handle).await??;
    Ok(())
}

async fn make_monitor_runtime(
    ctx: &TestContext,
) -> TestResult<(RuntimeContext, mpsc::Receiver<Event<JsonValue>>)> {
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        kv,
        "monitor-fire-once-test".to_string(),
        "test-group".to_string(),
        format!("test-consumer-{}", Uuid::now_v7().simple()),
    ));
    let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
    let emitter = EventEmitter::new(event_sender, false);
    let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
    let handles = RuntimeHandles::new_edge(
        checkpoint_manager,
        emitter,
        EventTransport::Nats(publisher),
        None,
    );
    let work_dir = tempfile::tempdir()?;
    let work_dir_path = work_dir.keep();
    let work_dir_utf8 =
        camino::Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
            SinexError::validation("temporary work dir should be utf-8")
                .with_context("path", path.display().to_string())
        })?;

    Ok((
        RuntimeContext::new(
            ServiceInfo::new_with_runtime_identity(
                "monitor-fire-once-test".to_string(),
                "test.monitor".to_string(),
                Some("test.monitor".to_string()),
                Some("hosted source binding".to_string()),
                HostName::from_static("test-host"),
                work_dir_path,
                false,
                format!("instance-{}", Uuid::now_v7().simple()),
                env!("CARGO_PKG_VERSION").to_string(),
                None,
            ),
            handles,
            HashMap::new(),
            work_dir_utf8,
        ),
        event_receiver,
    ))
}
