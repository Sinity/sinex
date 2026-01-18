use sinex_core::types::events::SystemMonitoringStartedPayload;
use sinex_core::{Event, JsonValue, Provenance, Ulid};
use sinex_node_sdk::{
    stream_processor::{Checkpoint, NodeInitContext, ScanArgs, TimeHorizon},
    Node,
};
use sinex_system_ingestor::{SystemConfig, SystemProcessor};
use sinex_test_utils::{node_runtime::TestRuntimeBuilder, sinex_test, TestContext, TestResult};
use std::time::Duration;
use tokio::sync::mpsc::Receiver;

#[sinex_test]
async fn system_processor_initializes_watchers_on_snapshot(ctx: TestContext) -> TestResult<()> {
    let runtime = TestRuntimeBuilder::new(&ctx, "system-ingestor-watchers")
        .with_dry_run(true)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let init_ctx = NodeInitContext::new(
        SystemConfig::default(),
        raw_config,
        service_info,
        handles,
        work_dir,
    );

    let mut processor = SystemProcessor::new();
    processor.initialize(init_ctx).await?;

    processor
        .scan(
            Checkpoint::stream("system-test", None),
            TimeHorizon::Snapshot,
            ScanArgs::default(),
        )
        .await?;

    let snapshot = processor.watcher_snapshot();
    assert!(
        snapshot.dbus_ready,
        "System processor should wire the D-Bus watcher before scan runs"
    );
    assert!(
        snapshot.journal_ready,
        "System processor should initialize the journal watcher so events can stream"
    );
    assert!(
        snapshot.udev_ready,
        "System processor should expose the udev watcher as ready once the subsystem is wired"
    );
    assert!(
        snapshot.systemd_ready,
        "System processor should initialize the systemd watcher to capture unit events"
    );

    Ok(())
}

async fn expect_event_type(
    rx: &mut Receiver<Event<JsonValue>>,
    expected: &str,
) -> TestResult<Event<JsonValue>> {
    let mut attempts = 0;
    while attempts < 4 {
        let event = tokio::time::timeout(Duration::from_millis(250), rx.recv())
            .await
            .map_err(|_| {
                color_eyre::eyre::eyre!(
                    "system processor failed to emit {} within the allotted time",
                    expected
                )
            })?
            .ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "system processor closed event channel before emitting {}",
                    expected
                )
            })?;
        if event.event_type.as_str() == expected {
            return Ok(event);
        }
        attempts += 1;
    }
    Err(color_eyre::eyre::eyre!(
        "system processor produced events but never emitted {}",
        expected
    ))
}

async fn run_monitoring_case(
    ctx: &TestContext,
    label: &str,
    config: SystemConfig,
) -> TestResult<SystemMonitoringStartedPayload> {
    let service_name = format!("system-monitoring-{}-{}", label, Ulid::new());
    let mut runtime = TestRuntimeBuilder::new(ctx, service_name)
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let init_ctx = NodeInitContext::new(config, raw_config, service_info, handles, work_dir);

    let mut processor = SystemProcessor::new();
    processor.initialize(init_ctx).await?;

    processor
        .scan(
            Checkpoint::stream("system-monitoring", None),
            TimeHorizon::Continuous,
            ScanArgs::default(),
        )
        .await?;

    let event = expect_event_type(&mut runtime.event_rx, "monitoring.started").await?;
    let payload = serde_json::from_value(event.payload)?;
    Ok(payload)
}

#[sinex_test]
async fn system_processor_emits_material_provenance(ctx: TestContext) -> TestResult<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "system-ingestor-provenance")
        .with_dry_run(false)
        .build()
        .await?;

    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let init_ctx = NodeInitContext::new(
        SystemConfig::default(),
        raw_config,
        service_info,
        handles,
        work_dir,
    );

    let mut processor = SystemProcessor::new();
    processor.initialize(init_ctx).await?;

    processor
        .scan(
            Checkpoint::stream("system-provenance", None),
            TimeHorizon::Snapshot,
            ScanArgs::default(),
        )
        .await?;

    let emitted = tokio::time::timeout(std::time::Duration::from_secs(1), runtime.event_rx.recv())
        .await?
        .expect("system processor should emit a snapshot event");

    assert!(
        matches!(emitted.provenance, Provenance::Material { .. }),
        "System processor should emit events with real material provenance"
    );

    Ok(())
}

#[sinex_test]
async fn system_processor_emits_monitoring_started_flags(ctx: TestContext) -> TestResult<()> {
    let cases = [
        ("dbus-only", true, false, false, false),
        ("journal-only", false, true, false, false),
        ("udev-only", false, false, true, false),
        ("systemd-only", false, false, false, true),
        ("all", true, true, true, true),
    ];

    for (label, dbus, journal, udev, systemd) in cases {
        let mut config = SystemConfig::default();
        config.dbus_enabled = dbus;
        config.journal_enabled = journal;
        config.udev_enabled = udev;
        config.systemd_enabled = systemd;

        let payload = run_monitoring_case(&ctx, label, config).await?;

        assert_eq!(payload.dbus_enabled, dbus, "{label} dbus flag mismatch");
        assert_eq!(
            payload.journal_enabled, journal,
            "{label} journal flag mismatch"
        );
        assert_eq!(payload.udev_enabled, udev, "{label} udev flag mismatch");
        assert_eq!(
            payload.systemd_enabled, systemd,
            "{label} systemd flag mismatch"
        );
    }

    Ok(())
}
