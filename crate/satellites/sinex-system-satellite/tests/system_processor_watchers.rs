use sinex_core::{Event, JsonValue, Provenance};
use sinex_satellite_sdk::{
    stream_processor::{Checkpoint, ProcessorInitContext, ScanArgs, TimeHorizon},
    StatefulStreamProcessor,
};
use sinex_system_satellite::{SystemConfig, SystemProcessor};
use sinex_test_utils::{satellite_runtime::TestRuntimeBuilder, sinex_test, TestContext};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;

#[sinex_test]
async fn system_processor_still_lacks_watchers(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let runtime = TestRuntimeBuilder::new(&ctx, "system-satellite-watchers")
        .with_dry_run(true)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let init_ctx = ProcessorInitContext::new(
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
    rx: &mut UnboundedReceiver<Event<JsonValue>>,
    expected: &str,
) -> color_eyre::Result<()> {
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
            return Ok(());
        }
        attempts += 1;
    }
    Err(color_eyre::eyre::eyre!(
        "system processor produced events but never emitted {}",
        expected
    ))
}

#[sinex_test]
async fn system_processor_still_uses_synthetic_provenance(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "system-satellite-provenance")
        .with_dry_run(false)
        .build()
        .await?;

    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let init_ctx = ProcessorInitContext::new(
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
        .expect("system processor should emit a snapshot heartbeat event");

    assert!(
        matches!(emitted.provenance, Provenance::Material { .. }),
        "System processor should emit events with real material provenance instead of the hard-coded synthesis ULID"
    );

    Ok(())
}

#[sinex_test]
async fn dbus_watcher_should_emit_signal_events(ctx: TestContext) -> color_eyre::Result<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "system-dbus-watchers")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let mut config = SystemConfig::default();
    config.journal_enabled = false;
    config.udev_enabled = false;
    config.systemd_enabled = false;

    let init_ctx = ProcessorInitContext::new(config, raw_config, service_info, handles, work_dir);

    let mut processor = SystemProcessor::new();
    processor.initialize(init_ctx).await?;

    processor
        .scan(
            Checkpoint::stream("system-dbus", None),
            TimeHorizon::Continuous,
            ScanArgs::default(),
        )
        .await?;

    expect_event_type(&mut runtime.event_rx, "system.dbus.signal").await
}

#[sinex_test]
async fn journal_watcher_should_emit_entry_events(ctx: TestContext) -> color_eyre::Result<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "system-journal-watchers")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let mut config = SystemConfig::default();
    config.dbus_enabled = false;
    config.udev_enabled = false;
    config.systemd_enabled = false;

    let init_ctx = ProcessorInitContext::new(config, raw_config, service_info, handles, work_dir);

    let mut processor = SystemProcessor::new();

    processor.initialize(init_ctx).await?;

    processor
        .scan(
            Checkpoint::stream("system-journal", None),
            TimeHorizon::Continuous,
            ScanArgs::default(),
        )
        .await?;

    expect_event_type(&mut runtime.event_rx, "system.journal.entry").await
}

#[sinex_test]
async fn udev_watcher_should_emit_device_events(ctx: TestContext) -> color_eyre::Result<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "system-udev-watchers")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let mut config = SystemConfig::default();
    config.dbus_enabled = false;
    config.journal_enabled = false;
    config.systemd_enabled = false;

    let init_ctx = ProcessorInitContext::new(config, raw_config, service_info, handles, work_dir);

    let mut processor = SystemProcessor::new();
    processor.initialize(init_ctx).await?;

    processor
        .scan(
            Checkpoint::stream("system-udev", None),
            TimeHorizon::Continuous,
            ScanArgs::default(),
        )
        .await?;

    expect_event_type(&mut runtime.event_rx, "system.udev.device").await
}

#[sinex_test]
async fn systemd_watcher_should_emit_unit_events(ctx: TestContext) -> color_eyre::Result<()> {
    let mut runtime = TestRuntimeBuilder::new(&ctx, "system-systemd-watchers")
        .with_dry_run(false)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let mut config = SystemConfig::default();
    config.dbus_enabled = false;
    config.journal_enabled = false;
    config.udev_enabled = false;

    let init_ctx = ProcessorInitContext::new(config, raw_config, service_info, handles, work_dir);

    let mut processor = SystemProcessor::new();
    processor.initialize(init_ctx).await?;

    processor
        .scan(
            Checkpoint::stream("system-systemd", None),
            TimeHorizon::Continuous,
            ScanArgs::default(),
        )
        .await?;

    expect_event_type(&mut runtime.event_rx, "system.systemd.unit_state").await
}
