//! Wave B production-path obligation tests for system source units.
//!
//! Source units covered:
//! - `system.journald`
//! - `system.systemd`
//! - `system.dbus`
//! - `system.udev`
//! - `system.monitor` (no parser — lifecycle event, not dispatch-driven)

use xtask::sandbox::prelude::*;

// ---------------------------------------------------------------------------
// Fixture constants
// ---------------------------------------------------------------------------

/// A minimal journald JSON line that produces a `journald.entry.written` event.
const JOURNAL_FIXTURE: &[u8] = br#"{"__CURSOR":"s=abc;i=1;b=x;m=y;t=z;x=w","__REALTIME_TIMESTAMP":"1700000000000000","MESSAGE":"test log entry","PRIORITY":"6","_HOSTNAME":"sinnix-prime"}"#;

/// A journald line with `_SYSTEMD_UNIT` that produces a `systemd.unit.started` event.
const SYSTEMD_FIXTURE: &[u8] = br#"{"__CURSOR":"s=abc;i=2;b=x;m=y;t=z;x=w","__REALTIME_TIMESTAMP":"1700000001000000","_SYSTEMD_UNIT":"nginx.service","MESSAGE":"Started A Web Server.","PRIORITY":"6"}"#;

/// A D-Bus JSON record with interface + member in metadata (signal.received).
const DBUS_FIXTURE: &[u8] = br#"{"key":"value"}"#;

/// A udev path payload (UTF-8 device path).
const UDEV_FIXTURE: &[u8] = b"/sys/bus/usb/devices/1-1";

/// A desktop-notification JSON record (the shape `NotificationParser` expects from
/// the D-Bus `Notify` stream): app_name, summary, body, urgency, timeout, …
const DESKTOP_NOTIFICATION_FIXTURE: &[u8] = br#"{"app_name":"sinex-tests","summary":"Build complete","body":"All checks passed","urgency":1,"timeout":-1,"actions":[],"hints":{}}"#;

// ---------------------------------------------------------------------------
// system.journald
// ---------------------------------------------------------------------------

#[sinex_test]
async fn test_system_journald_initial_ingestion() -> TestResult<()> {
    super::obligations::initial_ingestion::run(
        "system.journald",
        super::AdapterKind::Journal,
        JOURNAL_FIXTURE,
        &["entry.written"],
    )
    .await
    .map_err(|e| color_eyre::eyre::eyre!("{e}"))
}

// ---------------------------------------------------------------------------
// system.systemd
// ---------------------------------------------------------------------------

#[sinex_test]
async fn test_system_systemd_initial_ingestion() -> TestResult<()> {
    super::obligations::initial_ingestion::run(
        "system.systemd",
        super::AdapterKind::Journal,
        SYSTEMD_FIXTURE,
        &["unit.started"],
    )
    .await
    .map_err(|e| color_eyre::eyre::eyre!("{e}"))
}

// ---------------------------------------------------------------------------
// system.dbus
// ---------------------------------------------------------------------------

#[sinex_test]
async fn test_system_dbus_initial_ingestion() -> TestResult<()> {
    // The dbus parser reads interface/member from record metadata. In dispatch
    // mode, record metadata is populated from the adapter. For the bare-bytes
    // dispatch path, we feed the raw body bytes; the parser falls back to
    // signal.received for records without metadata fields.
    super::obligations::initial_ingestion::run(
        "system.dbus",
        super::AdapterKind::Dbus,
        DBUS_FIXTURE,
        &["signal.received"],
    )
    .await
    .map_err(|e| color_eyre::eyre::eyre!("{e}"))
}

// ---------------------------------------------------------------------------
// desktop.notification (D-Bus Notify stream → NotificationParser)
// ---------------------------------------------------------------------------

#[sinex_test]
async fn test_desktop_notification_initial_ingestion() -> TestResult<()> {
    super::obligations::initial_ingestion::run(
        "desktop.notification",
        super::AdapterKind::Dbus,
        DESKTOP_NOTIFICATION_FIXTURE,
        &["notification.sent"],
    )
    .await
    .map_err(|e| color_eyre::eyre::eyre!("{e}"))
}

// ---------------------------------------------------------------------------
// system.udev
// ---------------------------------------------------------------------------

#[sinex_test]
async fn test_system_udev_initial_ingestion() -> TestResult<()> {
    // The bare-path fixture carries no add/remove/change action metadata, so the
    // parser classifies it as `device.other` rather than guessing a connect (see
    // UdevParser: "emitting Other action instead of guessing kind"). Real udev
    // events carry an ACTION and classify as connected/disconnected — covered by
    // the unit tests in `sources/source_units/system/udev.rs`.
    super::obligations::initial_ingestion::run(
        "system.udev",
        super::AdapterKind::FileDrop,
        UDEV_FIXTURE,
        &["device.other"],
    )
    .await
    .map_err(|e| color_eyre::eyre::eyre!("{e}"))
}

// ---------------------------------------------------------------------------
// system.monitor — parser-less: verified via payload construction only
// ---------------------------------------------------------------------------

#[sinex_test]
async fn test_system_monitor_descriptor_registered() -> TestResult<()> {
    // system.monitor is a fire-once lifecycle event, not dispatch-driven.
    // Verify the source unit descriptor is in the inventory.
    use sinex_primitives::parser::SourceUnitId;
    use sinexd::sources::registry::SourceUnitRegistry;

    let registry = SourceUnitRegistry::from_inventory();
    let id = SourceUnitId::new("system.monitor").unwrap();
    let descriptor = registry.find(&id);

    assert!(
        descriptor.is_some(),
        "system.monitor descriptor must be registered in inventory"
    );

    let d = descriptor.unwrap();
    assert_eq!(d.id, "system.monitor");
    assert_eq!(d.namespace, "system");

    Ok(())
}

// Verify the node factory for system.monitor is registered.
#[sinex_test]
async fn test_system_monitor_factory_registered() -> TestResult<()> {
    use sinex_primitives::parser::SourceUnitId;
    use sinexd::sources::node_factory::find_node_factory;

    let id = SourceUnitId::new("system.monitor").unwrap();
    let factory = find_node_factory(&id);

    assert!(
        factory.is_some(),
        "system.monitor must have a node factory registered"
    );

    Ok(())
}
