use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_system_config_defaults() -> TestResult<()> {
    let config = sinex_system_ingestor::SystemConfig::default();

    assert_eq!(config.dbus_enabled, true);
    assert_eq!(config.journal_enabled, true);
    assert_eq!(config.udev_enabled, true);
    assert_eq!(config.systemd_enabled, true);
    assert_eq!(config.dbus_buses, sinex_system_ingestor::DbusBusScope::Both);
    assert_eq!(config.journal_timeout_secs.as_secs(), 5);

    Ok(())
}

#[sinex_test]
async fn test_watcher_snapshot_all_ready_true() -> TestResult<()> {
    let snapshot = sinex_system_ingestor::WatcherSnapshot {
        dbus_ready: true,
        journal_ready: true,
        udev_ready: true,
        systemd_ready: true,
    };

    assert!(snapshot.all_ready());

    Ok(())
}

#[sinex_test]
async fn test_watcher_snapshot_one_false() -> TestResult<()> {
    let snapshot = sinex_system_ingestor::WatcherSnapshot {
        dbus_ready: false,
        journal_ready: true,
        udev_ready: true,
        systemd_ready: true,
    };

    assert!(!snapshot.all_ready());

    Ok(())
}

#[sinex_test]
async fn test_watcher_snapshot_all_false() -> TestResult<()> {
    let snapshot = sinex_system_ingestor::WatcherSnapshot {
        dbus_ready: false,
        journal_ready: false,
        udev_ready: false,
        systemd_ready: false,
    };

    assert!(!snapshot.all_ready());

    Ok(())
}

#[sinex_test]
async fn test_watcher_snapshot_multiple_false() -> TestResult<()> {
    let snapshot = sinex_system_ingestor::WatcherSnapshot {
        dbus_ready: true,
        journal_ready: false,
        udev_ready: false,
        systemd_ready: true,
    };

    assert!(!snapshot.all_ready());

    Ok(())
}

#[sinex_test]
async fn test_dbus_config_defaults() -> TestResult<()> {
    let config = sinex_system_ingestor::DbusConfig::default();

    assert_eq!(config.monitor_session, true);
    assert_eq!(config.monitor_system, true);
    assert_eq!(config.include_interfaces.len(), 0);
    assert_eq!(config.exclude_interfaces.len(), 3); // DBus, Introspectable, Peer
    assert_eq!(config.extract_notifications, true);
    assert_eq!(config.extract_media, true);
    assert_eq!(config.extract_power, true);
    assert_eq!(config.extract_hardware, true);
    assert_eq!(config.extract_session, true);
    assert_eq!(config.extract_bluetooth, true);
    assert_eq!(config.extract_network, true);
    assert_eq!(config.extract_mounts, true);
    assert_eq!(config.health_check_interval_secs.as_secs(), 5);
    assert_eq!(config.inactivity_timeout_secs.as_secs(), 30);

    Ok(())
}

#[sinex_test]
async fn test_journal_config_defaults() -> TestResult<()> {
    let config = sinex_system_ingestor::JournalConfig::default();

    assert_eq!(config.follow, true);
    assert_eq!(config.import_on_startup, true);
    assert_eq!(config.import_hours, 0);
    assert_eq!(config.units.len(), 0);
    assert_eq!(config.priorities.len(), 0);
    assert_eq!(config.include_kernel, true);
    assert_eq!(config.include_user, true);
    assert_eq!(config.exclude_fields.len(), 4);
    assert!(config.cursor_file.is_some());
    assert_eq!(config.batch_size, 1000);
    assert_eq!(config.cursor_flush_event_threshold, 100);
    assert_eq!(config.cursor_flush_interval_secs.as_secs(), 10);

    Ok(())
}

#[sinex_test]
async fn test_systemd_config_defaults() -> TestResult<()> {
    let config = sinex_system_ingestor::SystemdConfig::default();

    assert_eq!(config.monitor_services, true);
    assert_eq!(config.monitor_timers, true);
    assert_eq!(config.monitor_all_units, false);
    assert_eq!(config.monitor_timeout_secs.as_secs(), 5);

    Ok(())
}
