use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_system_config_defaults() -> TestResult<()> {
    let config = sinex_system_ingestor::SystemConfig::default();

    assert!(config.dbus_enabled);
    assert!(config.journal_enabled);
    assert!(config.udev_enabled);
    assert!(config.systemd_enabled);
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

    assert!(config.monitor_session);
    assert!(config.monitor_system);
    assert_eq!(config.include_interfaces.len(), 0);
    assert_eq!(config.exclude_interfaces.len(), 2); // Introspectable, Peer
    assert!(config.extract_notifications);
    assert!(config.extract_media);
    assert!(config.extract_power);
    assert!(config.extract_hardware);
    assert!(!config.extract_session);
    assert!(config.extract_bluetooth);
    assert!(config.extract_network);
    assert!(config.extract_mounts);
    assert_eq!(config.health_check_interval_secs.as_secs(), 5);
    assert_eq!(config.inactivity_timeout_secs.as_secs(), 30);

    Ok(())
}

#[sinex_test]
async fn test_journal_config_defaults() -> TestResult<()> {
    let config = sinex_system_ingestor::JournalConfig::default();

    assert!(config.follow);
    assert_eq!(config.import_hours, 24);
    assert_eq!(config.units.len(), 0);
    assert_eq!(config.priorities.len(), 0);
    assert!(config.include_kernel);
    assert!(config.include_user);
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

    assert!(config.monitor_services);
    assert!(config.monitor_timers);
    assert!(!config.monitor_all_units);
    assert_eq!(config.monitor_timeout_secs.as_secs(), 5);

    Ok(())
}
