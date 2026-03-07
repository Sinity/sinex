//! Watcher logic tests — pure data-mapping and conversion functions.
//!
//! Tests the publicly accessible conversion logic, enum mappings, config
//! validation, and payload construction without requiring real D-Bus,
//! journal, udev, or systemd connections.

use serde_json::json;
use std::time::{Duration, Instant};
use xtask::sandbox::sinex_test;

// ============================================================================
// SystemdUnitType (payloads.rs) — from_unit_name
// ============================================================================

#[sinex_test]
async fn test_systemd_unit_type_from_service_name() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitType;

    let ut = SystemdUnitType::from_unit_name("nginx.service");
    assert_eq!(ut, SystemdUnitType::Service);
    Ok(())
}

#[sinex_test]
async fn test_systemd_unit_type_from_timer_name() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitType;

    let ut = SystemdUnitType::from_unit_name("logrotate.timer");
    assert_eq!(ut, SystemdUnitType::Timer);
    Ok(())
}

#[sinex_test]
async fn test_systemd_unit_type_from_socket_name() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitType;

    let ut = SystemdUnitType::from_unit_name("sshd.socket");
    assert_eq!(ut, SystemdUnitType::Socket);
    Ok(())
}

#[sinex_test]
async fn test_systemd_unit_type_from_target_name() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitType;

    let ut = SystemdUnitType::from_unit_name("multi-user.target");
    assert_eq!(ut, SystemdUnitType::Target);
    Ok(())
}

#[sinex_test]
async fn test_systemd_unit_type_from_mount_name() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitType;

    let ut = SystemdUnitType::from_unit_name("home.mount");
    assert_eq!(ut, SystemdUnitType::Mount);
    Ok(())
}

#[sinex_test]
async fn test_systemd_unit_type_from_unknown_suffix() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitType;

    let ut = SystemdUnitType::from_unit_name("something.swap");
    assert_eq!(ut, SystemdUnitType::Other);

    let ut2 = SystemdUnitType::from_unit_name("no-extension");
    assert_eq!(ut2, SystemdUnitType::Other);

    let ut3 = SystemdUnitType::from_unit_name("");
    assert_eq!(ut3, SystemdUnitType::Other);

    Ok(())
}

#[sinex_test]
async fn test_systemd_unit_type_display() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitType;

    assert_eq!(SystemdUnitType::Service.to_string(), "service");
    assert_eq!(SystemdUnitType::Timer.to_string(), "timer");
    assert_eq!(SystemdUnitType::Socket.to_string(), "socket");
    assert_eq!(SystemdUnitType::Target.to_string(), "target");
    assert_eq!(SystemdUnitType::Mount.to_string(), "mount");
    assert_eq!(SystemdUnitType::Other.to_string(), "other");
    Ok(())
}

// ============================================================================
// SystemdUnitState (payloads.rs) — from_status_string
// ============================================================================

#[sinex_test]
async fn test_systemd_unit_state_from_known_strings() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitState;

    assert_eq!(
        SystemdUnitState::from_status_string("active"),
        SystemdUnitState::Active
    );
    assert_eq!(
        SystemdUnitState::from_status_string("inactive"),
        SystemdUnitState::Inactive
    );
    assert_eq!(
        SystemdUnitState::from_status_string("failed"),
        SystemdUnitState::Failed
    );
    assert_eq!(
        SystemdUnitState::from_status_string("activating"),
        SystemdUnitState::Activating
    );
    assert_eq!(
        SystemdUnitState::from_status_string("deactivating"),
        SystemdUnitState::Deactivating
    );
    Ok(())
}

#[sinex_test]
async fn test_systemd_unit_state_unknown_maps_to_unknown() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitState;

    assert_eq!(
        SystemdUnitState::from_status_string("reloading"),
        SystemdUnitState::Unknown
    );
    assert_eq!(
        SystemdUnitState::from_status_string(""),
        SystemdUnitState::Unknown
    );
    assert_eq!(
        SystemdUnitState::from_status_string("garbage"),
        SystemdUnitState::Unknown
    );
    Ok(())
}

#[sinex_test]
async fn test_systemd_unit_state_display() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitState;

    assert_eq!(SystemdUnitState::Active.to_string(), "active");
    assert_eq!(SystemdUnitState::Inactive.to_string(), "inactive");
    assert_eq!(SystemdUnitState::Failed.to_string(), "failed");
    assert_eq!(SystemdUnitState::Activating.to_string(), "activating");
    assert_eq!(SystemdUnitState::Deactivating.to_string(), "deactivating");
    assert_eq!(SystemdUnitState::Unknown.to_string(), "unknown");
    Ok(())
}

#[sinex_test]
async fn test_systemd_unit_state_display_roundtrip() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitState;

    // from_status_string(display(x)) == x for known states
    for state in [
        SystemdUnitState::Active,
        SystemdUnitState::Inactive,
        SystemdUnitState::Failed,
        SystemdUnitState::Activating,
        SystemdUnitState::Deactivating,
    ] {
        let display_str = state.to_string();
        let roundtripped = SystemdUnitState::from_status_string(&display_str);
        assert_eq!(roundtripped, state, "roundtrip failed for {display_str}");
    }
    Ok(())
}

// ============================================================================
// systemd_integration::SystemdUnitType — broader from_name classification
// ============================================================================

#[sinex_test]
async fn test_integration_systemd_unit_type_extended_variants() -> TestResult<()> {
    use sinex_system_ingestor::systemd_integration::SystemdUnitType;

    assert_eq!(
        SystemdUnitType::from_name("nginx.service"),
        SystemdUnitType::Service
    );
    assert_eq!(
        SystemdUnitType::from_name("backup.timer"),
        SystemdUnitType::Timer
    );
    assert_eq!(
        SystemdUnitType::from_name("dbus.socket"),
        SystemdUnitType::Socket
    );
    assert_eq!(
        SystemdUnitType::from_name("graphical.target"),
        SystemdUnitType::Target
    );
    assert_eq!(
        SystemdUnitType::from_name("home.mount"),
        SystemdUnitType::Mount
    );
    assert_eq!(
        SystemdUnitType::from_name("home.automount"),
        SystemdUnitType::Mount
    );
    assert_eq!(
        SystemdUnitType::from_name("snd-pcm.device"),
        SystemdUnitType::Device
    );
    assert_eq!(
        SystemdUnitType::from_name("session-1.scope"),
        SystemdUnitType::Scope
    );
    assert_eq!(
        SystemdUnitType::from_name("user.slice"),
        SystemdUnitType::Slice
    );
    assert_eq!(
        SystemdUnitType::from_name("unknown_unit"),
        SystemdUnitType::Other
    );
    assert_eq!(SystemdUnitType::from_name(""), SystemdUnitType::Other);
    Ok(())
}

// ============================================================================
// DbusBusScope — enum logic
// ============================================================================

#[sinex_test]
async fn test_dbus_bus_scope_as_str() -> TestResult<()> {
    use sinex_system_ingestor::DbusBusScope;

    assert_eq!(DbusBusScope::Session.as_str(), "session");
    assert_eq!(DbusBusScope::System.as_str(), "system");
    assert_eq!(DbusBusScope::Both.as_str(), "both");
    Ok(())
}

#[sinex_test]
async fn test_dbus_bus_scope_bus_names() -> TestResult<()> {
    use sinex_system_ingestor::DbusBusScope;

    assert_eq!(DbusBusScope::Session.bus_names(), &["session"]);
    assert_eq!(DbusBusScope::System.bus_names(), &["system"]);
    assert_eq!(DbusBusScope::Both.bus_names(), &["session", "system"]);
    Ok(())
}

#[sinex_test]
async fn test_dbus_bus_scope_default_is_both() -> TestResult<()> {
    use sinex_system_ingestor::DbusBusScope;

    assert_eq!(DbusBusScope::default(), DbusBusScope::Both);
    Ok(())
}

#[sinex_test]
async fn test_dbus_bus_scope_display() -> TestResult<()> {
    use sinex_system_ingestor::DbusBusScope;

    assert_eq!(format!("{}", DbusBusScope::Session), "session");
    assert_eq!(format!("{}", DbusBusScope::System), "system");
    assert_eq!(format!("{}", DbusBusScope::Both), "both");
    Ok(())
}

#[sinex_test]
async fn test_dbus_bus_scope_serde_roundtrip() -> TestResult<()> {
    use sinex_system_ingestor::DbusBusScope;

    for scope in [
        DbusBusScope::Session,
        DbusBusScope::System,
        DbusBusScope::Both,
    ] {
        let json_str = serde_json::to_string(&scope)?;
        let deserialized: DbusBusScope = serde_json::from_str(&json_str)?;
        assert_eq!(deserialized, scope, "roundtrip failed for {scope}");
    }
    Ok(())
}

#[sinex_test]
async fn test_dbus_bus_scope_serde_rename_all_lowercase() -> TestResult<()> {
    use sinex_system_ingestor::DbusBusScope;

    // serde(rename_all = "lowercase") means JSON should be lowercase strings
    assert_eq!(
        serde_json::to_string(&DbusBusScope::Session)?,
        "\"session\""
    );
    assert_eq!(serde_json::to_string(&DbusBusScope::System)?, "\"system\"");
    assert_eq!(serde_json::to_string(&DbusBusScope::Both)?, "\"both\"");

    // Deserialization from lowercase strings
    assert_eq!(
        serde_json::from_str::<DbusBusScope>("\"session\"")?,
        DbusBusScope::Session
    );
    assert_eq!(
        serde_json::from_str::<DbusBusScope>("\"system\"")?,
        DbusBusScope::System
    );
    assert_eq!(
        serde_json::from_str::<DbusBusScope>("\"both\"")?,
        DbusBusScope::Both
    );
    Ok(())
}

// ============================================================================
// WatcherActivitySnapshot — health logic
// ============================================================================

#[sinex_test]
async fn test_watcher_activity_snapshot_defaults() -> TestResult<()> {
    use sinex_system_ingestor::WatcherActivitySnapshot;

    let snap = WatcherActivitySnapshot::new();
    assert!(!snap.active);
    assert!(snap.last_event.is_none());
    assert!(snap.last_error.is_none());
    assert_eq!(snap.events_processed, 0);
    Ok(())
}

#[sinex_test]
async fn test_watcher_activity_inactive_is_unhealthy() -> TestResult<()> {
    use sinex_system_ingestor::WatcherActivitySnapshot;

    let snap = WatcherActivitySnapshot {
        active: false,
        last_event: Some(Instant::now()),
        last_error: None,
        events_processed: 100,
    };
    assert!(!snap.is_healthy(60));
    Ok(())
}

#[sinex_test]
async fn test_watcher_activity_active_no_events_is_healthy() -> TestResult<()> {
    use sinex_system_ingestor::WatcherActivitySnapshot;

    // Active, no events yet: considered healthy (just started)
    let snap = WatcherActivitySnapshot {
        active: true,
        last_event: None,
        last_error: None,
        events_processed: 0,
    };
    assert!(snap.is_healthy(60));
    Ok(())
}

#[sinex_test]
async fn test_watcher_activity_recent_event_is_healthy() -> TestResult<()> {
    use sinex_system_ingestor::WatcherActivitySnapshot;

    let snap = WatcherActivitySnapshot {
        active: true,
        last_event: Some(Instant::now()),
        last_error: None,
        events_processed: 42,
    };
    assert!(snap.is_healthy(60));
    Ok(())
}

#[sinex_test]
async fn test_watcher_activity_stale_event_is_unhealthy() -> TestResult<()> {
    use sinex_system_ingestor::WatcherActivitySnapshot;

    // Last event was 120 seconds ago, idle threshold is 60s
    let stale_time = Instant::now()
        .checked_sub(Duration::from_mins(2))
        .expect("time subtraction");
    let snap = WatcherActivitySnapshot {
        active: true,
        last_event: Some(stale_time),
        last_error: None,
        events_processed: 42,
    };
    assert!(!snap.is_healthy(60));
    Ok(())
}

#[sinex_test]
async fn test_watcher_activity_boundary_idle_threshold() -> TestResult<()> {
    use sinex_system_ingestor::WatcherActivitySnapshot;

    // Event happened exactly at the boundary: elapsed == max_idle_secs
    // is_healthy checks elapsed < max_idle_secs, so elapsed == threshold is unhealthy
    let boundary_time = Instant::now()
        .checked_sub(Duration::from_secs(30))
        .expect("time subtraction");
    let snap = WatcherActivitySnapshot {
        active: true,
        last_event: Some(boundary_time),
        last_error: None,
        events_processed: 1,
    };
    // 30 elapsed vs threshold of 30: not < 30, so unhealthy
    assert!(!snap.is_healthy(30));

    // 30 elapsed vs threshold of 31: 30 < 31, healthy
    assert!(snap.is_healthy(31));
    Ok(())
}

// ============================================================================
// SystemdUnitType serde (payloads.rs)
// ============================================================================

#[sinex_test]
async fn test_payloads_systemd_unit_type_serde_roundtrip() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitType;

    for variant in [
        SystemdUnitType::Service,
        SystemdUnitType::Timer,
        SystemdUnitType::Socket,
        SystemdUnitType::Target,
        SystemdUnitType::Mount,
        SystemdUnitType::Other,
    ] {
        let json_str = serde_json::to_string(&variant)?;
        let deserialized: SystemdUnitType = serde_json::from_str(&json_str)?;
        assert_eq!(
            deserialized, variant,
            "serde roundtrip failed for {variant}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_payloads_systemd_unit_state_serde_roundtrip() -> TestResult<()> {
    use sinex_system_ingestor::SystemdUnitState;

    for variant in [
        SystemdUnitState::Active,
        SystemdUnitState::Inactive,
        SystemdUnitState::Failed,
        SystemdUnitState::Activating,
        SystemdUnitState::Deactivating,
        SystemdUnitState::Unknown,
    ] {
        let json_str = serde_json::to_string(&variant)?;
        let deserialized: SystemdUnitState = serde_json::from_str(&json_str)?;
        assert_eq!(
            deserialized, variant,
            "serde roundtrip failed for {variant}"
        );
    }
    Ok(())
}

// ============================================================================
// SystemConfig — combined config serde
// ============================================================================

#[sinex_test]
async fn test_system_config_serde_roundtrip() -> TestResult<()> {
    let config = sinex_system_ingestor::SystemConfig::default();

    let json_str = serde_json::to_string_pretty(&config)?;
    let deserialized: sinex_system_ingestor::SystemConfig = serde_json::from_str(&json_str)?;

    assert_eq!(deserialized.dbus_enabled, config.dbus_enabled);
    assert_eq!(deserialized.journal_enabled, config.journal_enabled);
    assert_eq!(deserialized.udev_enabled, config.udev_enabled);
    assert_eq!(deserialized.systemd_enabled, config.systemd_enabled);
    assert_eq!(deserialized.dbus_buses, config.dbus_buses);
    assert_eq!(
        deserialized.journal_timeout_secs.as_secs(),
        config.journal_timeout_secs.as_secs()
    );
    Ok(())
}

#[sinex_test]
async fn test_system_config_custom_values_roundtrip() -> TestResult<()> {
    use sinex_primitives::Seconds;
    use sinex_system_ingestor::DbusBusScope;

    let config = sinex_system_ingestor::SystemConfig {
        dbus_enabled: false,
        journal_enabled: true,
        udev_enabled: false,
        systemd_enabled: true,
        dbus_buses: DbusBusScope::Session,
        journal_timeout_secs: Seconds::from_secs(30),
        systemd_config: sinex_system_ingestor::SystemdConfig {
            monitor_services: false,
            monitor_timers: true,
            monitor_all_units: true,
            monitor_timeout_secs: Seconds::from_secs(10),
        },
        dbus_config: sinex_system_ingestor::DbusConfig::default(),
        journal_config: sinex_system_ingestor::JournalConfig::default(),
    };

    let json_str = serde_json::to_string(&config)?;
    let deserialized: sinex_system_ingestor::SystemConfig = serde_json::from_str(&json_str)?;

    assert!(!deserialized.dbus_enabled);
    assert!(!deserialized.udev_enabled);
    assert_eq!(deserialized.dbus_buses, DbusBusScope::Session);
    assert_eq!(deserialized.journal_timeout_secs.as_secs(), 30);
    assert!(!deserialized.systemd_config.monitor_services);
    assert!(deserialized.systemd_config.monitor_all_units);
    assert_eq!(
        deserialized.systemd_config.monitor_timeout_secs.as_secs(),
        10
    );
    Ok(())
}

// ============================================================================
// DbusConfig — interface filtering logic
// ============================================================================

#[sinex_test]
async fn test_dbus_config_default_excludes_noisy_interfaces() -> TestResult<()> {
    let config = sinex_system_ingestor::DbusConfig::default();

    // Three noisy interfaces excluded by default
    assert!(
        config
            .exclude_interfaces
            .contains(&"org.freedesktop.DBus.Properties".to_string())
    );
    assert!(
        config
            .exclude_interfaces
            .contains(&"org.freedesktop.DBus.Introspectable".to_string())
    );
    assert!(
        config
            .exclude_interfaces
            .contains(&"org.freedesktop.DBus.Peer".to_string())
    );
    Ok(())
}

#[sinex_test]
async fn test_dbus_config_serde_custom_filters() -> TestResult<()> {
    use sinex_primitives::Seconds;

    let config = sinex_system_ingestor::DbusConfig {
        monitor_session: true,
        monitor_system: false,
        include_interfaces: vec!["org.mpris.MediaPlayer2".to_string()],
        exclude_interfaces: vec![],
        extract_notifications: false,
        extract_media: true,
        extract_power: false,
        extract_hardware: false,
        extract_session: false,
        extract_bluetooth: false,
        extract_network: false,
        extract_mounts: false,
        health_check_interval_secs: Seconds::from_secs(10),
        inactivity_timeout_secs: Seconds::from_secs(60),
    };

    let json_str = serde_json::to_string(&config)?;
    let deserialized: sinex_system_ingestor::DbusConfig = serde_json::from_str(&json_str)?;

    assert!(deserialized.monitor_session);
    assert!(!deserialized.monitor_system);
    assert_eq!(deserialized.include_interfaces.len(), 1);
    assert_eq!(deserialized.include_interfaces[0], "org.mpris.MediaPlayer2");
    assert!(deserialized.exclude_interfaces.is_empty());
    assert!(!deserialized.extract_notifications);
    assert!(deserialized.extract_media);
    assert_eq!(deserialized.health_check_interval_secs.as_secs(), 10);
    assert_eq!(deserialized.inactivity_timeout_secs.as_secs(), 60);
    Ok(())
}

// ============================================================================
// JournalConfig — configuration logic
// ============================================================================

#[sinex_test]
async fn test_journal_config_default_excludes_internal_fields() -> TestResult<()> {
    let config = sinex_system_ingestor::JournalConfig::default();

    assert!(config.exclude_fields.contains(&"__CURSOR".to_string()));
    assert!(
        config
            .exclude_fields
            .contains(&"__REALTIME_TIMESTAMP".to_string())
    );
    assert!(
        config
            .exclude_fields
            .contains(&"__MONOTONIC_TIMESTAMP".to_string())
    );
    assert!(config.exclude_fields.contains(&"_TRANSPORT".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_journal_config_custom_units_and_priorities() -> TestResult<()> {
    use sinex_primitives::Seconds;

    let config = sinex_system_ingestor::JournalConfig {
        follow: false,
        import_on_startup: false,
        import_hours: 24,
        units: vec!["sshd.service".to_string(), "nginx.service".to_string()],
        priorities: vec![0, 1, 2, 3], // emergency through error
        include_kernel: false,
        include_user: false,
        exclude_fields: vec![],
        cursor_file: None,
        batch_size: 500,
        cursor_flush_event_threshold: 50,
        cursor_flush_interval_secs: Seconds::from_secs(5),
    };

    let json_str = serde_json::to_string(&config)?;
    let deserialized: sinex_system_ingestor::JournalConfig = serde_json::from_str(&json_str)?;

    assert!(!deserialized.follow);
    assert!(!deserialized.import_on_startup);
    assert_eq!(deserialized.import_hours, 24);
    assert_eq!(deserialized.units.len(), 2);
    assert_eq!(deserialized.priorities, vec![0, 1, 2, 3]);
    assert!(!deserialized.include_kernel);
    assert!(deserialized.cursor_file.is_none());
    assert_eq!(deserialized.batch_size, 500);
    assert_eq!(deserialized.cursor_flush_event_threshold, 50);
    Ok(())
}

// ============================================================================
// JournalEntryPayload — payload construction and serde
// ============================================================================

#[sinex_test]
async fn test_journal_entry_payload_full_serde_roundtrip() -> TestResult<()> {
    use sinex_primitives::temporal::Timestamp;
    use std::collections::HashMap;

    let mut fields = HashMap::new();
    fields.insert("_BOOT_ID".to_string(), "abc123".to_string());
    fields.insert("_MACHINE_ID".to_string(), "def456".to_string());

    let payload = sinex_system_ingestor::JournalEntryPayload {
        cursor: "s=abc;i=123;b=def;m=456;t=789;x=012".to_string(),
        timestamp_us: 1_700_000_000_000_000,
        timestamp: Timestamp::now(),
        hostname: Some("testhost".to_string()),
        unit: Some("nginx.service".to_string()),
        syslog_identifier: Some("nginx".to_string()),
        pid: Some(1234),
        uid: Some(0),
        gid: Some(0),
        cmdline: Some("/usr/sbin/nginx".to_string()),
        exe: Some("/usr/sbin/nginx".to_string()),
        unit_type: Some("service".to_string()),
        priority: Some(6), // informational
        facility: Some("daemon".to_string()),
        message: "worker process started".to_string(),
        fields,
    };

    let json_str = serde_json::to_string(&payload)?;
    let deserialized: sinex_system_ingestor::JournalEntryPayload = serde_json::from_str(&json_str)?;

    assert_eq!(deserialized.cursor, payload.cursor);
    assert_eq!(deserialized.timestamp_us, payload.timestamp_us);
    assert_eq!(deserialized.hostname.as_deref(), Some("testhost"));
    assert_eq!(deserialized.unit.as_deref(), Some("nginx.service"));
    assert_eq!(deserialized.syslog_identifier.as_deref(), Some("nginx"));
    assert_eq!(deserialized.pid, Some(1234));
    assert_eq!(deserialized.uid, Some(0));
    assert_eq!(deserialized.gid, Some(0));
    assert_eq!(deserialized.priority, Some(6));
    assert_eq!(deserialized.message, "worker process started");
    assert_eq!(
        deserialized.fields.get("_BOOT_ID").map(String::as_str),
        Some("abc123")
    );
    Ok(())
}

#[sinex_test]
async fn test_journal_entry_payload_minimal() -> TestResult<()> {
    use sinex_primitives::temporal::Timestamp;
    use std::collections::HashMap;

    // Minimal journal entry — only required fields
    let payload = sinex_system_ingestor::JournalEntryPayload {
        cursor: "s=1;i=2;b=3;m=4;t=5;x=6".to_string(),
        timestamp_us: 0,
        timestamp: Timestamp::now(),
        hostname: None,
        unit: None,
        syslog_identifier: None,
        pid: None,
        uid: None,
        gid: None,
        cmdline: None,
        exe: None,
        unit_type: None,
        priority: None,
        facility: None,
        message: String::new(),
        fields: HashMap::new(),
    };

    let json_str = serde_json::to_string(&payload)?;
    let deserialized: sinex_system_ingestor::JournalEntryPayload = serde_json::from_str(&json_str)?;

    assert!(deserialized.hostname.is_none());
    assert!(deserialized.unit.is_none());
    assert!(deserialized.pid.is_none());
    assert!(deserialized.priority.is_none());
    assert!(deserialized.message.is_empty());
    assert!(deserialized.fields.is_empty());
    Ok(())
}

// ============================================================================
// JournalSyncPayload — sync tracking serde
// ============================================================================

#[sinex_test]
async fn test_journal_sync_payload_serde_roundtrip() -> TestResult<()> {
    use sinex_primitives::events::enums::JournalSyncType;
    use sinex_primitives::temporal::Timestamp;

    let payload = sinex_system_ingestor::JournalSyncPayload {
        sync_type: JournalSyncType::InitialImport,
        start_cursor: Some("s=a;i=1;b=b;m=m;t=t;x=x".to_string()),
        end_cursor: "s=z;i=9;b=b;m=m;t=t;x=x".to_string(),
        entries_count: 42,
        time_start: Some(Timestamp::now()),
        time_end: Some(Timestamp::now()),
        duration_ms: 1500,
    };

    let json_str = serde_json::to_string(&payload)?;
    let deserialized: sinex_system_ingestor::JournalSyncPayload = serde_json::from_str(&json_str)?;

    assert_eq!(deserialized.entries_count, 42);
    assert_eq!(deserialized.duration_ms, 1500);
    assert!(deserialized.start_cursor.is_some());
    Ok(())
}

// ============================================================================
// Local payload types — D-Bus payload serde
// ============================================================================

#[sinex_test]
async fn test_power_event_payload_serde() -> TestResult<()> {
    let payload = sinex_system_ingestor::PowerEventPayload {
        event_type: "PrepareForSleep".to_string(),
        details: json!({"going_to_sleep": true}),
        timestamp: "2024-01-15T12:00:00Z".to_string(),
    };

    let json_str = serde_json::to_string(&payload)?;
    let deserialized: sinex_system_ingestor::PowerEventPayload = serde_json::from_str(&json_str)?;

    assert_eq!(deserialized.event_type, "PrepareForSleep");
    assert_eq!(deserialized.details["going_to_sleep"], true);
    Ok(())
}

#[sinex_test]
async fn test_hardware_event_payload_serde() -> TestResult<()> {
    use std::collections::HashMap;

    let mut properties = HashMap::new();
    properties.insert("ID_VENDOR".to_string(), json!("SanDisk"));
    properties.insert("ID_MODEL".to_string(), json!("Ultra"));

    let payload = sinex_system_ingestor::HardwareEventPayload {
        device_type: "usb".to_string(),
        event_type: "added".to_string(),
        device_path: "/sys/devices/usb1/1-1".to_string(),
        device_name: Some("SanDisk Ultra".to_string()),
        vendor: Some("SanDisk".to_string()),
        model: Some("Ultra".to_string()),
        serial: Some("ABC123".to_string()),
        properties,
        timestamp: "2024-01-15T12:00:00Z".to_string(),
    };

    let json_str = serde_json::to_string(&payload)?;
    let deserialized: sinex_system_ingestor::HardwareEventPayload =
        serde_json::from_str(&json_str)?;

    assert_eq!(deserialized.device_type, "usb");
    assert_eq!(deserialized.event_type, "added");
    assert_eq!(deserialized.device_name.as_deref(), Some("SanDisk Ultra"));
    assert_eq!(deserialized.properties.len(), 2);
    Ok(())
}

#[sinex_test]
async fn test_session_event_payload_serde() -> TestResult<()> {
    let payload = sinex_system_ingestor::SessionEventPayload {
        event_type: "idle".to_string(),
        session_id: Some("session-1".to_string()),
        idle_time_ms: Some(300_000),
        timestamp: "2024-01-15T12:00:00Z".to_string(),
    };

    let json_str = serde_json::to_string(&payload)?;
    let deserialized: sinex_system_ingestor::SessionEventPayload = serde_json::from_str(&json_str)?;

    assert_eq!(deserialized.event_type, "idle");
    assert_eq!(deserialized.idle_time_ms, Some(300_000));
    Ok(())
}

#[sinex_test]
async fn test_bluetooth_event_payload_serde() -> TestResult<()> {
    let payload = sinex_system_ingestor::BluetoothEventPayload {
        event_type: "connected".to_string(),
        device_address: "AA:BB:CC:DD:EE:FF".to_string(),
        device_name: Some("AirPods Pro".to_string()),
        device_class: Some("audio".to_string()),
        rssi: Some(-45),
        connected: true,
        paired: true,
        trusted: true,
        timestamp: "2024-01-15T12:00:00Z".to_string(),
    };

    let json_str = serde_json::to_string(&payload)?;
    let deserialized: sinex_system_ingestor::BluetoothEventPayload =
        serde_json::from_str(&json_str)?;

    assert_eq!(deserialized.device_address, "AA:BB:CC:DD:EE:FF");
    assert!(deserialized.connected);
    assert!(deserialized.paired);
    assert_eq!(deserialized.rssi, Some(-45));
    Ok(())
}

#[sinex_test]
async fn test_network_event_payload_serde() -> TestResult<()> {
    let payload = sinex_system_ingestor::NetworkEventPayload {
        event_type: "connected".to_string(),
        interface: "wlan0".to_string(),
        connection_type: "wifi".to_string(),
        ssid: Some("MyNetwork".to_string()),
        ip_address: Some("192.168.1.100".to_string()),
        state: "connected".to_string(),
        timestamp: "2024-01-15T12:00:00Z".to_string(),
    };

    let json_str = serde_json::to_string(&payload)?;
    let deserialized: sinex_system_ingestor::NetworkEventPayload = serde_json::from_str(&json_str)?;

    assert_eq!(deserialized.interface, "wlan0");
    assert_eq!(deserialized.connection_type, "wifi");
    assert_eq!(deserialized.ssid.as_deref(), Some("MyNetwork"));
    Ok(())
}

#[sinex_test]
async fn test_mount_event_payload_serde() -> TestResult<()> {
    let payload = sinex_system_ingestor::MountEventPayload {
        event_type: "mounted".to_string(),
        device: "/dev/sda1".to_string(),
        mount_point: "/mnt/usb".to_string(),
        filesystem: "ext4".to_string(),
        label: Some("USBDrive".to_string()),
        uuid: Some("1234-5678".to_string()),
        size_bytes: Some(128_000_000_000),
        timestamp: "2024-01-15T12:00:00Z".to_string(),
    };

    let json_str = serde_json::to_string(&payload)?;
    let deserialized: sinex_system_ingestor::MountEventPayload = serde_json::from_str(&json_str)?;

    assert_eq!(deserialized.device, "/dev/sda1");
    assert_eq!(deserialized.mount_point, "/mnt/usb");
    assert_eq!(deserialized.filesystem, "ext4");
    assert_eq!(deserialized.size_bytes, Some(128_000_000_000));
    Ok(())
}

#[sinex_test]
async fn test_dbus_method_call_payload_serde() -> TestResult<()> {
    let payload = sinex_system_ingestor::DbusMethodCallPayload {
        bus: "session".to_string(),
        sender: ":1.42".to_string(),
        destination: "org.freedesktop.Notifications".to_string(),
        path: "/org/freedesktop/Notifications".to_string(),
        interface: "org.freedesktop.Notifications".to_string(),
        method: "Notify".to_string(),
        args: json!(["app_name", 0, "", "Title", "Body", [], {}, -1]),
        timestamp: "2024-01-15T12:00:00Z".to_string(),
    };

    let json_str = serde_json::to_string(&payload)?;
    let deserialized: sinex_system_ingestor::DbusMethodCallPayload =
        serde_json::from_str(&json_str)?;

    assert_eq!(deserialized.method, "Notify");
    assert_eq!(deserialized.destination, "org.freedesktop.Notifications");
    Ok(())
}

// ============================================================================
// SystemdChange (systemd_integration.rs) — state change tracking
// ============================================================================

#[sinex_test]
async fn test_systemd_change_enum_debug() -> TestResult<()> {
    use sinex_system_ingestor::systemd_integration::{SystemdChange, SystemdUnitState};

    let change = SystemdChange::StateChanged {
        unit: "nginx.service".to_string(),
        old_state: SystemdUnitState::Active,
        new_state: SystemdUnitState::Failed,
    };

    let debug_str = format!("{change:?}");
    assert!(debug_str.contains("StateChanged"));
    assert!(debug_str.contains("nginx.service"));
    assert!(debug_str.contains("Active"));
    assert!(debug_str.contains("Failed"));

    let added = SystemdChange::UnitAdded {
        unit: "new.service".to_string(),
        state: SystemdUnitState::Active,
    };
    let debug_str = format!("{added:?}");
    assert!(debug_str.contains("UnitAdded"));
    assert!(debug_str.contains("new.service"));

    let removed = SystemdChange::UnitRemoved {
        unit: "old.service".to_string(),
    };
    let debug_str = format!("{removed:?}");
    assert!(debug_str.contains("UnitRemoved"));
    assert!(debug_str.contains("old.service"));

    Ok(())
}

// ============================================================================
// WatcherSnapshot — combined readiness
// ============================================================================

#[sinex_test]
async fn test_watcher_snapshot_partial_readiness() -> TestResult<()> {
    use sinex_system_ingestor::WatcherSnapshot;

    // Only journal and systemd ready
    let snap = WatcherSnapshot {
        dbus_ready: false,
        journal_ready: true,
        udev_ready: false,
        systemd_ready: true,
    };

    assert!(!snap.all_ready());
    assert!(snap.journal_ready);
    assert!(snap.systemd_ready);
    Ok(())
}

#[sinex_test]
async fn test_watcher_snapshot_equality() -> TestResult<()> {
    use sinex_system_ingestor::WatcherSnapshot;

    let a = WatcherSnapshot {
        dbus_ready: true,
        journal_ready: true,
        udev_ready: true,
        systemd_ready: true,
    };
    let b = WatcherSnapshot {
        dbus_ready: true,
        journal_ready: true,
        udev_ready: true,
        systemd_ready: true,
    };

    assert_eq!(a, b);

    let c = WatcherSnapshot {
        dbus_ready: false,
        journal_ready: true,
        udev_ready: true,
        systemd_ready: true,
    };
    assert_ne!(a, c);
    Ok(())
}

// ============================================================================
// Config from JSON — testing deserialization from raw JSON values
// ============================================================================

#[sinex_test]
async fn test_system_config_from_json_value() -> TestResult<()> {
    let json_val = json!({
        "dbus_enabled": false,
        "journal_enabled": true,
        "udev_enabled": false,
        "systemd_enabled": true,
        "dbus_buses": "session",
        "journal_timeout_secs": 15,
        "systemd_config": {
            "monitor_services": true,
            "monitor_timers": false,
            "monitor_all_units": true,
            "monitor_timeout_secs": 10
        },
        "dbus_config": {
            "monitor_session": true,
            "monitor_system": false,
            "include_interfaces": [],
            "exclude_interfaces": [],
            "extract_notifications": true,
            "extract_media": true,
            "extract_power": true,
            "extract_hardware": true,
            "extract_session": true,
            "extract_bluetooth": true,
            "extract_network": true,
            "extract_mounts": true,
            "health_check_interval_secs": 5,
            "inactivity_timeout_secs": 30
        },
        "journal_config": {
            "follow": true,
            "import_on_startup": false,
            "import_hours": 48,
            "units": ["nginx.service"],
            "priorities": [0, 1, 2],
            "include_kernel": false,
            "include_user": true,
            "exclude_fields": [],
            "cursor_file": null,
            "batch_size": 200,
            "cursor_flush_event_threshold": 25,
            "cursor_flush_interval_secs": 5
        }
    });

    let config: sinex_system_ingestor::SystemConfig = serde_json::from_value(json_val)?;

    assert!(!config.dbus_enabled);
    assert!(config.journal_enabled);
    assert!(!config.udev_enabled);
    assert!(config.systemd_enabled);
    assert_eq!(
        config.dbus_buses,
        sinex_system_ingestor::DbusBusScope::Session
    );
    assert_eq!(config.journal_timeout_secs.as_secs(), 15);
    assert!(config.systemd_config.monitor_all_units);
    assert!(!config.dbus_config.monitor_system);
    assert!(!config.journal_config.import_on_startup);
    assert_eq!(config.journal_config.import_hours, 48);
    assert_eq!(config.journal_config.units, vec!["nginx.service"]);
    assert_eq!(config.journal_config.priorities, vec![0, 1, 2]);
    assert!(config.journal_config.cursor_file.is_none());
    assert_eq!(config.journal_config.batch_size, 200);
    Ok(())
}

// ============================================================================
// Status type serde (unified_node.rs)
// ============================================================================

#[sinex_test]
async fn test_dbus_status_serde() -> TestResult<()> {
    let status = sinex_system_ingestor::DbusStatus {
        buses_monitored: vec!["session".to_string(), "system".to_string()],
        connection_active: true,
        recent_signal_count: 42,
    };

    let json_str = serde_json::to_string(&status)?;
    let deserialized: sinex_system_ingestor::DbusStatus = serde_json::from_str(&json_str)?;

    assert_eq!(deserialized.buses_monitored.len(), 2);
    assert!(deserialized.connection_active);
    assert_eq!(deserialized.recent_signal_count, 42);
    Ok(())
}

#[sinex_test]
async fn test_journal_status_serde() -> TestResult<()> {
    let status = sinex_system_ingestor::JournalStatus {
        following_active: true,
        cursor_position: Some("s=a;i=1;b=b;m=m;t=t;x=x".to_string()),
        recent_entry_count: 100,
    };

    let json_str = serde_json::to_string(&status)?;
    let deserialized: sinex_system_ingestor::JournalStatus = serde_json::from_str(&json_str)?;

    assert!(deserialized.following_active);
    assert!(deserialized.cursor_position.is_some());
    assert_eq!(deserialized.recent_entry_count, 100);
    Ok(())
}

#[sinex_test]
async fn test_udev_status_serde() -> TestResult<()> {
    let status = sinex_system_ingestor::UdevStatus {
        monitoring_active: true,
        recent_device_events: 5,
    };

    let json_str = serde_json::to_string(&status)?;
    let deserialized: sinex_system_ingestor::UdevStatus = serde_json::from_str(&json_str)?;

    assert!(deserialized.monitoring_active);
    assert_eq!(deserialized.recent_device_events, 5);
    Ok(())
}

#[sinex_test]
async fn test_systemd_status_serde() -> TestResult<()> {
    let status = sinex_system_ingestor::SystemdStatus {
        monitoring_active: false,
        units_tracked: 12,
        recent_state_changes: 3,
    };

    let json_str = serde_json::to_string(&status)?;
    let deserialized: sinex_system_ingestor::SystemdStatus = serde_json::from_str(&json_str)?;

    assert!(!deserialized.monitoring_active);
    assert_eq!(deserialized.units_tracked, 12);
    assert_eq!(deserialized.recent_state_changes, 3);
    Ok(())
}
