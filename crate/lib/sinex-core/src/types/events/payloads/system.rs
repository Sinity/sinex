//! System event payloads

use super::define_event_payload;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;
use std::collections::HashMap;

define_event_payload! {
    /// Emitted when a system scan kicks off.
    pub struct ScanStartedPayload {
        scan_type: String,
        target: String,
        options: HashMap<String, serde_json::Value>,
    } => ("system", "scan.started");
}

define_event_payload! {
    /// Summarises a completed scan.
    pub struct ScanCompletedPayload {
        scan_type: String,
        target: String,
        items_scanned: u64,
        items_found: u64,
        duration_ms: u64,
        errors: Vec<String>,
    } => ("system", "scan.completed");
}

define_event_payload! {
    /// Captures a journald log entry with raw fields.
    pub struct JournalEntryPayload {
        unit: Option<String>,
        priority: u8,
        message: String,
        fields: HashMap<String, String>,
        timestamp: DateTime<Utc>,
    } => ("journald", "log_entry.captured");
}

define_event_payload! {
    /// Records the outcome of a journald cursor sync.
    pub struct JournalSyncCompletedPayload {
        sync_type: String,
        start_cursor: Option<String>,
        end_cursor: String,
        entries_count: u64,
        time_start: Option<String>,
        time_end: Option<String>,
        duration_ms: u64,
    } => ("journald", "sync.completed");
}

define_event_payload! {
    /// Describes a journald entry flushed to disk.
    pub struct JournalEntryWrittenPayload {
        cursor: String,
        timestamp_us: i64,
        timestamp: String,
        hostname: Option<String>,
        unit: Option<String>,
        syslog_identifier: Option<String>,
        pid: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        cmdline: Option<String>,
        exe: Option<String>,
        unit_type: Option<String>,
        priority: Option<u8>,
        facility: Option<String>,
        message: String,
        fields: HashMap<String, String>,
    } => ("journald", "entry.written");
}

define_event_payload! {
    /// Raw D-Bus signal payload.
    pub struct DbusSignalPayload {
        bus: String,
        sender: String,
        path: String,
        interface: String,
        signal: String,
        args: serde_json::Value,
        timestamp: String,
    } => ("dbus", "signal.received");
}

define_event_payload! {
    /// Invocation of a D-Bus method.
    pub struct DbusMethodCalledPayload {
        bus: String,
        sender: String,
        destination: String,
        path: String,
        interface: String,
        method: String,
        args: serde_json::Value,
        timestamp: String,
    } => ("dbus", "method.called");
}

define_event_payload! {
    /// Desktop notification dispatched over D-Bus.
    pub struct DbusNotificationSentPayload {
        app_name: String,
        summary: String,
        body: String,
        urgency: u8,
        timeout: i32,
        actions: Vec<String>,
        hints: HashMap<String, serde_json::Value>,
        timestamp: String,
    } => ("dbus", "notification.sent");
}

define_event_payload! {
    /// Media player metadata snapshot extracted from D-Bus.
    pub struct DbusMediaStateChangedPayload {
        player: String,
        player_instance: String,
        status: String,
        track_id: Option<String>,
        title: Option<String>,
        artist: Option<Vec<String>>,
        album: Option<String>,
        album_artist: Option<Vec<String>>,
        track_number: Option<i32>,
        length: Option<i64>,
        position: Option<i64>,
        volume: Option<f64>,
        loop_status: Option<String>,
        shuffle: Option<bool>,
        can_go_next: bool,
        can_go_previous: bool,
        can_play: bool,
        can_pause: bool,
        can_seek: bool,
        art_url: Option<String>,
        timestamp: String,
    } => ("dbus", "media.state_changed");
}

define_event_payload! {
    /// High-level power management notification.
    pub struct DbusPowerStateChangedPayload {
        event_type: String,
        details: serde_json::Value,
        timestamp: String,
    } => ("dbus", "power.state_changed");
}

define_event_payload! {
    /// D-Bus announcement for a newly connected device.
    pub struct DbusDeviceConnectedPayload {
        device_type: String,
        event_type: String,
        device_path: String,
        device_name: Option<String>,
        vendor: Option<String>,
        model: Option<String>,
        serial: Option<String>,
        properties: HashMap<String, serde_json::Value>,
        timestamp: String,
    } => ("dbus", "device.connected");
}

define_event_payload! {
    /// Bluetooth device change event delivered via D-Bus.
    pub struct DbusBluetoothDeviceChangedPayload {
        event_type: String,
        device_address: String,
        device_name: Option<String>,
        device_class: Option<String>,
        rssi: Option<i16>,
        connected: bool,
        paired: bool,
        trusted: bool,
        timestamp: String,
    } => ("dbus", "bluetooth.device_changed");
}

define_event_payload! {
    /// Network state change event observed on D-Bus.
    pub struct DbusNetworkStateChangedPayload {
        event_type: String,
        interface: String,
        connection_type: String,
        ssid: Option<String>,
        ip_address: Option<String>,
        state: String,
        timestamp: String,
    } => ("dbus", "network.state_changed");
}

define_event_payload! {
    /// Mount/Unmount operation surfaced via D-Bus.
    pub struct DbusMountEventPayload {
        event_type: String,
        device: String,
        mount_point: String,
        filesystem_type: Option<String>,
        options: Option<String>,
        timestamp: String,
    } => ("dbus", "mount.event");
}

define_event_payload! {
    /// Structured log line captured during Stage-as-You-Go ingestion.
    pub struct LogLinePayload {
        line: String,
        line_number: u64,
        log_source: String,
        log_file: String,
        offset_start: i64,
        offset_end: i64,
        source_material_id: String,
    } => ("system.log", "line");
}
