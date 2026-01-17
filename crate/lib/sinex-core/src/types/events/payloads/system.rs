//! System event payloads

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "system", event_type = "scan.started")]
/// Emitted when a system scan kicks off.
pub struct ScanStartedPayload {
    pub scan_type: String,
    pub target: String,
    pub options: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "system", event_type = "scan.completed")]
/// Summarises a completed scan.
pub struct ScanCompletedPayload {
    pub scan_type: String,
    pub target: String,
    pub items_scanned: u64,
    pub items_found: u64,
    pub duration_ms: u64,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "journald", event_type = "log_entry.captured")]
/// Captures a journald log entry with raw fields.
pub struct JournalEntryPayload {
    pub unit: Option<String>,
    pub priority: u8,
    pub message: String,
    pub fields: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "journald", event_type = "sync.completed")]
/// Records the outcome of a journald cursor sync.
pub struct JournalSyncCompletedPayload {
    pub sync_type: String,
    pub start_cursor: Option<String>,
    pub end_cursor: String,
    pub entries_count: u64,
    pub time_start: Option<String>,
    pub time_end: Option<String>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "journald", event_type = "entry.written")]
/// Describes a journald entry flushed to disk.
pub struct JournalEntryWrittenPayload {
    pub cursor: String,
    pub timestamp_us: i64,
    pub timestamp: String,
    pub hostname: Option<String>,
    pub unit: Option<String>,
    pub syslog_identifier: Option<String>,
    pub pid: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub cmdline: Option<String>,
    pub exe: Option<String>,
    pub unit_type: Option<String>,
    pub priority: Option<u8>,
    pub facility: Option<String>,
    pub message: String,
    pub fields: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "signal.received")]
/// Raw D-Bus signal payload.
pub struct DbusSignalPayload {
    pub bus: String,
    pub sender: String,
    pub path: String,
    pub interface: String,
    pub signal: String, // member/signal name
    pub args: serde_json::Value,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "method.called")]
/// Invocation of a D-Bus method.
pub struct DbusMethodCalledPayload {
    pub bus: String,
    pub sender: String,
    pub destination: String,
    pub path: String,
    pub interface: String,
    pub method: String,
    pub args: serde_json::Value,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "notification.sent")]
/// Desktop notification dispatched over D-Bus.
pub struct DbusNotificationSentPayload {
    pub app_name: String,
    pub summary: String,
    pub body: String,
    pub urgency: u8,
    pub timeout: i32,
    pub actions: Vec<String>,
    pub hints: HashMap<String, serde_json::Value>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "media.state_changed")]
/// Media player metadata snapshot extracted from D-Bus.
pub struct DbusMediaStateChangedPayload {
    pub player: String,
    pub player_instance: String,
    pub status: String,
    pub track_id: Option<String>,
    pub title: Option<String>,
    pub artist: Option<Vec<String>>,
    pub album: Option<String>,
    pub album_artist: Option<Vec<String>>,
    pub track_number: Option<i32>,
    pub length: Option<i64>,
    pub position: Option<i64>,
    pub volume: Option<f64>,
    pub loop_status: Option<String>,
    pub shuffle: Option<bool>,
    pub can_go_next: bool,
    pub can_go_previous: bool,
    pub can_play: bool,
    pub can_pause: bool,
    pub can_seek: bool,
    pub art_url: Option<String>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "power.state_changed")]
/// High-level power management notification.
pub struct DbusPowerStateChangedPayload {
    pub event_type: String,
    pub details: serde_json::Value,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "device.connected")]
/// D-Bus announcement for a newly connected device.
pub struct DbusDeviceConnectedPayload {
    pub device_type: String,
    pub event_type: String,
    pub device_path: String,
    pub device_name: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, serde_json::Value>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "bluetooth.device_changed")]
pub struct DbusBluetoothDeviceChangedPayload {
    pub event_type: String,
    pub device_address: String,
    pub device_name: Option<String>,
    pub device_class: Option<String>,
    pub rssi: Option<i16>,
    pub connected: bool,
    pub paired: bool,
    pub trusted: bool,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "network.state_changed")]
pub struct DbusNetworkStateChangedPayload {
    pub event_type: String,
    pub interface: String,
    pub connection_type: String,
    pub ssid: Option<String>,
    pub ip_address: Option<String>,
    pub state: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "mount.event")]
pub struct DbusMountEventPayload {
    pub event_type: String, // mounted, unmounted
    pub device: String,
    pub mount_point: String,
    pub filesystem: String,
    pub label: Option<String>,
    pub uuid: Option<String>,
    pub size_bytes: Option<u64>,
    pub timestamp: String,
}

// Systemd unit events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.started")]
pub struct SystemdUnitStartedPayload {
    pub unit_name: String,
    pub unit_type: String,
    pub main_pid: Option<u32>,
    pub active_state: String,
    pub sub_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.stopped")]
pub struct SystemdUnitStoppedPayload {
    pub unit_name: String,
    pub unit_type: String,
    pub exit_code: Option<i32>,
    pub active_state: String,
    pub sub_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.status")]
pub struct SystemdUnitStatusPayload {
    pub unit_name: String,
    pub unit_type: String,
    pub description: String,
    pub action: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.failed")]
pub struct SystemdUnitFailedPayload {
    pub unit_name: String,
    pub message: String,
    pub cursor: String,
    pub pid: Option<String>,
    pub uid: Option<String>,
    pub timestamp: String,
    pub journal_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.reloaded")]
pub struct SystemdUnitReloadedPayload {
    pub unit_name: Option<String>,
    pub message: String,
    pub cursor: String,
    pub pid: Option<String>,
    pub uid: Option<String>,
    pub timestamp: String,
    pub journal_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "timer.triggered")]
pub struct SystemdTimerTriggeredPayload {
    pub unit_name: Option<String>,
    pub message: String,
    pub cursor: String,
    pub pid: Option<String>,
    pub uid: Option<String>,
    pub timestamp: String,
    pub journal_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.starting")]
pub struct SystemdUnitStartingPayload {
    pub status: String,
    pub status_detail: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.stopping")]
pub struct SystemdUnitStoppingPayload {
    pub status: String,
    pub status_detail: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.state_changed")]
pub struct SystemdUnitStateChangedPayload {
    pub status: String,
    pub status_detail: String,
    pub timestamp: String,
}

// udev device events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.added")]
pub struct UdevDeviceAddedPayload {
    pub syspath: String,
    pub devpath: String,
    pub subsystem: String,
    pub devtype: Option<String>,
    pub driver: Option<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.removed")]
pub struct UdevDeviceRemovedPayload {
    pub syspath: String,
    pub devpath: String,
    pub subsystem: String,
    pub devtype: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.connected")]
pub struct UdevDeviceConnectedPayload {
    pub action: String,
    pub device_path: String,
    pub device_type: String,
    pub subsystem: Option<String>,
    pub devtype: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, String>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.disconnected")]
pub struct UdevDeviceDisconnectedPayload {
    pub action: String,
    pub device_path: String,
    pub device_type: String,
    pub subsystem: Option<String>,
    pub devtype: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, String>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.changed")]
pub struct UdevDeviceChangedPayload {
    pub action: String,
    pub device_path: String,
    pub device_type: String,
    pub subsystem: Option<String>,
    pub devtype: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, String>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.driver_changed")]
pub struct UdevDeviceDriverChangedPayload {
    pub action: String,
    pub device_path: String,
    pub device_type: String,
    pub subsystem: Option<String>,
    pub devtype: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, String>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.other")]
pub struct UdevDeviceOtherPayload {
    pub action: String,
    pub device_path: String,
    pub device_type: String,
    pub subsystem: Option<String>,
    pub devtype: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, String>,
    pub timestamp: String,
}

// Log processing events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "log_processor", event_type = "log.line")]
pub struct LogLinePayload {
    pub line: String,
    pub line_number: u64,
    pub log_source: String, // "nginx", "apache", "syslog", etc.
    pub log_file: String,   // Full path to the log file
    pub offset_start: i64,
    pub offset_end: i64,
    pub source_material_id: String,
}

// Health aggregation events

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Failed,
    Missing,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ComponentHealth {
    pub service_name: String,
    pub status: HealthStatus,
    pub last_heartbeat: DateTime<Utc>,
    pub uptime_seconds: Option<i64>,
    pub memory_usage_mb: Option<i32>,
    pub events_processed: Option<i64>,
    pub version: Option<String>,
    pub git_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "health-aggregator", event_type = "system.health_summary")]
pub struct SystemHealthSummaryPayload {
    pub overall_status: HealthStatus,
    pub healthy_components: u32,
    pub degraded_components: u32,
    pub failed_components: u32,
    pub missing_components: u32,
    pub total_components: u32,
    pub last_updated: DateTime<Utc>,
    pub components: HashMap<String, ComponentHealth>,
}

// Satellite heartbeat events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "journald", event_type = "node.heartbeat")]
pub struct SatelliteHeartbeatPayload {
    pub service_name: String,
    pub uptime_seconds: Option<i64>,
    pub memory_usage_mb: Option<i32>,
    pub events_processed: Option<i64>,
    pub version: Option<String>,
    pub git_hash: Option<String>,
}

// System monitoring lifecycle events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "system", event_type = "monitoring.started")]
pub struct SystemMonitoringStartedPayload {
    pub dbus_enabled: bool,
    pub journal_enabled: bool,
    pub udev_enabled: bool,
    pub systemd_enabled: bool,
    pub start_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "system", event_type = "snapshot")]
pub struct SystemSnapshotPayload {
    pub active_watchers: usize,
    pub dbus_enabled: bool,
    pub journal_enabled: bool,
    pub udev_enabled: bool,
    pub systemd_enabled: bool,
    pub snapshot_time: DateTime<Utc>,
}
