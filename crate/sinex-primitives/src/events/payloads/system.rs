//! System event payloads

use crate::Timestamp;
use crate::events::enums::{
    BluetoothEventType, DBusBus, DeviceType, JournalSyncType, LoopStatus, MountEventType,
    NetworkConnectionType, NetworkEventType, NetworkState, PlaybackStatus, PowerEventType,
    SystemdActiveState, SystemdUnitType, UdevAction,
};
use crate::units::{ExitCode, Microseconds, ProcessId, SyslogPriority, UnixGid, UnixUid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "journald", event_type = "sync.completed")]
/// Records the outcome of a journald cursor sync.
pub struct JournalSyncCompletedPayload {
    pub sync_type: JournalSyncType,
    pub start_cursor: Option<String>,
    pub end_cursor: String,
    pub entries_count: u64,
    pub time_start: Option<Timestamp>,
    pub time_end: Option<Timestamp>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "journald", event_type = "entry.written")]
/// Describes a journald entry flushed to disk.
pub struct JournalEntryWrittenPayload {
    pub cursor: String,
    pub timestamp_us: Microseconds,
    pub timestamp: Timestamp,
    pub hostname: Option<String>,
    pub unit: Option<String>,
    pub syslog_identifier: Option<String>,
    pub pid: Option<ProcessId>,
    pub uid: Option<UnixUid>,
    pub gid: Option<UnixGid>,
    pub cmdline: Option<String>,
    pub exe: Option<String>,
    pub unit_type: Option<SystemdUnitType>,
    pub priority: Option<SyslogPriority>,
    pub facility: Option<String>,
    pub message: String,
    pub fields: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "signal.received")]
/// Raw D-Bus signal payload.
pub struct DbusSignalPayload {
    pub bus: DBusBus,
    pub sender: String,
    pub path: String,
    pub interface: String,
    pub signal: String, // member/signal name
    pub args: serde_json::Value,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "method.called")]
/// Invocation of a D-Bus method.
pub struct DbusMethodCalledPayload {
    pub bus: DBusBus,
    pub sender: String,
    pub destination: String,
    pub path: String,
    pub interface: String,
    pub method: String,
    pub args: serde_json::Value,
    pub timestamp: Timestamp,
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
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "media.state_changed")]
/// Media player metadata snapshot extracted from D-Bus.
pub struct DbusMediaStateChangedPayload {
    pub player: String,
    pub player_instance: String,
    pub status: PlaybackStatus,
    pub track_id: Option<String>,
    pub title: Option<String>,
    pub artist: Option<Vec<String>>,
    pub album: Option<String>,
    pub album_artist: Option<Vec<String>>,
    pub track_number: Option<i32>,
    pub length: Option<i64>,
    pub position: Option<i64>,
    pub volume: Option<f64>,
    pub loop_status: Option<LoopStatus>,
    pub shuffle: Option<bool>,
    pub can_go_next: bool,
    pub can_go_previous: bool,
    pub can_play: bool,
    pub can_pause: bool,
    pub can_seek: bool,
    pub art_url: Option<String>,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "power.state_changed")]
/// High-level power management notification.
pub struct DbusPowerStateChangedPayload {
    pub event_type: PowerEventType,
    pub details: serde_json::Value,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "device.connected")]
/// D-Bus announcement for a newly connected device.
pub struct DbusDeviceConnectedPayload {
    pub device_type: DeviceType,
    pub event_type: String,
    pub device_path: String,
    pub device_name: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, serde_json::Value>,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "bluetooth.device_changed")]
pub struct DbusBluetoothDeviceChangedPayload {
    pub event_type: BluetoothEventType,
    pub device_address: String,
    pub device_name: Option<String>,
    pub device_class: Option<String>,
    pub rssi: Option<i16>,
    pub connected: bool,
    pub paired: bool,
    pub trusted: bool,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "network.state_changed")]
pub struct DbusNetworkStateChangedPayload {
    pub event_type: NetworkEventType,
    pub interface: String,
    pub connection_type: NetworkConnectionType,
    pub ssid: Option<String>,
    pub ip_address: Option<String>,
    pub state: NetworkState,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "dbus", event_type = "mount.event")]
pub struct DbusMountEventPayload {
    pub event_type: MountEventType,
    pub device: String,
    pub mount_point: String,
    pub filesystem: String,
    pub label: Option<String>,
    pub uuid: Option<String>,
    pub size_bytes: Option<u64>,
    pub timestamp: Timestamp,
}

// Systemd unit events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.started")]
pub struct SystemdUnitStartedPayload {
    pub unit_name: String,
    pub unit_type: SystemdUnitType,
    pub main_pid: Option<ProcessId>,
    pub active_state: SystemdActiveState,
    pub sub_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.stopped")]
pub struct SystemdUnitStoppedPayload {
    pub unit_name: String,
    pub unit_type: SystemdUnitType,
    pub exit_code: Option<ExitCode>,
    pub active_state: SystemdActiveState,
    pub sub_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.failed")]
pub struct SystemdUnitFailedPayload {
    pub unit_name: String,
    pub message: String,
    pub cursor: String,
    pub pid: Option<String>,
    pub uid: Option<String>,
    pub timestamp: Timestamp,
    pub journal_timestamp: Option<Timestamp>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "unit.reloaded")]
pub struct SystemdUnitReloadedPayload {
    pub unit_name: Option<String>,
    pub message: String,
    pub cursor: String,
    pub pid: Option<String>,
    pub uid: Option<String>,
    pub timestamp: Timestamp,
    pub journal_timestamp: Option<Timestamp>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "systemd", event_type = "timer.triggered")]
pub struct SystemdTimerTriggeredPayload {
    pub unit_name: Option<String>,
    pub message: String,
    pub cursor: String,
    pub pid: Option<String>,
    pub uid: Option<String>,
    pub timestamp: Timestamp,
    pub journal_timestamp: Option<Timestamp>,
}

// udev device events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.connected")]
pub struct UdevDeviceConnectedPayload {
    pub action: UdevAction,
    pub device_path: String,
    pub device_type: DeviceType,
    pub subsystem: Option<String>,
    pub devtype: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, String>,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.disconnected")]
pub struct UdevDeviceDisconnectedPayload {
    pub action: UdevAction,
    pub device_path: String,
    pub device_type: DeviceType,
    pub subsystem: Option<String>,
    pub devtype: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, String>,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.changed")]
pub struct UdevDeviceChangedPayload {
    pub action: UdevAction,
    pub device_path: String,
    pub device_type: DeviceType,
    pub subsystem: Option<String>,
    pub devtype: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, String>,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.driver_changed")]
pub struct UdevDeviceDriverChangedPayload {
    pub action: UdevAction,
    pub device_path: String,
    pub device_type: DeviceType,
    pub subsystem: Option<String>,
    pub devtype: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, String>,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "udev", event_type = "device.other")]
pub struct UdevDeviceOtherPayload {
    pub action: UdevAction,
    pub device_path: String,
    pub device_type: DeviceType,
    pub subsystem: Option<String>,
    pub devtype: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, String>,
    pub timestamp: Timestamp,
}

// System monitoring lifecycle events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "system", event_type = "monitoring.started")]
pub struct SystemMonitoringStartedPayload {
    pub dbus_enabled: bool,
    pub journal_enabled: bool,
    pub udev_enabled: bool,
    pub systemd_enabled: bool,
    pub start_time: Timestamp,
}

// Test helpers for external tests
#[cfg(any(test, feature = "testing"))]
impl SystemdUnitStartedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            unit_name: "test.service".into(),
            unit_type: SystemdUnitType::Service,
            main_pid: None,
            active_state: SystemdActiveState::Active,
            sub_state: "running".into(),
        }
    }
}
