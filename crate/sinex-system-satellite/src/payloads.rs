//! Enhanced payload structures for system events
//!
//! This module provides comprehensive payload structures for D-Bus signals,
//! journal entries, and other system events, ported from the legacy
//! sinex-events-system implementation.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

// ============================================================================
// D-Bus Event Payloads
// ============================================================================

/// Generic D-Bus signal event with rich metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbusSignalPayload {
    /// Bus type (session or system)
    pub bus: String,
    /// Sender (e.g., :1.234 or org.mpris.MediaPlayer2.spotify)
    pub sender: String,
    /// Object path (e.g., /org/mpris/MediaPlayer2)
    pub path: String,
    /// Interface (e.g., org.mpris.MediaPlayer2.Player)
    pub interface: String,
    /// Signal name (e.g., PropertiesChanged)
    pub signal: String,
    /// Signal arguments as JSON
    pub args: JsonValue,
    /// Timestamp
    pub timestamp: String,
}

/// D-Bus method call event (for important method calls)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbusMethodCallPayload {
    pub bus: String,
    pub sender: String,
    pub destination: String,
    pub path: String,
    pub interface: String,
    pub method: String,
    pub args: JsonValue,
    pub timestamp: String,
}

/// Notification event (specialized from D-Bus signals)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPayload {
    pub app_name: String,
    pub summary: String,
    pub body: String,
    pub urgency: u8,
    pub timeout: i32,
    pub actions: Vec<String>,
    pub hints: HashMap<String, JsonValue>,
    pub timestamp: String,
}

/// Media playback event (from MPRIS interface)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaPlaybackPayload {
    pub player: String,
    pub player_instance: String,
    pub status: String, // Playing, Paused, Stopped
    pub track_id: Option<String>,
    pub title: Option<String>,
    pub artist: Option<Vec<String>>,
    pub album: Option<String>,
    pub album_artist: Option<Vec<String>>,
    pub track_number: Option<i32>,
    pub length: Option<i64>,   // microseconds
    pub position: Option<i64>, // microseconds
    pub volume: Option<f64>,
    pub loop_status: Option<String>, // None, Track, Playlist
    pub shuffle: Option<bool>,
    pub can_go_next: bool,
    pub can_go_previous: bool,
    pub can_play: bool,
    pub can_pause: bool,
    pub can_seek: bool,
    pub art_url: Option<String>,
    pub timestamp: String,
}

/// Power event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerEventPayload {
    pub event_type: String, // PrepareForSleep, PowerProfileChanged, etc.
    pub details: JsonValue,
    pub timestamp: String,
}

/// Hardware device event (via UDisks2, UPower, etc)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareEventPayload {
    pub device_type: String, // usb, disk, battery, bluetooth, etc
    pub event_type: String,  // added, removed, changed
    pub device_path: String,
    pub device_name: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, JsonValue>,
    pub timestamp: String,
}

/// Session/idle event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEventPayload {
    pub event_type: String, // idle, active, locked, unlocked
    pub session_id: Option<String>,
    pub idle_time_ms: Option<u64>,
    pub timestamp: String,
}

/// Bluetooth device event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BluetoothEventPayload {
    pub event_type: String, // connected, disconnected, paired, unpaired
    pub device_address: String,
    pub device_name: Option<String>,
    pub device_class: Option<String>,
    pub rssi: Option<i16>,
    pub connected: bool,
    pub paired: bool,
    pub trusted: bool,
    pub timestamp: String,
}

/// Network manager event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkEventPayload {
    pub event_type: String, // connected, disconnected, ip_changed
    pub interface: String,
    pub connection_type: String, // wifi, ethernet, vpn
    pub ssid: Option<String>,
    pub ip_address: Option<String>,
    pub state: String,
    pub timestamp: String,
}

/// Mount/unmount event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountEventPayload {
    pub event_type: String, // mounted, unmounted
    pub device: String,
    pub mount_point: String,
    pub filesystem: String,
    pub label: Option<String>,
    pub uuid: Option<String>,
    pub size_bytes: Option<u64>,
    pub timestamp: String,
}

// ============================================================================
// Journal Event Payloads
// ============================================================================

/// Enhanced systemd journal entry event with rich metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntryPayload {
    /// Journal cursor for this entry (unique identifier)
    pub cursor: String,
    /// Timestamp from journal (microseconds since epoch)
    pub timestamp_us: i64,
    /// Parsed timestamp
    pub timestamp: String,
    /// Hostname
    pub hostname: Option<String>,
    /// Unit name (for systemd services)
    pub unit: Option<String>,
    /// Syslog identifier
    pub syslog_identifier: Option<String>,
    /// Process ID
    pub pid: Option<u32>,
    /// User ID
    pub uid: Option<u32>,
    /// Group ID
    pub gid: Option<u32>,
    /// Command line
    pub cmdline: Option<String>,
    /// Executable path
    pub exe: Option<String>,
    /// systemd unit type (service, socket, etc)
    pub unit_type: Option<String>,
    /// Priority/severity level (0-7, emergency to debug)
    pub priority: Option<u8>,
    /// Facility (kernel, mail, etc)
    pub facility: Option<String>,
    /// Message content
    pub message: String,
    /// Additional fields from journal
    pub fields: HashMap<String, String>,
}

/// Journal sync/import status event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalSyncPayload {
    /// Sync operation type (initial_import, incremental_sync)
    pub sync_type: String,
    /// Starting cursor
    pub start_cursor: Option<String>,
    /// Ending cursor
    pub end_cursor: String,
    /// Number of entries processed
    pub entries_count: u64,
    /// Time range start
    pub time_start: Option<String>,
    /// Time range end
    pub time_end: Option<String>,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

// ============================================================================
// Configuration Structures
// ============================================================================

/// Enhanced D-Bus configuration with filtering and specialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbusConfig {
    /// Monitor session bus
    pub monitor_session: bool,
    /// Monitor system bus
    pub monitor_system: bool,
    /// Interfaces to monitor (empty = all)
    pub include_interfaces: Vec<String>,
    /// Interfaces to exclude
    pub exclude_interfaces: Vec<String>,
    /// Specialized event extraction
    pub extract_notifications: bool,
    pub extract_media: bool,
    pub extract_power: bool,
    pub extract_hardware: bool,
    pub extract_session: bool,
    pub extract_bluetooth: bool,
    pub extract_network: bool,
    pub extract_mounts: bool,
}

impl Default for DbusConfig {
    fn default() -> Self {
        Self {
            monitor_session: true,
            monitor_system: true,
            include_interfaces: vec![],
            exclude_interfaces: vec![
                // Exclude noisy interfaces by default
                "org.freedesktop.DBus.Properties".to_string(),
                "org.freedesktop.DBus.Introspectable".to_string(),
                "org.freedesktop.DBus.Peer".to_string(),
            ],
            extract_notifications: true,
            extract_media: true,
            extract_power: true,
            extract_hardware: true,
            extract_session: true,
            extract_bluetooth: true,
            extract_network: true,
            extract_mounts: true,
        }
    }
}

/// Enhanced journal configuration with historical import
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalConfig {
    /// Follow journal in real-time
    pub follow: bool,
    /// Import historical entries on startup
    pub import_on_startup: bool,
    /// How far back to import (in hours, 0 = all)
    pub import_hours: u32,
    /// Units to monitor (empty = all)
    pub units: Vec<String>,
    /// Priority levels to capture (0-7, empty = all)
    pub priorities: Vec<u8>,
    /// Include kernel messages
    pub include_kernel: bool,
    /// Include user session messages
    pub include_user: bool,
    /// Fields to exclude from additional fields
    pub exclude_fields: Vec<String>,
    /// Cursor file to track position
    pub cursor_file: Option<String>,
    /// Batch size for imports
    pub batch_size: usize,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            follow: true,
            import_on_startup: true,
            import_hours: 0, // Import all history
            units: vec![],   // Empty = capture all units
            priorities: vec![], // Empty = capture all priorities
            include_kernel: true,
            include_user: true,
            exclude_fields: vec![
                "__CURSOR".to_string(),
                "__REALTIME_TIMESTAMP".to_string(),
                "__MONOTONIC_TIMESTAMP".to_string(),
                "_TRANSPORT".to_string(),
            ],
            cursor_file: Some("/var/lib/sinex/journal.cursor".to_string()),
            batch_size: 1000,
        }
    }
}