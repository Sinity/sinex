#![doc = include_str!("../docs/payloads.md")]

//! Payload structures for system events handled by the system node.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sinex_primitives::Seconds;
use sinex_primitives::events::enums::JournalSyncType;
use sinex_primitives::temporal::Timestamp;
use std::collections::HashMap;
// Default configuration values for systemd journal monitoring
const DEFAULT_JOURNAL_BATCH_SIZE: usize = 1000;

fn optional_utf8_env(var: &'static str) -> Option<String> {
    sinex_primitives::env::var_optional(var, "journal cursor defaults")
}

fn default_journal_cursor_path() -> String {
    optional_utf8_env("SINEX_JOURNAL_CURSOR_FILE")
        .or_else(|| {
            optional_utf8_env("SINEX_STATE_DIR")
                .map(|state_dir| format!("{state_dir}/journal.cursor"))
        })
        .or_else(|| {
            optional_utf8_env("XDG_STATE_HOME").map(|d| format!("{d}/sinex/journal.cursor"))
        })
        .unwrap_or_else(|| "/var/lib/sinex/journal.cursor".to_string())
}

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
    /// Signal name (e.g., `PropertiesChanged`)
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

/// Hardware device event (via `UDisks2`, `UPower`, etc)
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

/// Systemd journal entry event with rich metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntryPayload {
    /// Journal cursor for this entry (unique identifier)
    pub cursor: String,
    /// Timestamp from journal (microseconds since epoch)
    pub timestamp_us: i64,
    /// Parsed timestamp
    pub timestamp: Timestamp,
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
    /// Sync operation type
    pub sync_type: JournalSyncType,
    /// Starting cursor
    pub start_cursor: Option<String>,
    /// Ending cursor
    pub end_cursor: String,
    /// Number of entries processed
    pub entries_count: u64,
    /// Time range start
    pub time_start: Option<Timestamp>,
    /// Time range end
    pub time_end: Option<Timestamp>,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

// ============================================================================
// Configuration Structures
// ============================================================================

/// D-Bus configuration with filtering and specialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbusConfig {
    /// Monitor session bus
    pub monitor_session: bool,
    /// Monitor system bus
    pub monitor_system: bool,
    /// Interfaces to monitor (empty = all)
    pub include_interfaces: Vec<String>,
    /// Interfaces to exclude
    ///
    /// Keep `org.freedesktop.DBus.Properties` visible: property-changed
    /// notifications feed media and state extraction.
    pub exclude_interfaces: Vec<String>,
    /// Specialized event extraction
    pub extract_notifications: bool,
    pub extract_media: bool,
    pub extract_power: bool,
    pub extract_hardware: bool,
    /// Session-bus classification is not a dedicated extraction surface yet.
    pub extract_session: bool,
    pub extract_bluetooth: bool,
    pub extract_network: bool,
    pub extract_mounts: bool,
    /// Connection health check interval in seconds (default: 5s)
    pub health_check_interval_secs: Seconds,
    /// Inactivity timeout before reconnection in seconds (default: 30s)
    pub inactivity_timeout_secs: Seconds,
}

impl Default for DbusConfig {
    fn default() -> Self {
        Self {
            monitor_session: true,
            monitor_system: true,
            include_interfaces: vec![],
            exclude_interfaces: vec![
                // Exclude noisy interfaces by default
                "org.freedesktop.DBus.Introspectable".to_string(),
                "org.freedesktop.DBus.Peer".to_string(),
            ],
            extract_notifications: true,
            extract_media: true,
            extract_power: true,
            extract_hardware: true,
            extract_session: false,
            extract_bluetooth: true,
            extract_network: true,
            extract_mounts: true,
            health_check_interval_secs: Seconds::from_secs(5),
            inactivity_timeout_secs: Seconds::from_secs(30),
        }
    }
}

/// Journal configuration with historical import
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalConfig {
    /// Follow journal in real-time
    pub follow: bool,
    /// Fallback historical scan window when no explicit checkpoint is supplied (0 = all)
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
    /// Units to explicitly exclude even when `units` is empty (catch-all mode).
    /// Defaults to sinex-* self-units to prevent feedback loops.
    pub exclude_units: Vec<String>,
    /// Cursor file to track position
    pub cursor_file: Option<String>,
    /// Batch size for imports
    pub batch_size: usize,
    /// Cursor flush event threshold (default: 100 events)
    /// Cursor is flushed to disk after this many events
    pub cursor_flush_event_threshold: u64,
    /// Cursor flush interval in seconds (default: 10s)
    /// Cursor is flushed to disk after this interval even if threshold not reached
    pub cursor_flush_interval_secs: Seconds,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            follow: true,
            import_hours: 24,
            units: vec![],      // Empty = capture all units (excluding sinex-* self-units by default)
            exclude_units: default_journal_exclude_units(),
            priorities: vec![], // Empty = capture all priorities
            include_kernel: true,
            include_user: true,
            exclude_fields: vec![
                "__CURSOR".to_string(),
                "__REALTIME_TIMESTAMP".to_string(),
                "__MONOTONIC_TIMESTAMP".to_string(),
                "_TRANSPORT".to_string(),
            ],
            cursor_file: Some(default_journal_cursor_path()),
            batch_size: DEFAULT_JOURNAL_BATCH_SIZE,
            cursor_flush_event_threshold: 100,
            cursor_flush_interval_secs: Seconds::from_secs(10),
        }
    }
}

/// Self-units excluded from journal capture by default.
/// Prevents the self-amplification feedback loop where sinex logs
/// become sinex inputs under failure (issue #581).
#[must_use]
pub fn default_journal_exclude_units() -> Vec<String> {
    vec![
        "sinex-*.service".into(),
        "sinex-*.timer".into(),
        "sinex-*.socket".into(),
    ]
}

/// `SystemD` unit types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemdUnitType {
    Service,
    Timer,
    Socket,
    Target,
    Mount,
    Other,
}

impl SystemdUnitType {
    /// Determine unit type from unit name
    #[must_use]
    pub fn from_unit_name(unit_name: &str) -> Self {
        if unit_name.ends_with(".service") {
            Self::Service
        } else if unit_name.ends_with(".timer") {
            Self::Timer
        } else if unit_name.ends_with(".socket") {
            Self::Socket
        } else if unit_name.ends_with(".target") {
            Self::Target
        } else if unit_name.ends_with(".mount") {
            Self::Mount
        } else {
            Self::Other
        }
    }
}

impl std::fmt::Display for SystemdUnitType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Service => write!(f, "service"),
            Self::Timer => write!(f, "timer"),
            Self::Socket => write!(f, "socket"),
            Self::Target => write!(f, "target"),
            Self::Mount => write!(f, "mount"),
            Self::Other => write!(f, "other"),
        }
    }
}

/// `SystemD` unit states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemdUnitState {
    Active,
    Inactive,
    Failed,
    Activating,
    Deactivating,
    Unknown,
}

impl SystemdUnitState {
    /// Parse unit state from systemctl output
    #[must_use]
    pub fn from_status_string(s: &str) -> Self {
        match s {
            "active" => Self::Active,
            "inactive" => Self::Inactive,
            "failed" => Self::Failed,
            "activating" => Self::Activating,
            "deactivating" => Self::Deactivating,
            _ => Self::Unknown,
        }
    }
}

impl std::fmt::Display for SystemdUnitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Inactive => write!(f, "inactive"),
            Self::Failed => write!(f, "failed"),
            Self::Activating => write!(f, "activating"),
            Self::Deactivating => write!(f, "deactivating"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Systemd watcher configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemdConfig {
    /// Monitor service state changes
    pub monitor_services: bool,
    /// Monitor timer state changes
    pub monitor_timers: bool,
    /// Monitor all unit types
    pub monitor_all_units: bool,
    /// systemctl monitor timeout in seconds
    pub monitor_timeout_secs: Seconds,
}

impl Default for SystemdConfig {
    fn default() -> Self {
        Self {
            monitor_services: true,
            monitor_timers: true,
            monitor_all_units: false, // Start conservative
            monitor_timeout_secs: Seconds::from_secs(5),
        }
    }
}

#[cfg(test)]
mod tests {
    // Inline because these helpers only exist to derive payload defaults from local env state.
    use super::{default_journal_cursor_path, optional_utf8_env};
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use xtask::sandbox::{EnvGuard, sinex_test};

    #[sinex_test]
    async fn default_journal_cursor_path_prefers_explicit_env() -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_JOURNAL_CURSOR_FILE", "/tmp/custom.cursor");
        env.set("SINEX_STATE_DIR", "/tmp/state");
        env.set("XDG_STATE_HOME", "/tmp/xdg");

        assert_eq!(default_journal_cursor_path(), "/tmp/custom.cursor");
        Ok(())
    }

    #[sinex_test]
    async fn default_journal_cursor_path_uses_state_dir_when_present()
    -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.clear("SINEX_JOURNAL_CURSOR_FILE");
        env.set("SINEX_STATE_DIR", "/tmp/state");
        env.set("XDG_STATE_HOME", "/tmp/xdg");

        assert_eq!(default_journal_cursor_path(), "/tmp/state/journal.cursor");
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn optional_utf8_env_rejects_non_utf8_values() -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_STATE_DIR", OsString::from_vec(vec![0xff]));

        assert_eq!(optional_utf8_env("SINEX_STATE_DIR"), None);
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn default_journal_cursor_path_ignores_non_utf8_state_dir()
    -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.clear("SINEX_JOURNAL_CURSOR_FILE");
        env.set("SINEX_STATE_DIR", OsString::from_vec(vec![0xff]));
        env.clear("XDG_STATE_HOME");

        assert_eq!(
            default_journal_cursor_path(),
            "/var/lib/sinex/journal.cursor"
        );
        Ok(())
    }
}
