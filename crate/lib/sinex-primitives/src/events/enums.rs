//! Strongly-typed enums for event payload fields
//!
//! These replace stringly-typed fields in event payloads with compile-time
//! verified enums, preventing invalid states and improving API clarity.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

// ─────────────────────────────────────────────────────────────
// Filesystem Enums
// ─────────────────────────────────────────────────────────────

/// Type of file modification detected
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum FileModificationType {
    /// File content was modified
    #[default]
    Content,
    /// File metadata (timestamps, etc.) changed
    Metadata,
    /// File permissions changed
    Permissions,
    /// File size changed
    Size,
    /// File ownership (uid/gid) changed
    Ownership,
    /// Multiple attributes changed
    Multiple,
}

impl fmt::Display for FileModificationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Content => write!(f, "content"),
            Self::Metadata => write!(f, "metadata"),
            Self::Permissions => write!(f, "permissions"),
            Self::Size => write!(f, "size"),
            Self::Ownership => write!(f, "ownership"),
            Self::Multiple => write!(f, "multiple"),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Process Enums
// ─────────────────────────────────────────────────────────────

/// Reason for process shutdown
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ShutdownReason {
    /// Graceful shutdown requested
    Requested,
    /// Process crashed unexpectedly
    Crashed,
    /// Shutdown due to timeout
    Timeout,
    /// Resource limits exceeded (memory, CPU, etc.)
    ResourceLimit,
    /// External signal received
    Signal,
    /// Dependency failure
    DependencyFailed,
    /// Configuration error
    ConfigError,
    /// Unknown reason
    #[default]
    Unknown,
}

impl fmt::Display for ShutdownReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Requested => write!(f, "requested"),
            Self::Crashed => write!(f, "crashed"),
            Self::Timeout => write!(f, "timeout"),
            Self::ResourceLimit => write!(f, "resource_limit"),
            Self::Signal => write!(f, "signal"),
            Self::DependencyFailed => write!(f, "dependency_failed"),
            Self::ConfigError => write!(f, "config_error"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Reason for sensor deactivation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeactivationReason {
    /// Normal shutdown
    Shutdown,
    /// Error occurred
    Error,
    /// Resource not available
    ResourceUnavailable,
    /// Configuration change
    ConfigChange,
    /// User requested
    UserRequested,
}

impl fmt::Display for DeactivationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shutdown => write!(f, "shutdown"),
            Self::Error => write!(f, "error"),
            Self::ResourceUnavailable => write!(f, "resource_unavailable"),
            Self::ConfigChange => write!(f, "config_change"),
            Self::UserRequested => write!(f, "user_requested"),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// System Scan Enums
// ─────────────────────────────────────────────────────────────

/// Type of system scan
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ScanType {
    /// Full system scan
    Full,
    /// Incremental scan (only changes)
    #[default]
    Incremental,
    /// Targeted scan of specific paths/resources
    Targeted,
    /// Initial discovery scan
    Discovery,
    /// Quick health check scan
    Quick,
}

impl fmt::Display for ScanType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::Incremental => write!(f, "incremental"),
            Self::Targeted => write!(f, "targeted"),
            Self::Discovery => write!(f, "discovery"),
            Self::Quick => write!(f, "quick"),
        }
    }
}

/// Type of journal sync operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JournalSyncType {
    /// Initial full import
    InitialImport,
    /// Incremental sync from cursor
    Incremental,
    /// Full re-sync
    Full,
}

impl fmt::Display for JournalSyncType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitialImport => write!(f, "initial_import"),
            Self::Incremental => write!(f, "incremental"),
            Self::Full => write!(f, "full"),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// D-Bus Enums
// ─────────────────────────────────────────────────────────────

/// D-Bus bus type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DBusBus {
    /// System bus
    System,
    /// Session bus
    Session,
}

impl fmt::Display for DBusBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::System => write!(f, "system"),
            Self::Session => write!(f, "session"),
        }
    }
}

/// Media playback status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackStatus {
    /// Currently playing
    Playing,
    /// Paused
    Paused,
    /// Stopped
    Stopped,
}

impl fmt::Display for PlaybackStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Playing => write!(f, "playing"),
            Self::Paused => write!(f, "paused"),
            Self::Stopped => write!(f, "stopped"),
        }
    }
}

/// Media loop/repeat status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LoopStatus {
    /// No looping
    None,
    /// Loop current track
    Track,
    /// Loop playlist
    Playlist,
}

impl fmt::Display for LoopStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Track => write!(f, "track"),
            Self::Playlist => write!(f, "playlist"),
        }
    }
}

/// Power event type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PowerEventType {
    /// System going to sleep
    Sleep,
    /// System waking from sleep
    Wake,
    /// Shutdown initiated
    Shutdown,
    /// Reboot initiated
    Reboot,
    /// Hibernate
    Hibernate,
    /// Power profile changed
    ProfileChanged,
    /// Battery level changed
    BatteryChanged,
    /// AC power connected/disconnected
    PowerSourceChanged,
}

impl fmt::Display for PowerEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sleep => write!(f, "sleep"),
            Self::Wake => write!(f, "wake"),
            Self::Shutdown => write!(f, "shutdown"),
            Self::Reboot => write!(f, "reboot"),
            Self::Hibernate => write!(f, "hibernate"),
            Self::ProfileChanged => write!(f, "profile_changed"),
            Self::BatteryChanged => write!(f, "battery_changed"),
            Self::PowerSourceChanged => write!(f, "power_source_changed"),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Device Enums
// ─────────────────────────────────────────────────────────────

/// Bluetooth device event type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BluetoothEventType {
    /// Device connected
    Connected,
    /// Device disconnected
    Disconnected,
    /// Device paired
    Paired,
    /// Device unpaired
    Unpaired,
    /// Device discovered
    Discovered,
    /// Device properties changed
    PropertiesChanged,
}

impl fmt::Display for BluetoothEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connected => write!(f, "connected"),
            Self::Disconnected => write!(f, "disconnected"),
            Self::Paired => write!(f, "paired"),
            Self::Unpaired => write!(f, "unpaired"),
            Self::Discovered => write!(f, "discovered"),
            Self::PropertiesChanged => write!(f, "properties_changed"),
        }
    }
}

/// Generic hardware device event type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeviceEventType {
    /// Device added/connected
    Added,
    /// Device removed/disconnected
    Removed,
    /// Device state changed
    Changed,
}

impl fmt::Display for DeviceEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Added => write!(f, "added"),
            Self::Removed => write!(f, "removed"),
            Self::Changed => write!(f, "changed"),
        }
    }
}

/// Device/subsystem type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    /// USB device
    Usb,
    /// Storage device (disk, partition)
    Storage,
    /// Network interface
    Network,
    /// Input device (keyboard, mouse)
    Input,
    /// Audio device
    Audio,
    /// Video/display device
    Video,
    /// Bluetooth device
    Bluetooth,
    /// Battery
    Battery,
    /// Other/unknown device type
    Other,
}

impl fmt::Display for DeviceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usb => write!(f, "usb"),
            Self::Storage => write!(f, "storage"),
            Self::Network => write!(f, "network"),
            Self::Input => write!(f, "input"),
            Self::Audio => write!(f, "audio"),
            Self::Video => write!(f, "video"),
            Self::Bluetooth => write!(f, "bluetooth"),
            Self::Battery => write!(f, "battery"),
            Self::Other => write!(f, "other"),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Network Enums
// ─────────────────────────────────────────────────────────────

/// Network state change event type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NetworkEventType {
    /// Network connected
    Connected,
    /// Network disconnected
    Disconnected,
    /// IP address changed
    IpChanged,
    /// Connection state changed
    StateChanged,
}

impl fmt::Display for NetworkEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connected => write!(f, "connected"),
            Self::Disconnected => write!(f, "disconnected"),
            Self::IpChanged => write!(f, "ip_changed"),
            Self::StateChanged => write!(f, "state_changed"),
        }
    }
}

/// Network connection type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NetworkConnectionType {
    /// `WiFi` connection
    Wifi,
    /// Wired ethernet
    Ethernet,
    /// VPN tunnel
    Vpn,
    /// Mobile/cellular
    Cellular,
    /// Other connection type
    Other,
}

impl fmt::Display for NetworkConnectionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wifi => write!(f, "wifi"),
            Self::Ethernet => write!(f, "ethernet"),
            Self::Vpn => write!(f, "vpn"),
            Self::Cellular => write!(f, "cellular"),
            Self::Other => write!(f, "other"),
        }
    }
}

/// Network interface state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum NetworkState {
    /// Unknown state
    #[default]
    Unknown,
    /// Interface is down
    Down,
    /// Interface is disconnected
    Disconnected,
    /// Connecting
    Connecting,
    /// Connected
    Connected,
    /// Disconnecting
    Disconnecting,
}

impl fmt::Display for NetworkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => write!(f, "unknown"),
            Self::Down => write!(f, "down"),
            Self::Disconnected => write!(f, "disconnected"),
            Self::Connecting => write!(f, "connecting"),
            Self::Connected => write!(f, "connected"),
            Self::Disconnecting => write!(f, "disconnecting"),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Mount/Filesystem Enums
// ─────────────────────────────────────────────────────────────

/// Mount/unmount event type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MountEventType {
    /// Filesystem mounted
    Mounted,
    /// Filesystem unmounted
    Unmounted,
}

impl fmt::Display for MountEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mounted => write!(f, "mounted"),
            Self::Unmounted => write!(f, "unmounted"),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// udev Enums
// ─────────────────────────────────────────────────────────────

/// udev action type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UdevAction {
    /// Device added
    Add,
    /// Device removed
    Remove,
    /// Device changed
    Change,
    /// Driver bound
    Bind,
    /// Driver unbound
    Unbind,
    /// Other action
    Other,
}

impl fmt::Display for UdevAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Add => write!(f, "add"),
            Self::Remove => write!(f, "remove"),
            Self::Change => write!(f, "change"),
            Self::Bind => write!(f, "bind"),
            Self::Unbind => write!(f, "unbind"),
            Self::Other => write!(f, "other"),
        }
    }
}

impl std::str::FromStr for UdevAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "add" => Ok(Self::Add),
            "remove" => Ok(Self::Remove),
            "change" => Ok(Self::Change),
            "bind" => Ok(Self::Bind),
            "unbind" => Ok(Self::Unbind),
            _ => Ok(Self::Other),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Systemd Enums
// ─────────────────────────────────────────────────────────────

/// Systemd unit active state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SystemdActiveState {
    /// Unit is active
    Active,
    /// Unit is reloading
    Reloading,
    /// Unit is inactive
    Inactive,
    /// Unit failed
    Failed,
    /// Unit is activating
    Activating,
    /// Unit is deactivating
    Deactivating,
    /// Unit is in maintenance
    Maintenance,
}

impl fmt::Display for SystemdActiveState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Reloading => write!(f, "reloading"),
            Self::Inactive => write!(f, "inactive"),
            Self::Failed => write!(f, "failed"),
            Self::Activating => write!(f, "activating"),
            Self::Deactivating => write!(f, "deactivating"),
            Self::Maintenance => write!(f, "maintenance"),
        }
    }
}

impl std::str::FromStr for SystemdActiveState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "active" => Ok(Self::Active),
            "reloading" => Ok(Self::Reloading),
            "inactive" => Ok(Self::Inactive),
            "failed" => Ok(Self::Failed),
            "activating" => Ok(Self::Activating),
            "deactivating" => Ok(Self::Deactivating),
            "maintenance" => Ok(Self::Maintenance),
            _ => Err(format!("unknown systemd active state: {s}")),
        }
    }
}

/// Systemd unit type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SystemdUnitType {
    /// Service unit
    Service,
    /// Socket unit
    Socket,
    /// Target unit
    Target,
    /// Device unit
    Device,
    /// Mount unit
    Mount,
    /// Automount unit
    Automount,
    /// Timer unit
    Timer,
    /// Swap unit
    Swap,
    /// Path unit
    Path,
    /// Slice unit
    Slice,
    /// Scope unit
    Scope,
    /// Other unit type
    Other,
}

impl fmt::Display for SystemdUnitType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Service => write!(f, "service"),
            Self::Socket => write!(f, "socket"),
            Self::Target => write!(f, "target"),
            Self::Device => write!(f, "device"),
            Self::Mount => write!(f, "mount"),
            Self::Automount => write!(f, "automount"),
            Self::Timer => write!(f, "timer"),
            Self::Swap => write!(f, "swap"),
            Self::Path => write!(f, "path"),
            Self::Slice => write!(f, "slice"),
            Self::Scope => write!(f, "scope"),
            Self::Other => write!(f, "other"),
        }
    }
}

impl SystemdUnitType {
    /// Determine unit type from unit name suffix
    #[must_use]
    pub fn from_unit_name(name: &str) -> Self {
        if name.ends_with(".service") {
            Self::Service
        } else if name.ends_with(".socket") {
            Self::Socket
        } else if name.ends_with(".target") {
            Self::Target
        } else if name.ends_with(".device") {
            Self::Device
        } else if name.ends_with(".mount") {
            Self::Mount
        } else if name.ends_with(".automount") {
            Self::Automount
        } else if name.ends_with(".timer") {
            Self::Timer
        } else if name.ends_with(".swap") {
            Self::Swap
        } else if name.ends_with(".path") {
            Self::Path
        } else if name.ends_with(".slice") {
            Self::Slice
        } else if name.ends_with(".scope") {
            Self::Scope
        } else {
            Self::Other
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Shell/Terminal Enums
// ─────────────────────────────────────────────────────────────

/// Terminal/shell type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TerminalType {
    /// Kitty terminal
    Kitty,
    /// Alacritty terminal
    Alacritty,
    /// Foot terminal
    Foot,
    /// `WezTerm`
    Wezterm,
    /// iTerm2
    Iterm2,
    /// Generic/unknown terminal
    Other,
}

impl fmt::Display for TerminalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kitty => write!(f, "kitty"),
            Self::Alacritty => write!(f, "alacritty"),
            Self::Foot => write!(f, "foot"),
            Self::Wezterm => write!(f, "wezterm"),
            Self::Iterm2 => write!(f, "iterm2"),
            Self::Other => write!(f, "other"),
        }
    }
}
