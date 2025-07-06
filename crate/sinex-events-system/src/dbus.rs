use async_trait::async_trait;
use chrono::Utc;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{error, info};

use sinex_core::RawEvent;
use sinex_core::{
    sources, ChannelSenderExt, EventSender, EventSource, EventSourceContext, EventType, JsonValue,
    Result, Timestamp,
};

// ============================================================================
// Event Payloads
// ============================================================================

/// Generic D-Bus signal event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DbusSignalPayload {
    /// Bus type (session or system)
    pub bus: String,
    /// Sender (e.g., :1.234 or org.freedesktop.Notifications)
    pub sender: String,
    /// Object path (e.g., /org/freedesktop/Notifications)
    pub path: String,
    /// Interface (e.g., org.freedesktop.Notifications)
    pub interface: String,
    /// Signal name (e.g., NotificationClosed)
    pub signal: String,
    /// Signal arguments as JSON
    pub args: JsonValue,
    /// Timestamp
    pub timestamp: Timestamp,
}

/// D-Bus method call event (for important method calls)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DbusMethodCallPayload {
    pub bus: String,
    pub sender: String,
    pub destination: String,
    pub path: String,
    pub interface: String,
    pub method: String,
    pub args: JsonValue,
    pub timestamp: Timestamp,
}

/// Notification event (specialized from D-Bus signals)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NotificationPayload {
    pub app_name: String,
    pub summary: String,
    pub body: String,
    pub urgency: u8,
    pub timeout: i32,
    pub actions: Vec<String>,
    pub hints: HashMap<String, JsonValue>,
    pub timestamp: Timestamp,
}

/// Media playback event (from MPRIS interface)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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
    pub timestamp: Timestamp,
}

/// Power event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PowerEventPayload {
    pub event_type: String, // PrepareForSleep, PowerProfileChanged, etc.
    pub details: JsonValue,
    pub timestamp: Timestamp,
}

/// Hardware device event (via UDisks2, UPower, etc)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct HardwareEventPayload {
    pub device_type: String, // usb, disk, battery, bluetooth, etc
    pub event_type: String,  // added, removed, changed
    pub device_path: String,
    pub device_name: Option<String>,
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub properties: HashMap<String, JsonValue>,
    pub timestamp: Timestamp,
}

/// Session/idle event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SessionEventPayload {
    pub event_type: String, // idle, active, locked, unlocked
    pub session_id: Option<String>,
    pub idle_time_ms: Option<u64>,
    pub timestamp: Timestamp,
}

/// PolicyKit authorization event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PolicyKitEventPayload {
    pub action_id: String, // org.freedesktop.policykit.exec
    pub subject_pid: u32,
    pub subject_uid: u32,
    pub subject_executable: Option<String>,
    pub requesting_user: Option<String>,
    pub authorized: bool,
    pub challenge_occurred: bool,
    pub timestamp: Timestamp,
}

/// Bluetooth device event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BluetoothEventPayload {
    pub event_type: String, // connected, disconnected, paired, unpaired
    pub device_address: String,
    pub device_name: Option<String>,
    pub device_class: Option<String>,
    pub rssi: Option<i16>,
    pub connected: bool,
    pub paired: bool,
    pub trusted: bool,
    pub timestamp: Timestamp,
}

/// Network manager event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NetworkEventPayload {
    pub event_type: String, // connected, disconnected, ip_changed
    pub interface: String,
    pub connection_type: String, // wifi, ethernet, vpn
    pub ssid: Option<String>,
    pub ip_address: Option<String>,
    pub state: String,
    pub timestamp: Timestamp,
}

/// Screen saver/lock event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ScreenSaverEventPayload {
    pub active: bool,
    pub locked: bool,
    pub idle_time_ms: Option<u64>,
    pub timestamp: Timestamp,
}

/// Mount/unmount event
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MountEventPayload {
    pub event_type: String, // mounted, unmounted
    pub device: String,
    pub mount_point: String,
    pub filesystem: String,
    pub label: Option<String>,
    pub uuid: Option<String>,
    pub size_bytes: Option<u64>,
    pub timestamp: Timestamp,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct DbusSignal;
impl EventType for DbusSignal {
    type Payload = DbusSignalPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "signal.received";
}

pub struct DbusMethodCall;
impl EventType for DbusMethodCall {
    type Payload = DbusMethodCallPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "method.called";
}

pub struct SystemNotification;
impl EventType for SystemNotification {
    type Payload = NotificationPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "notification.sent";
}

pub struct MediaPlaybackChanged;
impl EventType for MediaPlaybackChanged {
    type Payload = MediaPlaybackPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "media.state_changed";
}

pub struct PowerEvent;
impl EventType for PowerEvent {
    type Payload = PowerEventPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "power.state_changed";
}

pub struct HardwareEvent;
impl EventType for HardwareEvent {
    type Payload = HardwareEventPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "device.connected";
}

pub struct SessionEvent;
impl EventType for SessionEvent {
    type Payload = SessionEventPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "session.state_changed";
}

pub struct PolicyKitEvent;
impl EventType for PolicyKitEvent {
    type Payload = PolicyKitEventPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "security.authorization";
}

pub struct BluetoothEvent;
impl EventType for BluetoothEvent {
    type Payload = BluetoothEventPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "bluetooth.device_changed";
}

pub struct NetworkEvent;
impl EventType for NetworkEvent {
    type Payload = NetworkEventPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "network.state_changed";
}

pub struct ScreenSaverEvent;
impl EventType for ScreenSaverEvent {
    type Payload = ScreenSaverEventPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "screensaver.state_changed";
}

pub struct MountEvent;
impl EventType for MountEvent {
    type Payload = MountEventPayload;
    type SourceImpl = DbusMonitor;
    const EVENT_NAME: &'static str = "mount.changed";
}

// ============================================================================
// Event Source Configuration
// ============================================================================

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
    pub extract_policykit: bool,
    pub extract_bluetooth: bool,
    pub extract_network: bool,
    pub extract_screensaver: bool,
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
            extract_policykit: true,
            extract_bluetooth: true,
            extract_network: true,
            extract_screensaver: true,
            extract_mounts: true,
        }
    }
}

// ============================================================================
// Event Source Implementation
// ============================================================================

pub struct DbusMonitor {
    config: DbusConfig,
}

#[async_trait]
impl EventSource for DbusMonitor {
    type Config = DbusConfig;

    const SOURCE_NAME: &'static str = sources::DBUS;

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config: Self::Config = serde_json::from_value(ctx.config).map_err(|e| {
            sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e))
        })?;

        info!("Initializing D-Bus monitor");
        Ok(Self { config })
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        info!("Starting D-Bus monitoring");

        let config = self.config.clone();

        // Monitor session bus
        if config.monitor_session {
            let tx_session = tx.clone();
            let config_session = config.clone();
            tokio::spawn(async move {
                if let Err(e) = monitor_bus("session", tx_session, config_session).await {
                    error!("Session bus monitoring failed: {}", e);
                }
            });
        }

        // Monitor system bus
        if config.monitor_system {
            let tx_system = tx.clone();
            let config_system = config.clone();
            tokio::spawn(async move {
                if let Err(e) = monitor_bus("system", tx_system, config_system).await {
                    error!("System bus monitoring failed: {}", e);
                }
            });
        }

        // Keep the main task alive
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        }
    }
}

async fn monitor_bus(bus_type: &str, tx: EventSender, config: DbusConfig) -> Result<()> {
    use dbus::channel::MatchingReceiver;
    use dbus::message::MatchRule;
    use dbus_tokio::connection;

    info!("Connecting to {} bus", bus_type);

    let (resource, conn) = if bus_type == "session" {
        connection::new_session_sync().map_err(|e| {
            sinex_core::CoreError::Other(format!("Failed to connect to session bus: {}", e))
        })?
    } else {
        connection::new_system_sync().map_err(|e| {
            sinex_core::CoreError::Other(format!("Failed to connect to system bus: {}", e))
        })?
    };

    // Spawn the connection resource
    let bus_type_owned = bus_type.to_string();
    tokio::spawn(async move {
        let err = resource.await;
        error!("D-Bus {} connection lost: {:?}", bus_type_owned, err);
    });

    // Add match rules for all message types we want to capture
    let signal_rule = MatchRule::new().with_type(dbus::message::MessageType::Signal);
    conn.add_match(signal_rule).await.map_err(|e| {
        sinex_core::CoreError::Other(format!("Failed to add signal match rule: {}", e))
    })?;

    let method_rule = MatchRule::new().with_type(dbus::message::MessageType::MethodCall);
    conn.add_match(method_rule).await.map_err(|e| {
        sinex_core::CoreError::Other(format!("Failed to add method call match rule: {}", e))
    })?;

    // Clone values we need for the async context
    let bus_type = bus_type.to_string();
    let tx_clone = tx.clone();
    let config_clone = config.clone();

    // Start receiving messages
    conn.start_receive(
        MatchRule::new(),
        Box::new(move |msg, _| {
            // Extract all data from the message synchronously
            let msg_type = msg.msg_type();
            let interface = msg.interface().map(|i| i.to_string());
            let path = msg.path().map(|p| p.to_string());
            let member = msg.member().map(|m| m.to_string());
            let sender = msg.sender().map(|s| s.to_string());
            let destination = msg.destination().map(|d| d.to_string());
            let args_json = message_args_to_json(&msg);

            // Clone for the async block
            let bus_type = bus_type.clone();
            let tx = tx_clone.clone();
            let config = config_clone.clone();

            // Process message in a separate task
            tokio::spawn(async move {
                if let Err(e) = process_extracted_message(
                    &bus_type,
                    msg_type,
                    interface,
                    path,
                    member,
                    sender,
                    destination,
                    args_json,
                    tx,
                    &config,
                )
                .await
                {
                    error!("Error processing message: {}", e);
                }
            });

            true
        }),
    );

    // Keep the connection alive
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_extracted_message(
    bus_type: &str,
    msg_type: dbus::message::MessageType,
    interface: Option<String>,
    path: Option<String>,
    member: Option<String>,
    sender: Option<String>,
    destination: Option<String>,
    args: JsonValue,
    tx: EventSender,
    config: &DbusConfig,
) -> Result<()> {
    use dbus::message::MessageType;

    let interface = interface.unwrap_or_default();
    let path = path.unwrap_or_default();
    let member = member.unwrap_or_default();

    // Check filters
    if !config.include_interfaces.is_empty()
        && !config
            .include_interfaces
            .iter()
            .any(|i| interface.starts_with(i))
    {
        return Ok(());
    }

    if config
        .exclude_interfaces
        .iter()
        .any(|i| interface.starts_with(i))
    {
        return Ok(());
    }

    match msg_type {
        MessageType::Signal => {
            // Extract specialized events based on interface
            if config.extract_notifications
                && interface == "org.freedesktop.Notifications"
                && member == "Notify"
            {
                let payload = parse_notification_args(&args);
                let event = create_event(
                    SystemNotification::EVENT_NAME,
                    serde_json::to_value(payload)?,
                );
                tx.send_or_log(event, "dbus_notification").await?;
            }

            if config.extract_media
                && interface.starts_with("org.mpris.MediaPlayer2")
                && member == "PropertiesChanged"
            {
                let player = sender
                    .as_deref()
                    .and_then(|s| s.split('.').next_back())
                    .unwrap_or("unknown");

                let mut payload = parse_mpris_properties(&args).unwrap_or_else(|| MediaPlaybackPayload {
                    player: player.to_string(),
                    player_instance: sender.clone().unwrap_or_default(),
                    status: "Unknown".to_string(),
                    track_id: None,
                    title: None,
                    artist: None,
                    album: None,
                    album_artist: None,
                    track_number: None,
                    length: None,
                    position: None,
                    volume: None,
                    loop_status: None,
                    shuffle: None,
                    can_go_next: false,
                    can_go_previous: false,
                    can_play: false,
                    can_pause: false,
                    can_seek: false,
                    art_url: None,
                    timestamp: Utc::now(),
                });
                
                // Set player info that we can extract from the sender
                payload.player = player.to_string();
                payload.player_instance = sender.clone().unwrap_or_default();

                let event = create_event(
                    MediaPlaybackChanged::EVENT_NAME,
                    serde_json::to_value(payload)?,
                );
                tx.send_or_log(event, "dbus_media_playback").await?;
            }

            if config.extract_power
                && ((interface == "org.freedesktop.login1.Manager"
                    && matches!(member.as_str(), "PrepareForSleep" | "PrepareForShutdown"))
                    || (interface == "org.freedesktop.UPower" && member == "DeviceChanged"))
            {
                let payload = PowerEventPayload {
                    event_type: member.clone(),
                    details: serde_json::json!({
                        "bus": bus_type,
                        "interface": interface,
                        "path": path,
                    }),
                    timestamp: Utc::now(),
                };

                let event = create_event(PowerEvent::EVENT_NAME, serde_json::to_value(payload)?);
                tx.send_or_log(event, "dbus_power_event").await?;
            }

            if config.extract_hardware
                && (interface.starts_with("org.freedesktop.UDisks2")
                    || interface == "org.freedesktop.UPower.Device")
            {
                let device_type = if interface.contains("UDisks2") {
                    "storage"
                } else {
                    "power"
                };

                let payload = HardwareEventPayload {
                    device_type: device_type.to_string(),
                    event_type: member.clone(),
                    device_path: path.clone(),
                    device_name: None,
                    vendor: None,
                    model: None,
                    serial: None,
                    properties: HashMap::new(),
                    timestamp: Utc::now(),
                };

                let event = create_event(HardwareEvent::EVENT_NAME, serde_json::to_value(payload)?);
                tx.send_or_log(event, "dbus_hardware_event").await?;
            }

            if config.extract_session
                && (interface == "org.freedesktop.login1.Session"
                    || interface == "org.gnome.SessionManager"
                    || interface == "org.freedesktop.ScreenSaver")
            {
                let payload = SessionEventPayload {
                    event_type: member.clone(),
                    session_id: None,
                    idle_time_ms: None,
                    timestamp: Utc::now(),
                };

                let event = create_event(SessionEvent::EVENT_NAME, serde_json::to_value(payload)?);
                tx.send_or_log(event, "dbus_session_event").await?;
            }

            if config.extract_bluetooth && interface.starts_with("org.bluez") {
                let payload = BluetoothEventPayload {
                    event_type: member.clone(),
                    device_address: "unknown".to_string(),
                    device_name: None,
                    device_class: None,
                    rssi: None,
                    connected: false,
                    paired: false,
                    trusted: false,
                    timestamp: Utc::now(),
                };

                let event =
                    create_event(BluetoothEvent::EVENT_NAME, serde_json::to_value(payload)?);
                tx.send_or_log(event, "dbus_bluetooth_event").await?;
            }

            if config.extract_network && interface.starts_with("org.freedesktop.NetworkManager") {
                let payload = NetworkEventPayload {
                    event_type: member.clone(),
                    interface: path.clone(),
                    connection_type: "unknown".to_string(),
                    ssid: None,
                    ip_address: None,
                    state: "unknown".to_string(),
                    timestamp: Utc::now(),
                };

                let event = create_event(NetworkEvent::EVENT_NAME, serde_json::to_value(payload)?);
                tx.send_or_log(event, "dbus_network_event").await?;
            }

            if config.extract_screensaver
                && (interface == "org.freedesktop.ScreenSaver"
                    || interface == "org.gnome.ScreenSaver")
            {
                let active = member == "ActiveChanged";

                let payload = ScreenSaverEventPayload {
                    active,
                    locked: false,
                    idle_time_ms: None,
                    timestamp: Utc::now(),
                };

                let event =
                    create_event(ScreenSaverEvent::EVENT_NAME, serde_json::to_value(payload)?);
                tx.send_or_log(event, "dbus_screensaver_event").await?;
            }

            if config.extract_mounts && interface == "org.freedesktop.UDisks2.Filesystem" {
                let mounted = member == "Mount";

                let payload = MountEventPayload {
                    event_type: if mounted { "mounted" } else { "unmounted" }.to_string(),
                    device: path.clone(),
                    mount_point: "/unknown".to_string(),
                    filesystem: "unknown".to_string(),
                    label: None,
                    uuid: None,
                    size_bytes: None,
                    timestamp: Utc::now(),
                };

                let event = create_event(MountEvent::EVENT_NAME, serde_json::to_value(payload)?);
                tx.send_or_log(event, "dbus_mount_event").await?;
            }

            // Always emit generic signal events (capture everything)
            let payload = DbusSignalPayload {
                bus: bus_type.to_string(),
                sender: sender.unwrap_or_default(),
                path: path.clone(),
                interface: interface.clone(),
                signal: member.clone(),
                args,
                timestamp: Utc::now(),
            };

            let event = create_event(DbusSignal::EVENT_NAME, serde_json::to_value(payload)?);
            tx.send_or_log(event, "dbus_generic_signal").await?;
        }
        MessageType::MethodCall => {
            // Extract PolicyKit events
            if config.extract_policykit
                && interface == "org.freedesktop.PolicyKit1.Authority"
                && member == "CheckAuthorization"
            {
                let payload = PolicyKitEventPayload {
                    action_id: "unknown".to_string(),
                    subject_pid: 0,
                    subject_uid: 0,
                    subject_executable: None,
                    requesting_user: None,
                    authorized: false,
                    challenge_occurred: false,
                    timestamp: Utc::now(),
                };

                let event =
                    create_event(PolicyKitEvent::EVENT_NAME, serde_json::to_value(payload)?);
                tx.send_or_log(event, "dbus_policykit_event").await?;
            }

            // Always log method calls (capture everything)
            let payload = DbusMethodCallPayload {
                bus: bus_type.to_string(),
                sender: sender.unwrap_or_default(),
                destination: destination.unwrap_or_default(),
                path: path.clone(),
                interface: interface.clone(),
                method: member.clone(),
                args,
                timestamp: Utc::now(),
            };

            let event = create_event(DbusMethodCall::EVENT_NAME, serde_json::to_value(payload)?);
            tx.send_or_log(event, "dbus_generic_method_call").await?;
        }
        _ => {} // Ignore other message types
    }

    Ok(())
}

// TODO: These functions are placeholder implementations for future D-Bus event extraction features
#[allow(dead_code)]
fn extract_notification_event(msg: &dbus::Message) -> Result<Option<RawEvent>> {
    if let Some(member) = msg.member() {
        if member == dbus::strings::Member::new("Notify").unwrap() {
            // For now, create a basic notification event
            // Full D-Bus argument parsing would be more complex
            let payload = NotificationPayload {
                app_name: "Unknown".to_string(),
                summary: format!("{:?}", msg),
                body: String::new(),
                urgency: 1,
                timeout: -1,
                actions: vec![],
                hints: HashMap::new(),
                timestamp: Utc::now(),
            };

            let event = create_event(
                SystemNotification::EVENT_NAME,
                serde_json::to_value(payload)?,
            );
            return Ok(Some(event));
        }
    }
    Ok(None)
}

#[allow(dead_code)]
fn extract_media_event(msg: &dbus::Message) -> Result<Option<RawEvent>> {
    if let Some(member) = msg.member() {
        if member == dbus::strings::Member::new("PropertiesChanged").unwrap() {
            // Extract media player state changes
            // This is simplified - real implementation would parse the properties
            let sender = msg.sender().map(|s| s.to_string()).unwrap_or_default();
            let player = sender.split('.').next_back().unwrap_or("unknown");

            let payload = MediaPlaybackPayload {
                player: player.to_string(),
                player_instance: sender.clone(),
                status: "Unknown".to_string(),
                track_id: None,
                title: None,
                artist: None,
                album: None,
                album_artist: None,
                track_number: None,
                length: None,
                position: None,
                volume: None,
                loop_status: None,
                shuffle: None,
                can_go_next: false,
                can_go_previous: false,
                can_play: false,
                can_pause: false,
                can_seek: false,
                art_url: None,
                timestamp: Utc::now(),
            };

            let event = create_event(
                MediaPlaybackChanged::EVENT_NAME,
                serde_json::to_value(payload)?,
            );
            return Ok(Some(event));
        }
    }
    Ok(None)
}

#[allow(dead_code)]
fn extract_power_event(msg: &dbus::Message) -> Result<Option<RawEvent>> {
    if let Some(member) = msg.member() {
        let event_type = match member.as_ref() {
            "PrepareForSleep" => Some("sleep"),
            "PrepareForShutdown" => Some("shutdown"),
            "PowerProfileChanged" => Some("profile_changed"),
            _ => None,
        };

        if let Some(event_type) = event_type {
            let payload = PowerEventPayload {
                event_type: event_type.to_string(),
                details: message_args_to_json(msg),
                timestamp: Utc::now(),
            };

            let event = create_event(PowerEvent::EVENT_NAME, serde_json::to_value(payload)?);
            return Ok(Some(event));
        }
    }
    Ok(None)
}

#[allow(dead_code)]
fn extract_hardware_event(msg: &dbus::Message, interface: &str) -> Result<Option<RawEvent>> {
    if let Some(member) = msg.member() {
        if member == dbus::strings::Member::new("PropertiesChanged").unwrap() {
            let path = msg.path().map(|p| p.to_string()).unwrap_or_default();

            let (device_type, event_type) = if interface.contains("UDisks2") {
                ("disk", "changed")
            } else if interface.contains("UPower") {
                ("battery", "changed")
            } else {
                ("unknown", "changed")
            };

            let payload = HardwareEventPayload {
                device_type: device_type.to_string(),
                event_type: event_type.to_string(),
                device_path: path,
                device_name: None,
                vendor: None,
                model: None,
                serial: None,
                properties: HashMap::new(),
                timestamp: Utc::now(),
            };

            let event = create_event(HardwareEvent::EVENT_NAME, serde_json::to_value(payload)?);
            return Ok(Some(event));
        }
    }
    Ok(None)
}

#[allow(dead_code)]
fn extract_session_event(msg: &dbus::Message, interface: &str) -> Result<Option<RawEvent>> {
    if let Some(member) = msg.member() {
        let event_type = match (interface, member.as_ref()) {
            (_, "Lock") => Some("locked"),
            (_, "Unlock") => Some("unlocked"),
            (_, "IdleChanged") => Some("idle"),
            (_, "ActiveChanged") => Some("active"),
            _ => None,
        };

        if let Some(event_type) = event_type {
            let payload = SessionEventPayload {
                event_type: event_type.to_string(),
                session_id: msg.path().map(|p| p.to_string()),
                idle_time_ms: None,
                timestamp: Utc::now(),
            };

            let event = create_event(SessionEvent::EVENT_NAME, serde_json::to_value(payload)?);
            return Ok(Some(event));
        }
    }
    Ok(None)
}

#[allow(dead_code)]
fn extract_bluetooth_event(msg: &dbus::Message) -> Result<Option<RawEvent>> {
    if let Some(member) = msg.member() {
        if member == dbus::strings::Member::new("PropertiesChanged").unwrap() {
            let path = msg.path().map(|p| p.to_string()).unwrap_or_default();
            let device_address = path.split('/').next_back().unwrap_or("unknown");

            let payload = BluetoothEventPayload {
                event_type: "changed".to_string(),
                device_address: device_address.to_string(),
                device_name: None,
                device_class: None,
                rssi: None,
                connected: false,
                paired: false,
                trusted: false,
                timestamp: Utc::now(),
            };

            let event = create_event(BluetoothEvent::EVENT_NAME, serde_json::to_value(payload)?);
            return Ok(Some(event));
        }
    }
    Ok(None)
}

#[allow(dead_code)]
fn extract_network_event(msg: &dbus::Message) -> Result<Option<RawEvent>> {
    if let Some(member) = msg.member() {
        let event_type = match member.as_ref() {
            "StateChanged" => Some("state_changed"),
            "DeviceAdded" => Some("device_added"),
            "DeviceRemoved" => Some("device_removed"),
            "ActiveConnectionAdded" => Some("connected"),
            "ActiveConnectionRemoved" => Some("disconnected"),
            _ => None,
        };

        if let Some(event_type) = event_type {
            let payload = NetworkEventPayload {
                event_type: event_type.to_string(),
                interface: "unknown".to_string(),
                connection_type: "unknown".to_string(),
                ssid: None,
                ip_address: None,
                state: "unknown".to_string(),
                timestamp: Utc::now(),
            };

            let event = create_event(NetworkEvent::EVENT_NAME, serde_json::to_value(payload)?);
            return Ok(Some(event));
        }
    }
    Ok(None)
}

#[allow(dead_code)]
fn extract_screensaver_event(msg: &dbus::Message) -> Result<Option<RawEvent>> {
    if let Some(member) = msg.member() {
        if member == dbus::strings::Member::new("ActiveChanged").unwrap() {
            let payload = ScreenSaverEventPayload {
                active: true, // Would need to parse args
                locked: false,
                idle_time_ms: None,
                timestamp: Utc::now(),
            };

            let event = create_event(ScreenSaverEvent::EVENT_NAME, serde_json::to_value(payload)?);
            return Ok(Some(event));
        }
    }
    Ok(None)
}

#[allow(dead_code)]
fn extract_mount_event(msg: &dbus::Message) -> Result<Option<RawEvent>> {
    if let Some(member) = msg.member() {
        let event_type = match member.as_ref() {
            "Mount" => Some("mounted"),
            "Unmount" => Some("unmounted"),
            _ => None,
        };

        if let Some(event_type) = event_type {
            let path = msg.path().map(|p| p.to_string()).unwrap_or_default();

            let payload = MountEventPayload {
                event_type: event_type.to_string(),
                device: path,
                mount_point: "unknown".to_string(),
                filesystem: "unknown".to_string(),
                label: None,
                uuid: None,
                size_bytes: None,
                timestamp: Utc::now(),
            };

            let event = create_event(MountEvent::EVENT_NAME, serde_json::to_value(payload)?);
            return Ok(Some(event));
        }
    }
    Ok(None)
}

#[allow(dead_code)]
fn extract_policykit_event(msg: &dbus::Message) -> Result<Option<RawEvent>> {
    if let Some(member) = msg.member() {
        if member == dbus::strings::Member::new("CheckAuthorization").unwrap() {
            let payload = PolicyKitEventPayload {
                action_id: "unknown".to_string(),
                subject_pid: 0,
                subject_uid: 0,
                subject_executable: None,
                requesting_user: None,
                authorized: false,
                challenge_occurred: false,
                timestamp: Utc::now(),
            };

            let event = create_event(PolicyKitEvent::EVENT_NAME, serde_json::to_value(payload)?);
            return Ok(Some(event));
        }
    }
    Ok(None)
}

fn message_args_to_json(msg: &dbus::Message) -> JsonValue {
    // For now, return simplified parsing - full D-Bus type parsing is complex
    // and would require extensive type matching. Focus on getting structured data.
    let mut args = Vec::new();
    let mut iter = msg.iter_init();
    
    // Extract basic argument types we can handle
    while iter.next() {
        if let Some(s) = iter.get::<&str>() {
            args.push(JsonValue::String(s.to_string()));
        } else if let Some(i) = iter.get::<i32>() {
            args.push(JsonValue::Number(serde_json::Number::from(i)));
        } else if let Some(b) = iter.get::<bool>() {
            args.push(JsonValue::Bool(b));
        } else {
            // For complex types, use debug representation
            args.push(JsonValue::String(format!("Complex type: {:?}", iter.arg_type())));
        }
    }
    
    JsonValue::Array(args)
}

fn parse_notification_args(args: &JsonValue) -> NotificationPayload {
    // Notification arguments: app_name, replaces_id, app_icon, summary, body, actions, hints, expire_timeout
    if let JsonValue::Array(arg_array) = args {
        let app_name = arg_array.get(0)
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();
        
        let summary = arg_array.get(3)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        
        let body = arg_array.get(4)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        
        let actions = arg_array.get(5)
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect())
            .unwrap_or_default();
        
        let hints = arg_array.get(6)
            .and_then(|v| parse_notification_hints(v))
            .unwrap_or_default();
        
        let timeout = arg_array.get(7)
            .and_then(|v| v.as_i64())
            .unwrap_or(-1) as i32;
        
        let urgency = hints.get("urgency")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as u8;
        
        NotificationPayload {
            app_name,
            summary,
            body,
            urgency,
            timeout,
            actions,
            hints,
            timestamp: Utc::now(),
        }
    } else {
        NotificationPayload {
            app_name: "Unknown".to_string(),
            summary: "Failed to parse".to_string(),
            body: String::new(),
            urgency: 1,
            timeout: -1,
            actions: vec![],
            hints: HashMap::new(),
            timestamp: Utc::now(),
        }
    }
}

fn parse_notification_hints(hints_value: &JsonValue) -> Option<HashMap<String, JsonValue>> {
    // Hints are a dict of string -> variant
    if let JsonValue::Array(dict_entries) = hints_value {
        let mut hints = HashMap::new();
        
        // Process dict entries (each is a key-value pair)
        for entry in dict_entries.chunks(2) {
            if entry.len() == 2 {
                if let (Some(key), Some(value)) = (entry[0].as_str(), entry.get(1)) {
                    hints.insert(key.to_string(), value.clone());
                }
            }
        }
        
        Some(hints)
    } else {
        None
    }
}

fn parse_mpris_properties(args: &JsonValue) -> Option<MediaPlaybackPayload> {
    // MPRIS PropertiesChanged args: interface_name, changed_properties, invalidated_properties
    if let JsonValue::Array(arg_array) = args {
        if let Some(changed_props) = arg_array.get(1) {
            let mut payload = MediaPlaybackPayload {
                player: "unknown".to_string(),
                player_instance: String::new(),
                status: "Unknown".to_string(),
                track_id: None,
                title: None,
                artist: None,
                album: None,
                album_artist: None,
                track_number: None,
                length: None,
                position: None,
                volume: None,
                loop_status: None,
                shuffle: None,
                can_go_next: false,
                can_go_previous: false,
                can_play: false,
                can_pause: false,
                can_seek: false,
                art_url: None,
                timestamp: Utc::now(),
            };
            
            if let JsonValue::Array(props) = changed_props {
                // Parse property changes
                for prop_entry in props.chunks(2) {
                    if prop_entry.len() == 2 {
                        if let (Some(key), Some(value)) = (prop_entry[0].as_str(), prop_entry.get(1)) {
                            match key {
                                "PlaybackStatus" => {
                                    payload.status = value.as_str().unwrap_or("Unknown").to_string();
                                }
                                "Metadata" => {
                                    if let Some(metadata) = parse_mpris_metadata(value) {
                                        payload.title = metadata.get("xesam:title").and_then(|v| v.as_str()).map(|s| s.to_string());
                                        payload.artist = metadata.get("xesam:artist").and_then(|v| v.as_array()).map(|arr| 
                                            arr.iter().filter_map(|v| v.as_str()).map(|s| s.to_string()).collect()
                                        );
                                        payload.album = metadata.get("xesam:album").and_then(|v| v.as_str()).map(|s| s.to_string());
                                        payload.track_number = metadata.get("xesam:trackNumber").and_then(|v| v.as_i64()).map(|i| i as i32);
                                        payload.length = metadata.get("mpris:length").and_then(|v| v.as_i64());
                                        payload.art_url = metadata.get("mpris:artUrl").and_then(|v| v.as_str()).map(|s| s.to_string());
                                    }
                                }
                                "Volume" => {
                                    payload.volume = value.as_f64();
                                }
                                "Position" => {
                                    payload.position = value.as_i64();
                                }
                                "LoopStatus" => {
                                    payload.loop_status = value.as_str().map(|s| s.to_string());
                                }
                                "Shuffle" => {
                                    payload.shuffle = value.as_bool();
                                }
                                "CanGoNext" => {
                                    payload.can_go_next = value.as_bool().unwrap_or(false);
                                }
                                "CanGoPrevious" => {
                                    payload.can_go_previous = value.as_bool().unwrap_or(false);
                                }
                                "CanPlay" => {
                                    payload.can_play = value.as_bool().unwrap_or(false);
                                }
                                "CanPause" => {
                                    payload.can_pause = value.as_bool().unwrap_or(false);
                                }
                                "CanSeek" => {
                                    payload.can_seek = value.as_bool().unwrap_or(false);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            
            Some(payload)
        } else {
            None
        }
    } else {
        None
    }
}

fn parse_mpris_metadata(metadata_value: &JsonValue) -> Option<HashMap<String, JsonValue>> {
    // Metadata is a dict of string -> variant
    if let JsonValue::Array(dict_entries) = metadata_value {
        let mut metadata = HashMap::new();
        
        for entry in dict_entries.chunks(2) {
            if entry.len() == 2 {
                if let (Some(key), Some(value)) = (entry[0].as_str(), entry.get(1)) {
                    metadata.insert(key.to_string(), value.clone());
                }
            }
        }
        
        Some(metadata)
    } else {
        None
    }
}


fn create_event(event_type: &str, payload: JsonValue) -> RawEvent {
    RawEvent {
        id: sinex_ulid::Ulid::new(),
        source: DbusMonitor::SOURCE_NAME.to_string(),
        event_type: event_type.to_string(),
        ts_ingest: Utc::now(),
        ts_orig: Some(Utc::now()),
        host: gethostname::gethostname().to_string_lossy().to_string(),
        ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        payload_schema_id: None,
        payload,
    }
}
