#![doc = include_str!("../docs/dbus_watcher.md")]

//! D-Bus watcher with real-time signal subscription.
//!
//! Provides advanced D-Bus monitoring with direct signal subscription,
//! rich metadata extraction, and specialized event parsing.

use crate::payloads::DbusConfig; // Only import what we need
use crate::WatcherMaterialContext;
use dbus::channel::MatchingReceiver;
use dbus::message::{MatchRule, MessageType};
use dbus_tokio::connection;
use serde_json::json;
use sinex_db::models::Event;
use sinex_primitives::events::{
    DbusBluetoothDeviceChangedPayload, DbusDeviceConnectedPayload, DbusMediaStateChangedPayload,
    DbusMethodCalledPayload, DbusMountEventPayload, DbusNetworkStateChangedPayload,
    DbusNotificationSentPayload, DbusPowerStateChangedPayload, DbusSignalPayload,
};
use sinex_primitives::{
    events::enums::{
        BluetoothEventType, DBusBus, DeviceType, MountEventType, NetworkConnectionType,
        NetworkEventType, NetworkState, PlaybackStatus, PowerEventType,
    },
    JsonValue,
};
use time::OffsetDateTime;

use sinex_node_sdk::NodeResult;
use std::sync::Arc;
use std::{collections::HashMap, fmt, str::FromStr, time::Duration};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// Channel buffer size for D-Bus message processing
// Increased from 1000 to 10,000 to handle busy systems without dropping messages
const DBUS_MESSAGE_CHANNEL_SIZE: usize = 10_000;

/// D-Bus bus type enumeration
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DBusType {
    Session,
    System,
}

/// D-Bus message data for worker pool processing
#[derive(Clone)]
struct DbusMessageData {
    msg_type: MessageType,
    interface: Option<String>,
    path: Option<String>,
    member: Option<String>,
    sender: Option<String>,
    destination: Option<String>,
    args_json: serde_json::Value,
}

impl fmt::Display for DBusType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DBusType::Session => write!(f, "session"),
            DBusType::System => write!(f, "system"),
        }
    }
}

impl FromStr for DBusType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "session" => Ok(DBusType::Session),
            "system" => Ok(DBusType::System),
            _ => Err(format!("Unsupported DBus type: {}", s)),
        }
    }
}

/// Convert D-Bus member name to PowerEventType
fn parse_power_event(member: &str) -> PowerEventType {
    match member {
        "PrepareForSleep" => PowerEventType::Sleep,
        "PrepareForShutdown" => PowerEventType::Shutdown,
        "DeviceChanged" => PowerEventType::BatteryChanged,
        _ => PowerEventType::ProfileChanged, // Default for unknown
    }
}

/// Convert D-Bus member name to BluetoothEventType
fn parse_bluetooth_event(member: &str) -> BluetoothEventType {
    match member {
        "Connected" => BluetoothEventType::Connected,
        "Disconnected" => BluetoothEventType::Disconnected,
        "Paired" => BluetoothEventType::Paired,
        "Unpaired" => BluetoothEventType::Unpaired,
        "DeviceAdded" | "DeviceDiscovered" => BluetoothEventType::Discovered,
        _ => BluetoothEventType::PropertiesChanged, // Default for property changes
    }
}

/// Convert D-Bus member name to NetworkEventType
fn parse_network_event(member: &str) -> NetworkEventType {
    match member {
        "Connected" | "Activated" => NetworkEventType::Connected,
        "Disconnected" | "Deactivated" => NetworkEventType::Disconnected,
        "IpChanged" | "Ip4ConfigChanged" | "Ip6ConfigChanged" => NetworkEventType::IpChanged,
        _ => NetworkEventType::StateChanged,
    }
}

/// Parse bus type string to DBusBus enum
fn parse_bus_type(bus_type: &str) -> DBusBus {
    match bus_type {
        "session" => DBusBus::Session,
        _ => DBusBus::System,
    }
}

/// Parse playback status string to PlaybackStatus enum
fn parse_playback_status(s: &str) -> PlaybackStatus {
    match s {
        "Playing" => PlaybackStatus::Playing,
        "Paused" => PlaybackStatus::Paused,
        _ => PlaybackStatus::Stopped,
    }
}

/// Configuration for monitoring a specific D-Bus bus
#[derive(Debug, Clone)]
struct MonitorConfig {
    bus_type: DBusType,
    tx: mpsc::Sender<Event<JsonValue>>,
    config: DbusConfig,
    material: WatcherMaterialContext,
}

/// Helper to create processing errors with consistent formatting
fn dbus_error(message: &str, source: impl std::fmt::Display) -> sinex_node_sdk::SinexError {
    sinex_node_sdk::SinexError::processing(format!("{}: {}", message, source))
}

/// D-Bus watcher with real-time signal subscription
pub struct DbusWatcher {
    config: DbusConfig,
}

impl DbusWatcher {
    /// Create new D-Bus watcher
    pub async fn new(config: DbusConfig) -> NodeResult<Self> {
        info!("D-Bus watcher initialized with config: {:?}", config);
        Ok(Self { config })
    }

    /// Start monitoring both session and system buses concurrently
    pub(crate) async fn start_streaming(
        &mut self,
        tx: mpsc::Sender<Event<JsonValue>>,
        material: WatcherMaterialContext,
    ) -> NodeResult<()> {
        info!("Starting D-Bus monitoring");

        let cancel_token = tokio_util::sync::CancellationToken::new();
        let mut tasks = Vec::new();

        // Monitor session bus if enabled
        if self.config.monitor_session {
            let monitor_config = MonitorConfig {
                bus_type: DBusType::Session,
                tx: tx.clone(),
                config: self.config.clone(),
                material: material.clone(),
            };
            let token = cancel_token.clone();
            tasks.push(tokio::spawn(async move {
                tokio::select! {
                    res = Self::monitor_bus_with_config(monitor_config) => res,
                    () = token.cancelled() => Ok(()),
                }
            }));
        }

        // Monitor system bus if enabled
        if self.config.monitor_system {
            let monitor_config = MonitorConfig {
                bus_type: DBusType::System,
                tx: tx.clone(),
                config: self.config.clone(),
                material: material.clone(),
            };
            let token = cancel_token.clone();
            tasks.push(tokio::spawn(async move {
                tokio::select! {
                    res = Self::monitor_bus_with_config(monitor_config) => res,
                    () = token.cancelled() => Ok(()),
                }
            }));
        }

        if tasks.is_empty() {
            warn!("No D-Bus buses enabled for monitoring");
            return Ok(());
        }

        // Wait for any task to complete (or fail) with panic handling
        let (result, index, remaining) = futures::future::select_all(tasks).await;

        // Signal cancellation to other tasks
        cancel_token.cancel();

        // Check if the task panicked
        match result {
            Ok(Ok(())) => {
                warn!("D-Bus monitoring task {} completed normally", index);
            }
            Ok(Err(e)) => {
                error!("D-Bus monitoring task {} failed: {}", index, e);
            }
            Err(e) => {
                error!("D-Bus monitoring task {} panicked: {:?}", index, e);
            }
        }

        // Await remaining tasks with timeout
        for task in remaining {
            let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
        }

        error!("D-Bus monitoring stopped unexpectedly");
        Ok(())
    }

    /// Monitor a specific D-Bus bus with configuration struct
    async fn monitor_bus_with_config(monitor_config: MonitorConfig) -> NodeResult<()> {
        Self::monitor_bus(
            monitor_config.bus_type,
            monitor_config.tx,
            monitor_config.config,
            monitor_config.material,
        )
        .await
    }

    /// Monitor a specific D-Bus bus with real-time signal subscription using tokio-retry
    async fn monitor_bus(
        bus_type: DBusType,
        tx: mpsc::Sender<Event<JsonValue>>,
        config: DbusConfig,
        material: WatcherMaterialContext,
    ) -> NodeResult<()> {
        use tokio_retry::{strategy::ExponentialBackoff, Retry};

        // Retry strategy: exponential backoff starting at 1s, capped at 30s, max 5 attempts
        // This handles transient D-Bus connection failures (service restarts, socket issues)
        let retry_strategy = ExponentialBackoff::from_millis(1000)
            .max_delay(Duration::from_secs(30))
            .take(5);

        let tx_clone = tx.clone();
        let config_clone = config.clone();
        let bus_type = Arc::new(bus_type);
        let material_clone = material.clone();
        Retry::spawn(retry_strategy, move || {
            let tx = tx_clone.clone();
            let config = config_clone.clone();
            let bt = bus_type.clone();
            let material = material_clone.clone();
            async move {
                match Self::monitor_bus_inner((*bt).clone(), &tx, &config, &material).await {
                    Ok(()) => {
                        let bt_str = bt.to_string();
                        warn!("D-Bus {} bus monitoring ended normally", bt_str);
                        Ok(())
                    }
                    Err(e) => {
                        let bt_str = bt.to_string();
                        error!("D-Bus {} bus monitoring failed: {}", bt_str, e);
                        Err(e)
                    }
                }
            }
        })
        .await
    }

    /// Inner monitoring loop with proper error handling
    async fn monitor_bus_inner(
        bus_type: DBusType,
        tx: &mpsc::Sender<Event<JsonValue>>,
        config: &DbusConfig,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        info!("Connecting to D-Bus {} bus", bus_type);

        // Establish D-Bus connection
        let (resource, conn) = match bus_type {
            DBusType::Session => connection::new_session_sync()
                .map_err(|e| dbus_error("Failed to connect to session bus", e))?,
            DBusType::System => connection::new_system_sync()
                .map_err(|e| dbus_error("Failed to connect to system bus", e))?,
        };

        // Spawn the connection resource handler
        let bus_type_owned = bus_type.to_string();
        tokio::spawn(async move {
            let err = resource.await;
            error!("D-Bus {} connection lost: {:?}", bus_type_owned, err);
        });

        // Add match rules for signals and method calls
        let signal_rule = MatchRule::new().with_type(MessageType::Signal);
        conn.add_match(signal_rule)
            .await
            .map_err(|e| dbus_error("Failed to add signal match rule", e))?;

        let method_rule = MatchRule::new().with_type(MessageType::MethodCall);
        conn.add_match(method_rule)
            .await
            .map_err(|e| dbus_error("Failed to add method call match rule", e))?;

        info!("D-Bus {} bus monitoring started", bus_type);

        // Create bounded channel for D-Bus messages to prevent task explosion
        let (msg_tx, msg_rx) = mpsc::channel::<DbusMessageData>(DBUS_MESSAGE_CHANNEL_SIZE);
        let msg_tx_clone = msg_tx.clone();

        // Activity tracker for connection health monitoring
        let activity_tracker = Arc::new(std::sync::Mutex::new(std::time::Instant::now()));

        // Spawn a single worker to process messages (Receiver is not clonable)
        {
            let tx = tx.clone();
            let config = config.clone();
            let bus_type_str = bus_type.to_string();
            let mut msg_rx = msg_rx;
            let material = material.clone();

            tokio::spawn(async move {
                debug!("D-Bus worker started for {} bus", bus_type_str);
                while let Some(msg_data) = msg_rx.recv().await {
                    if let Err(e) = Self::process_message(
                        &bus_type_str,
                        msg_data.msg_type,
                        msg_data.interface,
                        msg_data.path,
                        msg_data.member,
                        msg_data.sender,
                        msg_data.destination,
                        msg_data.args_json,
                        tx.clone(),
                        &config,
                        &material,
                    )
                    .await
                    {
                        error!("Failed to process D-Bus message: {}", e);
                    }
                }
                debug!("D-Bus worker stopped for {} bus", bus_type_str);
            });
        }

        // Set up message processing
        let activity_for_callback = activity_tracker.clone();

        // Start receiving messages
        conn.start_receive(
            MatchRule::new(),
            Box::new(move |msg, _| {
                // Update activity tracker
                if let Ok(mut last_activity) = activity_for_callback.lock() {
                    *last_activity = std::time::Instant::now();
                }

                // Extract message data synchronously
                let msg_data = DbusMessageData {
                    msg_type: msg.msg_type(),
                    interface: msg.interface().map(|i| i.to_string()),
                    path: msg.path().map(|p| p.to_string()),
                    member: msg.member().map(|m| m.to_string()),
                    sender: msg.sender().map(|s| s.to_string()),
                    destination: msg.destination().map(|d| d.to_string()),
                    args_json: Self::message_args_to_json(&msg),
                };

                // Send to worker pool via bounded channel
                // Try fast-path; if full, drop oldest to avoid unbounded growth
                if let Err(mpsc::error::TrySendError::Full(_)) = msg_tx.try_send(msg_data.clone()) {
                    // Drop one to make room, then enqueue the newest
                    let _ = msg_tx_clone.try_send(msg_data.clone());
                    if let Err(e) = msg_tx.try_send(msg_data) {
                        warn!(
                            "D-Bus message channel at capacity, dropping message due to backpressure: {}",
                            e
                        );
                    }
                }

                true
            }),
        );

        // Keep connection alive with periodic health checks
        // If no messages received for configured timeout, attempt reconnection
        let health_check_interval =
            Duration::from_secs(config.health_check_interval_secs.as_secs());
        let inactivity_timeout = Duration::from_secs(config.inactivity_timeout_secs.as_secs());

        loop {
            tokio::time::sleep(health_check_interval).await;

            // Check if we've received activity recently
            if let Ok(last) = activity_tracker.lock() {
                if last.elapsed() > inactivity_timeout {
                    warn!(
                        "D-Bus {} bus: No messages received for {}s, connection may be stale",
                        bus_type,
                        config.inactivity_timeout_secs.as_secs()
                    );
                    // Return error to trigger reconnection via retry mechanism
                    return Err(sinex_node_sdk::SinexError::processing(format!(
                        "D-Bus {} bus inactive for {}s",
                        bus_type,
                        config.inactivity_timeout_secs.as_secs()
                    )));
                }
            }
        }
    }

    /// Process extracted D-Bus message and generate appropriate events
    #[allow(clippy::too_many_arguments)]
    async fn process_message(
        bus_type: &str,
        msg_type: MessageType,
        interface: Option<String>,
        path: Option<String>,
        member: Option<String>,
        sender: Option<String>,
        destination: Option<String>,
        args: serde_json::Value,
        tx: mpsc::Sender<Event<JsonValue>>,
        config: &DbusConfig,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        let interface = interface.unwrap_or_default();
        let path = path.unwrap_or_default();
        let member = member.unwrap_or_default();

        // Apply filtering
        if !Self::passes_filters(&interface, config) {
            return Ok(());
        }

        let timestamp = sinex_primitives::temporal::now();

        match msg_type {
            MessageType::Signal => {
                Self::process_signal(
                    bus_type, &interface, &path, &member, &sender, &args, *timestamp, &tx, config,
                    material,
                )
                .await?;
            }
            MessageType::MethodCall => {
                Self::process_method_call(
                    bus_type,
                    &interface,
                    &path,
                    &member,
                    &sender,
                    &destination,
                    &args,
                    *timestamp,
                    &tx,
                    config,
                    material,
                )
                .await?;
            }
            _ => {} // Ignore other message types
        }

        Ok(())
    }

    /// Process D-Bus signals with specialized event extraction
    #[allow(clippy::too_many_arguments)]
    async fn process_signal(
        bus_type: &str,
        interface: &str,
        path: &str,
        member: &str,
        sender: &Option<String>,
        args: &serde_json::Value,
        timestamp: OffsetDateTime,
        tx: &mpsc::Sender<Event<JsonValue>>,
        config: &DbusConfig,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        // Extract specialized events based on interface
        if config.extract_notifications
            && interface == "org.freedesktop.Notifications"
            && member == "Notify"
        {
            let payload = Self::parse_notification_args(args, timestamp);
            let event = Event::new(payload, material.initial_provenance()).to_json_event()?;
            Self::send_event(tx, event, "dbus_notification", material).await?;
        }

        if config.extract_media
            && interface.starts_with("org.mpris.MediaPlayer2")
            && member == "PropertiesChanged"
        {
            let player = sender
                .as_deref()
                .and_then(|s| s.split('.').next_back())
                .unwrap_or("unknown");

            let payload = Self::parse_mpris_properties(args, player, sender, timestamp)
                .unwrap_or_else(|| Self::default_media_payload(player, sender, timestamp));

            let event = Event::new(payload, material.initial_provenance()).to_json_event()?;
            Self::send_event(tx, event, "dbus_media_playback", material).await?;
        }

        if config.extract_power
            && ((interface == "org.freedesktop.login1.Manager"
                && matches!(member, "PrepareForSleep" | "PrepareForShutdown"))
                || (interface == "org.freedesktop.UPower" && member == "DeviceChanged"))
        {
            let event = Event::new(
                DbusPowerStateChangedPayload {
                    event_type: parse_power_event(member),
                    details: json!({
                        "bus": bus_type,
                        "interface": interface,
                        "path": path,
                    }),
                    timestamp: timestamp.into(),
                },
                material.initial_provenance(),
            )
            .to_json_event()?;
            Self::send_event(tx, event, "dbus_power_event", material).await?;
        }

        if config.extract_hardware
            && (interface.starts_with("org.freedesktop.UDisks2")
                || interface == "org.freedesktop.UPower.Device")
        {
            let device_type = if interface.contains("UDisks2") {
                DeviceType::Storage
            } else {
                DeviceType::Battery
            };

            let event = Event::new(
                DbusDeviceConnectedPayload {
                    device_type,
                    event_type: member.to_string(),
                    device_path: path.to_string(),
                    device_name: None,
                    vendor: None,
                    model: None,
                    serial: None,
                    properties: HashMap::new(),
                    timestamp: timestamp.into(),
                },
                material.initial_provenance(),
            )
            .to_json_event()?;
            Self::send_event(tx, event, "dbus_hardware_event", material).await?;
        }

        if config.extract_bluetooth && interface.starts_with("org.bluez") {
            let event = Event::new(
                DbusBluetoothDeviceChangedPayload {
                    event_type: parse_bluetooth_event(member),
                    device_address: "unknown".to_string(),
                    device_name: None,
                    device_class: None,
                    rssi: None,
                    connected: false,
                    paired: false,
                    trusted: false,
                    timestamp: timestamp.into(),
                },
                material.initial_provenance(),
            )
            .to_json_event()?;
            Self::send_event(tx, event, "dbus_bluetooth_event", material).await?;
        }

        if config.extract_network && interface.starts_with("org.freedesktop.NetworkManager") {
            let event = Event::new(
                DbusNetworkStateChangedPayload {
                    event_type: parse_network_event(member),
                    interface: path.to_string(),
                    connection_type: NetworkConnectionType::Other,
                    ssid: None,
                    ip_address: None,
                    state: NetworkState::Unknown,
                    timestamp: timestamp.into(),
                },
                material.initial_provenance(),
            )
            .to_json_event()?;
            Self::send_event(tx, event, "dbus_network_event", material).await?;
        }

        if config.extract_mounts && interface == "org.freedesktop.UDisks2.Filesystem" {
            let mount_event_type = if member == "Mount" {
                MountEventType::Mounted
            } else {
                MountEventType::Unmounted
            };

            let event = Event::new(
                DbusMountEventPayload {
                    event_type: mount_event_type,
                    device: path.to_string(),
                    mount_point: "/unknown".to_string(),
                    filesystem: "unknown".to_string(),
                    label: None,
                    uuid: None,
                    size_bytes: None,
                    timestamp: timestamp.into(),
                },
                material.initial_provenance(),
            )
            .to_json_event()?;
            Self::send_event(tx, event, "dbus_mount_event", material).await?;
        }

        // Always emit generic signal events
        let event = Event::new(
            DbusSignalPayload {
                bus: parse_bus_type(bus_type),
                sender: sender.as_deref().unwrap_or_default().to_string(),
                path: path.to_string(),
                interface: interface.to_string(),
                signal: member.to_string(),
                args: args.clone(),
                timestamp: timestamp.into(),
            },
            material.initial_provenance(),
        )
        .to_json_event()?;
        Self::send_event(tx, event, "dbus_generic_signal", material).await?;

        Ok(())
    }

    /// Process D-Bus method calls
    #[allow(clippy::too_many_arguments)]
    async fn process_method_call(
        bus_type: &str,
        interface: &str,
        path: &str,
        member: &str,
        sender: &Option<String>,
        destination: &Option<String>,
        args: &serde_json::Value,
        timestamp: OffsetDateTime,
        tx: &mpsc::Sender<Event<JsonValue>>,
        _config: &DbusConfig,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        let event = Event::new(
            DbusMethodCalledPayload {
                bus: parse_bus_type(bus_type),
                sender: sender.as_deref().unwrap_or_default().to_string(),
                destination: destination.as_deref().unwrap_or_default().to_string(),
                path: path.to_string(),
                interface: interface.to_string(),
                method: member.to_string(),
                args: args.clone(),
                timestamp: timestamp.into(),
            },
            material.initial_provenance(),
        )
        .to_json_event()?;
        Self::send_event(tx, event, "dbus_generic_method_call", material).await?;

        Ok(())
    }

    /// Check if interface passes include/exclude filters
    fn passes_filters(interface: &str, config: &DbusConfig) -> bool {
        // Check include list
        if !config.include_interfaces.is_empty()
            && !config
                .include_interfaces
                .iter()
                .any(|i| interface.starts_with(i))
        {
            return false;
        }

        // Check exclude list
        if config
            .exclude_interfaces
            .iter()
            .any(|i| interface.starts_with(i))
        {
            return false;
        }

        true
    }

    /// Convert D-Bus message arguments to JSON
    fn message_args_to_json(msg: &dbus::Message) -> serde_json::Value {
        let mut args = Vec::new();
        let mut iter = msg.iter_init();

        while iter.next() {
            args.push(Self::parse_dbus_argument(&mut iter));
        }

        serde_json::Value::Array(args)
    }

    /// Parse individual D-Bus argument to JSON
    fn parse_dbus_argument(iter: &mut dbus::arg::Iter) -> serde_json::Value {
        use dbus::arg::ArgType;

        match iter.arg_type() {
            ArgType::String => iter.get::<&str>().map_or(serde_json::Value::Null, |s| {
                serde_json::Value::String(s.to_string())
            }),
            ArgType::Int32 => iter.get::<i32>().map_or(serde_json::Value::Null, |i| {
                serde_json::Value::Number(serde_json::Number::from(i))
            }),
            ArgType::UInt32 => iter.get::<u32>().map_or(serde_json::Value::Null, |i| {
                serde_json::Value::Number(serde_json::Number::from(i))
            }),
            ArgType::Boolean => iter
                .get::<bool>()
                .map_or(serde_json::Value::Null, serde_json::Value::Bool),
            ArgType::Array => Self::parse_dbus_array(iter),
            ArgType::DictEntry => Self::parse_dbus_dict_entry(iter),
            ArgType::Variant => Self::parse_dbus_variant(iter),
            ArgType::Struct => Self::parse_dbus_struct(iter),
            ArgType::ObjectPath => iter
                .get::<dbus::Path>()
                .map_or(serde_json::Value::Null, |p| {
                    serde_json::Value::String(p.to_string())
                }),
            _ => serde_json::Value::String(format!("unsupported_type_{:?}", iter.arg_type())),
        }
    }

    /// Parse D-Bus array to JSON
    fn parse_dbus_array(iter: &mut dbus::arg::Iter) -> serde_json::Value {
        let mut array_values = Vec::new();

        if let Some(mut array_iter) = iter.recurse(dbus::arg::ArgType::Array) {
            while array_iter.next() {
                array_values.push(Self::parse_dbus_argument(&mut array_iter));
            }
        }

        serde_json::Value::Array(array_values)
    }

    /// Parse D-Bus dict entry to JSON
    fn parse_dbus_dict_entry(iter: &mut dbus::arg::Iter) -> serde_json::Value {
        let mut dict_obj = serde_json::Map::new();

        if let Some(mut dict_iter) = iter.recurse(dbus::arg::ArgType::DictEntry) {
            if dict_iter.next() {
                let key = Self::parse_dbus_argument(&mut dict_iter);
                if dict_iter.next() {
                    let value = Self::parse_dbus_argument(&mut dict_iter);

                    let key_str = match key {
                        serde_json::Value::String(s) => s,
                        _ => format!("{:?}", key),
                    };

                    dict_obj.insert(key_str, value);
                }
            }
        }

        serde_json::Value::Object(dict_obj)
    }

    /// Parse D-Bus variant to JSON
    fn parse_dbus_variant(iter: &mut dbus::arg::Iter) -> serde_json::Value {
        if let Some(mut variant_iter) = iter.recurse(dbus::arg::ArgType::Variant) {
            if variant_iter.next() {
                return Self::parse_dbus_argument(&mut variant_iter);
            }
        }

        serde_json::Value::Null
    }

    /// Parse D-Bus struct to JSON
    fn parse_dbus_struct(iter: &mut dbus::arg::Iter) -> serde_json::Value {
        let mut struct_values = Vec::new();

        if let Some(mut struct_iter) = iter.recurse(dbus::arg::ArgType::Struct) {
            while struct_iter.next() {
                struct_values.push(Self::parse_dbus_argument(&mut struct_iter));
            }
        }

        serde_json::Value::Array(struct_values)
    }

    /// Parse notification arguments into structured payload
    fn parse_notification_args(
        args: &serde_json::Value,
        timestamp: OffsetDateTime,
    ) -> DbusNotificationSentPayload {
        if let serde_json::Value::Array(arg_array) = args {
            let app_name = arg_array
                .first()
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown")
                .to_string();

            let summary = arg_array
                .get(3)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let body = arg_array
                .get(4)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let actions = arg_array
                .get(5)
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .map(|s| s.to_string())
                        .collect()
                })
                .unwrap_or_default();

            let hints = arg_array
                .get(6)
                .and_then(Self::parse_notification_hints)
                .unwrap_or_default();

            let timeout = arg_array.get(7).and_then(|v| v.as_i64()).unwrap_or(-1) as i32;

            let urgency = hints.get("urgency").and_then(|v| v.as_u64()).unwrap_or(1) as u8;

            DbusNotificationSentPayload {
                app_name,
                summary,
                body,
                urgency,
                timeout,
                actions,
                hints,
                timestamp: timestamp.into(),
            }
        } else {
            DbusNotificationSentPayload {
                app_name: "Unknown".to_string(),
                summary: "Failed to parse".to_string(),
                body: String::new(),
                urgency: 1,
                timeout: -1,
                actions: vec![],
                hints: HashMap::with_capacity(4), // Typical notification hints: urgency, category, desktop-entry, etc.
                timestamp: timestamp.into(),
            }
        }
    }

    /// Parse notification hints from D-Bus arguments
    fn parse_notification_hints(
        hints_value: &serde_json::Value,
    ) -> Option<HashMap<String, serde_json::Value>> {
        if let serde_json::Value::Array(dict_entries) = hints_value {
            Some(
                dict_entries
                    .iter()
                    .filter_map(|entry| entry.as_object())
                    .flat_map(|obj| obj.iter())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            )
        } else {
            None
        }
    }

    /// Parse MPRIS properties into media playback payload
    fn parse_mpris_properties(
        args: &serde_json::Value,
        player: &str,
        sender: &Option<String>,
        timestamp: OffsetDateTime,
    ) -> Option<DbusMediaStateChangedPayload> {
        if let serde_json::Value::Array(arg_array) = args {
            if let Some(changed_props) = arg_array.get(1) {
                let mut payload = Self::default_media_payload(player, sender, timestamp);

                if let serde_json::Value::Array(props) = changed_props {
                    for prop_entry in props {
                        if let serde_json::Value::Object(obj) = prop_entry {
                            for (key, value) in obj {
                                match key.as_str() {
                                    "PlaybackStatus" => {
                                        payload.status = parse_playback_status(
                                            value.as_str().unwrap_or("Stopped"),
                                        );
                                    }
                                    "Volume" => {
                                        payload.volume = value.as_f64();
                                    }
                                    "Position" => {
                                        payload.position = value.as_i64();
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

    /// Create default media payload
    fn default_media_payload(
        player: &str,
        sender: &Option<String>,
        timestamp: OffsetDateTime,
    ) -> DbusMediaStateChangedPayload {
        DbusMediaStateChangedPayload {
            player: player.to_string(),
            player_instance: sender.as_deref().unwrap_or_default().to_string(),
            status: PlaybackStatus::Stopped,
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
            timestamp: timestamp.into(),
        }
    }

    /// Send event with error logging
    async fn send_event(
        tx: &mpsc::Sender<Event<JsonValue>>,
        mut event: Event<JsonValue>,
        context: &str,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        material.decorate_event(&mut event).await?;
        if let Err(err) = tx.send(event).await {
            warn!("Event channel closed while sending {}: {}", context, err);
        }
        Ok(())
    }
}
