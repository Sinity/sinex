//! D-Bus watcher with real-time signal subscription
//!
//! This module provides advanced D-Bus monitoring with direct signal subscription,
//! rich metadata extraction, and specialized event parsing. Ported from the

use crate::payloads::DbusConfig; // Only import what we need
use dbus::channel::MatchingReceiver;
use dbus::message::{MatchRule, MessageType};
use dbus_tokio::connection;
use serde_json::json;
use sinex_core::types::events::{
    DbusBluetoothDeviceChangedPayload, DbusDeviceConnectedPayload, DbusMediaStateChangedPayload,
    DbusMethodCalledPayload, DbusMountEventPayload, DbusNetworkStateChangedPayload,
    DbusNotificationSentPayload, DbusPowerStateChangedPayload, DbusSignalPayload, Event,
};
use sinex_core::RawEvent;
use sinex_satellite_sdk::SatelliteResult;
use std::{collections::HashMap, fmt, str::FromStr, time::Duration};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// D-Bus bus type enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum DBusType {
    Session,
    System,
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

/// Configuration for monitoring a specific D-Bus bus
#[derive(Debug, Clone)]
struct MonitorConfig {
    bus_type: DBusType,
    tx: mpsc::UnboundedSender<RawEvent>,
    config: DbusConfig,
}

/// Helper to create processing errors with consistent formatting
fn dbus_error(
    message: &str,
    source: impl std::fmt::Display,
) -> sinex_satellite_sdk::SatelliteError {
    use sinex_satellite_sdk::SatelliteError::Processing;
    Processing(format!("{}: {}", message, source))
}

/// D-Bus watcher with real-time signal subscription
pub struct DbusWatcher {
    config: DbusConfig,
}

impl DbusWatcher {
    /// Create new D-Bus watcher
    pub async fn new(config: DbusConfig) -> SatelliteResult<Self> {
        info!("D-Bus watcher initialized with config: {:?}", config);
        Ok(Self { config })
    }

    /// Start monitoring both session and system buses concurrently
    pub async fn start_streaming(
        &mut self,
        tx: mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        info!("Starting D-Bus monitoring");

        let mut tasks = Vec::new();

        // Monitor session bus if enabled
        if self.config.monitor_session {
            let monitor_config = MonitorConfig {
                bus_type: DBusType::Session,
                tx: tx.clone(),
                config: self.config.clone(),
            };
            tasks.push(tokio::spawn(async move {
                Self::monitor_bus_with_config(monitor_config).await
            }));
        }

        // Monitor system bus if enabled
        if self.config.monitor_system {
            let monitor_config = MonitorConfig {
                bus_type: DBusType::System,
                tx: tx.clone(),
                config: self.config.clone(),
            };
            tasks.push(tokio::spawn(async move {
                Self::monitor_bus_with_config(monitor_config).await
            }));
        }

        if tasks.is_empty() {
            warn!("No D-Bus buses enabled for monitoring");
            return Ok(());
        }

        // Wait for any task to complete (or fail)
        let (_result, _index, _remaining) = futures::future::select_all(tasks).await;

        error!("D-Bus monitoring task stopped unexpectedly");
        Ok(())
    }

    /// Monitor a specific D-Bus bus with configuration struct
    async fn monitor_bus_with_config(monitor_config: MonitorConfig) -> SatelliteResult<()> {
        Self::monitor_bus(
            monitor_config.bus_type,
            monitor_config.tx,
            monitor_config.config,
        )
        .await
    }

    /// Monitor a specific D-Bus bus with real-time signal subscription using tokio-retry
    async fn monitor_bus(
        bus_type: DBusType,
        tx: mpsc::UnboundedSender<RawEvent>,
        config: DbusConfig,
    ) -> SatelliteResult<()> {
        use tokio_retry::{strategy::ExponentialBackoff, Retry};

        let retry_strategy = ExponentialBackoff::from_millis(1000)
            .max_delay(Duration::from_secs(30))
            .take(5);

        Retry::spawn(retry_strategy, || async {
            match Self::monitor_bus_inner(bus_type, &tx, &config).await {
                Ok(()) => {
                    warn!("D-Bus {} bus monitoring ended normally", bus_type);
                    Ok(())
                }
                Err(e) => {
                    error!("D-Bus {} bus monitoring failed: {}", bus_type, e);
                    Err(e)
                }
            }
        })
        .await
    }

    /// Inner monitoring loop with proper error handling
    async fn monitor_bus_inner(
        bus_type: DBusType,
        tx: &mpsc::UnboundedSender<RawEvent>,
        config: &DbusConfig,
    ) -> SatelliteResult<()> {
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

        // Set up message processing
        let bus_type = bus_type.to_string();
        let tx_clone = tx.clone();
        let config_clone = config.clone();

        // Start receiving messages
        conn.start_receive(
            MatchRule::new(),
            Box::new(move |msg, _| {
                // Extract message data synchronously
                let msg_type = msg.msg_type();
                let interface = msg.interface().map(|i| i.to_string());
                let path = msg.path().map(|p| p.to_string());
                let member = msg.member().map(|m| m.to_string());
                let sender = msg.sender().map(|s| s.to_string());
                let destination = msg.destination().map(|d| d.to_string());
                let args_json = Self::message_args_to_json(&msg);

                // Clone for async processing
                let bus_type = bus_type.clone();
                let tx = tx_clone.clone();
                let config = config_clone.clone();

                // Process message in separate task
                tokio::spawn(async move {
                    if let Err(e) = Self::process_message(
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
                        debug!("Error processing D-Bus message: {}", e);
                    }
                });

                true
            }),
        );

        // Keep connection alive
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
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
        tx: mpsc::UnboundedSender<RawEvent>,
        config: &DbusConfig,
    ) -> SatelliteResult<()> {
        let interface = interface.unwrap_or_default();
        let path = path.unwrap_or_default();
        let member = member.unwrap_or_default();

        // Apply filtering
        if !Self::passes_filters(&interface, config) {
            return Ok(());
        }

        let timestamp = chrono::Utc::now().to_rfc3339();

        match msg_type {
            MessageType::Signal => {
                Self::process_signal(
                    bus_type, &interface, &path, &member, &sender, &args, timestamp, &tx, config,
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
                    timestamp,
                    &tx,
                    config,
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
        timestamp: String,
        tx: &mpsc::UnboundedSender<RawEvent>,
        config: &DbusConfig,
    ) -> SatelliteResult<()> {
        // Extract specialized events based on interface
        if config.extract_notifications
            && interface == "org.freedesktop.Notifications"
            && member == "Notify"
        {
            let payload = Self::parse_notification_args(args, timestamp.clone());
            let event: RawEvent = Event::new(payload).into();
            Self::send_event(tx, event, "dbus_notification").await?;
        }

        if config.extract_media
            && interface.starts_with("org.mpris.MediaPlayer2")
            && member == "PropertiesChanged"
        {
            let player = sender
                .as_deref()
                .and_then(|s| s.split('.').next_back())
                .unwrap_or("unknown");

            let payload = Self::parse_mpris_properties(args, player, sender, timestamp.clone())
                .unwrap_or_else(|| Self::default_media_payload(player, sender, timestamp.clone()));

            let event: RawEvent = Event::new(payload).into();
            Self::send_event(tx, event, "dbus_media_playback").await?;
        }

        if config.extract_power
            && ((interface == "org.freedesktop.login1.Manager"
                && matches!(member, "PrepareForSleep" | "PrepareForShutdown"))
                || (interface == "org.freedesktop.UPower" && member == "DeviceChanged"))
        {
            let event: RawEvent = Event::new(DbusPowerStateChangedPayload {
                event_type: member.to_string(),
                details: json!({
                    "bus": bus_type,
                    "interface": interface,
                    "path": path,
                }),
                timestamp: timestamp.clone(),
            })
            .into();
            Self::send_event(tx, event, "dbus_power_event").await?;
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

            let event: RawEvent = Event::new(DbusDeviceConnectedPayload {
                device_type: device_type.to_string(),
                event_type: member.to_string(),
                device_path: path.to_string(),
                device_name: None,
                vendor: None,
                model: None,
                serial: None,
                properties: HashMap::new(),
                timestamp: timestamp.clone(),
            })
            .into();
            Self::send_event(tx, event, "dbus_hardware_event").await?;
        }

        if config.extract_bluetooth && interface.starts_with("org.bluez") {
            let event: RawEvent = Event::new(DbusBluetoothDeviceChangedPayload {
                event_type: member.to_string(),
                device_address: "unknown".to_string(),
                device_name: None,
                device_class: None,
                rssi: None,
                connected: false,
                paired: false,
                trusted: false,
                timestamp: timestamp.clone(),
            })
            .into();
            Self::send_event(tx, event, "dbus_bluetooth_event").await?;
        }

        if config.extract_network && interface.starts_with("org.freedesktop.NetworkManager") {
            let event: RawEvent = Event::new(DbusNetworkStateChangedPayload {
                event_type: member.to_string(),
                interface: path.to_string(),
                connection_type: "unknown".to_string(),
                ssid: None,
                ip_address: None,
                state: "unknown".to_string(),
                timestamp: timestamp.clone(),
            })
            .into();
            Self::send_event(tx, event, "dbus_network_event").await?;
        }

        if config.extract_mounts && interface == "org.freedesktop.UDisks2.Filesystem" {
            let mounted = member == "Mount";

            let event: RawEvent = Event::new(DbusMountEventPayload {
                event_type: if mounted { "mounted" } else { "unmounted" }.to_string(),
                device: path.to_string(),
                mount_point: "/unknown".to_string(),
                filesystem: "unknown".to_string(),
                label: None,
                uuid: None,
                size_bytes: None,
                timestamp: timestamp.clone(),
            })
            .into();
            Self::send_event(tx, event, "dbus_mount_event").await?;
        }

        // Always emit generic signal events
        let event: RawEvent = Event::new(DbusSignalPayload {
            bus: bus_type.to_string(),
            sender: sender.as_deref().unwrap_or_default().to_string(),
            path: path.to_string(),
            interface: interface.to_string(),
            signal: member.to_string(),
            args: args.clone(),
            timestamp,
        })
        .into();
        Self::send_event(tx, event, "dbus_generic_signal").await?;

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
        timestamp: String,
        tx: &mpsc::UnboundedSender<RawEvent>,
        _config: &DbusConfig,
    ) -> SatelliteResult<()> {
        let event: RawEvent = Event::new(DbusMethodCalledPayload {
            bus: bus_type.to_string(),
            sender: sender.as_deref().unwrap_or_default().to_string(),
            destination: destination.as_deref().unwrap_or_default().to_string(),
            path: path.to_string(),
            interface: interface.to_string(),
            method: member.to_string(),
            args: args.clone(),
            timestamp,
        })
        .into();
        Self::send_event(tx, event, "dbus_generic_method_call").await?;

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
            ArgType::String => iter
                .get::<&str>()
                .map(|s| serde_json::Value::String(s.to_string()))
                .unwrap_or(serde_json::Value::Null),
            ArgType::Int32 => iter
                .get::<i32>()
                .map(|i| serde_json::Value::Number(serde_json::Number::from(i)))
                .unwrap_or(serde_json::Value::Null),
            ArgType::UInt32 => iter
                .get::<u32>()
                .map(|i| serde_json::Value::Number(serde_json::Number::from(i)))
                .unwrap_or(serde_json::Value::Null),
            ArgType::Boolean => iter
                .get::<bool>()
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null),
            ArgType::Array => Self::parse_dbus_array(iter),
            ArgType::DictEntry => Self::parse_dbus_dict_entry(iter),
            ArgType::Variant => Self::parse_dbus_variant(iter),
            ArgType::Struct => Self::parse_dbus_struct(iter),
            ArgType::ObjectPath => iter
                .get::<dbus::Path>()
                .map(|p| serde_json::Value::String(p.to_string()))
                .unwrap_or(serde_json::Value::Null),
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
        timestamp: String,
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
                timestamp,
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
                timestamp,
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
        timestamp: String,
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
                                        payload.status =
                                            value.as_str().unwrap_or("Unknown").to_string();
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
        timestamp: String,
    ) -> DbusMediaStateChangedPayload {
        DbusMediaStateChangedPayload {
            player: player.to_string(),
            player_instance: sender.as_deref().unwrap_or_default().to_string(),
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
            timestamp,
        }
    }

    /// Send event with error logging
    async fn send_event(
        tx: &mpsc::UnboundedSender<RawEvent>,
        event: RawEvent,
        context: &str,
    ) -> SatelliteResult<()> {
        if tx.send(event).is_err() {
            warn!("Event channel closed while sending {}", context);
        }
        Ok(())
    }
}
