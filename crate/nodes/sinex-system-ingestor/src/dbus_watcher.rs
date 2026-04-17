#![doc = include_str!("../docs/dbus_watcher.md")]

//! D-Bus watcher with real-time signal subscription.
//!
//! Provides advanced D-Bus monitoring with direct signal subscription,
//! rich metadata extraction, and specialized event parsing.

use crate::WatcherMaterialContext;
use crate::payloads::DbusConfig; // Only import what we need
use dbus::channel::MatchingReceiver;
use dbus::message::{MatchRule, MessageType};
use dbus_tokio::connection;
use parking_lot::Mutex;
use serde_json::json;
use sinex_db::models::Event;
use sinex_primitives::events::{
    DbusBluetoothDeviceChangedPayload, DbusDeviceConnectedPayload, DbusMediaStateChangedPayload,
    DbusMethodCalledPayload, DbusMountEventPayload, DbusNetworkStateChangedPayload,
    DbusNotificationSentPayload, DbusPowerStateChangedPayload, DbusSignalPayload,
};
use sinex_primitives::{
    JsonValue,
    events::enums::{
        BluetoothEventType, DBusBus, DeviceType, MountEventType, NetworkConnectionType,
        NetworkEventType, NetworkState, PlaybackStatus, PowerEventType,
    },
    privacy::{self, ProcessingContext},
    temporal::Timestamp,
};

use sinex_node_sdk::NodeResult;
use std::sync::Arc;
use std::{collections::HashMap, fmt, str::FromStr, time::Duration};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// Channel buffer size for D-Bus message processing
// Increased from 1000 to 10,000 to handle busy systems without dropping messages
const DBUS_MESSAGE_CHANNEL_SIZE: usize = 10_000;
/// Maximum serialized size of a D-Bus message payload before it is dropped.
/// Prevents memory exhaustion from pathologically large messages.
const MAX_DBUS_MESSAGE_BYTES: usize = 1_048_576; // 1 MiB

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
            _ => Err(format!("Unsupported DBus type: {s}")),
        }
    }
}

fn record_activity_timestamp(
    activity_tracker: &Arc<Mutex<std::time::Instant>>,
) -> std::time::Instant {
    let mut last_activity = activity_tracker.lock();
    let now = std::time::Instant::now();
    *last_activity = now;
    now
}

fn activity_elapsed(activity_tracker: &Arc<Mutex<std::time::Instant>>) -> std::time::Duration {
    let last_activity = activity_tracker.lock();
    last_activity.elapsed()
}

/// Convert D-Bus member name to `PowerEventType`
fn parse_power_event(member: &str) -> PowerEventType {
    match member {
        "PrepareForSleep" => PowerEventType::Sleep,
        "PrepareForShutdown" => PowerEventType::Shutdown,
        "DeviceChanged" => PowerEventType::BatteryChanged,
        _ => PowerEventType::ProfileChanged, // Default for unknown
    }
}

/// Convert D-Bus member name to `BluetoothEventType`
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

/// Convert D-Bus member name to `NetworkEventType`
fn parse_network_event(member: &str) -> NetworkEventType {
    match member {
        "Connected" | "Activated" => NetworkEventType::Connected,
        "Disconnected" | "Deactivated" => NetworkEventType::Disconnected,
        "IpChanged" | "Ip4ConfigChanged" | "Ip6ConfigChanged" => NetworkEventType::IpChanged,
        _ => NetworkEventType::StateChanged,
    }
}

/// Parse bus type string to `DBusBus` enum
fn parse_bus_type(bus_type: &str) -> DBusBus {
    match bus_type {
        "session" => DBusBus::Session,
        _ => DBusBus::System,
    }
}

/// Parse playback status string to `PlaybackStatus` enum
fn parse_playback_status(s: &str) -> Option<PlaybackStatus> {
    match s {
        "Playing" => Some(PlaybackStatus::Playing),
        "Paused" => Some(PlaybackStatus::Paused),
        "Stopped" => Some(PlaybackStatus::Stopped),
        _ => None,
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

fn dbus_error(message: &str, source: impl std::fmt::Display) -> sinex_node_sdk::SinexError {
    sinex_node_sdk::SinexError::processing(format!("{message}: {source}"))
}

/// D-Bus watcher with real-time signal subscription
pub struct DbusWatcher {
    config: DbusConfig,
}

impl DbusWatcher {
    fn monitoring_task_exit_error(
        index: usize,
        result: std::result::Result<NodeResult<()>, tokio::task::JoinError>,
    ) -> sinex_node_sdk::SinexError {
        match result {
            Ok(Ok(())) => sinex_node_sdk::SinexError::invalid_state(
                "D-Bus monitoring task completed unexpectedly".to_string(),
            )
            .with_context("task_index", index.to_string())
            .with_operation("dbus_start_streaming"),
            Ok(Err(error)) => error
                .with_context("task_index", index.to_string())
                .with_operation("dbus_start_streaming"),
            Err(error) => {
                sinex_node_sdk::SinexError::processing("D-Bus monitoring task panicked".to_string())
                    .with_source(error.to_string())
                    .with_context("task_index", index.to_string())
                    .with_operation("dbus_start_streaming")
            }
        }
    }

    async fn drain_cancelled_monitor_task(
        index: usize,
        task: tokio::task::JoinHandle<NodeResult<()>>,
    ) {
        match tokio::time::timeout(Duration::from_secs(5), task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                warn!(
                    task_index = index,
                    error = %error,
                    "Cancelled D-Bus monitoring task failed while draining"
                );
            }
            Ok(Err(error)) => {
                warn!(
                    task_index = index,
                    error = %error,
                    "Cancelled D-Bus monitoring task panicked while draining"
                );
            }
            Err(_) => {
                warn!(
                    task_index = index,
                    "Timed out waiting for cancelled D-Bus monitoring task to drain"
                );
            }
        }
    }

    /// Create new D-Bus watcher
    pub fn new(config: DbusConfig) -> NodeResult<Self> {
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

        let primary_error = Self::monitoring_task_exit_error(index, result);
        error!(error = %primary_error, "D-Bus monitoring task exited unexpectedly");

        // Await remaining tasks with timeout
        for (remaining_index, task) in remaining.into_iter().enumerate() {
            Self::drain_cancelled_monitor_task(remaining_index, task).await;
        }

        Err(primary_error)
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
        use tokio_retry::{Retry, strategy::ExponentialBackoff};

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

        // Add match rules for signals and method calls
        let signal_rule = MatchRule::new().with_type(MessageType::Signal);
        let signal_rule_match = signal_rule.match_str();
        conn.add_match(signal_rule)
            .await
            .map_err(|e| dbus_error("Failed to add signal match rule", e))?;

        let method_rule = MatchRule::new().with_type(MessageType::MethodCall);
        let method_rule_match = method_rule.match_str();
        conn.add_match(method_rule)
            .await
            .map_err(|e| dbus_error("Failed to add method call match rule", e))?;

        info!("D-Bus {} bus monitoring started", bus_type);

        // Create bounded channel for D-Bus messages to prevent task explosion
        let (msg_tx, msg_rx) = mpsc::channel::<DbusMessageData>(DBUS_MESSAGE_CHANNEL_SIZE);
        // Activity tracker for connection health monitoring
        let activity_tracker = Arc::new(Mutex::new(std::time::Instant::now()));
        let mut resource_task = tokio::spawn(async move {
            Err::<(), _>(dbus_error("D-Bus connection lost", resource.await))
        });

        // Spawn a single worker to process messages (Receiver is not clonable)
        let tx = tx.clone();
        let worker_config = config.clone();
        let bus_type_str = bus_type.to_string();
        let mut msg_rx = msg_rx;
        let material = material.clone();
        let worker_bus_type = bus_type_str.clone();
        let mut worker_task = tokio::spawn(async move {
            debug!("D-Bus worker started for {} bus", worker_bus_type);
            while let Some(msg_data) = msg_rx.recv().await {
                if let Err(e) = Self::process_message(
                    &worker_bus_type,
                    msg_data.msg_type,
                    msg_data.interface,
                    msg_data.path,
                    msg_data.member,
                    msg_data.sender,
                    msg_data.destination,
                    msg_data.args_json,
                    tx.clone(),
                    &worker_config,
                    &material,
                )
                .await
                {
                    error!("Failed to process D-Bus message: {}", e);
                }
            }
            debug!("D-Bus worker stopped for {} bus", worker_bus_type);
        });

        // Set up message processing
        let activity_for_callback = activity_tracker.clone();
        let callback_tx = msg_tx.clone();

        // Start receiving messages
        let receive_token = conn.start_receive(
            MatchRule::new(),
            Box::new(move |msg, _| {
                // Update activity tracker
                record_activity_timestamp(&activity_for_callback);

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

                // Reject oversized messages before they consume channel capacity
                let estimated_size = msg_data.args_json.to_string().len();
                if estimated_size > MAX_DBUS_MESSAGE_BYTES {
                    warn!(
                        estimated_size,
                        limit = MAX_DBUS_MESSAGE_BYTES,
                        interface = msg_data.interface.as_deref().unwrap_or("?"),
                        "Dropping oversized D-Bus message"
                    );
                    return true;
                }

                // Send to worker pool via bounded channel
                // If channel is full, drop newest message (backpressure)
                if let Err(mpsc::error::TrySendError::Full(_)) = callback_tx.try_send(msg_data) {
                    warn!(
                        "D-Bus message channel full (capacity {}), dropping newest message",
                        DBUS_MESSAGE_CHANNEL_SIZE
                    );
                }

                true
            }),
        );

        // Keep connection alive with periodic health checks
        // If no messages received for configured timeout, attempt reconnection
        let health_check_interval =
            Duration::from_secs(config.health_check_interval_secs.as_secs());
        let inactivity_timeout = Duration::from_secs(config.inactivity_timeout_secs.as_secs());

        let exit_error = loop {
            tokio::time::sleep(health_check_interval).await;

            // Check if we've received activity recently
            if activity_elapsed(&activity_tracker) > inactivity_timeout {
                warn!(
                    "D-Bus {} bus: No messages received for {}s, connection may be stale",
                    bus_type,
                    config.inactivity_timeout_secs.as_secs()
                );
                // Return error to trigger reconnection via retry mechanism
                break sinex_node_sdk::SinexError::processing(format!(
                    "D-Bus {} bus inactive for {}s",
                    bus_type,
                    config.inactivity_timeout_secs.as_secs()
                ));
            }
        };

        let _ = conn.stop_receive(receive_token);
        if let Err(error) = conn.remove_match_no_cb(&signal_rule_match).await {
            warn!(
                bus = %bus_type_str,
                error = %error,
                "Failed to remove D-Bus signal match during watcher shutdown"
            );
        }
        if let Err(error) = conn.remove_match_no_cb(&method_rule_match).await {
            warn!(
                bus = %bus_type_str,
                error = %error,
                "Failed to remove D-Bus method match during watcher shutdown"
            );
        }
        drop(msg_tx);
        Self::drain_worker_task(&bus_type_str, &mut worker_task).await;
        Self::shutdown_resource_task(&bus_type_str, &mut resource_task).await;

        Err(exit_error)
    }

    async fn drain_worker_task(bus_type: &str, worker_task: &mut tokio::task::JoinHandle<()>) {
        match tokio::time::timeout(Duration::from_secs(1), &mut *worker_task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.is_cancelled() => {
                debug!(
                    bus = bus_type,
                    "D-Bus worker task cancelled during shutdown"
                );
            }
            Ok(Err(error)) => {
                warn!(
                    bus = bus_type,
                    error = %error,
                    "D-Bus worker task failed while draining"
                );
            }
            Err(_) => {
                worker_task.abort();
                match worker_task.await {
                    Ok(()) => {}
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        warn!(
                            bus = bus_type,
                            error = %error,
                            "D-Bus worker task panicked after forced shutdown"
                        );
                    }
                }
            }
        }
    }

    async fn shutdown_resource_task(
        bus_type: &str,
        resource_task: &mut tokio::task::JoinHandle<NodeResult<()>>,
    ) {
        if !resource_task.is_finished() {
            resource_task.abort();
        }

        match resource_task.await {
            Ok(Ok(())) => {
                debug!(bus = bus_type, "D-Bus connection resource stopped cleanly");
            }
            Ok(Err(error)) => {
                warn!(
                    bus = bus_type,
                    error = %error,
                    "D-Bus connection resource reported an error during shutdown"
                );
            }
            Err(error) if error.is_cancelled() => {
                debug!(
                    bus = bus_type,
                    "D-Bus connection resource cancelled during shutdown"
                );
            }
            Err(error) => {
                warn!(
                    bus = bus_type,
                    error = %error,
                    "D-Bus connection resource panicked during shutdown"
                );
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
        let interface = Self::require_message_field(interface, "interface")?;
        let path = Self::require_message_field(path, "path")?;
        let member = Self::require_message_field(member, "member")?;

        // Apply filtering
        if !Self::passes_filters(&interface, config) {
            return Ok(());
        }

        let timestamp = sinex_primitives::temporal::now();

        match msg_type {
            MessageType::Signal => {
                Self::process_signal(
                    bus_type, &interface, &path, &member, &sender, &args, timestamp, &tx, config,
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
                    timestamp,
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

    fn require_message_field(value: Option<String>, field: &str) -> NodeResult<String> {
        let value = value.ok_or_else(|| {
            sinex_node_sdk::SinexError::validation(format!(
                "D-Bus message is missing required field '{field}'"
            ))
        })?;
        if value.trim().is_empty() {
            return Err(sinex_node_sdk::SinexError::validation(format!(
                "D-Bus message field '{field}' must not be empty"
            )));
        }
        Ok(value)
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
        timestamp: Timestamp,
        tx: &mpsc::Sender<Event<JsonValue>>,
        config: &DbusConfig,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        let sender = Self::require_message_field(sender.clone(), "sender")?;

        // Extract specialized events based on interface
        if config.extract_notifications
            && interface == "org.freedesktop.Notifications"
            && member == "Notify"
        {
            let payload = Self::parse_notification_args(args, timestamp)?;
            let event = Event::new(payload, material.initial_provenance()).to_json_event()?;
            Self::send_event(tx, event, "dbus_notification", material).await?;
        }

        if config.extract_media
            && interface.starts_with("org.mpris.MediaPlayer2")
            && member == "PropertiesChanged"
        {
            let player = sender.split('.').next_back().ok_or_else(|| {
                sinex_node_sdk::SinexError::validation(
                    "MPRIS PropertiesChanged signal is missing sender".to_string(),
                )
            })?;

            let payload = Self::parse_mpris_properties(args, player, &sender, timestamp)?
                .ok_or_else(|| {
                    sinex_node_sdk::SinexError::validation(
                        "MPRIS PropertiesChanged signal did not contain changed properties"
                            .to_string(),
                    )
                })?;

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
                    timestamp,
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
                    timestamp,
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
                    timestamp,
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
                    timestamp,
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
                    timestamp,
                },
                material.initial_provenance(),
            )
            .to_json_event()?;
            Self::send_event(tx, event, "dbus_mount_event", material).await?;
        }

        // Always emit generic signal events (with redacted args)
        let privacy_engine = privacy::engine().map_err(|error| {
            sinex_node_sdk::SinexError::configuration(
                "failed to initialize privacy engine".to_string(),
            )
            .with_context("component", "dbus_signal_redaction")
            .with_std_error(error)
        })?;
        let event = Event::new(
            DbusSignalPayload {
                bus: parse_bus_type(bus_type),
                sender,
                path: path.to_string(),
                interface: interface.to_string(),
                signal: member.to_string(),
                args: privacy_engine.process_json(args, ProcessingContext::Dbus),
                timestamp,
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
        timestamp: Timestamp,
        tx: &mpsc::Sender<Event<JsonValue>>,
        _config: &DbusConfig,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        let sender = Self::require_message_field(sender.clone(), "sender")?;
        let destination = Self::require_message_field(destination.clone(), "destination")?;
        let privacy_engine = privacy::engine().map_err(|error| {
            sinex_node_sdk::SinexError::configuration(
                "failed to initialize privacy engine".to_string(),
            )
            .with_context("component", "dbus_method_redaction")
            .with_std_error(error)
        })?;
        let event = Event::new(
            DbusMethodCalledPayload {
                bus: parse_bus_type(bus_type),
                sender,
                destination,
                path: path.to_string(),
                interface: interface.to_string(),
                method: member.to_string(),
                args: privacy_engine.process_json(args, ProcessingContext::Dbus),
                timestamp,
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

    /// Maximum recursion depth for D-Bus argument parsing.
    /// Prevents stack overflow from deeply nested messages.
    const MAX_DBUS_PARSE_DEPTH: usize = 32;

    /// Convert D-Bus message arguments to JSON
    fn message_args_to_json(msg: &dbus::Message) -> serde_json::Value {
        let mut args = Vec::new();
        let mut iter = msg.iter_init();

        while iter.next() {
            args.push(Self::parse_dbus_argument(&mut iter, 0));
        }

        serde_json::Value::Array(args)
    }

    /// Parse individual D-Bus argument to JSON with depth limiting
    fn parse_dbus_argument(iter: &mut dbus::arg::Iter, depth: usize) -> serde_json::Value {
        use dbus::arg::ArgType;

        if depth >= Self::MAX_DBUS_PARSE_DEPTH {
            return serde_json::Value::String("[max_depth_exceeded]".to_string());
        }

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
            ArgType::Array => Self::parse_dbus_array(iter, depth + 1),
            ArgType::DictEntry => Self::parse_dbus_dict_entry(iter, depth + 1),
            ArgType::Variant => Self::parse_dbus_variant(iter, depth + 1),
            ArgType::Struct => Self::parse_dbus_struct(iter, depth + 1),
            ArgType::ObjectPath => iter
                .get::<dbus::Path>()
                .map_or(serde_json::Value::Null, |p| {
                    serde_json::Value::String(p.to_string())
                }),
            _ => serde_json::Value::String(format!("unsupported_type_{:?}", iter.arg_type())),
        }
    }

    /// Parse D-Bus array to JSON
    fn parse_dbus_array(iter: &mut dbus::arg::Iter, depth: usize) -> serde_json::Value {
        let mut array_values = Vec::new();

        if let Some(mut array_iter) = iter.recurse(dbus::arg::ArgType::Array) {
            while array_iter.next() {
                array_values.push(Self::parse_dbus_argument(&mut array_iter, depth));
            }
        }

        serde_json::Value::Array(array_values)
    }

    /// Parse D-Bus dict entry to JSON
    fn parse_dbus_dict_entry(iter: &mut dbus::arg::Iter, depth: usize) -> serde_json::Value {
        let mut dict_obj = serde_json::Map::new();

        if let Some(mut dict_iter) = iter.recurse(dbus::arg::ArgType::DictEntry)
            && dict_iter.next()
        {
            let key = Self::parse_dbus_argument(&mut dict_iter, depth);
            if dict_iter.next() {
                let value = Self::parse_dbus_argument(&mut dict_iter, depth);

                let key_str = match key {
                    serde_json::Value::String(s) => s,
                    _ => format!("{key:?}"),
                };

                dict_obj.insert(key_str, value);
            }
        }

        serde_json::Value::Object(dict_obj)
    }

    /// Parse D-Bus variant to JSON
    fn parse_dbus_variant(iter: &mut dbus::arg::Iter, depth: usize) -> serde_json::Value {
        if let Some(mut variant_iter) = iter.recurse(dbus::arg::ArgType::Variant)
            && variant_iter.next()
        {
            return Self::parse_dbus_argument(&mut variant_iter, depth);
        }

        serde_json::Value::Null
    }

    /// Parse D-Bus struct to JSON
    fn parse_dbus_struct(iter: &mut dbus::arg::Iter, depth: usize) -> serde_json::Value {
        let mut struct_values = Vec::new();

        if let Some(mut struct_iter) = iter.recurse(dbus::arg::ArgType::Struct) {
            while struct_iter.next() {
                struct_values.push(Self::parse_dbus_argument(&mut struct_iter, depth));
            }
        }

        serde_json::Value::Array(struct_values)
    }

    /// Parse notification arguments into structured payload
    fn parse_notification_args(
        args: &serde_json::Value,
        timestamp: Timestamp,
    ) -> NodeResult<DbusNotificationSentPayload> {
        let privacy_engine = privacy::engine().map_err(|error| {
            sinex_node_sdk::SinexError::configuration(
                "failed to initialize privacy engine".to_string(),
            )
            .with_context("component", "dbus_notification_redaction")
            .with_std_error(error)
        })?;
        let arg_array = args.as_array().ok_or_else(|| {
            sinex_node_sdk::SinexError::validation(
                "notification D-Bus arguments must be an array".to_string(),
            )
        })?;

        let app_name = Self::notification_string_arg(arg_array, 0, "app_name")?;
        let summary = privacy_engine
            .process(
                Self::notification_string_arg(arg_array, 3, "summary")?,
                ProcessingContext::Notification,
            )
            .text
            .into_owned();
        let body = privacy_engine
            .process(
                Self::notification_string_arg(arg_array, 4, "body")?,
                ProcessingContext::Notification,
            )
            .text
            .into_owned();

        let actions = arg_array
            .get(5)
            .ok_or_else(|| {
                sinex_node_sdk::SinexError::validation(
                    "notification D-Bus arguments are missing required field 'actions'".to_string(),
                )
            })?
            .as_array()
            .ok_or_else(|| {
                sinex_node_sdk::SinexError::validation(
                    "notification actions must be an array".to_string(),
                )
            })?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(std::string::ToString::to_string)
                    .ok_or_else(|| {
                        sinex_node_sdk::SinexError::validation(
                            "notification actions must contain only strings".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let hints = Self::parse_notification_hints(arg_array.get(6).ok_or_else(|| {
            sinex_node_sdk::SinexError::validation(
                "notification D-Bus arguments are missing required field 'hints'".to_string(),
            )
        })?)?;

        let timeout = i32::try_from(
            arg_array
                .get(7)
                .ok_or_else(|| {
                    sinex_node_sdk::SinexError::validation(
                        "notification D-Bus arguments are missing required field 'timeout'"
                            .to_string(),
                    )
                })?
                .as_i64()
                .ok_or_else(|| {
                    sinex_node_sdk::SinexError::validation(
                        "notification timeout must be an integer".to_string(),
                    )
                })?,
        )
        .map_err(|error| {
            sinex_node_sdk::SinexError::validation(format!(
                "notification timeout is out of range for i32: {error}"
            ))
        })?;

        let urgency = u8::try_from(
            hints
                .get("urgency")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(1),
        )
        .map_err(|error| {
            sinex_node_sdk::SinexError::validation(format!(
                "notification urgency is out of range for u8: {error}"
            ))
        })?;

        Ok(DbusNotificationSentPayload {
            app_name: app_name.to_string(),
            summary,
            body,
            urgency,
            timeout,
            actions,
            hints,
            timestamp,
        })
    }

    fn notification_string_arg<'a>(
        args: &'a [serde_json::Value],
        index: usize,
        field: &str,
    ) -> NodeResult<&'a str> {
        args.get(index)
            .ok_or_else(|| {
                sinex_node_sdk::SinexError::validation(format!(
                    "notification D-Bus arguments are missing required field '{field}'"
                ))
            })?
            .as_str()
            .ok_or_else(|| {
                sinex_node_sdk::SinexError::validation(format!(
                    "notification field '{field}' must be a string"
                ))
            })
    }

    /// Parse notification hints from D-Bus arguments, redacting sensitive string values
    fn parse_notification_hints(
        hints_value: &serde_json::Value,
    ) -> NodeResult<HashMap<String, serde_json::Value>> {
        let privacy_engine = privacy::engine().map_err(|error| {
            sinex_node_sdk::SinexError::configuration(
                "failed to initialize privacy engine".to_string(),
            )
            .with_context("component", "dbus_notification_hint_redaction")
            .with_std_error(error)
        })?;
        let dict_entries = hints_value.as_array().ok_or_else(|| {
            sinex_node_sdk::SinexError::validation(
                "notification hints must be an array of key/value objects".to_string(),
            )
        })?;
        dict_entries
            .iter()
            .map(|entry| {
                let obj = entry.as_object().ok_or_else(|| {
                    sinex_node_sdk::SinexError::validation(
                        "notification hints must contain only objects".to_string(),
                    )
                })?;
                if obj.len() != 1 {
                    return Err(sinex_node_sdk::SinexError::validation(
                        "notification hint objects must contain exactly one entry".to_string(),
                    ));
                }
                let Some((key, value)) = obj.iter().next() else {
                    return Err(sinex_node_sdk::SinexError::validation(
                        "notification hint objects must contain exactly one entry".to_string(),
                    ));
                };
                let redacted = if let Some(s) = value.as_str() {
                    serde_json::Value::String(
                        privacy_engine
                            .process(s, ProcessingContext::Notification)
                            .text
                            .into_owned(),
                    )
                } else {
                    value.clone()
                };
                Ok((key.clone(), redacted))
            })
            .collect()
    }

    /// Parse MPRIS properties into media playback payload
    fn parse_mpris_properties(
        args: &serde_json::Value,
        player: &str,
        sender: &str,
        timestamp: Timestamp,
    ) -> NodeResult<Option<DbusMediaStateChangedPayload>> {
        let arg_array = args.as_array().ok_or_else(|| {
            sinex_node_sdk::SinexError::validation(
                "MPRIS D-Bus arguments must be an array".to_string(),
            )
        })?;
        let Some(changed_props) = arg_array.get(1) else {
            return Ok(None);
        };

        let mut payload = Self::default_media_payload(player, sender, timestamp);
        let props = changed_props.as_array().ok_or_else(|| {
            sinex_node_sdk::SinexError::validation(
                "MPRIS changed properties must be an array of objects".to_string(),
            )
        })?;
        for prop_entry in props {
            let obj = prop_entry.as_object().ok_or_else(|| {
                sinex_node_sdk::SinexError::validation(
                    "MPRIS changed properties must contain only objects".to_string(),
                )
            })?;
            for (key, value) in obj {
                match key.as_str() {
                    "PlaybackStatus" => {
                        let status = value.as_str().ok_or_else(|| {
                            sinex_node_sdk::SinexError::validation(
                                "MPRIS PlaybackStatus must be a string".to_string(),
                            )
                        })?;
                        payload.status = parse_playback_status(status).ok_or_else(|| {
                            sinex_node_sdk::SinexError::validation(format!(
                                "MPRIS PlaybackStatus has invalid value '{status}'"
                            ))
                        })?;
                    }
                    "Volume" => {
                        payload.volume = value.as_f64();
                    }
                    "Position" => {
                        payload.position = value.as_i64();
                    }
                    "CanGoNext" => {
                        payload.can_go_next = value.as_bool().ok_or_else(|| {
                            sinex_node_sdk::SinexError::validation(
                                "MPRIS CanGoNext must be a boolean".to_string(),
                            )
                        })?;
                    }
                    "CanGoPrevious" => {
                        payload.can_go_previous = value.as_bool().ok_or_else(|| {
                            sinex_node_sdk::SinexError::validation(
                                "MPRIS CanGoPrevious must be a boolean".to_string(),
                            )
                        })?;
                    }
                    "CanPlay" => {
                        payload.can_play = value.as_bool().ok_or_else(|| {
                            sinex_node_sdk::SinexError::validation(
                                "MPRIS CanPlay must be a boolean".to_string(),
                            )
                        })?;
                    }
                    "CanPause" => {
                        payload.can_pause = value.as_bool().ok_or_else(|| {
                            sinex_node_sdk::SinexError::validation(
                                "MPRIS CanPause must be a boolean".to_string(),
                            )
                        })?;
                    }
                    "CanSeek" => {
                        payload.can_seek = value.as_bool().ok_or_else(|| {
                            sinex_node_sdk::SinexError::validation(
                                "MPRIS CanSeek must be a boolean".to_string(),
                            )
                        })?;
                    }
                    _ => {}
                }
            }
        }
        Ok(Some(payload))
    }

    /// Create default media payload
    fn default_media_payload(
        player: &str,
        sender: &str,
        timestamp: Timestamp,
    ) -> DbusMediaStateChangedPayload {
        DbusMediaStateChangedPayload {
            player: player.to_string(),
            player_instance: sender.to_string(),
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
            timestamp,
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
        tx.send(event).await.map_err(|err| {
            sinex_node_sdk::SinexError::processing("dbus event channel closed")
                .with_context("context", context.to_string())
                .with_std_error(&err)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::DbusWatcher;
    use crate::{WatcherMaterialContext, material_context::MaterialContext};
    use async_trait::async_trait;
    use serde_json::json;
    use sinex_db::models::{Event, Provenance};
    use sinex_node_sdk::NodeResult;
    use sinex_primitives::events::enums::PlaybackStatus;
    use sinex_primitives::{Id, JsonValue, temporal::Timestamp};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use xtask::sandbox::prelude::*;

    // Inline because these exercise private D-Bus parsing helpers directly.

    #[derive(Debug)]
    struct TestMaterialContext;

    #[async_trait]
    impl MaterialContext for TestMaterialContext {
        fn initial_provenance(&self) -> Provenance {
            Provenance::Material {
                id: Id::new(),
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: sinex_primitives::events::OffsetKind::Byte,
            }
        }

        async fn decorate_event(&self, _event: &mut Event<JsonValue>) -> NodeResult<()> {
            Ok(())
        }

        async fn finalize(&self, _reason: &str) -> NodeResult<()> {
            Ok(())
        }

        fn event_count(&self) -> u64 {
            0
        }
    }

    fn test_material() -> WatcherMaterialContext {
        Arc::new(TestMaterialContext)
    }

    #[sinex_test]
    async fn require_message_field_rejects_missing_values() -> TestResult<()> {
        let error = DbusWatcher::require_message_field(None, "interface")
            .expect_err("missing required fields should fail honestly");
        assert!(
            error
                .to_string()
                .contains("missing required field 'interface'")
        );
        Ok(())
    }

    #[sinex_test]
    async fn require_message_field_rejects_empty_values() -> TestResult<()> {
        let error = DbusWatcher::require_message_field(Some("  ".to_string()), "member")
            .expect_err("empty required fields should fail honestly");
        assert!(
            error
                .to_string()
                .contains("field 'member' must not be empty")
        );
        Ok(())
    }

    #[sinex_test]
    async fn parse_notification_args_rejects_non_array() -> TestResult<()> {
        let error = DbusWatcher::parse_notification_args(&json!({"bad": true}), Timestamp::now())
            .expect_err("non-array notification payloads should fail honestly");
        assert!(error.to_string().contains("arguments must be an array"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_notification_args_rejects_invalid_timeout() -> TestResult<()> {
        let args = json!([
            "notify-send",
            0,
            "dialog-information",
            "Summary",
            "Body",
            [],
            [],
            "later"
        ]);
        let error = DbusWatcher::parse_notification_args(&args, Timestamp::now())
            .expect_err("invalid timeout values should fail honestly");
        assert!(error.to_string().contains("timeout must be an integer"));
        Ok(())
    }

    #[sinex_test]
    async fn send_event_rejects_closed_channel() -> TestResult<()> {
        let material = test_material();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let event = Event::new_json(
            "system-watcher",
            "dbus.generic.signal",
            json!({"member": "TestSignal"}),
            material.initial_provenance(),
        );

        let error = DbusWatcher::send_event(&tx, event, "test_closed_send", &material)
            .await
            .expect_err("closed dbus event channels must fail honestly");

        assert!(error.to_string().contains("dbus event channel closed"));
        assert!(error.to_string().contains("test_closed_send"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_mpris_properties_rejects_invalid_playback_status() -> TestResult<()> {
        let args = json!([
            "org.mpris.MediaPlayer2.Player",
            [{"PlaybackStatus": "Exploding"}]
        ]);
        let error = DbusWatcher::parse_mpris_properties(
            &args,
            "player",
            "org.mpris.MediaPlayer2.spotify",
            Timestamp::now(),
        )
        .expect_err("invalid playback statuses should fail honestly");
        assert!(
            error
                .to_string()
                .contains("PlaybackStatus has invalid value")
        );
        Ok(())
    }

    #[sinex_test]
    async fn parse_mpris_properties_accepts_valid_payload() -> TestResult<()> {
        let args = json!([
            "org.mpris.MediaPlayer2.Player",
            [
                {"PlaybackStatus": "Playing"},
                {"CanPause": true},
                {"CanSeek": false}
            ]
        ]);
        let payload = DbusWatcher::parse_mpris_properties(
            &args,
            "spotify",
            "org.mpris.MediaPlayer2.spotify",
            Timestamp::now(),
        )?
        .expect("valid payload should parse");

        assert_eq!(payload.status, PlaybackStatus::Playing);
        assert!(payload.can_pause);
        assert!(!payload.can_seek);
        Ok(())
    }

    #[sinex_test]
    async fn monitoring_task_exit_error_rejects_normal_completion() -> TestResult<()> {
        let error = DbusWatcher::monitoring_task_exit_error(2, Ok(Ok(())));
        assert!(
            error.to_string().contains("completed unexpectedly"),
            "normal completion must not be treated as success"
        );
        assert!(error.to_string().contains("task_index"));
        Ok(())
    }

    #[sinex_test]
    async fn monitoring_task_exit_error_preserves_worker_failure() -> TestResult<()> {
        let error = DbusWatcher::monitoring_task_exit_error(
            1,
            Ok(Err(sinex_node_sdk::SinexError::processing(
                "worker failed".to_string(),
            ))),
        );
        assert!(error.to_string().contains("worker failed"));
        assert!(error.to_string().contains("task_index"));
        Ok(())
    }

    #[sinex_test]
    async fn monitoring_task_exit_error_rejects_panics() -> TestResult<()> {
        let join_error = tokio::spawn(async {
            panic!("dbus watcher panic");
        })
        .await
        .expect_err("panicing task must produce join error");
        let error = DbusWatcher::monitoring_task_exit_error(0, Err(join_error));
        let error_text = error.to_string();
        assert!(error_text.contains("panicked"));
        assert!(error_text.contains("task_index"));
        Ok(())
    }
}
