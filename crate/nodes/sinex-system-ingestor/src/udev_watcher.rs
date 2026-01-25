#![doc = include_str!("../docs/udev_watcher.md")]

//! udev watcher module with inotify-based device monitoring.
//!
//! Previously used 5-second polling loop. Now uses inotify for real-time
//! device event detection with <100ms latency.

use crate::WatcherMaterialContext;
use notify::{Event as NotifyEvent, EventKind, RecursiveMode, Watcher as NotifyWatcher};
use sinex_core::db::models::event::Event;
use sinex_core::types::events::{
    UdevDeviceChangedPayload, UdevDeviceConnectedPayload, UdevDeviceDisconnectedPayload,
    UdevDeviceDriverChangedPayload, UdevDeviceOtherPayload,
};
use sinex_core::{DeviceType, JsonValue, UdevAction};
use sinex_node_sdk::NodeResult;
use std::path::Path;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// Macro to create udev events with common fields
macro_rules! create_udev_event {
    ($material:expr, $payload_type:ident, $action:expr, $device_path:expr, $device_type:expr,
     $subsystem:expr, $devtype:expr, $vendor:expr, $model:expr, $serial:expr,
     $properties:expr, $timestamp:expr) => {{
        let event = Event::new(
            $payload_type {
                action: $action,
                device_path: $device_path.to_string(),
                device_type: $device_type,
                subsystem: $subsystem,
                devtype: $devtype,
                vendor: $vendor,
                model: $model,
                serial: $serial,
                properties: $properties,
                timestamp: $timestamp,
            },
            $material.initial_provenance(),
        )
        .to_json_event()?;

        Ok(event)
    }};
}

/// Parse udev action string to UdevAction enum
fn parse_udev_action(action: &str) -> UdevAction {
    match action {
        "add" => UdevAction::Add,
        "remove" => UdevAction::Remove,
        "change" => UdevAction::Change,
        "bind" => UdevAction::Bind,
        "unbind" => UdevAction::Unbind,
        _ => UdevAction::Other,
    }
}

/// Parse device type string to DeviceType enum
fn parse_udev_device_type(s: &str) -> DeviceType {
    match s.to_lowercase().as_str() {
        "usb" | "usb_device" | "usb_interface" => DeviceType::Usb,
        "disk" | "partition" | "block" => DeviceType::Storage,
        "net" | "network" => DeviceType::Network,
        "input" | "input_device" => DeviceType::Input,
        "sound" | "audio" => DeviceType::Audio,
        "video" | "drm" => DeviceType::Video,
        "bluetooth" => DeviceType::Bluetooth,
        "power_supply" | "battery" => DeviceType::Battery,
        _ => DeviceType::Other,
    }
}

/// udev watcher
pub struct UdevWatcher {
    _monitor_hotplug: bool,
}

impl UdevWatcher {
    /// Create new udev watcher
    pub async fn new(monitor_hotplug: bool) -> NodeResult<Self> {
        let watcher = Self {
            _monitor_hotplug: monitor_hotplug,
        };

        info!("udev watcher initialized (hotplug: {})", monitor_hotplug);
        Ok(watcher)
    }

    /// Create device event
    fn create_device_event(
        &self,
        action: &str,
        device_path: &str,
        device_type: &str,
        properties: std::collections::HashMap<String, String>,
        material: &WatcherMaterialContext,
    ) -> NodeResult<Event<JsonValue>> {
        // Extract common properties
        let subsystem = properties.get("SUBSYSTEM").cloned();
        let devtype = properties.get("DEVTYPE").cloned();
        let vendor = properties
            .get("ID_VENDOR_FROM_DATABASE")
            .or_else(|| properties.get("ID_VENDOR"))
            .cloned();
        let model = properties
            .get("ID_MODEL_FROM_DATABASE")
            .or_else(|| properties.get("ID_MODEL"))
            .cloned();
        let serial = properties
            .get("ID_SERIAL_SHORT")
            .or_else(|| properties.get("ID_SERIAL"))
            .cloned();
        let timestamp = chrono::Utc::now();

        // Parse string types to enums
        let action_enum = parse_udev_action(action);
        let device_type_enum = parse_udev_device_type(device_type);

        match action {
            "add" => create_udev_event!(
                material,
                UdevDeviceConnectedPayload,
                action_enum,
                device_path,
                device_type_enum,
                subsystem,
                devtype,
                vendor,
                model,
                serial,
                properties,
                timestamp
            ),
            "remove" => create_udev_event!(
                material,
                UdevDeviceDisconnectedPayload,
                action_enum,
                device_path,
                device_type_enum,
                subsystem,
                devtype,
                vendor,
                model,
                serial,
                properties,
                timestamp
            ),
            "change" => create_udev_event!(
                material,
                UdevDeviceChangedPayload,
                action_enum,
                device_path,
                device_type_enum,
                subsystem,
                devtype,
                vendor,
                model,
                serial,
                properties,
                timestamp
            ),
            "bind" | "unbind" => create_udev_event!(
                material,
                UdevDeviceDriverChangedPayload,
                action_enum,
                device_path,
                device_type_enum,
                subsystem,
                devtype,
                vendor,
                model,
                serial,
                properties,
                timestamp
            ),
            _ => create_udev_event!(
                material,
                UdevDeviceOtherPayload,
                action_enum,
                device_path,
                device_type_enum,
                subsystem,
                devtype,
                vendor,
                model,
                serial,
                properties,
                timestamp
            ),
        }
    }

    /// Get device properties from uevent file
    async fn get_device_properties(device_path: &str) -> std::collections::HashMap<String, String> {
        let mut properties = std::collections::HashMap::new();
        let uevent_path = std::path::Path::new(device_path).join("uevent");

        if let Ok(content) = tokio::fs::read_to_string(uevent_path).await {
            for line in content.lines() {
                if let Some((key, value)) = line.split_once('=') {
                    properties.insert(key.to_string(), value.to_string());
                }
            }
        }
        properties
    }

    /// Monitor udev events using inotify
    async fn monitor_udev_events(
        &self,
        tx: mpsc::Sender<Event<JsonValue>>,
        material: WatcherMaterialContext,
    ) -> NodeResult<()> {
        info!("Starting udev event monitoring via inotify");

        // Create a channel for inotify events
        let (notify_tx, mut notify_rx) = mpsc::channel::<Result<NotifyEvent, notify::Error>>(100);

        // Create watcher with inotify backend
        let mut watcher =
            notify::recommended_watcher(move |res: Result<NotifyEvent, notify::Error>| {
                // Send event to async channel
                if let Err(e) = notify_tx.blocking_send(res) {
                    error!("Failed to send inotify event: {}", e);
                }
            })
            .map_err(|e| {
                sinex_node_sdk::NodeError::Processing(format!(
                    "Failed to create inotify watcher: {}",
                    e
                ))
            })?;

        // Watch interesting device class directories
        let watch_paths = vec![
            "/sys/class/net",
            "/sys/class/block",
            "/sys/class/input",
            "/sys/class/usb",
            "/sys/class/sound",
        ];

        for path in &watch_paths {
            if let Err(e) = watcher.watch(Path::new(path), RecursiveMode::NonRecursive) {
                warn!("Failed to watch {}: {}", path, e);
            } else {
                info!("Watching {} for device changes", path);
            }
        }

        info!("udev inotify monitoring started");

        // Keep the watcher alive and process events
        loop {
            match notify_rx.recv().await {
                Some(Ok(event)) => {
                    if let Err(e) = self.handle_inotify_event(event, &tx, &material).await {
                        warn!("Error handling inotify event: {}", e);
                    }
                }
                Some(Err(e)) => {
                    error!("Inotify error: {}", e);
                }
                None => {
                    error!("Inotify channel closed");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle inotify event and emit udev events
    async fn handle_inotify_event(
        &self,
        event: NotifyEvent,
        tx: &mpsc::Sender<Event<JsonValue>>,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        // Determine action based on event kind
        let action = match event.kind {
            EventKind::Create(_) => "add",
            EventKind::Remove(_) => "remove",
            EventKind::Modify(_) => "change",
            _ => return Ok(()), // Ignore other events
        };

        for path in event.paths {
            let device_path = path.to_string_lossy().to_string();

            // Extract device class from path
            let class_name = if let Some(parent) = path.parent() {
                parent
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
            } else {
                "unknown"
            };

            // Determine device type
            let device_type = match class_name {
                "net" => "network",
                "block" => "storage",
                "input" => "input",
                "usb" => "usb",
                "sound" => "audio",
                _ => "other",
            };

            // Get device properties for create/add events
            let properties = if action == "add" || action == "change" {
                Self::get_device_properties(&device_path).await
            } else {
                std::collections::HashMap::with_capacity(8)
            };

            let raw_event =
                self.create_device_event(action, &device_path, device_type, properties, material)?;

            Self::send_event(tx, raw_event, &format!("udev_{}", action), material).await?;

            debug!("udev event: {} {} {}", action, device_type, device_path);
        }

        Ok(())
    }

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

    /// Start streaming events
    pub(crate) async fn start_streaming(
        &mut self,
        tx: mpsc::Sender<Event<JsonValue>>,
        material: WatcherMaterialContext,
    ) -> NodeResult<()> {
        info!("Starting udev event streaming");

        self.monitor_udev_events(tx, material).await
    }
}
