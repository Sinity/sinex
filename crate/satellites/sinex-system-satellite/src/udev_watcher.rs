#![doc = include_str!("../docs/udev_watcher.md")]

//! udev watcher module.

use sinex_core::db::models::event::Event;
use sinex_core::db::models::event::EventId;
use sinex_core::db::models::event::Provenance;
use sinex_core::types::events::{
    UdevDeviceChangedPayload, UdevDeviceConnectedPayload, UdevDeviceDisconnectedPayload,
    UdevDeviceDriverChangedPayload, UdevDeviceOtherPayload,
};
use sinex_core::types::Ulid;
use sinex_core::JsonValue;
use sinex_satellite_sdk::SatelliteResult;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

// Macro to create udev events with common fields
macro_rules! create_udev_event {
    ($payload_type:ident, $action:expr, $device_path:expr, $device_type:expr,
     $subsystem:expr, $devtype:expr, $vendor:expr, $model:expr, $serial:expr,
     $properties:expr, $timestamp:expr) => {{
        // System events currently do not originate from a concrete source material;
        // model them as synthesis events anchored to a bootstrap ID.
        let system_bootstrap_id = EventId::from_ulid(
            Ulid::from_bytes([
                0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00,
            ])
            .expect("hardcoded ULID bytes should be valid"),
        );
        let provenance = Provenance::from_synthesis_safe(system_bootstrap_id, vec![]);

        let event = Event::new(
            $payload_type {
                action: $action.to_string(),
                device_path: $device_path.to_string(),
                device_type: $device_type.to_string(),
                subsystem: $subsystem,
                devtype: $devtype,
                vendor: $vendor,
                model: $model,
                serial: $serial,
                properties: $properties,
                timestamp: $timestamp,
            },
            provenance,
        )
        .to_json_event()?;

        Ok(event)
    }};
}

/// udev watcher
pub struct UdevWatcher {
    _monitor_hotplug: bool,
}

impl UdevWatcher {
    /// Create new udev watcher
    pub async fn new(monitor_hotplug: bool) -> SatelliteResult<Self> {
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
    ) -> SatelliteResult<Event<JsonValue>> {
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
        let timestamp = chrono::Utc::now().to_rfc3339();

        match action {
            "add" => create_udev_event!(
                UdevDeviceConnectedPayload,
                action,
                device_path,
                device_type,
                subsystem,
                devtype,
                vendor,
                model,
                serial,
                properties,
                timestamp
            ),
            "remove" => create_udev_event!(
                UdevDeviceDisconnectedPayload,
                action,
                device_path,
                device_type,
                subsystem,
                devtype,
                vendor,
                model,
                serial,
                properties,
                timestamp
            ),
            "change" => create_udev_event!(
                UdevDeviceChangedPayload,
                action,
                device_path,
                device_type,
                subsystem,
                devtype,
                vendor,
                model,
                serial,
                properties,
                timestamp
            ),
            "bind" | "unbind" => create_udev_event!(
                UdevDeviceDriverChangedPayload,
                action,
                device_path,
                device_type,
                subsystem,
                devtype,
                vendor,
                model,
                serial,
                properties,
                timestamp
            ),
            _ => create_udev_event!(
                UdevDeviceOtherPayload,
                action,
                device_path,
                device_type,
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

    /// Monitor udev events using netlink socket (fallback implementation)
    async fn monitor_udev_events(
        &self,
        tx: mpsc::UnboundedSender<Event<JsonValue>>,
    ) -> SatelliteResult<()> {
        info!("Starting udev event monitoring via filesystem polling");

        // Since libudev is disabled, we'll do periodic scanning of /sys/class
        // This is less efficient but works without external dependencies

        let mut last_seen_devices = std::collections::HashSet::new();
        let mut poll_interval = tokio::time::interval(Duration::from_secs(5));

        info!("udev polling monitoring started");

        loop {
            let mut current_devices = std::collections::HashSet::new();

            // Scan /sys/class for device changes
            if let Ok(mut entries) = tokio::fs::read_dir("/sys/class").await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let class_name = entry.file_name().to_string_lossy().to_string();

                    // Focus on interesting device classes
                    if !["net", "block", "input", "usb", "sound"].contains(&class_name.as_str()) {
                        continue;
                    }

                    if let Ok(mut class_entries) = tokio::fs::read_dir(entry.path()).await {
                        while let Ok(Some(device_entry)) = class_entries.next_entry().await {
                            let device_name =
                                device_entry.file_name().to_string_lossy().to_string();
                            let device_path = device_entry.path().to_string_lossy().to_string();
                            let device_key = format!("{}:{}", class_name, device_name);

                            current_devices.insert(device_key.clone());

                            // Check if this is a new device
                            if !last_seen_devices.contains(&device_key) {
                                let properties = std::collections::HashMap::with_capacity(8); // Device properties: vendor, model, serial, etc.

                                let device_type = match class_name.as_str() {
                                    "usb" => "usb",
                                    "block" => "storage",
                                    "input" => "input",
                                    "net" => "network",
                                    "sound" => "audio",
                                    _ => "other",
                                };

                                let raw_event = self.create_device_event(
                                    "add",
                                    &device_path,
                                    device_type,
                                    properties,
                                )?;

                                if tx.send(raw_event).is_err() {
                                    warn!("Event channel closed");
                                    break;
                                }

                                debug!("udev event: add {} {}", device_type, device_path);
                            }
                        }
                    }
                }
            }

            // Check for removed devices
            for removed_device in last_seen_devices.difference(&current_devices) {
                let parts: Vec<&str> = removed_device.split(':').collect();
                if parts.len() == 2 {
                    let class_name = parts[0];
                    let device_name = parts[1];

                    let device_type = match class_name {
                        "usb" => "usb",
                        "block" => "storage",
                        "input" => "input",
                        "net" => "network",
                        "sound" => "audio",
                        _ => "other",
                    };

                    let properties = std::collections::HashMap::with_capacity(8); // Device properties: vendor, model, serial, etc.
                    let device_path = format!("/sys/class/{}/{}", class_name, device_name);

                    let raw_event =
                        self.create_device_event("remove", &device_path, device_type, properties)?;

                    if tx.send(raw_event).is_err() {
                        warn!("Event channel closed");
                        break;
                    }

                    debug!("udev event: remove {} {}", device_type, device_path);
                }
            }

            last_seen_devices = current_devices;
            poll_interval.tick().await;
        }
    }

    /// Start streaming events
    pub async fn start_streaming(
        &mut self,
        tx: mpsc::UnboundedSender<Event<JsonValue>>,
    ) -> SatelliteResult<()> {
        info!("Starting udev event streaming");

        self.monitor_udev_events(tx).await
    }
}
