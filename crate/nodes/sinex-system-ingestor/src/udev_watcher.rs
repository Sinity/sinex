#![doc = include_str!("../docs/udev_watcher.md")]

//! udev watcher module with inotify-based device monitoring.
//!
//! Uses inotify for real-time device event detection with <100ms latency.

use crate::WatcherMaterialContext;
use notify::{Event as NotifyEvent, EventKind, RecursiveMode, Watcher as NotifyWatcher};
use sinex_db::models::Event;
use sinex_node_sdk::NodeResult;
use sinex_primitives::events::{
    UdevDeviceChangedPayload, UdevDeviceConnectedPayload, UdevDeviceDisconnectedPayload,
    UdevDeviceDriverChangedPayload, UdevDeviceOtherPayload,
};
use sinex_primitives::{
    JsonValue,
    events::enums::{DeviceType, UdevAction},
};
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

/// Parse udev action string to `UdevAction` enum
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

/// Parse device type string to `DeviceType` enum
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
        let timestamp = sinex_primitives::temporal::now();

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
    async fn get_device_properties(
        device_path: &str,
    ) -> NodeResult<std::collections::HashMap<String, String>> {
        let mut properties = std::collections::HashMap::new();
        let uevent_path = std::path::Path::new(device_path).join("uevent");

        let content = tokio::fs::read_to_string(&uevent_path).await.map_err(|error| {
            sinex_node_sdk::SinexError::processing("Failed to read uevent properties")
                .with_context("uevent_path", uevent_path.display().to_string())
                .with_source(error)
        })?;

        for line in content.lines() {
            if let Some((key, value)) = line.split_once('=') {
                properties.insert(key.to_string(), value.to_string());
            }
        }
        Ok(properties)
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
                sinex_node_sdk::SinexError::processing(format!(
                    "Failed to create inotify watcher: {e}"
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
            let device_path = Self::device_path_for_event(&path)?;
            let class_name = Self::class_name_for_event(&path)?;

            // Determine device type
            let device_type = Self::device_type_for_class_name(class_name);

            // Get device properties for create/add events
            let properties = if action == "add" || action == "change" {
                Self::get_device_properties(&device_path).await?
            } else {
                std::collections::HashMap::with_capacity(8)
            };

            let raw_event =
                self.create_device_event(action, &device_path, device_type, properties, material)?;

            Self::send_event(tx, raw_event, &format!("udev_{action}"), material).await?;

            debug!("udev event: {} {} {}", action, device_type, device_path);
        }

        Ok(())
    }

    fn device_path_for_event(path: &Path) -> NodeResult<String> {
        path.to_str().map(str::to_owned).ok_or_else(|| {
            sinex_node_sdk::SinexError::processing("udev watcher received non-utf8 device path")
                .with_context("path_debug", format!("{path:?}"))
        })
    }

    fn class_name_for_event<'a>(path: &'a Path) -> NodeResult<&'a str> {
        let class_dir = path.parent().ok_or_else(|| {
            sinex_node_sdk::SinexError::processing("udev watcher path is missing class directory")
                .with_context("path_debug", format!("{path:?}"))
        })?;
        class_dir
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                sinex_node_sdk::SinexError::processing(
                    "udev watcher path has invalid class directory name",
                )
                .with_context("path_debug", format!("{path:?}"))
            })
    }

    fn device_type_for_class_name(class_name: &str) -> &'static str {
        match class_name {
            "net" => "network",
            "block" => "storage",
            "input" => "input",
            "usb" => "usb",
            "sound" => "audio",
            _ => "other",
        }
    }

    async fn send_event(
        tx: &mpsc::Sender<Event<JsonValue>>,
        mut event: Event<JsonValue>,
        context: &str,
        material: &WatcherMaterialContext,
    ) -> NodeResult<()> {
        material.decorate_event(&mut event).await?;
        tx.send(event).await.map_err(|err| {
            sinex_node_sdk::SinexError::processing("udev event channel closed")
                .with_context("context", context.to_string())
                .with_std_error(&err)
        })
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

#[cfg(test)]
mod tests {
    use super::UdevWatcher;
    use crate::{WatcherMaterialContext, material_context::MaterialContext};
    use async_trait::async_trait;
    use notify::{
        Event as NotifyEvent,
        EventKind,
        event::{CreateKind, ModifyKind},
    };
    use serde_json::json;
    use sinex_db::models::{Event, Provenance};
    use sinex_node_sdk::NodeResult;
    use sinex_primitives::{Id, JsonValue};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;
    use xtask::sandbox::prelude::*;

    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;

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

    fn notify_event(kind: EventKind, path: PathBuf) -> NotifyEvent {
        NotifyEvent::new(kind).add_path(path)
    }

    #[sinex_test]
    async fn get_device_properties_parses_uevent_file() -> TestResult<()> {
        let device_dir = std::env::temp_dir().join(format!(
            "sinex-udev-test-{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&device_dir)?;
        std::fs::write(
            device_dir.join("uevent"),
            "DEVNAME=/dev/test0\nSUBSYSTEM=net\n",
        )?;

        let properties =
            UdevWatcher::get_device_properties(device_dir.to_str().expect("utf-8 temp path"))
                .await?;

        assert_eq!(properties.get("DEVNAME").map(String::as_str), Some("/dev/test0"));
        assert_eq!(properties.get("SUBSYSTEM").map(String::as_str), Some("net"));
        std::fs::remove_dir_all(&device_dir)?;
        Ok(())
    }

    #[sinex_test]
    async fn get_device_properties_surfaces_read_failures() -> TestResult<()> {
        let device_dir = std::env::temp_dir().join(format!(
            "sinex-udev-test-{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&device_dir)?;

        let error =
            UdevWatcher::get_device_properties(device_dir.to_str().expect("utf-8 temp path"))
                .await
                .expect_err("missing uevent file must fail honestly");

        assert!(error.to_string().contains("Failed to read uevent properties"));
        assert!(error.to_string().contains("uevent"));
        std::fs::remove_dir_all(&device_dir)?;
        Ok(())
    }

    #[sinex_test]
    async fn send_event_rejects_closed_channel() -> TestResult<()> {
        let material = test_material();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let event = Event::new_json(
            "system-watcher",
            "udev.device.other",
            json!({"action": "change"}),
            material.initial_provenance(),
        );

        let error = UdevWatcher::send_event(&tx, event, "test_closed_send", &material)
            .await
            .expect_err("closed udev event channels must fail honestly");

        assert!(error.to_string().contains("udev event channel closed"));
        assert!(error.to_string().contains("test_closed_send"));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn handle_inotify_event_rejects_non_utf8_paths() -> TestResult<()> {
        let watcher = UdevWatcher {
            _monitor_hotplug: false,
        };
        let material = test_material();
        let (tx, _rx) = mpsc::channel(1);
        let invalid_path = PathBuf::from(OsString::from_vec(vec![
            b'/',
            b's',
            b'y',
            b's',
            b'/',
            b'c',
            b'l',
            b'a',
            b's',
            b's',
            b'/',
            b'n',
            b'e',
            b't',
            b'/',
            0xff,
        ]));

        let error = watcher
            .handle_inotify_event(
                notify_event(EventKind::Modify(ModifyKind::Any), invalid_path),
                &tx,
                &material,
            )
            .await
            .expect_err("non-utf8 udev paths must fail honestly");

        assert!(error
            .to_string()
            .contains("udev watcher received non-utf8 device path"));
        Ok(())
    }

    #[sinex_test]
    async fn handle_inotify_event_rejects_paths_without_class_name() -> TestResult<()> {
        let watcher = UdevWatcher {
            _monitor_hotplug: false,
        };
        let material = test_material();
        let (tx, _rx) = mpsc::channel(1);

        let error = watcher
            .handle_inotify_event(
                notify_event(EventKind::Create(CreateKind::Any), PathBuf::from("/ttyUSB0")),
                &tx,
                &material,
            )
            .await
            .expect_err("root-level device paths must fail honestly");

        assert!(error
            .to_string()
            .contains("udev watcher path has invalid class directory name"));
        Ok(())
    }
}
