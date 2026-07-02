//! `system.udev` — stream udev device events via `FileDropAdapter` over `/sys`.

use crate::runtime::parser::{
    FileDropEventKind, FileDropRecordMetadata, MaterialParser, ParserError,
};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::enums::{DeviceType, UdevAction};
use sinex_primitives::events::payloads::system::{
    UdevDeviceChangedPayload, UdevDeviceConnectedPayload, UdevDeviceDisconnectedPayload,
    UdevDeviceDriverChangedPayload, UdevDeviceOtherPayload,
};
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceId,
    SourceRecord, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::temporal::Timestamp;
use tracing::warn;

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parser for `system.udev` — maps `FileDropAdapter` inotify records to udev device events.
#[derive(Default, SourceMeta)]
#[source_meta(
    id = "system.udev",
    namespace = "system",
    event_source = "udev",
    event_type = "device.connected",
    event_types = "device.disconnected, device.changed, device.driver_changed, device.other",
    adapter = "FileDropAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Continuous),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Anchor,
    access_scope = AccessScope::KernelUevents,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Metadata,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::LiveObservation,
    runtime_shape = RuntimeShape::Continuous,
)]
pub struct UdevParser;

/// Infer `DeviceType` from a `/sys` path.
fn infer_device_type(path: &str) -> DeviceType {
    if path.contains("/usb") {
        DeviceType::Usb
    } else if path.contains("/block") {
        DeviceType::Storage
    } else if path.contains("/net") {
        DeviceType::Network
    } else if path.contains("/input") {
        DeviceType::Input
    } else {
        DeviceType::Other
    }
}

#[async_trait::async_trait]
impl MaterialParser for UdevParser {
    type Config = serde_json::Value;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("system.udev"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::FileDrop],
            source_id: SourceId::from_static("system.udev"),
            declared_event_types: vec![
                (
                    EventSource::from_static("udev"),
                    EventType::from_static("device.connected"),
                ),
                (
                    EventSource::from_static("udev"),
                    EventType::from_static("device.disconnected"),
                ),
                (
                    EventSource::from_static("udev"),
                    EventType::from_static("device.changed"),
                ),
                (
                    EventSource::from_static("udev"),
                    EventType::from_static("device.driver_changed"),
                ),
                (
                    EventSource::from_static("udev"),
                    EventType::from_static("device.other"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: "Maps FileDropAdapter inotify records to udev device events.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> Result<Vec<ParsedEventIntent>, ParserError> {
        let device_path = std::str::from_utf8(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("udev record path not UTF-8: {e}")))?
            .to_string();

        let metadata = match FileDropRecordMetadata::from_value(&record.metadata) {
            Ok(m) => Some(m),
            Err(e) => {
                warn!(
                    error = %e,
                    path = %device_path,
                    "udev metadata parse failed; emitting Other action instead of guessing kind"
                );
                None
            }
        };
        let event_kind = metadata
            .as_ref()
            .and_then(FileDropRecordMetadata::event_kind);

        let action = match event_kind {
            Some(FileDropEventKind::Created) => UdevAction::Add,
            Some(FileDropEventKind::Deleted) => UdevAction::Remove,
            Some(FileDropEventKind::Modified) => UdevAction::Change,
            _ => UdevAction::Other,
        };

        let device_type = infer_device_type(&device_path);
        let timestamp = Timestamp::now();
        let properties: HashMap<String, String> = HashMap::new();

        let (event_type, payload_value) = match action {
            UdevAction::Add => {
                let payload = UdevDeviceConnectedPayload {
                    action,
                    device_path: device_path.clone(),
                    device_type,
                    subsystem: None,
                    devtype: None,
                    vendor: None,
                    model: None,
                    serial: None,
                    properties,
                    timestamp,
                };
                (
                    EventType::from_static("device.connected"),
                    serde_json::to_value(&payload)
                        .map_err(|e| ParserError::Parse(e.to_string()))?,
                )
            }
            UdevAction::Remove => {
                let payload = UdevDeviceDisconnectedPayload {
                    action,
                    device_path: device_path.clone(),
                    device_type,
                    subsystem: None,
                    devtype: None,
                    vendor: None,
                    model: None,
                    serial: None,
                    properties,
                    timestamp,
                };
                (
                    EventType::from_static("device.disconnected"),
                    serde_json::to_value(&payload)
                        .map_err(|e| ParserError::Parse(e.to_string()))?,
                )
            }
            UdevAction::Change => {
                let payload = UdevDeviceChangedPayload {
                    action,
                    device_path: device_path.clone(),
                    device_type,
                    subsystem: None,
                    devtype: None,
                    vendor: None,
                    model: None,
                    serial: None,
                    properties,
                    timestamp,
                };
                (
                    EventType::from_static("device.changed"),
                    serde_json::to_value(&payload)
                        .map_err(|e| ParserError::Parse(e.to_string()))?,
                )
            }
            UdevAction::Bind | UdevAction::Unbind => {
                let payload = UdevDeviceDriverChangedPayload {
                    action,
                    device_path: device_path.clone(),
                    device_type,
                    subsystem: None,
                    devtype: None,
                    vendor: None,
                    model: None,
                    serial: None,
                    properties,
                    timestamp,
                };
                (
                    EventType::from_static("device.driver_changed"),
                    serde_json::to_value(&payload)
                        .map_err(|e| ParserError::Parse(e.to_string()))?,
                )
            }
            UdevAction::Other => {
                let payload = UdevDeviceOtherPayload {
                    action,
                    device_path: device_path.clone(),
                    device_type,
                    subsystem: None,
                    devtype: None,
                    vendor: None,
                    model: None,
                    serial: None,
                    properties,
                    timestamp,
                };
                (
                    EventType::from_static("device.other"),
                    serde_json::to_value(&payload)
                        .map_err(|e| ParserError::Parse(e.to_string()))?,
                )
            }
        };

        let intent = ParsedEventIntent::builder()
            .source_id(ctx.source_id.clone())
            .parser_id(ParserId::from_static("system.udev"))
            .parser_version("1.0.0")
            .event_type(event_type)
            .event_source(EventSource::from_static("udev"))
            .payload(payload_value)
            .ts_orig(timestamp)
            .timing(TimingEvidence::Atemporal)
            .anchor(record.anchor.clone())
            .privacy_context(ProcessingContext::Metadata)
            .build();

        Ok(vec![intent])
    }

    fn baseline_adapter_config() -> serde_json::Value {
        // /dev is where udev creates device-node files; FileDropAdapter
        // watches it for create/remove events that the UdevParser
        // classifies (`device.added`, `device.removed`). Nix binding may
        // override to a sandboxed subset.
        serde_json::json!({
            "watch_paths": ["/dev"],
            "recursive": false
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "udev_test.rs"]
mod tests;
