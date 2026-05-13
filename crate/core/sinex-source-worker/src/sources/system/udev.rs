//! `system.udev` — stream udev device events via `FileDropAdapter` over `/sys`.

use sinex_node_sdk::parser::{FileDropAdapter, MaterialParser, ParserError};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceRecord,
    SourceUnitId, TimingEvidence,
};
use sinex_primitives::privacy::{self, ProcessingContext};
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::events::payloads::system::{
    UdevDeviceChangedPayload, UdevDeviceConnectedPayload, UdevDeviceDisconnectedPayload,
    UdevDeviceDriverChangedPayload, UdevDeviceOtherPayload,
};
use sinex_primitives::events::enums::{DeviceType, UdevAction};
use sinex_primitives::{register_source_unit, register_source_unit_binding};
use crate::register_parser;

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Source-unit descriptor
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "system.udev",
        namespace: "system",
        event_types: &[
            ("udev", "device.connected"),
            ("udev", "device.disconnected"),
            ("udev", "device.changed"),
            ("udev", "device.driver_changed"),
            ("udev", "device.other"),
        ],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "udev_action_dispatch",
            "privacy_device_path",
        ],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "udev_monitor_read",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:system.udev"),
        "system.udev",
        "system",
    )
    .implementation("sinex-source-worker")
    .adapter("FileDropAdapter")
    .output_event_type("device.connected")
    .privacy_context("Metadata")
    .material_policy("udev_anchor")
    .checkpoint_policy("live_observation")
    .resource_shape("event_emitter")
    .source_unit_id("system.udev")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("system_udev_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parser for `system.udev` — maps `FileDropAdapter` inotify records to udev device events.
#[derive(Default)]
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
            source_unit_id: SourceUnitId::from_static("system.udev"),
            declared_event_types: vec![
                (EventSource::from_static("udev"), EventType::from_static("device.connected")),
                (EventSource::from_static("udev"), EventType::from_static("device.disconnected")),
                (EventSource::from_static("udev"), EventType::from_static("device.changed")),
                (EventSource::from_static("udev"), EventType::from_static("device.driver_changed")),
                (EventSource::from_static("udev"), EventType::from_static("device.other")),
            ],
            privacy_contexts: vec![ProcessingContext::Metadata],
            proof_obligations: vec![
                "udev_action_dispatch".into(),
                "privacy_device_path".into(),
            ],
            description: "Maps FileDropAdapter inotify records to udev device events.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> Result<Vec<ParsedEventIntent>, ParserError> {
        let raw_path = std::str::from_utf8(&record.bytes)
            .map_err(|e| ParserError::Parse(
                format!("udev record path not UTF-8: {e}"),
            ))?
            .to_string();

        let device_path = match privacy::engine() {
            Ok(eng) => eng.process(&raw_path, ProcessingContext::Metadata).text.into_owned(),
            Err(e) => return Err(ParserError::Privacy(format!("privacy engine: {e}"))),
        };

        let event_kind = record
            .metadata
            .get("event_kind")
            .and_then(|v| v.as_str())
            .unwrap_or("Created");

        let action = match event_kind {
            "Created" => UdevAction::Add,
            "Deleted" => UdevAction::Remove,
            "Modified" => UdevAction::Change,
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

        let intent = ParsedEventIntent {
            id: Id::new(),
            source_unit_id: ctx.source_unit_id.clone(),
            parser_id: ParserId::from_static("system.udev"),
            parser_version: "1.0.0".into(),
            event_type,
            event_source: EventSource::from_static("udev"),
            payload: payload_value,
            ts_orig: timestamp,
            timing: TimingEvidence::Atemporal,
            anchor: record.anchor.clone(),
            occurrence_key: None,
            privacy_context: ProcessingContext::Metadata,
            field_privacy_log: None,
            synthesis_parents: None,
        };

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

// Register for dispatch (replay path).
register_parser!("system.udev", UdevParser);

// Register node factory — FileDropAdapter + UdevParser.
crate::register_adapter_ingestor!(
    source_unit_id: "system.udev",
    adapter: FileDropAdapter,
    parser: UdevParser,
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::ids::Id;
    use sinex_primitives::events::SourceMaterial;
    use sinex_primitives::primitives::Uuid;
    use sinex_primitives::parser::MaterialAnchor;
    use xtask::sandbox::prelude::*;

    fn make_ctx(mid: Id<SourceMaterial>) -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static("system.udev"),
            source_material_id: mid,
            record_anchor: MaterialAnchor::DirectoryEntry {
                path: "/sys/bus/usb/devices/1-1".into(),
                content_hash: None,
            },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn make_udev_record(mid: Id<SourceMaterial>, path: &str, kind: &str) -> SourceRecord {
        SourceRecord {
            material_id: mid,
            anchor: MaterialAnchor::DirectoryEntry {
                path: path.into(),
                content_hash: None,
            },
            bytes: path.as_bytes().to_vec(),
            logical_path: Some(path.into()),
            source_ts_hint: None,
            metadata: serde_json::json!({
                "event_kind": kind,
                "path": path,
            }),
        }
    }

    #[sinex_test]
    async fn test_udev_parser_device_connected() -> TestResult<()> {
        let mid = Id::<SourceMaterial>::new();
        let record = make_udev_record(mid, "/sys/bus/usb/devices/1-1", "Created");

        let mut parser = UdevParser;
        let ctx = make_ctx(mid);
        let intents = parser.parse_record(record, &ctx).await.unwrap();

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type.as_str(), "device.connected");
        assert_eq!(intents[0].event_source.as_str(), "udev");
        Ok(())
    }

    #[sinex_test]
    async fn test_udev_parser_device_disconnected() -> TestResult<()> {
        let mid = Id::<SourceMaterial>::new();
        let record = make_udev_record(mid, "/sys/bus/usb/devices/1-2", "Deleted");

        let mut parser = UdevParser;
        let ctx = make_ctx(mid);
        let intents = parser.parse_record(record, &ctx).await.unwrap();

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type.as_str(), "device.disconnected");
        Ok(())
    }

    #[sinex_test]
    async fn test_infer_device_type() -> TestResult<()> {
        assert!(matches!(infer_device_type("/sys/bus/usb/devices/1-1"), DeviceType::Usb));
        assert!(matches!(infer_device_type("/sys/block/sda"), DeviceType::Storage));
        assert!(matches!(infer_device_type("/sys/class/net/eth0"), DeviceType::Network));
        assert!(matches!(infer_device_type("/sys/bus/other"), DeviceType::Other));
        Ok(())
    }
}
