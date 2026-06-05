//! `system.udev` — stream udev device events via `FileDropAdapter` over `/sys`.

use crate::runtime::parser::{
    FileDropAdapter, FileDropEventKind, FileDropRecordMetadata, MaterialParser, ParserError,
};
use crate::register_source;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::enums::{DeviceType, UdevAction};
use sinex_primitives::events::payloads::system::{
    UdevDeviceChangedPayload, UdevDeviceConnectedPayload, UdevDeviceDisconnectedPayload,
    UdevDeviceDriverChangedPayload, UdevDeviceOtherPayload,
};
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceRecord,
    SourceId, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceRuntimeBinding, SourceBuildImpact, SourceContract, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_contract, register_source_runtime_binding};
use tracing::warn;

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Source contract
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
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
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "udev_monitor_read",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:system.udev"),
        "system.udev",
        "system",
    )
    .implementation("sinexd")
    .adapter("FileDropAdapter")
    .output_event_type("device.connected")
    .privacy_context("Metadata")
    .material_policy("udev_anchor")
    .checkpoint_policy("live_observation")
    .resource_shape("event_emitter")
    .source_id("system.udev")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("system_udev_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
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

// Register for dispatch (replay path).
register_source!(source_id: "system.udev", parser: UdevParser);

// Register source factory — FileDropAdapter + UdevParser.
crate::register_source!(
    source_id: "system.udev",
    adapter: FileDropAdapter,
    parser: UdevParser,
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::events::SourceMaterial;
    use sinex_primitives::ids::Id;
    use sinex_primitives::parser::MaterialAnchor;
    use sinex_primitives::primitives::Uuid;
    use xtask::sandbox::prelude::*;

    fn make_ctx(mid: Id<SourceMaterial>) -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("system.udev"),
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
        assert_eq!(
            intents[0].payload["device_path"],
            "/sys/bus/usb/devices/1-1"
        );
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
    async fn test_udev_parser_untyped_metadata_emits_other() -> TestResult<()> {
        // When the record metadata carries no recognizable event kind, the parser
        // deliberately classifies the event as `device.other` rather than guessing
        // a connect/disconnect from absent data (see `parse_record`: "emitting Other
        // action instead of guessing kind"). Properly-typed records still classify
        // as connected/disconnected — see the sibling tests.
        let mid = Id::<SourceMaterial>::new();
        let mut record = make_udev_record(mid, "/sys/bus/usb/devices/1-3", "Deleted");
        record.metadata = serde_json::json!({});

        let mut parser = UdevParser;
        let ctx = make_ctx(mid);
        let intents = parser.parse_record(record, &ctx).await.unwrap();

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type.as_str(), "device.other");
        Ok(())
    }

    #[sinex_test]
    async fn test_infer_device_type() -> TestResult<()> {
        assert!(matches!(
            infer_device_type("/sys/bus/usb/devices/1-1"),
            DeviceType::Usb
        ));
        assert!(matches!(
            infer_device_type("/sys/block/sda"),
            DeviceType::Storage
        ));
        assert!(matches!(
            infer_device_type("/sys/class/net/eth0"),
            DeviceType::Network
        ));
        assert!(matches!(
            infer_device_type("/sys/bus/other"),
            DeviceType::Other
        ));
        Ok(())
    }
}
