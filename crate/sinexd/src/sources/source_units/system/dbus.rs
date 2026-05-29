//! `system.dbus` — stream D-Bus signals via `DbusStreamAdapter`.
//!
//! Dispatches 9 payload types based on interface + signal name patterns.
//! Notification body and D-Bus args are passed through the privacy engine.

use crate::register_parser;
use crate::node_sdk::parser::{DbusStreamAdapter, MaterialParser, ParserError};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::enums::{
    BluetoothEventType, DBusBus, DeviceType, MountEventType, NetworkConnectionType,
    NetworkEventType, NetworkState, PlaybackStatus, PowerEventType,
};
use sinex_primitives::events::payloads::system::{
    DbusBluetoothDeviceChangedPayload, DbusDeviceConnectedPayload, DbusMediaStateChangedPayload,
    DbusMethodCalledPayload, DbusMountEventPayload, DbusNetworkStateChangedPayload,
    DbusNotificationSentPayload, DbusPowerStateChangedPayload, DbusSignalPayload,
};
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
use sinex_primitives::{register_source_unit, register_source_unit_binding};

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Source-unit descriptor
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "system.dbus",
        namespace: "system",
        event_types: &[
            ("dbus", "signal.received"),
            ("dbus", "method.called"),
            ("dbus", "power.state_changed"),
            ("dbus", "bluetooth.device_changed"),
            ("dbus", "network.state_changed"),
            ("dbus", "device.connected"),
            ("dbus", "media.state_changed"),
            ("dbus", "mount.event"),
            ("dbus", "notification.sent"),
        ],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "dbus_interface_dispatch",
            "privacy_notification_body",
        ],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "system_bus_session_bus_read",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:system.dbus"),
        "system.dbus",
        "system",
    )
    .implementation("sinex-source-worker")
    .adapter("DbusStreamAdapter")
    .output_event_type("signal.received")
    .privacy_context("Dbus")
    .material_policy("bus_anchor")
    .checkpoint_policy("live_observation")
    .resource_shape("event_emitter")
    .source_unit_id("system.dbus")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("system_dbus_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

const MAX_DBUS_MESSAGE_BYTES: usize = 1_048_576;

/// Parser for `system.dbus` — dispatches to 9 payload types by interface + member.
#[derive(Default)]
pub struct DbusParser;

/// Classify a D-Bus message by interface and member into an event-type string.
fn classify_dbus_event(interface: &str, member: &str) -> &'static str {
    if interface.contains("Notifications") && (member == "Notify" || member == "ActionInvoked") {
        "notification.sent"
    } else if interface.contains("MPRIS") || interface.contains("MediaPlayer") {
        "media.state_changed"
    } else if interface.contains("UPower") || interface.contains("Power") {
        "power.state_changed"
    } else if interface.contains("Bluetooth") || interface.contains("bluez") {
        "bluetooth.device_changed"
    } else if interface.contains("NetworkManager") || interface.contains("network") {
        "network.state_changed"
    } else if interface.contains("UDisks") || interface.contains("udisks") {
        "mount.event"
    } else if interface.contains("UDev") || interface.contains("Device") {
        "device.connected"
    } else if member == "NameOwnerChanged" || interface.contains("DBus") {
        "method.called"
    } else {
        "signal.received"
    }
}

#[async_trait::async_trait]
impl MaterialParser for DbusParser {
    type Config = serde_json::Value;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("system.dbus"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::DbusSubscription],
            source_unit_id: SourceUnitId::from_static("system.dbus"),
            declared_event_types: vec![
                (
                    EventSource::from_static("dbus"),
                    EventType::from_static("signal.received"),
                ),
                (
                    EventSource::from_static("dbus"),
                    EventType::from_static("method.called"),
                ),
                (
                    EventSource::from_static("dbus"),
                    EventType::from_static("power.state_changed"),
                ),
                (
                    EventSource::from_static("dbus"),
                    EventType::from_static("bluetooth.device_changed"),
                ),
                (
                    EventSource::from_static("dbus"),
                    EventType::from_static("network.state_changed"),
                ),
                (
                    EventSource::from_static("dbus"),
                    EventType::from_static("device.connected"),
                ),
                (
                    EventSource::from_static("dbus"),
                    EventType::from_static("media.state_changed"),
                ),
                (
                    EventSource::from_static("dbus"),
                    EventType::from_static("mount.event"),
                ),
                (
                    EventSource::from_static("dbus"),
                    EventType::from_static("notification.sent"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Dbus, ProcessingContext::Notification],
            proof_obligations: vec![
                "dbus_interface_dispatch".into(),
                "privacy_notification_body".into(),
            ],
            description: "Dispatches D-Bus signals to 9 typed payload types.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> Result<Vec<ParsedEventIntent>, ParserError> {
        if record.bytes.len() > MAX_DBUS_MESSAGE_BYTES {
            return Err(ParserError::Parse(format!(
                "D-Bus message exceeds {MAX_DBUS_MESSAGE_BYTES} bytes, dropping"
            )));
        }

        let body_json: serde_json::Value =
            serde_json::from_slice(&record.bytes).unwrap_or(serde_json::Value::Null);

        let interface = record
            .metadata
            .get("interface")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let member = record
            .metadata
            .get("member")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let path = record
            .metadata
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let sender = record
            .metadata
            .get("sender")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let timestamp = Timestamp::now();
        let event_type_str = classify_dbus_event(&interface, &member);

        // Apply privacy to args for generic signals.
        let args_raw = body_json.to_string();
        let args_redacted = match privacy::engine() {
            Ok(eng) => eng
                .process(&args_raw, ProcessingContext::Dbus)
                .text
                .into_owned(),
            Err(e) => return Err(ParserError::Privacy(format!("privacy engine: {e}"))),
        };
        let args_value: serde_json::Value =
            serde_json::from_str(&args_redacted).unwrap_or(body_json.clone());

        let payload_value = match event_type_str {
            "notification.sent" => {
                let raw_summary = body_json
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let raw_body = body_json
                    .get(3)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let summary = match privacy::engine() {
                    Ok(eng) => eng
                        .process(&raw_summary, ProcessingContext::Notification)
                        .text
                        .into_owned(),
                    Err(e) => return Err(ParserError::Privacy(format!("privacy engine: {e}"))),
                };
                let body = match privacy::engine() {
                    Ok(eng) => eng
                        .process(&raw_body, ProcessingContext::Notification)
                        .text
                        .into_owned(),
                    Err(e) => return Err(ParserError::Privacy(format!("privacy engine: {e}"))),
                };
                let app_name = body_json
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let payload = DbusNotificationSentPayload {
                    app_name,
                    summary,
                    body,
                    urgency: 1,
                    timeout: -1,
                    actions: vec![],
                    hints: HashMap::new(),
                    timestamp,
                };
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?
            }
            "media.state_changed" => {
                let player = sender.clone();
                let payload = DbusMediaStateChangedPayload {
                    player: player.clone(),
                    player_instance: player,
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
                };
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?
            }
            "power.state_changed" => {
                let payload = DbusPowerStateChangedPayload {
                    event_type: PowerEventType::ProfileChanged,
                    details: args_value,
                    timestamp,
                };
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?
            }
            "bluetooth.device_changed" => {
                let payload = DbusBluetoothDeviceChangedPayload {
                    event_type: BluetoothEventType::Connected,
                    device_address: path.clone(),
                    device_name: None,
                    device_class: None,
                    rssi: None,
                    connected: true,
                    paired: false,
                    trusted: false,
                    timestamp,
                };
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?
            }
            "network.state_changed" => {
                let payload = DbusNetworkStateChangedPayload {
                    event_type: NetworkEventType::StateChanged,
                    interface: interface.clone(),
                    connection_type: NetworkConnectionType::Other,
                    ssid: None,
                    ip_address: None,
                    state: NetworkState::Unknown,
                    timestamp,
                };
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?
            }
            "mount.event" => {
                let payload = DbusMountEventPayload {
                    event_type: MountEventType::Mounted,
                    device: path.clone(),
                    mount_point: path.clone(),
                    filesystem: "unknown".into(),
                    label: None,
                    uuid: None,
                    size_bytes: None,
                    timestamp,
                };
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?
            }
            "device.connected" => {
                let payload = DbusDeviceConnectedPayload {
                    device_type: DeviceType::Other,
                    event_type: member.clone(),
                    device_path: path.clone(),
                    device_name: None,
                    vendor: None,
                    model: None,
                    serial: None,
                    properties: HashMap::new(),
                    timestamp,
                };
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?
            }
            "method.called" => {
                let payload = DbusMethodCalledPayload {
                    bus: DBusBus::Session,
                    sender: sender.clone(),
                    destination: String::new(),
                    path: path.clone(),
                    interface: interface.clone(),
                    method: member.clone(),
                    args: args_value,
                    timestamp,
                };
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?
            }
            _ => {
                // "signal.received" — generic fallback
                let payload = DbusSignalPayload {
                    bus: DBusBus::Session,
                    sender: sender.clone(),
                    path: path.clone(),
                    interface: interface.clone(),
                    signal: member.clone(),
                    args: args_value,
                    timestamp,
                };
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?
            }
        };

        let event_type = match event_type_str {
            "notification.sent" => EventType::from_static("notification.sent"),
            "media.state_changed" => EventType::from_static("media.state_changed"),
            "power.state_changed" => EventType::from_static("power.state_changed"),
            "bluetooth.device_changed" => EventType::from_static("bluetooth.device_changed"),
            "network.state_changed" => EventType::from_static("network.state_changed"),
            "mount.event" => EventType::from_static("mount.event"),
            "device.connected" => EventType::from_static("device.connected"),
            "method.called" => EventType::from_static("method.called"),
            _ => EventType::from_static("signal.received"),
        };

        let intent = ParsedEventIntent::builder()
            .source_unit_id(ctx.source_unit_id.clone())
            .parser_id(ParserId::from_static("system.dbus"))
            .parser_version("1.0.0")
            .event_type(event_type)
            .event_source(EventSource::from_static("dbus"))
            .payload(payload_value)
            .ts_orig(timestamp)
            .timing(TimingEvidence::Atemporal)
            .anchor(record.anchor.clone())
            .privacy_context(ProcessingContext::Dbus)
            .build();

        Ok(vec![intent])
    }

    fn baseline_adapter_config() -> serde_json::Value {
        // System D-Bus is the canonical bus for the signals this parser
        // classifies (power events, network changes, device add/remove).
        // Empty match_rules subscribes to all signals; Nix bindings can
        // override with a tighter filter.
        serde_json::json!({
            "bus": "system",
            "match_rules": []
        })
    }
}

// Register for dispatch (replay path).
register_parser!("system.dbus", DbusParser);

// Register node factory — DbusStreamAdapter + DbusParser.
crate::register_adapter_ingestor!(
    source_unit_id: "system.dbus",
    adapter: DbusStreamAdapter,
    parser: DbusParser,
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
            source_unit_id: SourceUnitId::from_static("system.dbus"),
            source_material_id: mid,
            record_anchor: MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 0,
            },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn make_dbus_record(
        mid: Id<SourceMaterial>,
        interface: &str,
        member: &str,
        body: serde_json::Value,
    ) -> SourceRecord {
        SourceRecord {
            material_id: mid,
            anchor: MaterialAnchor::StreamFrame {
                material_offset: 0,
                frame_index: 0,
            },
            bytes: serde_json::to_vec(&body).unwrap(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::json!({
                "interface": interface,
                "member": member,
                "path": "/org/test",
                "sender": ":1.42",
            }),
        }
    }

    #[sinex_test]
    async fn test_dbus_parser_signal_received() -> TestResult<()> {
        let mid = Id::<SourceMaterial>::new();
        let record = make_dbus_record(
            mid,
            "org.example.Unknown",
            "SomeSignal",
            serde_json::json!({"key": "value"}),
        );

        let mut parser = DbusParser;
        let ctx = make_ctx(mid);
        let intents = parser.parse_record(record, &ctx).await.unwrap();

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type.as_str(), "signal.received");
        assert_eq!(intents[0].event_source.as_str(), "dbus");
        Ok(())
    }

    #[sinex_test]
    async fn test_dbus_parser_notification() -> TestResult<()> {
        let mid = Id::<SourceMaterial>::new();
        let record = make_dbus_record(
            mid,
            "org.freedesktop.Notifications",
            "Notify",
            serde_json::json!(["MyApp", 0, "", "Summary", "Body", [], {}, -1]),
        );

        let mut parser = DbusParser;
        let ctx = make_ctx(mid);
        let intents = parser.parse_record(record, &ctx).await.unwrap();

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type.as_str(), "notification.sent");
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_dbus_event() -> TestResult<()> {
        assert_eq!(
            classify_dbus_event("org.freedesktop.Notifications", "Notify"),
            "notification.sent"
        );
        assert_eq!(
            classify_dbus_event("org.mpris.MediaPlayer2", "PropertiesChanged"),
            "media.state_changed"
        );
        assert_eq!(
            classify_dbus_event("org.example.Unknown", "Signal"),
            "signal.received"
        );
        Ok(())
    }
}
