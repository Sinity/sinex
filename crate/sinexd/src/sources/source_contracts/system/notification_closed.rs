//! Desktop notification close capture via D-Bus (#1647).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId, ParserManifest,
    SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::Timestamp;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationClosedParserConfig;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "desktop.notification.closed",
    namespace = "desktop",
    event_source = "dbus",
    event_type = "notification.closed",
    adapter = "DbusStreamAdapter",
    privacy_tier = "Public",
    horizons = "continuous",
    retention = "forever",
    occurrence_identity = "uuid5:(notification_id)",
    access_policy = "desktop_notifications",
    implementation = "live-capture",
    privacy_context = "Notification",
    material_policy = "notification_stream_frame",
    checkpoint_policy = "dbus_stream_cursor",
    resource_shape = "dbus_signal_stream",
    runner_pack = "live",
    checkpoint_family = "live_observation",
    runtime_shape = "continuous",
    package_impact = "desktop_notification_closed_source",
    implementation_mode = "live:dbus-notification-closed"
)]
pub struct NotificationClosedParser;

fn reason_label(reason: u32) -> &'static str {
    match reason {
        1 => "expired",
        2 => "dismissed",
        3 => "closed",
        _ => "undefined",
    }
}

#[async_trait]
impl MaterialParser for NotificationClosedParser {
    type Config = NotificationClosedParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("desktop-notification-closed"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![],
            source_id: SourceId::from_static("desktop.notification.closed"),
            declared_event_types: vec![(
                EventSource::from_static("dbus"),
                EventType::from_static("notification.closed"),
            )],
            privacy_contexts: vec![ProcessingContext::Notification],
            sensitivity_hints: Vec::new(),
            description: "Captures notification close events via D-Bus".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        _ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let payload: serde_json::Value = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("notification-closed JSON: {e}")))?;

        let ts_orig = Timestamp::now();
        let notification_id = payload["notification_id"].as_u64().unwrap_or_default() as u32;
        let reason = payload["reason"].as_u64().unwrap_or_default() as u32;
        let label = reason_label(reason);

        // No in-process correlation with notification.sent is available here;
        // cross-event linkage is a downstream analysis concern.

        Ok(vec![ParsedEventIntent::builder()
            .source_id(SourceId::from_static("desktop.notification.closed"))
            .parser_id(ParserId::from_static("desktop-notification-closed"))
            .parser_version("1.0.0")
            .event_source(EventSource::from_static("dbus"))
            .event_type(EventType::from_static("notification.closed"))
            .payload(serde_json::json!({
                "notification_id": notification_id,
                "reason": reason,
                "reason_label": label,
                "timestamp": ts_orig,
            }))
            .ts_orig(ts_orig)
            .timing(TimingEvidence::Intrinsic {
                field: "timestamp".into(),
                confidence: TimingConfidence::Intrinsic,
            })
            .anchor(MaterialAnchor::ByteRange { start: 0, len: 1 })
            .occurrence_key(OccurrenceKey {
                source_id: SourceId::from_static("desktop.notification.closed"),
                fields: vec![("notification_id".into(), notification_id.to_string())],
            })
            .privacy_context(ProcessingContext::Notification)
            .build()])
    }
}
