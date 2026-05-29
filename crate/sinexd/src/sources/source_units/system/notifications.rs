//! Desktop notification capture via D-Bus (#1033).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::node_sdk::parser::{MaterialParser, ParserError, ParserResult};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId, ParserManifest,
    SourceRecord, SourceUnitId, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_unit, register_source_unit_binding};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationParserConfig;

#[derive(Debug, Clone, Default)]
pub struct NotificationParser;

#[async_trait]
impl MaterialParser for NotificationParser {
    type Config = NotificationParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("desktop-notification"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![],
            source_unit_id: SourceUnitId::from_static("desktop.notification"),
            declared_event_types: vec![(
                EventSource::from_static("desktop"),
                EventType::from_static("notification"),
            )],
            privacy_contexts: vec![ProcessingContext::Notification],
            proof_obligations: vec!["timestamp_material_time".into()],
            description: "Captures desktop notifications via D-Bus".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        _ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let payload: serde_json::Value = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("notification JSON: {e}")))?;

        let ts_orig = Timestamp::now();
        let app_name = payload["app_name"].as_str().unwrap_or("");
        let summary = payload["summary"].as_str().unwrap_or("");
        let body = payload["body"].as_str().unwrap_or("");

        Ok(vec![ParsedEventIntent::builder()
            .source_unit_id(SourceUnitId::from_static("desktop.notification"))
            .parser_id(ParserId::from_static("desktop-notification"))
            .parser_version("1.0.0")
            .event_source(EventSource::from_static("desktop"))
            .event_type(EventType::from_static("notification"))
            .payload(serde_json::json!({"app_name": app_name, "summary": summary, "body": body}))
            .ts_orig(ts_orig)
            .timing(TimingEvidence::Intrinsic {
                field: "timestamp".into(),
                confidence: TimingConfidence::Intrinsic,
            })
            .anchor(MaterialAnchor::ByteRange { start: 0, len: 1 })
            .occurrence_key(OccurrenceKey {
                source_unit_id: SourceUnitId::from_static("desktop.notification"),
                fields: vec![("app".into(), app_name.into()), ("summary".into(), summary.into())],
            })
            .privacy_context(ProcessingContext::Notification)
            .build()])
    }
}

register_source_unit! {
    SourceUnitDescriptor {
        id: "desktop.notification",
        namespace: "desktop",
        event_types: &[("desktop", "notification")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &["dbus_notification_monitor"],
        occurrence_identity: OccurrenceIdentity::Uuid5From("(app_name, summary, body, ts)"),
        access_policy: "desktop_notifications",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:desktop.notification"),
        "desktop.notification",
        "desktop",
    )
    .implementation("live-capture")
    .adapter("DbusStreamAdapter")
    .output_event_type("notification")
    .privacy_context("Sensitive")
    .material_policy("notification_stream_frame")
    .checkpoint_policy("dbus_stream_cursor")
    .resource_shape("dbus_signal_stream")
    .source_unit_id("desktop.notification")
    .runner_pack("live")
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("desktop_notification_source_unit")
    .implementation_mode("live:dbus-notification")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}
