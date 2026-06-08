//! Desktop notification action invocation capture via D-Bus (#1647).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId, ParserManifest,
    SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceBuildImpact, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationActionParserConfig;

#[derive(Debug, Clone, Default)]
pub struct NotificationActionParser;

#[async_trait]
impl MaterialParser for NotificationActionParser {
    type Config = NotificationActionParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("desktop-notification-action"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![],
            source_id: SourceId::from_static("desktop.notification.action"),
            declared_event_types: vec![(
                EventSource::from_static("dbus"),
                EventType::from_static("notification.action_invoked"),
            )],
            privacy_contexts: vec![ProcessingContext::Notification],
            sensitivity_hints: Vec::new(),
            description: "Captures notification action invocations via D-Bus".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        _ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let payload: serde_json::Value = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("notification-action JSON: {e}")))?;

        let ts_orig = Timestamp::now();
        let notification_id = payload["notification_id"].as_u64().unwrap_or_default() as u32;
        let action_key = payload["action_key"].as_str().unwrap_or("").to_owned();

        Ok(vec![ParsedEventIntent::builder()
            .source_id(SourceId::from_static("desktop.notification.action"))
            .parser_id(ParserId::from_static("desktop-notification-action"))
            .parser_version("1.0.0")
            .event_source(EventSource::from_static("dbus"))
            .event_type(EventType::from_static("notification.action_invoked"))
            .payload(serde_json::json!({
                "notification_id": notification_id,
                "action_key": action_key,
                "timestamp": ts_orig,
            }))
            .ts_orig(ts_orig)
            .timing(TimingEvidence::Intrinsic {
                field: "timestamp".into(),
                confidence: TimingConfidence::Intrinsic,
            })
            .anchor(MaterialAnchor::ByteRange { start: 0, len: 1 })
            .occurrence_key(OccurrenceKey {
                source_id: SourceId::from_static("desktop.notification.action"),
                fields: vec![
                    ("notification_id".into(), notification_id.to_string()),
                    ("action_key".into(), action_key.clone()),
                ],
            })
            .privacy_context(ProcessingContext::Notification)
            .build()])
    }
}

register_source_contract! {
    SourceContract {
        id: "desktop.notification.action",
        namespace: "desktop",
        event_types: &[("dbus", "notification.action_invoked")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(notification_id, action_key)"),
        access_policy: "desktop_notifications",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:desktop.notification.action"),
        "desktop.notification.action",
        "desktop",
    )
    .implementation("live-capture")
    .adapter("DbusStreamAdapter")
    .output_event_type("notification.action_invoked")
    .privacy_context("Notification")
    .material_policy("notification_stream_frame")
    .checkpoint_policy("dbus_stream_cursor")
    .resource_shape("dbus_signal_stream")
    .source_id("desktop.notification.action")
    .runner_pack("live")
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("desktop_notification_action_source")
    .implementation_mode("live:dbus-notification-action")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

crate::register_source!(
    source_id: "desktop.notification.action",
    adapter: crate::runtime::parser::DbusStreamAdapter,
    parser: NotificationActionParser,
);
