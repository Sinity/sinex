//! Desktop notification capture via D-Bus (#1033).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::node_sdk::parser::{MaterialParser, ParserError, ParserResult};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId, ParserManifest,
    SourceRecord, SourceId, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceRuntimeBinding, SourceBuildImpact, SourceContract, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

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
            source_id: SourceId::from_static("desktop.notification"),
            declared_event_types: vec![(
                EventSource::from_static("dbus"),
                EventType::from_static("notification.sent"),
            )],
            privacy_contexts: vec![ProcessingContext::Notification],
            sensitivity_hints: Vec::new(),
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
        // Match DbusNotificationSentPayload's concrete types (urgency: u8,
        // timeout: i32) so out-of-range D-Bus values can't fail admission-time
        // schema validation. D-Bus urgency is 0..=2; timeout is i32 ms (-1 = never).
        let urgency =
            u8::try_from(payload["urgency"].as_u64().unwrap_or_default()).unwrap_or(u8::MAX);
        let timeout =
            i32::try_from(payload["timeout"].as_i64().unwrap_or_default()).unwrap_or(i32::MAX);
        let actions = payload["actions"]
            .as_array()
            .map(|actions| {
                actions
                    .iter()
                    .filter_map(|action| action.as_str().map(ToOwned::to_owned))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let hints = payload["hints"].as_object().cloned().unwrap_or_default();

        Ok(vec![
            ParsedEventIntent::builder()
                .source_id(SourceId::from_static("desktop.notification"))
                .parser_id(ParserId::from_static("desktop-notification"))
                .parser_version("1.0.0")
                .event_source(EventSource::from_static("dbus"))
                .event_type(EventType::from_static("notification.sent"))
                .payload(serde_json::json!({
                    "app_name": app_name,
                    "summary": summary,
                    "body": body,
                    "urgency": urgency,
                    "timeout": timeout,
                    "actions": actions,
                    "hints": hints,
                    "timestamp": ts_orig,
                }))
                .ts_orig(ts_orig)
                .timing(TimingEvidence::Intrinsic {
                    field: "timestamp".into(),
                    confidence: TimingConfidence::Intrinsic,
                })
                .anchor(MaterialAnchor::ByteRange { start: 0, len: 1 })
                .occurrence_key(OccurrenceKey {
                    source_id: SourceId::from_static("desktop.notification"),
                    fields: vec![
                        ("app".into(), app_name.into()),
                        ("summary".into(), summary.into()),
                    ],
                })
                .privacy_context(ProcessingContext::Notification)
                .build(),
        ])
    }
}

register_source_contract! {
    SourceContract {
        id: "desktop.notification",
        namespace: "desktop",
        event_types: &[("dbus", "notification.sent")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(app_name, summary, body, ts)"),
        access_policy: "desktop_notifications",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:desktop.notification"),
        "desktop.notification",
        "desktop",
    )
    .implementation("live-capture")
    .adapter("DbusStreamAdapter")
    .output_event_type("notification.sent")
    .privacy_context("Notification")
    .material_policy("notification_stream_frame")
    .checkpoint_policy("dbus_stream_cursor")
    .resource_shape("dbus_signal_stream")
    .source_id("desktop.notification")
    .runner_pack("live")
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("desktop_notification_source")
    .implementation_mode("live:dbus-notification")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

crate::register_adapter_ingestor!(
    source_id: "desktop.notification",
    adapter: crate::node_sdk::parser::DbusStreamAdapter,
    parser: NotificationParser,
);
