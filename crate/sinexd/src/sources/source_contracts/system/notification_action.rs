//! Desktop notification action invocation capture via D-Bus (#1647).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::source_contracts::{AccessScope, ResourceProfile, RunnerPack, PrivacyTier, CheckpointFamily, RuntimeShape, RetentionPolicy, OccurrenceIdentity, Horizon};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId, ParserManifest,
    SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::Timestamp;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationActionParserConfig;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "desktop.notification.action",
    namespace = "desktop",
    event_source = "dbus",
    event_type = "notification.action_invoked",
    adapter = "DbusStreamAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Continuous),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(notification_id, action_key)"),
    access_scope = AccessScope::SessionBus,
    implementation = "live-capture",
    privacy_context = ProcessingContext::Notification,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::Live,
    checkpoint_family = CheckpointFamily::LiveObservation,
    runtime_shape = RuntimeShape::Continuous,
)]
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
