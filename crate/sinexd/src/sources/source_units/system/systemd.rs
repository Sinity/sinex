//! `system.systemd` — systemd unit events filtered from journald.
//!
//! Uses `JournalctlStreamAdapter` (same subprocess as `system.journald`).
//! Records without `_SYSTEMD_UNIT` are silently skipped.

use crate::node_sdk::parser::{JournalctlStreamAdapter, MaterialParser, ParserError};
use crate::register_parser;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::enums::{SystemdActiveState, SystemdUnitType};
use sinex_primitives::events::payloads::system::{
    SystemdTimerTriggeredPayload, SystemdUnitFailedPayload, SystemdUnitReloadedPayload,
    SystemdUnitStartedPayload, SystemdUnitStoppedPayload,
};
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceRecord,
    SourceUnitId, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_unit, register_source_unit_binding};

// ---------------------------------------------------------------------------
// Source-unit descriptor
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "system.systemd",
        namespace: "system",
        event_types: &[
            ("systemd", "unit.started"),
            ("systemd", "unit.stopped"),
            ("systemd", "unit.failed"),
            ("systemd", "unit.reloaded"),
            ("systemd", "timer.triggered"),
        ],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "unit_name_present",
            "event_type_from_unit_result",
        ],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "systemd_journal_read",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:system.systemd"),
        "system.systemd",
        "system",
    )
    .implementation("sinex-source-worker")
    .adapter("JournalctlStreamAdapter")
    .output_event_type("unit.started")
    .privacy_context("Journal")
    .material_policy("journal_cursor")
    .checkpoint_policy("journal")
    .resource_shape("journal_tail")
    .source_unit_id("system.systemd")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::Journal)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("system_systemd_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parser for `system.systemd` — emits unit lifecycle events from journal records.
#[derive(Default)]
pub struct SystemdParser;

/// Infer `SystemdUnitType` from the unit name suffix.
fn infer_unit_type(name: &str) -> SystemdUnitType {
    if name.ends_with(".service") {
        SystemdUnitType::Service
    } else if name.ends_with(".socket") {
        SystemdUnitType::Socket
    } else if name.ends_with(".timer") {
        SystemdUnitType::Timer
    } else if name.ends_with(".target") {
        SystemdUnitType::Target
    } else if name.ends_with(".mount") {
        SystemdUnitType::Mount
    } else if name.ends_with(".slice") {
        SystemdUnitType::Slice
    } else if name.ends_with(".scope") {
        SystemdUnitType::Scope
    } else {
        SystemdUnitType::Other
    }
}

#[async_trait::async_trait]
impl MaterialParser for SystemdParser {
    type Config = serde_json::Value;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("system.systemd"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::Subprocess],
            source_unit_id: SourceUnitId::from_static("system.systemd"),
            declared_event_types: vec![
                (
                    EventSource::from_static("systemd"),
                    EventType::from_static("unit.started"),
                ),
                (
                    EventSource::from_static("systemd"),
                    EventType::from_static("unit.stopped"),
                ),
                (
                    EventSource::from_static("systemd"),
                    EventType::from_static("unit.failed"),
                ),
                (
                    EventSource::from_static("systemd"),
                    EventType::from_static("unit.reloaded"),
                ),
                (
                    EventSource::from_static("systemd"),
                    EventType::from_static("timer.triggered"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Journal],
            proof_obligations: vec![
                "unit_name_present".into(),
                "event_type_from_unit_result".into(),
            ],
            description: "Parses systemd unit lifecycle events from journald JSON lines.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> Result<Vec<ParsedEventIntent>, ParserError> {
        let json: serde_json::Value = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("failed to parse journal JSON: {e}")))?;

        // Only process records with a unit name.
        let unit_name = match json.get("_SYSTEMD_UNIT").and_then(|v| v.as_str()) {
            Some(u) => u.to_string(),
            None => return Ok(vec![]),
        };

        let cursor = json
            .get("__CURSOR")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let timestamp_us: i64 = json
            .get("__REALTIME_TIMESTAMP")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let timestamp = if timestamp_us > 0 {
            Timestamp::from_unix_timestamp(timestamp_us / 1_000_000).unwrap_or_else(Timestamp::now)
        } else {
            Timestamp::now()
        };

        let message = json
            .get("MESSAGE")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let pid_str = json
            .get("_PID")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);
        let uid_str = json
            .get("_UID")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        let unit_result = json
            .get("UNIT_RESULT")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let active_state_str = json
            .get("ACTIVE_STATE")
            .and_then(|v| v.as_str())
            .unwrap_or("inactive");
        let sub_state = json
            .get("SUB_STATE")
            .and_then(|v| v.as_str())
            .unwrap_or("dead")
            .to_string();

        let unit_type = infer_unit_type(&unit_name);
        let active_state = active_state_str
            .parse::<SystemdActiveState>()
            .unwrap_or(SystemdActiveState::Inactive);

        // Dispatch by message pattern / UNIT_RESULT.
        let (event_type_str, payload_value) = if unit_result == "failed"
            || (message.contains("Failed") && message.contains(&unit_name))
        {
            let payload = SystemdUnitFailedPayload {
                unit_name: unit_name.clone(),
                message,
                cursor,
                pid: pid_str,
                uid: uid_str,
                timestamp,
                journal_timestamp: Some(timestamp),
            };
            (
                "unit.failed",
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?,
            )
        } else if message.contains("Reloading") || message.contains("Reloaded") {
            let payload = SystemdUnitReloadedPayload {
                unit_name: Some(unit_name.clone()),
                message,
                cursor,
                pid: pid_str,
                uid: uid_str,
                timestamp,
                journal_timestamp: Some(timestamp),
            };
            (
                "unit.reloaded",
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?,
            )
        } else if unit_type == SystemdUnitType::Timer
            && (message.contains("Scheduled") || message.contains("Triggered"))
        {
            let payload = SystemdTimerTriggeredPayload {
                unit_name: Some(unit_name.clone()),
                message,
                cursor,
                pid: pid_str,
                uid: uid_str,
                timestamp,
                journal_timestamp: Some(timestamp),
            };
            (
                "timer.triggered",
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?,
            )
        } else if message.contains("Stopped") || message.contains("Deactivated") {
            let payload = SystemdUnitStoppedPayload {
                unit_name: unit_name.clone(),
                unit_type,
                exit_code: None,
                active_state,
                sub_state,
            };
            (
                "unit.stopped",
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?,
            )
        } else {
            let payload = SystemdUnitStartedPayload {
                unit_name: unit_name.clone(),
                unit_type,
                main_pid: None,
                active_state,
                sub_state,
            };
            (
                "unit.started",
                serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?,
            )
        };

        let event_type = match event_type_str {
            "unit.failed" => EventType::from_static("unit.failed"),
            "unit.reloaded" => EventType::from_static("unit.reloaded"),
            "timer.triggered" => EventType::from_static("timer.triggered"),
            "unit.stopped" => EventType::from_static("unit.stopped"),
            _ => EventType::from_static("unit.started"),
        };

        let intent = ParsedEventIntent::builder()
            .source_unit_id(ctx.source_unit_id.clone())
            .parser_id(ParserId::from_static("system.systemd"))
            .parser_version("1.0.0")
            .event_type(event_type)
            .event_source(EventSource::from_static("systemd"))
            .payload(payload_value)
            .ts_orig(timestamp)
            .timing(TimingEvidence::Intrinsic {
                field: "__REALTIME_TIMESTAMP".into(),
                confidence: TimingConfidence::Intrinsic,
            })
            .anchor(record.anchor.clone())
            .privacy_context(ProcessingContext::Journal)
            .build();

        Ok(vec![intent])
    }

    fn required_input_keys(&self) -> Vec<String> {
        [
            "/ACTIVE_STATE",
            "/MESSAGE",
            "/SUB_STATE",
            "/UNIT_RESULT",
            "/__CURSOR",
            "/__REALTIME_TIMESTAMP",
            "/_SYSTEMD_UNIT",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    }
}

// Register for dispatch (replay path).
register_parser!("system.systemd", SystemdParser);

// Register node factory — JournalctlStreamAdapter + SystemdParser.
crate::register_adapter_ingestor!(
    source_unit_id: "system.systemd",
    adapter: JournalctlStreamAdapter,
    parser: SystemdParser,
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_sdk::parser::records_from_journal_lines;
    use sinex_primitives::events::SourceMaterial;
    use sinex_primitives::ids::Id;
    use sinex_primitives::parser::MaterialAnchor;
    use sinex_primitives::primitives::Uuid;
    use xtask::sandbox::prelude::*;

    fn make_ctx(mid: Id<SourceMaterial>) -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static("system.systemd"),
            source_material_id: mid,
            record_anchor: MaterialAnchor::Line {
                byte_start: 0,
                line: 1,
            },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    #[sinex_test]
    async fn test_systemd_parser_unit_started() -> TestResult<()> {
        let mid = Id::<SourceMaterial>::new();
        let tok = ["ghp_", "0123456789abcdef0123456789abcdef0123"].concat();
        let line = format!(
            r#"{{"__CURSOR":"s=abc;i=2","__REALTIME_TIMESTAMP":"1700000001000000","_SYSTEMD_UNIT":"nginx.service","MESSAGE":"Started nginx.service with token {tok}.","PRIORITY":"6"}}"#
        );
        let records = records_from_journal_lines(mid, &[line.as_str()]);
        let record = records[0].as_ref().unwrap().clone();

        let mut parser = SystemdParser;
        let ctx = make_ctx(mid);
        let intents = parser.parse_record(record, &ctx).await.unwrap();

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type.as_str(), "unit.started");
        assert_eq!(intents[0].event_source.as_str(), "systemd");
        assert_eq!(
            intents[0].payload["message"],
            format!("Started nginx.service with token {tok}.")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_systemd_parser_skips_non_unit_records() -> TestResult<()> {
        let mid = Id::<SourceMaterial>::new();
        let line = r#"{"__CURSOR":"s=abc;i=3","MESSAGE":"generic log","PRIORITY":"6"}"#;
        let records = records_from_journal_lines(mid, &[line]);
        let record = records[0].as_ref().unwrap().clone();

        let mut parser = SystemdParser;
        let ctx = make_ctx(mid);
        let intents = parser.parse_record(record, &ctx).await.unwrap();

        assert_eq!(intents.len(), 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_infer_unit_type() -> TestResult<()> {
        assert!(matches!(
            infer_unit_type("nginx.service"),
            SystemdUnitType::Service
        ));
        assert!(matches!(
            infer_unit_type("cron.timer"),
            SystemdUnitType::Timer
        ));
        assert!(matches!(infer_unit_type("unknown"), SystemdUnitType::Other));
        Ok(())
    }
}
