//! `system.journald` — stream all journald entries via `JournalctlStreamAdapter`.

use crate::node_sdk::parser::{JournalctlStreamAdapter, MaterialParser, ParserError};
use crate::register_parser;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::enums::JournalSyncType;
use sinex_primitives::events::payloads::system::{
    JournalEntryWrittenPayload, JournalSyncCompletedPayload,
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
use sinex_primitives::units::{Microseconds, ProcessId, SyslogPriority, UnixGid, UnixUid};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Source-unit descriptor
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "system.journald",
        namespace: "system",
        event_types: &[
            ("journald", "entry.written"),
            ("journald", "sync.completed"),
        ],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "timestamp_intrinsic",
            "cursor_anchor",
            "privacy_context_declared",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From("(source_unit, journal_cursor)"),
        access_policy: "systemd_journal_read",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:system.journald"),
        "system.journald",
        "system",
    )
    .implementation("sinex-source-worker")
    .adapter("JournalctlStreamAdapter")
    .output_event_type("entry.written")
    .privacy_context("Journal")
    .material_policy("journal_cursor")
    .checkpoint_policy("journal")
    .resource_shape("journal_tail")
    .source_unit_id("system.journald")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::Journal)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("system_journald_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

const MAX_JOURNAL_LINE_BYTES: usize = 256 * 1024;

/// Parser for `system.journald` — converts journal JSON lines into typed events.
#[derive(Default)]
pub struct JournaldParser;

#[async_trait::async_trait]
impl MaterialParser for JournaldParser {
    type Config = serde_json::Value;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("system.journald"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::Subprocess],
            source_unit_id: SourceUnitId::from_static("system.journald"),
            declared_event_types: vec![
                (
                    EventSource::from_static("journald"),
                    EventType::from_static("entry.written"),
                ),
                (
                    EventSource::from_static("journald"),
                    EventType::from_static("sync.completed"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Journal, ProcessingContext::Command],
            sensitivity_hints: Vec::new(),
            proof_obligations: vec![
                "timestamp_intrinsic".into(),
                "cursor_anchor".into(),
                "privacy_context_declared".into(),
            ],
            description: "Parses journald JSON lines into entry.written and sync.completed events."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> Result<Vec<ParsedEventIntent>, ParserError> {
        if record.bytes.len() > MAX_JOURNAL_LINE_BYTES {
            return Err(ParserError::Parse(format!(
                "journal line exceeds {MAX_JOURNAL_LINE_BYTES} bytes, dropping"
            )));
        }

        let json: serde_json::Value = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("failed to parse journal JSON: {e}")))?;

        let cursor = json
            .get("__CURSOR")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Sync boundary events.
        if json.get("__JOURNAL_SYNC") == Some(&serde_json::Value::String("1".into()))
            || json
                .get("MESSAGE")
                .and_then(|v| v.as_str())
                .is_some_and(|m| m.contains("Journal sync"))
        {
            let payload = JournalSyncCompletedPayload {
                sync_type: JournalSyncType::Incremental,
                start_cursor: None,
                end_cursor: cursor.clone(),
                entries_count: 0,
                time_start: None,
                time_end: None,
                duration_ms: 0,
            };
            let intent = ParsedEventIntent::builder()
                .source_unit_id(ctx.source_unit_id.clone())
                .parser_id(ParserId::from_static("system.journald"))
                .parser_version("1.0.0")
                .event_type(EventType::from_static("sync.completed"))
                .event_source(EventSource::from_static("journald"))
                .payload(
                    serde_json::to_value(&payload)
                        .map_err(|e| ParserError::Parse(e.to_string()))?,
                )
                .ts_orig(Timestamp::now())
                .timing(TimingEvidence::Atemporal)
                .anchor(record.anchor.clone())
                .privacy_context(ProcessingContext::Journal)
                .build();
            return Ok(vec![intent]);
        }

        // Parse a normal journal entry.
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

        let cmdline = json
            .get("_CMDLINE")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        let exe = json
            .get("_EXE")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        let mut fields: HashMap<String, String> = HashMap::new();
        if let Some(obj) = json.as_object() {
            for (k, v) in obj {
                if let Some(s) = v.as_str() {
                    fields.insert(k.clone(), s.to_string());
                }
            }
        }

        let payload = JournalEntryWrittenPayload {
            cursor,
            timestamp_us: Microseconds::from(timestamp_us),
            timestamp,
            hostname: json
                .get("_HOSTNAME")
                .and_then(|v| v.as_str())
                .map(std::string::ToString::to_string),
            unit: json
                .get("_SYSTEMD_UNIT")
                .and_then(|v| v.as_str())
                .map(std::string::ToString::to_string),
            syslog_identifier: json
                .get("SYSLOG_IDENTIFIER")
                .and_then(|v| v.as_str())
                .map(std::string::ToString::to_string),
            pid: json
                .get("_PID")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u32>().ok())
                .map(ProcessId::from),
            uid: json
                .get("_UID")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u32>().ok())
                .map(UnixUid::from),
            gid: json
                .get("_GID")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u32>().ok())
                .map(UnixGid::from),
            cmdline,
            exe,
            unit_type: None,
            priority: json
                .get("PRIORITY")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u8>().ok())
                .map(SyslogPriority::from),
            facility: json
                .get("SYSLOG_FACILITY")
                .and_then(|v| v.as_str())
                .map(std::string::ToString::to_string),
            message,
            fields,
        };

        let intent = ParsedEventIntent::builder()
            .source_unit_id(ctx.source_unit_id.clone())
            .parser_id(ParserId::from_static("system.journald"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static("entry.written"))
            .event_source(EventSource::from_static("journald"))
            .payload(serde_json::to_value(&payload).map_err(|e| ParserError::Parse(e.to_string()))?)
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
        ["/MESSAGE", "/__CURSOR", "/__REALTIME_TIMESTAMP"]
            .into_iter()
            .map(str::to_string)
            .collect()
    }
}

// Register for dispatch (replay path).
register_parser!("system.journald", JournaldParser);

// Register node factory — JournalctlStreamAdapter + JournaldParser.
crate::register_adapter_ingestor!(
    source_unit_id: "system.journald",
    adapter: JournalctlStreamAdapter,
    parser: JournaldParser,
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
            source_unit_id: SourceUnitId::from_static("system.journald"),
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
    async fn test_journald_parser_entry_written() -> TestResult<()> {
        let mid = Id::<SourceMaterial>::new();
        let tok = ["ghp_", "0123456789abcdef0123456789abcdef0123"].concat();
        let line = format!(
            r#"{{"__CURSOR":"s=abc;i=1","__REALTIME_TIMESTAMP":"1700000000000000","MESSAGE":"export GITHUB_TOKEN={tok}","_CMDLINE":"curl -H token={tok}","_HOSTNAME":"host1","PRIORITY":"6"}}"#
        );
        let records = records_from_journal_lines(mid, &[line.as_str()]);
        let record = records[0].as_ref().unwrap().clone();

        let mut parser = JournaldParser;
        let ctx = make_ctx(mid);
        let intents = parser.parse_record(record, &ctx).await.unwrap();

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].event_type.as_str(), "entry.written");
        assert_eq!(intents[0].event_source.as_str(), "journald");
        assert_eq!(
            intents[0].payload["message"],
            format!("export GITHUB_TOKEN={tok}")
        );
        assert_eq!(
            intents[0].payload["cmdline"],
            format!("curl -H token={tok}")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_journald_parser_filters_empty_lines() -> TestResult<()> {
        let mid = Id::<SourceMaterial>::new();
        let line = "";
        let records = records_from_journal_lines(mid, &[line]);

        assert!(
            records.is_empty(),
            "journal helper should mirror live stream filtering for empty lines"
        );
        Ok(())
    }
}
