//! `system.journald` — stream all journald entries via `JournalctlStreamAdapter`.

use crate::runtime::parser::{MaterialParser, ParserError};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::enums::JournalSyncType;
use sinex_primitives::events::payloads::system::{
    JournalEntryWrittenPayload, JournalSyncCompletedPayload,
};
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceId,
    SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::units::{Microseconds, ProcessId, SyslogPriority, UnixGid, UnixUid};

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

const MAX_JOURNAL_LINE_BYTES: usize = 256 * 1024;

fn journal_field<'a>(json: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    json.get(key).and_then(|v| v.as_str())
}

fn is_sinexd_journal_entry(json: &serde_json::Value) -> bool {
    journal_field(json, "_SYSTEMD_UNIT") == Some("sinexd.service")
        || journal_field(json, "SYSLOG_IDENTIFIER") == Some("sinexd")
        || journal_field(json, "_EXE").is_some_and(|exe| exe.ends_with("/sinexd"))
        || journal_field(json, "_CMDLINE").is_some_and(|cmdline| {
            cmdline
                .split_whitespace()
                .any(|part| part.ends_with("/sinexd") || part == "sinexd")
        })
}

fn is_confirmation_feedback_message(message: &str) -> bool {
    message.contains("Late confirmation arrived after provisional timeout")
        || message.contains("Late confirmations accepted after timeout")
        || message.contains("runtime.confirmation_late_total")
}

fn suppresses_confirmation_feedback_entry(json: &serde_json::Value, message: &str) -> bool {
    is_sinexd_journal_entry(json) && is_confirmation_feedback_message(message)
}

/// Parser for `system.journald` — converts journal JSON lines into typed events.
#[derive(Default, SourceMeta)]
#[source_meta(
    id = "system.journald",
    namespace = "system",
    event_source = "journald",
    event_type = "entry.written",
    event_types = "sync.completed",
    adapter = "JournalctlStreamAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Continuous, Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(source, journal_cursor)"),
    access_scope = AccessScope::SystemdJournal,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Journal,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::Journal,
    runtime_shape = RuntimeShape::Continuous,
)]
pub struct JournaldParser;

#[async_trait::async_trait]
impl MaterialParser for JournaldParser {
    type Config = serde_json::Value;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("system.journald"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::Subprocess],
            source_id: SourceId::from_static("system.journald"),
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
                .source_id(ctx.source_id.clone())
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

        if suppresses_confirmation_feedback_entry(&json, &message) {
            return Ok(Vec::new());
        }

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
            .source_id(ctx.source_id.clone())
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "journald_test.rs"]
mod tests;
