//! `terminal.asciinema` — Asciinema session recording parser.
//!
//! Walks the staged captures tree via [`DirectoryWalkAdapter`] and emits
//! events from each session directory (`<year>/<mm>/<dd>/<session-id>/`):
//!
//! - `session.json` → one `terminal.asciinema/session.recorded` event per
//!   session, carrying metadata: `session_id`, `ts_ms`, `cwd`, `schema`.
//! - `events.jsonl` → one `terminal.asciinema/session.prompt` event per
//!   `"prompt"` record, carrying `session_id`, `cwd`, `ts_ms`, `exit_code`.
//! - `session.cast` — terminal output frames; deferred (out of scope).
//!
//! The adapter config's `roots` should include the asciinema captures root,
//! e.g. `/realm/data/captures/asciinema`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_macros::SourceMeta;
use sinex_primitives::source_contracts::{AccessScope, ResourceProfile, RunnerPack, PrivacyTier, CheckpointFamily, RuntimeShape, RetentionPolicy, OccurrenceIdentity, Horizon};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::Timestamp;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SOURCE_ID: &str = "terminal.asciinema";
const EVENT_SOURCE_ASCIINEMA: &str = "terminal.asciinema";
const EVENT_TYPE_SESSION_RECORDED: &str = "session.recorded";
const EVENT_TYPE_SESSION_PROMPT: &str = "session.prompt";
const PARSER_ID: &str = "asciinema-session";
const PARSER_VERSION: &str = "1.0.0";

// ---------------------------------------------------------------------------
// Parser configuration
// ---------------------------------------------------------------------------

/// Configuration for [`AsciinemaParser`] (empty — adapter roots come from
/// the runtime binding's `DirectoryWalkConfig`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AsciinemaParserConfig;

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parser for staged Asciinema session recordings.
///
/// Each [`SourceRecord`] is the content of one file encountered during a
/// directory walk of the asciinema captures tree. The parser dispatches on
/// the filename:
///
/// - `session.json` → `session.recorded` event
/// - `events.jsonl` → `session.prompt` event per `"prompt"` line
/// - `session.cast` → skipped (terminal output frames; out of scope)
///
/// `#[derive(SourceMeta)]` collapses the `SourceContract`,
/// `SourceRuntimeBinding`, and `register_source!` factory wiring (#1727 slice
/// 3); the hand-written `MaterialParser` is kept because of the
/// filename-dispatched multi-event fan-out (two event types from one source).
#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "terminal.asciinema",
    namespace = "terminal",
    event_source = "terminal.asciinema",
    event_type = "session.recorded",
    event_types = "session.prompt",
    adapter = "DirectoryWalkAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(session_id, record_type[, line_index])"),
    access_scope = AccessScope::TargetData { path: "captures/asciinema" },
    privacy_context = ProcessingContext::Command,
    resource_profile = ResourceProfile::DirectoryScan,
    checkpoint_family = CheckpointFamily::MutableSnapshot { backing_store_kind: "directory", occurrence_anchor: "file_path_fingerprint" },
    runtime_shape = RuntimeShape::Continuous,
)]
pub struct AsciinemaParser;

#[async_trait]
impl MaterialParser for AsciinemaParser {
    type Config = AsciinemaParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static(PARSER_ID),
            parser_version: PARSER_VERSION.into(),
            accepted_input_shapes: vec![InputShapeKind::DirectoryWalk],
            source_id: SourceId::from_static(SOURCE_ID),
            declared_event_types: vec![
                (
                    EventSource::from_static(EVENT_SOURCE_ASCIINEMA),
                    EventType::from_static(EVENT_TYPE_SESSION_RECORDED),
                ),
                (
                    EventSource::from_static(EVENT_SOURCE_ASCIINEMA),
                    EventType::from_static(EVENT_TYPE_SESSION_PROMPT),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Command, ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: "Parses staged Asciinema session recordings into session and prompt \
                events."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let file_name = record
            .logical_path
            .as_ref()
            .and_then(|p| p.file_name())
            .unwrap_or("");

        match file_name {
            "session.json" => parse_session_json(&record, ctx),
            "events.jsonl" => parse_events_jsonl(&record, ctx),
            // session.cast — terminal output frames; deferred (out of scope for this issue).
            "session.cast" => Ok(vec![]),
            _ => Ok(vec![]),
        }
    }
}

// ---------------------------------------------------------------------------
// session.json → session.recorded
// ---------------------------------------------------------------------------

fn parse_session_json(
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParserResult<Vec<ParsedEventIntent>> {
    let logical_path = record
        .logical_path
        .as_ref()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_default();

    let doc: serde_json::Value = serde_json::from_slice(&record.bytes).map_err(|e| {
        ParserError::Parse(format!(
            "asciinema: invalid JSON in session.json at {logical_path:?}: {e}"
        ))
    })?;

    let session_id = doc
        .get("session_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| session_dir_fallback(record));
    let ts_ms: Option<i64> = doc
        .get("ts_ms")
        .and_then(sinex_primitives::JsonValue::as_i64);
    let cwd = doc
        .get("cwd")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let schema = doc
        .get("schema")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let (ts_orig, timing) = timestamp_from_ms(ts_ms, ctx);

    let payload = serde_json::json!({
        "session_id": session_id,
        "ts_ms": ts_ms,
        "cwd": cwd,
        "schema": schema,
    });

    let anchor = MaterialAnchor::ByteRange {
        start: 0,
        len: record.bytes.len() as u64,
    };

    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static(SOURCE_ID),
        fields: vec![
            ("session_id".into(), session_id),
            ("record_type".into(), "session_json".into()),
        ],
    };

    let intent = ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static(PARSER_ID))
        .parser_version(PARSER_VERSION)
        .event_type(EventType::from_static(EVENT_TYPE_SESSION_RECORDED))
        .event_source(EventSource::from_static(EVENT_SOURCE_ASCIINEMA))
        .payload(payload)
        .ts_orig(ts_orig)
        .timing(timing)
        .anchor(anchor)
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Command)
        .build();

    Ok(vec![intent])
}

// ---------------------------------------------------------------------------
// events.jsonl → session.prompt (one per "prompt" record)
// ---------------------------------------------------------------------------

fn parse_events_jsonl(
    record: &SourceRecord,
    ctx: &ParserContext,
) -> ParserResult<Vec<ParsedEventIntent>> {
    let logical_path = record
        .logical_path
        .as_ref()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_default();

    let content = std::str::from_utf8(&record.bytes).map_err(|e| {
        ParserError::Parse(format!(
            "asciinema: events.jsonl at {logical_path:?} is not valid UTF-8: {e}"
        ))
    })?;

    let mut intents = Vec::new();
    let mut byte_offset: u64 = 0;

    for (line_idx, line) in content.lines().enumerate() {
        let line_len = line.len() as u64;

        let event: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => {
                byte_offset += line_len + 1; // +1 for the newline
                continue;
            }
        };

        let record_type = event
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Only emit events for prompt records; skip session_start, recorder, etc.
        if record_type != "prompt" {
            byte_offset += line_len + 1;
            continue;
        }

        let session_id = event
            .get("session_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| session_dir_fallback(record));
        let ts_ms: Option<i64> = event
            .get("ts_ms")
            .and_then(sinex_primitives::JsonValue::as_i64);
        let cwd = event
            .get("cwd")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let exit_code = event
            .get("exit_code")
            .and_then(sinex_primitives::JsonValue::as_i64);

        let (ts_orig, timing) = timestamp_from_ms(ts_ms, ctx);

        let mut payload = serde_json::json!({
            "session_id": session_id,
            "ts_ms": ts_ms,
            "cwd": cwd,
        });
        if let Some(code) = exit_code {
            payload["exit_code"] = serde_json::Value::Number(code.into());
        }

        let anchor = MaterialAnchor::ByteRange {
            start: byte_offset,
            len: line_len,
        };

        let occurrence_key = OccurrenceKey {
            source_id: SourceId::from_static(SOURCE_ID),
            fields: vec![
                ("session_id".into(), session_id),
                ("record_type".into(), "prompt".into()),
                ("line_index".into(), line_idx.to_string()),
            ],
        };

        let intent = ParsedEventIntent::builder()
            .source_id(ctx.source_id.clone())
            .parser_id(ParserId::from_static(PARSER_ID))
            .parser_version(PARSER_VERSION)
            .event_type(EventType::from_static(EVENT_TYPE_SESSION_PROMPT))
            .event_source(EventSource::from_static(EVENT_SOURCE_ASCIINEMA))
            .payload(payload)
            .ts_orig(ts_orig)
            .timing(timing)
            .anchor(anchor)
            .occurrence_key(occurrence_key)
            .privacy_context(ProcessingContext::Command)
            .build();

        intents.push(intent);
        byte_offset += line_len + 1;
    }

    Ok(intents)
}

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------

/// Derive `ts_orig` and [`TimingEvidence`] from an optional Unix millisecond
/// timestamp.  `None` (field absent in JSON) falls back to the context's
/// acquisition time with `StagedAtFallback` evidence.
fn timestamp_from_ms(ts_ms: Option<i64>, ctx: &ParserContext) -> (Timestamp, TimingEvidence) {
    match ts_ms.and_then(Timestamp::from_unix_timestamp_millis) {
        Some(ts) => (
            ts,
            TimingEvidence::Intrinsic {
                field: "ts_ms".into(),
                confidence: TimingConfidence::Intrinsic,
            },
        ),
        None => (ctx.acquisition_time, TimingEvidence::StagedAtFallback),
    }
}

/// Derive a stable session discriminator from the file's parent directory
/// name when the JSON `session_id` field is absent or empty.  The directory
/// layout is `<year>/<mm>/<dd>/<session-dir>/`, so the parent name doubles
/// as the session identity.
fn session_dir_fallback(record: &SourceRecord) -> String {
    record
        .logical_path
        .as_ref()
        .and_then(|p| {
            std::path::Path::new(p.as_str())
                .parent()
                .and_then(std::path::Path::file_name)
                .map(|n| n.to_string_lossy().into_owned())
        })
        .unwrap_or_default()
}
