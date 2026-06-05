//! `terminal.fish-history` — fish shell `SQLite` history adapter.
//!
//! Adapter: [`SqliteRowAdapter`] — reads from
//!          `~/.local/share/fish/fish_history`.
//! Parser:  [`FishHistoryParser`] — maps each row to
//!          [`HistoryCommandImportedPayload`].

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult, SqliteRowAdapter};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::shell::HistoryCommandImportedPayload;
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceId,
    TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceRuntimeBinding, SourceBuildImpact, SourceContract, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

use crate::register_adapter_ingestor;

// ---------------------------------------------------------------------------
// Source contract
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "terminal.fish-history",
        namespace: "terminal",
        event_types: &[("shell.history", "command.imported")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Natural,
        access_policy: "target_home_read:.local/share/fish/fish_history",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:terminal.fish-history"),
        "terminal.fish-history",
        "terminal",
    )
    .implementation("sinexd")
    .adapter("SqliteRowAdapter")
    .output_event_type("command.imported")
    .privacy_context("Command")
    .material_policy("sqlite_row_id")
    .checkpoint_policy("mutable_snapshot")
    .resource_shape("linear_rows_bounded_memory")
    .source_id("terminal.fish-history")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::MutableSnapshot {
        backing_store_kind: "sqlite",
        occurrence_anchor: "fish_history_row_id",
    })
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("fish_history_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FishHistoryParserConfig;

/// Parser for fish shell `SQLite` history rows.
///
/// Each [`SourceRecord`] carries a JSON-serialized row from the `history`
/// table with columns `ROWID`, `command`, `when` (optional Unix seconds).
#[derive(Debug, Clone, Default)]
pub struct FishHistoryParser;

#[async_trait]
impl MaterialParser for FishHistoryParser {
    type Config = FishHistoryParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("fish-history"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::SqliteQuery],
            source_id: SourceId::from_static("terminal.fish-history"),
            declared_event_types: vec![(
                EventSource::from_static("shell.history"),
                EventType::from_static("command.imported"),
            )],
            privacy_contexts: vec![ProcessingContext::Command],
            sensitivity_hints: Vec::new(),
            description: "Parses fish shell SQLite history rows into command.imported events."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: sinex_primitives::parser::SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let row: serde_json::Value = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("invalid JSON in fish row: {e}")))?;

        let command_raw = row
            .get("command")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if command_raw.is_empty() {
            return Ok(vec![]);
        }

        let when_unix: Option<i64> = row
            .get("when")
            .and_then(sinex_primitives::JsonValue::as_i64);

        let (ts_orig, timing, timestamp) = match when_unix {
            Some(unix_secs) => match Timestamp::from_unix_timestamp(unix_secs) {
                Some(t) => (
                    t,
                    TimingEvidence::Intrinsic {
                        field: "when".into(),
                        confidence: TimingConfidence::Intrinsic,
                    },
                    Some(t),
                ),
                None => (Timestamp::now(), TimingEvidence::StagedAtFallback, None),
            },
            None => (Timestamp::now(), TimingEvidence::StagedAtFallback, None),
        };

        let source_file = record
            .logical_path
            .as_ref()
            .map(std::string::ToString::to_string)
            .unwrap_or_default();

        let payload = HistoryCommandImportedPayload {
            command: command_raw,
            timestamp,
            shell_type: "fish".into(),
            source_file,
            line_number: None,
        };

        let payload_json = serde_json::to_value(&payload)
            .map_err(|e| ParserError::Parse(format!("payload serialization failed: {e}")))?;

        Ok(vec![
            ParsedEventIntent::builder()
                .source_id(ctx.source_id.clone())
                .parser_id(ParserId::from_static("fish-history"))
                .parser_version("1.0.0")
                .event_type(EventType::from_static("command.imported"))
                .event_source(EventSource::from_static("shell.history"))
                .payload(payload_json)
                .ts_orig(ts_orig)
                .timing(timing)
                .anchor(record.anchor)
                .privacy_context(ProcessingContext::Command)
                .build(),
        ])
    }

    fn required_input_keys(&self) -> Vec<String> {
        vec!["fish_history.command".to_owned()]
    }

    fn baseline_adapter_config() -> serde_json::Value {
        // fish_history table; SqliteRowAdapter expands to
        // `SELECT rowid, * FROM fish_history`.
        serde_json::json!({ "query": "fish_history", "table": "fish_history" })
    }
}

// ---------------------------------------------------------------------------
// Source factory registration
// ---------------------------------------------------------------------------

register_adapter_ingestor!(
    source_id: "terminal.fish-history",
    adapter: SqliteRowAdapter,
    parser: FishHistoryParser,
);
