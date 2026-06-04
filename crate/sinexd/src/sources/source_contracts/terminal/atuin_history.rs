//! `terminal.atuin-history` — Atuin `SQLite` history adapter.
//!
//! Folds the Atuin history source from `sinex-terminal-ingestor` into
//! the source dispatch and source factory registries.
//!
//! Adapter: [`SqliteRowAdapter`] — reads from `~/.local/share/atuin/history.db`.
//! Parser:  [`AtuinHistoryParser`] — maps each `SQLite` row to
//!          [`AtuinCommandExecutedPayload`].
//!
//! The source contract and binding are registered here; the
//! `terminal.atuin-history` binding in `sinex-primitives/src/proof.rs` is
//! the canonical primitive-level binding — this module registers the
//! source host implementation binding on top of it.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::node_sdk::parser::{MaterialParser, ParserError, ParserResult, SqliteRowAdapter};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::shell::AtuinCommandExecutedPayload;
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceId,
    TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceRuntimeBinding, SourceBuildImpact, SourceContract, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

use crate::register_adapter_ingestor;

// ---------------------------------------------------------------------------
// Source contract
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "terminal.atuin-history",
        namespace: "terminal",
        event_types: &[("shell.atuin", "command.executed")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Natural,
        access_policy: "target_home_read:.local/share/atuin/history.db",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:terminal.atuin-history"),
        "terminal.atuin-history",
        "terminal",
    )
    .implementation("sinexd")
    .adapter("SqliteRowAdapter")
    .output_event_type("command.executed")
    .privacy_context("Command")
    .material_policy("sqlite_row_id")
    .checkpoint_policy("mutable_snapshot")
    .resource_shape("linear_rows_bounded_memory")
    .source_id("terminal.atuin-history")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::MutableSnapshot {
        backing_store_kind: "sqlite",
        occurrence_anchor: "atuin_history_id",
    })
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("atuin_history_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parser configuration (empty — path comes from [`SqliteRowConfig`]).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AtuinHistoryParserConfig;

/// Parser for Atuin `SQLite` history rows.
///
/// Each [`SourceRecord`] carries a JSON-serialized row from the `history`
/// table. The parser extracts fields and builds [`AtuinCommandExecutedPayload`].
#[derive(Debug, Clone, Default)]
pub struct AtuinHistoryParser;

#[async_trait]
impl MaterialParser for AtuinHistoryParser {
    type Config = AtuinHistoryParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("atuin-history"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::SqliteQuery],
            source_id: SourceId::from_static("terminal.atuin-history"),
            declared_event_types: vec![(
                EventSource::from_static("shell.atuin"),
                EventType::from_static("command.executed"),
            )],
            privacy_contexts: vec![ProcessingContext::Command, ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
            description: "Parses Atuin SQLite history rows into command.executed events.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: sinex_primitives::parser::SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let row: serde_json::Value = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("invalid JSON in atuin row: {e}")))?;

        // Extract fields from the JSON row (serialised by SqliteRowAdapter).
        let command_string = row
            .get("command")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if command_string.is_empty() {
            return Ok(vec![]);
        }

        let history_id = row
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let session_id = row
            .get("session")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let hostname_raw = row
            .get("hostname")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let cwd_raw = row
            .get("cwd")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let timestamp_ns = row
            .get("timestamp")
            .and_then(sinex_primitives::JsonValue::as_i64)
            .unwrap_or(0);
        let duration_ns = row
            .get("duration")
            .and_then(sinex_primitives::JsonValue::as_i64)
            .unwrap_or(0);
        let exit_code = row
            .get("exit")
            .and_then(sinex_primitives::JsonValue::as_i64)
            .unwrap_or(0);

        let cwd = cwd_raw.into();
        let payload_result = AtuinCommandExecutedPayload::from_raw_history(
            command_string,
            cwd,
            exit_code,
            duration_ns,
            history_id,
            session_id,
            timestamp_ns,
            hostname_raw,
        );

        let payload = match payload_result {
            Ok(p) => p,
            Err(e) => {
                return Err(ParserError::Parse(format!(
                    "atuin payload construction failed: {e}"
                )));
            }
        };

        let ts_orig = payload.ts_start_orig;
        let payload_json = serde_json::to_value(&payload)
            .map_err(|e| ParserError::Parse(format!("payload serialization failed: {e}")))?;

        Ok(vec![
            ParsedEventIntent::builder()
                .source_id(ctx.source_id.clone())
                .parser_id(ParserId::from_static("atuin-history"))
                .parser_version("1.0.0")
                .event_type(EventType::from_static("command.executed"))
                .event_source(EventSource::from_static("shell.atuin"))
                .payload(payload_json)
                .ts_orig(ts_orig)
                .timing(TimingEvidence::Intrinsic {
                    field: "timestamp".into(),
                    confidence: TimingConfidence::Intrinsic,
                })
                .anchor(record.anchor)
                .privacy_context(ProcessingContext::Command)
                .build(),
        ])
    }

    fn required_input_keys(&self) -> Vec<String> {
        ["history.command", "history.timestamp"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }

    fn baseline_adapter_config() -> serde_json::Value {
        // Atuin's history table has columns (id, timestamp, duration, exit,
        // command, cwd, session, hostname, deleted_at). SqliteRowAdapter
        // expands query="history" to `SELECT rowid, * FROM history`, which
        // provides every column AtuinHistoryParser reads.
        serde_json::json!({ "query": "history", "table": "history" })
    }
}

// ---------------------------------------------------------------------------
// Source factory registration
// ---------------------------------------------------------------------------

register_adapter_ingestor!(
    source_id: "terminal.atuin-history",
    adapter: SqliteRowAdapter,
    parser: AtuinHistoryParser,
);
