//! `terminal.atuin-history` — Atuin `SQLite` history adapter.
//!
//! Folds the Atuin history source unit from `sinex-terminal-ingestor` into
//! the source-worker dispatch and node factory registries.
//!
//! Adapter: [`SqliteRowAdapter`] — reads from `~/.local/share/atuin/history.db`.
//! Parser:  [`AtuinHistoryParser`] — maps each `SQLite` row to
//!          [`AtuinCommandExecutedPayload`].
//!
//! The source-unit descriptor and binding are registered here; the
//! `terminal.atuin-history` binding in `sinex-primitives/src/proof.rs` is
//! the canonical primitive-level binding — this module registers the
//! source-worker implementation binding on top of it.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use sinex_node_sdk::parser::{MaterialParser, ParserError, ParserResult, SqliteRowAdapter};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::shell::AtuinCommandExecutedPayload;
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceUnitId,
    TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

use crate::register_adapter_ingestor;

// ---------------------------------------------------------------------------
// Source unit descriptor
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal.atuin-history",
        namespace: "terminal",
        event_types: &[("shell.atuin", "command.executed")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "obligation:source_unit.material_provenance",
            "obligation:source_unit.package_impact_rationale",
        ],
        occurrence_identity: OccurrenceIdentity::Natural,
        access_policy: "target_home_read:.local/share/atuin/history.db",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:terminal.atuin-history"),
        "terminal.atuin-history",
        "terminal",
    )
    .implementation("sinex-source-worker")
    .adapter("SqliteRowAdapter")
    .output_event_type("command.executed")
    .privacy_context("Command")
    .material_policy("sqlite_row_id")
    .checkpoint_policy("mutable_snapshot")
    .resource_shape("linear_rows_bounded_memory")
    .source_unit_id("terminal.atuin-history")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::MutableSnapshot {
        backing_store_kind: "sqlite",
        occurrence_anchor: "atuin_history_id",
    })
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("atuin_history_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
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
            source_unit_id: SourceUnitId::from_static("terminal.atuin-history"),
            declared_event_types: vec![(
                EventSource::from_static("shell.atuin"),
                EventType::from_static("command.executed"),
            )],
            privacy_contexts: vec![ProcessingContext::Command, ProcessingContext::Metadata],
            proof_obligations: vec![
                "obligation:source_unit.material_provenance".into(),
                "obligation:source_unit.package_impact_rationale".into(),
            ],
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

        // Apply privacy processing.
        let command_processed = {
            let result = sinex_primitives::privacy::engine()
                .map_err(|e| ParserError::Parse(format!("privacy engine unavailable: {e}")))?
                .process(&command_string, ProcessingContext::Command);
            if result.suppressed {
                return Ok(vec![]);
            }
            result.text.into_owned()
        };
        let cwd_processed = {
            let result = sinex_primitives::privacy::engine()
                .map_err(|e| ParserError::Parse(format!("privacy engine unavailable: {e}")))?
                .process(&cwd_raw, ProcessingContext::Metadata);
            result.text.into_owned()
        };

        let cwd = cwd_processed.into();
        let payload_result = AtuinCommandExecutedPayload::from_raw_history(
            command_processed,
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

        Ok(vec![ParsedEventIntent {
            id: sinex_primitives::ids::Id::new(),
            source_unit_id: ctx.source_unit_id.clone(),
            parser_id: ParserId::from_static("atuin-history"),
            parser_version: "1.0.0".into(),
            event_type: EventType::from_static("command.executed"),
            event_source: EventSource::from_static("shell.atuin"),
            payload: payload_json,
            ts_orig,
            timing: TimingEvidence::Intrinsic {
                field: "timestamp".into(),
                confidence: TimingConfidence::Intrinsic,
            },
            anchor: record.anchor,
            occurrence_key: None,
            privacy_context: ProcessingContext::Command,
            field_privacy_log: None,
            synthesis_parents: None,
        }])
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
// Node factory registration
// ---------------------------------------------------------------------------

register_adapter_ingestor!(
    source_unit_id: "terminal.atuin-history",
    adapter: SqliteRowAdapter,
    parser: AtuinHistoryParser,
);
