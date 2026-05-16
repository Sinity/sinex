//! `terminal.fish-history` — fish shell SQLite history adapter.
//!
//! Adapter: [`SqliteRowAdapter`] — reads from
//!          `~/.local/share/fish/fish_history`.
//! Parser:  [`FishHistoryParser`] — maps each row to
//!          [`HistoryCommandImportedPayload`].

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use sinex_node_sdk::parser::{MaterialParser, ParserError, ParserResult, SqliteRowAdapter};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::shell::HistoryCommandImportedPayload;
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceUnitId,
    TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_unit, register_source_unit_binding};

use crate::register_adapter_ingestor;

// ---------------------------------------------------------------------------
// Source unit descriptor
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal.fish-history",
        namespace: "terminal",
        event_types: &[("shell.history", "command.imported")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "obligation:source_unit.material_provenance",
            "obligation:source_unit.package_impact_rationale",
        ],
        occurrence_identity: OccurrenceIdentity::Natural,
        access_policy: "target_home_read:.local/share/fish/fish_history",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:terminal.fish-history"),
        "terminal.fish-history",
        "terminal",
    )
    .implementation("sinex-source-worker")
    .adapter("SqliteRowAdapter")
    .output_event_type("command.imported")
    .privacy_context("Command")
    .material_policy("sqlite_row_id")
    .checkpoint_policy("mutable_snapshot")
    .resource_shape("linear_rows_bounded_memory")
    .source_unit_id("terminal.fish-history")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::MutableSnapshot {
        backing_store_kind: "sqlite",
        occurrence_anchor: "fish_history_row_id",
    })
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("fish_history_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FishHistoryParserConfig;

/// Parser for fish shell SQLite history rows.
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
            source_unit_id: SourceUnitId::from_static("terminal.fish-history"),
            declared_event_types: vec![(
                EventSource::from_static("shell.history"),
                EventType::from_static("command.imported"),
            )],
            privacy_contexts: vec![ProcessingContext::Command],
            proof_obligations: vec![
                "obligation:source_unit.material_provenance".into(),
                "obligation:source_unit.package_impact_rationale".into(),
            ],
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

        let when_unix: Option<i64> = row.get("when").and_then(|v| v.as_i64());

        // Privacy processing.
        let processed = {
            let result = sinex_primitives::privacy::engine()
                .map_err(|e| ParserError::Parse(format!("privacy engine unavailable: {e}")))?
                .process(&command_raw, ProcessingContext::Command);
            if result.suppressed {
                return Ok(vec![]);
            }
            result.text.into_owned()
        };

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
            .map(|p| p.to_string())
            .unwrap_or_default();

        let payload = HistoryCommandImportedPayload {
            command: processed,
            timestamp,
            shell_type: "fish".into(),
            source_file,
            line_number: None,
        };

        let payload_json = serde_json::to_value(&payload)
            .map_err(|e| ParserError::Parse(format!("payload serialization failed: {e}")))?;

        Ok(vec![ParsedEventIntent {
            id: sinex_primitives::ids::Id::new(),
            source_unit_id: ctx.source_unit_id.clone(),
            parser_id: ParserId::from_static("fish-history"),
            parser_version: "1.0.0".into(),
            event_type: EventType::from_static("command.imported"),
            event_source: EventSource::from_static("shell.history"),
            payload: payload_json,
            ts_orig,
            timing,
            anchor: record.anchor,
            occurrence_key: None,
            privacy_context: ProcessingContext::Command,
            field_privacy_log: None,
            synthesis_parents: None,
        }])
    }

    fn baseline_adapter_config() -> serde_json::Value {
        // fish_history table; SqliteRowAdapter expands to
        // `SELECT rowid, * FROM fish_history`.
        serde_json::json!({ "query": "fish_history", "table": "fish_history" })
    }
}

// ---------------------------------------------------------------------------
// Node factory registration
// ---------------------------------------------------------------------------

register_adapter_ingestor!(
    source_unit_id: "terminal.fish-history",
    adapter: SqliteRowAdapter,
    parser: FishHistoryParser,
);
