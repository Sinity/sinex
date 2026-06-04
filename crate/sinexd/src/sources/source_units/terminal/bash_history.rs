//! `terminal.bash-history` — bash history append-only file adapter.
//!
//! Folds the bash history source unit from `sinex-terminal-ingestor` into the
//! source-unit registries.
//!
//! Adapter: [`AppendOnlyFileAdapter`] — tails `~/.bash_history` line by line.
//! Parser:  [`BashHistoryParser`] — one `command.imported` event per line.
//!          Layers [`ContentHashWindow`] for dedup across rotation boundaries.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::node_sdk::parser::dedup::ContentHashWindow;
use crate::node_sdk::parser::{AppendOnlyFileAdapter, MaterialParser, ParserError, ParserResult};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::shell::HistoryCommandImportedPayload;
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceUnitId,
    TimingEvidence,
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
        id: "terminal.bash-history",
        namespace: "terminal",
        event_types: &[("shell.history", "command.imported")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "obligation:source_unit.material_provenance",
            "obligation:source_unit.package_impact_rationale",
        ],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "target_home_read:.bash_history",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:terminal.bash-history"),
        "terminal.bash-history",
        "terminal",
    )
    .implementation("sinexd")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("command.imported")
    .privacy_context("Command")
    .material_policy("text_history_anchor")
    .checkpoint_policy("append_stream")
    .resource_shape("linear_rows_bounded_memory")
    .source_unit_id("terminal.bash-history")
    .runner_pack("sinexd-source-unit")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("bash_history_source_unit")
    .implementation_mode("sinexd:source-unit")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BashHistoryParserConfig;

/// Parser for bash plain-text history files.
///
/// Each line is a raw command string. Maintains a [`ContentHashWindow`] to
/// suppress re-emission of lines that appear after a file rotation.
#[derive(Debug, Default)]
pub struct BashHistoryParser {
    dedup: ContentHashWindow,
}

#[async_trait]
impl MaterialParser for BashHistoryParser {
    type Config = BashHistoryParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("bash-history"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::AppendOnlyFile],
            source_unit_id: SourceUnitId::from_static("terminal.bash-history"),
            declared_event_types: vec![(
                EventSource::from_static("shell.history"),
                EventType::from_static("command.imported"),
            )],
            privacy_contexts: vec![ProcessingContext::Command],
            sensitivity_hints: Vec::new(),
            proof_obligations: vec![
                "obligation:source_unit.material_provenance".into(),
                "obligation:source_unit.package_impact_rationale".into(),
            ],
            description: "Parses bash plain-text history lines into command.imported events."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: sinex_primitives::parser::SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        // On rotation, clear the dedup window so the new file starts fresh.
        if record
            .metadata
            .get("rotation_detected")
            .and_then(sinex_primitives::JsonValue::as_bool)
            .unwrap_or(false)
        {
            self.dedup.clear();
        }

        let line = std::str::from_utf8(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("invalid UTF-8 in bash history: {e}")))?
            .trim();

        if line.is_empty() {
            return Ok(vec![]);
        }

        // Dedup: skip if seen within the trailing window.
        if self.dedup.contains(line.as_bytes()) {
            return Ok(vec![]);
        }
        self.dedup.observe(line.as_bytes());

        let line_number = match &record.anchor {
            sinex_primitives::parser::MaterialAnchor::Line { line, .. } => Some(*line as u32),
            _ => None,
        };

        let source_file = record
            .logical_path
            .as_ref()
            .map(std::string::ToString::to_string)
            .unwrap_or_default();

        let payload = HistoryCommandImportedPayload {
            command: line.to_owned(),
            timestamp: None,
            shell_type: "bash".into(),
            source_file,
            line_number,
        };

        let payload_json = serde_json::to_value(&payload)
            .map_err(|e| ParserError::Parse(format!("payload serialization failed: {e}")))?;

        Ok(vec![
            ParsedEventIntent::builder()
                .source_unit_id(ctx.source_unit_id.clone())
                .parser_id(ParserId::from_static("bash-history"))
                .parser_version("1.0.0")
                .event_type(EventType::from_static("command.imported"))
                .event_source(EventSource::from_static("shell.history"))
                .payload(payload_json)
                .ts_orig(sinex_primitives::temporal::Timestamp::now())
                .timing(TimingEvidence::StagedAtFallback)
                .anchor(record.anchor)
                .privacy_context(ProcessingContext::Command)
                .build(),
        ])
    }
}

// ---------------------------------------------------------------------------
// Node factory registration
// ---------------------------------------------------------------------------

register_adapter_ingestor!(
    source_unit_id: "terminal.bash-history",
    adapter: AppendOnlyFileAdapter,
    parser: BashHistoryParser,
);
