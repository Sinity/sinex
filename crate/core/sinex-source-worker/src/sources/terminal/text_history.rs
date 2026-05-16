//! `terminal.text-history` — generic plain-text history append-only file.
//!
//! Catch-all for unknown shell history files. One command per line, no
//! timestamp. Layers [`ContentHashWindow`] for dedup across rotations.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use sinex_node_sdk::parser::dedup::ContentHashWindow;
use sinex_node_sdk::parser::{AppendOnlyFileAdapter, MaterialParser, ParserError, ParserResult};
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
        id: "terminal.text-history",
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
        access_policy: "target_home_read:shell_history_text",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:terminal.text-history"),
        "terminal.text-history",
        "terminal",
    )
    .implementation("sinex-source-worker")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("command.imported")
    .privacy_context("Command")
    .material_policy("text_history_anchor")
    .checkpoint_policy("append_stream")
    .resource_shape("linear_rows_bounded_memory")
    .source_unit_id("terminal.text-history")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("text_history_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TextHistoryParserConfig;

/// Parser for generic plain-text history files.
#[derive(Debug, Default)]
pub struct TextHistoryParser {
    dedup: ContentHashWindow,
}

#[async_trait]
impl MaterialParser for TextHistoryParser {
    type Config = TextHistoryParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("text-history"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::AppendOnlyFile],
            source_unit_id: SourceUnitId::from_static("terminal.text-history"),
            declared_event_types: vec![(
                EventSource::from_static("shell.history"),
                EventType::from_static("command.imported"),
            )],
            privacy_contexts: vec![ProcessingContext::Command],
            proof_obligations: vec![
                "obligation:source_unit.material_provenance".into(),
                "obligation:source_unit.package_impact_rationale".into(),
            ],
            description: "Parses generic plain-text history files into command.imported events."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: sinex_primitives::parser::SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        // On rotation, clear the dedup window.
        if record
            .metadata
            .get("rotation_detected")
            .and_then(sinex_primitives::JsonValue::as_bool)
            .unwrap_or(false)
        {
            self.dedup.clear();
        }

        let line = std::str::from_utf8(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("invalid UTF-8 in text history: {e}")))?
            .trim();

        if line.is_empty() {
            return Ok(vec![]);
        }

        if self.dedup.contains(line.as_bytes()) {
            return Ok(vec![]);
        }
        self.dedup.observe(line.as_bytes());

        let processed = {
            let result = sinex_primitives::privacy::engine()
                .map_err(|e| ParserError::Parse(format!("privacy engine unavailable: {e}")))?
                .process(line, ProcessingContext::Command);
            if result.suppressed {
                return Ok(vec![]);
            }
            result.text.into_owned()
        };

        let line_number = match &record.anchor {
            sinex_primitives::parser::MaterialAnchor::Line { line: ln, .. } => Some(*ln as u32),
            _ => None,
        };

        let source_file = record
            .logical_path
            .as_ref()
            .map(std::string::ToString::to_string)
            .unwrap_or_default();

        let payload = HistoryCommandImportedPayload {
            command: processed,
            timestamp: None,
            shell_type: "unknown".into(),
            source_file,
            line_number,
        };

        let payload_json = serde_json::to_value(&payload)
            .map_err(|e| ParserError::Parse(format!("payload serialization failed: {e}")))?;

        Ok(vec![ParsedEventIntent {
            id: sinex_primitives::ids::Id::new(),
            source_unit_id: ctx.source_unit_id.clone(),
            parser_id: ParserId::from_static("text-history"),
            parser_version: "1.0.0".into(),
            event_type: EventType::from_static("command.imported"),
            event_source: EventSource::from_static("shell.history"),
            payload: payload_json,
            ts_orig: sinex_primitives::temporal::Timestamp::now(),
            timing: TimingEvidence::StagedAtFallback,
            anchor: record.anchor,
            occurrence_key: None,
            privacy_context: ProcessingContext::Command,
            field_privacy_log: None,
            synthesis_parents: None,
        }])
    }
}

// ---------------------------------------------------------------------------
// Node factory registration
// ---------------------------------------------------------------------------

register_adapter_ingestor!(
    source_unit_id: "terminal.text-history",
    adapter: AppendOnlyFileAdapter,
    parser: TextHistoryParser,
);
