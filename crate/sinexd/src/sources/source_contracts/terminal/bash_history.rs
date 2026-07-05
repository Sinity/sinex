//! `terminal.bash-history` — bash history append-only file adapter.
//!
//! Folds the bash history source from `sinex-terminal-source` into the
//! source registries.
//!
//! Adapter: [`AppendOnlyFileAdapter`] — tails `~/.bash_history` line by line.
//! Parser:  [`BashHistoryParser`] — one `command.imported` event per line.
//!          Layers [`ContentHashWindow`] for dedup across rotation boundaries.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_macros::SourceMeta;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RuntimeShape,
};

use crate::runtime::parser::dedup::{ContentHashWindow, ContentHashWindowSnapshot};
use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::shell::HistoryCommandImportedPayload;
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceId,
    TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BashHistoryParserConfig;

/// Parser for bash plain-text history files.
///
/// Each line is a raw command string. Maintains a [`ContentHashWindow`] to
/// suppress re-emission of lines that appear after a file rotation.
///
/// `#[derive(SourceMeta)]` collapses the `SourceContract`,
/// `SourceRuntimeBinding`, and `register_source!` factory wiring (#1727 slice
/// 3); the hand-written `MaterialParser` below is kept verbatim because the
/// stateful rotation-aware dedup is beyond the declarative DSL.
#[derive(Debug, Default, SourceMeta)]
#[source_meta(
    id = "terminal.bash-history",
    namespace = "terminal",
    event_source = "shell.history",
    event_type = "command.imported",
    adapter = "AppendOnlyFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Continuous, Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Anchor,
    access_scope = AccessScope::TargetHome { path: ".bash_history" },
    privacy_context = ProcessingContext::Command,
    resource_profile = ResourceProfile::BoundedStream,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::Continuous,
)]
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
            source_id: SourceId::from_static("terminal.bash-history"),
            declared_event_types: vec![(
                EventSource::from_static("shell.history"),
                EventType::from_static("command.imported"),
            )],
            privacy_contexts: vec![ProcessingContext::Command],
            sensitivity_hints: Vec::new(),
            description: "Parses bash plain-text history lines into command.imported events."
                .into(),
        }
    }

    fn restore_checkpoint_state(
        &mut self,
        state: Option<&sinex_primitives::JsonValue>,
    ) -> ParserResult<()> {
        let Some(state) = state else {
            return Ok(());
        };
        let snapshot: ContentHashWindowSnapshot =
            serde_json::from_value(state.clone()).map_err(|error| {
                ParserError::Parse(format!("invalid bash history dedup checkpoint: {error}"))
            })?;
        self.dedup = ContentHashWindow::from_snapshot(snapshot).map_err(|error| {
            ParserError::Parse(format!("invalid bash history dedup hash: {error}"))
        })?;
        Ok(())
    }

    fn checkpoint_state(&self) -> ParserResult<Option<sinex_primitives::JsonValue>> {
        serde_json::to_value(self.dedup.snapshot())
            .map(Some)
            .map_err(|error| {
                ParserError::Parse(format!("bash history dedup checkpoint failed: {error}"))
            })
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
                .source_id(ctx.source_id.clone())
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

#[cfg(test)]
#[path = "bash_history_test.rs"]
mod tests;
