//! `desktop.clipboard` source.
//!
//! Polls the system clipboard at a configurable interval and emits an event
//! for each content change (detected by BLAKE3 hash comparison).
//!
//! Adapter: `ClipboardPollingAdapter`
//! Anchor: `StreamFrame` (monotonic change counter; no durable cursor)
//! Privacy tier: `Secret` — content is emitted with `ProcessingContext::Clipboard`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceId,
    TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::temporal::Timestamp;

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};

// ---------------------------------------------------------------------------
// Parser config
// ---------------------------------------------------------------------------

/// Configuration for [`ClipboardParser`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardParserConfig {
    /// Maximum preview length to include in the payload (characters).
    #[serde(default = "default_max_preview")]
    pub max_preview_length: usize,
}

fn default_max_preview() -> usize {
    100
}

impl Default for ClipboardParserConfig {
    fn default() -> Self {
        Self {
            max_preview_length: default_max_preview(),
        }
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parses clipboard change records from a `ClipboardPollingAdapter`.
///
/// Each record contains the raw clipboard text bytes. The parser:
/// 1. Decodes clipboard text bytes.
/// 2. Builds a preview (first `max_preview_length` chars of raw content).
/// 3. Emits a `clipboard.copied` intent (primary clipboard) with clipboard
///    privacy context metadata.
///
/// Selection (primary clipboard) vs copy (clipboard) distinction: the
/// `ClipboardPollingAdapter` currently provides a single stream; the parser
/// emits `clipboard.copied` for all changes.  A future adapter extension that
/// exposes `LinuxClipboardKind` in record metadata can split these.
#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "desktop.clipboard",
    namespace = "desktop",
    event_source = "clipboard",
    event_type = "clipboard.copied",
    event_types = "clipboard.selected",
    adapter = "ClipboardPollingAdapter",
    privacy_tier = PrivacyTier::Secret,
    horizons(Horizon::Continuous),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Anchor,
    access_scope = AccessScope::RuntimeBridge { surface: "clipboard" },
    implementation = "sinexd",
    privacy_context = ProcessingContext::Clipboard,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::LiveObservation,
    runtime_shape = RuntimeShape::Continuous,
)]
pub struct ClipboardParser;

#[async_trait]
impl MaterialParser for ClipboardParser {
    type Config = ClipboardParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("clipboard-poller"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::Polling],
            source_id: SourceId::from_static("desktop.clipboard"),
            declared_event_types: vec![
                (
                    EventSource::from_static("clipboard"),
                    EventType::from_static("clipboard.copied"),
                ),
                (
                    EventSource::from_static("clipboard"),
                    EventType::from_static("clipboard.selected"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Clipboard],
            sensitivity_hints: Vec::new(),
            description: "Parses clipboard polling records into clipboard change events.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: sinex_primitives::parser::SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        if record.bytes.is_empty() {
            return Ok(vec![]);
        }

        let raw_text = std::str::from_utf8(&record.bytes).map_err(|e| {
            ParserError::Parse(format!("clipboard content is not valid UTF-8: {e}"))
        })?;

        // Preview: first N characters of the raw text. Admission policy owns
        // redaction/suppression for the stored payload.
        let preview: String = raw_text.chars().take(100).collect();

        let ts_now = Timestamp::now();

        let payload = serde_json::json!({
            "content_preview": preview,
            "content_size_bytes": record.bytes.len(),
        });

        let intent = ParsedEventIntent::builder()
            .source_id(ctx.source_id.clone())
            .parser_id(ParserId::from_static("clipboard-poller"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static("clipboard.copied"))
            .event_source(EventSource::from_static("clipboard"))
            .payload(payload)
            .ts_orig(ts_now)
            .timing(TimingEvidence::StagedAtFallback)
            .anchor(record.anchor.clone())
            .privacy_context(ProcessingContext::Clipboard)
            .build();

        Ok(vec![intent])
    }
}
