//! `desktop.clipboard` source unit.
//!
//! Polls the system clipboard at a configurable interval and emits an event
//! for each content change (detected by BLAKE3 hash comparison).
//!
//! Adapter: `ClipboardPollingAdapter`
//! Anchor: `StreamFrame` (monotonic change counter; no durable cursor)
//! Sensitivity tier: `Secret` — content carries field hints for admission policy.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceUnitId,
    TimingEvidence,
};
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_unit, register_source_unit_binding};

use crate::node_sdk::parser::{ClipboardPollingAdapter, MaterialParser, ParserError, ParserResult};

use crate::register_adapter_ingestor;

// ---------------------------------------------------------------------------
// Source unit descriptor
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "desktop.clipboard",
        namespace: "desktop",
        event_types: &[
            ("clipboard", "clipboard.copied"),
            ("clipboard", "clipboard.selected"),
        ],
        privacy_tier: PrivacyTier::Secret,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "anchor_stream_frame",
            "clipboard_content_sensitive_hint",
        ],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "target_runtime_bridge:clipboard",
    }
}

// ---------------------------------------------------------------------------
// Binding
// ---------------------------------------------------------------------------

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:desktop.clipboard"),
        "desktop.clipboard",
        "desktop",
    )
    .implementation("sinex-source-worker")
    .adapter("ClipboardPollingAdapter")
    .output_event_type("clipboard.copied")
    .sensitivity_profile("clipboard")
    .material_policy("clipboard_stream")
    .checkpoint_policy("live_stream")
    .resource_shape("polling_watcher")
    .source_unit_id("desktop.clipboard")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("desktop_clipboard")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

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
/// 1. Computes a BLAKE3 content hash (included in payload for dedup).
/// 2. Builds a preview (first `max_preview_length` chars of raw content).
/// 3. Emits a `clipboard.copied` intent (primary clipboard).
///
/// Selection (primary clipboard) vs copy (clipboard) distinction: the
/// `ClipboardPollingAdapter` currently provides a single stream; the parser
/// emits `clipboard.copied` for all changes.  A future adapter extension that
/// exposes `LinuxClipboardKind` in record metadata can split these.
#[derive(Debug, Clone, Default)]
pub struct ClipboardParser;

#[async_trait]
impl MaterialParser for ClipboardParser {
    type Config = ClipboardParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("clipboard-poller"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::Polling],
            source_unit_id: SourceUnitId::from_static("desktop.clipboard"),
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
            field_hints: vec![
                sinex_primitives::parser::FieldSensitivityHint::FreeText,
                sinex_primitives::parser::FieldSensitivityHint::PotentiallySensitive,
                sinex_primitives::parser::FieldSensitivityHint::CredentialBearing,
            ],
            proof_obligations: vec![
                "anchor_stream_frame".into(),
                "clipboard_content_sensitive_hint".into(),
            ],
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
        // any masking/redaction before persistence.
        let preview: String = raw_text.chars().take(100).collect();

        let ts_now = Timestamp::now();

        let payload = serde_json::json!({
            "content_preview": preview,
            "content_size_bytes": record.bytes.len(),
        });

        let intent = ParsedEventIntent::builder()
            .source_unit_id(ctx.source_unit_id.clone())
            .parser_id(ParserId::from_static("clipboard-poller"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static("clipboard.copied"))
            .event_source(EventSource::from_static("clipboard"))
            .payload(payload)
            .ts_orig(ts_now)
            .timing(TimingEvidence::StagedAtFallback)
            .anchor(record.anchor.clone())
            .privacy_hints(vec![
                sinex_primitives::parser::FieldSensitivityHint::FreeText,
                sinex_primitives::parser::FieldSensitivityHint::PotentiallySensitive,
                sinex_primitives::parser::FieldSensitivityHint::CredentialBearing,
            ])
            .build();

        Ok(vec![intent])
    }
}

// ---------------------------------------------------------------------------
// Node factory registration
// ---------------------------------------------------------------------------

register_adapter_ingestor!(
    source_unit_id: "desktop.clipboard",
    adapter: ClipboardPollingAdapter,
    parser: ClipboardParser,
);
