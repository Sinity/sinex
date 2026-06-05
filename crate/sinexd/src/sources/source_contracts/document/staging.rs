//! `document.staging` source — document parser dispatch plus node-factory
//! registration for the imperative document runtime.
//!
//! The source scans configured root directories for documents, fingerprints
//! them for skip-unchanged logic, stages their bytes via `AcquisitionManager`,
//! and emits one `document.ingested` material event plus zero or more
//! `tag.applied` derived events (auto-tagged from MIME type) per file.
//!
//! # Why imperative `MaterialParser`, not `register_source!`
//!
//! `document.staging` emits both material-provenance (`document.ingested`) and
//! derived-provenance (`tag.applied`) events from a single parse. The
//! `register_source!` macro assumes one event type per source and
//! cannot express this multi-event emit pattern. An imperative `MaterialParser`
//! + `register_source!` is required.
//!
//! The `DocumentStagingParser` handles the dispatch-path (replay, testing).
//! The full ingestion path uses `DocumentNode` directly via
//! `register_source!`.

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use crate::runtime::tags;
use async_trait::async_trait;
use mime_guess::MimeGuess;
use serde::{Deserialize, Serialize};
use sinex_primitives::{
    domain::{EventSource, EventType},
    events::{
        EventPayload,
        payloads::{KnowledgeTagAppliedPayload, document::DocumentIngestedPayload},
    },
    parser::{
        InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceRecord,
        SourceId, TimingEvidence,
    },
    privacy::{ProcessingContext, SensitivityHint},
    proof::{
        CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
        SourceRuntimeBinding, SourceBuildImpact, SourceContract, SubjectRef,
    },
    temporal::Timestamp,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

// ---------------------------------------------------------------------------
// Source contract — "document.staging"
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "document.staging",
        namespace: "document",
        event_types: &[
            ("document-source", "document.ingested"),
        ],
        privacy_tier: PrivacyTier::Secret,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "configured_document_roots",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:document.staging"),
        "document.staging",
        "document",
    )
    .implementation("sinexd")
    .adapter("DocumentStagingParser")
    .output_event_type("document.ingested")
    .privacy_context("document_body")
    .material_policy("document_anchor")
    .checkpoint_policy("fingerprint_dedup")
    .resource_shape("on_demand_batch")
    .source_id("document.staging")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("no_new_output")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// DocumentStagingParser — imperative MaterialParser
//
// In the dispatch path (testing / replay), the parser receives a SourceRecord
// whose `bytes` are the UTF-8 encoded file path (the same bytes the
// FileDropAdapter or a directory-walk driver would emit for a discovered file).
// It re-reads MIME type from the path extension, runs path privacy redaction,
// and constructs the `document.ingested` intent plus any `tag.applied`
// derived children.
// ---------------------------------------------------------------------------

/// No per-parse config needed; binding config lives on the `DocumentNode`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DocumentStagingParserConfig {}

/// Imperative parser for the `document.staging` source.
///
/// Used in the dispatch path (replay, testing). The full ingestion path uses
/// `DocumentNode` registered via `register_source!`.
#[derive(Debug, Default)]
pub struct DocumentStagingParser;

#[async_trait]
impl MaterialParser for DocumentStagingParser {
    type Config = DocumentStagingParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("document-staging"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::FileDrop],
            source_id: SourceId::from_static("document.staging"),
            declared_event_types: vec![
                (
                    EventSource::from_static("document-source"),
                    EventType::from_static("document.ingested"),
                ),
                (
                    EventSource::from_static("knowledge"),
                    EventType::from_static("tag.applied"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Metadata],
            // Document titles/bodies are free-form text and the source path may
            // leak home structure; exported for policy tooling, never auto-acted (#1611).
            sensitivity_hints: vec![
                SensitivityHint::FreeText,
                SensitivityHint::PotentiallySensitive,
                SensitivityHint::SourcePath,
            ],
            description:
                "Stages document files and emits document.ingested + auto-tag derived events".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        _ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let path = match std::str::from_utf8(&record.bytes) {
            Ok(p) => p.trim().to_string(),
            Err(_) => {
                return Ok(vec![]);
            }
        };

        if path.is_empty() {
            return Ok(vec![]);
        }

        let mime = MimeGuess::from_path(&path)
            .first_raw()
            .unwrap_or("application/octet-stream")
            .to_string();

        let file_size = std::fs::metadata(&path).map_or(0, |m| m.len());

        let source_material_id = record.material_id.to_uuid().to_string();

        let payload = DocumentIngestedPayload {
            file_path: path,
            source_material_id,
            size_bytes: file_size,
            mime_type: Some(mime.clone()),
            encoding: None,
        };

        let material_intent = ParsedEventIntent::builder()
            .source_id(SourceId::from_static("document.staging"))
            .parser_id(ParserId::from_static("document-staging"))
            .parser_version("1.0.0")
            .event_type(payload.event_type())
            .event_source(payload.event_source())
            .payload(serde_json::to_value(&payload).map_err(|e| {
                ParserError::Parse(format!("failed to serialize DocumentIngestedPayload: {e}"))
            })?)
            .ts_orig(Timestamp::now())
            .timing(TimingEvidence::StagedAtFallback)
            .anchor(record.anchor.clone())
            .privacy_context(ProcessingContext::Metadata)
            .build();

        let mut intents = vec![material_intent.clone()];

        for tag_name in tags::auto_tags_for_mime(&mime) {
            let tag_payload = KnowledgeTagAppliedPayload {
                entity_id: record.material_id.to_uuid(),
                tag_name: tag_name.clone(),
                tag_source: "auto.mime".into(),
            };
            if let Ok(derived) = material_intent.derive_from_parents(&tag_payload) {
                intents.push(derived);
            }
        }

        Ok(intents)
    }
}

// ---------------------------------------------------------------------------
// Registrations
// ---------------------------------------------------------------------------

crate::register_source!(source_id: "document.staging", parser: DocumentStagingParser);

// The full source runtime lifecycle uses DocumentNode; see `super::runtime`.
// It is a `SourceDriver` implementation that manages its own checkpoint state
// (`manages_own_checkpoints: true`) and supports snapshot + historical scans
// but not continuous mode.
crate::register_source!(source_id: "document.staging", driver: super::runtime::DocumentNode);
