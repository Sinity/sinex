//! `document.staging` source unit — folds the legacy `sinex-document-ingestor`
//! into the source-worker dispatch and node-factory registries.
//!
//! The ingestor scans configured root directories for documents, fingerprints
//! them for skip-unchanged logic, stages their bytes via `AcquisitionManager`,
//! and emits one `document.ingested` material event plus zero or more
//! `tag.applied` derived events (auto-tagged from MIME type) per file.
//!
//! # Why imperative `MaterialParser`, not `register_adapter_ingestor!`
//!
//! `document.staging` emits both material-provenance (`document.ingested`) and
//! derived-provenance (`tag.applied`) events from a single parse. The
//! `register_adapter_ingestor!` macro assumes one event type per source unit and
//! cannot express this multi-event emit pattern. An imperative `MaterialParser`
//! + `register_node_factory!` is required.
//!
//! The `DocumentStagingParser` handles the dispatch-path (replay, testing).
//! The full ingestion path uses `DocumentNode` from `sinex-document-ingestor`
//! directly via `register_node_factory!`.

use crate::node_sdk::parser::{MaterialParser, ParserError, ParserResult};
use crate::node_sdk::tags;
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
        SourceUnitId, TimingEvidence,
    },
    privacy::{self, ProcessingContext},
    proof::{
        CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
        SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
    },
    temporal::Timestamp,
};
use sinex_primitives::{register_source_unit, register_source_unit_binding};

// ---------------------------------------------------------------------------
// Source unit descriptor — "document.staging"
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "document.staging",
        namespace: "document",
        event_types: &[
            ("document-ingestor", "document.ingested"),
        ],
        privacy_tier: PrivacyTier::Secret,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "configured_document_roots",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:document.staging"),
        "document.staging",
        "document",
    )
    .implementation("sinex-source-worker")
    .adapter("DocumentStagingParser")
    .output_event_type("document.ingested")
    .privacy_context("document_body")
    .material_policy("document_anchor")
    .checkpoint_policy("fingerprint_dedup")
    .resource_shape("on_demand_batch")
    .source_unit_id("document.staging")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
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

/// Imperative parser for the `document.staging` source unit.
///
/// Used in the dispatch path (replay, testing). The full ingestion path uses
/// `DocumentNode` (the legacy `SourceUnit` from `sinex-document-ingestor`)
/// registered via `register_node_factory!`.
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
            source_unit_id: SourceUnitId::from_static("document.staging"),
            declared_event_types: vec![
                (
                    EventSource::from_static("document-ingestor"),
                    EventType::from_static("document.ingested"),
                ),
                (
                    EventSource::from_static("knowledge"),
                    EventType::from_static("tag.applied"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Metadata],
            proof_obligations: vec![],
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

        let redacted_path = privacy::process(&path, ProcessingContext::Metadata)
            .map_or_else(|_| path.clone(), |r| r.text.into_owned());

        let source_material_id = record.material_id.to_uuid().to_string();

        let payload = DocumentIngestedPayload {
            file_path: redacted_path,
            source_material_id,
            size_bytes: file_size,
            mime_type: Some(mime.clone()),
            encoding: None,
        };

        let material_intent = ParsedEventIntent::builder()
            .source_unit_id(SourceUnitId::from_static("document.staging"))
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
            if let Ok(derived) = material_intent.derive_from_parents(tag_payload) {
                intents.push(derived);
            }
        }

        Ok(intents)
    }
}

// ---------------------------------------------------------------------------
// Registrations
// ---------------------------------------------------------------------------

crate::register_parser!("document.staging", DocumentStagingParser);

// The full node lifecycle uses DocumentNode (moved verbatim from the legacy
// `sinex-document-ingestor` crate during the Wave-B fold; see `super::node`).
// It is an `SourceUnit` implementation that manages its own checkpoint state
// (`manages_own_checkpoints: true`) and supports snapshot + historical scans
// but not continuous mode.
crate::register_node_factory!("document.staging", super::node::DocumentNode);
