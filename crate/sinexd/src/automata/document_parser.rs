//! Document parser automaton — derived-provenance v1 document layer.
//!
//! Implements [`MultiOutputTransducer`]: one input event produces
//! `document.parsed` + N× `document.chunked` events.
//!
//! ## v1 corpora
//!
//! | Corpus | Input event type | Chunking |
//! |--------|-----------------|----------|
//! | `DendronMarkdown` | `document.ingested` | Paragraph (`\n\n+`) |
//! | `TerminalOutput` | `command.canonical` | Line-group (blank-line split) |
//!
//! ## Chunking
//!
//! Paragraph split on `\n\n+`, dropping empty paragraphs. Frontmatter is
//! stripped before chunking for Dendron (content between leading `---`
//! delimiters). Wikilinks are extracted via `[[...]]` patterns.
//!
//! ## Privacy
//!
//! Chunk text is emitted as parsed text. DB/user privacy policy applies at the
//! event-engine chokepoint using the emitted event metadata and payload hints.
//!
//! Ref: `crate/sinex-schema/docs/document_layer.md`.

use crate::runtime::automaton::{
    AutomatonContext, DerivedOutput, InputProvenanceFilter, MultiOutputTransducer,
};
use crate::runtime::processing::AutomatonLogicError;
use sinex_primitives::JsonValue;
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    CanonicalCommandPayload, DocumentIngestedPayload, DocumentKind,
};
use sinex_primitives::ids::derive_document_id;
use std::collections::HashMap;

// ── Constants ──────────────────────────────────────────────────────────

/// Maximum document size in bytes (4 MiB).
const MAX_DOCUMENT_BYTES: u64 = 4 * 1024 * 1024;

/// Maximum chunk size in bytes (64 KiB).
const MAX_CHUNK_BYTES: usize = 64 * 1024;

// ── State ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DocumentParserState {
    /// Documents processed since last checkpoint.
    pub processed_count: u64,
    /// Chunks emitted since last checkpoint.
    pub chunk_count: u64,
}

// ── RuntimeModule ───────────────────────────────────────────────────────────────

/// Derivation control-plane declarations for `document-parser` (sinex-0vx.1/0vx.3).
///
/// One declaration per event type in [`MultiOutputTransducer::output_event_types`]
/// — `document.parsed` and `document.chunked` are genuinely distinct output
/// shapes from a single processing call, unlike `interval-lift`'s
/// single-type-many-instances use of the same trait.
pub const DOCUMENT_PARSER_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] = &[
    DerivationOutputDeclaration {
        declaration_id: "document-parser.document.parsed",
        owner: "document-parser",
        product_class: DerivedProductClass::CanonicalDerivedEvent,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("document-parser"),
        output_event_type: Some("document.parsed"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::DefaultCanonicalInput,
        default_support: ClaimSupportTemplate::new(
            SupportLevel::Direct,
            SourceCoverage::Covered,
            ClaimTemporalQuality::InheritParent,
        ),
        verification_command: "xtask test -p sinexd -E 'test(document_parser)'",
    },
    DerivationOutputDeclaration {
        declaration_id: "document-parser.document.chunked",
        owner: "document-parser",
        product_class: DerivedProductClass::CanonicalDerivedEvent,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("document-parser"),
        output_event_type: Some("document.chunked"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::DefaultCanonicalInput,
        default_support: ClaimSupportTemplate::new(
            SupportLevel::Direct,
            SourceCoverage::Covered,
            ClaimTemporalQuality::InheritParent,
        ),
        verification_command: "xtask test -p sinexd -E 'test(document_parser)'",
    },
];

#[derive(Debug, Clone, Default)]
pub struct DocumentParserAutomaton {
    /// Optional Dendron vault root for path-based operations.
    pub vault_root: Option<String>,
}

impl MultiOutputTransducer for DocumentParserAutomaton {
    type State = DocumentParserState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "document-parser"
    }

    fn input_event_type(&self) -> &'static str {
        "*"
    }

    fn input_event_types(&self) -> Vec<&'static str> {
        vec![
            DocumentIngestedPayload::EVENT_TYPE.as_static_str(),
            CanonicalCommandPayload::EVENT_TYPE.as_static_str(),
        ]
    }

    fn output_event_types(&self) -> &[&'static str] {
        &["document.parsed", "document.chunked"]
    }

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] =
        DOCUMENT_PARSER_OUTPUT_DECLARATIONS;

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
    }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: JsonValue,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        let event_type = context.event_type.as_str();

        match event_type {
            "document.ingested" => self.process_dendron(state, input, context),
            "command.canonical" => self.process_terminal(state, input, context),
            _ => Ok(Vec::new()),
        }
    }
}

// ── Processing ──────────────────────────────────────────────────────────

impl DocumentParserAutomaton {
    /// Process a `document.ingested` event into parsed + chunked output.
    fn process_dendron(
        &self,
        _state: &mut DocumentParserState,
        input: JsonValue,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        let file_path = input["file_path"].as_str().unwrap_or("unknown").to_string();

        // Read file content. In production the parser runs as the sinex service
        // user and may not have access to the original file path. The long-term
        // fix is to retrieve content via `source_material_id` through the content
        // store (BLAKE3 CAS), which is world-readable. Tracked as a follow-up to
        // the document parser reliability hardening.
        //
        // For now, fall back gracefully: if the file is unreadable, skip it and
        // log at warn level so the operator can diagnose the gap.
        let content = match std::fs::read_to_string(&file_path) {
            Ok(c) => c,
            Err(e) => {
                let material_id = input["source_material_id"].as_str().unwrap_or("unknown");
                tracing::warn!(
                    file_path = %file_path,
                    source_material_id = %material_id,
                    error = %e,
                    "Document parser could not read source file — content store retrieval \
                     not yet wired (see document parser reliability follow-up)"
                );
                return Ok(Vec::new());
            }
        };

        if content.len() as u64 > MAX_DOCUMENT_BYTES {
            tracing::warn!(
                file_path = %file_path,
                size = content.len(),
                max = MAX_DOCUMENT_BYTES,
                "Document exceeds size cap, skipping"
            );
            return Ok(Vec::new());
        }

        let natural_key = file_path.clone();
        let document_id = derive_document_id("dendron", &natural_key);

        // Extract frontmatter
        let (frontmatter, body) = extract_frontmatter(&content);
        let title = frontmatter.get("title").cloned();
        let wikilinks = extract_wikilinks(&body);

        // Chunk the body after frontmatter removal. Privacy policy is not
        // applied here; the event engine owns admission/redaction decisions.
        let raw_chunks: Vec<String> = paragraph_split(&body);
        let chunk_count = raw_chunks.len() as u32;
        let total_bytes: u64 = raw_chunks.iter().map(|c| c.len() as u64).sum();

        // Build side_data
        let mut side_data = serde_json::Map::new();
        side_data.insert(
            "frontmatter".into(),
            serde_json::to_value(&frontmatter).unwrap_or_default(),
        );
        side_data.insert(
            "wikilinks".into(),
            serde_json::to_value(&wikilinks).unwrap_or_default(),
        );
        if let Some(t) = &title {
            side_data.insert("title".into(), JsonValue::String(t.clone()));
        }

        let parent_event_id = context.trigger_uuid();
        let ts_orig = context
            .ts_orig
            .unwrap_or_else(sinex_primitives::Timestamp::now);
        let mut outputs = Vec::with_capacity(1 + raw_chunks.len());

        // Emit document.parsed
        let parsed_payload = serde_json::to_value(serde_json::json!({
            "document_id": document_id,
            "kind": DocumentKind::DendronMarkdown.as_str(),
            "natural_key": natural_key,
            "extraction_version": 1,
            "chunk_count": chunk_count,
            "text_byte_len": total_bytes,
            "side_data": side_data,
        }))
        .map_err(|e| AutomatonLogicError::Processing(format!("serialize document.parsed: {e}")))?;

        let parsed_output = DerivedOutput::transduced(parsed_payload, ts_orig, parent_event_id)
            .with_event_type("document.parsed");

        outputs.push(parsed_output);

        // The parsed event ID is the UUIDv7 generated by the adapter at emit time.
        // For v1, chunk provenance references the parent document.parsed event via
        // the adapter's event_id — we use a placeholder that gets replaced during
        // emission. In practice, the projection writer knows the parsed event ID
        // because it processes the batch sequentially.
        //
        // For now, emit chunks with the same parent (document.ingested). The
        // projection writer normalizes the parent chain. This is correct per the
        // design doc's "inline chunks" Option 2 approach.

        // Emit document.chunked for each chunk
        let mut byte_offset: u64 = 0;
        for (i, chunk_text) in raw_chunks.into_iter().enumerate() {
            let chunk_len = chunk_text.len() as u64;

            let chunk_payload = serde_json::to_value(serde_json::json!({
                "document_id": document_id,
                "chunk_index": i as u32,
                "text": chunk_text,
                "byte_offset_start": byte_offset,
                "byte_offset_end": byte_offset + chunk_len,
                "source_anchor_start": byte_offset,
                "source_anchor_end": byte_offset + chunk_len,
            }))
            .map_err(|e| {
                AutomatonLogicError::Processing(format!("serialize document.chunked: {e}"))
            })?;

            let chunk_output = DerivedOutput::transduced(chunk_payload, ts_orig, parent_event_id)
                .with_event_type("document.chunked");

            outputs.push(chunk_output);
            byte_offset += chunk_len;
        }

        Ok(outputs)
    }

    /// Process a `command.canonical` event into a terminal-output document.
    fn process_terminal(
        &self,
        _state: &mut DocumentParserState,
        input: JsonValue,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<JsonValue>>, AutomatonLogicError> {
        let parent_event_id = context.trigger_uuid();
        let parent_id_str = parent_event_id.to_string();
        let natural_key = parent_id_str.clone();

        // Extract command output from the canonicalized event.
        let stdout = input["output"].as_str().unwrap_or("");
        let command = input["command"].as_str().unwrap_or("");

        if stdout.is_empty() {
            return Ok(Vec::new());
        }

        if stdout.len() as u64 > MAX_DOCUMENT_BYTES {
            tracing::warn!(
                parent_id = %parent_id_str,
                size = stdout.len(),
                "Terminal output exceeds size cap, skipping"
            );
            return Ok(Vec::new());
        }

        let document_id = derive_document_id("terminal", &natural_key);
        let raw_chunks: Vec<String> = line_group_split(stdout);
        let chunk_count = raw_chunks.len() as u32;
        let total_bytes: u64 = raw_chunks.iter().map(|c| c.len() as u64).sum();
        let ts_orig = context
            .ts_orig
            .unwrap_or_else(sinex_primitives::Timestamp::now);

        let mut side_data = serde_json::Map::new();
        side_data.insert("command".into(), JsonValue::String(command.to_string()));
        side_data.insert("shell".into(), JsonValue::String("zsh".into()));

        let mut outputs = Vec::with_capacity(1 + raw_chunks.len());

        let parsed_payload = serde_json::to_value(serde_json::json!({
            "document_id": document_id,
            "kind": DocumentKind::TerminalOutput.as_str(),
            "natural_key": natural_key,
            "extraction_version": 1,
            "chunk_count": chunk_count,
            "text_byte_len": total_bytes,
            "side_data": side_data,
        }))
        .map_err(|e| AutomatonLogicError::Processing(format!("serialize document.parsed: {e}")))?;

        outputs.push(
            DerivedOutput::transduced(parsed_payload, ts_orig, parent_event_id)
                .with_event_type("document.parsed"),
        );

        let mut byte_offset: u64 = 0;
        for (i, chunk_text) in raw_chunks.into_iter().enumerate() {
            let chunk_len = chunk_text.len() as u64;

            let chunk_payload = serde_json::to_value(serde_json::json!({
                "document_id": document_id,
                "chunk_index": i as u32,
                "text": chunk_text,
                "byte_offset_start": byte_offset,
                "byte_offset_end": byte_offset + chunk_len,
                "source_anchor_start": null,
                "source_anchor_end": null,
            }))
            .map_err(|e| {
                AutomatonLogicError::Processing(format!("serialize document.chunked: {e}"))
            })?;

            outputs.push(
                DerivedOutput::transduced(chunk_payload, ts_orig, parent_event_id)
                    .with_event_type("document.chunked"),
            );
            byte_offset += chunk_len;
        }

        Ok(outputs)
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Extract YAML-like frontmatter between leading `---` delimiters.
/// Returns `(frontmatter_map, body_without_frontmatter)`.
fn extract_frontmatter(content: &str) -> (HashMap<String, String>, String) {
    let mut map = HashMap::new();

    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return (map, content.to_string());
    }

    // Find the closing `---`
    let after_first = &trimmed[3..];
    if let Some(end) = after_first.find("\n---") {
        let fm_block = &after_first[..end];
        let body = after_first[end + 4..].to_string();

        // Crude YAML-like parsing: `key: value` lines.
        for line in fm_block.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once(':') {
                let key = k.trim().to_string();
                let val = v.trim().trim_matches('"').trim_matches('\'').to_string();
                if !key.is_empty() {
                    map.insert(key, val);
                }
            }
        }

        (map, body)
    } else {
        (map, content.to_string())
    }
}

/// Extract `[[wikilink]]` references from text.
fn extract_wikilinks(text: &str) -> Vec<String> {
    let mut links = Vec::new();
    let mut remaining = text;
    while let Some(start) = remaining.find("[[") {
        let after_open = &remaining[start + 2..];
        if let Some(end) = after_open.find("]]") {
            let link = &after_open[..end];
            if !link.is_empty() && !link.contains('[') {
                links.push(link.to_string());
            }
            remaining = &after_open[end + 2..];
        } else {
            break;
        }
    }
    links.sort();
    links.dedup();
    links
}

/// Split text into paragraphs on `\n\n+`, dropping empty paragraphs.
fn paragraph_split(text: &str) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut blank_count = 0u32;

    for line in text.lines() {
        if line.trim().is_empty() {
            blank_count += 1;
        } else {
            if blank_count >= 1 && !current.is_empty() {
                chunks.push(std::mem::take(&mut current));
            }
            blank_count = 0;
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    // Enforce 64 KiB per-chunk cap: hard-split overlong paragraphs.
    let mut capped = Vec::new();
    for chunk in chunks {
        if chunk.len() <= MAX_CHUNK_BYTES {
            capped.push(chunk);
        } else {
            // Split on sentence boundaries or mid-chunk as fallback.
            let mut pos = 0usize;
            while pos < chunk.len() {
                let end = (pos + MAX_CHUNK_BYTES).min(chunk.len());
                let slice = if end < chunk.len() {
                    // Try to split at a sentence boundary.
                    let search_end = end.min(chunk.len());
                    match chunk[search_end.saturating_sub(100)..search_end]
                        .rfind(". ")
                        .or_else(|| chunk[search_end.saturating_sub(100)..search_end].rfind('\n'))
                    {
                        Some(local) => &chunk[pos..=(search_end.saturating_sub(100) + local)],
                        None => &chunk[pos..end],
                    }
                } else {
                    &chunk[pos..end]
                };
                if !slice.trim().is_empty() {
                    capped.push(slice.trim().to_string());
                }
                pos += slice.len();
            }
        }
    }

    if capped.is_empty() {
        // Single empty paragraph for truly empty documents (avoids 0-chunk edge case).
        capped.push(String::new());
    }

    capped
}

/// Split terminal output into line groups on blank lines.
fn line_group_split(text: &str) -> Vec<String> {
    paragraph_split(text)
}

#[cfg(test)]
#[path = "document_parser_test.rs"]
mod tests;

/// Adapter type alias that wires `DocumentParserAutomaton` through the runtime's
/// `MultiOutputTransducerAdapter`.
pub type DocumentParserRuntime =
    crate::runtime::automaton::MultiOutputTransducerAdapter<DocumentParserAutomaton>;

// ── Source descriptor ─────────────────────────────────────────────

use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    ResourceProfile, RetentionPolicy as ContractRetentionPolicy, RunnerPack,
    RuntimeShape as ContractRuntimeShape, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

register_source_contract! {
    SourceContract {
        id: "document-parser",
        namespace: "derived",
        event_types: &[
            ("document-parser", "document.parsed"),
            ("document-parser", "document.chunked"),
        ],
        privacy_tier: ContractPrivacyTier::Sensitive,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(source, parent_event_id, output_event_type, chunk_index)",
        ),
        access_scope: AccessScope::Internal,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:document-parser"),
        "document-parser",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("document.parsed")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("document-parser")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}
