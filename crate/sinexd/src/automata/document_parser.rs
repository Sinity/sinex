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
        let (frontmatter, body, body_offset) = extract_frontmatter(&content);
        let title = frontmatter.get("title").cloned();
        let wikilinks = extract_wikilinks(&body);

        // Chunk the body after frontmatter removal. Privacy policy is not
        // applied here; the event engine owns admission/redaction decisions.
        // Each chunk carries its real byte span within `body` (see
        // `paragraph_chunks`); `body_offset` translates that back into a span
        // within the original `content` for the source-anchor columns.
        let raw_chunks: Vec<ChunkSpan> = paragraph_chunks(&body);
        let chunk_count = raw_chunks.len() as u32;
        let total_bytes: u64 = raw_chunks.iter().map(|c| c.text.len() as u64).sum();

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

        let parsed_declaration = &DOCUMENT_PARSER_OUTPUT_DECLARATIONS[0];
        let parsed_output = DerivedOutput::transduced(parsed_payload, ts_orig, parent_event_id)
            .with_event_type("document.parsed")
            .with_declaration_id(parsed_declaration.declaration_id)
            .with_product_class(parsed_declaration.product_class)
            .with_claim_support(parsed_declaration.default_support.instantiate(
                1,
                1,
                1,
                0,
            ))
            .with_semantics_version(parsed_declaration.semantics_version)
            .with_equivalence_key(format!("document-parser:parsed:{document_id}"));

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

        // Emit document.chunked for each chunk. `byte_offset_start/end` are the
        // chunk's real byte span within `body`; `source_anchor_start/end` are
        // the same span translated into the original `content` (i.e. offset
        // by the stripped frontmatter's byte length), so both anchor into
        // actual source-material positions rather than a sum of returned
        // fragment lengths.
        for (i, chunk) in raw_chunks.into_iter().enumerate() {
            let chunk_payload = serde_json::to_value(serde_json::json!({
                "document_id": document_id,
                "chunk_index": i as u32,
                "text": chunk.text,
                "byte_offset_start": chunk.start as u64,
                "byte_offset_end": chunk.end as u64,
                "source_anchor_start": (body_offset + chunk.start) as u64,
                "source_anchor_end": (body_offset + chunk.end) as u64,
            }))
            .map_err(|e| {
                AutomatonLogicError::Processing(format!("serialize document.chunked: {e}"))
            })?;

            let chunk_declaration = &DOCUMENT_PARSER_OUTPUT_DECLARATIONS[1];
            let chunk_output = DerivedOutput::transduced(chunk_payload, ts_orig, parent_event_id)
                .with_event_type("document.chunked")
                .with_declaration_id(chunk_declaration.declaration_id)
                .with_product_class(chunk_declaration.product_class)
                .with_claim_support(chunk_declaration.default_support.instantiate(
                    1,
                    1,
                    1,
                    0,
                ))
                .with_semantics_version(chunk_declaration.semantics_version)
                .with_equivalence_key(format!("document-parser:chunk:{document_id}:{i}"));

            outputs.push(chunk_output);
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
        let raw_chunks: Vec<ChunkSpan> = line_group_chunks(stdout);
        let chunk_count = raw_chunks.len() as u32;
        let total_bytes: u64 = raw_chunks.iter().map(|c| c.text.len() as u64).sum();
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

        let parsed_declaration = &DOCUMENT_PARSER_OUTPUT_DECLARATIONS[0];
        outputs.push(
            DerivedOutput::transduced(parsed_payload, ts_orig, parent_event_id)
                .with_event_type("document.parsed")
                .with_declaration_id(parsed_declaration.declaration_id)
                .with_product_class(parsed_declaration.product_class)
                .with_claim_support(parsed_declaration.default_support.instantiate(
                    1,
                    1,
                    1,
                    0,
                ))
                .with_semantics_version(parsed_declaration.semantics_version)
                .with_equivalence_key(format!("document-parser:parsed:{document_id}")),
        );

        // `byte_offset_start/end` are the chunk's real byte span within
        // `stdout` (see `line_group_chunks`). There is no source material to
        // anchor into for terminal output, so `source_anchor_*` stay null.
        for (i, chunk) in raw_chunks.into_iter().enumerate() {
            let chunk_payload = serde_json::to_value(serde_json::json!({
                "document_id": document_id,
                "chunk_index": i as u32,
                "text": chunk.text,
                "byte_offset_start": chunk.start as u64,
                "byte_offset_end": chunk.end as u64,
                "source_anchor_start": null,
                "source_anchor_end": null,
            }))
            .map_err(|e| {
                AutomatonLogicError::Processing(format!("serialize document.chunked: {e}"))
            })?;

            let chunk_declaration = &DOCUMENT_PARSER_OUTPUT_DECLARATIONS[1];
            outputs.push(
                DerivedOutput::transduced(chunk_payload, ts_orig, parent_event_id)
                    .with_event_type("document.chunked")
                    .with_declaration_id(chunk_declaration.declaration_id)
                    .with_product_class(chunk_declaration.product_class)
                    .with_claim_support(chunk_declaration.default_support.instantiate(
                        1,
                        1,
                        1,
                        0,
                    ))
                    .with_semantics_version(chunk_declaration.semantics_version)
                    .with_equivalence_key(format!("document-parser:chunk:{document_id}:{i}")),
            );
        }

        Ok(outputs)
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Extract YAML-like frontmatter between leading `---` delimiters.
/// Returns `(frontmatter_map, body_without_frontmatter, body_byte_offset)`,
/// where `body_byte_offset` is the byte offset of `body`'s first byte within
/// the original `content`. Callers need this offset to translate chunk spans
/// computed against `body` back into real positions in the source material —
/// without it, every offset downstream of a stripped frontmatter block would
/// be short by the frontmatter's byte length.
fn extract_frontmatter(content: &str) -> (HashMap<String, String>, String, usize) {
    let mut map = HashMap::new();

    let trimmed = content.trim_start();
    let trim_prefix_len = content.len() - trimmed.len();
    if !trimmed.starts_with("---") {
        return (map, content.to_string(), 0);
    }

    // Find the closing `---`
    let after_first = &trimmed[3..];
    if let Some(end) = after_first.find("\n---") {
        let fm_block = &after_first[..end];
        let body_start_in_trimmed = 3 + end + 4;
        let body = trimmed[body_start_in_trimmed..].to_string();

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

        (map, body, trim_prefix_len + body_start_in_trimmed)
    } else {
        (map, content.to_string(), 0)
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

/// A chunk of text together with its real byte span `[start, end)` within
/// the `text` argument it was split from. `text` is `source[start..end]` —
/// callers must not trust a running sum of chunk lengths as an offset
/// substitute, since separator bytes between chunks (and any trimming inside
/// an oversized paragraph split) are not part of any chunk's own length.
struct ChunkSpan {
    start: usize,
    end: usize,
    text: String,
}

/// Byte spans of each line in `text` (start, end-of-content — excluding the
/// `\n` / `\r\n` terminator), mirroring `str::lines()` semantics but
/// preserving absolute byte offsets into `text`.
fn line_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut start = 0usize;
    let bytes = text.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\n' {
            let mut end = i;
            if end > start && bytes[end - 1] == b'\r' {
                end -= 1;
            }
            spans.push((start, end));
            start = i + 1;
        }
    }
    if start < text.len() {
        spans.push((start, text.len()));
    }
    spans
}

/// Byte spans of paragraphs in `text`, split on runs of one or more blank
/// lines (mirroring the historical line-accumulation logic), but computed
/// directly from `line_spans` so each paragraph's span is the real
/// `[first_line_start, last_line_end)` range in `text` — not a
/// reconstruction that drops the separator bytes between lines.
fn paragraph_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut para_start: Option<usize> = None;
    let mut para_end = 0usize;
    let mut blank_run = 0u32;

    for (start, end) in line_spans(text) {
        if text[start..end].trim().is_empty() {
            blank_run += 1;
            continue;
        }
        if blank_run >= 1 {
            if let Some(ps) = para_start.take() {
                spans.push((ps, para_end));
            }
        }
        blank_run = 0;
        if para_start.is_none() {
            para_start = Some(start);
        }
        para_end = end;
    }
    if let Some(ps) = para_start {
        spans.push((ps, para_end));
    }
    spans
}

/// Trim leading/trailing whitespace from `text[start..end]`, returning the
/// trimmed sub-span's real absolute bounds (not just the trimmed string).
fn trim_span(text: &str, start: usize, end: usize) -> (usize, usize) {
    let slice = &text[start..end];
    let trimmed = slice.trim();
    if trimmed.is_empty() {
        return (start, start);
    }
    // Safe: `trimmed` is always a subslice of `slice` returned by `str::trim`.
    let offset = trimmed.as_ptr() as usize - slice.as_ptr() as usize;
    let new_start = start + offset;
    (new_start, new_start + trimmed.len())
}

/// Hard-split an oversized paragraph span into sub-spans no larger than
/// `MAX_CHUNK_BYTES`, preferring sentence/line boundaries near the cap —
/// same policy as the historical implementation, but operating on absolute
/// byte offsets into `text` so every emitted sub-span is still a real
/// position in the source, not a reconstructed fragment.
fn split_capped_span(text: &str, start: usize, end: usize) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut pos = start;
    while pos < end {
        let want_end = (pos + MAX_CHUNK_BYTES).min(end);
        let slice_end = if want_end < end {
            let search_start = want_end.saturating_sub(100).max(pos);
            let search_slice = &text[search_start..want_end];
            match search_slice
                .rfind(". ")
                .or_else(|| search_slice.rfind('\n'))
            {
                Some(local) => search_start + local + 1,
                None => want_end,
            }
        } else {
            want_end
        };
        let actual_end = if slice_end > pos {
            slice_end
        } else {
            want_end.max(pos + 1).min(end)
        };
        let (t_start, t_end) = trim_span(text, pos, actual_end);
        if t_end > t_start {
            out.push((t_start, t_end));
        }
        pos = actual_end;
    }
    out
}

/// Split `text` into paragraph chunks with real byte spans, dropping empty
/// paragraphs and enforcing the 64 KiB per-chunk cap via `split_capped_span`.
fn paragraph_chunks(text: &str) -> Vec<ChunkSpan> {
    let mut spans = Vec::new();
    for (start, end) in paragraph_spans(text) {
        if end - start <= MAX_CHUNK_BYTES {
            spans.push((start, end));
        } else {
            spans.extend(split_capped_span(text, start, end));
        }
    }

    if spans.is_empty() {
        // Single empty paragraph for truly empty documents (avoids 0-chunk edge case).
        return vec![ChunkSpan {
            start: 0,
            end: 0,
            text: String::new(),
        }];
    }

    spans
        .into_iter()
        .map(|(start, end)| ChunkSpan {
            start,
            end,
            text: text[start..end].to_string(),
        })
        .collect()
}

/// Split terminal output into line-group chunks with real byte spans, on
/// blank lines.
fn line_group_chunks(text: &str) -> Vec<ChunkSpan> {
    paragraph_chunks(text)
}

/// Split text into paragraphs on `\n\n+`, dropping empty paragraphs. Thin
/// wrapper over [`paragraph_chunks`] for callers that only need the text
/// (e.g. tests exercising the split policy without offsets).
fn paragraph_split(text: &str) -> Vec<String> {
    paragraph_chunks(text).into_iter().map(|c| c.text).collect()
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
