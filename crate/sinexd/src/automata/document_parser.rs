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
use sinex_primitives::events::payloads::DocumentKind;
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

    fn output_event_types(&self) -> &[&'static str] {
        &["document.parsed", "document.chunked"]
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::MaterialOnly
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
mod tests {
    use super::*;
    use sinex_primitives::domain::{ProcessingMode, TriggerKind};
    use sinex_primitives::{Id, Timestamp};
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_frontmatter_extraction() -> TestResult<()> {
        let input = "---\ntitle: My Note\ntags: rust\n---\n\nBody text here.";
        let (fm, body) = extract_frontmatter(input);
        assert_eq!(
            fm.get("title").map(std::string::String::as_str),
            Some("My Note")
        );
        assert_eq!(
            fm.get("tags").map(std::string::String::as_str),
            Some("rust")
        );
        assert!(body.contains("Body text here"));
        Ok(())
    }

    #[sinex_test]
    async fn test_wikilink_extraction() -> TestResult<()> {
        let text = "See [[design-doc]] and also [[rust/ownership]] for details.";
        let links = extract_wikilinks(text);
        assert!(links.contains(&"design-doc".to_string()));
        assert!(links.contains(&"rust/ownership".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_paragraph_split_basic() -> TestResult<()> {
        let text = "Para one.\n\nPara two.\n\n\nPara three.";
        let chunks = paragraph_split(text);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], "Para one.");
        assert_eq!(chunks[1], "Para two.");
        assert_eq!(chunks[2], "Para three.");
        Ok(())
    }

    #[sinex_test]
    async fn test_paragraph_split_empty() -> TestResult<()> {
        let chunks = paragraph_split("");
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_document_id_determinism() -> TestResult<()> {
        let id1 = derive_document_id("dendron", "notes/design.md");
        let id2 = derive_document_id("dendron", "notes/design.md");
        assert_eq!(id1, id2);

        let id3 = derive_document_id("dendron", "notes/other.md");
        assert_ne!(id1, id3);
        Ok(())
    }

    #[sinex_test]
    async fn test_frontmatter_no_closing() -> TestResult<()> {
        let input = "---\ntitle: Unclosed\nBody here.";
        let (fm, body) = extract_frontmatter(input);
        assert!(fm.is_empty() || body.contains("Body"));
        Ok(())
    }

    #[sinex_test]
    async fn test_overlong_chunk_split() -> TestResult<()> {
        let mut big = String::with_capacity(MAX_CHUNK_BYTES + 1000);
        for _ in 0..((MAX_CHUNK_BYTES / 44) + 10) {
            big.push_str("This is a sentence that takes up some space. ");
        }
        let chunks = paragraph_split(&big);
        assert!(chunks.len() > 1, "overlong paragraph should be split");
        for chunk in &chunks {
            assert!(
                chunk.len() <= MAX_CHUNK_BYTES + 200, // allowance for sentence-boundary fudge
                "chunk {} > cap {}",
                chunk.len(),
                MAX_CHUNK_BYTES
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn terminal_chunks_are_not_parser_redacted() -> TestResult<()> {
        let automaton = DocumentParserAutomaton::default();
        let mut state = DocumentParserState::default();
        let event_id = Id::new();
        let context = AutomatonContext {
            trigger_event_id: event_id,
            source: "terminal".into(),
            event_type: "command.canonical".into(),
            ts_orig: Some(Timestamp::UNIX_EPOCH),
            ts_coided: event_id.timestamp(),
            processing_mode: ProcessingMode::Live,
            trigger_kind: TriggerKind::NewEvent,
            created_by_operation_id: None,
        };
        let token = ["ghp_", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"].concat();

        let outputs = automaton.process_terminal(
            &mut state,
            serde_json::json!({
                "command": "cat token",
                "output": format!("token={token}"),
            }),
            &context,
        )?;

        let chunk = outputs
            .iter()
            .find(|output| output.event_type == Some("document.chunked"));
        assert!(chunk.is_some(), "document.chunked output");
        let text = chunk
            .and_then(|output| output.payload["text"].as_str())
            .unwrap_or("");
        assert!(
            text.contains(token.as_str()),
            "document parser must preserve parsed text; DB/user policy owns redaction"
        );
        Ok(())
    }
}

/// Adapter type alias that wires `DocumentParserAutomaton` through the runtime's
/// `MultiOutputTransducerAdapter`.
pub type DocumentParserRuntime =
    crate::runtime::automaton::MultiOutputTransducerAdapter<DocumentParserAutomaton>;

// ── Source descriptor ─────────────────────────────────────────────

use sinex_primitives::source_contracts::{
    CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    RetentionPolicy as ContractRetentionPolicy, RuntimeShape as ContractRuntimeShape,
    SourceContract, SourceRuntimeBinding, SubjectRef,
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
        access_policy: "event_stream_read",
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
    .privacy_context("inherits_from_parents")
    .material_policy("derived_parents")
    .checkpoint_policy("append_stream")
    .resource_shape("event_stream_consumer")
    .source_id("document-parser")
    .runner_pack("sinexd")
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("in_process:sinexd")
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}
