//! `sinexctl docs` — document search, retrieval, and chunk browsing.
//!
//! Calls the three gateway RPC methods added in #332 part 3 (A3 #1280):
//!   - `documents.search`  — FTS + trigram search over `core.document_chunks`
//!   - `documents.get`     — single document metadata lookup
//!   - `documents.get_chunks` — paginated chunk listing for a document

use clap::{Args, Subcommand};
use console::style;
use sinex_primitives::Uuid;
use sinex_primitives::rpc::documents::{
    DocumentsGetChunksRequest, DocumentsGetChunksResponse, DocumentsGetRequest,
    DocumentsGetResponse, DocumentsSearchRequest, DocumentsSearchResponse,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::{
    CaveatView, ReadinessCaveatId, SinexObjectKind, SinexObjectRef, ViewEnvelope,
};
use tabled::{builder::Builder, settings::Style};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::print_finite_envelope;
use crate::model::OutputFormat;

// ---------------------------------------------------------------------------
// Top-level command
// ---------------------------------------------------------------------------

/// Document search, retrieval, and chunk browsing
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Full-text search for 'error handling'
    sinexctl docs search \"error handling\" --limit 10

    # Restrict to Dendron notes
    sinexctl docs search \"async tokio\" --kind dendron_markdown

    # Get document metadata
    sinexctl docs get 0196ed62-8f7a-7000-8000-000000000001

    # List chunks for a document
    sinexctl docs chunks 0196ed62-8f7a-7000-8000-000000000001 --limit 5

    # Machine-readable output
    sinexctl docs search \"session\" --format json
")]
pub struct DocumentsCommand {
    #[command(subcommand)]
    cmd: DocumentsSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum DocumentsSubcommand {
    /// Search document chunks using full-text + trigram search
    Search(SearchArgs),
    /// Get document metadata by ID
    Get(GetArgs),
    /// List chunks for a document
    Chunks(ChunksArgs),
}

impl DocumentsCommand {
    #[must_use]
    pub fn subcommand(&self) -> &DocumentsSubcommand {
        &self.cmd
    }

    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            DocumentsSubcommand::Search(args) => args.execute(client, format).await,
            DocumentsSubcommand::Get(args) => args.execute(client, format).await,
            DocumentsSubcommand::Chunks(args) => args.execute(client, format).await,
        }
    }
}

// ---------------------------------------------------------------------------
// `docs search`
// ---------------------------------------------------------------------------

/// Search document chunks
#[derive(Debug, Args)]
pub struct SearchArgs {
    /// Search query (parsed by `websearch_to_tsquery`)
    #[arg(value_name = "QUERY")]
    query: String,

    /// Restrict to document kind: `dendron_markdown` or `terminal_output`
    #[arg(long, value_name = "KIND")]
    kind: Option<String>,

    /// Maximum results (default 20, max 100)
    #[arg(long, default_value_t = 20)]
    limit: u32,

    /// Zero-based offset for pagination
    #[arg(long, default_value_t = 0)]
    offset: u64,

    /// Filter documents updated after this RFC 3339 timestamp
    #[arg(long, value_name = "RFC3339")]
    updated_after: Option<String>,

    /// Filter documents updated before this RFC 3339 timestamp
    #[arg(long, value_name = "RFC3339")]
    updated_before: Option<String>,

    /// Filter by natural-key prefix (e.g. `projects/sinex/`)
    #[arg(long, value_name = "PREFIX")]
    natural_key_prefix: Option<String>,

    /// Restrict to specific document UUIDs (repeatable)
    #[arg(long, value_name = "UUID")]
    document_id: Vec<Uuid>,
}

impl SearchArgs {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let updated_after = self
            .updated_after
            .as_ref()
            .map(|s| {
                Timestamp::parse_rfc3339(s)
                    .map_err(|e| color_eyre::eyre::eyre!("invalid --updated-after: {e}"))
            })
            .transpose()?;

        let updated_before = self
            .updated_before
            .as_ref()
            .map(|s| {
                Timestamp::parse_rfc3339(s)
                    .map_err(|e| color_eyre::eyre::eyre!("invalid --updated-before: {e}"))
            })
            .transpose()?;

        let request = DocumentsSearchRequest {
            query: self.query.clone(),
            kind: self.kind.clone(),
            document_ids: if self.document_id.is_empty() {
                None
            } else {
                Some(self.document_id.clone())
            },
            natural_key_prefix: self.natural_key_prefix.clone(),
            updated_after,
            updated_before,
            limit: Some(self.limit),
            offset: if self.offset == 0 {
                None
            } else {
                Some(self.offset)
            },
        };

        let response: DocumentsSearchResponse = client.documents_search(request.clone()).await?;

        let envelope = documents_search_envelope(response, &request)?;
        if !print_finite_envelope(&envelope, format)? {
            println!("{}", render_search_table(&envelope.payload));
        }
        Ok(())
    }
}

fn documents_search_envelope(
    response: DocumentsSearchResponse,
    request: &DocumentsSearchRequest,
) -> Result<ViewEnvelope<DocumentsSearchResponse>> {
    let mut envelope = ViewEnvelope::new("sinexctl.docs.search", response)
        .with_query_echo(serde_json::to_value(request)?);
    if envelope.payload.results.is_empty() {
        let reason = envelope
            .payload
            .empty_reason
            .as_deref()
            .unwrap_or("unknown");
        envelope.caveats.push(documents_caveat(
            "sinexctl.docs.search",
            format!(
                "document search returned no chunks (empty_reason={reason}); this is an empty document read-model window, not proof that relevant source material never existed"
            ),
            "sinexctl docs search <query>",
        ));
    }
    Ok(envelope)
}

fn render_search_table(response: &DocumentsSearchResponse) -> String {
    if response.results.is_empty() {
        return format!(
            "No results. Search mode: {}",
            style(&response.search_mode).dim()
        );
    }

    let mut builder = Builder::new();
    builder.push_record(["#", "SCORE", "KIND", "NATURAL KEY", "HEADLINE"]);

    for (rank, result) in response.results.iter().enumerate() {
        // Strip <mark> tags from headline for plain-text rendering
        let headline = result.headline.replace("<mark>", "").replace("</mark>", "");
        let headline = truncate_str(&headline, 64);
        let natural_key = truncate_str(&result.natural_key, 40);

        builder.push_record([
            (rank + 1).to_string(),
            format!("{:.4}", result.score),
            result.kind.clone(),
            natural_key,
            headline,
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    let search_mode_note = format!(
        "\nSearch mode: {}  Results: {}",
        style(&response.search_mode).dim(),
        response.results.len()
    );
    format!("{table}{search_mode_note}")
}

// ---------------------------------------------------------------------------
// `docs get`
// ---------------------------------------------------------------------------

/// Get document metadata by ID
#[derive(Debug, Args)]
pub struct GetArgs {
    /// Document UUID
    #[arg(value_name = "UUID")]
    id: Uuid,
}

impl GetArgs {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let request = DocumentsGetRequest { id: self.id };
        let response = client.documents_get(request.clone()).await?;

        let envelope = documents_get_envelope(response, &request)?;
        if !print_finite_envelope(&envelope, format)? {
            println!("{}", render_document_table(&envelope.payload));
        }
        Ok(())
    }
}

fn documents_get_envelope(
    response: DocumentsGetResponse,
    request: &DocumentsGetRequest,
) -> Result<ViewEnvelope<DocumentsGetResponse>> {
    Ok(ViewEnvelope::new("sinexctl.docs.get", response)
        .with_query_echo(serde_json::to_value(request)?))
}

fn render_document_table(doc: &DocumentsGetResponse) -> String {
    let mut builder = Builder::new();
    builder.push_record(["FIELD", "VALUE"]);

    let fields = [
        ("id", doc.id.to_string()),
        ("kind", doc.kind.clone()),
        ("natural_key", doc.natural_key.clone()),
        ("chunk_count", doc.chunk_count.to_string()),
        ("extraction_version", doc.extraction_version.to_string()),
        ("text_byte_len", doc.text_byte_len.to_string()),
        ("updated_at", doc.updated_at.to_string()),
        ("created_at", doc.created_at.to_string()),
    ];
    for (label, value) in fields {
        builder.push_record([label.to_string(), value]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

// ---------------------------------------------------------------------------
// `docs chunks`
// ---------------------------------------------------------------------------

/// List chunks for a document
#[derive(Debug, Args)]
pub struct ChunksArgs {
    /// Document UUID
    #[arg(value_name = "UUID")]
    document_id: Uuid,

    /// Maximum chunks to return (default 20, max 100)
    #[arg(long, default_value_t = 20)]
    limit: u32,

    /// Zero-based offset for pagination
    #[arg(long, default_value_t = 0)]
    offset: u64,
}

impl ChunksArgs {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let request = DocumentsGetChunksRequest {
            document_id: self.document_id,
            limit: Some(self.limit),
            offset: if self.offset == 0 {
                None
            } else {
                Some(self.offset)
            },
        };
        let response = client.documents_get_chunks(request.clone()).await?;

        let envelope = documents_chunks_envelope(response, &request)?;
        if !print_finite_envelope(&envelope, format)? {
            println!("{}", render_chunks_table(&envelope.payload));
        }
        Ok(())
    }
}

fn documents_chunks_envelope(
    response: DocumentsGetChunksResponse,
    request: &DocumentsGetChunksRequest,
) -> Result<ViewEnvelope<DocumentsGetChunksResponse>> {
    let mut envelope = ViewEnvelope::new("sinexctl.docs.chunks", response)
        .with_query_echo(serde_json::to_value(request)?);
    if envelope.payload.chunks.is_empty() {
        envelope.caveats.push(documents_caveat(
            "sinexctl.docs.chunks",
            "document chunk listing returned no chunks; document text coverage is unmeasurable from this response",
            "sinexctl docs chunks <document-id>",
        ));
    }
    Ok(envelope)
}

fn render_chunks_table(response: &DocumentsGetChunksResponse) -> String {
    if response.chunks.is_empty() {
        return "No chunks found for this document.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record(["IDX", "OFFSET START", "OFFSET END", "TEXT PREVIEW"]);

    for chunk in &response.chunks {
        let preview = truncate_str(&chunk.text.replace('\n', " "), 72);

        builder.push_record([
            chunk.chunk_index.to_string(),
            chunk.byte_offset_start.to_string(),
            chunk.byte_offset_end.to_string(),
            preview,
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

fn truncate_str(s: &str, max_len: usize) -> String {
    let cutoff = max_len.saturating_sub(3);
    match s.char_indices().nth(max_len) {
        None => s.to_string(),
        Some(_) => match s.char_indices().nth(cutoff) {
            None => s.to_string(),
            Some((byte_pos, _)) => format!("{}...", &s[..byte_pos]),
        },
    }
}

fn documents_caveat(
    source_surface: &'static str,
    message: impl Into<String>,
    command_hint: &'static str,
) -> CaveatView {
    CaveatView {
        id: ReadinessCaveatId::CoverageUnmeasurable.as_str().to_string(),
        message: message.into(),
        ref_: Some(
            SinexObjectRef::new(SinexObjectKind::Command, source_surface)
                .with_label(source_surface)
                .with_command_hint(command_hint),
        ),
    }
}

#[cfg(test)]
#[path = "documents_test.rs"]
mod tests;
