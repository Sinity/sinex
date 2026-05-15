//! `sinexctl documents` — document search, retrieval, and chunk browsing.
//!
//! Calls the three gateway RPC methods added in #332 part 3 (A3 #1280):
//!   - `documents.search`  — FTS + trigram search over `core.document_chunks`
//!   - `documents.get`     — single document metadata lookup
//!   - `documents.get_chunks` — paginated chunk listing for a document

use clap::{Args, Subcommand};
use console::style;
use sinex_primitives::Uuid;
use sinex_primitives::rpc::documents::{
    DocumentsGetChunksRequest, DocumentsGetRequest, DocumentsSearchRequest, DocumentsSearchResponse,
};
use sinex_primitives::rpc::methods;
use sinex_primitives::temporal::Timestamp;
use tabled::{builder::Builder, settings::Style};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;

// ---------------------------------------------------------------------------
// Top-level command
// ---------------------------------------------------------------------------

/// Document search, retrieval, and chunk browsing
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Full-text search for 'error handling'
    sinexctl documents search \"error handling\" --limit 10

    # Restrict to Dendron notes
    sinexctl documents search \"async tokio\" --kind dendron_markdown

    # Get document metadata
    sinexctl documents get 0196ed62-8f7a-7000-8000-000000000001

    # List chunks for a document
    sinexctl documents chunks 0196ed62-8f7a-7000-8000-000000000001 --limit 5

    # Machine-readable output
    sinexctl documents search \"session\" --format json
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
// `documents search`
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

        let params = serde_json::to_value(&request)?;
        let raw = client
            .call_raw_rpc(methods::DOCUMENTS_SEARCH, params)
            .await?;
        let response: DocumentsSearchResponse = serde_json::from_value(raw)?;

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", format_json(&response)?);
            }
            OutputFormat::Yaml => {
                println!("{}", format_yaml(&response)?);
            }
            OutputFormat::Table => {
                println!("{}", render_search_table(&response));
            }
        }
        Ok(())
    }
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
        let headline = result
            .headline
            .replace("<mark>", "")
            .replace("</mark>", "");
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
// `documents get`
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
        let params = serde_json::to_value(&request)?;
        let raw = client
            .call_raw_rpc(methods::DOCUMENTS_GET, params)
            .await?;

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", format_json(&raw)?);
            }
            OutputFormat::Yaml => {
                println!("{}", format_yaml(&raw)?);
            }
            OutputFormat::Table => {
                println!("{}", render_document_table(&raw));
            }
        }
        Ok(())
    }
}

fn render_document_table(doc: &serde_json::Value) -> String {
    let mut builder = Builder::new();
    builder.push_record(["FIELD", "VALUE"]);

    let fields: &[(&str, &str)] = &[
        ("id", "id"),
        ("kind", "kind"),
        ("natural_key", "natural_key"),
        ("chunk_count", "chunk_count"),
        ("extraction_version", "extraction_version"),
        ("text_byte_len", "text_byte_len"),
        ("updated_at", "updated_at"),
        ("created_at", "created_at"),
    ];
    for (label, key) in fields {
        let value = doc
            .get(*key)
            .and_then(|v| {
                if v.is_string() {
                    v.as_str().map(str::to_string)
                } else {
                    Some(v.to_string())
                }
            })
            .unwrap_or_else(|| style("-").dim().to_string());
        builder.push_record([(*label).to_string(), value]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

// ---------------------------------------------------------------------------
// `documents chunks`
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
        let params = serde_json::to_value(&request)?;
        let raw = client
            .call_raw_rpc(methods::DOCUMENTS_GET_CHUNKS, params)
            .await?;

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", format_json(&raw)?);
            }
            OutputFormat::Yaml => {
                println!("{}", format_yaml(&raw)?);
            }
            OutputFormat::Table => {
                println!("{}", render_chunks_table(&raw));
            }
        }
        Ok(())
    }
}

fn render_chunks_table(raw: &serde_json::Value) -> String {
    // The gateway returns the `get_chunks` response as a JSON object with a
    // `chunks` array field (matching DocumentsGetChunksResponse from A3).
    // Gracefully fall back to treating the value itself as an array.
    let chunks = raw
        .get("chunks")
        .and_then(|v| v.as_array())
        .or_else(|| raw.as_array())
        .cloned()
        .unwrap_or_default();

    if chunks.is_empty() {
        return "No chunks found for this document.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record(["IDX", "OFFSET START", "OFFSET END", "TEXT PREVIEW"]);

    for chunk in &chunks {
        let idx = chunk
            .get("chunk_index")
            .and_then(|v| v.as_i64())
            .unwrap_or(-1);
        let start = chunk
            .get("byte_offset_start")
            .and_then(|v| v.as_i64())
            .map_or_else(|| "-".to_string(), |v| v.to_string());
        let end = chunk
            .get("byte_offset_end")
            .and_then(|v| v.as_i64())
            .map_or_else(|| "-".to_string(), |v| v.to_string());
        let text = chunk
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let preview = truncate_str(&text.replace('\n', " "), 72);

        builder.push_record([idx.to_string(), start, end, preview]);
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
