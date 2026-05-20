//! Documents RPC handlers: `documents.search`, `documents.get`, `documents.get_chunks`.
//!
//! All three methods are `ReadOnly` — they query `core.documents` and
//! `core.document_chunks` but never write.
//!
//! The search handler runs FTS via `websearch_to_tsquery` with automatic
//! `pg_trgm` fallback when FTS returns zero results (logic lives in
//! `DocumentSearchRepository::search`).

use sinex_db::DbPoolExt;
use sinex_db::repositories::{DocumentSearchQuery, SearchMode};
use sinex_primitives::rpc::documents::{
    DocumentsChunkEntry, DocumentsGetChunksRequest, DocumentsGetChunksResponse,
    DocumentsGetRequest, DocumentsGetResponse, DocumentsSearchRequest, DocumentsSearchResponse,
    DocumentsSearchResult,
};
use sinex_primitives::{Result, SinexError};
use sqlx::PgPool;

// ── documents.search ─────────────────────────────────────────────────────────

/// Handle `documents.search` — ranked FTS with `pg_trgm` fallback.
///
/// Maps the RPC [`DocumentsSearchRequest`] to a [`DocumentSearchQuery`],
/// calls the repository, then converts the result set to
/// [`DocumentsSearchResponse`].
pub async fn handle_documents_search(
    pool: &PgPool,
    req: DocumentsSearchRequest,
) -> Result<DocumentsSearchResponse> {
    if req.query.trim().is_empty() {
        return Err(
            SinexError::validation("documents.search: query must not be empty")
                .with_context("field", "query"),
        );
    }

    let query = DocumentSearchQuery {
        query: req.query,
        kind: req.kind,
        document_ids: req.document_ids.map(|ids| ids.into_iter().collect()),
        natural_key_prefix: req.natural_key_prefix,
        updated_after: req.updated_after,
        updated_before: req.updated_before,
        limit: req.limit.map(i64::from),
        offset: req.offset.map(|v| v as i64),
    };

    let requested_offset = req.offset.unwrap_or(0);
    let results = pool.documents().search(&query).await?;

    let search_mode = match results.search_mode {
        SearchMode::Fts => "fts",
        SearchMode::TrigramFallback => "trigram_fallback",
    };

    let response = DocumentsSearchResponse {
        has_more: results.has_more,
        next_offset: results
            .has_more
            .then_some(requested_offset + results.results.len() as u64),
        empty_reason: results
            .empty_reason
            .as_ref()
            .map(|reason| reason.as_str().to_string()),
        results: results
            .results
            .into_iter()
            .map(|r| DocumentsSearchResult {
                document_id: r.document_id,
                kind: r.kind,
                natural_key: r.natural_key,
                chunk_index: r.chunk_index,
                headline: r.headline,
                text: r.text,
                score: r.score,
                byte_offset_start: r.byte_offset_start,
                byte_offset_end: r.byte_offset_end,
                extraction_version: r.extraction_version,
                side_data: r.side_data,
                updated_at: r.updated_at,
            })
            .collect(),
        search_mode: search_mode.to_owned(),
    };

    Ok(response)
}

// ── documents.get ─────────────────────────────────────────────────────────────

/// Handle `documents.get` — fetch one document by deterministic UUID.
///
/// Returns 404 when the document does not exist.
pub async fn handle_documents_get(
    pool: &PgPool,
    req: DocumentsGetRequest,
) -> Result<DocumentsGetResponse> {
    let record = pool
        .documents()
        .get_document(req.id)
        .await?
        .ok_or_else(|| SinexError::not_found(format!("document not found: {}", req.id)))?;

    let response = DocumentsGetResponse {
        id: record.id,
        kind: record.kind,
        natural_key: record.natural_key,
        extraction_version: record.extraction_version,
        chunk_count: record.chunk_count,
        text_byte_len: record.text_byte_len,
        side_data: record.side_data,
        created_at: record.created_at,
        updated_at: record.updated_at,
    };

    Ok(response)
}

// ── documents.get_chunks ──────────────────────────────────────────────────────

/// Handle `documents.get_chunks` — fetch ordered chunks for one document.
///
/// Applies optional `limit` (default 50, max 200) and `offset` pagination.
pub async fn handle_documents_get_chunks(
    pool: &PgPool,
    req: DocumentsGetChunksRequest,
) -> Result<DocumentsGetChunksResponse> {
    const DEFAULT_LIMIT: u32 = 50;
    const MAX_LIMIT: u32 = 200;

    let limit = req.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT) as usize;
    let offset = req.offset.unwrap_or(0) as usize;

    let chunks = pool
        .documents()
        .get_document_chunks(req.document_id, limit, offset)
        .await?;

    let response = DocumentsGetChunksResponse {
        document_id: req.document_id,
        chunks: chunks
            .into_iter()
            .map(|c| DocumentsChunkEntry {
                document_id: c.document_id,
                chunk_index: c.chunk_index,
                text: c.text,
                byte_offset_start: c.byte_offset_start,
                byte_offset_end: c.byte_offset_end,
                source_anchor_start: c.source_anchor_start,
                source_anchor_end: c.source_anchor_end,
            })
            .collect(),
    };

    Ok(response)
}
