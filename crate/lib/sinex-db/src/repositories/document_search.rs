//! `DocumentSearchRepository` — FTS + trigram retrieval over `core.documents`
//! and `core.document_chunks`.
//!
//! Primary path: `websearch_to_tsquery` + `ts_rank_cd` over the GIN FTS index
//! (`ix_document_chunks_text_fts`) added in #1277.
//!
//! Fallback path: `pg_trgm` similarity over the trigram GIN index
//! (`ix_document_chunks_text_trgm`) — fires only when FTS returns zero results.
//!
//! Pagination is offset-based on `(score DESC, document_id ASC, chunk_index ASC)`.
//!
//! **Query form:** runtime `sqlx::query(...)` is used throughout rather than
//! `sqlx::query!()` macros because the `document_ids: Option<&[Uuid]>` bind
//! cannot be expressed cleanly in the macro form (optional array binds are not
//! supported by sqlx compile-time checking).  Correctness is covered by the
//! integration tests in `crate/lib/sinex-db/tests/document_search_test.rs`.

use super::common::DbResult;
use sinex_primitives::SinexError;
use sinex_primitives::Timestamp;
use sinex_primitives::Uuid;
use sinex_schema::schema::documents::{DocumentChunkRecord, DocumentRecord};
use sqlx::PgPool;
use sqlx::Row;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const DEFAULT_PAGE_SIZE: i64 = 20;
pub const MAX_PAGE_SIZE: i64 = 100;
/// `pg_trgm` similarity threshold.  0.15 catches common typos while filtering
/// unrelated short-text false positives.
pub const TRIGRAM_SIMILARITY_THRESHOLD: f64 = 0.15;

// ---------------------------------------------------------------------------
// Public request / response types
// ---------------------------------------------------------------------------

/// Parameters for a document chunk search.
#[derive(Debug, Clone)]
pub struct DocumentSearchQuery {
    /// Free-text query processed by `websearch_to_tsquery('english', ...)`.
    pub query: String,

    /// Restrict to one document kind (`dendron_markdown` or `terminal_output`).
    pub kind: Option<String>,

    /// Restrict to a list of specific document IDs.
    pub document_ids: Option<Vec<Uuid>>,

    /// Filter documents whose `natural_key` starts with this prefix.
    pub natural_key_prefix: Option<String>,

    /// Lower bound on `documents.updated_at` (inclusive).
    pub updated_after: Option<Timestamp>,

    /// Upper bound on `documents.updated_at` (inclusive).
    pub updated_before: Option<Timestamp>,

    /// Number of results per page.  Capped at `MAX_PAGE_SIZE`.
    pub limit: Option<i64>,

    /// Zero-based offset for page-2+ retrieval.
    pub offset: Option<i64>,
}

/// Which text-search path fired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchMode {
    Fts,
    TrigramFallback,
}

impl SearchMode {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            SearchMode::Fts => "fts",
            SearchMode::TrigramFallback => "trigram_fallback",
        }
    }
}

/// A single ranked chunk result.
#[derive(Debug, Clone)]
pub struct DocumentSearchResult {
    pub document_id: Uuid,
    pub chunk_index: i32,
    pub text: String,
    pub byte_offset_start: i64,
    pub byte_offset_end: i64,
    pub score: f64,
    /// `ts_headline` output with `<mark>`/`</mark>` tags (FTS path).
    /// On trigram fallback this is the raw chunk text.
    pub headline: String,
    pub kind: String,
    pub natural_key: String,
    pub extraction_version: i32,
    pub side_data: serde_json::Value,
    pub updated_at: Timestamp,
}

/// Paginated search response.
#[derive(Debug, Clone)]
pub struct DocumentSearchResults {
    pub results: Vec<DocumentSearchResult>,
    pub search_mode: SearchMode,
}

// ---------------------------------------------------------------------------
// Repository
// ---------------------------------------------------------------------------

pub struct DocumentSearchRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> DocumentSearchRepository<'a> {
    #[must_use]
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Full-text + trigram search over `core.document_chunks`.
    ///
    /// Runs the FTS path first.  If that returns zero results, falls back to
    /// trigram similarity.  The response includes `search_mode` so callers
    /// know which path fired.
    pub async fn search(&self, query: &DocumentSearchQuery) -> DbResult<DocumentSearchResults> {
        if query.query.trim().is_empty() {
            return Err(SinexError::validation(
                "document search query cannot be empty",
            ));
        }

        let limit = query
            .limit
            .unwrap_or(DEFAULT_PAGE_SIZE)
            .min(MAX_PAGE_SIZE)
            .max(1);
        let offset = query.offset.unwrap_or(0).max(0);

        let fts_results = self.search_fts(query, limit, offset).await?;
        if !fts_results.is_empty() {
            return Ok(DocumentSearchResults {
                results: fts_results,
                search_mode: SearchMode::Fts,
            });
        }

        let trgm_results = self.search_trigram(query, limit, offset).await?;
        Ok(DocumentSearchResults {
            results: trgm_results,
            search_mode: SearchMode::TrigramFallback,
        })
    }

    /// Fetch a single document by ID.
    pub async fn get_document(&self, id: Uuid) -> DbResult<Option<DocumentRecord>> {
        let row = sqlx::query_as!(
            DocumentRecord,
            r#"
            SELECT
                id                  as "id!: Uuid",
                kind                as "kind!",
                natural_key         as "natural_key!",
                parsed_event_id     as "parsed_event_id!: Uuid",
                extraction_version  as "extraction_version!",
                chunk_count         as "chunk_count!",
                text_byte_len       as "text_byte_len!",
                side_data           as "side_data!",
                created_at          as "created_at!: Timestamp",
                updated_at          as "updated_at!: Timestamp"
            FROM core.documents
            WHERE id = $1
            "#,
            id as Uuid,
        )
        .fetch_optional(self.pool)
        .await?;
        Ok(row)
    }

    /// Fetch chunks for a document, ordered by `chunk_index`.
    pub async fn get_document_chunks(
        &self,
        doc_id: Uuid,
        limit: usize,
        offset: usize,
    ) -> DbResult<Vec<DocumentChunkRecord>> {
        let limit = limit.min(MAX_PAGE_SIZE as usize) as i64;
        let offset = offset as i64;
        let rows = sqlx::query_as!(
            DocumentChunkRecord,
            r#"
            SELECT
                document_id         as "document_id!: Uuid",
                chunk_index         as "chunk_index!",
                text                as "text!",
                byte_offset_start   as "byte_offset_start!",
                byte_offset_end     as "byte_offset_end!",
                source_anchor_start as "source_anchor_start?: i64",
                source_anchor_end   as "source_anchor_end?: i64",
                chunked_event_id    as "chunked_event_id!: Uuid"
            FROM core.document_chunks
            WHERE document_id = $1
            ORDER BY chunk_index ASC
            LIMIT $2 OFFSET $3
            "#,
            doc_id as Uuid,
            limit,
            offset,
        )
        .fetch_all(self.pool)
        .await?;
        Ok(rows)
    }

    // --- private helpers ---

    async fn search_fts(
        &self,
        query: &DocumentSearchQuery,
        limit: i64,
        offset: i64,
    ) -> DbResult<Vec<DocumentSearchResult>> {
        // Build predicate fragments for optional filters.
        let mut predicates = vec![
            "to_tsvector('english', dc.text) @@ websearch_to_tsquery('english', $1)".to_string(),
        ];
        let mut bind_index: u32 = 2; // $1 is query text

        if query.kind.is_some() {
            predicates.push(format!("d.kind = ${bind_index}"));
            bind_index += 1;
        }
        if query.updated_after.is_some() {
            predicates.push(format!("d.updated_at >= ${bind_index}"));
            bind_index += 1;
        }
        if query.updated_before.is_some() {
            predicates.push(format!("d.updated_at <= ${bind_index}"));
            bind_index += 1;
        }
        if query.document_ids.is_some() {
            predicates.push(format!("d.id = ANY(${bind_index}::uuid[])"));
            bind_index += 1;
        }
        if query.natural_key_prefix.is_some() {
            predicates.push(format!("d.natural_key LIKE (${bind_index} || '%')"));
            bind_index += 1;
        }
        let limit_bind = bind_index;
        bind_index += 1;
        let offset_bind = bind_index;

        let where_clause = predicates.join(" AND ");
        let sql = format!(
            r#"
            WITH search AS (
                SELECT
                    dc.document_id,
                    dc.chunk_index,
                    dc.text,
                    dc.byte_offset_start,
                    dc.byte_offset_end,
                    ts_rank_cd(
                        to_tsvector('english', dc.text),
                        websearch_to_tsquery('english', $1)
                    )::float8 AS score,
                    ts_headline(
                        'english',
                        dc.text,
                        websearch_to_tsquery('english', $1),
                        'MaxWords=40,MinWords=20,StartSel=<mark>,StopSel=</mark>'
                    ) AS headline
                FROM core.document_chunks dc
                JOIN core.documents d ON d.id = dc.document_id
                WHERE {where_clause}
            )
            SELECT
                s.document_id,
                s.chunk_index,
                s.text,
                s.byte_offset_start,
                s.byte_offset_end,
                s.score,
                s.headline,
                d.kind,
                d.natural_key,
                d.extraction_version,
                d.side_data,
                d.updated_at
            FROM search s
            JOIN core.documents d ON d.id = s.document_id
            ORDER BY s.score DESC, s.document_id ASC, s.chunk_index ASC
            LIMIT ${limit_bind} OFFSET ${offset_bind}
            "#
        );

        let mut q = sqlx::query(&sql);
        q = q.bind(&query.query);
        if let Some(ref kind) = query.kind {
            q = q.bind(kind);
        }
        if let Some(ref ts) = query.updated_after {
            q = q.bind(time::OffsetDateTime::from(*ts));
        }
        if let Some(ref ts) = query.updated_before {
            q = q.bind(time::OffsetDateTime::from(*ts));
        }
        if let Some(ref ids) = query.document_ids {
            q = q.bind(ids.as_slice());
        }
        if let Some(ref prefix) = query.natural_key_prefix {
            q = q.bind(prefix);
        }
        q = q.bind(limit);
        q = q.bind(offset);

        let rows = q.fetch_all(self.pool).await?;
        rows.into_iter().map(Self::map_search_row).collect()
    }

    async fn search_trigram(
        &self,
        query: &DocumentSearchQuery,
        limit: i64,
        offset: i64,
    ) -> DbResult<Vec<DocumentSearchResult>> {
        let mut predicates = vec![format!("similarity(dc.text, $1) > $2")];
        let mut bind_index: u32 = 3; // $1 = query, $2 = threshold

        if query.kind.is_some() {
            predicates.push(format!("d.kind = ${bind_index}"));
            bind_index += 1;
        }
        if query.updated_after.is_some() {
            predicates.push(format!("d.updated_at >= ${bind_index}"));
            bind_index += 1;
        }
        if query.updated_before.is_some() {
            predicates.push(format!("d.updated_at <= ${bind_index}"));
            bind_index += 1;
        }
        if query.document_ids.is_some() {
            predicates.push(format!("d.id = ANY(${bind_index}::uuid[])"));
            bind_index += 1;
        }
        if query.natural_key_prefix.is_some() {
            predicates.push(format!("d.natural_key LIKE (${bind_index} || '%')"));
            bind_index += 1;
        }
        let limit_bind = bind_index;
        bind_index += 1;
        let offset_bind = bind_index;

        let where_clause = predicates.join(" AND ");
        let sql = format!(
            r#"
            SELECT
                dc.document_id,
                dc.chunk_index,
                dc.text,
                dc.byte_offset_start,
                dc.byte_offset_end,
                similarity(dc.text, $1)::float8 AS score,
                dc.text AS headline,
                d.kind,
                d.natural_key,
                d.extraction_version,
                d.side_data,
                d.updated_at
            FROM core.document_chunks dc
            JOIN core.documents d ON d.id = dc.document_id
            WHERE {where_clause}
            ORDER BY score DESC, dc.document_id ASC, dc.chunk_index ASC
            LIMIT ${limit_bind} OFFSET ${offset_bind}
            "#
        );

        let mut q = sqlx::query(&sql);
        q = q.bind(&query.query);
        q = q.bind(TRIGRAM_SIMILARITY_THRESHOLD);
        if let Some(ref kind) = query.kind {
            q = q.bind(kind);
        }
        if let Some(ref ts) = query.updated_after {
            q = q.bind(time::OffsetDateTime::from(*ts));
        }
        if let Some(ref ts) = query.updated_before {
            q = q.bind(time::OffsetDateTime::from(*ts));
        }
        if let Some(ref ids) = query.document_ids {
            q = q.bind(ids.as_slice());
        }
        if let Some(ref prefix) = query.natural_key_prefix {
            q = q.bind(prefix);
        }
        q = q.bind(limit);
        q = q.bind(offset);

        let rows = q.fetch_all(self.pool).await?;
        rows.into_iter().map(Self::map_search_row).collect()
    }

    fn map_search_row(row: sqlx::postgres::PgRow) -> DbResult<DocumentSearchResult> {
        Ok(DocumentSearchResult {
            document_id: row.try_get::<Uuid, _>("document_id")?,
            chunk_index: row.try_get::<i32, _>("chunk_index")?,
            text: row.try_get::<String, _>("text")?,
            byte_offset_start: row.try_get::<i64, _>("byte_offset_start")?,
            byte_offset_end: row.try_get::<i64, _>("byte_offset_end")?,
            score: row.try_get::<f64, _>("score")?,
            headline: row.try_get::<String, _>("headline")?,
            kind: row.try_get::<String, _>("kind")?,
            natural_key: row.try_get::<String, _>("natural_key")?,
            extraction_version: row.try_get::<i32, _>("extraction_version")?,
            side_data: row.try_get::<serde_json::Value, _>("side_data")?,
            updated_at: row
                .try_get::<time::OffsetDateTime, _>("updated_at")
                .map(Timestamp::from)?,
        })
    }
}
