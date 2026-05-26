//! `core.documents` and `core.document_chunks` — projections of the
//! derived events emitted by the document-layer parser (`#733`).
//!
//! Both tables are application-managed projections of the
//! `document.parsed` / `document.chunked` events; rebuild semantics live
//! in `docs/architecture/document-layer-v1.md`. The Rust definitions
//! here participate in declarative schema convergence (`apply.rs`) and
//! the `schema-strict-diff` covered drift surface.

use crate::primitives::{Timestamp, Uuid};
use crate::schema::TableDef;
use sea_query::{
    Alias, ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, Index, IndexCreateStatement, Table,
    TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// `core.documents`
// =============================================================================

#[derive(Iden, Copy, Clone)]
pub enum Documents {
    Table,
    Id,
    Kind,
    NaturalKey,
    ParsedEventId,
    ExtractionVersion,
    ChunkCount,
    TextByteLen,
    SideData,
    CreatedAt,
    UpdatedAt,
}

impl TableDef for Documents {
    fn table_name() -> &'static str {
        "documents"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct DocumentRecord {
    pub id: Uuid,
    pub kind: String,
    pub natural_key: String,
    pub parsed_event_id: Uuid,
    pub extraction_version: i32,
    pub chunk_count: i32,
    pub text_byte_len: i64,
    pub side_data: JsonValue,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Documents {
    /// `CREATE TABLE` for `core.documents`. The deterministic `id`
    /// is set by the parser (`UUIDv5` over `(NS_DOCUMENTS, source_unit ||
    /// natural_key)`) and is *not* defaulted to `uuidv7()` — the
    /// projection writer always supplies it.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Documents::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key(),
            )
            .col(ColumnDef::new(Documents::Kind).text().not_null())
            .col(ColumnDef::new(Documents::NaturalKey).text().not_null())
            .col(
                ColumnDef::new(Documents::ParsedEventId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Documents::ExtractionVersion)
                    .integer()
                    .not_null(),
            )
            .col(ColumnDef::new(Documents::ChunkCount).integer().not_null())
            .col(
                ColumnDef::new(Documents::TextByteLen)
                    .big_integer()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Documents::SideData)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Documents::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Documents::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .check(Expr::cust(
                "kind IN ('dendron_markdown', 'terminal_output')",
            ))
            .check(Expr::cust("extraction_version >= 1"))
            .check(Expr::cust("chunk_count >= 0"))
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .unique()
                .name("uk_documents_kind_natural_key")
                .table(Self::table_iden())
                .col(Documents::Kind)
                .col(Documents::NaturalKey)
                .to_owned(),
            Index::create()
                .if_not_exists()
                .name("ix_documents_parsed_event_id")
                .table(Self::table_iden())
                .col(Documents::ParsedEventId)
                .to_owned(),
        ]
    }
}

// =============================================================================
// `core.document_chunks`
// =============================================================================

#[derive(Iden, Copy, Clone)]
pub enum DocumentChunks {
    Table,
    DocumentId,
    ChunkIndex,
    Text,
    ByteOffsetStart,
    ByteOffsetEnd,
    SourceAnchorStart,
    SourceAnchorEnd,
    ChunkedEventId,
}

impl TableDef for DocumentChunks {
    fn table_name() -> &'static str {
        "document_chunks"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    /// Composite PK: `(document_id, chunk_index)`. `TableDef::primary_key`
    /// returns the leading column for repository helpers that key on a
    /// single column; chunk lookup helpers use both.
    fn primary_key() -> &'static str {
        "document_id"
    }
}

#[derive(Debug, FromRow, serde::Serialize, serde::Deserialize)]
pub struct DocumentChunkRecord {
    pub document_id: Uuid,
    pub chunk_index: i32,
    pub text: String,
    pub byte_offset_start: i64,
    pub byte_offset_end: i64,
    pub source_anchor_start: Option<i64>,
    pub source_anchor_end: Option<i64>,
    pub chunked_event_id: Uuid,
}

impl DocumentChunks {
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(DocumentChunks::DocumentId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(DocumentChunks::ChunkIndex)
                    .integer()
                    .not_null(),
            )
            .col(ColumnDef::new(DocumentChunks::Text).text().not_null())
            .col(
                ColumnDef::new(DocumentChunks::ByteOffsetStart)
                    .big_integer()
                    .not_null(),
            )
            .col(
                ColumnDef::new(DocumentChunks::ByteOffsetEnd)
                    .big_integer()
                    .not_null(),
            )
            .col(ColumnDef::new(DocumentChunks::SourceAnchorStart).big_integer())
            .col(ColumnDef::new(DocumentChunks::SourceAnchorEnd).big_integer())
            .col(
                ColumnDef::new(DocumentChunks::ChunkedEventId)
                    .custom(Alias::new("UUID"))
                    .not_null(),
            )
            .primary_key(
                Index::create()
                    .col(DocumentChunks::DocumentId)
                    .col(DocumentChunks::ChunkIndex),
            )
            .foreign_key(
                ForeignKey::create()
                    .name("fk_document_chunks_document_id")
                    .from(Self::table_iden(), DocumentChunks::DocumentId)
                    .to(Documents::table_iden(), Documents::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .check(Expr::cust("byte_offset_end >= byte_offset_start"))
            .check(Expr::cust(
                "(source_anchor_start IS NULL) = (source_anchor_end IS NULL)",
            ))
            .check(Expr::cust(
                "source_anchor_start IS NULL OR source_anchor_end >= source_anchor_start",
            ))
            .to_owned()
    }

    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            Index::create()
                .if_not_exists()
                .name("ix_document_chunks_chunked_event_id")
                .table(Self::table_iden())
                .col(DocumentChunks::ChunkedEventId)
                .to_owned(),
        ]
    }

    /// Generates raw SQL for expression GIN indexes on the `text` column.
    ///
    /// - `ix_document_chunks_text_fts` — `to_tsvector('english', text)` for
    ///   `websearch_to_tsquery`-based full-text search (primary retrieval path).
    /// - `ix_document_chunks_text_trgm` — `text gin_trgm_ops` for
    ///   `similarity()`-based fuzzy matching (typo-tolerance fallback, requires
    ///   `pg_trgm` which is already a required extension in `apply.rs`).
    ///
    /// Expression indexes cannot be expressed via sea-query's `Index::create()`,
    /// so they live here as raw SQL following the same pattern as
    /// `Entities::create_gin_indexes_sql()` and
    /// `Entities::create_trigram_indexes_sql()`.
    #[must_use]
    pub fn create_fts_indexes_sql() -> Vec<String> {
        vec![
            format!(
                "CREATE INDEX IF NOT EXISTS ix_document_chunks_text_fts \
                 ON {}.{} USING GIN (to_tsvector('english', text))",
                Self::schema_name(),
                Self::table_name()
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS ix_document_chunks_text_trgm \
                 ON {}.{} USING GIN (text gin_trgm_ops)",
                Self::schema_name(),
                Self::table_name()
            ),
        ]
    }

    /// AFTER INSERT trigger on `core.events` that projects `document.parsed`
    /// and `document.chunked` events into the relational projection tables.
    ///
    /// `document.parsed` → `core.documents` (upsert by `id`)
    /// `document.chunked` → `core.document_chunks` (insert, FK to `core.documents`)
    #[must_use]
    pub fn create_projection_trigger_sql() -> &'static str {
        r"
        CREATE OR REPLACE FUNCTION core.fn_document_projection()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.event_type = 'document.parsed' THEN
            INSERT INTO core.documents (
              id, kind, natural_key, parsed_event_id, extraction_version,
              chunk_count, text_byte_len, side_data, created_at, updated_at
            ) VALUES (
              (NEW.payload->>'document_id')::uuid,
              NEW.payload->>'kind',
              NEW.payload->>'natural_key',
              NEW.id,
              COALESCE((NEW.payload->>'extraction_version')::int, 1),
              COALESCE((NEW.payload->>'chunk_count')::int, 0),
              COALESCE((NEW.payload->>'text_byte_len')::bigint, 0),
              COALESCE(NEW.payload->'side_data', '{}'::jsonb),
              now(), now()
            )
            ON CONFLICT (id) DO UPDATE SET
              parsed_event_id = EXCLUDED.parsed_event_id,
              extraction_version = EXCLUDED.extraction_version,
              chunk_count = EXCLUDED.chunk_count,
              text_byte_len = EXCLUDED.text_byte_len,
              side_data = EXCLUDED.side_data,
              updated_at = now();

          ELSIF NEW.event_type = 'document.chunked' THEN
            INSERT INTO core.document_chunks (
              document_id, chunk_index, text, byte_offset_start, byte_offset_end,
              source_anchor_start, source_anchor_end, chunked_event_id
            ) VALUES (
              (NEW.payload->>'document_id')::uuid,
              COALESCE((NEW.payload->>'chunk_index')::int, 0),
              COALESCE(NEW.payload->>'text', ''),
              COALESCE((NEW.payload->>'byte_offset_start')::bigint, 0),
              COALESCE((NEW.payload->>'byte_offset_end')::bigint, 0),
              (NEW.payload->>'source_anchor_start')::bigint,
              (NEW.payload->>'source_anchor_end')::bigint,
              NEW.id
            )
            ON CONFLICT (document_id, chunk_index) DO NOTHING;
          END IF;

          RETURN NEW;
        END $$;

        DROP TRIGGER IF EXISTS trg_document_projection ON core.events;
        CREATE TRIGGER trg_document_projection
        AFTER INSERT ON core.events
        FOR EACH ROW
        WHEN (NEW.event_type IN ('document.parsed', 'document.chunked'))
        EXECUTE FUNCTION core.fn_document_projection();
        "
    }
}
