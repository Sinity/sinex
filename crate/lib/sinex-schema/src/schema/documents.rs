//! `core.documents` and `core.document_chunks` — projections of the
//! synthesis events emitted by the document-layer parser (`#733`).
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
    /// is set by the parser (UUIDv5 over `(NS_DOCUMENTS, source_unit ||
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
            .col(ColumnDef::new(Documents::TextByteLen).big_integer().not_null())
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
            .col(
                ColumnDef::new(DocumentChunks::SourceAnchorStart)
                    .big_integer(),
            )
            .col(
                ColumnDef::new(DocumentChunks::SourceAnchorEnd)
                    .big_integer(),
            )
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
}

