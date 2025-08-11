//! Schema definitions for blob storage tables

use sea_query::{ColumnDef, Iden, Table};

/// Blobs table
#[derive(Iden)]
pub enum Blobs {
    Table,
    Id,
    CreatedAt,
    UpdatedAt,
    ContentHash,
    SizeBytes,
    StoredAt,
    ContentType,
    Metadata,
}

impl Blobs {
    pub fn create_table() -> String {
        Table::create()
            .table(Blobs::Table)
            .if_not_exists()
            .col(ColumnDef::new(Blobs::Id).uuid().not_null().primary_key())
            .col(
                ColumnDef::new(Blobs::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .col(
                ColumnDef::new(Blobs::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .col(ColumnDef::new(Blobs::ContentHash).text().not_null())
            .col(ColumnDef::new(Blobs::SizeBytes).big_integer().not_null())
            .col(ColumnDef::new(Blobs::StoredAt).timestamp_with_time_zone())
            .col(ColumnDef::new(Blobs::ContentType).text())
            .col(ColumnDef::new(Blobs::Metadata).json_binary())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            "CREATE INDEX IF NOT EXISTS idx_blobs_content_hash ON blobs (content_hash);"
                .to_string(),
        ]
    }
}
