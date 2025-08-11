use crate::schema::TableDef;
use sea_query::{Alias, ColumnDef, Expr, Index, PostgresQueryBuilder, Table};

/// Blobs table schema definition
#[derive(Copy, Clone)]
pub struct Blobs;

impl TableDef for Blobs {
    fn table_name() -> &'static str {
        "blobs"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl Blobs {
    pub const TABLE: &'static str = "blobs";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const ANNEX_KEY: &'static str = "annex_key";
    pub const ORIGINAL_FILENAME: &'static str = "original_filename";
    pub const SIZE_BYTES: &'static str = "size_bytes";
    pub const MIME_TYPE: &'static str = "mime_type";
    pub const CHECKSUM_SHA256: &'static str = "checksum_sha256";
    pub const CHECKSUM_BLAKE3: &'static str = "checksum_blake3";
    pub const STORAGE_BACKEND: &'static str = "storage_backend";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";
    pub const LAST_VERIFIED_AT: &'static str = "last_verified_at";
    pub const VERIFICATION_STATUS: &'static str = "verification_status";

    /// Create the blobs table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ANNEX_KEY))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ORIGINAL_FILENAME))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SIZE_BYTES))
                    .big_integer()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::MIME_TYPE)).text())
            .col(
                ColumnDef::new(Alias::new(Self::CHECKSUM_SHA256))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::CHECKSUM_BLAKE3)).text())
            .col(
                ColumnDef::new(Alias::new(Self::STORAGE_BACKEND))
                    .text()
                    .not_null()
                    .default("git-annex"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::LAST_VERIFIED_AT)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::VERIFICATION_STATUS)).text())
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the blobs table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Unique index on annex_key
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_blobs_annex_key")
                .col(Alias::new(Self::ANNEX_KEY))
                .unique()
                .build(PostgresQueryBuilder),
            // Index on checksum_sha256 for deduplication
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_blobs_checksum_sha256")
                .col(Alias::new(Self::CHECKSUM_SHA256))
                .build(PostgresQueryBuilder),
            // Index on checksum_blake3 for deduplication
            format!(
                "CREATE INDEX idx_blobs_checksum_blake3 ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                Self::CHECKSUM_BLAKE3,
                Self::CHECKSUM_BLAKE3
            ),
        ]
    }
}
