use super::TableDef;
use sea_query::{Alias, ColumnDef, Expr, Index, IndexOrder, PostgresQueryBuilder, Table};

/// Source materials table schema definition
#[derive(Copy, Clone)]
pub struct SourceMaterials;

impl TableDef for SourceMaterials {
    fn table_name() -> &'static str {
        "source_material_registry"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl SourceMaterials {
    pub const TABLE: &'static str = "source_material_registry";
    pub const SCHEMA: &'static str = "raw";

    pub const ID: &'static str = "id";
    pub const SOURCE: &'static str = "source";
    pub const ACQUISITION_TIME: &'static str = "acquisition_time";
    pub const PATH: &'static str = "path";
    pub const FORMAT: &'static str = "format";
    pub const COMPRESSION: &'static str = "compression";
    pub const SIZE_BYTES: &'static str = "size_bytes";
    pub const CHECKSUM_SHA256: &'static str = "checksum_sha256";
    pub const METADATA: &'static str = "metadata";
    pub const PROCESSING_STATUS: &'static str = "processing_status";
    pub const PROCESSING_ERROR: &'static str = "processing_error";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const FILE_METADATA: &'static str = "file_metadata";
    pub const EXTRACTION_METADATA: &'static str = "extraction_metadata";
    pub const CONTENT_TYPE: &'static str = "content_type";
    pub const ENCODING: &'static str = "encoding";
    pub const PARENT_ID: &'static str = "parent_id";
    pub const TEMPORAL_LEDGER_ID: &'static str = "temporal_ledger_id";
    pub const BLOB_STORAGE_ID: &'static str = "blob_storage_id";
    pub const DATA: &'static str = "data";

    /// Create the source materials table
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
            .col(ColumnDef::new(Alias::new(Self::SOURCE)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::ACQUISITION_TIME))
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::PATH)).text())
            .col(ColumnDef::new(Alias::new(Self::FORMAT)).text())
            .col(ColumnDef::new(Alias::new(Self::COMPRESSION)).text())
            .col(ColumnDef::new(Alias::new(Self::SIZE_BYTES)).big_integer())
            .col(ColumnDef::new(Alias::new(Self::CHECKSUM_SHA256)).text())
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::PROCESSING_STATUS))
                    .text()
                    .default("pending"),
            )
            .col(ColumnDef::new(Alias::new(Self::PROCESSING_ERROR)).text())
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::FILE_METADATA))
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EXTRACTION_METADATA))
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(ColumnDef::new(Alias::new(Self::CONTENT_TYPE)).text())
            .col(ColumnDef::new(Alias::new(Self::ENCODING)).text())
            .col(ColumnDef::new(Alias::new(Self::PARENT_ID)).custom(Alias::new("ULID")))
            .col(ColumnDef::new(Alias::new(Self::TEMPORAL_LEDGER_ID)).custom(Alias::new("ULID")))
            .col(ColumnDef::new(Alias::new(Self::BLOB_STORAGE_ID)).custom(Alias::new("ULID")))
            .col(ColumnDef::new(Alias::new(Self::DATA)).binary())
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the source materials table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on source
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_source_material_source")
                .col(Alias::new(Self::SOURCE))
                .build(PostgresQueryBuilder),
            // Index on acquisition_time
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_source_material_acquisition_time")
                .col((Alias::new(Self::ACQUISITION_TIME), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // Index on processing_status for processing queries
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_source_material_processing_status")
                .col(Alias::new(Self::PROCESSING_STATUS))
                .build(PostgresQueryBuilder),
            // Index on checksum for deduplication
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_source_material_checksum")
                .col(Alias::new(Self::CHECKSUM_SHA256))
                .build(PostgresQueryBuilder),
            // Index on parent_id for hierarchical queries
            format!(
                "CREATE INDEX idx_source_material_parent ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA, Self::TABLE, Self::PARENT_ID, Self::PARENT_ID
            ),
            // Index on temporal_ledger_id
            format!(
                "CREATE INDEX idx_source_material_temporal_ledger ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA, Self::TABLE, Self::TEMPORAL_LEDGER_ID, Self::TEMPORAL_LEDGER_ID
            ),
        ]
    }
}
