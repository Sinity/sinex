//! Schema definitions for temporal ledger table

use sea_orm_migration::prelude::*;

#[derive(Iden)]
pub enum TemporalLedger {
    Table,
    #[iden = "entry_id"]
    EntryId,
    #[iden = "material_id"]
    MaterialId,
    #[iden = "offset_start"]
    OffsetStart,
    #[iden = "offset_end"]
    OffsetEnd,
    #[iden = "offset_kind"]
    OffsetKind,
    #[iden = "ts_capture"]
    TsCapture,
    #[iden = "precision"]
    Precision,
    #[iden = "clock"]
    Clock,
    #[iden = "source_type"]
    SourceType,
    #[iden = "note"]
    Note,
    #[iden = "created_at"]
    CreatedAt,
}

#[derive(Iden)]
pub enum SourceMaterialRegistry {
    Table,
    #[iden = "blob_id"]
    BlobId,
}

impl TemporalLedger {
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table((Alias::new("raw"), TemporalLedger::Table))
            .if_not_exists()
            // Primary key
            .col(
                ColumnDef::new(TemporalLedger::EntryId)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            // Foreign key to source material
            .col(
                ColumnDef::new(TemporalLedger::MaterialId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            // Offset information
            .col(
                ColumnDef::new(TemporalLedger::OffsetStart)
                    .big_integer()
                    .not_null(),
            )
            .col(
                ColumnDef::new(TemporalLedger::OffsetEnd)
                    .big_integer()
                    .not_null(),
            )
            .col(ColumnDef::new(TemporalLedger::OffsetKind).text().not_null())
            // Timestamp information
            .col(
                ColumnDef::new(TemporalLedger::TsCapture)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(ColumnDef::new(TemporalLedger::Precision).text().not_null())
            .col(ColumnDef::new(TemporalLedger::Clock).text().not_null())
            .col(ColumnDef::new(TemporalLedger::SourceType).text().not_null())
            // Optional note field
            .col(ColumnDef::new(TemporalLedger::Note).text())
            // Creation timestamp
            .col(
                ColumnDef::new(TemporalLedger::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_owned()
    }

    pub fn create_foreign_key_constraints() -> Vec<String> {
        vec![format!(
            r#"ALTER TABLE raw.{} ADD CONSTRAINT fk_temporal_ledger_material_id 
                   FOREIGN KEY (material_id) REFERENCES raw.source_material_registry(blob_id) ON DELETE CASCADE"#,
            TemporalLedger::Table.to_string()
        )]
    }

    pub fn create_check_constraints() -> Vec<String> {
        vec![
            format!(
                r#"ALTER TABLE raw.{} ADD CONSTRAINT chk_temporal_ledger_offset_kind 
                   CHECK (offset_kind IN ('byte', 'line', 'rowid', 'logical'))"#,
                TemporalLedger::Table.to_string()
            ),
            format!(
                r#"ALTER TABLE raw.{} ADD CONSTRAINT chk_temporal_ledger_precision 
                   CHECK (precision IN ('exact', 'bounded'))"#,
                TemporalLedger::Table.to_string()
            ),
            format!(
                r#"ALTER TABLE raw.{} ADD CONSTRAINT chk_temporal_ledger_clock 
                   CHECK (clock IN ('monotonic', 'wall'))"#,
                TemporalLedger::Table.to_string()
            ),
            format!(
                r#"ALTER TABLE raw.{} ADD CONSTRAINT chk_temporal_ledger_source_type 
                   CHECK (source_type IN ('realtime_capture', 'intrinsic_content', 'inferred_mtime', 'inferred_ctime', 'inferred_user'))"#,
                TemporalLedger::Table.to_string()
            ),
            format!(
                r#"ALTER TABLE raw.{} ADD CONSTRAINT chk_temporal_ledger_offsets 
                   CHECK (offset_end >= offset_start)"#,
                TemporalLedger::Table.to_string()
            ),
        ]
    }

    pub fn create_unique_constraints() -> Vec<String> {
        vec![format!(
            r#"ALTER TABLE raw.{} ADD CONSTRAINT uq_temporal_ledger_material_offset 
                   UNIQUE (material_id, offset_start)"#,
            TemporalLedger::Table.to_string()
        )]
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_tl_material_offsets 
                   ON raw.{} (material_id, offset_start, offset_end)"#,
                TemporalLedger::Table.to_string()
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_tl_ts 
                   ON raw.{} (ts_capture, source_type)"#,
                TemporalLedger::Table.to_string()
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_tl_created_at 
                   ON raw.{} (created_at)"#,
                TemporalLedger::Table.to_string()
            ),
        ]
    }

    /// Create append-only trigger function
    pub fn create_append_only_function() -> String {
        r#"CREATE OR REPLACE FUNCTION raw.fn_temporal_ledger_append_only()
           RETURNS TRIGGER AS $$
           BEGIN
             RAISE EXCEPTION 'raw.temporal_ledger is append-only (no % allowed)', TG_OP;
           END;
           $$ LANGUAGE plpgsql;"#
            .to_string()
    }

    /// Create append-only trigger
    pub fn create_append_only_trigger() -> String {
        format!(
            r#"CREATE TRIGGER trg_tl_no_update
               BEFORE UPDATE OR DELETE ON raw.{}
               FOR EACH ROW EXECUTE FUNCTION raw.fn_temporal_ledger_append_only()"#,
            TemporalLedger::Table.to_string()
        )
    }

    /// Drop trigger if exists (for migration rollback)
    pub fn drop_append_only_trigger() -> String {
        format!(
            r#"DROP TRIGGER IF EXISTS trg_tl_no_update ON raw.{}"#,
            TemporalLedger::Table.to_string()
        )
    }
}
