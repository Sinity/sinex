//! Schema definitions for core events tables

use sea_query::{Alias, ColumnDef, DeferredForeignKey, ForeignKey, ForeignKeyAction, Index, Iden, Table};

/// Events table (main events storage)
#[derive(Iden)]
pub enum Events {
    Table,
    Id,
    CreatedAt,
    UpdatedAt,
    TsOrig,
    Source,
    EventType,
    Payload,
    PayloadSchemaId,
    ProcessedAt,
    SourceEventIds,
    SourceMaterialId,
    ProcessorName,
    ProcessorVersion,
    AssociatedBlobIds,
    EventClusterId,
}

impl Events {
    pub fn create_table() -> String {
        Table::create()
            .table(Events::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(Events::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Events::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .col(
                ColumnDef::new(Events::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .col(
                ColumnDef::new(Events::TsOrig)
                    .timestamp_with_time_zone(),
            )
            .col(ColumnDef::new(Events::Source).text().not_null())
            .col(ColumnDef::new(Events::EventType).text().not_null())
            .col(ColumnDef::new(Events::Payload).json_binary().not_null())
            .col(ColumnDef::new(Events::PayloadSchemaId).uuid())
            .col(ColumnDef::new(Events::ProcessedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(Events::SourceEventIds).array(sea_query::ColumnType::Uuid))
            .col(ColumnDef::new(Events::SourceMaterialId).uuid())
            .col(ColumnDef::new(Events::ProcessorName).text())
            .col(ColumnDef::new(Events::ProcessorVersion).text())
            .col(ColumnDef::new(Events::AssociatedBlobIds).array(sea_query::ColumnType::Uuid))
            .col(ColumnDef::new(Events::EventClusterId).uuid())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            "CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at);".to_string(),
            "CREATE INDEX IF NOT EXISTS idx_events_source ON events (source);".to_string(),
            "CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);".to_string(),
            "CREATE INDEX IF NOT EXISTS idx_events_ts_orig ON events (ts_orig);".to_string(),
        ]
    }
}