//! Schema definitions for knowledge graph tables

use sea_orm_migration::prelude::*;

#[derive(Iden)]
pub enum ArchivedEvents {
    Table,
    Id,
    EventType,
    Source,
    TsOrig,
    TsIngest,
    Host,
    Payload,
    SourceMaterialId,
    SourceEventIds,
    PayloadSchemaId,
    OffsetStart,
    OffsetEnd,
    AnchorByte,
    ArchivedAt,
    ArchivedBy,
    ArchiveReason,
    SupersededByEventId,
}

impl ArchivedEvents {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("audit"), ArchivedEvents::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(ArchivedEvents::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(ArchivedEvents::EventType).text().not_null())
            .col(ColumnDef::new(ArchivedEvents::Source).text().not_null())
            .col(ColumnDef::new(ArchivedEvents::TsOrig).timestamp_with_time_zone())
            .col(
                ColumnDef::new(ArchivedEvents::TsIngest)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(ColumnDef::new(ArchivedEvents::Host).text().not_null())
            .col(
                ColumnDef::new(ArchivedEvents::Payload)
                    .json_binary()
                    .not_null(),
            )
            .col(ColumnDef::new(ArchivedEvents::SourceMaterialId).uuid())
            .col(ColumnDef::new(ArchivedEvents::SourceEventIds).array(sea_query::ColumnType::Uuid))
            .col(ColumnDef::new(ArchivedEvents::PayloadSchemaId).uuid())
            .col(ColumnDef::new(ArchivedEvents::OffsetStart).big_integer())
            .col(ColumnDef::new(ArchivedEvents::OffsetEnd).big_integer())
            .col(ColumnDef::new(ArchivedEvents::AnchorByte).big_integer())
            .col(
                ColumnDef::new(ArchivedEvents::ArchivedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(ArchivedEvents::ArchivedBy).text())
            .col(ColumnDef::new(ArchivedEvents::ArchiveReason).text())
            .col(ColumnDef::new(ArchivedEvents::SupersededByEventId).custom(Alias::new("ULID")))
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}
