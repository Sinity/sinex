//! Schema definitions for outbox pattern tables

use sea_orm_migration::prelude::*;

#[derive(Iden)]
pub enum Outbox {
    Table,
    Id,
    EventId,
    Destination,
    Payload,
    Status,
    CreatedAt,
    ProcessedAt,
    RetryCount,
    ErrorMessage,
}

impl Outbox {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), Outbox::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(Outbox::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Outbox::EventId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(ColumnDef::new(Outbox::Destination).text().not_null())
            .col(ColumnDef::new(Outbox::Payload).json_binary().not_null())
            .col(
                ColumnDef::new(Outbox::Status)
                    .text()
                    .not_null()
                    .default("'pending'"),
            )
            .col(
                ColumnDef::new(Outbox::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Outbox::ProcessedAt).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Outbox::RetryCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(ColumnDef::new(Outbox::ErrorMessage).text())
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}
