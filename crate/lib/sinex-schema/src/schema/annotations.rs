//! Schema definitions for annotations tables

use sea_orm_migration::prelude::*;

#[derive(Iden)]
pub enum EventAnnotations {
    #[iden = "event_annotations"]
    Table,
    Id,
    EventId,
    AnnotationType,
    Content,
    Metadata,
    AnnotationData,
    CreatedAt,
    UpdatedAt,
    CreatedBy,
}

impl EventAnnotations {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), EventAnnotations::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(EventAnnotations::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(EventAnnotations::EventId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventAnnotations::AnnotationType)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventAnnotations::AnnotationData)
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventAnnotations::Metadata)
                    .json_binary()
                    .default("{}"),
            )
            .col(
                ColumnDef::new(EventAnnotations::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(EventAnnotations::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(EventAnnotations::CreatedBy).text())
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

#[derive(Iden)]
pub enum Tags {
    Table,
    Id,
}

impl Tags {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), Tags::Table))
            .if_not_exists()
            .col(ColumnDef::new(Tags::Id).uuid().not_null().primary_key())
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}
