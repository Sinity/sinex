//! Schema definitions for entities and knowledge graph tables

use sea_orm_migration::prelude::*;

#[derive(Iden, Copy, Clone)]
pub enum Entities {
    Table,
    Id,
    Type,
    Name,
    CanonicalName,
    Aliases,
    Description,
    Metadata,
    MergedIntoId,
    CreatedAt,
    UpdatedAt,
}

#[derive(Iden)]
pub enum EntityRelations {
    Table,
    Id,
    FromEntityId,
    ToEntityId,
    RelationType,
    Strength,
    Metadata,
    ValidFrom,
    ValidUntil,
    CreatedFromEventId,
    CreatedAt,
    UpdatedAt,
}

impl Entities {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), Entities::Table))
            .if_not_exists()
            .col(ColumnDef::new(Entities::Id).uuid().not_null().primary_key())
            .col(ColumnDef::new(Entities::Type).text().not_null())
            .col(ColumnDef::new(Entities::Name).text().not_null())
            .col(ColumnDef::new(Entities::CanonicalName).text())
            .col(ColumnDef::new(Entities::Aliases).array(sea_query::ColumnType::Text))
            .col(ColumnDef::new(Entities::Description).text())
            .col(ColumnDef::new(Entities::Metadata).json_binary())
            .col(ColumnDef::new(Entities::MergedIntoId).uuid())
            .col(
                ColumnDef::new(Entities::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .col(
                ColumnDef::new(Entities::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl EntityRelations {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), EntityRelations::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(EntityRelations::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(EntityRelations::FromEntityId)
                    .uuid()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EntityRelations::ToEntityId)
                    .uuid()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EntityRelations::RelationType)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(EntityRelations::Strength).double())
            .col(ColumnDef::new(EntityRelations::Metadata).json_binary())
            .col(ColumnDef::new(EntityRelations::ValidFrom).timestamp_with_time_zone())
            .col(ColumnDef::new(EntityRelations::ValidUntil).timestamp_with_time_zone())
            .col(ColumnDef::new(EntityRelations::CreatedFromEventId).custom("ulid"))
            .col(
                ColumnDef::new(EntityRelations::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .col(
                ColumnDef::new(EntityRelations::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}
