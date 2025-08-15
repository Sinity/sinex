//! Schema definitions for schema management tables

use sea_orm_migration::prelude::*;

#[derive(Iden)]
pub enum EventPayloadSchemas {
    #[iden = "event_payload_schemas"]
    Table,
    Id,
    SchemaName,
    SchemaVersion,
    EventType,
    SourceType,
    SchemaContent,
    IsActive,
    CreatedAt,
    UpdatedAt,
    DeprecatedAt,
    DeprecationReason,
    Examples,
    ContentHash,
    Description,
    EventTypes,
    ApprovedBy,
}

#[derive(Iden)]
pub enum SchemaCompatibility {
    Table,
    Id,
}

#[derive(Iden)]
pub enum GitopsSchemaSource {
    Table,
    Id,
}

#[derive(Iden)]
pub enum ValidationCache {
    Table,
    Id,
}

impl EventPayloadSchemas {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("sinex_schemas"), EventPayloadSchemas::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(EventPayloadSchemas::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::SchemaName)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::SchemaVersion)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(EventPayloadSchemas::EventType).text())
            .col(ColumnDef::new(EventPayloadSchemas::SourceType).text())
            .col(
                ColumnDef::new(EventPayloadSchemas::SchemaContent)
                    .json_binary()
                    .not_null(),
            )
            .col(ColumnDef::new(EventPayloadSchemas::ContentHash).text())
            .col(
                ColumnDef::new(EventPayloadSchemas::EventTypes)
                    .array(sea_query::ColumnType::Text)
                    .default("{}"),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::IsActive)
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(EventPayloadSchemas::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(EventPayloadSchemas::DeprecatedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(EventPayloadSchemas::DeprecationReason).text())
            .col(ColumnDef::new(EventPayloadSchemas::Examples).json_binary())
            .col(ColumnDef::new(EventPayloadSchemas::Description).text())
            .col(ColumnDef::new(EventPayloadSchemas::ApprovedBy).text())
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_event_payload_schemas_name_version_unique ON sinex_schemas.event_payload_schemas (schema_name, schema_version);".to_string(),
        ]
    }
}

impl SchemaCompatibility {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("sinex_schemas"), SchemaCompatibility::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(SchemaCompatibility::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl GitopsSchemaSource {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("sinex_schemas"), GitopsSchemaSource::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(GitopsSchemaSource::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl ValidationCache {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("sinex_schemas"), ValidationCache::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(ValidationCache::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}
