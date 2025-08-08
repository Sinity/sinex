use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add content_hash column to event_payload_schemas table
        manager
            .alter_table(
                Table::alter()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .add_column(ColumnDef::new(Alias::new("content_hash")).text())
                    .to_owned(),
            )
            .await?;

        // Populate content_hash for existing rows
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                UPDATE sinex_schemas.event_payload_schemas
                SET content_hash = encode(sha256(schema_content::text::bytea), 'hex')
                WHERE content_hash IS NULL;
                "#,
            )
            .await?;

        // Make content_hash NOT NULL
        manager
            .alter_table(
                Table::alter()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .modify_column(ColumnDef::new(Alias::new("content_hash")).text().not_null())
                    .to_owned(),
            )
            .await?;

        // Add unique constraint on content_hash
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE sinex_schemas.event_payload_schemas
                ADD CONSTRAINT unique_content_hash UNIQUE(content_hash);
                "#,
            )
            .await?;

        // Add index for fast lookups by content hash
        manager
            .create_index(
                Index::create()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .name("idx_schemas_content_hash")
                    .col(Alias::new("content_hash"))
                    .to_owned(),
            )
            .await?;

        // Add source and event_type columns
        manager
            .alter_table(
                Table::alter()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .add_column(ColumnDef::new(Alias::new("source")).text())
                    .add_column(ColumnDef::new(Alias::new("event_type")).text())
                    .to_owned(),
            )
            .await?;

        // Drop old unique constraint
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE sinex_schemas.event_payload_schemas
                DROP CONSTRAINT IF EXISTS unique_schema_name_version;
                "#,
            )
            .await?;

        // Populate source and event_type from schema_name
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                UPDATE sinex_schemas.event_payload_schemas
                SET 
                    source = split_part(schema_name, '.', 1),
                    event_type = substring(schema_name from position('.' in schema_name) + 1)
                WHERE source IS NULL;
                "#,
            )
            .await?;

        // Make source and event_type NOT NULL
        manager
            .alter_table(
                Table::alter()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .modify_column(ColumnDef::new(Alias::new("source")).text().not_null())
                    .modify_column(ColumnDef::new(Alias::new("event_type")).text().not_null())
                    .to_owned(),
            )
            .await?;

        // Add new unique constraint
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE sinex_schemas.event_payload_schemas
                ADD CONSTRAINT unique_schema_identity UNIQUE(source, event_type, schema_version);
                "#,
            )
            .await?;

        // Add comments
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                COMMENT ON COLUMN sinex_schemas.event_payload_schemas.content_hash IS 'SHA-256 hash of the canonical JSON schema content for change detection';
                COMMENT ON COLUMN sinex_schemas.event_payload_schemas.source IS 'Event source (e.g., fs-watcher, terminal)';
                COMMENT ON COLUMN sinex_schemas.event_payload_schemas.event_type IS 'Event type (e.g., file.created, command.executed)';
                "#
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop new unique constraint
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE sinex_schemas.event_payload_schemas
                DROP CONSTRAINT IF EXISTS unique_schema_identity;
                "#,
            )
            .await?;

        // Re-add old unique constraint
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE sinex_schemas.event_payload_schemas
                ADD CONSTRAINT unique_schema_name_version UNIQUE (schema_name, schema_version);
                "#,
            )
            .await?;

        // Drop columns
        manager
            .alter_table(
                Table::alter()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .drop_column(Alias::new("source"))
                    .drop_column(Alias::new("event_type"))
                    .to_owned(),
            )
            .await?;

        // Drop index
        manager
            .drop_index(
                Index::drop()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .name("idx_schemas_content_hash")
                    .to_owned(),
            )
            .await?;

        // Drop unique constraint
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE sinex_schemas.event_payload_schemas
                DROP CONSTRAINT IF EXISTS unique_content_hash;
                "#,
            )
            .await?;

        // Drop content_hash column
        manager
            .alter_table(
                Table::alter()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .drop_column(Alias::new("content_hash"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
