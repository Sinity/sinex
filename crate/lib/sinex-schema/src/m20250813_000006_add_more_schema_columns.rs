use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. Add strength column to entity_relations
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("entity_relations")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("strength"))
                            .double()
                            .not_null()
                            .default(1.0),
                    )
                    .to_owned(),
            )
            .await?;

        // 2. Add source_uri column to source_material_registry
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("raw"), Alias::new("source_material_registry")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("source_uri"))
                            .text()
                            .not_null()
                            .default(""),
                    )
                    .to_owned(),
            )
            .await?;

        // 3. Add encoding column to source_material_registry
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("raw"), Alias::new("source_material_registry")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("encoding"))
                            .text()
                            .not_null()
                            .default("utf-8"),
                    )
                    .to_owned(),
            )
            .await?;

        // 4. Add is_archived column to source_material_registry
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("raw"), Alias::new("source_material_registry")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("is_archived"))
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .to_owned(),
            )
            .await?;

        // 5. Add finished_at column to operations_log
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("operations_log")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("finished_at")).timestamp_with_time_zone(),
                    )
                    .to_owned(),
            )
            .await?;

        // 6. Add checkpoint column to processor_checkpoints
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.processor_checkpoints 
                ADD COLUMN IF NOT EXISTS checkpoint JSONB NOT NULL DEFAULT '{}'::jsonb
            "#,
            )
            .await?;

        // 7. Add version column to processor_manifests
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("processor_manifests")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("version"))
                            .text()
                            .not_null()
                            .default("0.0.0"),
                    )
                    .to_owned(),
            )
            .await?;

        // 8. Add deployment_status column to processor_manifests
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("processor_manifests")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("deployment_status"))
                            .text()
                            .not_null()
                            .default("inactive"),
                    )
                    .to_owned(),
            )
            .await?;

        // 9. Create index for JSONB ILIKE operator support
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE EXTENSION IF NOT EXISTS pg_trgm;
                CREATE INDEX IF NOT EXISTS idx_events_payload_text 
                ON core.events USING gin ((payload::text) gin_trgm_ops);
            "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the added columns
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("entity_relations")))
                    .drop_column(Alias::new("strength"))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("raw"), Alias::new("source_material_registry")))
                    .drop_column(Alias::new("source_uri"))
                    .drop_column(Alias::new("encoding"))
                    .drop_column(Alias::new("is_archived"))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("operations_log")))
                    .drop_column(Alias::new("finished_at"))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("processor_checkpoints")))
                    .drop_column(Alias::new("checkpoint"))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("processor_manifests")))
                    .drop_column(Alias::new("version"))
                    .drop_column(Alias::new("deployment_status"))
                    .to_owned(),
            )
            .await?;

        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP INDEX IF EXISTS idx_events_payload_text;
            "#,
            )
            .await?;

        Ok(())
    }
}
