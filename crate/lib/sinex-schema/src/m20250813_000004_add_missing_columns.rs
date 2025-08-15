use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add description column to event_payload_schemas
        manager
            .alter_table(
                Table::alter()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .add_column_if_not_exists(ColumnDef::new(Alias::new("description")).text())
                    .to_owned(),
            )
            .await?;

        // Add content column to event_annotations (alias for annotation_data)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.event_annotations 
                ADD COLUMN IF NOT EXISTS content JSONB;
                
                UPDATE core.event_annotations 
                SET content = annotation_data
                WHERE content IS NULL AND annotation_data IS NOT NULL;
                "#,
            )
            .await?;

        // Add type column to entities
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("entities")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("type"))
                            .text()
                            .not_null()
                            .default("unknown"),
                    )
                    .to_owned(),
            )
            .await?;

        // Add unique constraint for processor_checkpoints (processor_name, consumer_group, consumer_name)
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("unique_processor_checkpoint")
                    .table((Alias::new("core"), Alias::new("processor_checkpoints")))
                    .col(Alias::new("processor_name"))
                    .col(Alias::new("consumer_group"))
                    .col(Alias::new("consumer_name"))
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the added columns
        manager
            .alter_table(
                Table::alter()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .drop_column(Alias::new("description"))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("event_annotations")))
                    .drop_column(Alias::new("content"))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("entities")))
                    .drop_column(Alias::new("type"))
                    .to_owned(),
            )
            .await?;

        // Drop the unique constraint
        manager
            .drop_index(Index::drop().name("unique_processor_checkpoint").to_owned())
            .await?;

        Ok(())
    }
}
