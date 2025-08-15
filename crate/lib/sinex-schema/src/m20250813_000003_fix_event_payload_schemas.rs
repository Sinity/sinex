use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add event_types column as array of text
        manager
            .alter_table(
                Table::alter()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("event_types"))
                            .array(sea_query::ColumnType::Text),
                    )
                    .to_owned(),
            )
            .await?;

        // Populate event_types from event_type if it exists and event_types is null
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                UPDATE sinex_schemas.event_payload_schemas 
                SET event_types = ARRAY[event_type]
                WHERE event_type IS NOT NULL 
                  AND event_types IS NULL
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the event_types column
        manager
            .alter_table(
                Table::alter()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .drop_column(Alias::new("event_types"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
