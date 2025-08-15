use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add unique constraint on (schema_name, schema_version)
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_event_payload_schemas_name_version_unique")
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .col(Alias::new("schema_name"))
                    .col(Alias::new("schema_version"))
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the unique constraint
        manager
            .drop_index(
                Index::drop()
                    .name("idx_event_payload_schemas_name_version_unique")
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
