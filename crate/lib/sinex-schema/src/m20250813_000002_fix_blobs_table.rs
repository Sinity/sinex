use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add missing columns to blobs table
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("blobs")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("annex_key"))
                            .text()
                            .not_null()
                            .default(""),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("original_filename")).text(),
                    )
                    .add_column_if_not_exists(ColumnDef::new(Alias::new("mime_type")).text())
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("checksum_sha256"))
                            .text()
                            .not_null()
                            .default(""),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("checksum_blake3"))
                            .text()
                            .not_null()
                            .default(""),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("storage_backend"))
                            .text()
                            .not_null()
                            .default("local"),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("last_verified_at")).timestamp_with_time_zone(),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("verification_status"))
                            .text()
                            .not_null()
                            .default("unverified"),
                    )
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
                    .table((Alias::new("core"), Alias::new("blobs")))
                    .drop_column(Alias::new("annex_key"))
                    .drop_column(Alias::new("original_filename"))
                    .drop_column(Alias::new("mime_type"))
                    .drop_column(Alias::new("checksum_sha256"))
                    .drop_column(Alias::new("checksum_blake3"))
                    .drop_column(Alias::new("storage_backend"))
                    .drop_column(Alias::new("last_verified_at"))
                    .drop_column(Alias::new("verification_status"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
