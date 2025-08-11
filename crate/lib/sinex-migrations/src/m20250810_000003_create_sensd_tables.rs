//! Migration to create sensd-related tables
//!
//! Creates source_materials, temporal_ledger, and sensor_jobs tables

use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create raw.source_materials table
        manager
            .create_table(
                Table::create()
                    .table((Alias::new("raw"), SourceMaterials::Table))
                    .if_not_exists()
                    .col(
                        ColumnDef::new(SourceMaterials::MaterialId)
                            .custom(Alias::new("ULID"))
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(SourceMaterials::SourceType)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(SourceMaterials::SourcePath)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(SourceMaterials::ContentType).string())
                    .col(
                        ColumnDef::new(SourceMaterials::Status)
                            .string()
                            .not_null()
                            .default("sensing"),
                    )
                    .col(ColumnDef::new(SourceMaterials::TotalBytes).big_integer())
                    .col(
                        ColumnDef::new(SourceMaterials::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(ColumnDef::new(SourceMaterials::FinalizedAt).timestamp_with_time_zone())
                    .col(ColumnDef::new(SourceMaterials::Metadata).json())
                    .to_owned(),
            )
            .await?;

        // Create raw.temporal_ledger table
        manager
            .create_table(
                Table::create()
                    .table((Alias::new("raw"), TemporalLedger::Table))
                    .if_not_exists()
                    .col(
                        ColumnDef::new(TemporalLedger::Id)
                            .custom(Alias::new("ULID"))
                            .not_null()
                            .primary_key()
                            .default(Expr::cust("gen_ulid()")),
                    )
                    .col(
                        ColumnDef::new(TemporalLedger::MaterialId)
                            .custom(Alias::new("ULID"))
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TemporalLedger::OffsetStart)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TemporalLedger::OffsetEnd)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TemporalLedger::TsCaptureStart)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TemporalLedger::TsCaptureEnd)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(ColumnDef::new(TemporalLedger::SliceHash).string())
                    .col(
                        ColumnDef::new(TemporalLedger::CaptureMetadata)
                            .json()
                            .not_null()
                            .default(Expr::cust("'{}'::jsonb")),
                    )
                    .col(
                        ColumnDef::new(TemporalLedger::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                (Alias::new("raw"), TemporalLedger::Table),
                                TemporalLedger::MaterialId,
                            )
                            .to(
                                (Alias::new("raw"), SourceMaterials::Table),
                                SourceMaterials::MaterialId,
                            )
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        // Create indexes
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_temporal_ledger_material")
                    .table((Alias::new("raw"), TemporalLedger::Table))
                    .col(TemporalLedger::MaterialId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_temporal_ledger_unique")
                    .table((Alias::new("raw"), TemporalLedger::Table))
                    .col(TemporalLedger::MaterialId)
                    .col(TemporalLedger::OffsetStart)
                    .unique()
                    .to_owned(),
            )
            .await?;

        // Create raw.sensor_jobs table
        manager
            .create_table(
                Table::create()
                    .table((Alias::new("raw"), SensorJobs::Table))
                    .if_not_exists()
                    .col(
                        ColumnDef::new(SensorJobs::JobId)
                            .custom(Alias::new("ULID"))
                            .not_null()
                            .primary_key()
                            .default(Expr::cust("gen_ulid()")),
                    )
                    .col(ColumnDef::new(SensorJobs::SensorType).string().not_null())
                    .col(ColumnDef::new(SensorJobs::TargetPath).text().not_null())
                    .col(
                        ColumnDef::new(SensorJobs::Config)
                            .json()
                            .not_null()
                            .default(Expr::cust("'{}'::jsonb")),
                    )
                    .col(
                        ColumnDef::new(SensorJobs::Status)
                            .string()
                            .not_null()
                            .default("pending"),
                    )
                    .col(
                        ColumnDef::new(SensorJobs::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(ColumnDef::new(SensorJobs::StartedAt).timestamp_with_time_zone())
                    .col(ColumnDef::new(SensorJobs::CompletedAt).timestamp_with_time_zone())
                    .col(ColumnDef::new(SensorJobs::ErrorMessage).text())
                    .col(ColumnDef::new(SensorJobs::MaterialId).custom(Alias::new("ULID")))
                    .foreign_key(
                        ForeignKey::create()
                            .from(
                                (Alias::new("raw"), SensorJobs::Table),
                                SensorJobs::MaterialId,
                            )
                            .to(
                                (Alias::new("raw"), SourceMaterials::Table),
                                SourceMaterials::MaterialId,
                            )
                            .on_delete(ForeignKeyAction::SetNull),
                    )
                    .to_owned(),
            )
            .await?;

        // Create indexes for sensor_jobs
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_sensor_jobs_status")
                    .table((Alias::new("raw"), SensorJobs::Table))
                    .col(SensorJobs::Status)
                    .to_owned(),
            )
            .await?;

        // Create append-only trigger for temporal_ledger
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION enforce_temporal_ledger_append_only()
                RETURNS TRIGGER AS $$
                BEGIN
                    RAISE EXCEPTION 'Temporal ledger is append-only';
                END;
                $$ LANGUAGE plpgsql;

                CREATE TRIGGER temporal_ledger_append_only
                BEFORE UPDATE OR DELETE ON raw.temporal_ledger
                FOR EACH ROW EXECUTE FUNCTION enforce_temporal_ledger_append_only();
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop triggers
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TRIGGER IF EXISTS temporal_ledger_append_only ON raw.temporal_ledger;
                DROP FUNCTION IF EXISTS enforce_temporal_ledger_append_only();
                "#,
            )
            .await?;

        // Drop tables
        manager
            .drop_table(
                Table::drop()
                    .table((Alias::new("raw"), SensorJobs::Table))
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table((Alias::new("raw"), TemporalLedger::Table))
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table((Alias::new("raw"), SourceMaterials::Table))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum SourceMaterials {
    Table,
    MaterialId,
    SourceType,
    SourcePath,
    ContentType,
    Status,
    TotalBytes,
    CreatedAt,
    FinalizedAt,
    Metadata,
}

#[derive(DeriveIden)]
enum TemporalLedger {
    Table,
    Id,
    MaterialId,
    OffsetStart,
    OffsetEnd,
    TsCaptureStart,
    TsCaptureEnd,
    SliceHash,
    CaptureMetadata,
    CreatedAt,
}

#[derive(DeriveIden)]
enum SensorJobs {
    Table,
    JobId,
    SensorType,
    TargetPath,
    Config,
    Status,
    CreatedAt,
    StartedAt,
    CompletedAt,
    ErrorMessage,
    MaterialId,
}
