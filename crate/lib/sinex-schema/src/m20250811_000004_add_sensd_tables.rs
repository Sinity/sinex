use sea_orm_migration::prelude::*;

use crate::schema::{SensorJobs, TemporalLedger};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Source material registry is created by the initial schema; do not recreate it here

        // Create sensor_jobs table
        manager
            .create_table(SensorJobs::create_table_statement())
            .await?;

        // Add constraints to sensor_jobs
        for constraint_sql in SensorJobs::create_check_constraints() {
            manager
                .get_connection()
                .execute_unprepared(&constraint_sql)
                .await?;
        }

        // Create indexes for sensor_jobs
        for index_sql in SensorJobs::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Add updated_at trigger for sensor_jobs
        manager
            .get_connection()
            .execute_unprepared(&SensorJobs::create_updated_at_trigger())
            .await?;

        // Create temporal_ledger table
        manager
            .create_table(TemporalLedger::create_table_statement())
            .await?;

        // Add foreign key constraints to temporal_ledger
        for constraint_sql in TemporalLedger::create_foreign_key_constraints() {
            manager
                .get_connection()
                .execute_unprepared(&constraint_sql)
                .await?;
        }

        // Add check constraints to temporal_ledger
        for constraint_sql in TemporalLedger::create_check_constraints() {
            manager
                .get_connection()
                .execute_unprepared(&constraint_sql)
                .await?;
        }

        // Add unique constraints to temporal_ledger
        for constraint_sql in TemporalLedger::create_unique_constraints() {
            manager
                .get_connection()
                .execute_unprepared(&constraint_sql)
                .await?;
        }

        // Create indexes for temporal_ledger
        for index_sql in TemporalLedger::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Create append-only function for temporal_ledger
        manager
            .get_connection()
            .execute_unprepared(&TemporalLedger::create_append_only_function())
            .await?;

        // Create append-only trigger for temporal_ledger
        manager
            .get_connection()
            .execute_unprepared(&TemporalLedger::create_append_only_trigger())
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop temporal_ledger trigger first
        manager
            .get_connection()
            .execute_unprepared(&TemporalLedger::drop_append_only_trigger())
            .await?;

        // Drop append-only function
        manager
            .get_connection()
            .execute_unprepared(
                "DROP FUNCTION IF EXISTS raw.fn_temporal_ledger_append_only() CASCADE",
            )
            .await?;

        // Drop tables in reverse dependency order
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS raw.temporal_ledger CASCADE")
            .await?;

        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS raw.sensor_jobs CASCADE")
            .await?;

        // Do not drop source_material_registry here

        Ok(())
    }
}
