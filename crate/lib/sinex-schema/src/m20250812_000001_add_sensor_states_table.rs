//! Migration to add the sensor_states table
//!
//! Based on TARGET_canonical.md line 250:
//! Adds raw.sensor_states for tracking job state and progress

use sea_orm_migration::prelude::*;

use crate::schema::SensorStates;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create sensor_states table
        manager
            .create_table(SensorStates::create_table_statement())
            .await?;

        // Add foreign key constraint
        for constraint_sql in SensorStates::create_foreign_key_constraints() {
            manager
                .get_connection()
                .execute_unprepared(&constraint_sql)
                .await?;
        }

        // Add check constraints
        for constraint_sql in SensorStates::create_check_constraints() {
            manager
                .get_connection()
                .execute_unprepared(&constraint_sql)
                .await?;
        }

        // Create indexes
        for index_sql in SensorStates::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Add updated_at trigger
        manager
            .get_connection()
            .execute_unprepared(&SensorStates::create_updated_at_trigger())
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop trigger first
        manager
            .get_connection()
            .execute_unprepared(&format!(
                "DROP TRIGGER IF EXISTS trg_sensor_states_updated_at ON raw.{}",
                SensorStates::Table.to_string()
            ))
            .await?;

        // Drop function
        manager
            .get_connection()
            .execute_unprepared("DROP FUNCTION IF EXISTS raw.fn_sensor_states_updated_at()")
            .await?;

        // Drop table (will cascade to constraints and indexes)
        manager
            .drop_table(
                Table::drop()
                    .table((Alias::new("raw"), SensorStates::Table))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
