//! Migration to standardize primary key naming for source_material_registry
//!
//! Renames the primary key column from `source_material_id` to `id` to match
//! the standard naming convention used by all other tables in the schema.

use async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Rename the primary key column from source_material_id to id
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                RENAME COLUMN source_material_id TO id;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Revert the column rename
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                RENAME COLUMN id TO source_material_id;
                "#,
            )
            .await?;

        Ok(())
    }
}
