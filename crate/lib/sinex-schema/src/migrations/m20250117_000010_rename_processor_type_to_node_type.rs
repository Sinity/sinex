//! Rename processor_type column to node_type in processor_manifests
//!
//! Part of the satellite→node terminology migration. The column describes
//! the type of node (ingestor, automaton, agent, system), aligning with
//! the NodeType enum in the schema definition.
//!
//! This migration is conditional - it only renames if processor_type exists.
//! New databases created with the updated schema already have node_type.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Check if processor_type column exists (old databases) or node_type already exists (new databases)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'core'
                        AND table_name = 'processor_manifests'
                        AND column_name = 'processor_type'
                    ) THEN
                        ALTER TABLE core.processor_manifests RENAME COLUMN processor_type TO node_type;
                    END IF;
                END $$;
                "#,
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Reverse: rename node_type back to processor_type if node_type exists
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'core'
                        AND table_name = 'processor_manifests'
                        AND column_name = 'node_type'
                    ) THEN
                        ALTER TABLE core.processor_manifests RENAME COLUMN node_type TO processor_type;
                    END IF;
                END $$;
                "#,
            )
            .await?;
        Ok(())
    }
}
