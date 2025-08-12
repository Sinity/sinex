use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop obsolete artifact-related tables in order of dependencies
        // These tables were replaced by the new synthesis architecture and are no longer needed

        // First drop tables that have foreign key dependencies on core.artifacts
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS core.artifact_embeddings CASCADE;")
            .await?;

        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS core.artifact_event_sources CASCADE;")
            .await?;

        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS core.event_artifact_refs CASCADE;")
            .await?;

        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS core.artifact_relations CASCADE;")
            .await?;

        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS core.artifact_tags CASCADE;")
            .await?;

        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS core.artifact_contents CASCADE;")
            .await?;

        // Finally drop the main artifacts table
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS core.artifacts CASCADE;")
            .await?;

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // This migration is irreversible as the artifact system has been
        // replaced by the new synthesis architecture. The artifact tables
        // were obsolete and contained no data that needs to be preserved.
        //
        // If rollback is needed, the artifact tables would need to be recreated
        // from scratch using the original schema definitions, but this is not
        // implemented as the artifact system is permanently deprecated.

        Err(DbErr::Migration(
            "This migration is irreversible. The artifact system has been permanently replaced by the synthesis architecture.".to_string()
        ))
    }
}
