//! Tests for generic repository operations

#[cfg(test)]
mod tests {
    use crate::db::schema::*;
    use crate::repositories::{DbPoolExt, DbResult, EnhancedRepository, TableDef};
    use crate::types::ulid::Ulid;
    // Use sea_query from this crate for builders, but avoid mixing iden types
    use sea_query::{Alias, PostgresQueryBuilder, Query};
    use sinex_test_utils::{sinex_test, TestContext};

    use color_eyre::eyre::Result;

    use serde_json::json;

    #[sinex_test]
    async fn test_enhanced_repository_count_all(ctx: TestContext) -> Result<()> {
        let pool = &ctx.pool;

        // Test with checkpoints repository
        let checkpoints_repo = pool.checkpoints();
        let initial_count = checkpoints_repo.count_all().await?;

        // The count should be >= 0 since we may have existing test data
        assert!(initial_count >= 0, "Count should be non-negative");

        Ok(())
    }

    #[sinex_test]
    async fn test_enhanced_repository_exists_by_id(ctx: TestContext) -> Result<()> {
        let pool = &ctx.pool;

        // Test with checkpoints repository
        let checkpoints_repo = pool.checkpoints();

        // Generate a random ID that doesn't exist
        let non_existent_id = Ulid::new();
        let exists = checkpoints_repo.exists_by_id(&non_existent_id).await?;
        assert!(!exists, "Random ID should not exist");

        Ok(())
    }

    #[sinex_test]
    async fn test_repository_polymorphism(ctx: TestContext) -> Result<()> {
        let pool = &ctx.pool;

        // We can use EnhancedRepository trait methods on different repository types
        async fn count_records<'a, R: EnhancedRepository<'a>>(repo: &R) -> DbResult<i64> {
            repo.count_all().await
        }

        // Test with different repository types
        let events_count = count_records(&pool.events()).await?;
        let checkpoints_count = count_records(&pool.checkpoints()).await?;
        let entities_count = count_records(&pool.knowledge_graph()).await?;

        // All should work without compilation errors
        assert!(events_count >= 0);
        assert!(checkpoints_count >= 0);
        assert!(entities_count >= 0);

        Ok(())
    }

    #[sinex_test]
    async fn test_seaquery_integration(ctx: TestContext) -> Result<()> {
        let pool = &ctx.pool;

        // Build a query using SeaQuery and TableDef
        let query = Query::select()
            // Avoid cross-crate Iden types by using literal column names
            .column(Alias::new(Events::primary_key()))
            .column(Alias::new("source"))
            .column(Alias::new("event_type"))
            // Build table ref from strings to keep sea_query versions consistent
            .from((
                Alias::new(Events::schema_name()),
                Alias::new(Events::table_name()),
            ))
            .limit(1)
            .to_string(PostgresQueryBuilder);

        // Execute the query - it should work even with no data
        let _rows: Vec<(sqlx::types::Uuid, String, String)> =
            sqlx::query_as(&query).fetch_all(pool).await?;

        // Success if query executes without error
        Ok(())
    }

    #[sinex_test]
    async fn test_table_def_constants(_ctx: TestContext) -> Result<()> {
        // Test that all TableDef implementations have correct values
        assert_eq!(Events::table_name(), "events");
        assert_eq!(Events::schema_name(), "core");
        assert_eq!(Events::primary_key(), "id");

        assert_eq!(ProcessorCheckpoints::table_name(), "processor_checkpoints");
        assert_eq!(ProcessorCheckpoints::schema_name(), "core");
        assert_eq!(ProcessorCheckpoints::primary_key(), "id");

        assert_eq!(EventPayloadSchemas::table_name(), "event_payload_schemas");
        assert_eq!(EventPayloadSchemas::schema_name(), "sinex_schemas");
        assert_eq!(EventPayloadSchemas::primary_key(), "id");

        assert_eq!(
            SourceMaterialRegistry::table_name(),
            "source_material_registry"
        );
        assert_eq!(SourceMaterialRegistry::schema_name(), "raw");
        assert_eq!(SourceMaterialRegistry::primary_key(), "id");

        assert_eq!(OperationsLog::table_name(), "operations_log");
        assert_eq!(OperationsLog::schema_name(), "core");
        assert_eq!(OperationsLog::primary_key(), "id");

        assert_eq!(Entities::table_name(), "entities");
        assert_eq!(Entities::schema_name(), "core");
        assert_eq!(Entities::primary_key(), "id");

        assert_eq!(EntityRelations::table_name(), "entity_relations");
        assert_eq!(EntityRelations::schema_name(), "core");
        assert_eq!(EntityRelations::primary_key(), "id");

        Ok(())
    }
}
