use sinex_db::repositories::{DbPoolExt, DbResult, EnhancedRepository, TableDef};
use sinex_db::schema::*;
use sinex_db::{Event, Id, JsonValue};
use xtask::sandbox::sinex_test;

// #[sinex_test]
// async fn enhanced_repository_counts_records(ctx: TestContext) -> TestResult<()> {
//     let checkpoints_repo = ctx.pool.checkpoints();
//     let count = checkpoints_repo.count_all().await?;
//     assert!(count >= 0);
//     Ok(())
// }

#[sinex_test]
async fn enhanced_repository_exists_by_id(ctx: TestContext) -> TestResult<()> {
    let id = Id::<Event<JsonValue>>::new();
    let exists = ctx.pool.events().exists_by_id(id.as_ulid()).await?;
    assert!(!exists);
    Ok(())
}

#[sinex_test]
async fn repository_trait_methods_work_across_tables(ctx: TestContext) -> TestResult<()> {
    async fn count_records<'a, R: EnhancedRepository<'a>>(repo: &R) -> DbResult<i64> {
        repo.count_all().await
    }

    assert!(count_records(&ctx.pool.events()).await? >= 0);
    // assert!(count_records(&ctx.pool.checkpoints()).await? >= 0);
    assert!(count_records(&ctx.pool.knowledge_graph()).await? >= 0);
    Ok(())
}

#[sinex_test]
async fn seaquery_builder_works_with_table_defs(ctx: TestContext) -> TestResult<()> {
    let query = format!(
        "SELECT {}, source, event_type FROM {}.{} LIMIT 1",
        Events::primary_key(),
        Events::schema_name(),
        Events::table_name()
    );

    let _rows: Vec<(sqlx::types::Uuid, String, String)> =
        sqlx::query_as(&query).fetch_all(&ctx.pool).await?;
    Ok(())
}

#[sinex_test]
async fn table_def_constants_match_expectations() -> TestResult<()> {
    assert_eq!(Events::table_name(), "events");
    assert_eq!(Events::schema_name(), "core");
    assert_eq!(Events::primary_key(), "id");

    // assert_eq!(ProcessorCheckpoints::schema_name(), "core");
    // assert_eq!(ProcessorCheckpoints::primary_key(), "id");

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
