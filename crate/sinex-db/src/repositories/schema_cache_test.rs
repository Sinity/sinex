use super::*;
use crate::repositories::schema_management::{NewEventSchema, SchemaManagementRepository};
use xtask::sandbox::{TestResult, sinex_test};

async fn setup_test_schema(pool: &PgPool) -> TestResult<Id<EventPayloadSchema>> {
    let repo = SchemaManagementRepository::new(pool);
    let schema = NewEventSchema {
        source: EventSource::from_static("test-source"),
        event_type: EventType::from_static("test.event"),
        schema_version: "1.0.0".to_string(),
        schema_content: serde_json::json!({
            "type": "object",
            "properties": {
                "test": {"type": "string"}
            }
        }),
    };
    let result = repo.register_schema(schema).await?;
    Ok(result.id)
}

#[sinex_test]
async fn test_lookup_schema_id(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let schema_id = setup_test_schema(pool).await?;
    let cache_repo = SchemaCacheRepository::new(pool);

    let source = EventSource::from("test-source".to_string());
    let event_type = EventType::from("test.event".to_string());

    let found_id = cache_repo.lookup_schema_id(&source, &event_type).await?;
    assert_eq!(found_id, Some(schema_id));

    Ok(())
}

#[sinex_test]
async fn test_lookup_schema_version(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let schema_id = setup_test_schema(pool).await?;
    let cache_repo = SchemaCacheRepository::new(pool);

    let version = cache_repo.lookup_schema_version(schema_id).await?;
    assert_eq!(version, Some("1.0.0".to_string()));

    Ok(())
}

#[sinex_test]
async fn test_get_schema_content(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let schema_id = setup_test_schema(pool).await?;
    let cache_repo = SchemaCacheRepository::new(pool);

    let content = cache_repo.get_schema_content(schema_id).await?;
    assert!(content.is_some());
    let json = content.unwrap();
    assert_eq!(json["type"], "object");

    Ok(())
}

#[sinex_test]
async fn test_fetch_latest_active_schemas(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    setup_test_schema(pool).await?;
    let cache_repo = SchemaCacheRepository::new(pool);

    let schemas = cache_repo.fetch_latest_active_schemas().await?;
    assert!(!schemas.is_empty());

    let test_schema = schemas
        .iter()
        .find(|s| s.source.as_str() == "test-source" && s.event_type.as_str() == "test.event");
    assert!(test_schema.is_some());

    Ok(())
}

#[sinex_test]
async fn test_get_schemas_by_ids(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let schema_id = setup_test_schema(pool).await?;
    let cache_repo = SchemaCacheRepository::new(pool);

    let schemas = cache_repo.get_schemas_by_ids(&[schema_id]).await?;
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].id, schema_id);

    Ok(())
}

#[sinex_test]
async fn test_preload_schema_metadata(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let schema_id = setup_test_schema(pool).await?;
    let cache_repo = SchemaCacheRepository::new(pool);

    let metadata = cache_repo.preload_schema_metadata().await?;
    assert!(!metadata.is_empty());

    let found = metadata.iter().find(|(id, _, _, _)| *id == schema_id);
    assert!(found.is_some());

    let (id, source, event_type, version) = found.unwrap();
    assert_eq!(*id, schema_id);
    assert_eq!(source.as_str(), "test-source");
    assert_eq!(event_type.as_str(), "test.event");
    assert_eq!(version, "1.0.0");

    Ok(())
}
