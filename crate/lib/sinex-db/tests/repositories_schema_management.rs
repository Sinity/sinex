use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::schema_management::NewEventSchema;
use sinex_primitives::domain::{EventSource, EventType};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn schema_content_hash_has_sufficient_entropy() -> color_eyre::Result<()> {
    let schema = NewEventSchema {
        source: EventSource::new("hash-source"),
        event_type: EventType::new("hash.event"),
        schema_version: "1.0.0".to_string(),
        schema_content: json!({ "type": "object", "properties": { "id": { "type": "string" } } }),
    };

    let hash = schema.calculate_content_hash()?;
    assert!(
        hash.len() >= 32,
        "expected a stable cryptographic hash, got `{hash}`"
    );

    Ok(())
}

#[sinex_test]
async fn register_new_schema_records_metadata(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    let new_schema = NewEventSchema {
        source: EventSource::new("test-source"),
        event_type: EventType::new("user.created"),
        schema_version: "1.0.0".to_string(),
        schema_content: json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "user_id": { "type": "string", "format": "uuid" },
                "email": { "type": "string", "format": "email" },
                "created_at": { "type": "string", "format": "date-time" }
            },
            "required": ["user_id", "email", "created_at"]
        }),
    };

    let schema = repo.register_schema(new_schema.clone()).await?;
    assert_eq!(schema.source, EventSource::new("test-source"));
    assert_eq!(schema.event_type, EventType::new("user.created"));
    assert_eq!(schema.schema_version.as_ref(), "1.0.0");
    assert!(schema.is_active);
    assert_eq!(schema.content_hash, new_schema.calculate_content_hash()?);
    Ok(())
}

#[sinex_test]
async fn registering_duplicate_schema_returns_existing(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    let new_schema = NewEventSchema {
        source: EventSource::new("test-source"),
        event_type: EventType::new("user.updated"),
        schema_version: "1.0.0".to_string(),
        schema_content: json!({ "type": "object", "properties": { "user_id": { "type": "string" } } }),
    };

    let first = repo.register_schema(new_schema.clone()).await?;
    let second = repo.register_schema(new_schema).await?;
    assert_eq!(first.id, second.id);
    assert_eq!(first.content_hash, second.content_hash);
    Ok(())
}

#[sinex_test]
async fn active_schema_returns_latest_version(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    repo.register_schema(NewEventSchema {
        source: EventSource::new("test-source"),
        event_type: EventType::new("config.changed"),
        schema_version: "1.0.0".to_string(),
        schema_content: json!({ "type": "object", "properties": { "key": { "type": "string" } } }),
    })
    .await?;

    let v2 = repo
        .register_schema(NewEventSchema {
            source: EventSource::new("test-source"),
            event_type: EventType::new("config.changed"),
            schema_version: "2.0.0".to_string(),
            schema_content: json!({
                "type": "object",
                "properties": {
                    "key": { "type": "string" },
                    "value": { "type": "string" }
                }
            }),
        })
        .await?;

    let active = repo
        .get_active_schema("test-source", "config.changed")
        .await?;
    assert_eq!(active.id, v2.id);
    assert_eq!(active.schema_version.as_ref(), "2.0.0");
    assert!(active.is_active);
    Ok(())
}

#[sinex_test]
async fn list_schemas_for_source_returns_all(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    for i in 1..=3 {
        repo.register_schema(NewEventSchema {
            source: EventSource::new("multi-source"),
            event_type: EventType::new(&format!("event.type{i}")),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object", "properties": { "id": { "type": "integer" } } })})
        .await?;
    }

    let schemas = repo.list_schemas_for_source("multi-source", false).await?;
    assert!(
        schemas.len() >= 3,
        "Expected at least 3 schemas, saw {}",
        schemas.len()
    );
    assert!(
        schemas
            .iter()
            .all(|s| s.source == EventSource::new("multi-source") && s.is_active)
    );
    Ok(())
}

#[sinex_test]
async fn deprecating_schema_disables_active_version(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    let schema = repo
        .register_schema(NewEventSchema {
            source: EventSource::new("test-source"),
            event_type: EventType::new("deprecated.event"),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object" }),
        })
        .await?;

    repo.deprecate_schema(schema.id.as_ulid()).await?;
    let active = repo
        .get_active_schema("test-source", "deprecated.event")
        .await;
    assert!(active.is_err());
    Ok(())
}

#[sinex_test]
async fn schema_statistics_aggregates_counts(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    // Capture baseline — template DB may have pre-deployed schemas
    let baseline = repo.get_schema_statistics().await?;

    let sources = ["source1", "source2"];
    let event_types = ["event.a", "event.b", "event.c"];

    for source in &sources {
        for event_type in &event_types {
            repo.register_schema(NewEventSchema {
                source: EventSource::new(*source),
                event_type: EventType::new(*event_type),
                schema_version: "1.0.0".to_string(),
                schema_content: json!({ "type": "object" }),
            })
            .await?;
        }
    }

    let stats = repo.get_schema_statistics().await?;
    assert_eq!(stats.total_schemas - baseline.total_schemas, 6);
    assert_eq!(stats.active_schemas - baseline.active_schemas, 6);
    assert_eq!(stats.unique_sources - baseline.unique_sources, 2);
    assert_eq!(stats.unique_event_types - baseline.unique_event_types, 3);
    Ok(())
}

#[sinex_test]
async fn re_registering_schema_reactivates_latest(ctx: TestContext) -> color_eyre::Result<()> {
    let repo = ctx.pool.schemas();
    let schema = repo
        .register_schema(NewEventSchema {
            source: EventSource::new("reactivate-source"),
            event_type: EventType::new("reactivate.event"),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object" }),
        })
        .await?;

    repo.deprecate_schema(schema.id.as_ulid()).await?;
    let inactive = repo
        .find_schema_by_hash(&schema.content_hash)
        .await
        .expect("schema should exist");
    assert!(
        !inactive.is_active,
        "expected schema to be inactive after deprecation"
    );

    let reactivated = repo
        .register_schema(NewEventSchema {
            source: EventSource::new("reactivate-source"),
            event_type: EventType::new("reactivate.event"),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object" }),
        })
        .await?;

    assert!(
        reactivated.is_active,
        "expected identical schema re-registration to reactivate entry"
    );
    Ok(())
}

#[sinex_test]
async fn failed_schema_registration_does_not_clear_active(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let repo = ctx.pool.schemas();
    let original = repo
        .register_schema(NewEventSchema {
            source: EventSource::new("conflict-source"),
            event_type: EventType::new("conflict.event"),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object", "properties": { "legacy": { "type": "string" } }, "required": ["legacy"] })})
        .await?;

    let conflict = repo
        .register_schema(NewEventSchema {
            source: EventSource::new("conflict-source"),
            event_type: EventType::new("conflict.event"),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object", "properties": { "modern": { "type": "string" } } })})
        .await;

    assert!(
        conflict.is_err(),
        "expected duplicate schema version to raise an error"
    );

    let active = repo
        .get_active_schema("conflict-source", "conflict.event")
        .await?;
    assert_eq!(
        active.id, original.id,
        "original schema should remain active when new registration fails"
    );
    Ok(())
}
