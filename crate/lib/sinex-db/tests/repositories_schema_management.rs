use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::schema_management::NewEventSchema;
use sinex_primitives::domain::{EventSource, EventType};
use uuid::Uuid;
use xtask::sandbox::sinex_test;

fn unique_schema_source(prefix: &str) -> EventSource {
    format!("{prefix}-{}", Uuid::now_v7()).into()
}

fn unique_schema_event_type(prefix: &str) -> EventType {
    format!("{prefix}.{}", Uuid::now_v7()).into()
}

#[sinex_test]
async fn schema_content_hash_has_sufficient_entropy() -> color_eyre::Result<()> {
    let schema = NewEventSchema {
        source: EventSource::from_static("hash-source"),
        event_type: EventType::from_static("hash.event"),
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
    let source = unique_schema_source("test-source");
    let event_type = unique_schema_event_type("user.created");
    let new_schema = NewEventSchema {
        source: source.clone(),
        event_type: event_type.clone(),
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
    assert_eq!(schema.source, source);
    assert_eq!(schema.event_type, event_type);
    assert_eq!(schema.schema_version.as_ref(), "1.0.0");
    assert!(schema.is_active);
    assert_eq!(schema.content_hash, new_schema.calculate_content_hash()?);
    Ok(())
}

#[sinex_test]
async fn registering_duplicate_schema_returns_existing(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    let source = unique_schema_source("test-source");
    let event_type = unique_schema_event_type("user.updated");
    let new_schema = NewEventSchema {
        source,
        event_type,
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
    let source = unique_schema_source("test-source");
    let event_type = unique_schema_event_type("config.changed");
    repo.register_schema(NewEventSchema {
        source: source.clone(),
        event_type: event_type.clone(),
        schema_version: "1.0.0".to_string(),
        schema_content: json!({ "type": "object", "properties": { "key": { "type": "string" } } }),
    })
    .await?;

    let v2 = repo
        .register_schema(NewEventSchema {
            source: source.clone(),
            event_type: event_type.clone(),
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
        .get_active_schema(source.as_str(), event_type.as_str())
        .await?;
    assert_eq!(active.id, v2.id);
    assert_eq!(active.schema_version.as_ref(), "2.0.0");
    assert!(active.is_active);
    Ok(())
}

#[sinex_test]
async fn list_schemas_for_source_returns_all(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    let source = unique_schema_source("multi-source");
    for i in 1..=3 {
        repo.register_schema(NewEventSchema {
            source: source.clone(),
            event_type: format!("event.type{i}").into(),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object", "properties": { "id": { "type": "integer" } } })})
        .await?;
    }

    let schemas = repo.list_schemas_for_source(source.as_str(), false).await?;
    assert!(
        schemas.len() >= 3,
        "Expected at least 3 schemas, saw {}",
        schemas.len()
    );
    assert!(schemas.iter().all(|s| s.source == source && s.is_active));
    Ok(())
}

#[sinex_test]
async fn list_with_retention_returns_only_active_retention_rows(
    ctx: TestContext,
) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    let source = unique_schema_source("retention-source");
    let active_event_type = unique_schema_event_type("retention.active");
    let inactive_event_type = unique_schema_event_type("retention.inactive");
    let no_retention_event_type = unique_schema_event_type("retention.none");

    let active = repo
        .register_schema(NewEventSchema {
            source: source.clone(),
            event_type: active_event_type.clone(),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object" }),
        })
        .await?;
    let inactive = repo
        .register_schema(NewEventSchema {
            source: source.clone(),
            event_type: inactive_event_type,
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object", "properties": { "old": { "type": "boolean" } } }),
        })
        .await?;
    repo.register_schema(NewEventSchema {
        source: source.clone(),
        event_type: no_retention_event_type,
        schema_version: "1.0.0".to_string(),
        schema_content: json!({ "type": "object", "properties": { "kept": { "type": "boolean" } } }),
    })
    .await?;

    sqlx::query!(
        r#"
        UPDATE sinex_schemas.event_payload_schemas
        SET retention_seconds = CASE
                WHEN id = $1::uuid THEN 3600
                WHEN id = $2::uuid THEN 60
                ELSE retention_seconds
            END,
            is_active = CASE
                WHEN id = $2::uuid THEN false
                ELSE is_active
            END
        WHERE id IN ($1::uuid, $2::uuid)
        "#,
        active.id.to_uuid(),
        inactive.id.to_uuid()
    )
    .execute(&ctx.pool)
    .await?;

    let rows = repo.list_with_retention().await?;
    let row = rows
        .iter()
        .find(|row| row.source == source && row.event_type == active_event_type)
        .expect("active retention row should be returned");

    assert_eq!(row.retention_seconds, 3600);
    assert!(
        rows.iter()
            .all(|row| row.source != source || row.event_type == active_event_type),
        "inactive or NULL retention rows should not be returned"
    );
    Ok(())
}

#[sinex_test]
async fn deprecating_schema_disables_active_version(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    let source = unique_schema_source("test-source");
    let event_type = unique_schema_event_type("deprecated.event");
    let schema = repo
        .register_schema(NewEventSchema {
            source: source.clone(),
            event_type: event_type.clone(),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object" }),
        })
        .await?;

    repo.deprecate_schema(&schema.id).await?;
    let active = repo
        .get_active_schema(source.as_str(), event_type.as_str())
        .await;
    assert!(active.is_err());
    Ok(())
}

#[sinex_test]
async fn schema_statistics_aggregates_counts(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    // Capture baseline — template DB may have pre-deployed schemas
    let baseline = repo.get_schema_statistics().await?;

    let sources = [
        unique_schema_source("schema-stats-source1"),
        unique_schema_source("schema-stats-source2"),
    ];
    let event_types = [
        unique_schema_event_type("event.a"),
        unique_schema_event_type("event.b"),
        unique_schema_event_type("event.c"),
    ];

    for source in &sources {
        for event_type in &event_types {
            repo.register_schema(NewEventSchema {
                source: source.clone(),
                event_type: event_type.clone(),
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
    let source = unique_schema_source("reactivate-source");
    let event_type = unique_schema_event_type("reactivate.event");
    let schema = repo
        .register_schema(NewEventSchema {
            source: source.clone(),
            event_type: event_type.clone(),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object" }),
        })
        .await?;

    repo.deprecate_schema(&schema.id).await?;
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
            source,
            event_type,
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
    let source = unique_schema_source("conflict-source");
    let event_type = unique_schema_event_type("conflict.event");
    let original = repo
        .register_schema(NewEventSchema {
            source: source.clone(),
            event_type: event_type.clone(),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object", "properties": { "legacy": { "type": "string" } }, "required": ["legacy"] })})
        .await?;

    let conflict = repo
        .register_schema(NewEventSchema {
            source: source.clone(),
            event_type: event_type.clone(),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({ "type": "object", "properties": { "modern": { "type": "string" } } })})
        .await;

    assert!(
        conflict.is_err(),
        "expected duplicate schema version to raise an error"
    );

    let active = repo
        .get_active_schema(source.as_str(), event_type.as_str())
        .await?;
    assert_eq!(
        active.id, original.id,
        "original schema should remain active when new registration fails"
    );
    Ok(())
}

#[sinex_test]
async fn sync_schema_bundle_reactivates_inactive_matching_row(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let repo = ctx.pool.schemas();
    let source = unique_schema_source("sync-source");
    let event_type = unique_schema_event_type("sync.event");
    let schema = NewEventSchema {
        source,
        event_type,
        schema_version: "1.0.0".to_string(),
        schema_content: json!({
            "type": "object",
            "properties": { "value": { "type": "string" } },
            "required": ["value"]
        }),
    };

    let registered = repo.register_schema(schema.clone()).await?;
    repo.deprecate_schema(&registered.id).await?;

    let sync_result = repo
        .sync_schema_bundle([
            sinex_primitives::events::schema_registry::SchemaBundleEntry::new(
                schema.source.to_string(),
                schema.event_type.to_string(),
                schema.schema_version.clone(),
                schema.schema_content.clone(),
            )?,
        ])
        .await?;

    assert_eq!(sync_result.created, 0);
    assert_eq!(sync_result.updated, 1);

    let active = repo
        .get_active_schema(schema.source.as_str(), schema.event_type.as_str())
        .await?;
    assert_eq!(active.id, registered.id);
    assert!(active.is_active);
    Ok(())
}

#[sinex_test]
async fn sync_schema_bundle_converges_same_version_content_drift(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let repo = ctx.pool.schemas();
    let source = unique_schema_source("sync-drift-source");
    let event_type = unique_schema_event_type("sync.drift.event");
    let original = NewEventSchema {
        source: source.clone(),
        event_type: event_type.clone(),
        schema_version: "1.0.0".to_string(),
        schema_content: json!({
            "type": "object",
            "properties": { "legacy": { "type": "string" } },
            "required": ["legacy"]
        }),
    };

    let registered = repo.register_schema(original).await?;

    let discovered = NewEventSchema {
        source,
        event_type,
        schema_version: "1.0.0".to_string(),
        schema_content: json!({
            "type": "object",
            "properties": {
                "modern": { "type": "string" },
                "count": { "type": "integer" }
            },
            "required": ["modern"]
        }),
    };
    let discovered_entry = sinex_primitives::events::schema_registry::SchemaBundleEntry::new(
        discovered.source.to_string(),
        discovered.event_type.to_string(),
        discovered.schema_version.clone(),
        discovered.schema_content.clone(),
    )?;

    let sync_result = repo.sync_schema_bundle([discovered_entry.clone()]).await?;

    assert_eq!(sync_result.created, 0);
    assert_eq!(sync_result.updated, 1);
    assert_eq!(sync_result.unchanged, 0);

    let active = repo
        .get_active_schema(discovered.source.as_str(), discovered.event_type.as_str())
        .await?;
    assert_eq!(active.id, registered.id);
    assert_eq!(active.content_hash, discovered_entry.content_hash);
    assert_eq!(active.schema_content, discovered_entry.schema_content);
    assert!(active.is_active);
    Ok(())
}
