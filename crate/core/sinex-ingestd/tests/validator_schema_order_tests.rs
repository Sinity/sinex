use serde_json::json;
use sinex_db::repositories::schema_management::NewEventSchema;
use sinex_db::repositories::DbPoolExt;
use sinex_ingestd::validator::{EventValidator, ValidationResult};
use xtask::sandbox::{sinex_test, TestContext};

#[sinex_test]
async fn validator_prefers_latest_semver(ctx: TestContext) -> color_eyre::Result<()> {
    let repo = ctx.pool.schemas();

    ensure_ulid_extension(&ctx.pool).await?;

    repo.register_schema(NewEventSchema {
        source: "semver-source".to_string(),
        event_type: "semver.event".to_string(),
        schema_version: "1.9.9".to_string(),
        schema_content: json!({
            "type": "object",
            "properties": { "legacy": { "type": "string" } },
            "required": ["legacy"]
        }),
    })
    .await?;

    repo.register_schema(NewEventSchema {
        source: "semver-source".to_string(),
        event_type: "semver.event".to_string(),
        schema_version: "1.10.0".to_string(),
        schema_content: json!({
            "type": "object",
            "properties": {
                "modern": { "type": "string" }
            },
            "required": []
        }),
    })
    .await?;

    let validator = EventValidator::load_schemas_from_db(&ctx.pool, true).await?;
    let result = validator.validate_payload_for(
        "semver-source",
        "semver.event",
        &json!({ "modern": "value" }),
    );

    match result {
        ValidationResult::Valid => Ok(()),
        other => Err(color_eyre::eyre::eyre!(
            "expected payload to validate with newest schema, got {:?}",
            other
        )),
    }
}

async fn ensure_ulid_extension(pool: &sinex_db::DbPool) -> color_eyre::Result<()> {
    let available = sqlx::query_scalar::<_, String>(
        "SELECT name FROM pg_available_extensions WHERE name IN ('ulid', 'pgx_ulid') LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;

    let Some(extension) = available else {
        return Err(color_eyre::eyre::eyre!(
            "Neither 'ulid' nor 'pgx_ulid' extensions are available in this PostgreSQL instance"
        ));
    };

    let stmt = format!(r#"CREATE EXTENSION IF NOT EXISTS "{extension}""#);
    sqlx::query(&stmt).execute(pool).await?;

    Ok(())
}

#[sinex_test]
async fn validator_handles_double_digit_versions(ctx: TestContext) -> color_eyre::Result<()> {
    let repo = ctx.pool.schemas();

    repo.register_schema(NewEventSchema {
        source: "digit-source".to_string(),
        event_type: "digit.event".to_string(),
        schema_version: "9.0.0".to_string(),
        schema_content: json!({
            "type": "object",
            "properties": { "legacy": { "type": "string" } },
            "required": ["legacy"]
        }),
    })
    .await?;

    repo.register_schema(NewEventSchema {
        source: "digit-source".to_string(),
        event_type: "digit.event".to_string(),
        schema_version: "10.0.0".to_string(),
        schema_content: json!({
            "type": "object",
            "properties": { "modern": { "type": "string" } },
            "required": []
        }),
    })
    .await?;

    let validator = EventValidator::load_schemas_from_db(&ctx.pool, true).await?;
    let result = validator.validate_payload_for(
        "digit-source",
        "digit.event",
        &json!({ "modern": "value" }),
    );

    match result {
        ValidationResult::Valid => Ok(()),
        other => Err(color_eyre::eyre::eyre!(
            "expected payload to validate with double-digit version schema, got {:?}",
            other
        )),
    }
}
