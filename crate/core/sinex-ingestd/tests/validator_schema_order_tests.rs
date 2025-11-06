use serde_json::json;
use sinex_core::db::repositories::schema_management::NewEventSchema;
use sinex_core::repositories::DbPoolExt;
use sinex_ingestd::validator::{EventValidator, ValidationResult};
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn validator_prefers_latest_semver(ctx: TestContext) -> color_eyre::Result<()> {
    let repo = ctx.pool.schemas();

    sqlx::query!("CREATE EXTENSION IF NOT EXISTS ulid")
        .execute(&ctx.pool)
        .await?;

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
    )?;

    match result {
        ValidationResult::Valid => Ok(()),
        other => Err(color_eyre::eyre::eyre!(
            "expected payload to validate with newest schema, got {:?}",
            other
        )),
    }
}
