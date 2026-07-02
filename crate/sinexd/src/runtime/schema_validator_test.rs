use super::*;
use serde_json::json;
use xtask::sandbox::prelude::*;

fn simple_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": { "n": { "type": "integer" } },
        "required": ["n"]
    })
}

#[sinex_test]
async fn validate_succeeds_for_registered_schema() -> TestResult<()> {
    let validator = RuntimeSchemaValidator::new();
    let schema_id = Uuid::now_v7();
    validator.register_test_schema(schema_id, "test", "event", &simple_schema())?;

    let id = validator
        .validate("test", "event", &json!({ "n": 1 }))
        .await?;
    assert_eq!(id, schema_id);

    let err = validator
        .validate("test", "event", &json!({ "n": "not-an-int" }))
        .await
        .expect_err("schema-violating payload must be rejected");
    assert!(err.to_string().contains("Schema validation failed"));
    Ok(())
}

// Regression for the schema-cache split-write race: the validator now holds
// both read guards together, so a lookup key whose compiled schema is absent
// is a genuine inconsistency (no longer a transient window). validate() must
// surface a clean error rather than panic or mis-route.
#[sinex_test]
async fn validate_reports_inconsistency_for_orphan_lookup_entry() -> TestResult<()> {
    let validator = RuntimeSchemaValidator::new();
    let schema_id = Uuid::now_v7();
    validator.register_test_schema(schema_id, "test", "event", &simple_schema())?;

    // Drop the compiled schema while leaving the lookup entry in place.
    validator.schemas.write().remove(&schema_id);

    let err = validator
        .validate("test", "event", &json!({ "n": 1 }))
        .await
        .expect_err("orphan lookup entry must yield an inconsistency error");
    assert!(err.to_string().contains("inconsistent"));
    Ok(())
}
