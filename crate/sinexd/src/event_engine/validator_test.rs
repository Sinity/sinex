use super::*;
use serde_json::json;
use sinex_primitives::Id;
use sinex_primitives::events::{DynamicPayload, SourceMaterial};
use xtask::sandbox::sinex_test;

fn event_without_registered_schema() -> sinex_db::models::Event<JsonValue> {
    DynamicPayload::new("validator-test", "validator.test", json!({ "ok": true }))
        .from_material(Id::<SourceMaterial>::new())
        .build()
        .expect("test event should build")
}

#[sinex_test]
async fn validate_event_preserves_no_schema_result() -> xtask::sandbox::TestResult<()> {
    let validator = IngestEventValidator::new(true);
    let result = validator.validate_event(&event_without_registered_schema());

    assert!(
        matches!(result, ValidationResult::NoSchema),
        "validate_event must not fabricate a nil schema UUID when no schema is registered: {result:?}"
    );
    Ok(())
}
