use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::types::events::test_helpers::{migrate_payload, test_migration};
use sinex_macros::EventPayload;
use sinex_test_utils::sinex_test;

#[derive(Debug, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "test", event_type = "test.v1", version = "1.0.0")]
struct TestPayloadV1 {
    name: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "test", event_type = "test.v2", version = "2.0.0")]
struct TestPayloadV2 {
    name: String,
    #[serde(default)]
    count: u32,
}

impl From<TestPayloadV1> for TestPayloadV2 {
    fn from(v1: TestPayloadV1) -> Self {
        Self {
            name: v1.name,
            count: 0,
        }
    }
}

#[sinex_test]
fn migration_helpers_upgrade_payloads() -> color_eyre::eyre::Result<()> {
    let v1_json = json!({ "name": "test" });
    test_migration::<TestPayloadV1, TestPayloadV2>(v1_json.clone(), "1.0.0")?;

    let v2 = migrate_payload::<TestPayloadV1, TestPayloadV2>(v1_json, "1.0.0")?;
    assert_eq!(v2.name, "test");
    assert_eq!(v2.count, 0);
    Ok(())
}
