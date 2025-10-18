use color_eyre::eyre::eyre;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::types::events::test_helpers::{migrate_payload, test_migration};
use sinex_core::{EventPayload, EventSource, EventType};
use sinex_test_utils::sinex_test;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct TestPayloadV1 {
    name: String,
}

impl EventPayload for TestPayloadV1 {
    const SOURCE: EventSource = EventSource::from_static("test");
    const EVENT_TYPE: EventType = EventType::from_static("test.v1");
    const VERSION: &'static str = "1.0.0";
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct TestPayloadV2 {
    name: String,
    #[serde(default)]
    count: u32,
}

impl EventPayload for TestPayloadV2 {
    const SOURCE: EventSource = EventSource::from_static("test");
    const EVENT_TYPE: EventType = EventType::from_static("test.v2");
    const VERSION: &'static str = "2.0.0";
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
    test_migration::<TestPayloadV1, TestPayloadV2>(v1_json.clone(), "1.0.0")
        .map_err(|err| eyre!("migration failed: {err}"))?;

    let v2 = migrate_payload::<TestPayloadV1, TestPayloadV2>(v1_json, "1.0.0")?;
    assert_eq!(v2.name, "test");
    assert_eq!(v2.count, 0);
    Ok(())
}
