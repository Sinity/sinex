use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json;
use sinex_primitives::events::EventPayload;
use xtask::sandbox::prelude::*;

// Test 1: Basic EventPayload derive
#[sinex_test]
fn test_event_payload_derives_correctly() -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(source = "test-source", event_type = "test.event")]
    pub struct TestPayload {
        pub message: String,
        pub count: u32,
    }

    // Verify the trait is implemented
    assert_eq!(TestPayload::SOURCE.as_str(), "test-source");
    assert_eq!(TestPayload::EVENT_TYPE.as_str(), "test.event");
    assert_eq!(TestPayload::VERSION, "1.0.0");
    Ok(())
}

// Test 2: Custom version
#[sinex_test]
fn test_event_payload_custom_version() -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(
        source = "custom-source",
        event_type = "custom.event",
        version = "2.5.0"
    )]
    pub struct CustomVersionPayload {
        pub data: String,
    }

    assert_eq!(CustomVersionPayload::VERSION, "2.5.0");
    Ok(())
}

// Test 3: With optional fields
#[sinex_test]
fn test_event_payload_with_optional_fields() -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(source = "optional-source", event_type = "optional.event")]
    pub struct OptionalPayload {
        pub required_field: String,
        pub optional_field: Option<String>,
    }

    assert_eq!(OptionalPayload::SOURCE.as_str(), "optional-source");
    assert_eq!(OptionalPayload::EVENT_TYPE.as_str(), "optional.event");
    Ok(())
}

// Test 4: Multiple fields with different types
#[sinex_test]
fn test_event_payload_multiple_fields() -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(source = "multi-source", event_type = "multi.event")]
    pub struct MultiFieldPayload {
        pub id: u64,
        pub name: String,
        pub active: bool,
        pub score: f64,
        pub tags: Vec<String>,
    }

    assert_eq!(MultiFieldPayload::SOURCE.as_str(), "multi-source");
    assert_eq!(MultiFieldPayload::EVENT_TYPE.as_str(), "multi.event");
    assert_eq!(MultiFieldPayload::VERSION, "1.0.0");
    Ok(())
}

// Test 5: Serialization of derived payload
#[sinex_test]
fn test_event_payload_serialization() -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(source = "serial-source", event_type = "serial.event")]
    pub struct SerializePayload {
        pub value: String,
    }

    let payload = SerializePayload {
        value: "test".to_string(),
    };

    let json = serde_json::to_string(&payload).expect("Should serialize");
    assert!(json.contains("\"value\""));
    assert!(json.contains("\"test\""));

    let deserialized: SerializePayload = serde_json::from_str(&json).expect("Should deserialize");
    assert_eq!(deserialized.value, "test");
    Ok(())
}

// Test 6: Nested struct in payload
#[sinex_test]
fn test_event_payload_with_nested_struct() -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
    pub struct NestedData {
        pub field1: String,
        pub field2: u32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(source = "nested-source", event_type = "nested.event")]
    pub struct NestedPayload {
        pub data: NestedData,
    }

    assert_eq!(NestedPayload::SOURCE.as_str(), "nested-source");
    assert_eq!(NestedPayload::EVENT_TYPE.as_str(), "nested.event");
    Ok(())
}

// Test 7: Constants are accessible
#[sinex_test]
fn test_event_payload_constants_accessible() -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(source = "const-source", event_type = "const.event")]
    pub struct ConstPayload {
        pub data: String,
    }

    // Constants should be accessible without instantiation
    let source = ConstPayload::SOURCE;
    let event_type = ConstPayload::EVENT_TYPE;
    let version = ConstPayload::VERSION;

    assert_eq!(source.as_str(), "const-source");
    assert_eq!(event_type.as_str(), "const.event");
    assert_eq!(version, "1.0.0");
    Ok(())
}

// Test 8: Multiple payloads with different sources/types
#[sinex_test]
fn test_multiple_event_payloads() -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(source = "source-a", event_type = "event.a")]
    pub struct PayloadA {
        pub data: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(source = "source-b", event_type = "event.b", version = "1.5.0")]
    pub struct PayloadB {
        pub count: u32,
    }

    assert_eq!(PayloadA::SOURCE.as_str(), "source-a");
    assert_eq!(PayloadB::SOURCE.as_str(), "source-b");
    assert_eq!(PayloadA::EVENT_TYPE.as_str(), "event.a");
    assert_eq!(PayloadB::EVENT_TYPE.as_str(), "event.b");
    assert_eq!(PayloadA::VERSION, "1.0.0");
    assert_eq!(PayloadB::VERSION, "1.5.0");
    Ok(())
}

// Test 9: Payload with complex generic types
#[sinex_test]
fn test_event_payload_with_generics() -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(source = "generic-source", event_type = "generic.event")]
    pub struct GenericPayload {
        pub items: Vec<String>,
        pub metadata: std::collections::HashMap<String, String>,
    }

    assert_eq!(GenericPayload::SOURCE.as_str(), "generic-source");
    assert_eq!(GenericPayload::EVENT_TYPE.as_str(), "generic.event");
    Ok(())
}

// Test 10: Minimal payload (single field)
#[sinex_test]
fn test_event_payload_minimal() -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(source = "minimal-source", event_type = "minimal.event")]
    pub struct MinimalPayload {
        pub id: String,
    }

    assert_eq!(MinimalPayload::SOURCE.as_str(), "minimal-source");
    assert_eq!(MinimalPayload::VERSION, "1.0.0");

    let payload = MinimalPayload {
        id: "123".to_string(),
    };
    let json = serde_json::to_string(&payload).expect("Should serialize");
    assert!(json.contains("\"id\""));
    Ok(())
}
