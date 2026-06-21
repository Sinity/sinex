use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_primitives::events::EventPayload;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, MaterialLifecyclePolicy, OccurrenceIdentity,
    PrivacyTier, ResourceProfile, RetentionPolicy, RunnerPack, RuntimeShape, TransportKind,
    TransportSemantics, source_runtime_bindings,
};
use xtask::sandbox::prelude::*;

// Test 1: Basic EventPayload derive
#[sinex_test]
async fn test_event_payload_derives_correctly() -> TestResult<()> {
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
async fn test_event_payload_custom_version() -> TestResult<()> {
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
async fn test_event_payload_with_optional_fields() -> TestResult<()> {
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
async fn test_event_payload_multiple_fields() -> TestResult<()> {
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
async fn test_event_payload_serialization() -> TestResult<()> {
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
async fn test_event_payload_with_nested_struct() -> TestResult<()> {
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
async fn test_event_payload_constants_accessible() -> TestResult<()> {
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
async fn test_multiple_event_payloads() -> TestResult<()> {
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
async fn test_event_payload_with_generics() -> TestResult<()> {
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
async fn test_event_payload_minimal() -> TestResult<()> {
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

// Test 11: Generic payloads preserve impl generics
#[sinex_test]
async fn test_event_payload_const_generic() -> TestResult<()> {
    use std::marker::PhantomData;

    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[event_payload(source = "generic-source", event_type = "generic.const-array")]
    pub struct ConstGenericPayload<const N: usize> {
        pub chunk_count: usize,
        #[serde(skip)]
        #[schemars(skip)]
        pub marker: PhantomData<[u8; N]>,
    }

    assert_eq!(ConstGenericPayload::<4>::SOURCE.as_str(), "generic-source");
    assert_eq!(
        ConstGenericPayload::<4>::EVENT_TYPE.as_str(),
        "generic.const-array"
    );
    assert_eq!(ConstGenericPayload::<4>::VERSION, "1.0.0");

    Ok(())
}

#[sinex_test]
async fn test_event_payload_enum_derives_schema_contract() -> TestResult<()> {
    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
    #[serde(untagged)]
    #[event_payload(source = "enum-source", event_type = "enum.event")]
    pub enum EnumPayload {
        Text { value: String },
        Count { count: u64 },
    }

    assert_eq!(EnumPayload::SOURCE.as_str(), "enum-source");
    assert_eq!(EnumPayload::EVENT_TYPE.as_str(), "enum.event");
    assert_eq!(EnumPayload::VERSION, "1.0.0");

    let payload = EnumPayload::Count { count: 42 };
    let json = serde_json::to_value(payload)?;
    assert_eq!(
        json.get("count").and_then(serde_json::Value::as_u64),
        Some(42)
    );
    Ok(())
}

// Proves the SourceRecord derive output resolves its async-trait dependency
// through sinex-primitives (`__sinex_macros_reexport::async_trait`) rather than
// requiring each consumer crate to declare async-trait. This crate does not
// depend on sinexd; a raw-line MaterialParser deriving and running here is the
// evidence the generated parser contract is primitives-backed.
#[sinex_test]
async fn test_source_record_derive_uses_primitives_parser_contract() -> TestResult<()> {
    use sinex_primitives::Uuid;
    use sinex_primitives::events::SourceMaterial;
    use sinex_primitives::ids::Id;
    use sinex_primitives::parser::{
        MaterialAnchor, MaterialParser, ParserContext, SourceId, SourceRecord,
    };
    use sinex_primitives::temporal::Timestamp;

    #[derive(Default, sinex_macros::SourceRecord)]
    #[source_record(
        id = "macro-raw-line",
        source_id = "test.raw-line",
        input_shape = "raw_line",
        event_source = "test",
        event_type = "test.raw"
    )]
    struct MacroRawLineRecord {
        #[source(raw_line)]
        #[required]
        #[allow(dead_code)]
        message: String,
    }

    let mut parser = MacroRawLineRecord::default();
    let manifest = parser.manifest();
    assert_eq!(manifest.parser_id.as_str(), "macro-raw-line");
    assert_eq!(manifest.source_id.as_str(), "test.raw-line");

    let material_id = Id::<SourceMaterial>::from_uuid(Uuid::nil());
    let anchor = MaterialAnchor::Line {
        byte_start: 0,
        line: 1,
    };
    let ctx = ParserContext {
        source_id: SourceId::from_static("test.raw-line"),
        source_material_id: material_id,
        record_anchor: anchor.clone(),
        operation_id: Uuid::nil(),
        job_id: Uuid::nil(),
        host: "test-host".to_string(),
        acquisition_time: Timestamp::now(),
    };
    let record = SourceRecord {
        material_id,
        anchor,
        bytes: b"hello from primitives".to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };

    let intents = parser.parse_record(record, &ctx).await?;
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_source.as_str(), "test");
    assert_eq!(intents[0].event_type.as_str(), "test.raw");
    assert_eq!(intents[0].payload["message"], "hello from primitives");

    Ok(())
}

#[sinex_test]
async fn test_source_meta_primary_subject_override() -> TestResult<()> {
    #[derive(Default, sinex_macros::SourceMeta)]
    #[source_meta(
        id = "macro.subject-package",
        namespace = "macro",
        subject = "source:macro.subject-package.mode",
        event_source = "macro.subject",
        event_type = "macro.subject.observed",
        adapter = "FileContentDropAdapter",
        implementation = "macro-subject-test",
        privacy_tier = PrivacyTier::Sensitive,
        horizons(Horizon::Historical),
        retention = RetentionPolicy::Forever,
        occurrence_identity = OccurrenceIdentity::Uuid5From("(material_id, row)"),
        access_scope = AccessScope::StagedExport,
        privacy_context = sinex_primitives::privacy::ProcessingContext::Document,
        resource_profile = ResourceProfile::BoundedFile,
        runner_pack = RunnerPack::Staged,
        checkpoint_family = CheckpointFamily::AppendStream,
        runtime_shape = RuntimeShape::Scheduled,
        material_lifecycle = MaterialLifecyclePolicy::ExternalReferenceOnly,
        transport_semantics = TransportSemantics::JETSTREAM_DURABLE,
        factory = "none"
    )]
    struct MacroSubjectPackage;

    let _ = MacroSubjectPackage;
    let binding = source_runtime_bindings()
        .into_iter()
        .find(|binding| binding.id == "macro.subject-package")
        .expect("SourceMeta should register runtime binding");

    assert_eq!(
        binding.subject.as_str(),
        "source:macro.subject-package.mode"
    );
    assert_eq!(binding.source_id, "macro.subject-package");
    assert_eq!(binding.output_event_type, "macro.subject.observed");
    assert_eq!(
        binding.material_lifecycle,
        MaterialLifecyclePolicy::ExternalReferenceOnly
    );
    assert_eq!(
        binding.transport_semantics.transport,
        TransportKind::JetStream
    );
    assert!(binding.transport_semantics.replayable);
    assert!(binding.transport_semantics.dlq);

    Ok(())
}
