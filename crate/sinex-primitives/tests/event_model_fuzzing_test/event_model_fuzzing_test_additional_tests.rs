use super::*;
use std::panic;

#[sinex_test]
async fn test_event_source_rejects_null_bytes() -> TestResult<()> {
    // Source and event_type with null bytes are now rejected at construction
    assert!(EventSource::new("test\0null\0bytes").is_err());
    assert!(EventType::new("test\0event").is_err());

    // Null bytes in payload are still tolerated by serialization, but hostnames are validated.
    let mut event = event_fixture(
        EventSource::from_static("test"),
        EventType::from_static("test.event"),
        serde_json::json!({"null_bytes": "test\0data"}),
    );
    assert!(HostName::new("test\0host".to_string()).is_err());
    event.id = Some(Id::from_uuid(Uuid::now_v7()));

    // Serialization should still work for payloads with null bytes and preserve the payload.
    let json_str = serde_json::to_string(&event)?;
    let decoded = serde_json::from_str::<Event<JsonValue>>(&json_str)?;
    assert_eq!(decoded.payload, event.payload);

    Ok(())
}

#[sinex_test]
async fn test_event_with_extremely_large_payload() -> TestResult<()> {
    // Create a very large payload (10MB of data)
    let large_string = "x".repeat(10_000_000);
    let large_payload = serde_json::json!({
        "huge_field": large_string,
        "nested": {
            "also_huge": "y".repeat(5_000_000)
        }
    });

    let mut event = event_fixture(
        EventSource::from_static("test"),
        EventType::from_static("test.large"),
        large_payload,
    );
    event.id = Some(Id::from_uuid(Uuid::now_v7()));

    // Large payloads should remain serializable instead of disappearing behind a panic-only smoke test.
    let json_str = serde_json::to_string(&event)?;
    let decoded = serde_json::from_str::<Event<JsonValue>>(&json_str)?;
    assert_eq!(decoded.event_type.as_str(), event.event_type.as_str());
    assert_eq!(
        decoded.payload["huge_field"].as_str().map(str::len),
        Some(10_000_000)
    );

    Ok(())
}

#[sinex_test]
async fn test_event_with_infinite_numbers() -> TestResult<()> {
    let payload = serde_json::json!({
        "infinity": f64::INFINITY,
        "neg_infinity": f64::NEG_INFINITY,
        "nan": f64::NAN,
        "very_large": f64::MAX,
        "very_small": f64::MIN,
    });

    let mut event = event_fixture(
        EventSource::from_static("test"),
        EventType::from_static("test.numbers"),
        payload,
    );
    event.id = Some(Id::from_uuid(Uuid::now_v7()));

    // Special float payload values should remain serializable after serde_json normalization.
    let json_str = serde_json::to_string(&event)?;
    let decoded = serde_json::from_str::<Event<JsonValue>>(&json_str)?;
    assert_eq!(decoded.payload, event.payload);

    Ok(())
}

#[sinex_test]
async fn test_panic_safety_with_catch_unwind() -> TestResult<()> {
    // Verify that invalid source/event_type are rejected, not panicking
    assert!(EventSource::new("\x00\x01\x02").is_err());
    assert!(EventType::new("💀🔥test").is_err());

    // Test that valid source/type with problematic payload doesn't panic
    let result = panic::catch_unwind(|| {
        let mut event = event_fixture(
            EventSource::from_static("test"),
            EventType::from_static("test.event"),
            serde_json::json!({
                "🔥": "💀",
                "\x00": "\x01",
                "nested": {
                    "💀": [1, 2, 3, f64::INFINITY]
                }
            }),
        );
        assert!(HostName::new("🦀".to_string()).is_err());
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        // Test JSON serialization and payload preservation.
        let json_str = serde_json::to_string(&event)
            .expect("pathological payload event should serialize to JSON");
        let decoded = serde_json::from_str::<Event<JsonValue>>(&json_str)
            .expect("pathological payload event should deserialize from JSON");
        assert_eq!(decoded.payload, event.payload);

        // Test field access
        let _ = event.source.as_str();
        let _ = event.event_type.as_str();
        let _ = event.host.as_str();
    });

    // This should not panic.
    assert!(
        result.is_ok(),
        "valid event construction and serialization path should not panic"
    );
    Ok(())
}
