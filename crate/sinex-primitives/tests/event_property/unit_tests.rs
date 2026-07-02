use super::*;

#[sinex_test]
async fn test_event_builder_defaults() -> Result<()> {
    let mut event = test_event(
        EventSource::from_static("test_source"),
        EventType::from_static("test.event"),
        json!({"key": "value"}),
    );
    event.id = Some(Id::from_uuid(Uuid::now_v7()));

    assert_eq!(event.source.as_str(), "test_source");
    assert_eq!(event.event_type.as_str(), "test.event");
    assert_eq!(event.payload, json!({"key": "value"}));
    let ts_orig = event
        .ts_orig
        .expect("test_event should stamp an original timestamp");
    let now = Timestamp::now();
    assert!(ts_orig <= now);
    assert!(now - ts_orig < TimeDuration::seconds(5));
    assert!(!event.host.is_empty()); // Should get hostname
    assert!(event.module_run_id.is_some());
    assert!(event.payload_schema_id.is_none());
    Ok(())
}

#[sinex_test]
async fn test_json_values_equal_function() -> Result<()> {
    // Test exact equality
    assert!(json_values_equal(&json!(42), &json!(42)));
    assert!(json_values_equal(&json!("test"), &json!("test")));
    assert!(json_values_equal(&json!(true), &json!(true)));
    assert!(json_values_equal(&json!(null), &json!(null)));

    // Test floating point tolerance - use a looser tolerance for JSON roundtrip
    assert!(json_values_equal(&json!(1.0), &json!(1.0000001)));
    assert!(!json_values_equal(&json!(1.0), &json!(2.0)));

    // Test nested objects
    let obj1 = json!({"key": "value", "num": 42});
    let obj2 = json!({"key": "value", "num": 42});
    assert!(json_values_equal(&obj1, &obj2));

    // Test arrays
    let arr1 = json!([1, 2, 3]);
    let arr2 = json!([1, 2, 3]);
    assert!(json_values_equal(&arr1, &arr2));
    Ok(())
}

#[sinex_test]
async fn test_arb_generators_produce_valid_values() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::deterministic();

    // Test source name generator
    let source = arb_source_name().new_tree(&mut runner).unwrap().current();
    assert!(!source.is_empty());
    assert!(source.len() <= 52); // 50 + 2 minimum

    // Test event type generator
    let event_type = arb_event_type_name()
        .new_tree(&mut runner)
        .unwrap()
        .current();
    assert!(!event_type.is_empty());

    // Test hostname generator
    let hostname = arb_hostname().new_tree(&mut runner).unwrap().current();
    assert!(!hostname.is_empty());

    // Test version generator
    let version = arb_version().new_tree(&mut runner).unwrap().current();
    assert!(!version.is_empty());
    assert!(version.matches('.').count() >= 2); // At least major.minor.patch

    // Test timestamp generator
    let timestamp = arb_timestamp().new_tree(&mut runner).unwrap().current();
    let now = Timestamp::now();
    assert!(timestamp >= now - TimeDuration::days(366));
    assert!(timestamp <= now + TimeDuration::hours(2));

    Ok(())
}
