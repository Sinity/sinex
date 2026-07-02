use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_seed_clock_advances() -> ::xtask::sandbox::TestResult<()> {
    let clock = SeedClock::new();
    let t1 = clock.tick(100);
    let t2 = clock.tick(100);
    assert!(t2 > t1);
    Ok(())
}

#[sinex_test]
async fn test_event_spec_builder() -> ::xtask::sandbox::TestResult<()> {
    let spec = EventSpec::new("source", "type").with_payload(json!({"key": "value"}));
    assert_eq!(spec.source, "source");
    assert_eq!(spec.event_type, "type");
    assert_eq!(spec.payload["key"], "value");
    Ok(())
}

#[sinex_test]
async fn test_event_spec_from_typed_captures_source_and_type()
-> ::xtask::sandbox::TestResult<()> {
    let spec = EventSpec::from_typed(&FileCreatedPayload::test_default("/test"))?;
    assert_eq!(spec.source, FileCreatedPayload::SOURCE.as_static_str());
    assert_eq!(
        spec.event_type,
        FileCreatedPayload::EVENT_TYPE.as_static_str()
    );
    // Typed payload serializes with correct structure
    assert!(spec.payload.get("path").is_some());
    assert!(spec.payload.get("size").is_some());
    assert!(spec.payload.get("created_at").is_some());
    Ok(())
}

#[sinex_test]
async fn test_analytics_dataset_semantic_min_uses_typed_payloads()
-> ::xtask::sandbox::TestResult<()> {
    let dataset = AnalyticsDataset::semantic_min()?;
    assert_eq!(dataset.expected_total, 5);
    // Shell commands should have correct source from KittyCommandExecutedPayload
    assert_eq!(dataset.expected_source_counts.get("shell.kitty"), Some(&3));
    assert_eq!(
        dataset
            .expected_source_counts
            .get(FileCreatedPayload::SOURCE.as_static_str()),
        Some(&2)
    );
    Ok(())
}
