use super::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_arb_event_source_generates_valid_sources() -> TestResult<()> {
    let mut runner = TestRunner::deterministic();
    for _ in 0..100 {
        let source = arb_event_source()
            .new_tree(&mut runner)
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?
            .current();
        let s = source.as_str();
        assert!(!s.is_empty());
        assert!(s.len() <= 255);
        assert!(s.chars().next().is_some_and(|c| c.is_ascii_lowercase()));
        assert!(
            s.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_')
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_arb_event_type_generates_valid_types() -> TestResult<()> {
    let mut runner = TestRunner::deterministic();
    for _ in 0..100 {
        let event_type = arb_event_type()
            .new_tree(&mut runner)
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?
            .current();
        let s = event_type.as_str();
        assert!(!s.is_empty());
        assert!(s.len() <= 255);
        assert!(s.chars().next().is_some_and(|c| c.is_ascii_lowercase()));
    }
    Ok(())
}

#[sinex_test]
async fn test_arb_uuid_generates_valid_uuids() -> TestResult<()> {
    let mut runner = TestRunner::deterministic();
    for _ in 0..100 {
        let uuid = arb_uuid()
            .new_tree(&mut runner)
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?
            .current();
        assert_eq!(uuid.get_version_num(), 7);
        let parsed =
            Uuid::parse_str(&uuid.to_string()).map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        assert_eq!(parsed, uuid);
    }
    Ok(())
}

#[sinex_test]
async fn test_arb_timestamp_range_has_valid_order() -> TestResult<()> {
    let mut runner = TestRunner::deterministic();
    for _ in 0..100 {
        let (start, end) = arb_timestamp_range()
            .new_tree(&mut runner)
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?
            .current();
        assert!(start < end, "Start should be before end");
        let duration = end - start;
        assert!(duration.whole_seconds() > 0, "Duration should be positive");
    }
    Ok(())
}

#[sinex_test]
async fn test_arb_json_payload_generates_valid_json() -> TestResult<()> {
    let mut runner = TestRunner::deterministic();
    for _ in 0..50 {
        let payload = arb_json_payload()
            .new_tree(&mut runner)
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?
            .current();
        // Should be serializable
        let serialized =
            serde_json::to_string(&payload).map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        // Should be deserializable to the exact same JSON value.
        let deserialized: Value =
            serde_json::from_str(&serialized).map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        assert_eq!(deserialized, payload);
    }
    Ok(())
}
