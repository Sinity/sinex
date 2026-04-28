use serde_json::json;
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::{EventQuery, SinexError, TimeRange};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_time_range_rejects_equal_bounds() -> TestResult<()> {
    let now = Timestamp::now();
    assert!(TimeRange::new(Some(now), Some(now)).is_err());
    Ok(())
}

#[sinex_test]
async fn test_event_query_validate_rejects_deserialized_inverted_time_range() -> TestResult<()> {
    let now = Timestamp::now();
    let earlier = now - Duration::hours(1);

    let mut query: EventQuery = serde_json::from_value(json!({
        "time_range": {
            "start": now,
            "end": earlier
        }
    }))?;

    let error = query
        .validate()
        .expect_err("invalid deserialized time range should fail");
    // Match on the error variant rather than a substring of the message string,
    // so this assertion is stable across message rewording.
    assert!(
        matches!(error, SinexError::Validation(_)),
        "expected SinexError::Validation, got: {error:?}"
    );
    Ok(())
}
