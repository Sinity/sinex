use sinex_primitives::temporal::Duration;
use sinex_primitives::utils::timestamp_helpers::parse_relative_duration;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_parse_relative_duration_basic() -> TestResult<()> {
    assert_eq!(parse_relative_duration("1h"), Some(Duration::hours(1)));
    assert_eq!(parse_relative_duration("2d"), Some(Duration::days(2)));
    assert_eq!(parse_relative_duration("30m"), Some(Duration::minutes(30)));
    assert_eq!(parse_relative_duration("1w"), Some(Duration::weeks(1)));
    assert_eq!(parse_relative_duration("15s"), Some(Duration::seconds(15)));
    Ok(())
}
