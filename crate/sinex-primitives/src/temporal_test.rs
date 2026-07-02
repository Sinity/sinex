use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn parse_duration_accepts_legacy_compact_units() -> TestResult<()> {
    assert_eq!(parse_duration("1h"), Some(Duration::hours(1)));
    assert_eq!(parse_duration("30m"), Some(Duration::minutes(30)));
    assert_eq!(parse_duration("2d"), Some(Duration::days(2)));
    assert_eq!(parse_duration("1w"), Some(Duration::weeks(1)));
    Ok(())
}

#[sinex_test]
async fn parse_duration_accepts_human_duration_grammar() -> TestResult<()> {
    assert_eq!(
        parse_duration("1 hour 30 minutes"),
        Some(Duration::minutes(90))
    );
    assert_eq!(parse_duration("500ms"), Some(Duration::milliseconds(500)));
    Ok(())
}

#[sinex_test]
async fn parse_duration_rejects_invalid_input() -> TestResult<()> {
    assert_eq!(parse_duration(""), None);
    assert_eq!(parse_duration("invalid"), None);
    Ok(())
}
