use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn format_bytes_uses_binary_units() -> xtask::sandbox::TestResult<()> {
    assert_eq!(format_bytes(999), "999 B");
    assert_eq!(format_bytes(1536), "1.5 KiB");
    assert_eq!(format_bytes(10 * 1024), "10 KiB");
    Ok(())
}

#[sinex_test]
async fn format_duration_age_keeps_compact_age_shape() -> xtask::sandbox::TestResult<()> {
    assert_eq!(format_duration_age(time::Duration::seconds(62)), "1m2s ago");
    assert_eq!(
        format_duration_age(time::Duration::seconds(3660)),
        "1h1m ago"
    );
    Ok(())
}

#[sinex_test]
async fn format_duration_compact_secs_matches_report_shape() -> xtask::sandbox::TestResult<()> {
    assert_eq!(format_duration_compact_secs(47), "47s");
    assert_eq!(format_duration_compact_secs(120), "2m");
    assert_eq!(format_duration_compact_secs(198 * 60), "3h 18m");
    Ok(())
}
