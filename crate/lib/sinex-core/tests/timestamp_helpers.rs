use chrono::TimeZone;
use sinex_core::types::utils::timestamp_helpers::{
    parse_flexible_timestamp, timestamp_nanos_to_datetime, timestamp_to_datetime,
    timestamp_with_nanos_to_datetime,
};
use xtask::sandbox::sinex_test;

#[sinex_test]
fn timestamp_conversions_cover_common_units() -> TestResult<()> {
    let dt = timestamp_to_datetime(1_700_000_000).unwrap();
    assert_eq!(dt.timestamp(), 1_700_000_000);

    let dt = timestamp_with_nanos_to_datetime(1_700_000_000, 123_456_789).unwrap();
    assert_eq!(dt.timestamp(), 1_700_000_000);
    assert_eq!(dt.timestamp_subsec_nanos(), 123_456_789);

    let timestamp_ns = 1_700_000_000_123_456_789_i64;
    let dt = timestamp_nanos_to_datetime(timestamp_ns).unwrap();
    assert_eq!(dt.timestamp(), 1_700_000_000);
    assert_eq!(dt.timestamp_subsec_nanos(), 123_456_789);
    Ok(())
}

#[sinex_test]
fn flexible_parsing_handles_strings_and_numbers() -> TestResult<()> {
    let dt = parse_flexible_timestamp("2023-11-14T12:00:00Z").unwrap();
    assert_eq!(
        dt,
        chrono::Utc
            .with_ymd_and_hms(2023, 11, 14, 12, 0, 0)
            .unwrap()
    );

    let dt = parse_flexible_timestamp("1700000000").unwrap();
    assert_eq!(dt.timestamp(), 1_700_000_000);

    let dt = parse_flexible_timestamp("5000000001").unwrap();
    assert_eq!(dt.timestamp(), 5_000_000_001);

    let dt = parse_flexible_timestamp("1700000000000").unwrap();
    assert_eq!(dt.timestamp_millis(), 1_700_000_000_000);
    Ok(())
}
