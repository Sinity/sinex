use super::*;

fn ts(unix: i64) -> Timestamp {
    Timestamp::from_unix_secs(unix).unwrap()
}

fn midnight_settings() -> TimeSettings {
    TimeSettings::new("Europe/Warsaw", 0, 0).unwrap()
}

#[devkit::test]
fn spring_forward_day_is_23_hours() {
    let c = midnight_settings();
    // 2024-03-31 noon local.
    let day = c.day(ts(1_711_879_200)).unwrap();
    let len = c.day_end(day).unwrap().unix_ns() - c.day_start(day).unwrap().unix_ns();
    assert_eq!(len, 23 * 3_600_000_000_000);
}

#[devkit::test]
fn fall_back_day_is_25_hours() {
    let c = midnight_settings();
    let day = c.day(ts(1_730_026_800)).unwrap();
    // Local midnight 2024-10-27 00:00 CEST is 2024-10-26 22:00 UTC.
    assert_eq!(c.day_start(day).unwrap(), ts(1_729_980_000));
    let len = c.day_end(day).unwrap().unix_ns() - c.day_start(day).unwrap().unix_ns();
    assert_eq!(len, 25 * 3_600_000_000_000);
}

#[devkit::test]
fn repeated_hour_at_fall_back_is_two_buckets() {
    let c = midnight_settings();
    // 2024-10-27: 02:30 CEST is 00:30 UTC; 02:30 CET is 01:30 UTC.
    let first = c.hour_start(ts(1_729_989_000)).unwrap();
    let second = c.hour_start(ts(1_729_992_600)).unwrap();
    assert_eq!(first, ts(1_729_987_200), "02:00 CEST is 00:00 UTC");
    assert_eq!(second, ts(1_729_990_800), "02:00 CET is 01:00 UTC");
    assert_eq!(c.hour_end(first).unwrap(), second);
}

#[devkit::test]
fn late_evening_stays_in_its_local_day() {
    let c = midnight_settings();
    // 2024-06-15 23:30 CEST.
    assert_eq!(
        c.day(ts(1_718_487_000)).unwrap(),
        jiff::civil::date(2024, 6, 15)
    );
    assert_eq!(c.hour_start(ts(1_718_455_040)).unwrap(), ts(1_718_452_800));
}

#[devkit::test]
fn logical_day_starts_at_four() {
    let c = TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting");
    // 2024-06-15 03:59 CEST belongs to the 14th; 04:00 to the 15th.
    assert_eq!(
        c.day(ts(1_718_416_740)).unwrap(),
        jiff::civil::date(2024, 6, 14)
    );
    assert_eq!(
        c.day(ts(1_718_416_800)).unwrap(),
        jiff::civil::date(2024, 6, 15)
    );
    assert_eq!(
        c.day_start(jiff::civil::date(2024, 6, 15)).unwrap(),
        ts(1_718_416_800)
    );
}

#[devkit::test]
fn unknown_zone_is_an_error_not_utc() {
    assert!(matches!(
        TimeSettings::new("Not/A_Zone", 4, 0),
        Err(TimeSettingsError::UnknownZone(_))
    ));
    assert!(matches!(
        TimeSettings::new("UTC", 24, 0),
        Err(TimeSettingsError::BadDayStart(24, 0))
    ));
}

#[test]
fn parsed_settings_keep_the_exact_day_start_and_zone() {
    let settings = TimeSettings::from_parts(
        jiff::tz::TimeZone::UTC,
        jiff::civil::Time::constant(6, 30, 15, 0),
    );
    let before = Timestamp::from_unix_secs(1_718_433_014).unwrap();
    let after = Timestamp::from_unix_secs(1_718_433_015).unwrap();
    assert_ne!(settings.day(before).unwrap(), settings.day(after).unwrap());
    assert_eq!(
        settings.day_start(settings.day(after).unwrap()).unwrap(),
        after
    );
}
