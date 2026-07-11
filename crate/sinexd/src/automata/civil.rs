//! Operator-local civil-time bucketing for calendar-shaped derived products
//! (sinex-2ged).
//!
//! `ts_orig` stays a UTC instant everywhere (storage/identity untouched). Civil
//! time is a bucketing/rendering concern parameterized by a single declared
//! operator timezone (`SINEX_LOCAL_TZ`, default `Europe/Warsaw`), with DST-aware
//! day/hour boundaries: a spring-forward day is 23h, a fall-back day is 25h, and
//! hour-of-week baselines keyed on civil hour do not smear across a transition.
//!
//! Without this, hourly/daily summaries bucket in UTC, so the operator's civil
//! "day" boundary sat at 01:00–02:00 local and late-evening work landed in the
//! next civil day.

use std::sync::LazyLock;

use jiff::{ToSpan, Timestamp as JiffTimestamp, Zoned};
use sinex_primitives::temporal::Timestamp;
use time::OffsetDateTime;

/// The single declared operator timezone (IANA name). One source of truth for
/// every civil bucket.
static OPERATOR_TZ: LazyLock<String> =
    LazyLock::new(|| std::env::var("SINEX_LOCAL_TZ").unwrap_or_else(|_| "Europe/Warsaw".to_string()));

/// Operator timezone IANA name (e.g. `Europe/Warsaw`).
#[must_use]
pub fn operator_tz() -> &'static str {
    OPERATOR_TZ.as_str()
}

fn to_zoned(ts: Timestamp) -> Option<Zoned> {
    JiffTimestamp::from_nanosecond(ts.inner().unix_timestamp_nanos())
        .ok()?
        .in_tz(operator_tz())
        .ok()
}

fn from_zoned(zoned: &Zoned, fallback: Timestamp) -> Timestamp {
    OffsetDateTime::from_unix_timestamp_nanos(zoned.timestamp().as_nanosecond())
        .map(Timestamp::from)
        .unwrap_or(fallback)
}

/// Floor to the start of the operator-local civil hour (DST-aware), returned as
/// a UTC instant. Falls back to the input unchanged if the timezone or instant
/// cannot be resolved.
#[must_use]
pub fn floor_to_civil_hour(ts: Timestamp) -> Timestamp {
    let Some(zoned) = to_zoned(ts) else {
        return ts;
    };
    let Ok(floored) = zoned
        .datetime()
        .with()
        .minute(0)
        .second(0)
        .subsec_nanosecond(0)
        .build()
    else {
        return ts;
    };
    match floored.in_tz(operator_tz()) {
        Ok(zoned) => from_zoned(&zoned, ts),
        Err(_) => ts,
    }
}

/// Floor to the start of the operator-local civil day (midnight, DST-aware).
#[must_use]
pub fn floor_to_civil_day(ts: Timestamp) -> Timestamp {
    let Some(zoned) = to_zoned(ts) else {
        return ts;
    };
    let Ok(floored) = zoned
        .datetime()
        .with()
        .hour(0)
        .minute(0)
        .second(0)
        .subsec_nanosecond(0)
        .build()
    else {
        return ts;
    };
    match floored.in_tz(operator_tz()) {
        Ok(zoned) => from_zoned(&zoned, ts),
        Err(_) => ts,
    }
}

/// End of the civil hour that starts at `hour_start` (== next civil hour start).
#[must_use]
pub fn civil_hour_end(hour_start: Timestamp) -> Timestamp {
    let Some(zoned) = to_zoned(hour_start) else {
        return hour_start;
    };
    match zoned.checked_add(1.hour()) {
        Ok(zoned) => from_zoned(&zoned, hour_start),
        Err(_) => hour_start,
    }
}

/// End of the civil day that starts at `day_start` (== next civil midnight; 23h
/// or 25h across a DST transition, exactly one calendar day).
#[must_use]
pub fn civil_day_end(day_start: Timestamp) -> Timestamp {
    // The next civil midnight, computed directly from the calendar date (the next
    // day at 00:00 local) so DST transitions produce exactly 23h/24h/25h days
    // without relying on instant re-conversion.
    let Some(zoned) = to_zoned(day_start) else {
        return day_start;
    };
    let Ok(tomorrow) = zoned.date().tomorrow() else {
        return day_start;
    };
    match tomorrow.at(0, 0, 0, 0).in_tz(operator_tz()) {
        Ok(zoned) => from_zoned(&zoned, day_start),
        Err(_) => day_start,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(unix: i64) -> Timestamp {
        Timestamp::from_unix_timestamp(unix).expect("valid timestamp")
    }

    #[test]
    fn spring_forward_civil_day_is_23h() {
        // Europe/Warsaw 2024-03-31: clocks jump 02:00 -> 03:00 (CET->CEST), so the
        // civil day is 23 hours. Instant = 2024-03-31 10:00 UTC (noon local).
        let noon = ts(1_711_879_200);
        let day_start = floor_to_civil_day(noon);
        let day_end = civil_day_end(day_start);
        assert_eq!((day_end.inner() - day_start.inner()).whole_hours(), 23);
    }

    #[test]
    fn fall_back_civil_day_is_25h() {
        // Europe/Warsaw 2024-10-27: clocks fall 03:00 -> 02:00 (CEST->CET), civil
        // day is 25 hours. Instant = 2024-10-27 11:00 UTC (13:00->noon local).
        let noon = ts(1_730_026_800);
        let day_start = floor_to_civil_day(noon);
        // Local midnight 2024-10-27 00:00 CEST == 2024-10-26 22:00 UTC.
        assert_eq!(day_start, ts(1_729_980_000), "fall-back local midnight");
        let day_end = civil_day_end(day_start);
        assert_eq!((day_end.inner() - day_start.inner()).whole_hours(), 25);
    }

    #[test]
    fn civil_day_floors_to_local_midnight_not_utc() {
        // 2024-06-15 23:30 CEST (== 21:30 UTC) floors to the SAME civil day's local
        // midnight (2024-06-15 00:00 CEST == 2024-06-14 22:00 UTC), not UTC midnight
        // (which would fall into the next civil day).
        let late_evening_local = ts(1_718_487_000);
        assert_eq!(floor_to_civil_day(late_evening_local), ts(1_718_402_400));
    }

    #[test]
    fn civil_hour_floors_to_local_hour() {
        // 2024-06-15 14:37:20 CEST (== 12:37:20 UTC) floors to 14:00 CEST (12:00 UTC).
        assert_eq!(floor_to_civil_hour(ts(1_718_455_040)), ts(1_718_452_800));
    }
}
