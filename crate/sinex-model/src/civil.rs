//! Civil time in the operator's zone (`2ged`). `ts_orig` stays a UTC instant
//! everywhere; civil time is only for bucketing and rendering: hours, and
//! logical days that start at a configured local time (04:00 by default, one
//! setting). Days across a DST change are 23 or 25 hours long.
//!
//! Hours are floored with the instant's own UTC offset, so the repeated hour
//! at the autumn change (02:00–03:00 twice in Europe/Warsaw) gives two
//! distinct buckets. The predecessor floored the local clock time and resolved
//! it again, which put the second 02:xx into the first 02:00 bucket.
use crate::Timestamp;
use jiff::civil::{Date, DateTime, Time};
use jiff::tz::{AmbiguousOffset, TimeZone};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CivilError {
    #[error("unknown time zone {0}")]
    UnknownZone(String),
    #[error("day start {0:02}:{1:02} is not a time of day")]
    BadDayStart(u8, u8),
    #[error("{0} is outside the supported time range")]
    OutOfRange(String),
}

/// The operator's zone and logical-day start.
#[derive(Clone, Debug)]
pub struct Civil {
    zone: TimeZone,
    zone_name: String,
    day_starts_at: Time,
}

impl Civil {
    pub fn new(zone: &str, day_start_hour: u8, day_start_minute: u8) -> Result<Self, CivilError> {
        let tz = TimeZone::get(zone).map_err(|_| CivilError::UnknownZone(zone.to_owned()))?;
        let day_starts_at = Time::new(
            i8::try_from(day_start_hour)
                .map_err(|_| CivilError::BadDayStart(day_start_hour, day_start_minute))?,
            i8::try_from(day_start_minute)
                .map_err(|_| CivilError::BadDayStart(day_start_hour, day_start_minute))?,
            0,
            0,
        )
        .map_err(|_| CivilError::BadDayStart(day_start_hour, day_start_minute))?;
        Ok(Self {
            zone: tz,
            zone_name: zone.to_owned(),
            day_starts_at,
        })
    }

    /// The operator's setting today: Europe/Warsaw, days from 04:00.
    pub fn operator_default() -> Self {
        Self::new("Europe/Warsaw", 4, 0).expect("the bundled tz database has Europe/Warsaw")
    }

    pub fn zone_name(&self) -> &str {
        &self.zone_name
    }

    fn instant(t: Timestamp) -> Result<jiff::Timestamp, CivilError> {
        jiff::Timestamp::from_nanosecond(t.unix_ns())
            .map_err(|_| CivilError::OutOfRange(format!("{t:?}")))
    }

    fn back(t: jiff::Timestamp) -> Result<Timestamp, CivilError> {
        Timestamp::from_unix_ns(t.as_nanosecond())
            .map_err(|_| CivilError::OutOfRange(t.to_string()))
    }

    /// Resolve a local date-time to an instant. In a fold, `prefer` picks the
    /// candidate with that offset; otherwise the earlier one. A gap resolves
    /// to the instant just after it.
    fn resolve(
        &self,
        dt: DateTime,
        prefer: Option<jiff::tz::Offset>,
    ) -> Result<Timestamp, CivilError> {
        let ambiguous = self.zone.to_ambiguous_timestamp(dt);
        let offset = match ambiguous.offset() {
            AmbiguousOffset::Unambiguous { offset } => offset,
            AmbiguousOffset::Fold { before, after } => match prefer {
                Some(p) if p == after => after,
                _ => before,
            },
            AmbiguousOffset::Gap { after, .. } => after,
        };
        let t = offset
            .to_timestamp(dt)
            .map_err(|_| CivilError::OutOfRange(dt.to_string()))?;
        Self::back(t)
    }

    /// Start of the civil hour containing `t`.
    pub fn hour_start(&self, t: Timestamp) -> Result<Timestamp, CivilError> {
        let zoned = Self::instant(t)?.to_zoned(self.zone.clone());
        let floored = zoned
            .datetime()
            .with()
            .minute(0)
            .second(0)
            .subsec_nanosecond(0)
            .build()
            .map_err(|_| CivilError::OutOfRange(zoned.to_string()))?;
        self.resolve(floored, Some(zoned.offset()))
    }

    /// End of the civil hour that starts at `start`.
    pub fn hour_end(&self, start: Timestamp) -> Result<Timestamp, CivilError> {
        let next = Self::instant(start)?
            .checked_add(jiff::SignedDuration::from_hours(1))
            .map_err(|_| CivilError::OutOfRange(format!("{start:?}")))?;
        self.hour_start(Self::back(next)?)
    }

    /// The logical day `t` belongs to: its local date, shifted back by the
    /// day-start time, so 03:59 belongs to the previous day.
    pub fn day(&self, t: Timestamp) -> Result<Date, CivilError> {
        let local = Self::instant(t)?.to_zoned(self.zone.clone()).datetime();
        let date = local.date();
        Ok(if local.time() < self.day_starts_at {
            date.yesterday()
                .map_err(|_| CivilError::OutOfRange(date.to_string()))?
        } else {
            date
        })
    }

    pub fn day_start(&self, day: Date) -> Result<Timestamp, CivilError> {
        self.resolve(day.to_datetime(self.day_starts_at), None)
    }

    pub fn day_end(&self, day: Date) -> Result<Timestamp, CivilError> {
        let next = day
            .tomorrow()
            .map_err(|_| CivilError::OutOfRange(day.to_string()))?;
        self.day_start(next)
    }

    /// The local wall-clock hour of `t` (0–23), for hour-of-day statistics.
    pub fn local_hour(&self, t: Timestamp) -> Result<i8, CivilError> {
        Ok(Self::instant(t)?.to_zoned(self.zone.clone()).hour())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(unix: i64) -> Timestamp {
        Timestamp::from_unix_secs(unix).unwrap()
    }

    fn midnight_civil() -> Civil {
        Civil::new("Europe/Warsaw", 0, 0).unwrap()
    }

    #[test]
    fn spring_forward_day_is_23_hours() {
        let c = midnight_civil();
        // 2024-03-31 noon local.
        let day = c.day(ts(1_711_879_200)).unwrap();
        let len = c.day_end(day).unwrap().unix_ns() - c.day_start(day).unwrap().unix_ns();
        assert_eq!(len, 23 * 3_600_000_000_000);
    }

    #[test]
    fn fall_back_day_is_25_hours() {
        let c = midnight_civil();
        let day = c.day(ts(1_730_026_800)).unwrap();
        // Local midnight 2024-10-27 00:00 CEST is 2024-10-26 22:00 UTC.
        assert_eq!(c.day_start(day).unwrap(), ts(1_729_980_000));
        let len = c.day_end(day).unwrap().unix_ns() - c.day_start(day).unwrap().unix_ns();
        assert_eq!(len, 25 * 3_600_000_000_000);
    }

    #[test]
    fn repeated_hour_at_fall_back_is_two_buckets() {
        let c = midnight_civil();
        // 2024-10-27: 02:30 CEST is 00:30 UTC; 02:30 CET is 01:30 UTC.
        let first = c.hour_start(ts(1_729_989_000)).unwrap();
        let second = c.hour_start(ts(1_729_992_600)).unwrap();
        assert_eq!(first, ts(1_729_987_200), "02:00 CEST is 00:00 UTC");
        assert_eq!(second, ts(1_729_990_800), "02:00 CET is 01:00 UTC");
        assert_eq!(c.hour_end(first).unwrap(), second);
    }

    #[test]
    fn late_evening_stays_in_its_local_day() {
        let c = midnight_civil();
        // 2024-06-15 23:30 CEST.
        assert_eq!(
            c.day(ts(1_718_487_000)).unwrap(),
            jiff::civil::date(2024, 6, 15)
        );
        assert_eq!(c.hour_start(ts(1_718_455_040)).unwrap(), ts(1_718_452_800));
    }

    #[test]
    fn logical_day_starts_at_four() {
        let c = Civil::operator_default();
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

    #[test]
    fn unknown_zone_is_an_error_not_utc() {
        assert!(matches!(
            Civil::new("Not/A_Zone", 4, 0),
            Err(CivilError::UnknownZone(_))
        ));
        assert!(matches!(
            Civil::new("UTC", 24, 0),
            Err(CivilError::BadDayStart(24, 0))
        ));
    }
}
