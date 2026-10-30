//! Local hours and logical days in the operator's zone. `ts_orig` stays a UTC instant everywhere;
//! civil time is only for bucketing and rendering: hours, and logical days that
//! start at a configured local time (04:00 by default, one setting). Days
//! across a DST change are 23 or 25 hours long.
//!
//! Hours are floored with the instant's own UTC offset, so the repeated hour at
//! the autumn change (02:00–03:00 twice in Europe/Warsaw) gives two distinct
//! buckets. Flooring the local clock time and resolving it again would put the
//! second 02:xx into the first 02:00 bucket.
use crate::Timestamp;
use jiff::civil::{Date, DateTime, Time};
use jiff::tz::{AmbiguousOffset, TimeZone};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TimeSettingsError {
    #[error("unknown time zone {0}")]
    UnknownZone(String),
    #[error("day start {0:02}:{1:02} is not a time of day")]
    BadDayStart(u8, u8),
    #[error("{0} is outside the supported time range")]
    OutOfRange(String),
}

/// The operator's zone and logical-day start.
#[derive(Clone, Debug)]
pub struct TimeSettings {
    zone: TimeZone,
    zone_name: String,
    day_starts_at: Time,
}

impl TimeSettings {
    pub fn new(
        zone: &str,
        day_start_hour: u8,
        day_start_minute: u8,
    ) -> Result<Self, TimeSettingsError> {
        let tz =
            TimeZone::get(zone).map_err(|_| TimeSettingsError::UnknownZone(zone.to_owned()))?;
        let day_starts_at = Time::new(
            i8::try_from(day_start_hour)
                .map_err(|_| TimeSettingsError::BadDayStart(day_start_hour, day_start_minute))?,
            i8::try_from(day_start_minute)
                .map_err(|_| TimeSettingsError::BadDayStart(day_start_hour, day_start_minute))?,
            0,
            0,
        )
        .map_err(|_| TimeSettingsError::BadDayStart(day_start_hour, day_start_minute))?;
        Ok(Self {
            zone: tz,
            zone_name: zone.to_owned(),
            day_starts_at,
        })
    }

    pub fn from_parts(zone: TimeZone, day_starts_at: Time) -> Self {
        let zone_name = zone
            .iana_name()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{zone:?}"));
        Self {
            zone,
            zone_name,
            day_starts_at,
        }
    }

    pub fn zone_name(&self) -> &str {
        &self.zone_name
    }

    /// The local day start and zone, including any configured seconds.
    pub fn label(&self) -> String {
        format!("{} {}", self.day_starts_at, self.zone_name)
    }

    fn instant(t: Timestamp) -> Result<jiff::Timestamp, TimeSettingsError> {
        jiff::Timestamp::from_nanosecond(t.unix_ns())
            .map_err(|_| TimeSettingsError::OutOfRange(format!("{t:?}")))
    }

    fn back(t: jiff::Timestamp) -> Result<Timestamp, TimeSettingsError> {
        Timestamp::from_unix_ns(t.as_nanosecond())
            .map_err(|_| TimeSettingsError::OutOfRange(t.to_string()))
    }

    /// Resolve a local date-time to an instant. In a fold, `prefer` picks the
    /// instant with that offset; otherwise the earlier one. A gap resolves to
    /// the instant just after it.
    fn resolve(
        &self,
        dt: DateTime,
        prefer: Option<jiff::tz::Offset>,
    ) -> Result<Timestamp, TimeSettingsError> {
        let ambiguous = self.zone.to_ambiguous_timestamp(dt);
        let offset = match ambiguous.offset() {
            AmbiguousOffset::Unambiguous { offset } => offset,
            AmbiguousOffset::Fold { before, after } => match prefer {
                Some(p) if p == after => after,
                _ => before,
            },
            AmbiguousOffset::Gap { .. } => {
                return Self::back(
                    self.day_setting()?
                        .local(dt)
                        .map_err(|error| TimeSettingsError::OutOfRange(error.to_string()))?
                        .instant,
                );
            }
        };
        let t = offset
            .to_timestamp(dt)
            .map_err(|_| TimeSettingsError::OutOfRange(dt.to_string()))?;
        Self::back(t)
    }

    /// Start of the local hour containing `t`.
    pub fn hour_start(&self, t: Timestamp) -> Result<Timestamp, TimeSettingsError> {
        let zoned = Self::instant(t)?.to_zoned(self.zone.clone());
        let floored = zoned
            .datetime()
            .with()
            .minute(0)
            .second(0)
            .subsec_nanosecond(0)
            .build()
            .map_err(|_| TimeSettingsError::OutOfRange(zoned.to_string()))?;
        self.resolve(floored, Some(zoned.offset()))
    }

    /// End of the local hour that starts at `start`.
    pub fn hour_end(&self, start: Timestamp) -> Result<Timestamp, TimeSettingsError> {
        let next = Self::instant(start)?
            .checked_add(jiff::SignedDuration::from_hours(1))
            .map_err(|_| TimeSettingsError::OutOfRange(format!("{start:?}")))?;
        self.hour_start(Self::back(next)?)
    }

    /// The shared logical-day setting; unnamed zones are refused.
    pub fn day_setting(&self) -> Result<query_language::time::DaySetting, TimeSettingsError> {
        query_language::time::DaySetting::new(self.zone.clone(), self.day_starts_at)
            .map_err(|_| TimeSettingsError::UnknownZone(self.zone_name.clone()))
    }
    pub fn day(&self, t: Timestamp) -> Result<Date, TimeSettingsError> {
        self.day_setting()?
            .logical_day(Self::instant(t)?)
            .map_err(|error| TimeSettingsError::OutOfRange(error.to_string()))
    }
    pub fn day_start(&self, day: Date) -> Result<Timestamp, TimeSettingsError> {
        Self::back(
            self.day_setting()?
                .start_of(day)
                .map_err(|error| TimeSettingsError::OutOfRange(error.to_string()))?,
        )
    }
    pub fn day_end(&self, day: Date) -> Result<Timestamp, TimeSettingsError> {
        Self::back(
            self.day_setting()?
                .day(day)
                .map_err(|error| TimeSettingsError::OutOfRange(error.to_string()))?
                .end,
        )
    }
}

#[cfg(test)]
mod tests;
