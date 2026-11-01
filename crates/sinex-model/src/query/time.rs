//! Times in queries. Every relative time is resolved once, against one
//! [`Context`]: `now`, the zone clock times are read in, and the hour a logical
//! day starts (04:00 by default), so 01:30 belongs to the day before.
use jiff::civil::{Date, DateTime, Time};
use jiff::tz::TimeZone;
use jiff::{Timestamp, Zoned};

#[derive(Clone, Debug)]
pub struct Context {
    pub now: Timestamp,
    pub zone: TimeZone,
    pub day_start: Time,
    pub catalog: Option<super::QueryCatalog>,
}

impl Context {
    pub fn new(now: Timestamp, zone: TimeZone, day_start: Time) -> Self {
        Self {
            now,
            zone,
            day_start,
            catalog: None,
        }
    }

    /// An explicit UTC clock with logical days from 04:00.
    pub fn utc(now: Timestamp) -> Self {
        Self::new(now, TimeZone::get("UTC").expect("UTC zone"), Time::constant(4, 0, 0, 0))
    }

    pub fn zone_name(&self) -> String {
        self.zone.iana_name().expect("validated named query zone").to_string()
    }

    /// Explicit clock passed to the shared resolver; no zone database lookup.
    pub fn clock(&self) -> Result<query_language::time::Clock, super::QueryError> {
        let day = query_language::time::DaySetting::new(self.zone.clone(), self.day_start)
            .map_err(super::syntax::error)?;
        Ok(query_language::time::Clock::new(self.now, day))
    }

    pub fn logical_date(&self, at: Timestamp) -> Date {
        self.clock().expect("validated query clock").day.logical_day(at)
            .expect("logical date is in the supported calendar")
    }
    pub fn local(&self, at: DateTime) -> Option<Timestamp> {
        self.clock().ok()?.day.local(at).ok().map(|r| r.instant)
    }
    pub fn day(&self, date: Date) -> Option<(Timestamp, Timestamp)> {
        let r = self.clock().ok()?.day.day(date).ok()?;
        Some((r.start, r.end))
    }
    pub fn days(&self, from: Date, to: Date) -> Option<(Timestamp, Timestamp)> {
        let r = self.clock().ok()?.day.days(from, to).ok()?;
        Some((r.start, r.end))
    }
    pub fn today(&self) -> Date { self.logical_date(self.now) }
    pub fn week_of(&self, date: Date) -> Option<(Timestamp, Timestamp)> {
        let at = self.day(date)?.0;
        let mut clock = self.clock().ok()?;
        clock.now = at;
        let r = query_language::time::resolve("this week", &clock).ok()?.value.range()?;
        Some((r.start, r.end))
    }
    pub fn month_of(&self, year: i16, month: i8) -> Option<(Timestamp, Timestamp)> {
        let r = query_language::time::resolve(&format!("{year:04}-{month:02}"), &self.clock().ok()?).ok()?.value.range()?;
        Some((r.start, r.end))
    }
    pub fn year_of(&self, year: i16) -> Option<(Timestamp, Timestamp)> {
        let r = query_language::time::resolve(&format!("{year:04}"), &self.clock().ok()?).ok()?.value.range()?;
        Some((r.start, r.end))
    }

    pub fn zoned(&self, at: Timestamp) -> Zoned {
        at.to_zoned(self.zone.clone())
    }
}

/// `30s`, `5m`, `2h`, `3d`, `2w`, in seconds.
pub fn duration(word: &str) -> Option<f64> {
    query_language::time::duration(word).map(|value| value.as_secs_f64())
}

pub fn date(word: &str) -> Option<Date> {
    if word.len() != 10 {
        return None;
    }
    word.parse::<Date>().ok()
}

/// `2026-08`.
pub fn month(word: &str) -> Option<(i16, i8)> {
    let (y, m) = word.split_once('-')?;
    if y.len() != 4 || m.len() != 2 {
        return None;
    }
    let year = y.parse().ok()?;
    let month = m.parse().ok()?;
    (1..=12).contains(&month).then_some((year, month))
}

/// `2026`.
pub fn year(word: &str) -> Option<i16> {
    (word.len() == 4 && word.bytes().all(|b| b.is_ascii_digit()))
        .then(|| word.parse().ok())
        .flatten()
}

/// `14:05` or `14:05:30`.
pub fn clock(word: &str) -> Option<Time> {
    let parts: Vec<&str> = word.split(':').collect();
    if !(2..=3).contains(&parts.len()) || parts.iter().any(|p| p.len() != 2) {
        return None;
    }
    let h: i8 = parts[0].parse().ok()?;
    let m: i8 = parts[1].parse().ok()?;
    let s: i8 = parts.get(2).map_or(Some(0), |s| s.parse().ok())?;
    Time::new(h, m, s, 0).ok()
}

/// An instant: RFC 3339 with an offset, or a local `2026-03-02T14:05[:30]`.
pub fn instant(ctx: &Context, word: &str) -> Option<Timestamp> {
    query_language::time::resolve(word, &ctx.clock().ok()?).ok().map(|r| r.value.start())
}

#[cfg(test)]
mod tests;
