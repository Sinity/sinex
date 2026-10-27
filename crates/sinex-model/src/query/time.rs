//! Times in queries. Every relative time is resolved once, against one
//! [`Context`]: `now`, the zone clock times are read in, and the hour a logical
//! day starts (04:00 by default), so 01:30 belongs to the day before.
use jiff::civil::{Date, DateTime, Time};
use jiff::tz::TimeZone;
use jiff::{Timestamp, ToSpan, Zoned};

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

    /// UTC, logical days from 04:00. For tests and callers without a zone.
    pub fn utc(now: Timestamp) -> Self {
        Self::new(now, TimeZone::UTC, Time::constant(4, 0, 0, 0))
    }

    /// The zone's IANA name, or `UTC` for a fixed zone without one.
    pub fn zone_name(&self) -> String {
        self.zone.iana_name().unwrap_or("UTC").to_string()
    }

    /// The logical day an instant falls on.
    pub fn logical_date(&self, at: Timestamp) -> Date {
        let local = at.to_zoned(self.zone.clone()).datetime();
        if local.time() < self.day_start {
            local.date().yesterday().unwrap_or(local.date())
        } else {
            local.date()
        }
    }

    /// Where a local wall-clock time falls. A time skipped or repeated by a
    /// change of offset resolves the way the zone's rules say (compatible).
    pub fn local(&self, at: DateTime) -> Option<Timestamp> {
        self.zone
            .to_ambiguous_zoned(at)
            .compatible()
            .ok()
            .map(|z| z.timestamp())
    }

    /// `[start, end)` of a logical day.
    pub fn day(&self, date: Date) -> Option<(Timestamp, Timestamp)> {
        let start = self.local(date.to_datetime(self.day_start))?;
        let end = self.local(date.tomorrow().ok()?.to_datetime(self.day_start))?;
        Some((start, end))
    }

    /// `[start, end)` of logical days `from ..= to`.
    pub fn days(&self, from: Date, to: Date) -> Option<(Timestamp, Timestamp)> {
        Some((self.day(from)?.0, self.day(to)?.1))
    }

    pub fn today(&self) -> Date {
        self.logical_date(self.now)
    }

    /// The logical week (Monday first) containing `date`.
    pub fn week_of(&self, date: Date) -> Option<(Timestamp, Timestamp)> {
        let monday = date
            .checked_sub((date.weekday().to_monday_zero_offset() as i64).days())
            .ok()?;
        self.days(monday, monday.checked_add(6.days()).ok()?)
    }

    pub fn month_of(&self, year: i16, month: i8) -> Option<(Timestamp, Timestamp)> {
        let first = Date::new(year, month, 1).ok()?;
        self.days(first, first.last_of_month())
    }

    pub fn year_of(&self, year: i16) -> Option<(Timestamp, Timestamp)> {
        self.days(Date::new(year, 1, 1).ok()?, Date::new(year, 12, 31).ok()?)
    }

    pub fn zoned(&self, at: Timestamp) -> Zoned {
        at.to_zoned(self.zone.clone())
    }
}

/// `30s`, `5m`, `2h`, `3d`, `2w`, in seconds.
pub fn duration(word: &str) -> Option<f64> {
    let split = word.find(|c: char| !c.is_ascii_digit())?;
    let (digits, unit) = word.split_at(split);
    let n: f64 = digits.parse().ok()?;
    let scale = match unit {
        "s" => 1.0,
        "m" => 60.0,
        "h" => 3600.0,
        "d" => 86_400.0,
        "w" => 604_800.0,
        _ => return None,
    };
    Some(n * scale)
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
    if let Ok(ts) = word.parse::<Timestamp>() {
        return Some(ts);
    }
    match word {
        "now" => return Some(ctx.now),
        "today" => return ctx.day(ctx.today()).map(|r| r.0),
        "yesterday" => {
            return ctx
                .today()
                .yesterday()
                .ok()
                .and_then(|d| ctx.day(d))
                .map(|r| r.0);
        }
        _ => {}
    }
    if let Some(date) = date(word) {
        return ctx.day(date).map(|r| r.0);
    }
    if let Some(clock) = clock(word) {
        return ctx.local(ctx.zoned(ctx.now).date().to_datetime(clock));
    }
    let (d, t) = word.split_once('T')?;
    ctx.local(date(d)?.to_datetime(clock(t)?))
}

#[cfg(test)]
mod tests;
