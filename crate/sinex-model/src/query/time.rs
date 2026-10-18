//! Times in queries (spec `query.md` §2, "Times and durations"). Every
//! relative time is resolved once, against one [`Context`]: `now`, the zone
//! clock times are read in, and the hour a logical day starts (04:00 by
//! default, joint decision 15), so 01:30 belongs to the day before.
use jiff::civil::{Date, DateTime, Time};
use jiff::tz::TimeZone;
use jiff::{Timestamp, ToSpan, Zoned};

#[derive(Clone, Debug)]
pub struct Context {
    pub now: Timestamp,
    pub zone: TimeZone,
    pub day_start: Time,
}

impl Context {
    pub fn new(now: Timestamp, zone: TimeZone, day_start: Time) -> Self {
        Self {
            now,
            zone,
            day_start,
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
    let (d, t) = word.split_once('T')?;
    ctx.local(date(d)?.to_datetime(clock(t)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> Context {
        Context::new(
            "2026-09-14T10:00:00Z".parse().unwrap(),
            TimeZone::get("Europe/Warsaw").unwrap(),
            Time::constant(4, 0, 0, 0),
        )
    }

    #[test]
    fn logical_days_start_at_four() {
        let c = ctx();
        // 01:30 local on the 14th belongs to the 13th.
        let early = c.local("2026-09-14T01:30".parse().unwrap()).unwrap();
        assert_eq!(c.logical_date(early), "2026-09-13".parse::<Date>().unwrap());
        let (start, end) = c.day("2026-09-14".parse().unwrap()).unwrap();
        assert_eq!(start.to_string(), "2026-09-14T02:00:00Z");
        assert_eq!(end.to_string(), "2026-09-15T02:00:00Z");
    }

    #[test]
    fn days_across_a_change_of_offset_have_their_real_length() {
        let c = ctx();
        // Europe/Warsaw leaves summer time at 03:00 on 2026-10-25, before the
        // 04:00 day start, so the long day is logical day 2026-10-24.
        let (start, end) = c.day("2026-10-24".parse().unwrap()).unwrap();
        assert_eq!(end.duration_since(start).as_secs(), 25 * 3600);
    }

    #[test]
    fn parses_words() {
        assert_eq!(duration("5m"), Some(300.0));
        assert_eq!(duration("3d"), Some(259_200.0));
        assert_eq!(duration("5x"), None);
        assert_eq!(month("2026-08"), Some((2026, 8)));
        assert_eq!(year("2026"), Some(2026));
        assert_eq!(clock("14:05"), Some(Time::constant(14, 5, 0, 0)));
        let c = ctx();
        assert_eq!(
            instant(&c, "2026-03-02T14:05").unwrap().to_string(),
            "2026-03-02T13:05:00Z"
        );
        assert_eq!(
            instant(&c, "2026-03-02T14:05:00Z").unwrap().to_string(),
            "2026-03-02T14:05:00Z"
        );
    }

    #[test]
    fn weeks_start_on_monday() {
        let c = ctx();
        let (start, _) = c.week_of("2026-09-17".parse().unwrap()).unwrap();
        assert_eq!(c.logical_date(start), "2026-09-14".parse::<Date>().unwrap());
    }
}
