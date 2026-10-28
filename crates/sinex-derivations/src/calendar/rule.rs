//! RFC 5545 recurrence rules (`RRULE`), expanded in civil time: the starts a
//! rule gives are local dates and times in the series' zone, so a weekly
//! 09:00 stays at 09:00 across a daylight-saving change.
//!
//! Every frequency from daily to yearly is read, with `INTERVAL`, `COUNT`,
//! `UNTIL`, `WKST` and the `BYMONTH`, `BYWEEKNO`-free day rules (`BYMONTHDAY`,
//! `BYYEARDAY`, `BYDAY` with or without an ordinal), the time rules
//! (`BYHOUR`, `BYMINUTE`, `BYSECOND`) and `BYSETPOS`. Hourly and finer
//! frequencies and `BYWEEKNO` are refused by name: calendars do not write
//! them, and the series then gives only its first start.
use jiff::civil::{Date, DateTime, Time, Weekday};
use jiff::{Span, ToSpan};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Frequency {
    Daily,
    Weekly,
    Monthly,
    Yearly,
}

/// The end `UNTIL` gives, before it is compared with starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Until {
    /// A date: starts on that local date are included.
    Date(Date),
    /// A local date and time, inclusive.
    Local(DateTime),
    /// A UTC date and time, inclusive.
    Utc(DateTime),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    pub frequency: Frequency,
    pub interval: i64,
    pub count: Option<u64>,
    pub until: Option<Until>,
    pub months: Vec<i8>,
    pub month_days: Vec<i8>,
    pub year_days: Vec<i16>,
    /// Weekdays, each with its ordinal within the month or year (`2MO`,
    /// `-1SU`) or none.
    pub weekdays: Vec<(Option<i8>, Weekday)>,
    pub hours: Vec<i8>,
    pub minutes: Vec<i8>,
    pub seconds: Vec<i8>,
    pub set_positions: Vec<i32>,
    pub week_start: Weekday,
}

fn weekday(code: &str) -> Option<Weekday> {
    Some(match code {
        "MO" => Weekday::Monday,
        "TU" => Weekday::Tuesday,
        "WE" => Weekday::Wednesday,
        "TH" => Weekday::Thursday,
        "FR" => Weekday::Friday,
        "SA" => Weekday::Saturday,
        "SU" => Weekday::Sunday,
        _ => return None,
    })
}

fn numbers<T: std::str::FromStr>(
    name: &str,
    value: &str,
    valid: impl Fn(&T) -> bool,
) -> Result<Vec<T>, String> {
    value
        .split(',')
        .map(|n| {
            n.trim()
                .parse::<T>()
                .ok()
                .filter(&valid)
                .ok_or_else(|| format!("{name}={value} is not a valid list"))
        })
        .collect()
}

fn until(value: &str) -> Result<Until, String> {
    let bad = |e: jiff::Error| format!("UNTIL={value}: {e}");
    if value.len() == 8 {
        return Date::strptime("%Y%m%d", value)
            .map(Until::Date)
            .map_err(bad);
    }
    match value.strip_suffix('Z') {
        Some(body) => DateTime::strptime("%Y%m%dT%H%M%S", body)
            .map(Until::Utc)
            .map_err(bad),
        None => DateTime::strptime("%Y%m%dT%H%M%S", value)
            .map(Until::Local)
            .map_err(bad),
    }
}

impl Rule {
    /// Read an `RRULE` value (`FREQ=WEEKLY;BYDAY=MO,WE`).
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut rule = Rule {
            frequency: Frequency::Daily,
            interval: 1,
            count: None,
            until: None,
            months: vec![],
            month_days: vec![],
            year_days: vec![],
            weekdays: vec![],
            hours: vec![],
            minutes: vec![],
            seconds: vec![],
            set_positions: vec![],
            week_start: Weekday::Monday,
        };
        let mut frequency = None;
        for part in text.trim().split(';').filter(|p| !p.is_empty()) {
            let (name, value) = part
                .split_once('=')
                .ok_or_else(|| format!("{part:?} is not NAME=VALUE"))?;
            let name = name.trim().to_ascii_uppercase();
            let value = value.trim();
            match name.as_str() {
                "FREQ" => {
                    frequency = Some(match value {
                        "DAILY" => Frequency::Daily,
                        "WEEKLY" => Frequency::Weekly,
                        "MONTHLY" => Frequency::Monthly,
                        "YEARLY" => Frequency::Yearly,
                        "HOURLY" | "MINUTELY" | "SECONDLY" => {
                            return Err(format!("FREQ={value} is not expanded"));
                        }
                        other => return Err(format!("FREQ={other} is not a frequency")),
                    });
                }
                "INTERVAL" => {
                    rule.interval = value
                        .parse()
                        .ok()
                        .filter(|n| *n > 0)
                        .ok_or_else(|| format!("INTERVAL={value} is not a positive number"))?;
                }
                "COUNT" => {
                    rule.count = Some(
                        value
                            .parse()
                            .map_err(|_| format!("COUNT={value} is not a number"))?,
                    );
                }
                "UNTIL" => rule.until = Some(until(value)?),
                "BYMONTH" => rule.months = numbers(&name, value, |m: &i8| (1..=12).contains(m))?,
                "BYMONTHDAY" => {
                    rule.month_days =
                        numbers(&name, value, |d: &i8| *d != 0 && (-31..=31).contains(d))?;
                }
                "BYYEARDAY" => {
                    rule.year_days =
                        numbers(&name, value, |d: &i16| *d != 0 && (-366..=366).contains(d))?;
                }
                "BYHOUR" => rule.hours = numbers(&name, value, |h: &i8| (0..=23).contains(h))?,
                "BYMINUTE" => rule.minutes = numbers(&name, value, |m: &i8| (0..=59).contains(m))?,
                "BYSECOND" => rule.seconds = numbers(&name, value, |s: &i8| (0..=60).contains(s))?,
                "BYSETPOS" => {
                    rule.set_positions =
                        numbers(&name, value, |p: &i32| *p != 0 && (-366..=366).contains(p))?;
                }
                "BYDAY" => {
                    for day in value.split(',').map(str::trim) {
                        if !day.is_ascii() {
                            return Err(format!("BYDAY={value}: not an ASCII weekday"));
                        }
                        let split = day.len().saturating_sub(2);
                        let (ordinal, code) = day.split_at(split);
                        let ordinal = match ordinal.trim_start_matches('+') {
                            "" => None,
                            n => Some(
                                n.parse::<i8>()
                                    .ok()
                                    .filter(|n| *n != 0 && (-53..=53).contains(n))
                                    .ok_or_else(|| format!("BYDAY={value}: bad ordinal"))?,
                            ),
                        };
                        let weekday = weekday(code)
                            .ok_or_else(|| format!("BYDAY={value}: {code:?} is not a day"))?;
                        rule.weekdays.push((ordinal, weekday));
                    }
                }
                "WKST" => {
                    rule.week_start =
                        weekday(value).ok_or_else(|| format!("WKST={value} is not a day"))?;
                }
                "BYWEEKNO" => return Err("BYWEEKNO is not expanded".into()),
                // Extension parts (`X-…`) do not change the starts.
                other if other.starts_with("X-") => {}
                other => return Err(format!("{other} is not a rule part")),
            }
        }
        rule.frequency = frequency.ok_or("the rule has no FREQ")?;
        if rule.count.is_some() && rule.until.is_some() {
            return Err("the rule has both COUNT and UNTIL".into());
        }
        Ok(rule)
    }

    /// The starts the rule gives from `first` (the series' `DTSTART`, always
    /// its first start, as RFC 5545 counts it), in order, while `within`
    /// accepts them: `within` says whether a local start is no later than
    /// both `UNTIL` and the caller's horizon. Each period of the frequency
    /// is read in turn until one begins past the horizon, so the expansion
    /// ends even when a rule gives no start at all.
    pub fn starts(&self, first: DateTime, within: impl Fn(DateTime) -> bool) -> Vec<DateTime> {
        let mut out = vec![first];
        let limit = self.count.unwrap_or(u64::MAX);
        for period in 0_i64.. {
            if out.len() as u64 >= limit {
                break;
            }
            let Some(days) = self.period(first.date(), period) else {
                break;
            };
            // A period whose first day is past the horizon ends the expansion.
            match days.first() {
                Some(day) if !within(day.to_datetime(Time::midnight())) => break,
                None => {
                    if !within(self.period_start(first.date(), period)) {
                        break;
                    }
                    continue;
                }
                Some(_) => {}
            }
            let mut set: Vec<DateTime> = Vec::new();
            for day in days {
                for time in self.times(first.time()) {
                    set.push(day.to_datetime(time));
                }
            }
            set.sort_unstable();
            set.dedup();
            for start in self.positions(set) {
                if start <= first {
                    continue;
                }
                if !within(start) || out.len() as u64 >= limit {
                    return out;
                }
                out.push(start);
            }
        }
        out
    }

    /// The local midnight a period starts at, for a period with no days.
    fn period_start(&self, first: Date, period: i64) -> DateTime {
        let step = period.saturating_mul(self.interval);
        let date = match self.frequency {
            Frequency::Daily => first.checked_add(step.days()),
            Frequency::Weekly => first.checked_add(step.weeks()),
            Frequency::Monthly => first.first_of_month().checked_add(step.months()),
            Frequency::Yearly => first.first_of_year().checked_add(step.years()),
        };
        date.map_or(DateTime::MAX, |d| d.to_datetime(Time::midnight()))
    }

    /// The days the rule gives in one period, sorted; `None` once the
    /// period is past the representable dates.
    fn period(&self, first: Date, period: i64) -> Option<Vec<Date>> {
        let step = period.checked_mul(self.interval)?;
        let mut days = match self.frequency {
            Frequency::Daily => vec![first.checked_add(step.days()).ok()?],
            Frequency::Weekly => {
                let back = (7 + first.weekday().to_monday_zero_offset()
                    - self.week_start.to_monday_zero_offset())
                    % 7;
                let start = first
                    .checked_sub(i64::from(back).days())
                    .ok()?
                    .checked_add(step.weeks())
                    .ok()?;
                let week: Vec<Date> = (0..7)
                    .filter_map(|n| start.checked_add(Span::new().days(n)).ok())
                    .collect();
                if self.weekdays.is_empty() {
                    week.into_iter()
                        .filter(|d| d.weekday() == first.weekday())
                        .collect()
                } else {
                    week
                }
            }
            Frequency::Monthly => {
                let month = first.first_of_month().checked_add(step.months()).ok()?;
                if self.month_days.is_empty() && self.weekdays.is_empty() {
                    month_day(month, first.day()).into_iter().collect()
                } else {
                    days_of_month(month)
                }
            }
            Frequency::Yearly => {
                let year = first.first_of_year().checked_add(step.years()).ok()?;
                if self.months.is_empty()
                    && self.month_days.is_empty()
                    && self.year_days.is_empty()
                    && self.weekdays.is_empty()
                {
                    Date::new(year.year(), first.month(), first.day())
                        .ok()
                        .into_iter()
                        .collect()
                } else if !self.months.is_empty()
                    && self.month_days.is_empty()
                    && self.year_days.is_empty()
                    && self.weekdays.is_empty()
                {
                    self.months
                        .iter()
                        .filter_map(|m| Date::new(year.year(), *m, 1).ok())
                        .filter_map(|m| month_day(m, first.day()))
                        .collect()
                } else {
                    days_of_year(year)
                }
            }
        };
        days.retain(|d| self.keeps(*d));
        days.sort_unstable();
        days.dedup();
        Some(days)
    }

    /// Whether the day rules keep `day`. A weekday's ordinal counts within
    /// the month for a monthly rule or a yearly one with `BYMONTH`, otherwise
    /// within the year.
    fn keeps(&self, day: Date) -> bool {
        if !self.months.is_empty() && !self.months.contains(&day.month()) {
            return false;
        }
        if !self.month_days.is_empty() {
            let length = i16::from(day.days_in_month());
            let index = i16::from(day.day());
            if !self
                .month_days
                .iter()
                .any(|d| i16::from(*d) == index || i16::from(*d) == index - length - 1)
            {
                return false;
            }
        }
        if !self.year_days.is_empty() {
            let length = day.days_in_year();
            let index = day.day_of_year();
            if !self
                .year_days
                .iter()
                .any(|d| *d == index || *d == index - length - 1)
            {
                return false;
            }
        }
        if !self.weekdays.is_empty() {
            let in_month = self.frequency == Frequency::Monthly
                || (self.frequency == Frequency::Yearly && !self.months.is_empty());
            let ordinals_count = matches!(self.frequency, Frequency::Monthly | Frequency::Yearly);
            if !self.weekdays.iter().any(|(ordinal, weekday)| {
                if day.weekday() != *weekday {
                    return false;
                }
                let Some(n) = ordinal.filter(|_| ordinals_count) else {
                    return true;
                };
                let (index, length) = if in_month {
                    (i16::from(day.day()), i16::from(day.days_in_month()))
                } else {
                    (day.day_of_year(), day.days_in_year())
                };
                let from_start = (index - 1) / 7 + 1;
                let from_end = -((length - index) / 7 + 1);
                i16::from(n) == from_start || i16::from(n) == from_end
            }) {
                return false;
            }
        }
        true
    }

    /// The times of day each day of the rule gives.
    fn times(&self, first: Time) -> Vec<Time> {
        let pick = |values: &[i8], own: i8| -> Vec<i8> {
            if values.is_empty() {
                vec![own]
            } else {
                values.to_vec()
            }
        };
        let mut out = Vec::new();
        for h in pick(&self.hours, first.hour()) {
            for m in pick(&self.minutes, first.minute()) {
                // A leap second is read as the last second of its minute.
                for s in pick(&self.seconds, first.second()) {
                    if let Ok(t) = Time::new(h, m, s.min(59), 0) {
                        out.push(t);
                    }
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// `BYSETPOS`: the members of a period's sorted set at those positions.
    fn positions(&self, set: Vec<DateTime>) -> Vec<DateTime> {
        if self.set_positions.is_empty() {
            return set;
        }
        let length = i64::try_from(set.len()).unwrap_or(i64::MAX);
        let mut out: Vec<DateTime> = self
            .set_positions
            .iter()
            .filter_map(|p| {
                let p = i64::from(*p);
                let index = if p > 0 { p - 1 } else { length + p };
                usize::try_from(index)
                    .ok()
                    .and_then(|i| set.get(i))
                    .copied()
            })
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

/// Day `day` of the month starting at `month`, when the month has it.
fn month_day(month: Date, day: i8) -> Option<Date> {
    Date::new(month.year(), month.month(), day).ok()
}

fn days_of_month(month: Date) -> Vec<Date> {
    (1..=month.days_in_month())
        .filter_map(|d| month_day(month, d))
        .collect()
}

fn days_of_year(year: Date) -> Vec<Date> {
    (0..year.days_in_year())
        .filter_map(|n| year.checked_add(i64::from(n).days()).ok())
        .collect()
}

#[cfg(test)]
mod tests;
