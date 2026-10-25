//! The analysis layer in the daemon: reads relations and coverage evidence
//! from the store, runs the pure code in `sinex-analysis`, and stores
//! maintained metric values and products.
//!
//! - [`cells`]: coverage, panels and the maintained metric values;
//! - [`recipes`]: the shipped recipes and their stored results;
//! - [`brief`]: the daily brief and why a cell is what it is.
use sinex_analysis::Span;
use sinex_model::Timestamp;
use sinex_model::civil::Civil;
use time::OffsetDateTime;

mod brief;
mod cells;
mod recipes;

pub(crate) use brief::{brief, why_not};
pub(crate) use cells::{MetricValues, coverage, dirty_name, panel, table};
pub(crate) use recipes::{Recipe, run, stored_finding};

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// The day setting every day-grain value is computed under.
#[derive(Clone)]
pub(crate) struct Settings {
    pub civil: Civil,
    /// `04:00 Europe/Warsaw`: recorded with every stored value.
    pub label: String,
}

impl Settings {
    pub(crate) fn new(zone: &str, day_start: &str) -> Result<Self, String> {
        let (h, m) = day_start
            .split_once(':')
            .and_then(|(h, m)| Some((h.parse::<u8>().ok()?, m.parse::<u8>().ok()?)))
            .ok_or_else(|| format!("day start `{day_start}` is not a clock time like 04:00"))?;
        let civil = Civil::new(zone, h, m).map_err(|e| e.to_string())?;
        Ok(Self {
            civil,
            label: format!("{h:02}:{m:02} {zone}"),
        })
    }

    /// The API's day setting. A zone without an IANA name (a bare offset)
    /// cannot be stored as a setting, so it is refused rather than guessed.
    pub(crate) fn from_api(
        zone: &jiff::tz::TimeZone,
        day_start: jiff::civil::Time,
    ) -> Result<Self, String> {
        let name = zone
            .iana_name()
            .ok_or("the daemon's zone has no IANA name; start it with --zone")?;
        Self::new(
            name,
            &format!("{:02}:{:02}", day_start.hour(), day_start.minute()),
        )
    }

    pub(crate) fn operator_default() -> Self {
        Self::new("Europe/Warsaw", "04:00").expect("the operator's default day setting is valid")
    }

    pub(crate) fn day_span(
        &self,
        from: jiff::civil::Date,
        to_inclusive: jiff::civil::Date,
    ) -> Result<Span, String> {
        let start = self.civil.day_start(from).map_err(|e| e.to_string())?;
        let end = self
            .civil
            .day_end(to_inclusive)
            .map_err(|e| e.to_string())?;
        Ok(Span::new(micros(start.0), micros(end.0)))
    }

    fn today(&self) -> Result<jiff::civil::Date, String> {
        self.civil.day(Timestamp::now()).map_err(|e| e.to_string())
    }
}

pub(crate) fn parse_day(s: &str) -> Result<jiff::civil::Date, String> {
    s.parse()
        .map_err(|e| format!("`{s}` is not a date like 2026-10-18: {e}"))
}

fn micros(t: OffsetDateTime) -> i64 {
    i64::try_from(t.unix_timestamp_nanos() / 1000).unwrap_or(i64::MAX)
}

fn odt(us: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(us) * 1000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn rfc3339(us: i64) -> String {
    odt(us)
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
