//! Analysis in the daemon: reads the daily metric figures through the query
//! language and the coverage evidence from the store, and runs the pure code
//! in `sinex-analysis` over them. Nothing here is stored: a panel, a method's
//! answer and the brief are computed when asked for, at one commit position.
//!
//! - [`panel`]: figures, coverage and the cells built from them;
//! - [`recipes`]: the shipped recipes;
//! - [`brief`]: the daily brief and why a cell is what it is.
use sinex_analysis::Span;
use sinex_model::Timestamp;
use sinex_model::civil::Civil;

mod brief;
mod panel;
mod recipes;

pub(crate) use brief::{brief, why_not};
pub(crate) use panel::{panel, table};
pub(crate) use recipes::{Recipe, run};

/// The day setting the figures are derived under.
#[derive(Clone)]
pub(crate) struct Settings {
    pub civil: Civil,
    /// `04:00 Europe/Warsaw`: reported with every answer.
    pub label: String,
}

impl Settings {
    pub(crate) fn operator_default() -> Self {
        let civil = Civil::operator_default();
        Self {
            label: civil.label(),
            civil,
        }
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
        Ok(Span::new(micros(start), micros(end)))
    }

    pub(crate) fn today(&self) -> Result<jiff::civil::Date, String> {
        self.civil.day(Timestamp::now()).map_err(|e| e.to_string())
    }
}

pub(crate) fn parse_day(s: &str) -> Result<jiff::civil::Date, String> {
    s.parse()
        .map_err(|e| format!("`{s}` is not a date like 2026-10-18: {e}"))
}

fn micros(t: Timestamp) -> i64 {
    i64::try_from(t.unix_ns() / 1000).unwrap_or(i64::MAX)
}

fn rfc3339(us: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(us) * 1000)
        .ok()
        .and_then(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
