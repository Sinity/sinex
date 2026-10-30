//! Stored analysis products and their presentation. Requests retain their
//! source bytes; the keyed product derivation maintains panels, answers,
//! findings and context packs through the ordinary admission lifecycle.
use sinex_analysis::Span;
use sinex_model::Timestamp;
use sinex_model::time_settings::TimeSettings;

mod access;
use sinex_sources::sources::analysis as request;
mod brief;
mod panel;
pub(crate) mod recall;
mod recipes;
pub(crate) mod stored;

pub(crate) use access::question;
pub(crate) use access::{load_pack, materialize};
pub(crate) use brief::{render_pack, why_not};
pub(crate) use panel::{gaps, panel, table};
pub(crate) use recipes::{Recipe, run};

/// The day setting the figures are derived under.
#[derive(Clone)]
pub(crate) struct Settings {
    pub time_settings: TimeSettings,
    /// The effective logical-day boundary and named zone, reported with every answer.
    pub label: String,
}

impl Settings {
    #[cfg(test)]
    pub(crate) fn operator_default() -> Self {
        let time_settings = TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting");
        Self {
            label: time_settings.label(),
            time_settings,
        }
    }

    pub(crate) fn day_span(
        &self,
        from: jiff::civil::Date,
        to_inclusive: jiff::civil::Date,
    ) -> Result<Span, String> {
        let start = self
            .time_settings
            .day_start(from)
            .map_err(|e| e.to_string())?;
        let end = self
            .time_settings
            .day_end(to_inclusive)
            .map_err(|e| e.to_string())?;
        Ok(Span::new(micros(start), micros(end)))
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

/// The daemon's complete keyed catalog, including analyses over derived figures.
pub(crate) fn keyed_with(
    configuration: &sinex_derivations::Configuration,
) -> Vec<Box<dyn sinex_derivations::AnyKeyed>> {
    let mut all = sinex_derivations::keyed_with(configuration);
    all.push(Box::new(stored::StoredProducts));
    all.push(Box::new(recall::RecallProducts));
    all
}
pub(crate) fn keyed() -> Vec<Box<dyn sinex_derivations::AnyKeyed>> {
    keyed_with(&sinex_derivations::Configuration::default())
}
