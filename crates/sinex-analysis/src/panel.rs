//! Panels: logical days by metrics, each cell with its observation state,
//! from the daily metric figures and the coverage of each metric's sources.
//! A day is observed when the coverage spans it, partial when it spans part
//! of it, and missing with its reason otherwise; a zero is only ever a
//! measured zero. A figure exists only for a day something was counted on,
//! so an observed day without one is a zero.
use crate::cell::{Cell, Missing};
use crate::coverage::{Completeness, Coverage};
use crate::span::{MICROS_PER_SECOND, Span, SpanSet};
use serde::{Deserialize, Serialize};
use sinex_derivations::figures::{Law, Metric};
use sinex_model::Timestamp;
use sinex_model::time_settings::{TimeSettings, TimeSettingsError};

/// One logical day.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Unit {
    pub label: String,
    pub span: Span,
}

fn micros(t: Timestamp) -> i64 {
    i64::try_from(t.unix_ns() / 1000).unwrap_or(i64::MAX)
}

/// The logical days from `from` to `to`, both included, under the day setting.
pub fn days(
    time_settings: &TimeSettings,
    from: jiff::civil::Date,
    to: jiff::civil::Date,
) -> Result<Vec<Unit>, TimeSettingsError> {
    let mut out = Vec::new();
    let mut day = from;
    while day <= to {
        out.push(Unit {
            label: day.to_string(),
            span: Span::new(
                micros(time_settings.day_start(day)?),
                micros(time_settings.day_end(day)?),
            ),
        });
        day = day
            .tomorrow()
            .map_err(|e| TimeSettingsError::OutOfRange(e.to_string()))?;
    }
    Ok(out)
}

/// A daily `metric.figure`, as a panel reads it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DayFigure {
    pub metric: String,
    pub day: String,
    pub dimension: String,
    pub dimension_value: String,
    pub value: Option<f64>,
    pub numerator: Option<f64>,
    pub denominator: Option<f64>,
    pub rows: u64,
    /// The figure's citation, `sinex:ev/<id>`.
    pub reference: String,
}

/// One cell of a panel.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PanelCell {
    pub metric: &'static str,
    pub unit: Unit,
    /// The breakdown field and its value; empty for the total.
    pub dimension: String,
    pub dimension_value: String,
    pub cell: Cell<f64>,
    pub numerator: Option<f64>,
    pub denominator: Option<f64>,
    pub rows: u64,
    pub observed_seconds: f64,
    /// The figure the value came from; `None` for a zero or a missing day.
    pub reference: Option<String>,
}

/// A day counts as fully observed when coverage misses less than this.
const OBSERVED_FRACTION: f64 = 0.999;

/// What the metric's sources covered, any of them sufficing, and whether any
/// of the coverage is of unknown completeness (which makes figures lower
/// bounds).
fn covered(metric: &Metric, coverage: &[Coverage]) -> (SpanSet, bool) {
    let mine = |c: &&Coverage| metric.sources.contains(&c.source.as_str());
    let known = SpanSet::from_spans(
        coverage
            .iter()
            .filter(mine)
            .filter(|c| c.completeness != Completeness::Unknown)
            .map(|c| c.span),
    );
    let unknown = coverage
        .iter()
        .filter(mine)
        .any(|c| c.completeness == Completeness::Unknown);
    (known, unknown)
}

/// The cells of one metric over `units`: one total cell per day, plus one
/// per breakdown value a figure exists for.
pub fn cells(
    metric: &'static Metric,
    units: &[Unit],
    figures: &[DayFigure],
    coverage: &[Coverage],
) -> Vec<PanelCell> {
    let (spans, unknown_coverage) = covered(metric, coverage);
    let mut out = Vec::new();
    for unit in units {
        let observed = spans.clip(unit.span);
        let fraction = observed.len() as f64 / unit.span.len().max(1) as f64;
        let state = |figure: Option<&DayFigure>| -> Cell<f64> {
            let value = match figure {
                Some(f) => f.value,
                // Nothing counted: zero, unless the metric is a ratio of nothing.
                None => match metric.law {
                    Law::Count | Law::Seconds { .. } => Some(0.0),
                    Law::Ratio { .. } => None,
                },
            };
            if fraction <= 0.0 {
                return match (figure, value) {
                    (Some(_), Some(value)) => Cell::Recorded { value },
                    _ if unknown_coverage => Cell::Missing {
                        reason: Missing::CoverageUnknown,
                    },
                    _ => Cell::Missing {
                        reason: Missing::NoCoverage,
                    },
                };
            }
            match value {
                None => Cell::Missing {
                    reason: Missing::NotApplicable,
                },
                Some(value) if fraction >= OBSERVED_FRACTION => Cell::Observed { value },
                Some(value) => Cell::Partial {
                    value,
                    observed_fraction: (fraction * 1000.0).round() / 1000.0,
                },
            }
        };
        let of_day: Vec<&DayFigure> = figures
            .iter()
            .filter(|f| f.metric == metric.name && f.day == unit.label)
            .collect();
        let cell = |dimension: &str, value: &str, figure: Option<&DayFigure>| PanelCell {
            metric: metric.name,
            unit: unit.clone(),
            dimension: dimension.to_owned(),
            dimension_value: value.to_owned(),
            cell: state(figure),
            numerator: figure.and_then(|f| f.numerator),
            denominator: figure.and_then(|f| f.denominator),
            rows: figure.map_or(0, |f| f.rows),
            observed_seconds: observed.len() as f64 / MICROS_PER_SECOND as f64,
            reference: figure.map(|f| f.reference.clone()),
        };
        let total = of_day.iter().copied().find(|f| f.dimension.is_empty());
        out.push(cell("", "", total));
        let mut parts: Vec<&DayFigure> = of_day
            .into_iter()
            .filter(|f| !f.dimension.is_empty())
            .collect();
        parts.sort_by(|a, b| {
            (&a.dimension, &a.dimension_value).cmp(&(&b.dimension, &b.dimension_value))
        });
        for f in parts {
            out.push(cell(&f.dimension, &f.dimension_value, Some(f)));
        }
    }
    out
}

#[cfg(test)]
mod tests;
