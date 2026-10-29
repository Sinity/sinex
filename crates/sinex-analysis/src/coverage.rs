//! Coverage selected from admitted source observations. A successful read
//! without declared temporal bounds leaves measurement coverage unknown.
use crate::span::Span;
use serde::{Deserialize, Serialize};
use sinex_model::event_types::coverage::{CoverageCompleteness, CoverageState, SourceCoverage};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Completeness {
    Complete,
    Sample,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Coverage {
    pub source: String,
    pub span: Span,
    pub completeness: Completeness,
    pub bounded_by: String,
}

/// Missing reads cannot supply observed coverage. Successful reads retain the
/// declared completeness; access alone never supplies temporal bounds.
pub fn from_observation(row: &SourceCoverage) -> Option<Coverage> {
    if row.state == CoverageState::Missing {
        return None;
    }
    let completeness = if row.state == CoverageState::RecordedUnknownCoverage {
        Completeness::Unknown
    } else {
        match row.completeness {
            CoverageCompleteness::Complete => Completeness::Complete,
            CoverageCompleteness::Sample => Completeness::Sample,
            CoverageCompleteness::Unknown => Completeness::Unknown,
        }
    };
    let micros = |t: sinex_model::Timestamp| {
        i64::try_from(t.unix_ns() / 1000).expect("timestamps fit microseconds")
    };
    Some(Coverage {
        source: row.observed_source.clone(),
        span: Span::new(micros(row.started_at), micros(row.ended_at)),
        completeness,
        bounded_by: row.bounded_by.clone(),
    })
}

/// A rounded display cannot turn a partial observation into zero or full coverage.
pub fn percent_label(fraction: f64) -> String {
    let percent = (fraction * 100.0).round();
    if fraction > 0.0 && percent == 0.0 { return ">0%".into(); }
    if fraction < 1.0 && percent == 100.0 { return "<100%".into(); }
    format!("{percent:.0}%")
}

#[cfg(test)]
mod tests;
