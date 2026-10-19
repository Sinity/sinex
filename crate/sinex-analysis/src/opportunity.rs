//! Where observation opportunity comes from (spec `analysis.md` §3.2),
//! computed from records Sinex already keeps, never stored as events.
use crate::panel::{Completeness, Opportunity};
use crate::span::Span;
use serde::{Deserialize, Serialize};

/// How a source sees the world, which decides how its opportunity is bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Basis {
    /// Live capture: the lifetimes of the runs that captured it.
    Live,
    /// A polled store (a history database or file): from its earliest record
    /// to the last time Sinex read it; each read sees everything before it.
    Polled,
    /// An export: from its earliest record through its latest record (the
    /// export date is not stored separately yet, so the records declare it).
    Export,
}

/// What Sinex holds about one source's coverage.
#[derive(Clone, Debug, Default)]
pub struct Evidence {
    /// Earliest and latest `ts_orig` of the source's live events.
    pub records: Option<Span>,
    /// When Sinex last read the source's input.
    pub last_read: Option<i64>,
    /// The lifetimes of live-capture runs (an open run ends at its latest record).
    pub runs: Vec<Span>,
}

pub fn opportunity(
    source: &str,
    basis: Basis,
    completeness: Completeness,
    e: &Evidence,
) -> Vec<Opportunity> {
    let row = |span: Span, basis: &str| Opportunity {
        source: source.to_owned(),
        span,
        completeness,
        basis: basis.to_owned(),
    };
    match basis {
        Basis::Live => e.runs.iter().map(|s| row(*s, "run lifetime")).collect(),
        Basis::Polled => match (e.records, e.last_read) {
            (Some(r), Some(read)) => vec![row(
                Span::new(r.start, read.max(r.end)),
                "earliest record to last read",
            )],
            (Some(r), None) => vec![row(Span::new(r.start, r.end + 1), "records")],
            _ => Vec::new(),
        },
        Basis::Export => e
            .records
            .map(|r| row(Span::new(r.start, r.end + 1), "export records"))
            .into_iter()
            .collect(),
    }
}

#[cfg(test)]
mod tests;
