//! Panels: units by metrics, each cell with its observation state (spec
//! `analysis.md` §3, §4.4). A unit is observed when the opportunity of the
//! metric's sources covers it, partial when it covers part, and missing with
//! its reason otherwise; a zero is only ever a measured zero.
use crate::cell::{Cell, Missing};
use crate::metric::{Aggregate, MetricDef, OpportunityRule, Row, UnitGrain, aggregate};
use crate::span::{MICROS_PER_SECOND, Span, SpanSet};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::Timestamp;
use sinex_model::civil::{Civil, CivilError};
use std::collections::{BTreeMap, BTreeSet};

/// How much of what a source records it sees in its opportunity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Completeness {
    /// Everything of this kind (a shell history for commands typed on that host).
    Complete,
    /// A subset (one browser profile, not all browsing).
    Sample,
    /// Whether the source saw the whole span is unknown.
    Unknown,
}

/// One stretch in which a source could have recorded what it records.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Opportunity {
    pub source: String,
    pub span: Span,
    pub completeness: Completeness,
    /// Where the bounds came from (run lifetime, poll, export records).
    pub basis: String,
}

/// One unit: a logical day or a clock hour.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Unit {
    pub label: String,
    pub span: Span,
}

fn micros(t: Timestamp) -> i64 {
    i64::try_from(t.unix_ns() / 1000).unwrap_or(i64::MAX)
}

fn from_micros(us: i64) -> Result<Timestamp, CivilError> {
    Timestamp::from_unix_ns(i128::from(us) * 1000)
        .map_err(|e| CivilError::OutOfRange(e.to_string()))
}

/// The units of `grain` covering `[from, to)` under the day setting.
pub fn units(civil: &Civil, grain: UnitGrain, from: Span) -> Result<Vec<Unit>, CivilError> {
    let mut out = Vec::new();
    match grain {
        UnitGrain::Day => {
            let mut day = civil.day(from_micros(from.start)?)?;
            loop {
                let start = micros(civil.day_start(day)?);
                if start >= from.end {
                    break;
                }
                let end = micros(civil.day_end(day)?);
                out.push(Unit {
                    label: day.to_string(),
                    span: Span::new(start, end),
                });
                day = day
                    .tomorrow()
                    .map_err(|e| CivilError::OutOfRange(e.to_string()))?;
            }
        }
        UnitGrain::Hour => {
            let mut start = micros(civil.hour_start(from_micros(from.start)?)?);
            while start < from.end {
                let end = micros(civil.hour_end(from_micros(start)?)?);
                out.push(Unit {
                    label: format!("{}", from_micros(start)?.0),
                    span: Span::new(start, end),
                });
                start = end;
            }
        }
    }
    Ok(out)
}

/// The opportunity a metric's rule gives, and whether any of it is of unknown
/// completeness (which makes recorded rows lower bounds).
pub fn opportunity_for(rule: &OpportunityRule, rows: &[Opportunity]) -> (SpanSet, bool) {
    let per_source = |src: &str, known: bool| {
        SpanSet::from_spans(
            rows.iter()
                .filter(|o| o.source == src && (o.completeness != Completeness::Unknown) == known)
                .map(|o| o.span),
        )
    };
    let unknown = rule
        .sources()
        .iter()
        .any(|s| !per_source(s, false).is_empty());
    let set = match rule {
        OpportunityRule::AnyOf(sources) => sources
            .iter()
            .fold(SpanSet::default(), |acc, s| acc.union(&per_source(s, true))),
        OpportunityRule::Sources(sources) => {
            let mut it = sources.iter();
            match it.next() {
                None => SpanSet::default(),
                Some(first) => it.fold(per_source(first, true), |acc, s| {
                    acc.intersect(&per_source(s, true))
                }),
            }
        }
    };
    (set, unknown)
}

/// One cell of a panel.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PanelCell {
    pub metric: &'static str,
    pub metric_version: u32,
    pub unit: Unit,
    /// The breakdown dimension and its value; empty for the total.
    pub dimension: String,
    pub dimension_value: String,
    pub cell: Cell<f64>,
    pub numerator: Option<f64>,
    pub denominator: Option<f64>,
    pub n_rows: u64,
    pub observed_seconds: f64,
}

/// A unit counts as fully observed when coverage misses less than this.
const OBSERVED_FRACTION: f64 = 0.999;

/// The cells of one metric over `units`, from the metric's rows, the
/// opportunity relation and the recorded gaps. One total cell per unit, plus
/// one per value of each declared dimension.
pub fn compute(
    def: &MetricDef,
    units: &[Unit],
    rows: &[Row<'_>],
    opportunity: &[Opportunity],
    gaps: &SpanSet,
) -> Vec<PanelCell> {
    let (opp, unknown_coverage) = opportunity_for(&def.opportunity, opportunity);
    let rows: Vec<&Row<'_>> = rows
        .iter()
        .filter(|r| r.payload.is_object() && def.row_matches(r))
        .collect();
    let mut out = Vec::new();
    for unit in units {
        let covered = opp.clip(unit.span).subtract(gaps);
        let gap = gaps.clip(unit.span);
        let fraction = covered.len() as f64 / unit.span.len().max(1) as f64;
        let allocated: Vec<(Row<'_>, Span)> = rows
            .iter()
            .filter_map(|r| def.allocate(r, unit.span).map(|s| (**r, s)))
            .collect();
        let state = |agg: Option<Aggregate>, has_rows: bool| -> Cell<f64> {
            if fraction <= 0.0 {
                return match (has_rows, agg.as_ref()) {
                    (true, Some(a)) => Cell::Recorded { value: a.value },
                    _ if gap.len() >= unit.span.len() => Cell::Missing {
                        reason: Missing::Gap,
                    },
                    _ if unknown_coverage => Cell::Missing {
                        reason: Missing::CoverageUnknown,
                    },
                    _ => Cell::Missing {
                        reason: Missing::NoOpportunity,
                    },
                };
            }
            match agg {
                None => Cell::Missing {
                    reason: Missing::NotApplicable,
                },
                Some(a) if fraction >= OBSERVED_FRACTION => Cell::Observed { value: a.value },
                Some(a) => Cell::Partial {
                    value: a.value,
                    observed_fraction: (fraction * 1000.0).round() / 1000.0,
                },
            }
        };
        let mut push = |dimension: &str, value: &str, part: &[(Row<'_>, Span)]| {
            let agg = aggregate(def, part);
            out.push(PanelCell {
                metric: def.id,
                metric_version: def.version,
                unit: unit.clone(),
                dimension: dimension.to_owned(),
                dimension_value: value.to_owned(),
                numerator: agg.as_ref().and_then(|a| a.numerator),
                denominator: agg.as_ref().and_then(|a| a.denominator),
                n_rows: part.len() as u64,
                observed_seconds: covered.len() as f64 / MICROS_PER_SECOND as f64,
                cell: state(agg, !part.is_empty()),
            });
        };
        push("", "", &allocated);
        for dim in def.dimensions {
            let mut groups: BTreeMap<String, Vec<(Row<'_>, Span)>> = BTreeMap::new();
            for (r, s) in &allocated {
                let v = match r.payload.get(*dim) {
                    Some(Value::String(s)) => s.clone(),
                    Some(v) if !v.is_null() => v.to_string(),
                    _ => "unknown".to_owned(),
                };
                groups.entry(v).or_default().push((*r, *s));
            }
            for (v, part) in &groups {
                push(dim, v, part);
            }
        }
    }
    out
}

/// The distinct dimension values a set of cells holds, for one dimension.
pub fn dimension_values(cells: &[PanelCell], dimension: &str) -> BTreeSet<String> {
    cells
        .iter()
        .filter(|c| c.dimension == dimension)
        .map(|c| c.dimension_value.clone())
        .collect()
}

#[cfg(test)]
mod tests;
