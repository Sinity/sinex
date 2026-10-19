//! Metric definitions and their aggregation laws (spec `analysis.md` §4.2,
//! `k4c.2`). A metric turns one relation into one number per unit: counters
//! sum only disjoint increments, ratios carry numerator and denominator,
//! means carry sum and count, durations union their intervals, quantiles and
//! distinct counts are recomputed per unit and never combined.
use crate::span::{MICROS_PER_SECOND, Span, SpanSet};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;

/// A literal in a predicate.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(untagged)]
pub enum Lit {
    Str(&'static str),
    Num(f64),
    Bool(bool),
}

/// A condition on a row's payload. Fields are top-level payload keys.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Predicate {
    Eq {
        field: &'static str,
        value: Lit,
    },
    Ne {
        field: &'static str,
        value: Lit,
    },
    /// Numeric comparison; a missing or non-numeric field never matches.
    Lt {
        field: &'static str,
        value: f64,
    },
    Le {
        field: &'static str,
        value: f64,
    },
    Gt {
        field: &'static str,
        value: f64,
    },
    Ge {
        field: &'static str,
        value: f64,
    },
    OneOf {
        field: &'static str,
        values: &'static [&'static str],
    },
    /// The field is present and not null.
    Present {
        field: &'static str,
    },
    Not {
        pred: &'static Predicate,
    },
    All {
        preds: &'static [Predicate],
    },
    Any {
        preds: &'static [Predicate],
    },
}

impl Predicate {
    pub fn matches(&self, payload: &Value) -> bool {
        let num = |f: &str| payload.get(f).and_then(Value::as_f64);
        match *self {
            Predicate::Eq { field, value } => lit_eq(payload.get(field), value),
            // A missing field is not "different": unknown never matches.
            Predicate::Ne { field, value } => payload
                .get(field)
                .is_some_and(|v| !v.is_null() && !lit_eq(Some(v), value)),
            Predicate::Lt { field, value } => num(field).is_some_and(|v| v < value),
            Predicate::Le { field, value } => num(field).is_some_and(|v| v <= value),
            Predicate::Gt { field, value } => num(field).is_some_and(|v| v > value),
            Predicate::Ge { field, value } => num(field).is_some_and(|v| v >= value),
            Predicate::OneOf { field, values } => payload
                .get(field)
                .and_then(Value::as_str)
                .is_some_and(|s| values.contains(&s)),
            Predicate::Present { field } => payload.get(field).is_some_and(|v| !v.is_null()),
            Predicate::Not { pred } => !pred.matches(payload),
            Predicate::All { preds } => preds.iter().all(|p| p.matches(payload)),
            Predicate::Any { preds } => preds.iter().any(|p| p.matches(payload)),
        }
    }
}

fn lit_eq(v: Option<&Value>, lit: Lit) -> bool {
    match (v, lit) {
        (Some(Value::String(s)), Lit::Str(l)) => s == l,
        (Some(v), Lit::Num(n)) => v.as_f64() == Some(n),
        (Some(Value::Bool(b)), Lit::Bool(l)) => *b == l,
        _ => false,
    }
}

/// What one unit of a metric is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitGrain {
    /// A logical day under the deployment's day setting.
    Day,
    /// A clock hour in the deployment's zone.
    Hour,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Quantity {
    Count,
    Seconds,
    Ratio,
    Milliseconds,
}

/// What a value is, so a proxy or a self-report is never shown as a measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceRole {
    Measured,
    SelfReport,
    Proxy,
    Modeled,
    Classification,
}

/// How a row's end is found, for relations whose rows are intervals.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IntervalShape {
    /// An RFC 3339 timestamp field holds the end.
    EndField { field: &'static str },
    /// An integer field holds the length.
    Length {
        field: &'static str,
        per_second: i64,
    },
}

/// A term of a ratio: a count of matching rows or a sum of a field.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Term {
    Count { filter: Option<&'static Predicate> },
    Sum { field: &'static str },
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "law", rename_all = "snake_case")]
pub enum AggLaw {
    Count,
    /// Sums a field whose increments do not overlap.
    SumDisjoint {
        field: &'static str,
        per_unit: f64,
    },
    /// The length of the union of the rows' intervals, in seconds.
    UnionDuration,
    /// Pooled Σnum / Σden, never a mean of ratios.
    Ratio {
        num: Term,
        den: Term,
    },
    Mean {
        field: &'static str,
    },
    Min {
        field: &'static str,
    },
    Max {
        field: &'static str,
    },
    /// Recomputed from rows per unit; never averaged across units.
    Quantile {
        field: &'static str,
        q: f64,
    },
    Distinct {
        field: &'static str,
    },
}

/// Where a row lands among units.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Allocation {
    /// The unit containing the row's time.
    Instant,
    /// An interval is clipped across every unit it overlaps.
    ClipInterval,
    AssignToStart,
    AssignToEnd,
}

/// Which sources' observation opportunity makes a unit observed.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "rule", content = "sources", rename_all = "snake_case")]
pub enum OpportunityRule {
    /// Every listed source must have been observing (intersection).
    Sources(&'static [&'static str]),
    /// Any listed source suffices (union): for fused kinds.
    AnyOf(&'static [&'static str]),
}

impl OpportunityRule {
    pub fn sources(&self) -> &'static [&'static str] {
        match *self {
            OpportunityRule::Sources(s) | OpportunityRule::AnyOf(s) => s,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct MetricDef {
    pub id: &'static str,
    /// Bumped whenever the same inputs would give another value.
    pub version: u32,
    pub description: &'static str,
    pub relation: &'static str,
    pub filter: Option<Predicate>,
    pub unit: UnitGrain,
    pub quantity: Quantity,
    pub law: AggLaw,
    pub interval: Option<IntervalShape>,
    pub allocation: Allocation,
    pub opportunity: OpportunityRule,
    /// Payload fields a panel may break the metric down by.
    pub dimensions: &'static [&'static str],
    pub role: SourceRole,
    /// Whether a prior-only deviation is computed for it.
    pub deviation: bool,
}

/// One input row: an event of the metric's relation.
#[derive(Clone, Copy, Debug)]
pub struct Row<'a> {
    pub source: &'a str,
    /// `ts_orig` in microseconds; rows with an unknown time never reach a metric.
    pub at: i64,
    pub payload: &'a Value,
}

impl MetricDef {
    /// The row's interval, when the metric's relation has interval rows.
    pub fn span_of(&self, row: &Row<'_>) -> Span {
        match self.interval {
            None => Span::new(row.at, row.at),
            Some(IntervalShape::EndField { field }) => {
                let end = row
                    .payload
                    .get(field)
                    .and_then(Value::as_str)
                    .and_then(parse_rfc3339_micros)
                    .unwrap_or(row.at);
                Span::new(row.at, end)
            }
            Some(IntervalShape::Length { field, per_second }) => {
                let len = row.payload.get(field).and_then(Value::as_i64).unwrap_or(0);
                let micros =
                    i128::from(len) * i128::from(MICROS_PER_SECOND) / i128::from(per_second);
                Span::new(
                    row.at,
                    row.at
                        .saturating_add(i64::try_from(micros).unwrap_or(i64::MAX)),
                )
            }
        }
    }

    /// The part of a row that falls in `unit`, or `None` when the row does not
    /// belong to the unit under the metric's allocation.
    pub fn allocate(&self, row: &Row<'_>, unit: Span) -> Option<Span> {
        let span = self.span_of(row);
        match self.allocation {
            Allocation::Instant | Allocation::AssignToStart => {
                unit.contains(span.start).then_some(span)
            }
            Allocation::AssignToEnd => {
                // An instant row's end is its start.
                let end = if span.is_empty() {
                    span.start
                } else {
                    span.end - 1
                };
                unit.contains(end).then_some(span)
            }
            Allocation::ClipInterval => {
                if span.is_empty() {
                    unit.contains(span.start).then_some(span)
                } else {
                    span.intersect(&unit)
                }
            }
        }
    }

    pub fn row_matches(&self, row: &Row<'_>) -> bool {
        self.filter.as_ref().is_none_or(|f| f.matches(row.payload))
    }
}

/// A metric's value over one unit, with what the law needs to combine units.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Aggregate {
    pub value: f64,
    pub numerator: Option<f64>,
    pub denominator: Option<f64>,
    pub n_rows: u64,
}

/// Apply the metric's law to the rows already allocated to one unit (each
/// with its part of the unit). `None` means the value is undefined here: a
/// ratio with a zero denominator, a mean, minimum or quantile of no rows.
pub fn aggregate(def: &MetricDef, rows: &[(Row<'_>, Span)]) -> Option<Aggregate> {
    let n_rows = rows.len() as u64;
    let field_values = |field: &str| -> Vec<f64> {
        rows.iter()
            .filter_map(|(r, _)| r.payload.get(field).and_then(Value::as_f64))
            .collect()
    };
    let simple = |value: f64| {
        Some(Aggregate {
            value,
            numerator: None,
            denominator: None,
            n_rows,
        })
    };
    match def.law {
        AggLaw::Count => simple(n_rows as f64),
        AggLaw::SumDisjoint { field, per_unit } => {
            simple(field_values(field).iter().sum::<f64>() / per_unit)
        }
        AggLaw::UnionDuration => {
            let set = SpanSet::from_spans(rows.iter().map(|(_, s)| *s));
            simple(set.len() as f64 / MICROS_PER_SECOND as f64)
        }
        AggLaw::Ratio { num, den } => {
            let n = term(num, rows);
            let d = term(den, rows);
            if d == 0.0 {
                return None;
            }
            Some(Aggregate {
                value: n / d,
                numerator: Some(n),
                denominator: Some(d),
                n_rows,
            })
        }
        AggLaw::Mean { field } => {
            let v = field_values(field);
            if v.is_empty() {
                return None;
            }
            let sum: f64 = v.iter().sum();
            Some(Aggregate {
                value: sum / v.len() as f64,
                numerator: Some(sum),
                denominator: Some(v.len() as f64),
                n_rows,
            })
        }
        AggLaw::Min { field } => field_values(field)
            .into_iter()
            .reduce(f64::min)
            .and_then(simple),
        AggLaw::Max { field } => field_values(field)
            .into_iter()
            .reduce(f64::max)
            .and_then(simple),
        AggLaw::Quantile { field, q } => {
            let mut v = field_values(field);
            if v.is_empty() {
                return None;
            }
            v.sort_by(f64::total_cmp);
            simple(quantile_sorted(&v, q))
        }
        AggLaw::Distinct { field } => {
            let set: BTreeSet<String> = rows
                .iter()
                .filter_map(|(r, _)| r.payload.get(field))
                .filter(|v| !v.is_null())
                .map(Value::to_string)
                .collect();
            simple(set.len() as f64)
        }
    }
}

fn term(t: Term, rows: &[(Row<'_>, Span)]) -> f64 {
    match t {
        Term::Count { filter } => rows
            .iter()
            .filter(|(r, _)| filter.is_none_or(|f| f.matches(r.payload)))
            .count() as f64,
        Term::Sum { field } => rows
            .iter()
            .filter_map(|(r, _)| r.payload.get(field).and_then(Value::as_f64))
            .sum(),
    }
}

/// Linear-interpolation quantile (type 7) of a sorted, non-empty slice.
pub fn quantile_sorted(v: &[f64], q: f64) -> f64 {
    let h = (v.len() - 1) as f64 * q.clamp(0.0, 1.0);
    let lo = h.floor() as usize;
    let hi = h.ceil() as usize;
    v[lo] + (h - lo as f64) * (v[hi] - v[lo])
}

/// Combine unit aggregates into one value over several units by the law
/// (a week from days, a pooled value across days). Quantiles and distinct
/// counts refuse: they are recomputed from rows.
pub fn combine(def: &MetricDef, parts: &[Aggregate]) -> Result<Option<f64>, &'static str> {
    if parts.is_empty() {
        return Ok(None);
    }
    match def.law {
        AggLaw::Count | AggLaw::SumDisjoint { .. } | AggLaw::UnionDuration => {
            Ok(Some(parts.iter().map(|p| p.value).sum()))
        }
        AggLaw::Ratio { .. } | AggLaw::Mean { .. } => {
            let n: f64 = parts.iter().filter_map(|p| p.numerator).sum();
            let d: f64 = parts.iter().filter_map(|p| p.denominator).sum();
            Ok((d != 0.0).then(|| n / d))
        }
        AggLaw::Min { .. } => Ok(parts.iter().map(|p| p.value).reduce(f64::min)),
        AggLaw::Max { .. } => Ok(parts.iter().map(|p| p.value).reduce(f64::max)),
        AggLaw::Quantile { .. } | AggLaw::Distinct { .. } => {
            Err("quantiles and distinct counts are recomputed from rows, never combined")
        }
    }
}

/// Parse an RFC 3339 timestamp into microseconds since the epoch.
pub fn parse_rfc3339_micros(s: &str) -> Option<i64> {
    let t = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok()?;
    i64::try_from(t.unix_timestamp_nanos() / 1000).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const EXIT_NONZERO: Predicate = Predicate::Ne {
        field: "exit_code",
        value: Lit::Num(0.0),
    };

    fn def(law: AggLaw) -> MetricDef {
        MetricDef {
            id: "t",
            version: 1,
            description: "",
            relation: "r",
            filter: None,
            unit: UnitGrain::Day,
            quantity: Quantity::Count,
            law,
            interval: None,
            allocation: Allocation::Instant,
            opportunity: OpportunityRule::AnyOf(&["s"]),
            dimensions: &[],
            role: SourceRole::Measured,
            deviation: false,
        }
    }

    fn rows(payloads: &[Value]) -> Vec<(Row<'_>, Span)> {
        payloads
            .iter()
            .map(|p| {
                (
                    Row {
                        source: "s",
                        at: 0,
                        payload: p,
                    },
                    Span::new(0, 0),
                )
            })
            .collect()
    }

    #[test]
    fn ratios_pool_numerators_and_denominators() {
        let law = AggLaw::Ratio {
            num: Term::Count {
                filter: Some(&EXIT_NONZERO),
            },
            den: Term::Count { filter: None },
        };
        let d = def(law);
        let day1 = [json!({"exit_code": 1}), json!({"exit_code": 0})];
        let day2 = [
            json!({"exit_code": 0}),
            json!({"exit_code": 0}),
            json!({"exit_code": 0}),
            json!({"exit_code": 0}),
            json!({"exit_code": null}),
            json!({"exit_code": 2}),
        ];
        let a = aggregate(&d, &rows(&day1)).unwrap();
        let b = aggregate(&d, &rows(&day2)).unwrap();
        assert_eq!(a.value, 0.5);
        assert!((b.value - 1.0 / 6.0).abs() < 1e-12);
        // Pooled 2/8, not the mean of 0.5 and 1/6.
        assert_eq!(combine(&d, &[a, b]).unwrap(), Some(0.25));
    }

    #[test]
    fn an_unknown_exit_code_is_not_a_failure() {
        assert!(!EXIT_NONZERO.matches(&json!({"exit_code": null})));
        assert!(!EXIT_NONZERO.matches(&json!({})));
        assert!(EXIT_NONZERO.matches(&json!({"exit_code": 127})));
    }

    #[test]
    fn a_zero_denominator_is_undefined_not_zero() {
        let law = AggLaw::Ratio {
            num: Term::Count { filter: None },
            den: Term::Sum { field: "n" },
        };
        assert_eq!(aggregate(&def(law), &rows(&[json!({"n": 0})])), None);
        assert_eq!(aggregate(&def(AggLaw::Mean { field: "x" }), &[]), None);
        // A count of nothing is an observed zero.
        assert_eq!(aggregate(&def(AggLaw::Count), &[]).unwrap().value, 0.0);
    }

    #[test]
    fn overlapping_intervals_are_unioned() {
        let mut d = def(AggLaw::UnionDuration);
        d.interval = Some(IntervalShape::EndField { field: "ended_at" });
        d.allocation = Allocation::ClipInterval;
        let p1 = json!({"ended_at": "1970-01-01T00:00:10Z"});
        let p2 = json!({"ended_at": "1970-01-01T00:00:15Z"});
        let r1 = Row {
            source: "s",
            at: 0,
            payload: &p1,
        };
        let r2 = Row {
            source: "s",
            at: 5_000_000,
            payload: &p2,
        };
        let unit = Span::new(0, 12_000_000);
        let allocated: Vec<_> = [r1, r2]
            .into_iter()
            .filter_map(|r| d.allocate(&r, unit).map(|s| (r, s)))
            .collect();
        // [0,10) ∪ [5,12) clipped to the unit = 12 s, not 10 + 7.
        assert_eq!(aggregate(&d, &allocated).unwrap().value, 12.0);
    }

    #[test]
    fn quantiles_refuse_to_combine() {
        let d = def(AggLaw::Quantile { field: "x", q: 0.5 });
        let a = aggregate(&d, &rows(&[json!({"x": 1}), json!({"x": 3})])).unwrap();
        assert_eq!(a.value, 2.0);
        assert!(combine(&d, &[a]).is_err());
        assert_eq!(quantile_sorted(&[1.0, 2.0, 3.0, 4.0], 0.25), 1.75);
    }

    #[test]
    fn assign_to_end_credits_the_unit_the_interval_ends_in() {
        let mut d = def(AggLaw::Count);
        d.interval = Some(IntervalShape::Length {
            field: "ms",
            per_second: 1000,
        });
        d.allocation = Allocation::AssignToEnd;
        let p = json!({"ms": 10_000});
        let r = Row {
            source: "s",
            at: 5_000_000,
            payload: &p,
        };
        assert!(d.allocate(&r, Span::new(0, 10_000_000)).is_none());
        assert!(d.allocate(&r, Span::new(10_000_000, 20_000_000)).is_some());
    }
}
