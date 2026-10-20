//! Findings (spec `analysis.md` §8): typed statements bound to a result, with
//! slots filled from the result, an epistemic role, a support status, typed
//! caveats and separate checks. Rendering comes from the slots, never from
//! improvised text, and there is no free-floating confidence number.
use crate::cell::Cell;
use crate::methods::MethodResult;
use crate::panel::PanelCell;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    MeasuredFact,
    DerivedStatistic,
    Association,
    SelfReport,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Support {
    Supported,
    PartiallySupported,
    Contested,
    NotFoundWithCoverage,
    Unknown,
}

/// A typed caveat that travels with its claim.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Caveat {
    /// The unit was only partly observed.
    PartialCoverage { observed_fraction: f64 },
    /// The value is a lower bound: the source's coverage is unknown.
    LowerBound,
    /// The source sees a subset (one profile, these shells).
    Sample { sources: Vec<String> },
    /// The result is exploratory: the family was not fixed in advance.
    Exploratory,
    /// Few units: the minimum detectable effect is large.
    SmallN {
        n: u64,
        min_detectable_r: Option<f64>,
    },
    /// The day is not settled yet.
    Provisional,
    /// Units excluded from the population, by reason.
    Excluded { units: u64 },
}

/// Typed predicates with slots (`analysis.md` §8.2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "template", rename_all = "snake_case")]
pub enum Template {
    MetricValue {
        metric: String,
        unit: String,
        value: f64,
        state: String,
        description: String,
    },
    MetricChange {
        metric: String,
        unit_a: String,
        a: f64,
        unit_b: String,
        b: f64,
    },
    Association {
        x: String,
        y: String,
        lag: i64,
        r: f64,
        ci95: Option<[f64; 2]>,
        n: u64,
        n_eff: f64,
        q: Option<f64>,
        family_size: usize,
    },
    LevelShift {
        metric: String,
        boundary: String,
        before: f64,
        after: f64,
        method: String,
    },
    Deviation {
        metric: String,
        unit: String,
        value: f64,
        median: f64,
        distance: f64,
    },
    NotFound {
        what: String,
        scope: String,
    },
    Missing {
        metric: String,
        unit: String,
        reason: String,
    },
    /// A monotonic trend: Sen's slope per unit with the Mann-Kendall p.
    Trend {
        metric: String,
        slope_per_unit: f64,
        p: f64,
        from: String,
        to: String,
        units: u64,
    },
    /// How much of a unit a source covered (data quality comes first in a brief).
    Coverage {
        source: String,
        unit: String,
        observed_fraction: f64,
        completeness: String,
        bounded_by: String,
    },
}

/// Separate checks, never one green tick: a reader sees which check a
/// finding passed and which it did not.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Check {
    Passed,
    Failed,
    /// Coverage short of what the claim needs; the claim carries a caveat.
    Caveated,
    /// Deterministic templates pass semantics by construction.
    ByConstruction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checks {
    pub references: Check,
    pub calculation: Check,
    pub coverage: Check,
    pub semantics: Check,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    pub template: Template,
    pub role: Role,
    pub status: Support,
    pub caveats: Vec<Caveat>,
    /// Citations of the cells or results the slots came from.
    pub evidence: Vec<String>,
    pub checks: Checks,
}

impl Finding {
    /// The sort key within a section: support status, then effect size
    /// relative to spread, so two compilations of the same results rank alike.
    pub fn rank(&self) -> (Support, i64) {
        let effect = match &self.template {
            Template::Association { r, .. } => r.abs(),
            Template::Deviation { distance, .. } => distance.abs() / 10.0,
            Template::LevelShift { before, after, .. } => {
                ((after - before) / before.abs().max(1.0)).abs()
            }
            Template::MetricChange { a, b, .. } => ((b - a) / a.abs().max(1.0)).abs(),
            Template::Trend { p, .. } => 1.0 - p,
            Template::MetricValue { .. }
            | Template::NotFound { .. }
            | Template::Missing { .. }
            | Template::Coverage { .. } => 0.0,
        };
        (self.status, -(effect * 1e6) as i64)
    }

    /// The finding with its caveats, in Markdown. A finding is rendered whole
    /// or not at all.
    pub fn markdown(&self) -> String {
        let mut s = match &self.template {
            Template::MetricValue {
                metric,
                unit,
                value,
                state,
                description,
            } => {
                let at_least = if state == "recorded" { "at least " } else { "" };
                format!(
                    "{description} ({metric}) on {unit}: {at_least}{}",
                    num(*value)
                )
            }
            Template::MetricChange {
                metric,
                unit_a,
                a,
                unit_b,
                b,
            } => {
                format!(
                    "{metric}: {} on {unit_b}, against {} on {unit_a}",
                    num(*b),
                    num(*a)
                )
            }
            Template::Association {
                x,
                y,
                lag,
                r,
                ci95,
                n,
                n_eff,
                q,
                family_size,
            } => {
                let mut t = format!(
                    "{x} and {y} (lag {lag}) are associated, r = {r:.2} over {n} units (effective {n_eff:.1})"
                );
                if let Some([lo, hi]) = ci95 {
                    let _ = write!(t, ", 95% interval {lo:.2} to {hi:.2}");
                }
                if let Some(q) = q {
                    let _ = write!(t, ", q = {q:.3} across {family_size} tested pairs");
                }
                t
            }
            Template::LevelShift {
                metric,
                boundary,
                before,
                after,
                method,
            } => format!(
                "{metric} shifted from about {} to about {} at {boundary} ({method})",
                num(*before),
                num(*after)
            ),
            Template::Deviation {
                metric,
                unit,
                value,
                median,
                distance,
            } => format!(
                "{metric} on {unit} was {}, against a median of {} over the prior window ({distance:+.1} robust units)",
                num(*value),
                num(*median)
            ),
            Template::NotFound { what, scope } => format!("No {what} in {scope}"),
            Template::Missing {
                metric,
                unit,
                reason,
            } => {
                format!(
                    "{metric} on {unit} is not known: {}",
                    reason.replace('_', " ")
                )
            }
            Template::Trend {
                metric,
                slope_per_unit,
                p,
                from,
                to,
                units,
            } => format!(
                "{metric} {} by {:.2} per day from {from} to {to} ({units} days, Mann-Kendall p = {p:.3})",
                if *slope_per_unit >= 0.0 {
                    "rose"
                } else {
                    "fell"
                },
                slope_per_unit.abs()
            ),
            Template::Coverage {
                source,
                unit,
                observed_fraction,
                completeness,
                bounded_by,
            } => {
                let seen = if *observed_fraction <= 0.0 {
                    "not observed".to_owned()
                } else {
                    format!("{:.0}% observed", observed_fraction * 100.0)
                };
                format!("{source} on {unit}: {seen} ({completeness}; {bounded_by})")
            }
        };
        for c in &self.caveats {
            let note = match c {
                Caveat::PartialCoverage { observed_fraction } => {
                    format!(
                        "only {:.0}% of the unit was observed",
                        observed_fraction * 100.0
                    )
                }
                Caveat::LowerBound => "a lower bound: the source's coverage is unknown".into(),
                Caveat::Sample { sources } => format!("on {}", sources.join(", ")),
                Caveat::Exploratory => "exploratory".into(),
                Caveat::SmallN {
                    n,
                    min_detectable_r,
                } => match min_detectable_r {
                    Some(m) => format!("{n} units; effects below r = {m:.2} would go undetected"),
                    None => format!("{n} units"),
                },
                Caveat::Provisional => "the day is not over".into(),
                Caveat::Excluded { units } => format!("{units} unit(s) excluded for missing data"),
            };
            let _ = write!(s, " [{note}]");
        }
        s
    }
}

fn num(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v:.2}")
    }
}

/// The finding for one panel cell: a measured value, a lower bound, or the
/// reason it is unknown.
pub fn from_cell(
    cell: &PanelCell,
    description: &str,
    sources: &[&str],
    provisional: bool,
) -> Finding {
    let citation = format!(
        "sinex:metric/{}@{}/{}",
        cell.metric, cell.metric_version, cell.unit.label
    );
    let mut caveats = Vec::new();
    if provisional {
        caveats.push(Caveat::Provisional);
    }
    let (template, status, coverage) = match &cell.cell {
        Cell::Missing { reason } => (
            Template::Missing {
                metric: cell.metric.to_owned(),
                unit: cell.unit.label.clone(),
                reason: serde_json::to_value(reason)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default(),
            },
            Support::Unknown,
            Check::Caveated,
        ),
        other => {
            let value = *other.value().expect("non-missing cells have values");
            let coverage = match other {
                Cell::Partial {
                    observed_fraction, ..
                } => {
                    caveats.push(Caveat::PartialCoverage {
                        observed_fraction: *observed_fraction,
                    });
                    Check::Caveated
                }
                Cell::Recorded { .. } => {
                    caveats.push(Caveat::LowerBound);
                    Check::Caveated
                }
                _ => Check::Passed,
            };
            if sources.len() > 1 {
                caveats.push(Caveat::Sample {
                    sources: sources.iter().map(|s| (*s).to_owned()).collect(),
                });
            }
            (
                Template::MetricValue {
                    metric: cell.metric.to_owned(),
                    unit: cell.unit.label.clone(),
                    value,
                    state: other.state_name().to_owned(),
                    description: description.to_owned(),
                },
                if coverage == Check::Passed {
                    Support::Supported
                } else {
                    Support::PartiallySupported
                },
                coverage,
            )
        }
    };
    Finding {
        template,
        role: Role::MeasuredFact,
        status,
        caveats,
        evidence: vec![citation],
        checks: Checks {
            references: Check::Passed,
            calculation: Check::Passed,
            coverage,
            semantics: Check::ByConstruction,
        },
    }
}

/// The association finding for a correlation result inside a family.
pub fn from_correlation(
    result: &MethodResult,
    q: Option<f64>,
    family_size: usize,
    result_id: &str,
) -> Option<Finding> {
    let o = &result.output;
    let r = o["r"].as_f64()?;
    let n_eff = o["n_eff"].as_f64()?;
    let ci95 = o["ci95"]
        .as_array()
        .and_then(|a| Some([a.first()?.as_f64()?, a.get(1)?.as_f64()?]));
    let mdr = result.diagnostics["min_detectable_r"].as_f64();
    let mut caveats = vec![Caveat::Exploratory];
    if mdr.is_some_and(|m| m > 0.5) {
        caveats.push(Caveat::SmallN {
            n: result.n,
            min_detectable_r: mdr,
        });
    }
    if result.n_excluded.total() > 0 {
        caveats.push(Caveat::Excluded {
            units: result.n_excluded.total(),
        });
    }
    Some(Finding {
        template: Template::Association {
            x: o["x"].as_str()?.to_owned(),
            y: o["y"].as_str()?.to_owned(),
            lag: o["lag"].as_i64()?,
            r,
            ci95,
            n: result.n,
            n_eff,
            q,
            family_size,
        },
        role: Role::Association,
        status: Support::Supported,
        caveats,
        evidence: vec![format!("sinex:result/{result_id}")],
        checks: Checks {
            references: Check::Passed,
            calculation: Check::Passed,
            coverage: if result.n_excluded.total() > 0 {
                Check::Caveated
            } else {
                Check::Passed
            },
            semantics: Check::ByConstruction,
        },
    })
}

#[cfg(test)]
mod tests;
