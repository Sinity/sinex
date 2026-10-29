//! Findings: typed statements with slots filled from figures and method
//! results, typed caveats, and the events they cite. Rendering comes from the
//! slots, never from improvised text, and there is no free-floating
//! confidence number.
use crate::cell::Cell;
use crate::methods::MethodResult;
use crate::panel::PanelCell;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

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

/// Statement templates with slots.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "template", rename_all = "snake_case")]
pub enum Template {
    Observation {
        event_type: String,
        source: String,
        when: Option<String>,
        description: String,
    },
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    pub template: Template,
    pub caveats: Vec<Caveat>,
    /// Citations of the figures the slots came from.
    pub evidence: Vec<String>,
}

impl Finding {
    /// The sort key within a section: fully observed statements before those
    /// over partly observed or lower-bound values, before unknown values, then
    /// effect size relative to spread, so two compilations of the same results
    /// rank alike.
    pub fn rank(&self) -> (u8, i64) {
        let effect = match &self.template {
            Template::Association { r, .. } => r.abs(),
            Template::Deviation { distance, .. } => distance.abs() / 10.0,
            Template::LevelShift { before, after, .. } => {
                ((after - before) / before.abs().max(1.0)).abs()
            }
            Template::MetricChange { a, b, .. } => ((b - a) / a.abs().max(1.0)).abs(),
            Template::Trend { p, .. } => 1.0 - p,
            Template::Observation { .. }
            | Template::MetricValue { .. }
            | Template::NotFound { .. }
            | Template::Missing { .. }
            | Template::Coverage { .. } => 0.0,
        };
        let short_of_coverage = self
            .caveats
            .iter()
            .any(|c| matches!(c, Caveat::PartialCoverage { .. } | Caveat::LowerBound));
        let tier = match &self.template {
            Template::Missing { .. } => 2,
            _ if short_of_coverage => 1,
            _ => 0,
        };
        (tier, -(effect * 1e6) as i64)
    }

    /// The finding with its caveats, in Markdown. A finding is rendered whole
    /// or not at all.
    pub fn markdown(&self) -> String {
        let mut s = match &self.template {
            Template::Observation { event_type, source, when, description } => format!(
                "{description} ({event_type}, {source}{})",
                when.as_ref().map(|at| format!(", {at}")).unwrap_or_default()),
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
                    format!("{} observed", crate::coverage::percent_label(*observed_fraction))
                };
                format!("{source} on {unit}: {seen} ({completeness}; {bounded_by})")
            }
        };
        for c in &self.caveats {
            let note = match c {
                Caveat::PartialCoverage { observed_fraction } => {
                    format!(
                        "only {} of the unit was observed",
                        crate::coverage::percent_label(*observed_fraction)
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
        one_line(&s)
    }
}

fn one_line(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\r' => output.push_str("\\r"),
            '\n' => output.push_str("\\n"),
            '\u{85}' => output.push_str("\\u{85}"),
            '\u{2028}' => output.push_str("\\u{2028}"),
            '\u{2029}' => output.push_str("\\u{2029}"),
            character => output.push(character),
        }
    }
    output
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
    let mut caveats = Vec::new();
    if provisional {
        caveats.push(Caveat::Provisional);
    }
    let template = match &cell.cell {
        Cell::Missing { reason } => Template::Missing {
            metric: cell.metric.to_owned(),
            unit: cell.unit.label.clone(),
            reason: serde_json::to_value(reason)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default(),
        },
        other => {
            let value = *other.value().expect("non-missing cells have values");
            match other {
                Cell::Partial {
                    observed_fraction, ..
                } => caveats.push(Caveat::PartialCoverage {
                    observed_fraction: *observed_fraction,
                }),
                Cell::Recorded { .. } => caveats.push(Caveat::LowerBound),
                _ => {}
            }
            if sources.len() > 1 {
                caveats.push(Caveat::Sample {
                    sources: sources.iter().map(|s| (*s).to_owned()).collect(),
                });
            }
            Template::MetricValue {
                metric: cell.metric.to_owned(),
                unit: cell.unit.label.clone(),
                value,
                state: other.state_name().to_owned(),
                description: description.to_owned(),
            }
        }
    };
    Finding {
        template,
        caveats,
        evidence: cell.reference.iter().cloned().collect(),
    }
}

/// The association finding for a correlation result inside a family.
pub fn from_correlation(
    result: &MethodResult,
    q: Option<f64>,
    family_size: usize,
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
        caveats,
        evidence: Vec::new(),
    })
}

#[cfg(test)]
mod tests;
