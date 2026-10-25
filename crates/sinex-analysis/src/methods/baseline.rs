//! Robust baselines (median and MAD×1.4826) and deviations from them. "28 days"
//! is calendar days, and the minimum counts observed units, never samples. A
//! prior-only baseline for a unit reads only units before it, so a later unit
//! never changes it.
use super::stats::{median, scaled_mad};
use super::{Dependence, Estimand, Excluded, Method, MethodInput, MethodResult, NotEstimable};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BaselineParams {
    pub metric: String,
    /// The baseline window in calendar units, ending before `at` (prior-only)
    /// or covering the whole series (retrospective) when `at` is absent.
    #[serde(default = "default_window")]
    pub window_units: usize,
    /// The unit the prior-only window ends before.
    #[serde(default)]
    pub at: Option<String>,
    #[serde(default = "default_min_observed")]
    pub min_observed: usize,
}

fn default_window() -> usize {
    28
}

fn default_min_observed() -> usize {
    14
}

pub struct RobustBaseline;

impl Method for RobustBaseline {
    const ID: &'static str = "robust_baseline";
    const VERSION: u32 = 1;
    const SUMMARY: &'static str =
        "median and MAD×1.4826 over a declared calendar window (prior-only or retrospective)";
    type Params = BaselineParams;

    fn estimand(p: &Self::Params) -> Estimand {
        Estimand {
            unit: "panel unit",
            population: match &p.at {
                Some(at) => format!(
                    "eligible {} units in the {} units before {at}",
                    p.metric, p.window_units
                ),
                None => format!(
                    "eligible {} units in the last {} units of the range",
                    p.metric, p.window_units
                ),
            },
            question: format!("typical level and spread of {}", p.metric),
            dependence: Dependence::Independent,
            causal: false,
        }
    }

    fn run(p: &Self::Params, input: &MethodInput) -> Result<MethodResult, NotEstimable> {
        let points = input.get(&p.metric)?;
        let end = match &p.at {
            Some(at) => points
                .iter()
                .position(|q| &q.unit == at)
                .ok_or_else(|| NotEstimable::new(format!("unit {at} is not in the series")))?,
            None => points.len(),
        };
        let window = &points[end.saturating_sub(p.window_units)..end];
        let (values, excluded) = input.eligible(window);
        let values: Vec<f64> = values.into_iter().map(|(_, v)| v).collect();
        let b = baseline(&values, p.min_observed)
            .map_err(|r| NotEstimable::new(r).with_counts(values.len() as u64, excluded.clone()))?;
        Ok(MethodResult {
            method: Self::ID,
            version: Self::VERSION,
            estimand: Self::estimand(p),
            output: json!({
                "metric": p.metric, "median": b.0, "scaled_mad": b.1,
                "window": {"from": window.first().map(|q| &q.unit), "to": window.last().map(|q| &q.unit), "units": window.len()},
                "prior_only": p.at.is_some(),
            }),
            n: values.len() as u64,
            n_excluded: excluded,
            diagnostics: json!({}),
        })
    }
}

fn baseline(values: &[f64], min_observed: usize) -> Result<(f64, f64), String> {
    if values.len() < min_observed {
        return Err(format!(
            "{} observed units in the window, {min_observed} needed",
            values.len()
        ));
    }
    match (median(values), scaled_mad(values)) {
        (Some(m), Some(s)) => Ok((m, s)),
        _ => Err("no values".into()),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnomalyParams {
    pub metric: String,
    #[serde(default = "default_window")]
    pub window_units: usize,
    #[serde(default = "default_min_observed")]
    pub min_observed: usize,
    /// |value − median| / scaled MAD at or above this is flagged.
    #[serde(default = "default_threshold")]
    pub threshold: f64,
}

fn default_threshold() -> f64 {
    3.5
}

pub struct Anomaly;

impl Method for Anomaly {
    const ID: &'static str = "anomaly";
    const VERSION: u32 = 1;
    const SUMMARY: &'static str =
        "each unit's distance from its own prior-only robust baseline, MAD rule";
    type Params = AnomalyParams;

    fn estimand(p: &Self::Params) -> Estimand {
        Estimand {
            unit: "panel unit",
            population: format!(
                "eligible {} units with at least {} eligible units in the {} before",
                p.metric, p.min_observed, p.window_units
            ),
            question: format!("units where {} departs from its recent level", p.metric),
            dependence: Dependence::Independent,
            causal: false,
        }
    }

    fn run(p: &Self::Params, input: &MethodInput) -> Result<MethodResult, NotEstimable> {
        let points = input.get(&p.metric)?;
        let mut units = Vec::new();
        let mut excluded = Excluded::default();
        let mut too_few_prior = 0u64;
        let mut zero_spread = 0u64;
        for (i, point) in points.iter().enumerate() {
            let value = match point.cell.eligible(input.eligibility) {
                Ok(v) => v,
                Err(e) => {
                    excluded.add(e);
                    continue;
                }
            };
            let (prior, _) = input.eligible(&points[i.saturating_sub(p.window_units)..i]);
            let prior: Vec<f64> = prior.into_iter().map(|(_, v)| v).collect();
            let Ok((m, s)) = baseline(&prior, p.min_observed) else {
                too_few_prior += 1;
                continue;
            };
            if s == 0.0 {
                zero_spread += 1;
                continue;
            }
            let z = (value - m) / s;
            units.push(json!({
                "unit": point.unit, "value": value, "median": m, "scaled_mad": s,
                "distance": z, "flagged": z.abs() >= p.threshold,
            }));
        }
        if units.is_empty() {
            return Err(NotEstimable::new(format!(
                "no unit has a baseline: {too_few_prior} with too few prior units, {zero_spread} with zero spread"
            ))
            .with_counts(0, excluded));
        }
        let flagged = units.iter().filter(|u| u["flagged"] == true).count();
        Ok(MethodResult {
            method: Self::ID,
            version: Self::VERSION,
            estimand: Self::estimand(p),
            output: json!({"metric": p.metric, "threshold": p.threshold, "units": units, "flagged": flagged}),
            n: units.len() as u64,
            n_excluded: excluded,
            diagnostics: json!({"too_few_prior_units": too_few_prior, "zero_spread": zero_spread}),
        })
    }
}

#[cfg(test)]
mod tests;
