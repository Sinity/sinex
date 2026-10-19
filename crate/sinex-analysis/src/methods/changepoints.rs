//! Change points in a piecewise-constant mean, by binary segmentation with a
//! BIC penalty of log(n)·σ². σ² is estimated robustly from first differences,
//! so the steps themselves do not inflate it. A detected boundary stays where
//! it was detected; nearby known events are a separate annotation
//! (`analysis.md` §6.4).
use super::stats::{mean, scaled_mad};
use super::{Dependence, Estimand, Method, MethodInput, MethodResult, NotEstimable, dense};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangePointParams {
    pub metric: String,
    /// Change points at least this many units apart (and from either end).
    #[serde(default = "default_min_segment")]
    pub min_segment: usize,
    #[serde(default = "default_max_points")]
    pub max_points: usize,
}

fn default_min_segment() -> usize {
    7
}

fn default_max_points() -> usize {
    10
}

pub struct ChangePoints;

impl Method for ChangePoints {
    const ID: &'static str = "change_points";
    const VERSION: u32 = 1;
    const SUMMARY: &'static str = "binary segmentation on a piecewise-constant mean, BIC penalty log(n)·σ², on a dense series";
    type Params = ChangePointParams;

    fn estimand(p: &Self::Params) -> Estimand {
        Estimand {
            unit: "panel unit",
            population: format!(
                "every unit of the range; {} must be eligible in each",
                p.metric
            ),
            question: format!("level shifts in {}", p.metric),
            dependence: Dependence::Independent,
            causal: false,
        }
    }

    fn run(p: &Self::Params, input: &MethodInput) -> Result<MethodResult, NotEstimable> {
        let points = input.get(&p.metric)?;
        let x = dense(input, points)?;
        let n = x.len();
        if n < 2 * p.min_segment.max(1) {
            return Err(NotEstimable::new(format!(
                "{n} units; two segments of at least {} needed",
                p.min_segment
            ))
            .with_counts(n as u64, Default::default()));
        }
        let diffs: Vec<f64> = x.windows(2).map(|w| w[1] - w[0]).collect();
        let sigma2 = scaled_mad(&diffs).map_or(0.0, |m| m * m / 2.0);
        let penalty = (n as f64).ln() * sigma2.max(f64::MIN_POSITIVE);
        let cuts = segment(&x, p.min_segment.max(1), p.max_points, penalty);
        let mut bounds = vec![0];
        bounds.extend(cuts.iter().copied());
        bounds.push(n);
        let segments: Vec<_> = bounds
            .windows(2)
            .map(|w| {
                json!({
                    "from": points[w[0]].unit, "to": points[w[1] - 1].unit,
                    "units": w[1] - w[0], "mean": mean(&x[w[0]..w[1]]),
                })
            })
            .collect();
        let found: Vec<_> = cuts
            .iter()
            .map(|&k| {
                let before = bounds[bounds.iter().position(|&b| b == k).unwrap_or(1) - 1];
                let after = bounds[bounds.iter().position(|&b| b == k).unwrap_or(0) + 1];
                json!({
                    "unit": points[k].unit,
                    "index": k,
                    "before_mean": mean(&x[before..k]),
                    "after_mean": mean(&x[k..after]),
                })
            })
            .collect();
        Ok(MethodResult {
            method: Self::ID,
            version: Self::VERSION,
            estimand: Self::estimand(p),
            output: json!({"metric": p.metric, "change_points": found, "segments": segments}),
            n: n as u64,
            n_excluded: Default::default(),
            diagnostics: json!({"sigma2": sigma2, "penalty": penalty}),
        })
    }
}

fn cost(x: &[f64]) -> f64 {
    let m = mean(x);
    x.iter().map(|v| (v - m) * (v - m)).sum()
}

/// The best split of `x[a..b]` and its cost reduction.
fn best_split(x: &[f64], a: usize, b: usize, min_seg: usize) -> Option<(usize, f64)> {
    let whole = cost(&x[a..b]);
    (a + min_seg..=b.saturating_sub(min_seg))
        .map(|k| (k, whole - cost(&x[a..k]) - cost(&x[k..b])))
        .max_by(|l, r| l.1.total_cmp(&r.1))
}

/// Greedy binary segmentation: repeatedly take the split with the largest
/// gain anywhere while it beats the penalty.
fn segment(x: &[f64], min_seg: usize, max_points: usize, penalty: f64) -> Vec<usize> {
    let mut cuts: Vec<usize> = Vec::new();
    while cuts.len() < max_points {
        let mut bounds = vec![0];
        bounds.extend(cuts.iter().copied());
        bounds.push(x.len());
        let best = bounds
            .windows(2)
            .filter_map(|w| best_split(x, w[0], w[1], min_seg))
            .max_by(|l, r| l.1.total_cmp(&r.1));
        match best {
            Some((k, gain)) if gain > penalty => {
                cuts.push(k);
                cuts.sort_unstable();
            }
            _ => break,
        }
    }
    cuts
}

#[cfg(test)]
mod tests {
    use super::super::observed;
    use super::*;

    fn run(x: &[f64]) -> serde_json::Value {
        let mut input = MethodInput::default();
        input.series.insert("m".into(), observed(x));
        let p = ChangePointParams {
            metric: "m".into(),
            min_segment: 7,
            max_points: 10,
        };
        ChangePoints::run(&p, &input).unwrap().output
    }

    #[test]
    fn a_level_shift_is_found_where_it_happened() {
        let noise = [0.3, -0.2, 0.1, -0.4, 0.2, 0.0, -0.1, 0.4, -0.3, 0.1];
        let x: Vec<f64> = (0..30)
            .map(|i| if i < 18 { 5.0 } else { 9.0 } + noise[i % 10])
            .collect();
        let out = run(&x);
        let cps = out["change_points"].as_array().unwrap();
        assert_eq!(cps.len(), 1);
        assert_eq!(cps[0]["index"], 18);
        assert_eq!(cps[0]["unit"], "u18");
    }

    #[test]
    fn noise_alone_gives_no_change_point() {
        let x: Vec<f64> = (0..40)
            .map(|i| 5.0 + [0.3, -0.2, 0.1, -0.4, 0.2][i % 5])
            .collect();
        assert!(run(&x)["change_points"].as_array().unwrap().is_empty());
    }
}
