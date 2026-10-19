//! Lagged Pearson and Spearman correlation between two metric series, with
//! the p-value taken against Bartlett's effective n for autocorrelated
//! series (carried from Lynchpin's `core/analytics.py`, with its stated
//! assumptions enforced).
use super::stats::{min_detectable_r, pearson, r_interval, r_p_value, ranks};
use super::{
    Dependence, Estimand, Excluded, Method, MethodInput, MethodResult, NotEstimable, Point,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorrelationParams {
    pub x: String,
    pub y: String,
    /// Pairs X at unit t with Y at unit t + lag.
    #[serde(default)]
    pub lag: i32,
    #[serde(default = "default_min_pairs")]
    pub min_pairs: u64,
}

fn default_min_pairs() -> u64 {
    10
}

pub struct LaggedCorrelation;
pub struct RankCorrelation;

impl Method for LaggedCorrelation {
    const ID: &'static str = "lagged_correlation";
    const VERSION: u32 = 1;
    const SUMMARY: &'static str = "Pearson r of X at t with Y at t+lag over paired eligible units; p against Bartlett effective n";
    type Params = CorrelationParams;

    fn estimand(p: &Self::Params) -> Estimand {
        estimand(p, "Pearson")
    }

    fn run(p: &Self::Params, input: &MethodInput) -> Result<MethodResult, NotEstimable> {
        correlate::<Self>(p, input, false)
    }
}

impl Method for RankCorrelation {
    const ID: &'static str = "rank_correlation";
    const VERSION: u32 = 1;
    const SUMMARY: &'static str = "Spearman rho of X at t with Y at t+lag, same rules as lagged_correlation; for skewed counts";
    type Params = CorrelationParams;

    fn estimand(p: &Self::Params) -> Estimand {
        estimand(p, "Spearman")
    }

    fn run(p: &Self::Params, input: &MethodInput) -> Result<MethodResult, NotEstimable> {
        correlate::<Self>(p, input, true)
    }
}

fn estimand(p: &CorrelationParams, kind: &str) -> Estimand {
    Estimand {
        unit: "panel unit",
        population: format!(
            "units where {} and {} (lag {}) are both eligible",
            p.x, p.y, p.lag
        ),
        question: format!("{kind} lag-{} association of {} with {}", p.lag, p.x, p.y),
        dependence: Dependence::Autocorrelated,
        causal: false,
    }
}

/// Lag-1 autocorrelation over consecutive eligible units only; a gap breaks
/// the chain rather than joining the units around it.
fn lag1(points: &[Point], input: &MethodInput) -> Option<f64> {
    let (a, b): (Vec<f64>, Vec<f64>) = points
        .windows(2)
        .filter_map(|w| {
            Some((
                w[0].cell.eligible(input.eligibility).ok()?,
                w[1].cell.eligible(input.eligibility).ok()?,
            ))
        })
        .unzip();
    if a.len() < 3 { None } else { pearson(&a, &b) }
}

fn correlate<M: Method<Params = CorrelationParams>>(
    p: &CorrelationParams,
    input: &MethodInput,
    rank: bool,
) -> Result<MethodResult, NotEstimable> {
    let xs = input.get(&p.x)?;
    let ys = input.get(&p.y)?;
    if xs.len() != ys.len() {
        return Err(NotEstimable::new("the two series cover different units"));
    }
    let mut x = Vec::new();
    let mut y = Vec::new();
    let mut excluded = Excluded::default();
    let mut breaks = 0u64;
    let mut prev_paired = false;
    for (t, xp) in xs.iter().enumerate() {
        let Some(u) = t
            .checked_add_signed(p.lag as isize)
            .filter(|&u| u < ys.len())
        else {
            continue;
        };
        let a = xp.cell.eligible(input.eligibility);
        let b = ys[u].cell.eligible(input.eligibility);
        match (a, b) {
            (Ok(a), Ok(b)) => {
                x.push(a);
                y.push(b);
                prev_paired = true;
            }
            (Err(e1), Err(e2)) => {
                excluded.add(e1.worse(e2));
                breaks += u64::from(prev_paired);
                prev_paired = false;
            }
            (Err(e), Ok(_)) | (Ok(_), Err(e)) => {
                excluded.add(e);
                breaks += u64::from(prev_paired);
                prev_paired = false;
            }
        }
    }
    let n = x.len() as u64;
    if n < p.min_pairs {
        return Err(
            NotEstimable::new(format!("{n} paired units, {} needed", p.min_pairs))
                .with_counts(n, excluded),
        );
    }
    let (xv, yv) = if rank { (ranks(&x), ranks(&y)) } else { (x, y) };
    let Some(r) = pearson(&xv, &yv) else {
        return Err(
            NotEstimable::new("a series is constant over the paired units")
                .with_counts(n, excluded),
        );
    };
    let r1x = lag1(xs, input);
    let r1y = lag1(ys, input);
    let nf = n as f64;
    let n_eff = match (r1x, r1y) {
        (Some(a), Some(b)) => {
            let adj = nf * (1.0 - a * b) / (1.0 + a * b);
            // Applied only when it lowers n; never below 5.
            if adj < nf { adj.max(5.0) } else { nf }
        }
        _ => nf,
    };
    let interval = r_interval(r, n_eff);
    Ok(MethodResult {
        method: M::ID,
        version: M::VERSION,
        estimand: M::estimand(p),
        output: json!({
            "x": p.x, "y": p.y, "lag": p.lag,
            "r": r,
            "p": r_p_value(r, n_eff),
            "p_naive": r_p_value(r, nf),
            "n_eff": n_eff,
            "ci95": interval.map(|(a, b)| [a, b]),
        }),
        n,
        n_excluded: excluded,
        diagnostics: json!({
            "lag1_autocorrelation": {"x": r1x, "y": r1y},
            "min_detectable_r": min_detectable_r(n_eff),
            "series_breaks": breaks,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::super::observed;
    use super::*;
    use crate::cell::{Cell, Exclusion, Missing};

    fn input(x: Vec<Point>, y: Vec<Point>) -> MethodInput {
        let mut i = MethodInput::default();
        i.series.insert("x".into(), x);
        i.series.insert("y".into(), y);
        i
    }

    fn params(lag: i32) -> CorrelationParams {
        CorrelationParams {
            x: "x".into(),
            y: "y".into(),
            lag,
            min_pairs: 10,
        }
    }

    #[test]
    fn r_matches_the_direct_formula_and_lag_shifts_pairs() {
        let x: Vec<f64> = (0..20).map(|i| f64::from(i % 7)).collect();
        // y at t+1 equals x at t.
        let mut y = vec![0.0];
        y.extend_from_slice(&x[..19]);
        let i = input(observed(&x), observed(&y));
        let r0 = LaggedCorrelation::run(&params(0), &i).unwrap();
        let direct = pearson(&x, &y).unwrap();
        assert!((r0.output["r"].as_f64().unwrap() - direct).abs() < 1e-12);
        let r1 = LaggedCorrelation::run(&params(1), &i).unwrap();
        assert!((r1.output["r"].as_f64().unwrap() - 1.0).abs() < 1e-12);
        assert_eq!(r1.n, 19);
    }

    #[test]
    fn missing_units_are_excluded_by_reason_never_zero_filled() {
        let mut x = observed(&(0..14).map(f64::from).collect::<Vec<_>>());
        let y = observed(&(0..14).map(|v| f64::from(v * v)).collect::<Vec<_>>());
        x[3].cell = Cell::Missing {
            reason: Missing::NoOpportunity,
        };
        x[4].cell = Cell::Partial {
            value: 1.0,
            observed_fraction: 0.5,
        };
        let res = LaggedCorrelation::run(&params(0), &input(x, y)).unwrap();
        assert_eq!(res.n, 12);
        assert_eq!(res.n_excluded.0[&Exclusion::NoOpportunity], 1);
        assert_eq!(res.n_excluded.0[&Exclusion::Partial], 1);
        assert_eq!(res.diagnostics["series_breaks"], 1);
    }

    #[test]
    fn too_few_pairs_is_an_answer_not_an_error() {
        let i = input(observed(&[1.0, 2.0, 3.0]), observed(&[1.0, 3.0, 2.0]));
        let e = LaggedCorrelation::run(&params(0), &i).unwrap_err();
        assert_eq!(e.reason, "3 paired units, 10 needed");
        assert_eq!(e.n, 3);
    }

    /// The deliberately faulty variant (naive n for autocorrelated series)
    /// must disagree: two trending series get a larger, honest p.
    #[test]
    fn autocorrelated_series_use_the_effective_n() {
        let x: Vec<f64> = (0..40).map(|i| f64::from(i) + f64::from(i % 3)).collect();
        let y: Vec<f64> = (0..40)
            .map(|i| f64::from(i) * 0.5 + f64::from((i * 7) % 5))
            .collect();
        let res = LaggedCorrelation::run(&params(0), &input(observed(&x), observed(&y))).unwrap();
        let n_eff = res.output["n_eff"].as_f64().unwrap();
        assert!((5.0..40.0).contains(&n_eff));
        assert!(res.output["p"].as_f64().unwrap() > res.output["p_naive"].as_f64().unwrap());
    }

    #[test]
    fn spearman_is_rank_based() {
        let x: Vec<f64> = (1..=12).map(f64::from).collect();
        let y: Vec<f64> = x.iter().map(|v| v.powi(3)).collect();
        let res = RankCorrelation::run(&params(0), &input(observed(&x), observed(&y))).unwrap();
        assert!((res.output["r"].as_f64().unwrap() - 1.0).abs() < 1e-12);
        let pearson_r = LaggedCorrelation::run(&params(0), &input(observed(&x), observed(&y)))
            .unwrap()
            .output["r"]
            .as_f64()
            .unwrap();
        assert!(pearson_r < 1.0);
    }
}
