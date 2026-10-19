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
mod tests;
