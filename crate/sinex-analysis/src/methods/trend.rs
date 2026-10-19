//! Mann-Kendall trend test with Sen's slope per calendar unit. Needs a dense
//! series: the slope divides by calendar distance, so a hole would make row
//! distance and calendar distance disagree.
use super::stats::{median, z_two_sided_p};
use super::{Dependence, Estimand, Method, MethodInput, MethodResult, NotEstimable, dense};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrendParams {
    pub metric: String,
    #[serde(default = "default_min_units")]
    pub min_units: u64,
}

fn default_min_units() -> u64 {
    10
}

pub struct Trend;

impl Method for Trend {
    const ID: &'static str = "trend";
    const VERSION: u32 = 1;
    const SUMMARY: &'static str =
        "Mann-Kendall S with Sen's slope per calendar unit, on a dense series";
    type Params = TrendParams;

    fn estimand(p: &Self::Params) -> Estimand {
        Estimand {
            unit: "panel unit",
            population: format!(
                "every unit of the range; {} must be eligible in each",
                p.metric
            ),
            question: format!("monotonic trend in {}", p.metric),
            dependence: Dependence::Independent,
            causal: false,
        }
    }

    fn run(p: &Self::Params, input: &MethodInput) -> Result<MethodResult, NotEstimable> {
        let points = input.get(&p.metric)?;
        let x = dense(input, points)?;
        let n = x.len();
        if (n as u64) < p.min_units {
            return Err(
                NotEstimable::new(format!("{n} units, {} needed", p.min_units))
                    .with_counts(n as u64, Default::default()),
            );
        }
        let (s, var) = mann_kendall(&x);
        let z = if var <= 0.0 {
            0.0
        } else if s > 0.0 {
            (s - 1.0) / var.sqrt()
        } else if s < 0.0 {
            (s + 1.0) / var.sqrt()
        } else {
            0.0
        };
        let mut slopes = Vec::with_capacity(n * (n - 1) / 2);
        for i in 0..n {
            for j in i + 1..n {
                slopes.push((x[j] - x[i]) / (j - i) as f64);
            }
        }
        Ok(MethodResult {
            method: Self::ID,
            version: Self::VERSION,
            estimand: Self::estimand(p),
            output: json!({
                "metric": p.metric,
                "s": s,
                "z": z,
                "p": z_two_sided_p(z),
                "sen_slope_per_unit": median(&slopes),
                "first_unit": points.first().map(|q| q.unit.clone()),
                "last_unit": points.last().map(|q| q.unit.clone()),
            }),
            n: n as u64,
            n_excluded: Default::default(),
            diagnostics: json!({"variance_s": var}),
        })
    }
}

/// S and its variance with the correction for tied values.
pub fn mann_kendall(x: &[f64]) -> (f64, f64) {
    let n = x.len();
    let mut s = 0.0;
    for i in 0..n {
        for j in i + 1..n {
            s += (x[j] - x[i]).signum() * f64::from(u8::from(x[j] != x[i]));
        }
    }
    let mut ties: BTreeMap<u64, u64> = BTreeMap::new();
    for v in x {
        *ties.entry(v.to_bits()).or_default() += 1;
    }
    let nf = n as f64;
    let tie_term: f64 = ties
        .values()
        .filter(|&&t| t > 1)
        .map(|&t| {
            let t = t as f64;
            t * (t - 1.0) * (2.0 * t + 5.0)
        })
        .sum();
    (s, (nf * (nf - 1.0) * (2.0 * nf + 5.0) - tie_term) / 18.0)
}

#[cfg(test)]
mod tests {
    use super::super::observed;
    use super::*;
    use crate::cell::{Cell, Missing};

    #[test]
    fn worked_examples() {
        // Five increasing values: S = 10, Var = 5·4·15/18.
        let (s, v) = mann_kendall(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(s, 10.0);
        assert!((v - 50.0 / 3.0).abs() < 1e-12);
        // A tie pair: S = 2, Var = (3·2·11 − 2·1·9)/18.
        let (s, v) = mann_kendall(&[1.0, 1.0, 2.0]);
        assert_eq!(s, 2.0);
        assert!((v - 48.0 / 18.0).abs() < 1e-12);
    }

    #[test]
    fn sen_slope_is_per_calendar_unit_and_holes_refuse() {
        let x: Vec<f64> = (0..12)
            .map(|i| 2.0 * f64::from(i) + f64::from(i % 2))
            .collect();
        let mut input = MethodInput::default();
        input.series.insert("m".into(), observed(&x));
        let p = TrendParams {
            metric: "m".into(),
            min_units: 10,
        };
        let res = Trend::run(&p, &input).unwrap();
        assert!((res.output["sen_slope_per_unit"].as_f64().unwrap() - 2.0).abs() < 1e-9);
        assert!(res.output["p"].as_f64().unwrap() < 0.001);

        let mut holed = observed(&x);
        holed[5].cell = Cell::Missing {
            reason: Missing::NoOpportunity,
        };
        input.series.insert("m".into(), holed);
        let e = Trend::run(&p, &input).unwrap_err();
        assert!(
            e.reason
                .starts_with("needs a dense series; 1 unit(s) not eligible: u5")
        );
    }
}
