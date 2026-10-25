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

#[devkit::test]
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

#[devkit::test]
fn missing_units_are_excluded_by_reason_never_zero_filled() {
    let mut x = observed(&(0..14).map(f64::from).collect::<Vec<_>>());
    let y = observed(&(0..14).map(|v| f64::from(v * v)).collect::<Vec<_>>());
    x[3].cell = Cell::Missing {
        reason: Missing::NoCoverage,
    };
    x[4].cell = Cell::Partial {
        value: 1.0,
        observed_fraction: 0.5,
    };
    let res = LaggedCorrelation::run(&params(0), &input(x, y)).unwrap();
    assert_eq!(res.n, 12);
    assert_eq!(res.n_excluded.0[&Exclusion::NoCoverage], 1);
    assert_eq!(res.n_excluded.0[&Exclusion::Partial], 1);
    assert_eq!(res.diagnostics["series_breaks"], 1);
}

#[devkit::test]
fn too_few_pairs_is_an_answer_not_an_error() {
    let i = input(observed(&[1.0, 2.0, 3.0]), observed(&[1.0, 3.0, 2.0]));
    let e = LaggedCorrelation::run(&params(0), &i).unwrap_err();
    assert_eq!(e.reason, "3 paired units, 10 needed");
    assert_eq!(e.n, 3);
}

/// The deliberately faulty variant (naive n for autocorrelated series)
/// must disagree: two trending series get a larger, honest p.
#[devkit::test]
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

#[devkit::test]
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
