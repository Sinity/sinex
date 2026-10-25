use super::super::observed;
use super::*;
use crate::cell::{Cell, Missing};

#[devkit::test]
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

#[devkit::test]
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
        reason: Missing::NoCoverage,
    };
    input.series.insert("m".into(), holed);
    let e = Trend::run(&p, &input).unwrap_err();
    assert!(
        e.reason
            .starts_with("needs a dense series; 1 unit(s) not eligible: u5")
    );
}
