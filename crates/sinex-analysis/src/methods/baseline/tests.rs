use super::super::observed;
use super::*;
use crate::cell::{Cell, Missing};

fn series(x: &[f64]) -> MethodInput {
    let mut input = MethodInput::default();
    input.series.insert("m".into(), observed(x));
    input
}

#[devkit::test]
fn prior_only_baseline_ignores_the_unit_and_later_ones() {
    let mut x: Vec<f64> = (0..30).map(|i| 10.0 + f64::from(i % 3)).collect();
    x[29] = 1000.0;
    let p = BaselineParams {
        metric: "m".into(),
        window_units: 28,
        at: Some("u29".into()),
        min_observed: 14,
    };
    let res = RobustBaseline::run(&p, &series(&x)).unwrap();
    assert_eq!(res.output["median"], 11.0);
    assert_eq!(res.n, 28);
}

#[devkit::test]
fn the_minimum_counts_observed_units_not_calendar_units() {
    let mut points = observed(&[5.0; 28]);
    for p in points.iter_mut().take(20) {
        p.cell = Cell::Missing {
            reason: Missing::NoCoverage,
        };
    }
    let mut input = MethodInput::default();
    input.series.insert("m".into(), points);
    let p = BaselineParams {
        metric: "m".into(),
        window_units: 28,
        at: None,
        min_observed: 14,
    };
    let e = RobustBaseline::run(&p, &input).unwrap_err();
    assert_eq!(e.reason, "8 observed units in the window, 14 needed");
}

#[devkit::test]
fn a_spike_is_flagged_against_its_prior_window() {
    let mut x: Vec<f64> = (0..40).map(|i| 10.0 + f64::from(i % 4)).collect();
    x[35] = 40.0;
    let p = AnomalyParams {
        metric: "m".into(),
        window_units: 28,
        min_observed: 14,
        threshold: 3.5,
    };
    let res = Anomaly::run(&p, &series(&x)).unwrap();
    let flagged: Vec<_> = res.output["units"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|u| u["flagged"] == true)
        .map(|u| u["unit"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(flagged, ["u35"]);
    // The first 14 units have too few prior units.
    assert_eq!(res.diagnostics["too_few_prior_units"], 14);
}
