use super::*;
fn pair(id: &str, p: f64, outcome: Option<bool>) -> Pair {
    Pair {
        forecast: id.into(),
        probability: p,
        outcome,
    }
}
#[devkit::test]
fn forecast_brier_and_calibration_use_only_resolved_pairs() {
    // (.8-1)^2 + (.3-0)^2 + (.6-1)^2 = .29; mean = 29/300.
    let r = score(vec![
        pair("a", 0.8, Some(true)),
        pair("b", 0.3, Some(false)),
        pair("c", 0.6, Some(true)),
        pair("d", 0.9, None),
    ])
    .unwrap();
    assert!((r.brier.unwrap() - 29.0 / 300.0).abs() < 1e-12);
    assert_eq!((r.issued, r.resolved, r.unresolved), (4, 3, 1));
    assert_eq!(r.calibration[8].outcome_frequency, Some(1.0));
    assert_eq!(r.calibration[9].mean_probability, None);
}
#[devkit::test]
fn forecast_no_resolutions_is_not_estimable() {
    for pairs in [vec![], vec![pair("a", 0.0, None)]] {
        let r = score(pairs).unwrap();
        assert_eq!(r.status, "not_estimable");
        assert_eq!(r.brier, None);
    }
}
#[devkit::test]
fn forecast_endpoints_invalid_probabilities_and_duplicate_inputs() {
    let r = score(vec![
        pair("a", 0.0, Some(false)),
        pair("b", 1.0, Some(true)),
    ])
    .unwrap();
    assert_eq!(r.brier, Some(0.0));
    assert_eq!(r.calibration[9].count, 1);
    for p in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
        assert!(score(vec![pair("a", p, None)]).is_err());
    }
    assert!(score(vec![pair("a", 0.5, None), pair("a", 0.5, None)]).is_err());
}
