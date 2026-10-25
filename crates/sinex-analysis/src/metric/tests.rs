use super::*;
use serde_json::json;

const EXIT_NONZERO: Predicate = Predicate::Ne {
    field: "exit_code",
    value: Lit::Num(0.0),
};

fn def(law: AggLaw) -> MetricDef {
    MetricDef {
        id: "t",
        version: 1,
        description: "",
        relation: "r",
        filter: None,
        unit: UnitGrain::Day,
        quantity: Quantity::Count,
        law,
        interval: None,
        allocation: Allocation::Instant,
        coverage: CoverageRule::AnyOf(&["s"]),
        dimensions: &[],
        role: SourceRole::Measured,
        deviation: false,
    }
}

fn rows(payloads: &[Value]) -> Vec<(Row<'_>, Span)> {
    payloads
        .iter()
        .map(|p| {
            (
                Row {
                    source: "s",
                    at: 0,
                    payload: p,
                },
                Span::new(0, 0),
            )
        })
        .collect()
}

#[devkit::test]
fn a_ratio_keeps_its_numerator_and_denominator() {
    let law = AggLaw::Ratio {
        num: Term::Count {
            filter: Some(&EXIT_NONZERO),
        },
        den: Term::Count { filter: None },
    };
    let d = def(law);
    let day1 = [json!({"exit_code": 1}), json!({"exit_code": 0})];
    let day2 = [
        json!({"exit_code": 0}),
        json!({"exit_code": 0}),
        json!({"exit_code": 0}),
        json!({"exit_code": 0}),
        json!({"exit_code": null}),
        json!({"exit_code": 2}),
    ];
    let a = aggregate(&d, &rows(&day1)).unwrap();
    let b = aggregate(&d, &rows(&day2)).unwrap();
    assert_eq!(a.value, 0.5);
    assert_eq!((a.numerator, a.denominator), (Some(1.0), Some(2.0)));
    // An unknown exit code counts in the denominator, not as a failure.
    assert!((b.value - 1.0 / 6.0).abs() < 1e-12);
    assert_eq!((b.numerator, b.denominator), (Some(1.0), Some(6.0)));
}

#[devkit::test]
fn an_unknown_exit_code_is_not_a_failure() {
    assert!(!EXIT_NONZERO.matches(&json!({"exit_code": null})));
    assert!(!EXIT_NONZERO.matches(&json!({})));
    assert!(EXIT_NONZERO.matches(&json!({"exit_code": 127})));
}

#[devkit::test]
fn a_zero_denominator_is_undefined_not_zero() {
    let law = AggLaw::Ratio {
        num: Term::Count { filter: None },
        den: Term::Sum { field: "n" },
    };
    assert_eq!(aggregate(&def(law), &rows(&[json!({"n": 0})])), None);
    assert_eq!(aggregate(&def(AggLaw::Mean { field: "x" }), &[]), None);
    // A count of nothing is an observed zero.
    assert_eq!(aggregate(&def(AggLaw::Count), &[]).unwrap().value, 0.0);
}

#[devkit::test]
fn overlapping_intervals_are_unioned() {
    let mut d = def(AggLaw::UnionDuration);
    d.interval = Some(IntervalShape::EndField { field: "ended_at" });
    d.allocation = Allocation::ClipInterval;
    let p1 = json!({"ended_at": "1970-01-01T00:00:10Z"});
    let p2 = json!({"ended_at": "1970-01-01T00:00:15Z"});
    let r1 = Row {
        source: "s",
        at: 0,
        payload: &p1,
    };
    let r2 = Row {
        source: "s",
        at: 5_000_000,
        payload: &p2,
    };
    let unit = Span::new(0, 12_000_000);
    let allocated: Vec<_> = [r1, r2]
        .into_iter()
        .filter_map(|r| d.allocate(&r, unit).map(|s| (r, s)))
        .collect();
    // [0,10) ∪ [5,12) clipped to the unit = 12 s, not 10 + 7.
    assert_eq!(aggregate(&d, &allocated).unwrap().value, 12.0);
}

#[devkit::test]
fn quantiles_interpolate_between_rows() {
    let d = def(AggLaw::Quantile { field: "x", q: 0.5 });
    let a = aggregate(&d, &rows(&[json!({"x": 1}), json!({"x": 3})])).unwrap();
    assert_eq!(a.value, 2.0);
    assert_eq!(quantile_sorted(&[1.0, 2.0, 3.0, 4.0], 0.25), 1.75);
}

#[devkit::test]
fn assign_to_end_credits_the_unit_the_interval_ends_in() {
    let mut d = def(AggLaw::Count);
    d.interval = Some(IntervalShape::Length {
        field: "ms",
        per_second: 1000,
    });
    d.allocation = Allocation::AssignToEnd;
    let p = json!({"ms": 10_000});
    let r = Row {
        source: "s",
        at: 5_000_000,
        payload: &p,
    };
    assert!(d.allocate(&r, Span::new(0, 10_000_000)).is_none());
    assert!(d.allocate(&r, Span::new(10_000_000, 20_000_000)).is_some());
}
