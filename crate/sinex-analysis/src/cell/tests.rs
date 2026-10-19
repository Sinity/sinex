use super::*;

#[test]
fn partial_cells_enter_only_above_the_requested_fraction() {
    let c = Cell::Partial {
        value: 6.0,
        observed_fraction: 0.83,
    };
    assert_eq!(c.eligible(Eligibility::default()), Err(Exclusion::Partial));
    let rule = Eligibility {
        min_observed_fraction: Some(0.8),
        admit_recorded: false,
    };
    assert_eq!(c.eligible(rule), Ok(6.0));
    let low = Cell::Partial {
        value: 6.0,
        observed_fraction: 0.29,
    };
    assert_eq!(low.eligible(rule), Err(Exclusion::Partial));
}

#[test]
fn missing_outranks_partial_when_a_pair_drops_a_unit() {
    assert_eq!(
        Exclusion::Partial.worse(Exclusion::NoOpportunity),
        Exclusion::NoOpportunity
    );
    assert_eq!(Exclusion::Gap.worse(Exclusion::Recorded), Exclusion::Gap);
}

#[test]
fn a_zero_is_observed_not_missing() {
    let c: Cell<f64> = Cell::Observed { value: 0.0 };
    assert_eq!(c.state_name(), "observed");
    assert_eq!(c.value(), Some(&0.0));
    let m: Cell<f64> = Cell::Missing {
        reason: Missing::NoOpportunity,
    };
    assert_eq!(m.value(), None);
    assert_eq!(
        serde_json::to_value(&m).unwrap(),
        serde_json::json!({"state": "missing", "reason": "no_opportunity"})
    );
}
