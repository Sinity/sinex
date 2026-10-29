use super::*;
use sinex_model::Timestamp;

#[devkit::test]
fn access_failure_and_unknown_temporal_bounds_never_supply_observed_coverage() {
    let mut row = SourceCoverage {
        observed_run: None,
        observed_class: None,
        observation: "synthetic".into(),
        observed_source: "synthetic".into(),
        binding: "synthetic".into(),
        host: "synthetic".into(),
        started_at: Timestamp::from_unix_secs(100).unwrap(),
        ended_at: Timestamp::from_unix_secs(200).unwrap(),
        state: CoverageState::RecordedUnknownCoverage,
        reason: None,
        completeness: CoverageCompleteness::Complete,
        bounded_by: "successful access".into(),
        events: 0,
        failures: 0,
    };
    assert_eq!(
        from_observation(&row).unwrap().completeness,
        Completeness::Unknown
    );
    row.state = CoverageState::Missing;
    assert!(from_observation(&row).is_none());
    row.state = CoverageState::Observed;
    assert_eq!(
        from_observation(&row).unwrap().span,
        Span::new(100_000_000, 200_000_000)
    );
    assert_eq!(
        from_observation(&row).unwrap().completeness,
        Completeness::Complete
    );
}

#[devkit::test]
fn rounded_display_preserves_partial_coverage_at_both_ends() {
    assert_eq!(percent_label(0.000001),">0%");
    assert_eq!(percent_label(0.999999),"<100%");
    assert_eq!(percent_label(0.0),"0%");
    assert_eq!(percent_label(1.0),"100%");
}
