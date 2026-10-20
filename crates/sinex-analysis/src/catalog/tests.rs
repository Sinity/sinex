use super::*;

#[test]
fn metric_ids_are_unique_and_rules_name_known_sources() {
    let mut ids: Vec<_> = METRICS.iter().map(|m| m.id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), METRICS.len());
    for s in coverage_sources() {
        assert_ne!(
            completeness(s),
            Completeness::Unknown,
            "{s} has no declared completeness"
        );
    }
}

#[test]
fn metrics_read_only_the_fields_they_need() {
    assert_eq!(JOURNAL_ERRORS.fields(), ["priority", "unit"]);
    assert_eq!(SHELL_FAILED_RATIO.fields(), ["exit_code"]);
    assert_eq!(FOCUS_SECONDS.fields(), ["app", "ended_at"]);
    assert!(SHELL_COMMANDS.fields() == ["host"]);
}

#[test]
fn the_family_is_fixed_by_the_catalog() {
    let fam = correlation_family();
    // Four tracked metrics: 6 pairs × 2 lags.
    assert_eq!(fam.len(), 12);
    assert!(fam.contains(&("shell.commands", "activity.active_seconds", 1)));
}
