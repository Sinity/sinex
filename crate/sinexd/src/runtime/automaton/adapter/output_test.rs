use super::*;

#[test]
fn parent_limit_warning_limiter_suppresses_until_interval() {
    let mut limiter = ParentLimitWarnState::default();
    let key = ParentLimitWarnKey {
        automaton: "analytics-automaton",
        phase: "live processing",
        output_event_type: "activity.window.summary",
    };
    let start = Instant::now();

    assert_eq!(limiter.should_log(key.clone(), start), Some(0));
    assert_eq!(
        limiter.should_log(key.clone(), start + Duration::from_secs(1)),
        None
    );
    assert_eq!(
        limiter.should_log(key.clone(), start + Duration::from_secs(30)),
        None
    );
    assert_eq!(
        limiter.should_log(
            key,
            start + DERIVED_OUTPUT_PARENT_WARN_LOG_INTERVAL + Duration::from_secs(1)
        ),
        Some(2)
    );
}
