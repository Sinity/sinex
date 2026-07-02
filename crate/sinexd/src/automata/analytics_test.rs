use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn analytics_filters_to_trusted_activity_event_types() -> xtask::sandbox::TestResult<()> {
    let automaton = AnalyticsAutomaton::default();

    assert_eq!(automaton.input_event_type(), "*");
    assert_eq!(
        automaton.input_event_types(),
        vec![
            HyprlandWindowFocusedPayload::EVENT_TYPE.as_static_str(),
            ActivityWatchWindowActivePayload::EVENT_TYPE.as_static_str(),
            ActivityWatchBrowserTabActivePayload::EVENT_TYPE.as_static_str(),
            PageVisitedPayload::EVENT_TYPE.as_static_str(),
            KittyCommandExecutedPayload::EVENT_TYPE.as_static_str(),
        ]
    );
    assert_eq!(
        automaton.input_provenance_filter(),
        InputProvenanceFilter::MaterialOnly
    );
    Ok(())
}
