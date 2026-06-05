//! Workspace-level contracts for production derived-node output schemas.

use sinex_primitives::events::schema_registry::get_all_payloads;
use sinexd::automata::{
    analytics::AnalyticsAutomaton, canonicalizer::TerminalCommandCanonicalizer,
    health::HealthAggregator, session::SessionDetector,
};
use sinexd::runtime::{ScopeReconciler, Transducer, Windowed};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn production_automaton_outputs_have_registered_payload_schemas() -> TestResult<()> {
    let analytics = AnalyticsAutomaton::default();
    let health = HealthAggregator::default();
    let session = SessionDetector;
    let canonicalizer = TerminalCommandCanonicalizer::new();

    let expected_outputs = [
        (
            analytics.name(),
            analytics.output_event_source(),
            analytics.output_event_type(),
        ),
        (
            health.name(),
            health.output_event_source(),
            health.output_event_type(),
        ),
        (
            session.name(),
            session.output_event_source(),
            session.output_event_type(),
        ),
        (
            canonicalizer.name(),
            canonicalizer.output_event_source(),
            canonicalizer.output_event_type(),
        ),
    ];

    let registered_payloads = get_all_payloads().collect::<Vec<_>>();
    for (module_name, source, event_type) in expected_outputs {
        let has_schema = registered_payloads
            .iter()
            .any(|payload| payload.source == source && payload.event_type == event_type);
        assert!(
            has_schema,
            "automaton {module_name} emits {source}/{event_type} without a registered EventPayload schema",
        );
    }

    Ok(())
}
