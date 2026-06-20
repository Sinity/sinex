//! Test entrypoint for the orphan automata test files.
//!
//! These tests were authored against the old split automata crate and were
//! orphaned during the `sinexd::automata` fold. This module declaration restores
//! them; the individual files import from `sinexd::automata::*`.

mod automata {
    mod aggregation_test;
    mod analytics_test;
    mod canonicalization_test;
    mod config_defaults_test;
    mod daily_summarizer_test;
    mod entity_enricher_test;
    mod entity_resolver_test;
    mod hourly_summarizer_test;
    mod instruction_reconciler_test;
    mod relation_extractor_test;
    mod session_detector_test;
    mod summarizer_support;
}
