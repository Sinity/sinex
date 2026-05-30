//! Test entrypoint for the orphan automata test files.
//!
//! These tests were authored against `sinex_process::automata::*` and got
//! orphaned when sinex-process was folded into `sinexd::automata` (commit
//! 567266c29). They were not wired back into the test harness during the
//! fold. This module declaration restores them; the individual files have
//! been updated to import from `sinexd::automata::*` instead.

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
}
