//! Aggregated test entrypoint for tests/sources/*.rs (sinex-v7od).
//!
//! Consolidates what used to be N separate `[[test]]` binaries (each
//! relinking the full sinexd lib) into one. cargo-nextest still runs every
//! test function in its own process regardless of binary layout, so this
//! loses no test isolation.
//!
//! `required_input_keys_test.rs` and `production_path.rs` carry their own
//! internal `#[path]` trees and needed NO adjustment for the extra nesting
//! level: `#[path = "P"]` always resolves `P` relative to the directory
//! that physically contains the file the attribute appears in, regardless
//! of how deep that file's own module path is nested (that's the whole
//! point of `#[path]` -- it escapes the nesting-derived directory
//! inference). Only a *bare* `mod x;` (no `#[path]`) is nesting-depth
//! sensitive, since it implies a `<containing-dir>/<enclosing-file-stem>/x.rs`
//! lookup -- see tests/event_engine.rs and tests/api.rs for cases that
//! actually needed a new `#[path]` override for exactly that reason.

mod sources {
    mod browser_history_parser_test;
    mod email_mailbox_parser_test;
    mod email_provider_cursor_parser_test;
    mod media_parser_test;
    mod parse_listener_integration_test;
    mod privacy_coverage_matrix_test;
    pub(crate) mod production_path;
    mod registry_dispatch_test;
    mod required_input_keys_test;
    mod source_catalog_drift_test;
    mod terminal_history_parser_test;
}

// The production_path harness and its obligation/fixture files were written
// when production_path.rs was its own [[test]] crate root, so they address
// shared harness items as `crate::AdapterKind`, `crate::ProductionPathCase`,
// `crate::obligations::...`, etc. Re-export those items here so that `crate::`
// still resolves under the aggregated root. (`crate::production_path_case_test`
// needs no entry: `#[macro_export]` macros always live at the crate root.)
pub(crate) use sources::production_path::{
    ALL_OBLIGATIONS, AdapterKind, ProductionPathCase, _run_case_with_directory_entry,
    _run_case_with_logical_path, fixtures, obligations, run_production_path_case,
};
