//! Production-path test harness root.
//!
//! Wave B subagents add per-source-unit `case!(...)` invocations inside the
//! fenced regions in `obligations/initial_ingestion.rs` and
//! `obligations/privacy.rs`.
//!
//! # Usage
//!
//! ```rust,ignore
//! use crate::production_path::case;
//!
//! // In the appropriate obligation file, inside the fenced region for your domain:
//! case! {
//!     source_unit_id: "terminal.atuin-history",
//!     adapter_kind: AppendOnlyFile,
//!     fixture_data: b"2024-01-15 14:23:45\techo hello\n",
//!     expected_event_types: &["shell.command"],
//! }
//! ```
//!
//! # Adapter kinds
//!
//! Use one of the `AdapterKind` variants to pick the right fixture:
//! - `AppendOnlyFile` — log-style file, writes lines to a tempfile
//! - `SqliteRow` — in-memory rusqlite DB with rows
//! - `StaticFile` — one-shot file read
//! - `FileDrop` — inotify-driven watched directory
//! - `Journal` — journalctl lines via `records_from_journal_lines`
//! - `Dbus` — D-Bus signals via `MockDbusBackend`
//! - `Clipboard` — clipboard snapshots via `MockClipboardBackend`
//! - `UnixSocket` — line-delimited Unix socket server in temp dir

#[path = "production_path/fixtures/mod.rs"]
pub mod fixtures;

#[path = "production_path/obligations/mod.rs"]
pub mod obligations;

// ---------------------------------------------------------------------------
// Adapter kind discriminator
// ---------------------------------------------------------------------------

/// Selects which fixture type to construct for a production-path case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterKind {
    AppendOnlyFile,
    SqliteRow,
    StaticFile,
    FileDrop,
    Journal,
    Dbus,
    Clipboard,
    UnixSocket,
}

// ---------------------------------------------------------------------------
// case! macro
// ---------------------------------------------------------------------------

/// Declare a production-path test case for a source unit.
///
/// # Parameters
///
/// - `source_unit_id` — the registered source unit id, e.g. `"terminal.atuin-history"`
/// - `adapter_kind` — which fixture type to build (one of the `AdapterKind` variants)
/// - `fixture_data` — raw bytes (for file adapters) or adapter-specific seed type
/// - `expected_event_types` — slice of event type strings to verify appear in `core.events`
/// - `obligations` (optional) — comma-separated list of obligation sets to run;
///   defaults to `[initial_ingestion, privacy]`
///
/// # Example
///
/// ```rust,ignore
/// // In a #[sinex_test] body, inside a fenced region in initial_ingestion.rs:
/// let failures = case! {
///     source_unit_id: "terminal.atuin-history",
///     adapter_kind: AppendOnlyFile,
///     fixture_data: b"2024-01-15 14:23:45\techo hello\n",
///     expected_event_types: &["shell.command"],
/// }.await;
/// assert!(failures.is_empty(), "{failures:?}");
/// ```
///
/// ```rust,ignore
/// let failures = case! {
///     source_unit_id: "browser.firefox-history",
///     adapter_kind: SqliteRow,
///     fixture_data: b"INSERT ...",
///     expected_event_types: &["webhistory.page.visited"],
///     obligations: [initial_ingestion, replay, privacy],
/// }.await;
/// assert!(failures.is_empty(), "{failures:?}");
/// ```
#[allow(unused_macros)]
macro_rules! case {
    // Full form: explicit obligation list
    (
        source_unit_id: $unit_id:expr,
        adapter_kind: $kind:ident,
        fixture_data: $data:expr,
        expected_event_types: $types:expr,
        obligations: [$($obligation:ident),+ $(,)?] $(,)?
    ) => {
        crate::production_path::_run_case(
            $unit_id,
            crate::production_path::AdapterKind::$kind,
            $data,
            $types,
            &[$(stringify!($obligation)),+],
        )
    };

    // Short form: default obligations (initial_ingestion + privacy)
    (
        source_unit_id: $unit_id:expr,
        adapter_kind: $kind:ident,
        fixture_data: $data:expr,
        expected_event_types: $types:expr $(,)?
    ) => {
        crate::production_path::_run_case(
            $unit_id,
            crate::production_path::AdapterKind::$kind,
            $data,
            $types,
            &["initial_ingestion", "privacy"],
        )
    };

    // Full-coverage form: exercises every obligation the harness knows about.
    // Use for Wave-B subagents to cover initial_ingestion, replay, drain,
    // isolation, and privacy with a single invocation.
    (
        source_unit_id: $unit_id:expr,
        adapter_kind: $kind:ident,
        fixture_data: $data:expr,
        expected_event_types: $types:expr,
        obligations: all $(,)?
    ) => {
        crate::production_path::_run_case(
            $unit_id,
            crate::production_path::AdapterKind::$kind,
            $data,
            $types,
            $crate::production_path::ALL_OBLIGATIONS,
        )
    };
}

/// Canonical list of every obligation supported by the harness.
///
/// Wave-B per-source-unit tests use this via `obligations: all` in the
/// `case!` macro to get full coverage in one line. Update this list (and
/// `_run_obligation`) whenever a new obligation family is added.
pub const ALL_OBLIGATIONS: &[&str] = &[
    "initial_ingestion",
    "replay",
    "drain",
    "isolation",
    "privacy",
];

/// Re-export the `case!` macro so submodules can use it via `use`.
#[allow(unused_imports)]
pub(crate) use case;

// ---------------------------------------------------------------------------
// Internal case runner
// ---------------------------------------------------------------------------

/// Internal: runs the named obligation set against the given fixture.
///
/// Called by the `case!` macro. Not intended for direct use.
///
/// Returns a list of failures as strings. An empty vec means all obligations
/// passed. The caller (typically a `#[sinex_test]`) should assert this is empty.
pub async fn _run_case(
    source_unit_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    expected_event_types: &[&str],
    obligation_names: &[&str],
) -> Vec<String> {
    let mut failures = Vec::new();
    for &obligation in obligation_names {
        let result = _run_obligation(
            obligation,
            source_unit_id,
            adapter_kind,
            fixture_data,
            expected_event_types,
        )
        .await;
        if let Err(e) = result {
            failures.push(format!("[{source_unit_id}] obligation '{obligation}': {e}"));
        }
    }
    failures
}

async fn _run_obligation(
    obligation: &str,
    source_unit_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    expected_event_types: &[&str],
) -> Result<(), String> {
    match obligation {
        "initial_ingestion" => {
            obligations::initial_ingestion::run(
                source_unit_id,
                adapter_kind,
                fixture_data,
                expected_event_types,
            )
            .await
        }
        "replay" => {
            obligations::replay::run(
                source_unit_id,
                adapter_kind,
                fixture_data,
                expected_event_types,
            )
            .await
        }
        "drain" => {
            obligations::drain::run(
                source_unit_id,
                adapter_kind,
                fixture_data,
            )
            .await
        }
        "isolation" => {
            obligations::isolation::run(
                source_unit_id,
                adapter_kind,
                fixture_data,
            )
            .await
        }
        "privacy" => {
            obligations::privacy::run(
                source_unit_id,
                adapter_kind,
                fixture_data,
                expected_event_types,
            )
            .await
        }
        unknown => Err(format!(
            "unknown obligation '{unknown}'; valid: initial_ingestion, replay, drain, isolation, privacy"
        )),
    }
}

// ---------------------------------------------------------------------------
// Per-domain test modules (Wave B)
// ---------------------------------------------------------------------------

#[path = "production_path/document.rs"]
mod document;

#[path = "production_path/fs.rs"]
mod fs;
