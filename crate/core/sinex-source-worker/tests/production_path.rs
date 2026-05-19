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

/// Variant of `_run_case` for parsers whose production contract depends on
/// `SourceRecord.logical_path`.
pub async fn _run_case_with_logical_path(
    source_unit_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    logical_path: &str,
    expected_event_types: &[&str],
    obligation_names: &[&str],
) -> Vec<String> {
    let mut failures = Vec::new();
    for &obligation in obligation_names {
        let result = _run_obligation_with_logical_path(
            obligation,
            source_unit_id,
            adapter_kind,
            fixture_data,
            logical_path,
            expected_event_types,
        )
        .await;
        if let Err(e) = result {
            failures.push(format!("[{source_unit_id}] obligation '{obligation}': {e}"));
        }
    }
    failures
}

/// Variant of `_run_case` for directory-walk parsers whose production contract
/// depends on a `DirectoryEntry` anchor.
pub async fn _run_case_with_directory_entry(
    source_unit_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    directory_entry_path: &str,
    content_hash: Option<&str>,
    expected_event_types: &[&str],
    obligation_names: &[&str],
) -> Vec<String> {
    let mut failures = Vec::new();
    for &obligation in obligation_names {
        let result = _run_obligation_with_directory_entry(
            obligation,
            source_unit_id,
            adapter_kind,
            fixture_data,
            directory_entry_path,
            content_hash,
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
        "drain" => obligations::drain::run(source_unit_id, adapter_kind, fixture_data).await,
        "isolation" => {
            obligations::isolation::run(source_unit_id, adapter_kind, fixture_data).await
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

async fn _run_obligation_with_logical_path(
    obligation: &str,
    source_unit_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    logical_path: &str,
    expected_event_types: &[&str],
) -> Result<(), String> {
    match obligation {
        "initial_ingestion" => {
            run_record_initial_ingestion(
                source_unit_id,
                fixture_data,
                logical_path,
                expected_event_types,
            )
            .await
        }
        "replay" => {
            run_record_replay(
                source_unit_id,
                fixture_data,
                logical_path,
                expected_event_types,
            )
            .await
        }
        "drain" => obligations::drain::run(source_unit_id, adapter_kind, fixture_data).await,
        "isolation" => {
            obligations::isolation::run(source_unit_id, adapter_kind, fixture_data).await
        }
        "privacy" => {
            run_record_privacy(
                source_unit_id,
                adapter_kind,
                fixture_data,
                logical_path,
                expected_event_types,
            )
            .await
        }
        unknown => Err(format!(
            "unknown obligation '{unknown}'; valid: initial_ingestion, replay, drain, isolation, privacy"
        )),
    }
}

async fn _run_obligation_with_directory_entry(
    obligation: &str,
    source_unit_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    directory_entry_path: &str,
    content_hash: Option<&str>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    match obligation {
        "initial_ingestion" => {
            run_directory_entry_initial_ingestion(
                source_unit_id,
                fixture_data,
                directory_entry_path,
                content_hash,
                expected_event_types,
            )
            .await
        }
        "replay" => {
            run_directory_entry_replay(
                source_unit_id,
                fixture_data,
                directory_entry_path,
                content_hash,
                expected_event_types,
            )
            .await
        }
        "drain" => obligations::drain::run(source_unit_id, adapter_kind, fixture_data).await,
        "isolation" => {
            obligations::isolation::run(source_unit_id, adapter_kind, fixture_data).await
        }
        "privacy" => {
            run_directory_entry_privacy(
                source_unit_id,
                adapter_kind,
                fixture_data,
                directory_entry_path,
                content_hash,
                expected_event_types,
            )
            .await
        }
        unknown => Err(format!(
            "unknown obligation '{unknown}'; valid: initial_ingestion, replay, drain, isolation, privacy"
        )),
    }
}

async fn run_record_initial_ingestion(
    source_unit_id: &str,
    fixture_data: &[u8],
    logical_path: &str,
    expected_event_types: &[&str],
) -> Result<(), String> {
    let material_id = sinex_primitives::Uuid::now_v7();
    let outcome = dispatch_record_fixture(source_unit_id, fixture_data, logical_path, material_id)
        .await
        .map_err(|e| format!("dispatch error for '{source_unit_id}': {e}"))?;

    if outcome.events.is_empty() {
        return Err(format!(
            "initial ingestion for '{source_unit_id}': parser returned no events for fixture data ({} bytes)",
            fixture_data.len()
        ));
    }

    let produced_types: Vec<String> = outcome
        .events
        .iter()
        .map(|e| e.event_type.as_str().to_string())
        .collect();

    for &expected in expected_event_types {
        if !produced_types.iter().any(|t| t == expected) {
            return Err(format!(
                "initial ingestion for '{source_unit_id}': expected event type '{expected}' \
                 not found in output. Produced: {produced_types:?}"
            ));
        }
    }

    Ok(())
}

async fn run_directory_entry_initial_ingestion(
    source_unit_id: &str,
    fixture_data: &[u8],
    directory_entry_path: &str,
    content_hash: Option<&str>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    use camino::Utf8PathBuf;
    use sinex_primitives::parser::MaterialAnchor;

    let material_id = sinex_primitives::Uuid::now_v7();
    let anchor = MaterialAnchor::DirectoryEntry {
        path: Utf8PathBuf::from(directory_entry_path),
        content_hash: content_hash.map(str::to_string),
    };
    let outcome = dispatch_record_fixture_with_anchor(
        source_unit_id,
        fixture_data,
        anchor,
        Some(directory_entry_path),
        material_id,
    )
    .await
    .map_err(|e| format!("dispatch error for '{source_unit_id}': {e}"))?;

    if outcome.events.is_empty() {
        return Err(format!(
            "initial ingestion for '{source_unit_id}': parser returned no events for directory entry fixture data ({} bytes)",
            fixture_data.len()
        ));
    }

    let produced_types: Vec<String> = outcome
        .events
        .iter()
        .map(|e| e.event_type.as_str().to_string())
        .collect();

    for &expected in expected_event_types {
        if !produced_types.iter().any(|t| t == expected) {
            return Err(format!(
                "initial ingestion for '{source_unit_id}': expected event type '{expected}' \
                 not found in output. Produced: {produced_types:?}"
            ));
        }
    }

    Ok(())
}

async fn run_record_replay(
    source_unit_id: &str,
    fixture_data: &[u8],
    logical_path: &str,
    expected_event_types: &[&str],
) -> Result<(), String> {
    let material_id_1 = sinex_primitives::Uuid::now_v7();
    let outcome_1 =
        dispatch_record_fixture(source_unit_id, fixture_data, logical_path, material_id_1)
            .await
            .map_err(|e| format!("replay first dispatch error for '{source_unit_id}': {e}"))?;

    let material_id_2 = sinex_primitives::Uuid::now_v7();
    let outcome_2 =
        dispatch_record_fixture(source_unit_id, fixture_data, logical_path, material_id_2)
            .await
            .map_err(|e| format!("replay second dispatch error for '{source_unit_id}': {e}"))?;

    if material_id_1 == material_id_2 {
        return Err("material IDs must differ between replay runs".into());
    }

    let types_1: Vec<&str> = outcome_1
        .events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    let types_2: Vec<&str> = outcome_2
        .events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    if types_1 != types_2 {
        return Err(format!(
            "replay for '{source_unit_id}': event types differ between runs. \
             run1={types_1:?} run2={types_2:?}"
        ));
    }

    for &expected in expected_event_types {
        if !types_1.contains(&expected) {
            return Err(format!(
                "replay for '{source_unit_id}': expected event type '{expected}' \
                 missing from replay output. Got: {types_1:?}"
            ));
        }
    }

    Ok(())
}

async fn run_directory_entry_replay(
    source_unit_id: &str,
    fixture_data: &[u8],
    directory_entry_path: &str,
    content_hash: Option<&str>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    use camino::Utf8PathBuf;
    use sinex_primitives::parser::MaterialAnchor;

    let material_id_1 = sinex_primitives::Uuid::now_v7();
    let anchor_1 = MaterialAnchor::DirectoryEntry {
        path: Utf8PathBuf::from(directory_entry_path),
        content_hash: content_hash.map(str::to_string),
    };
    let outcome_1 = dispatch_record_fixture_with_anchor(
        source_unit_id,
        fixture_data,
        anchor_1,
        Some(directory_entry_path),
        material_id_1,
    )
    .await
    .map_err(|e| format!("replay first dispatch error for '{source_unit_id}': {e}"))?;

    let material_id_2 = sinex_primitives::Uuid::now_v7();
    let anchor_2 = MaterialAnchor::DirectoryEntry {
        path: Utf8PathBuf::from(directory_entry_path),
        content_hash: content_hash.map(str::to_string),
    };
    let outcome_2 = dispatch_record_fixture_with_anchor(
        source_unit_id,
        fixture_data,
        anchor_2,
        Some(directory_entry_path),
        material_id_2,
    )
    .await
    .map_err(|e| format!("replay second dispatch error for '{source_unit_id}': {e}"))?;

    if material_id_1 == material_id_2 {
        return Err("material IDs must differ between replay runs".into());
    }

    let types_1: Vec<&str> = outcome_1
        .events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    let types_2: Vec<&str> = outcome_2
        .events
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    if types_1 != types_2 {
        return Err(format!(
            "replay for '{source_unit_id}': event types differ between runs. \
             run1={types_1:?} run2={types_2:?}"
        ));
    }

    for &expected in expected_event_types {
        if !types_1.contains(&expected) {
            return Err(format!(
                "replay for '{source_unit_id}': expected event type '{expected}' \
                 missing from replay output. Got: {types_1:?}"
            ));
        }
    }

    Ok(())
}

async fn run_record_privacy(
    source_unit_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    logical_path: &str,
    expected_event_types: &[&str],
) -> Result<(), String> {
    use sinex_primitives::privacy::{self, ProcessingContext};

    run_record_initial_ingestion(
        source_unit_id,
        fixture_data,
        logical_path,
        expected_event_types,
    )
    .await
    .map_err(|e| format!("privacy/clean-path: {e}"))?;

    let secret_text = "export TOKEN=ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let engine_result = privacy::engine()
        .map_err(|e| format!("privacy engine init failed: {e}"))?
        .process(secret_text, ProcessingContext::Command);

    if !engine_result.any_matched() {
        return Err(format!(
            "privacy for '{source_unit_id}': privacy engine did not match decoy token"
        ));
    }

    let redacted = engine_result.text.as_ref();
    if redacted.contains("ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") {
        return Err(format!(
            "privacy for '{source_unit_id}': redacted text still contains decoy token"
        ));
    }

    let material_id = sinex_primitives::Uuid::now_v7();
    if let Ok(outcome) = dispatch_record_fixture(
        source_unit_id,
        redacted.as_bytes(),
        logical_path,
        material_id,
    )
    .await
    {
        for event in &outcome.events {
            let payload_str = event.payload.to_string();
            if payload_str.contains("ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") {
                return Err(format!(
                    "privacy for '{source_unit_id}': event payload contains raw decoy token \
                     after redaction. Payload: {payload_str}"
                ));
            }
        }
    }

    let _ = adapter_kind;
    Ok(())
}

async fn run_directory_entry_privacy(
    source_unit_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    directory_entry_path: &str,
    content_hash: Option<&str>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    use camino::Utf8PathBuf;
    use sinex_primitives::parser::MaterialAnchor;
    use sinex_primitives::privacy::{self, ProcessingContext};

    run_directory_entry_initial_ingestion(
        source_unit_id,
        fixture_data,
        directory_entry_path,
        content_hash,
        expected_event_types,
    )
    .await
    .map_err(|e| format!("privacy/clean-path: {e}"))?;

    let secret_text = "export TOKEN=ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let engine_result = privacy::engine()
        .map_err(|e| format!("privacy engine init failed: {e}"))?
        .process(secret_text, ProcessingContext::Command);

    if !engine_result.any_matched() {
        return Err(format!(
            "privacy for '{source_unit_id}': privacy engine did not match decoy token"
        ));
    }

    let redacted = engine_result.text.as_ref();
    if redacted.contains("ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") {
        return Err(format!(
            "privacy for '{source_unit_id}': redacted text still contains decoy token"
        ));
    }

    let material_id = sinex_primitives::Uuid::now_v7();
    let anchor = MaterialAnchor::DirectoryEntry {
        path: Utf8PathBuf::from(directory_entry_path),
        content_hash: content_hash.map(str::to_string),
    };
    if let Ok(outcome) = dispatch_record_fixture_with_anchor(
        source_unit_id,
        redacted.as_bytes(),
        anchor,
        Some(directory_entry_path),
        material_id,
    )
    .await
    {
        for event in &outcome.events {
            let payload_str = event.payload.to_string();
            if payload_str.contains("ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") {
                return Err(format!(
                    "privacy for '{source_unit_id}': event payload contains raw decoy token \
                     after redaction. Payload: {payload_str}"
                ));
            }
        }
    }

    let _ = adapter_kind;
    Ok(())
}

async fn dispatch_record_fixture(
    source_unit_id: &str,
    fixture_data: &[u8],
    logical_path: &str,
    material_id: sinex_primitives::Uuid,
) -> Result<sinex_source_worker::dispatch::ParseOutcome, String> {
    use sinex_primitives::parser::MaterialAnchor;

    dispatch_record_fixture_with_anchor(
        source_unit_id,
        fixture_data,
        MaterialAnchor::ByteRange {
            start: 0,
            len: fixture_data.len() as u64,
        },
        Some(logical_path),
        material_id,
    )
    .await
}

async fn dispatch_record_fixture_with_anchor(
    source_unit_id: &str,
    fixture_data: &[u8],
    anchor: sinex_primitives::parser::MaterialAnchor,
    logical_path: Option<&str>,
    material_id: sinex_primitives::Uuid,
) -> Result<sinex_source_worker::dispatch::ParseOutcome, String> {
    use camino::Utf8PathBuf;
    use sinex_primitives::events::SourceMaterial;
    use sinex_primitives::ids::Id;
    use sinex_primitives::parser::{ParserContext, SourceRecord, SourceUnitId};
    use sinex_primitives::temporal::Timestamp;
    use sinex_source_worker::dispatch::find_parser_factory;

    let source_unit_id = SourceUnitId::new(source_unit_id)
        .map_err(|e| format!("invalid source unit id '{source_unit_id}': {e}"))?;
    let factory = find_parser_factory(&source_unit_id).ok_or_else(|| {
        format!(
            "source unit '{}' has no parser registered",
            source_unit_id.as_str()
        )
    })?;
    let mut parser = factory();
    let material_id = Id::<SourceMaterial>::from_uuid(material_id);

    let record = SourceRecord {
        material_id,
        anchor: anchor.clone(),
        bytes: fixture_data.to_vec(),
        logical_path: logical_path.map(Utf8PathBuf::from),
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };
    let ctx = ParserContext {
        source_unit_id,
        source_material_id: material_id,
        record_anchor: anchor,
        operation_id: sinex_primitives::Uuid::now_v7(),
        job_id: sinex_primitives::Uuid::now_v7(),
        host: std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("HOST"))
            .unwrap_or_else(|_| "unknown-host".to_string()),
        acquisition_time: Timestamp::now(),
    };
    let manifest = parser.manifest();
    let events = parser
        .parse_record_erased(record, &ctx)
        .await
        .map_err(|e| format!("parse error: {e}"))?;

    Ok(sinex_source_worker::dispatch::ParseOutcome {
        events,
        parser_id: manifest.parser_id.to_string(),
        parser_version: manifest.parser_version,
    })
}

// ---------------------------------------------------------------------------
// Coverage matrix
// ---------------------------------------------------------------------------

#[cfg(test)]
mod coverage_matrix {
    use std::collections::{BTreeMap, BTreeSet};

    use sinex_primitives::parser::SourceUnitId;
    use sinex_source_worker::dispatch::find_parser_factory;
    use sinex_source_worker::node_factory::registered_node_factory_ids;
    use sinex_source_worker::registry::SourceUnitRegistry;
    use xtask::sandbox::prelude::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum SmokeCoverage {
        BinaryPath,
        ObligationHarness,
        StructuralOnly,
        Blocked,
    }

    #[derive(Debug, Clone, Copy)]
    struct SmokeMatrixEntry {
        source_unit_id: &'static str,
        coverage: SmokeCoverage,
        evidence: &'static str,
        blocker_issue: Option<&'static str>,
    }

    const SMOKE_MATRIX: &[SmokeMatrixEntry] = &[
        entry(
            "ai-session-chatgpt",
            SmokeCoverage::ObligationHarness,
            "production_path/ai_session.rs",
        ),
        entry(
            "ai-session-claude",
            SmokeCoverage::ObligationHarness,
            "production_path/ai_session.rs",
        ),
        entry(
            "browser.history",
            SmokeCoverage::ObligationHarness,
            "production_path/browser.rs",
        ),
        entry(
            "desktop.activitywatch",
            SmokeCoverage::ObligationHarness,
            "production_path/desktop.rs",
        ),
        entry(
            "desktop.clipboard",
            SmokeCoverage::ObligationHarness,
            "production_path/desktop.rs",
        ),
        blocked(
            "desktop.window-manager",
            "production_path/desktop.rs ignored Hyprland fixture",
            "#1234",
        ),
        entry(
            "docs-library-index",
            SmokeCoverage::ObligationHarness,
            "production_path/document.rs",
        ),
        entry(
            "document.staging",
            SmokeCoverage::ObligationHarness,
            "production_path/document.rs",
        ),
        entry(
            "facebook-messenger-thread",
            SmokeCoverage::ObligationHarness,
            "production_path/export_parsers.rs",
        ),
        blocked(
            "fs",
            "production_path/fs.rs; native fs adapter fold pending",
            "#1224",
        ),
        entry(
            "git-commit-history",
            SmokeCoverage::ObligationHarness,
            "production_path/path_sensitive.rs",
        ),
        entry(
            "hledger-journal",
            SmokeCoverage::ObligationHarness,
            "production_path/export_parsers.rs",
        ),
        entry(
            "knowledgebase-vault",
            SmokeCoverage::ObligationHarness,
            "production_path/path_sensitive.rs",
        ),
        entry(
            "noop",
            SmokeCoverage::StructuralOnly,
            "registry_dispatch_test.rs",
        ),
        entry(
            "raindrop-bookmarks",
            SmokeCoverage::ObligationHarness,
            "production_path/export_parsers.rs",
        ),
        entry(
            "reddit-gdpr-comments",
            SmokeCoverage::ObligationHarness,
            "production_path/social_exports.rs",
        ),
        entry(
            "reddit-gdpr-posts",
            SmokeCoverage::ObligationHarness,
            "production_path/social_exports.rs",
        ),
        entry(
            "sleep-merged-summary",
            SmokeCoverage::ObligationHarness,
            "production_path/health_exports.rs",
        ),
        entry(
            "spotify-extended-history",
            SmokeCoverage::ObligationHarness,
            "production_path/export_parsers.rs",
        ),
        entry(
            "system.dbus",
            SmokeCoverage::ObligationHarness,
            "production_path/system.rs",
        ),
        entry(
            "system.journald",
            SmokeCoverage::ObligationHarness,
            "production_path/system.rs",
        ),
        entry(
            "system.monitor",
            SmokeCoverage::StructuralOnly,
            "production_path/system.rs",
        ),
        entry(
            "system.systemd",
            SmokeCoverage::ObligationHarness,
            "production_path/system.rs",
        ),
        entry(
            "system.udev",
            SmokeCoverage::ObligationHarness,
            "production_path/system.rs",
        ),
        entry(
            "terminal.atuin-history",
            SmokeCoverage::ObligationHarness,
            "production_path/terminal.rs",
        ),
        entry(
            "terminal.bash-history",
            SmokeCoverage::ObligationHarness,
            "production_path/terminal.rs",
        ),
        entry(
            "terminal.fish-history",
            SmokeCoverage::ObligationHarness,
            "production_path/terminal.rs",
        ),
        entry(
            "terminal.monitor",
            SmokeCoverage::StructuralOnly,
            "sources/terminal/monitor.rs tests",
        ),
        entry(
            "terminal.text-history",
            SmokeCoverage::ObligationHarness,
            "production_path/terminal.rs",
        ),
        entry(
            "terminal.zsh-history",
            SmokeCoverage::ObligationHarness,
            "production_path/terminal.rs",
        ),
        entry(
            "weechat",
            SmokeCoverage::BinaryPath,
            "production_path/obligations/initial_ingestion.rs binary_path",
        ),
        entry(
            "wykop-entries",
            SmokeCoverage::ObligationHarness,
            "production_path/social_exports.rs",
        ),
        entry(
            "wykop-entry-comments",
            SmokeCoverage::ObligationHarness,
            "production_path/social_exports.rs",
        ),
    ];

    const fn entry(
        source_unit_id: &'static str,
        coverage: SmokeCoverage,
        evidence: &'static str,
    ) -> SmokeMatrixEntry {
        SmokeMatrixEntry {
            source_unit_id,
            coverage,
            evidence,
            blocker_issue: None,
        }
    }

    const fn blocked(
        source_unit_id: &'static str,
        evidence: &'static str,
        blocker_issue: &'static str,
    ) -> SmokeMatrixEntry {
        SmokeMatrixEntry {
            source_unit_id,
            coverage: SmokeCoverage::Blocked,
            evidence,
            blocker_issue: Some(blocker_issue),
        }
    }

    #[sinex_test]
    async fn source_worker_smoke_matrix_covers_every_registered_factory(
        _ctx: TestContext,
    ) -> TestResult<()> {
        let factory_ids: BTreeSet<String> = registered_node_factory_ids()
            .into_iter()
            .map(|id| id.as_str().to_string())
            .collect();
        let matrix_ids: BTreeSet<String> = SMOKE_MATRIX
            .iter()
            .map(|entry| entry.source_unit_id.to_string())
            .collect();

        let missing: Vec<&String> = factory_ids.difference(&matrix_ids).collect();
        let stale: Vec<&String> = matrix_ids.difference(&factory_ids).collect();

        assert!(
            missing.is_empty(),
            "source-worker node factories missing smoke-matrix entries: {missing:#?}"
        );
        assert!(
            stale.is_empty(),
            "smoke-matrix entries without a registered node factory: {stale:#?}"
        );

        Ok(())
    }

    #[sinex_test]
    async fn source_worker_smoke_matrix_entries_are_actionable(
        _ctx: TestContext,
    ) -> TestResult<()> {
        let registry = SourceUnitRegistry::from_inventory();
        let mut seen = BTreeMap::new();

        for entry in SMOKE_MATRIX {
            assert!(
                !entry.evidence.trim().is_empty(),
                "{} must cite concrete smoke or fixture evidence",
                entry.source_unit_id
            );

            if let Some(previous) = seen.insert(entry.source_unit_id, entry.evidence) {
                panic!(
                    "duplicate smoke-matrix entry for {}: {previous} and {}",
                    entry.source_unit_id, entry.evidence
                );
            }

            let id = SourceUnitId::new(entry.source_unit_id)?;
            let descriptor = registry.find(&id).unwrap_or_else(|| {
                panic!("{} descriptor must be registered", entry.source_unit_id)
            });
            assert_eq!(descriptor.id, entry.source_unit_id);

            if matches!(
                entry.coverage,
                SmokeCoverage::BinaryPath | SmokeCoverage::ObligationHarness
            ) {
                assert!(
                    find_parser_factory(&id).is_some(),
                    "{} must have a parser factory for {:?} coverage",
                    entry.source_unit_id,
                    entry.coverage
                );
            }

            if matches!(entry.coverage, SmokeCoverage::Blocked) {
                let issue = entry.blocker_issue.unwrap_or("");
                assert!(
                    issue.starts_with('#'),
                    "{} blocked smoke entry must cite a concrete issue",
                    entry.source_unit_id
                );
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Per-domain test modules (Wave B)
// ---------------------------------------------------------------------------

#[path = "production_path/browser.rs"]
mod browser;

#[path = "production_path/ai_session.rs"]
mod ai_session;

#[path = "production_path/desktop.rs"]
mod desktop;

#[path = "production_path/document.rs"]
mod document;

#[path = "production_path/export_parsers.rs"]
mod export_parsers;

#[path = "production_path/fs.rs"]
mod fs;

#[path = "production_path/health_exports.rs"]
mod health_exports;

#[path = "production_path/path_sensitive.rs"]
mod path_sensitive;

#[path = "production_path/social_exports.rs"]
mod social_exports;

#[path = "production_path/system.rs"]
mod system;

#[path = "production_path/terminal.rs"]
mod terminal;
