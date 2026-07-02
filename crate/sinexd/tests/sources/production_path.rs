//! Production-path test harness root.
//!
//! Per-source test modules call `_run_case(...)` with fixture data,
//! expected event types, and the obligation set they want to exercise.
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

/// Canonical parser-fixture obligations exercised by each source case.
///
/// Per-source tests pass this to `_run_case(...)` when they need full
/// parser-contract coverage. Shared obligations that do not inspect parser
/// output, such as drain-controller state transitions, belong in one shared
/// test instead of being repeated for every source fixture.
pub const ALL_OBLIGATIONS: &[ProductionPathObligation] = &[
    ProductionPathObligation::InitialIngestion,
    ProductionPathObligation::Replay,
    ProductionPathObligation::Isolation,
    ProductionPathObligation::Privacy,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductionPathObligation {
    InitialIngestion,
    Replay,
    Drain,
    Isolation,
    Privacy,
}

impl ProductionPathObligation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InitialIngestion => "initial_ingestion",
            Self::Replay => "replay",
            Self::Drain => "drain",
            Self::Isolation => "isolation",
            Self::Privacy => "privacy",
        }
    }

    #[must_use]
    pub const fn can_reuse_initial_ingestion(self) -> bool {
        matches!(self, Self::Privacy)
    }

    #[must_use]
    pub const fn marks_initial_ingestion_verified(self) -> bool {
        matches!(self, Self::InitialIngestion)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProductionPathCase {
    pub label: &'static str,
    pub source_id: &'static str,
    pub adapter_kind: AdapterKind,
    pub fixture_data: &'static [u8],
    pub expected_event_types: &'static [&'static str],
    pub obligations: &'static [ProductionPathObligation],
}

impl ProductionPathCase {
    #[must_use]
    pub const fn new(
        label: &'static str,
        source_id: &'static str,
        adapter_kind: AdapterKind,
        fixture_data: &'static [u8],
        expected_event_types: &'static [&'static str],
    ) -> Self {
        Self {
            label,
            source_id,
            adapter_kind,
            fixture_data,
            expected_event_types,
            obligations: ALL_OBLIGATIONS,
        }
    }

    #[must_use]
    pub const fn with_obligations(
        mut self,
        obligations: &'static [ProductionPathObligation],
    ) -> Self {
        self.obligations = obligations;
        self
    }
}

pub async fn run_production_path_case(case: ProductionPathCase) -> Result<(), String> {
    let failures = _run_case(
        case.source_id,
        case.adapter_kind,
        case.fixture_data,
        case.expected_event_types,
        case.obligations,
    )
    .await;

    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} production-path obligations failed:\n{failures:#?}",
            case.label
        ))
    }
}

#[macro_export]
macro_rules! production_path_case_test {
    ($test_name:ident, $case:expr) => {
        #[xtask::sandbox::sinex_test]
        async fn $test_name() -> xtask::sandbox::prelude::TestResult<()> {
            $crate::run_production_path_case($case)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
            Ok(())
        }
    };
}

fn missing_obligation_failure(source_id: &str) -> String {
    format!(
        "[{source_id}] production-path case has no obligations; use ALL_OBLIGATIONS or an explicit non-empty subset"
    )
}

// ---------------------------------------------------------------------------
// Internal case runner
// ---------------------------------------------------------------------------

/// Internal: runs the named obligation set against the given fixture.
///
/// Called by per-source production-path tests.
///
/// Returns a list of failures as strings. An empty vec means all obligations
/// passed. The caller (typically a `#[sinex_test]`) should assert this is empty.
pub async fn _run_case(
    source_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    expected_event_types: &[&str],
    obligations: &[ProductionPathObligation],
) -> Vec<String> {
    if obligations.is_empty() {
        return vec![missing_obligation_failure(source_id)];
    }

    let mut failures = Vec::new();
    let mut initial_ingestion_verified = false;
    for &obligation in obligations {
        let result = if obligation.can_reuse_initial_ingestion() && initial_ingestion_verified {
            obligations::privacy::run_metadata_only(source_id).await
        } else {
            _run_obligation(
                obligation,
                source_id,
                adapter_kind,
                fixture_data,
                expected_event_types,
            )
            .await
        };
        if let Err(e) = result {
            failures.push(format!(
                "[{source_id}] obligation '{}': {e}",
                obligation.as_str()
            ));
        } else if obligation.marks_initial_ingestion_verified() {
            initial_ingestion_verified = true;
        }
    }
    failures
}

/// Variant of `_run_case` for parsers whose production contract depends on
/// `SourceRecord.logical_path`.
pub async fn _run_case_with_logical_path(
    source_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    logical_path: &str,
    expected_event_types: &[&str],
    obligations: &[ProductionPathObligation],
) -> Vec<String> {
    _run_case_with_record_fixture(
        source_id,
        adapter_kind,
        RecordFixtureSpec::byte_range(fixture_data, Some(logical_path)),
        expected_event_types,
        obligations,
    )
    .await
}

/// Variant of `_run_case` for directory-walk parsers whose production contract
/// depends on a `DirectoryEntry` anchor.
pub async fn _run_case_with_directory_entry(
    source_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    directory_entry_path: &str,
    content_hash: Option<&str>,
    expected_event_types: &[&str],
    obligations: &[ProductionPathObligation],
) -> Vec<String> {
    use camino::Utf8PathBuf;
    use sinex_primitives::parser::MaterialAnchor;

    let anchor = MaterialAnchor::DirectoryEntry {
        path: Utf8PathBuf::from(directory_entry_path),
        content_hash: content_hash.map(str::to_string),
    };

    _run_case_with_record_fixture(
        source_id,
        adapter_kind,
        RecordFixtureSpec {
            fixture_data,
            anchor,
            logical_path: Some(directory_entry_path),
            input_label: "directory entry fixture data",
        },
        expected_event_types,
        obligations,
    )
    .await
}

#[derive(Clone)]
struct RecordFixtureSpec<'a> {
    fixture_data: &'a [u8],
    anchor: sinex_primitives::parser::MaterialAnchor,
    logical_path: Option<&'a str>,
    input_label: &'static str,
}

impl<'a> RecordFixtureSpec<'a> {
    fn byte_range(fixture_data: &'a [u8], logical_path: Option<&'a str>) -> Self {
        Self {
            fixture_data,
            anchor: sinex_primitives::parser::MaterialAnchor::ByteRange {
                start: 0,
                len: fixture_data.len() as u64,
            },
            logical_path,
            input_label: "fixture data",
        }
    }
}

async fn _run_case_with_record_fixture(
    source_id: &str,
    adapter_kind: AdapterKind,
    fixture: RecordFixtureSpec<'_>,
    expected_event_types: &[&str],
    obligations: &[ProductionPathObligation],
) -> Vec<String> {
    if obligations.is_empty() {
        return vec![missing_obligation_failure(source_id)];
    }

    let mut failures = Vec::new();
    let mut initial_ingestion_verified = false;
    for &obligation in obligations {
        let result = if obligation.can_reuse_initial_ingestion() && initial_ingestion_verified {
            obligations::privacy::run_metadata_only(source_id).await
        } else {
            _run_record_fixture_obligation(
                obligation,
                source_id,
                adapter_kind,
                fixture.clone(),
                expected_event_types,
            )
            .await
        };
        if let Err(e) = result {
            failures.push(format!(
                "[{source_id}] obligation '{}': {e}",
                obligation.as_str()
            ));
        } else if obligation.marks_initial_ingestion_verified() {
            initial_ingestion_verified = true;
        }
    }
    failures
}

#[cfg(test)]
#[path = "production_path_harness_contract_tests.rs"]
mod harness_contract_tests;

async fn _run_obligation(
    obligation: ProductionPathObligation,
    source_id: &str,
    adapter_kind: AdapterKind,
    fixture_data: &[u8],
    expected_event_types: &[&str],
) -> Result<(), String> {
    match obligation {
        ProductionPathObligation::InitialIngestion => {
            obligations::initial_ingestion::run(
                source_id,
                adapter_kind,
                fixture_data,
                expected_event_types,
            )
            .await
        }
        ProductionPathObligation::Replay => {
            obligations::replay::run(source_id, adapter_kind, fixture_data, expected_event_types)
                .await
        }
        ProductionPathObligation::Drain => {
            obligations::drain::run(source_id, adapter_kind, fixture_data).await
        }
        ProductionPathObligation::Isolation => {
            obligations::isolation::run(source_id, adapter_kind, fixture_data).await
        }
        ProductionPathObligation::Privacy => {
            obligations::privacy::run(source_id, adapter_kind, fixture_data, expected_event_types)
                .await
        }
    }
}

async fn _run_record_fixture_obligation(
    obligation: ProductionPathObligation,
    source_id: &str,
    adapter_kind: AdapterKind,
    fixture: RecordFixtureSpec<'_>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    match obligation {
        ProductionPathObligation::InitialIngestion => {
            run_record_fixture_initial_ingestion(source_id, fixture, expected_event_types).await
        }
        ProductionPathObligation::Replay => {
            run_record_fixture_replay(source_id, fixture, expected_event_types).await
        }
        ProductionPathObligation::Drain => {
            obligations::drain::run(source_id, adapter_kind, fixture.fixture_data).await
        }
        ProductionPathObligation::Isolation => {
            obligations::isolation::run(source_id, adapter_kind, fixture.fixture_data).await
        }
        ProductionPathObligation::Privacy => {
            run_record_fixture_privacy(source_id, fixture, expected_event_types).await
        }
    }
}

async fn run_record_fixture_initial_ingestion(
    source_id: &str,
    fixture: RecordFixtureSpec<'_>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    let material_id = sinex_primitives::Uuid::now_v7();
    let outcome = dispatch_record_fixture_with_anchor(
        source_id,
        fixture.fixture_data,
        fixture.anchor,
        fixture.logical_path,
        material_id,
    )
    .await
    .map_err(|e| format!("dispatch error for '{source_id}': {e}"))?;

    if outcome.events.is_empty() {
        return Err(format!(
            "initial ingestion for '{source_id}': parser returned no events for {} ({} bytes)",
            fixture.input_label,
            fixture.fixture_data.len()
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
                "initial ingestion for '{source_id}': expected event type '{expected}' \
                 not found in output. Produced: {produced_types:?}"
            ));
        }
    }

    Ok(())
}

async fn run_record_fixture_replay(
    source_id: &str,
    fixture: RecordFixtureSpec<'_>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    let material_id_1 = sinex_primitives::Uuid::now_v7();
    let outcome_1 = dispatch_record_fixture_with_anchor(
        source_id,
        fixture.fixture_data,
        fixture.anchor.clone(),
        fixture.logical_path,
        material_id_1,
    )
    .await
    .map_err(|e| format!("replay first dispatch error for '{source_id}': {e}"))?;

    let material_id_2 = sinex_primitives::Uuid::now_v7();
    let outcome_2 = dispatch_record_fixture_with_anchor(
        source_id,
        fixture.fixture_data,
        fixture.anchor,
        fixture.logical_path,
        material_id_2,
    )
    .await
    .map_err(|e| format!("replay second dispatch error for '{source_id}': {e}"))?;

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
            "replay for '{source_id}': event types differ between runs. \
             run1={types_1:?} run2={types_2:?}"
        ));
    }

    for &expected in expected_event_types {
        if !types_1.contains(&expected) {
            return Err(format!(
                "replay for '{source_id}': expected event type '{expected}' \
                 missing from replay output. Got: {types_1:?}"
            ));
        }
    }

    Ok(())
}

async fn run_record_fixture_privacy(
    source_id: &str,
    fixture: RecordFixtureSpec<'_>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    run_record_fixture_initial_ingestion(source_id, fixture.clone(), expected_event_types)
        .await
        .map_err(|e| format!("privacy/clean-path: {e}"))?;

    obligations::privacy::run_metadata_only(source_id).await
}

async fn dispatch_record_fixture_with_anchor(
    source_id: &str,
    fixture_data: &[u8],
    anchor: sinex_primitives::parser::MaterialAnchor,
    logical_path: Option<&str>,
    material_id: sinex_primitives::Uuid,
) -> Result<sinexd::sources::dispatch::ParseOutcome, String> {
    use camino::Utf8PathBuf;
    use sinex_primitives::events::SourceMaterial;
    use sinex_primitives::ids::Id;
    use sinex_primitives::parser::{ParserContext, SourceId, SourceRecord};
    use sinex_primitives::temporal::Timestamp;
    use sinexd::sources::dispatch::find_parser_factory;

    let source_id =
        SourceId::new(source_id).map_err(|e| format!("invalid source id '{source_id}': {e}"))?;
    let factory = find_parser_factory(&source_id)
        .ok_or_else(|| format!("source '{}' has no parser registered", source_id.as_str()))?;
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
        source_id,
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

    Ok(sinexd::sources::dispatch::ParseOutcome {
        events,
        parser_id: manifest.parser_id.to_string(),
        parser_version: manifest.parser_version,
    })
}

// ---------------------------------------------------------------------------
// Coverage matrix
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "production_path_coverage_matrix.rs"]
mod coverage_matrix;

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

#[path = "production_path/email.rs"]
mod email;

#[path = "production_path/export_parsers.rs"]
mod export_parsers;

#[path = "production_path/fs.rs"]
mod fs;

#[path = "production_path/health_exports.rs"]
mod health_exports;

#[path = "production_path/media.rs"]
mod media;

#[path = "production_path/path_sensitive.rs"]
mod path_sensitive;

#[path = "production_path/social_exports.rs"]
mod social_exports;

#[path = "production_path/system.rs"]
mod system;

#[path = "production_path/terminal.rs"]
mod terminal;
