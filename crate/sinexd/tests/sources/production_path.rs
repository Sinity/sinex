//! Production-path test harness root.
//!
//! Per-source-unit proof modules call `_run_case(...)` with fixture data,
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

/// Canonical parser-fixture obligations exercised by each source-unit case.
///
/// Per-source-unit tests pass this to `_run_case(...)` when they need full
/// parser-contract coverage. Shared obligations that do not inspect parser
/// output, such as drain-controller state transitions, belong in one shared
/// test instead of being repeated for every source-unit fixture.
pub const ALL_OBLIGATIONS: &[&str] = &["initial_ingestion", "replay", "isolation", "privacy"];

// ---------------------------------------------------------------------------
// Internal case runner
// ---------------------------------------------------------------------------

/// Internal: runs the named obligation set against the given fixture.
///
/// Called by per-source-unit production-path tests.
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
    let mut initial_ingestion_verified = false;
    for &obligation in obligation_names {
        let result = if obligation == "privacy" && initial_ingestion_verified {
            obligations::privacy::run_metadata_only(source_unit_id).await
        } else {
            _run_obligation(
                obligation,
                source_unit_id,
                adapter_kind,
                fixture_data,
                expected_event_types,
            )
            .await
        };
        if let Err(e) = result {
            failures.push(format!("[{source_unit_id}] obligation '{obligation}': {e}"));
        } else if obligation == "initial_ingestion" {
            initial_ingestion_verified = true;
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
    _run_case_with_record_fixture(
        source_unit_id,
        adapter_kind,
        RecordFixtureSpec::byte_range(fixture_data, Some(logical_path)),
        expected_event_types,
        obligation_names,
    )
    .await
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
    use camino::Utf8PathBuf;
    use sinex_primitives::parser::MaterialAnchor;

    let anchor = MaterialAnchor::DirectoryEntry {
        path: Utf8PathBuf::from(directory_entry_path),
        content_hash: content_hash.map(str::to_string),
    };

    _run_case_with_record_fixture(
        source_unit_id,
        adapter_kind,
        RecordFixtureSpec {
            fixture_data,
            anchor,
            logical_path: Some(directory_entry_path),
            input_label: "directory entry fixture data",
        },
        expected_event_types,
        obligation_names,
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
    source_unit_id: &str,
    adapter_kind: AdapterKind,
    fixture: RecordFixtureSpec<'_>,
    expected_event_types: &[&str],
    obligation_names: &[&str],
) -> Vec<String> {
    let mut failures = Vec::new();
    let mut initial_ingestion_verified = false;
    for &obligation in obligation_names {
        let result = if obligation == "privacy" && initial_ingestion_verified {
            obligations::privacy::run_metadata_only(source_unit_id).await
        } else {
            _run_record_fixture_obligation(
                obligation,
                source_unit_id,
                adapter_kind,
                fixture.clone(),
                expected_event_types,
            )
            .await
        };
        if let Err(e) = result {
            failures.push(format!("[{source_unit_id}] obligation '{obligation}': {e}"));
        } else if obligation == "initial_ingestion" {
            initial_ingestion_verified = true;
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

async fn _run_record_fixture_obligation(
    obligation: &str,
    source_unit_id: &str,
    adapter_kind: AdapterKind,
    fixture: RecordFixtureSpec<'_>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    match obligation {
        "initial_ingestion" => {
            run_record_fixture_initial_ingestion(source_unit_id, fixture, expected_event_types)
                .await
        }
        "replay" => run_record_fixture_replay(source_unit_id, fixture, expected_event_types).await,
        "drain" => {
            obligations::drain::run(source_unit_id, adapter_kind, fixture.fixture_data).await
        }
        "isolation" => {
            obligations::isolation::run(source_unit_id, adapter_kind, fixture.fixture_data).await
        }
        "privacy" => {
            run_record_fixture_privacy(source_unit_id, fixture, expected_event_types).await
        }
        unknown => Err(format!(
            "unknown obligation '{unknown}'; valid: initial_ingestion, replay, drain, isolation, privacy"
        )),
    }
}

async fn run_record_fixture_initial_ingestion(
    source_unit_id: &str,
    fixture: RecordFixtureSpec<'_>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    let material_id = sinex_primitives::Uuid::now_v7();
    let outcome = dispatch_record_fixture_with_anchor(
        source_unit_id,
        fixture.fixture_data,
        fixture.anchor,
        fixture.logical_path,
        material_id,
    )
    .await
    .map_err(|e| format!("dispatch error for '{source_unit_id}': {e}"))?;

    if outcome.events.is_empty() {
        return Err(format!(
            "initial ingestion for '{source_unit_id}': parser returned no events for {} ({} bytes)",
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
                "initial ingestion for '{source_unit_id}': expected event type '{expected}' \
                 not found in output. Produced: {produced_types:?}"
            ));
        }
    }

    Ok(())
}

async fn run_record_fixture_replay(
    source_unit_id: &str,
    fixture: RecordFixtureSpec<'_>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    let material_id_1 = sinex_primitives::Uuid::now_v7();
    let outcome_1 = dispatch_record_fixture_with_anchor(
        source_unit_id,
        fixture.fixture_data,
        fixture.anchor.clone(),
        fixture.logical_path,
        material_id_1,
    )
    .await
    .map_err(|e| format!("replay first dispatch error for '{source_unit_id}': {e}"))?;

    let material_id_2 = sinex_primitives::Uuid::now_v7();
    let outcome_2 = dispatch_record_fixture_with_anchor(
        source_unit_id,
        fixture.fixture_data,
        fixture.anchor,
        fixture.logical_path,
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

async fn run_record_fixture_privacy(
    source_unit_id: &str,
    fixture: RecordFixtureSpec<'_>,
    expected_event_types: &[&str],
) -> Result<(), String> {
    run_record_fixture_initial_ingestion(source_unit_id, fixture.clone(), expected_event_types)
        .await
        .map_err(|e| format!("privacy/clean-path: {e}"))?;

    obligations::privacy::run_metadata_only(source_unit_id).await
}

async fn dispatch_record_fixture_with_anchor(
    source_unit_id: &str,
    fixture_data: &[u8],
    anchor: sinex_primitives::parser::MaterialAnchor,
    logical_path: Option<&str>,
    material_id: sinex_primitives::Uuid,
) -> Result<sinexd::sources::dispatch::ParseOutcome, String> {
    use camino::Utf8PathBuf;
    use sinex_primitives::events::SourceMaterial;
    use sinex_primitives::ids::Id;
    use sinex_primitives::parser::{ParserContext, SourceRecord, SourceUnitId};
    use sinex_primitives::temporal::Timestamp;
    use sinexd::sources::dispatch::find_parser_factory;

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
mod coverage_matrix {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::AdapterKind;
    use crate::obligations::drain;
    use sinex_primitives::parser::SourceUnitId;
    use sinexd::sources::dispatch::find_parser_factory;
    use sinexd::sources::node_factory::registered_node_factory_ids;
    use sinexd::sources::registry::SourceUnitRegistry;
    use xtask::sandbox::prelude::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum SmokeCoverage {
        BinaryPath,
        ObligationHarness,
        MonitorHarness,
        NoopHarness,
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
        entry(
            "desktop.window-manager",
            SmokeCoverage::ObligationHarness,
            "production_path/desktop.rs unix socket fixture",
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
        entry(
            "fs",
            SmokeCoverage::ObligationHarness,
            "production_path/fs.rs",
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
            SmokeCoverage::NoopHarness,
            "noop.rs noop_source_unit_reports_zero_work",
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
            SmokeCoverage::MonitorHarness,
            "monitor_node.rs monitor_fire_once_opens_material_and_emits_event",
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
            SmokeCoverage::MonitorHarness,
            "monitor_node.rs monitor_fire_once_opens_material_and_emits_event",
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

    #[sinex_test]
    async fn source_worker_smoke_matrix_covers_every_registered_factory() -> TestResult<()> {
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
    async fn source_worker_smoke_matrix_entries_are_actionable() -> TestResult<()> {
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

            if matches!(entry.coverage, SmokeCoverage::MonitorHarness) {
                assert!(
                    find_parser_factory(&id).is_none(),
                    "{} monitor smoke entry must remain parserless",
                    entry.source_unit_id
                );
            }

            if matches!(entry.coverage, SmokeCoverage::NoopHarness) {
                assert!(
                    find_parser_factory(&id).is_none(),
                    "{} noop smoke entry must remain parserless",
                    entry.source_unit_id
                );
                assert!(
                    descriptor.event_types.is_empty(),
                    "{} noop smoke entry must remain eventless",
                    entry.source_unit_id
                );
            }

            assert!(
                entry.blocker_issue.is_none(),
                "{} smoke entry has obsolete blocker metadata",
                entry.source_unit_id
            );
        }

        Ok(())
    }

    #[sinex_test]
    async fn source_worker_drain_obligation_covers_shared_controller() -> TestResult<()> {
        drain::run("source-worker.shared-drain", AdapterKind::StaticFile, b"")
            .await
            .map_err(SinexError::processing)?;

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
