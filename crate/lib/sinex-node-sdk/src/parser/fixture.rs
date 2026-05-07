//! Parser fixture harness for staged-source parser testing (#1130).
//!
//! This module provides reusable test infrastructure for parser authors.
//! The harness owns source material setup, adapter cursor setup, event-intent
//! comparison, and evidence capture so parser authors focus on parsing.
//!
//! # Architecture
//!
//! ```text
//! FixtureSpec ─► ParserFixtureHarness ─► MaterialParser ─► Vec<FixtureAssertion>
//!                   │                         ▲
//!                   │                         │
//!                   └─ InputShapeAdapter ──────┘
//!                       (stages material,
//!                        opens adapter,
//!                        invokes parser,
//!                        collects intents,
//!                        runs assertions)
//! ```
//!
//! The harness can operate at two levels:
//! - **Unit**: no NATS, no Postgres — pure adapter → parser → assertions.
//! - **Integration** (future): route accepted intents through source-worker path.

use std::collections::HashMap;

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use sinex_primitives::ids::Id;
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, ParsedEventIntent, ParserContext, ParserManifest,
    TimingEvidence,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::Uuid;
use sinex_primitives::events::SourceMaterial;

use super::{InputShapeAdapter, MaterialParser};

// =============================================================================
// ParserTestContext
// =============================================================================

/// Minimal test context for a parser fixture.
///
/// Provides identifiers and helpers that parsers need during testing without
/// requiring a live source-worker, NATS, or Postgres.
#[derive(Debug, Clone)]
pub struct ParserTestContext {
    /// The source unit this test belongs to.
    pub source_unit_id: String,

    /// The source material being parsed (test-generated).
    pub source_material_id: Id<SourceMaterial>,

    /// The operation that triggered this parse (test-generated).
    pub operation_id: Uuid,

    /// The parse job identifier (test-generated).
    pub job_id: Uuid,

    /// The host identifier (defaults to "test-host").
    pub host: String,

    /// When the record was acquired (test-generated).
    pub acquisition_time: Timestamp,
}

impl ParserTestContext {
    /// Create a new test context with generated identifiers.
    #[must_use]
    pub fn new(source_unit_id: impl Into<String>) -> Self {
        Self {
            source_unit_id: source_unit_id.into(),
            source_material_id: Id::new(),
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    /// Build a [`ParserContext`] for the given record.
    #[must_use]
    pub fn parser_context(&self, record_anchor: MaterialAnchor) -> ParserContext {
        ParserContext {
            source_unit_id: sinex_primitives::parser::SourceUnitId::from_static(
                "test-source-unit"
            ),
            source_material_id: self.source_material_id,
            record_anchor,
            operation_id: self.operation_id,
            job_id: self.job_id,
            host: self.host.clone(),
            acquisition_time: self.acquisition_time,
        }
    }
}

// =============================================================================
// FixtureSpec
// =============================================================================

/// A complete parser fixture specification.
///
/// A fixture describes: what material to stage, which adapter to use,
/// which parser to invoke, and what assertions to verify.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureSpec {
    /// Human-readable name for the fixture.
    pub name: String,

    /// Description of what the fixture tests.
    #[serde(default)]
    pub description: String,

    /// The input shape kind for this fixture.
    pub input_shape_kind: InputShapeKind,

    /// Raw bytes to stage as source material content.
    ///
    /// The harness creates a temp file with this content and passes it
    /// to the adapter. For SQLite adapters, this should be a valid
    /// SQLite database.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub material_bytes: Vec<u8>,

    /// Alternative: path to an existing file to use as material.
    /// When set, `material_bytes` is ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub material_path: Option<Utf8PathBuf>,

    /// Expected event intents from parsing.
    ///
    /// Each expectation specifies assertions on parser output.
    /// If empty, the harness expects no events (skip/negative fixture).
    #[serde(default)]
    pub expectations: Vec<FixtureExpectation>,

    /// If true, expect parsing to succeed but produce zero intents
    /// (e.g., unrecognized format, empty material).
    #[serde(default)]
    pub expect_no_intents: bool,

    /// If true, expect parsing to return an error.
    #[serde(default)]
    pub expect_error: bool,

    /// If `expect_error` is true, the error message must contain this string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_error_contains: Option<String>,

    /// Tags for fixture categorization and filtering.
    #[serde(default)]
    pub tags: Vec<String>,
}

// =============================================================================
// FixtureExpectation
// =============================================================================

/// An expected parsed event intent with assertions.
///
/// Each expectation describes what the `n`th event intent should look like.
/// Assertions are partial — only specified fields are checked.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureExpectation {
    /// 0-based index of the expected event intent.
    #[serde(default)]
    pub index: usize,

    /// Assertions on individual fields of the event intent.
    #[serde(default)]
    pub assertions: Vec<FixtureAssertion>,

    /// Optionally: compare against a golden artifact file.
    ///
    /// When set, the fixture harness loads the golden file and compares
    /// the full event intent JSON against it. `assertions` are still
    /// checked first for fast failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub golden_artifact: Option<Utf8PathBuf>,
}

// =============================================================================
// FixtureAssertion
// =============================================================================

/// A single assertion on a parsed event intent field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FixtureAssertion {
    /// Assert that `ts_orig` equals a specific value.
    Timestamp {
        value: Timestamp,
    },

    /// Assert that the event type field equals a specific value.
    EventType {
        expected: String,
    },

    /// Assert that the event source equals a specific value.
    EventSource {
        expected: String,
    },

    /// Assert that a JSON path in the payload equals a value.
    PayloadField {
        /// Dot-separated path into the payload JSON (e.g., "command", "path.file").
        path: String,
        /// Expected JSON value.
        value: serde_json::Value,
    },

    /// Assert that the anchor matches.
    Anchor {
        expected: MaterialAnchor,
    },

    /// Assert that the timing evidence matches.
    Timing {
        expected: TimingEvidence,
    },

    /// Assert that an occurrence key is present with specific fields.
    OccurrenceKey {
        expected_fields: Vec<(String, String)>,
    },

    /// Assert that an occurrence key is absent.
    NoOccurrenceKey,

    /// Assert that the parser ID and version match.
    ParserMetadata {
        parser_id: String,
        parser_version: String,
    },

    /// Assert that a specific JSON path is present (non-null).
    FieldPresent {
        path: String,
    },

    /// Assert that a specific JSON path is absent or null.
    FieldAbsent {
        path: String,
    },
}

// =============================================================================
// FixtureOutcome
// =============================================================================

/// The outcome of running a single fixture.
#[derive(Debug)]
pub struct FixtureOutcome {
    /// The fixture name.
    pub fixture_name: String,

    /// Whether all assertions passed.
    pub passed: bool,

    /// Number of event intents produced by the parser.
    pub intent_count: usize,

    /// Failures discovered during the run.
    pub failures: Vec<FixtureFailure>,

    /// The raw event intents produced (for debugging).
    pub intents: Vec<ParsedEventIntent>,
}

/// A single assertion failure.
#[derive(Debug, Clone)]
pub struct FixtureFailure {
    /// The index of the event intent that failed (if applicable).
    pub intent_index: Option<usize>,

    /// Description of what was expected.
    pub expected: String,

    /// Description of what was found.
    pub found: String,
}

// =============================================================================
// ParserFixtureHarness
// =============================================================================

/// Reusable test harness for parser fixtures.
///
/// The harness stages source material, opens an input-shape adapter,
/// invokes a parser, collects event intents, and runs assertions.
///
/// # Usage
///
/// ```ignore
/// let harness = ParserFixtureHarness::new();
///
/// // Option 1: Run a single fixture spec
/// let outcome = harness.run(
///     &fixture_spec,
///     &adapter,
///     &mut parser,
///     &test_context,
/// ).await?;
/// assert!(outcome.passed);
///
/// // Option 2: Load and run fixtures from a directory
/// let outcomes = harness.run_directory(
///     &PathBuf::from("tests/fixtures/atuin"),
///     &adapter,
///     &mut parser,
///     &test_context,
/// ).await?;
/// ```
pub struct ParserFixtureHarness {
    /// Directory for temporary material files.
    /// If None, a new temp dir is created per run.
    temp_dir: Option<TempDir>,
    /// Loaded golden artifacts, keyed by path.
    golden_cache: HashMap<Utf8PathBuf, serde_json::Value>,
}

impl Default for ParserFixtureHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl ParserFixtureHarness {
    /// Create a new harness with a fresh golden artifact cache.
    #[must_use]
    pub fn new() -> Self {
        Self {
            temp_dir: None,
            golden_cache: HashMap::new(),
        }
    }

    /// Create a harness with a specific temp directory.
    ///
    /// The caller is responsible for cleaning up the temp directory.
    #[must_use]
    pub fn with_temp_dir(temp_dir: TempDir) -> Self {
        Self {
            temp_dir: Some(temp_dir),
            golden_cache: HashMap::new(),
        }
    }

    /// Load a golden artifact from disk and return the expected JSON value.
    ///
    /// Golden artifacts are cached in memory after first load.
    pub fn load_golden(
        &mut self,
        path: &Utf8PathBuf,
    ) -> Result<serde_json::Value, String> {
        if let Some(cached) = self.golden_cache.get(path) {
            return Ok(cached.clone());
        }

        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read golden artifact {path}: {e}"))?;

        let value: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| format!("failed to parse golden artifact {path}: {e}"))?;

        self.golden_cache.insert(path.clone(), value.clone());
        Ok(value)
    }

    /// Run a fixture with explicit adapter and parser configs.
    ///
    /// This is the primary execution method. Callers supply concrete configs
    /// and the harness manages the pipeline: material → adapter → parser → assertions.
    pub async fn run_with_config<A, P>(
        &mut self,
        spec: &FixtureSpec,
        adapter: &A,
        adapter_config: &A::Config,
        adapter_cursor: Option<A::Cursor>,
        parser: &mut P,
        _parser_config: &P::Config,
        test_ctx: &ParserTestContext,
        manifest: &ParserManifest,
    ) -> FixtureOutcome
    where
        A: InputShapeAdapter + Sync,
        P: MaterialParser + Send,
        A::Config: Serialize + serde::de::DeserializeOwned + Send + Sync,
        A::Cursor: Serialize + serde::de::DeserializeOwned + Send + Sync,
        P::Config: Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        use futures::StreamExt;

        let material_id = test_ctx.source_material_id;
        let mut failures: Vec<FixtureFailure> = Vec::new();
        let mut intents: Vec<ParsedEventIntent> = Vec::new();

        // Validate manifest matches expectation.
        if let Some(exp) = spec
            .expectations
            .iter()
            .find(|e| {
                e.assertions.iter().any(|a| matches!(a, FixtureAssertion::ParserMetadata { .. }))
            })
        {
            for assertion in &exp.assertions {
                if let FixtureAssertion::ParserMetadata {
                    parser_id,
                    parser_version,
                } = assertion
                {
                    if manifest.parser_id.as_str() != parser_id {
                        failures.push(FixtureFailure {
                            intent_index: None,
                            expected: format!("parser_id={parser_id}"),
                            found: format!("parser_id={}", manifest.parser_id.as_str()),
                        });
                    }
                    if &manifest.parser_version != parser_version {
                        failures.push(FixtureFailure {
                            intent_index: None,
                            expected: format!("parser_version={parser_version}"),
                            found: format!("parser_version={}", manifest.parser_version),
                        });
                    }
                }
            }
        }

        // Open the adapter and consume all records.
        let stream = match adapter
            .open(material_id, adapter_config, adapter_cursor)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                if spec.expect_error {
                    let err_str = format!("{e}");
                    if let Some(ref expected_contains) = spec.expected_error_contains {
                        if !err_str.contains(expected_contains.as_str()) {
                            failures.push(FixtureFailure {
                                intent_index: None,
                                expected: format!("error containing \"{expected_contains}\""),
                                found: format!("error: {err_str}"),
                            });
                        }
                    }
                    return FixtureOutcome {
                        fixture_name: spec.name.clone(),
                        passed: failures.is_empty(),
                        intent_count: 0,
                        failures,
                        intents: vec![],
                    };
                }

                failures.push(FixtureFailure {
                    intent_index: None,
                    expected: "adapter to open successfully".into(),
                    found: format!("adapter error: {e}"),
                });
                return FixtureOutcome {
                    fixture_name: spec.name.clone(),
                    passed: false,
                    intent_count: 0,
                    failures,
                    intents: vec![],
                };
            }
        };

        tokio::pin!(stream);

        // Process each record through the parser.
        while let Some(record_result) = stream.next().await {
            match record_result {
                Ok(record) => {
                    let anchor = record.anchor.clone();
                    let ctx = test_ctx.parser_context(anchor);

                    match parser.parse_record(record, &ctx).await {
                        Ok(record_intents) => {
                            intents.extend(record_intents);
                        }
                        Err(e) => {
                            if spec.expect_error {
                                let err_str = format!("{e}");
                                if let Some(ref expected_contains) = spec.expected_error_contains {
                                    if !err_str.contains(expected_contains.as_str()) {
                                        failures.push(FixtureFailure {
                                            intent_index: None,
                                            expected: format!(
                                                "error containing \"{expected_contains}\""
                                            ),
                                            found: format!("error: {err_str}"),
                                        });
                                    }
                                }
                                return FixtureOutcome {
                                    fixture_name: spec.name.clone(),
                                    passed: failures.is_empty(),
                                    intent_count: intents.len(),
                                    failures,
                                    intents,
                                };
                            }

                            failures.push(FixtureFailure {
                                intent_index: None,
                                expected: "successful parse".into(),
                                found: format!("parse error: {e}"),
                            });
                        }
                    }
                }
                Err(e) => {
                    failures.push(FixtureFailure {
                        intent_index: None,
                        expected: "valid source record".into(),
                        found: format!("record error: {e}"),
                    });
                }
            }
        }

        // Check expectations.
        if spec.expect_no_intents {
            if !intents.is_empty() {
                failures.push(FixtureFailure {
                    intent_index: None,
                    expected: "no event intents".into(),
                    found: format!("{} intent(s) produced", intents.len()),
                });
            }
        } else if spec.expect_error {
            if failures.is_empty() {
                failures.push(FixtureFailure {
                    intent_index: None,
                    expected: "parse error".into(),
                    found: "no error returned".into(),
                });
            }
        } else {
            // Run field assertions on each expectation.
            for expectation in &spec.expectations {
                let intent = if expectation.index < intents.len() {
                    &intents[expectation.index]
                } else {
                    failures.push(FixtureFailure {
                        intent_index: Some(expectation.index),
                        expected: format!("intent at index {}", expectation.index),
                        found: format!(
                            "only {} intent(s) produced",
                            intents.len()
                        ),
                    });
                    continue;
                };

                // Check against golden artifact if specified.
                if let Some(ref golden_path) = expectation.golden_artifact {
                    match self.load_golden(golden_path) {
                        Ok(expected_json) => {
                            let actual_json = serde_json::to_value(intent).unwrap_or_default();
                            if expected_json != actual_json {
                                failures.push(FixtureFailure {
                                    intent_index: Some(expectation.index),
                                    expected: format!(
                                        "intent matching golden artifact {golden_path}"
                                    ),
                                    found: format!(
                                        "golden mismatch (expected != actual)"
                                    ),
                                });
                            }
                        }
                        Err(e) => {
                            failures.push(FixtureFailure {
                                intent_index: Some(expectation.index),
                                expected: format!("golden artifact {golden_path} to load"),
                                found: format!("load error: {e}"),
                            });
                        }
                    }
                }

                // Run field-level assertions.
                for assertion in &expectation.assertions {
                    match assertion {
                        FixtureAssertion::EventType { expected } => {
                            let actual = intent.event_type.as_str();
                            if actual != expected {
                                failures.push(FixtureFailure {
                                    intent_index: Some(expectation.index),
                                    expected: format!("event_type={expected}"),
                                    found: format!("event_type={actual}"),
                                });
                            }
                        }
                        FixtureAssertion::EventSource { expected } => {
                            let actual = intent.event_source.as_str();
                            if actual != expected {
                                failures.push(FixtureFailure {
                                    intent_index: Some(expectation.index),
                                    expected: format!("event_source={expected}"),
                                    found: format!("event_source={actual}"),
                                });
                            }
                        }
                        FixtureAssertion::Timestamp { value } => {
                            if intent.ts_orig != *value {
                                failures.push(FixtureFailure {
                                    intent_index: Some(expectation.index),
                                    expected: format!("ts_orig={value}"),
                                    found: format!("ts_orig={}", intent.ts_orig),
                                });
                            }
                        }
                        FixtureAssertion::Anchor { expected } => {
                            if intent.anchor != *expected {
                                failures.push(FixtureFailure {
                                    intent_index: Some(expectation.index),
                                    expected: format!("anchor={expected:?}"),
                                    found: format!("anchor={:?}", intent.anchor),
                                });
                            }
                        }
                        FixtureAssertion::Timing { expected } => {
                            if intent.timing != *expected {
                                failures.push(FixtureFailure {
                                    intent_index: Some(expectation.index),
                                    expected: format!("timing={expected:?}"),
                                    found: format!("timing={:?}", intent.timing),
                                });
                            }
                        }
                        FixtureAssertion::OccurrenceKey { expected_fields } => {
                            match &intent.occurrence_key {
                                Some(key) => {
                                    let actual: Vec<(String, String)> = key
                                        .fields
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect();
                                    if &actual != expected_fields {
                                        failures.push(FixtureFailure {
                                            intent_index: Some(expectation.index),
                                            expected: format!(
                                                "occurrence_key fields={expected_fields:?}"
                                            ),
                                            found: format!(
                                                "occurrence_key fields={actual:?}"
                                            ),
                                        });
                                    }
                                }
                                None => {
                                    failures.push(FixtureFailure {
                                        intent_index: Some(expectation.index),
                                        expected: format!(
                                            "occurrence_key with fields={expected_fields:?}"
                                        ),
                                        found: "no occurrence_key".into(),
                                    });
                                }
                            }
                        }
                        FixtureAssertion::NoOccurrenceKey => {
                            if intent.occurrence_key.is_some() {
                                failures.push(FixtureFailure {
                                    intent_index: Some(expectation.index),
                                    expected: "no occurrence_key".into(),
                                    found: "occurrence_key present".into(),
                                });
                            }
                        }
                        FixtureAssertion::ParserMetadata {
                            parser_id,
                            parser_version,
                        } => {
                            if intent.parser_id.as_str() != parser_id {
                                failures.push(FixtureFailure {
                                    intent_index: Some(expectation.index),
                                    expected: format!("parser_id={parser_id}"),
                                    found: format!("parser_id={}", intent.parser_id.as_str()),
                                });
                            }
                            if intent.parser_version != *parser_version {
                                failures.push(FixtureFailure {
                                    intent_index: Some(expectation.index),
                                    expected: format!("parser_version={parser_version}"),
                                    found: format!("parser_version={}", intent.parser_version),
                                });
                            }
                        }
                        FixtureAssertion::PayloadField { path, value } => {
                            let actual = json_path_get(&intent.payload, path);
                            match actual {
                                Some(v) if v == value => {}
                                Some(v) => {
                                    failures.push(FixtureFailure {
                                        intent_index: Some(expectation.index),
                                        expected: format!("payload.{path}={value}"),
                                        found: format!("payload.{path}={v}"),
                                    });
                                }
                                None => {
                                    failures.push(FixtureFailure {
                                        intent_index: Some(expectation.index),
                                        expected: format!("payload.{path}={value}"),
                                        found: format!("payload.{path}=<absent>"),
                                    });
                                }
                            }
                        }
                        FixtureAssertion::FieldPresent { path } => {
                            let actual = json_path_get(&intent.payload, path);
                            if actual.is_none() {
                                failures.push(FixtureFailure {
                                    intent_index: Some(expectation.index),
                                    expected: format!("payload.{path} present"),
                                    found: format!("payload.{path} absent or null"),
                                });
                            }
                        }
                        FixtureAssertion::FieldAbsent { path } => {
                            let actual = json_path_get(&intent.payload, path);
                            if actual.is_some() {
                                failures.push(FixtureFailure {
                                    intent_index: Some(expectation.index),
                                    expected: format!("payload.{path} absent"),
                                    found: format!("payload.{path}={}", actual.unwrap()),
                                });
                            }
                        }
                    }
                }
            }
        }

        FixtureOutcome {
            fixture_name: spec.name.clone(),
            passed: failures.is_empty(),
            intent_count: intents.len(),
            failures,
            intents,
        }
    }

    /// Create a temporary file with the given bytes for use as source material.
    fn create_temp_material(
        &mut self,
        name: &str,
        bytes: &[u8],
    ) -> Result<std::path::PathBuf, std::io::Error> {
        use std::io::Write;

        // Lazily create temp_dir if not already set.
        if self.temp_dir.is_none() {
            self.temp_dir = Some(tempfile::tempdir()?);
        }

        let dir = self.temp_dir.as_ref().unwrap().path();
        let safe_name = name.replace(['/', '\\', ' '], "_");
        let file_path = dir.join(safe_name);

        let mut f = std::fs::File::create(&file_path)?;
        f.write_all(bytes)?;
        f.flush()?;

        Ok(file_path)
    }

    /// Validate a negative fixture: run the adapter-parser pipeline and
    /// assert that parsing fails (or produces specific error content).
    pub async fn assert_negative<A, P>(
        &mut self,
        spec: &FixtureSpec,
        adapter: &A,
        adapter_config: &A::Config,
        adapter_cursor: Option<A::Cursor>,
        parser: &mut P,
        _parser_config: &P::Config,
        test_ctx: &ParserTestContext,
        manifest: &ParserManifest,
    ) -> FixtureOutcome
    where
        A: InputShapeAdapter + Sync,
        P: MaterialParser + Send,
        A::Config: Serialize + serde::de::DeserializeOwned + Send + Sync,
        A::Cursor: Serialize + serde::de::DeserializeOwned + Send + Sync,
        P::Config: Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        if !spec.expect_error {
            // For negative fixtures, set expect_error = true.
            let mut spec = spec.clone();
            spec.expect_error = true;
            return self
                .run_with_config(
                    &spec,
                    adapter,
                    adapter_config,
                    adapter_cursor,
                    parser,
                    _parser_config,
                    test_ctx,
                    manifest,
                )
                .await;
        }
        self.run_with_config(
            spec,
            adapter,
            adapter_config,
            adapter_cursor,
            parser,
            _parser_config,
            test_ctx,
            manifest,
        )
        .await
    }
}

// =============================================================================
// JSON path helpers
// =============================================================================

/// Get a value at a dot-separated JSON path.
fn json_path_get<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(segment)?;
            }
            serde_json::Value::Array(arr) => {
                let index: usize = segment.parse().ok()?;
                current = arr.get(index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}
