//! JUnit XML parser for extracting test outputs from nextest JUnit reports.
//!
//! nextest's `libtest-json-plus` format only includes `stdout` for failed tests.
//! However, the JUnit XML report (when `store-success-output = true` in nextest.toml)
//! includes `<system-out>` for ALL tests. This module parses the JUnit XML after a
//! test run to back-fill output for passing tests into the history database.

use color_eyre::eyre::{Result, WrapErr, eyre};
use quick_xml::escape::unescape;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::collections::HashMap;
use std::path::Path;

/// Relative path within `target/nextest/{profile}/` to the JUnit XML report.
/// Configured in `.config/nextest.toml` under `[profile.default.junit]`.
const JUNIT_XML_RELATIVE: &str = ".sinex/nextest/junit.xml";

/// A parsed test entry from JUnit XML with its captured output.
#[derive(Debug)]
pub struct JunitTestOutput {
    /// Test function name (matches libtest-json `name` field)
    pub test_name: String,
    /// Captured stdout/stderr (from `<system-out>` element)
    pub output: String,
}

/// Enriched test metadata extracted from JUnit XML.
///
/// Contains output, classname (reliable package source), and failure details.
#[derive(Debug, Default)]
pub struct JunitTestMeta {
    /// Captured stdout/stderr (from `<system-out>` element)
    pub output: Option<String>,
    /// Crate/package name from the `classname` attribute (more reliable than name parsing)
    pub classname: Option<String>,
    /// Failure message from `<failure message="...">` (None if test passed)
    pub failure_message: Option<String>,
    /// Failure type from `<failure type="...">` (None if test passed)
    pub failure_type: Option<String>,
}

/// Aggregated test counts extracted from JUnit XML.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct JunitSummary {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub ignored: usize,
}

/// Parse the nextest JUnit XML report and extract test metadata.
///
/// Returns a map of `test_name -> JunitTestMeta` for all tests. Tests without any
/// extractable metadata (no output, no failure) are omitted.
pub fn parse_junit_metadata(path: &Path) -> Result<HashMap<String, JunitTestMeta>> {
    let xml_content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read JUnit XML at {}", path.display()))?;

    let mut reader = Reader::from_str(&xml_content);
    let mut results: HashMap<String, JunitTestMeta> = HashMap::new();

    // State machine for parsing
    let mut current_test_name: Option<String> = None;
    let mut current_classname: Option<String> = None;
    let mut current_failure_message: Option<String> = None;
    let mut current_failure_type: Option<String> = None;
    let mut in_system_out = false;
    let mut system_out_buf = String::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                match e.name().as_ref() {
                    b"testcase" => {
                        // Extract `name` and `classname` attributes
                        current_test_name = None;
                        current_classname = None;
                        current_failure_message = None;
                        current_failure_type = None;
                        for attr in e.attributes() {
                            let attr = attr.map_err(|error| {
                                eyre!(
                                    "malformed JUnit testcase attribute at position {}: {error}",
                                    reader.error_position()
                                )
                            })?;
                            match attr.key.as_ref() {
                                b"name" => {
                                    current_test_name = Some(String::from_utf8(attr.value.to_vec()).map_err(
                                        |error| {
                                            eyre!(
                                                "JUnit testcase name is not valid UTF-8 at position {}: {error}",
                                                reader.error_position()
                                            )
                                        },
                                    )?);
                                }
                                b"classname" => {
                                    current_classname =
                                        Some(String::from_utf8(attr.value.to_vec()).map_err(
                                            |error| {
                                                eyre!(
                                                    "JUnit testcase classname is not valid UTF-8 at position {}: {error}",
                                                    reader.error_position()
                                                )
                                            },
                                        )?);
                                }
                                _ => {}
                            }
                        }
                    }
                    b"system-out" => {
                        in_system_out = true;
                        system_out_buf.clear();
                    }
                    b"failure" => {
                        // Extract message and type attributes from <failure>
                        for attr in e.attributes() {
                            let attr = attr.map_err(|error| {
                                eyre!(
                                    "malformed JUnit failure attribute at position {}: {error}",
                                    reader.error_position()
                                )
                            })?;
                            match attr.key.as_ref() {
                                b"message" => {
                                    current_failure_message =
                                        Some(String::from_utf8(attr.value.to_vec()).map_err(
                                            |error| {
                                                eyre!(
                                                    "JUnit failure message is not valid UTF-8 at position {}: {error}",
                                                    reader.error_position()
                                                )
                                            },
                                        )?);
                                }
                                b"type" => {
                                    current_failure_type =
                                        Some(String::from_utf8(attr.value.to_vec()).map_err(
                                            |error| {
                                                eyre!(
                                                    "JUnit failure type is not valid UTF-8 at position {}: {error}",
                                                    reader.error_position()
                                                )
                                            },
                                        )?);
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => match e.name().as_ref() {
                b"testcase" => {
                    // Flush accumulated metadata for this test
                    if let Some(ref name) = current_test_name {
                        let trimmed_output = system_out_buf.trim();
                        let has_output = !trimmed_output.is_empty();
                        let has_failure = current_failure_message.is_some();
                        let has_classname = current_classname.is_some();

                        if has_output || has_failure || has_classname {
                            results.insert(
                                name.clone(),
                                JunitTestMeta {
                                    output: if has_output {
                                        Some(trimmed_output.to_string())
                                    } else {
                                        None
                                    },
                                    classname: current_classname.take(),
                                    failure_message: current_failure_message.take(),
                                    failure_type: current_failure_type.take(),
                                },
                            );
                        }
                    }
                    current_test_name = None;
                    current_classname = None;
                    current_failure_message = None;
                    current_failure_type = None;
                    system_out_buf.clear();
                }
                b"system-out" => {
                    in_system_out = false;
                }
                _ => {}
            },
            Ok(Event::Empty(ref e)) => {
                // Self-closing <testcase ... /> — extract classname if present
                if e.name().as_ref() == b"testcase" {
                    let mut name = None;
                    let mut classname = None;
                    for attr in e.attributes() {
                        let attr = attr.map_err(|error| {
                            eyre!(
                                "malformed self-closing JUnit testcase attribute at position {}: {error}",
                                reader.error_position()
                            )
                        })?;
                        match attr.key.as_ref() {
                            b"name" => {
                                name = Some(String::from_utf8(attr.value.to_vec()).map_err(
                                    |error| {
                                        eyre!(
                                            "JUnit testcase name is not valid UTF-8 at position {}: {error}",
                                            reader.error_position()
                                        )
                                    },
                                )?);
                            }
                            b"classname" => {
                                classname = Some(String::from_utf8(attr.value.to_vec()).map_err(
                                    |error| {
                                        eyre!(
                                            "JUnit testcase classname is not valid UTF-8 at position {}: {error}",
                                            reader.error_position()
                                        )
                                    },
                                )?);
                            }
                            _ => {}
                        }
                    }
                    if let Some(n) = name
                        && classname.is_some()
                    {
                        results.insert(
                            n,
                            JunitTestMeta {
                                classname,
                                ..Default::default()
                            },
                        );
                    }
                }
            }
            Ok(Event::Text(e)) => {
                if in_system_out {
                    let raw = std::str::from_utf8(&e).map_err(|error| {
                        eyre!(
                            "JUnit system-out text is not valid UTF-8 at position {}: {error}",
                            reader.error_position()
                        )
                    })?;
                    let text = unescape(raw).map_err(|error| {
                        eyre!(
                            "JUnit system-out text contains invalid escape sequences at position {}: {error}",
                            reader.error_position()
                        )
                    })?;
                    system_out_buf.push_str(&text);
                }
            }
            Ok(Event::CData(e)) => {
                if in_system_out {
                    let text = std::str::from_utf8(&e).map_err(|error| {
                        eyre!(
                            "JUnit system-out CDATA is not valid UTF-8 at position {}: {error}",
                            reader.error_position()
                        )
                    })?;
                    system_out_buf.push_str(text);
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(eyre!(
                    "JUnit XML parse error at position {}: {e}",
                    reader.error_position()
                ));
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(results)
}

/// Parse aggregate test counts from the nextest JUnit XML report.
///
/// This is used to repair streamed nextest stats when libtest-json-plus output
/// underreports passing or failing tests.
pub fn parse_junit_summary(path: &Path) -> Result<JunitSummary> {
    let xml_content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read JUnit XML at {}", path.display()))?;

    let mut reader = Reader::from_str(&xml_content);
    let mut summary = JunitSummary::default();
    let mut in_testcase = false;
    let mut testcase_failed = false;
    let mut testcase_ignored = false;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name().as_ref() {
                b"testcase" => {
                    in_testcase = true;
                    testcase_failed = false;
                    testcase_ignored = false;
                }
                b"failure" if in_testcase => testcase_failed = true,
                b"skipped" if in_testcase => testcase_ignored = true,
                _ => {}
            },
            Ok(Event::Empty(ref e)) => match e.name().as_ref() {
                b"testcase" => {
                    summary.total += 1;
                    summary.passed += 1;
                }
                b"failure" if in_testcase => testcase_failed = true,
                b"skipped" if in_testcase => testcase_ignored = true,
                _ => {}
            },
            Ok(Event::End(ref e)) => {
                if e.name().as_ref() == b"testcase" && in_testcase {
                    summary.total += 1;
                    if testcase_failed {
                        summary.failed += 1;
                    } else if testcase_ignored {
                        summary.ignored += 1;
                    } else {
                        summary.passed += 1;
                    }
                    in_testcase = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(eyre!(
                    "JUnit XML summary parse error at position {}: {e}",
                    reader.error_position()
                ));
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(summary)
}

/// Parse the nextest JUnit XML report and extract test outputs.
///
/// Returns a map of `test_name -> output` for all tests that have `<system-out>` content.
/// Tests without output (empty `<testcase>` elements) are skipped.
///
/// This is a convenience wrapper around `parse_junit_metadata()` for callers
/// that only need the output strings.
pub fn parse_junit_outputs(path: &Path) -> Result<HashMap<String, String>> {
    let meta = parse_junit_metadata(path)?;
    Ok(meta
        .into_iter()
        .filter_map(|(name, m)| m.output.map(|o| (name, o)))
        .collect())
}

/// Get the JUnit XML path for a given nextest profile.
///
/// nextest writes JUnit XML to `target/nextest/{profile}/{junit.path}` where
/// `junit.path` comes from `[profile.{name}.junit]` in nextest.toml (inherited
/// from `[profile.default.junit]` if not overridden).
#[must_use]
pub fn junit_path_for_profile(profile: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("target/nextest/{profile}/{JUNIT_XML_RELATIVE}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[sinex_test]
    async fn test_parse_passing_test_with_output() -> TestResult<()> {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="2" failures="0">
    <testsuite name="my-crate" tests="2">
        <testcase name="test_basic" classname="my-crate" time="0.5">
            <system-out>
running 1 test
test output here
test result: ok. 1 passed
            </system-out>
        </testcase>
        <testcase name="test_empty" classname="my-crate" time="0.1">
        </testcase>
    </testsuite>
</testsuites>"#;

        let mut f = NamedTempFile::new()?;
        f.write_all(xml.as_bytes())?;

        let outputs = parse_junit_outputs(f.path())?;
        assert_eq!(outputs.len(), 1);
        assert!(outputs.contains_key("test_basic"));
        assert!(outputs["test_basic"].contains("test output here"));
        assert!(!outputs.contains_key("test_empty"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_failure_with_output() -> TestResult<()> {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="1" failures="1">
    <testsuite name="my-crate" tests="1" failures="1">
        <testcase name="test_failing" classname="my-crate" time="1.0">
            <failure message="assertion failed" type="test failure">assertion failed at line 42</failure>
            <system-out>
running 1 test
detailed failure output
test result: FAILED
            </system-out>
            <system-err>(stdout and stderr are combined)</system-err>
        </testcase>
    </testsuite>
</testsuites>"#;

        let mut f = NamedTempFile::new()?;
        f.write_all(xml.as_bytes())?;

        let outputs = parse_junit_outputs(f.path())?;
        assert_eq!(outputs.len(), 1);
        assert!(outputs.contains_key("test_failing"));
        assert!(outputs["test_failing"].contains("detailed failure output"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_multiple_suites() -> TestResult<()> {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="3" failures="0">
    <testsuite name="crate-a" tests="1">
        <testcase name="test_a" classname="crate-a" time="0.1">
            <system-out>output a</system-out>
        </testcase>
    </testsuite>
    <testsuite name="crate-b" tests="2">
        <testcase name="test_b1" classname="crate-b" time="0.2">
            <system-out>output b1</system-out>
        </testcase>
        <testcase name="test_b2" classname="crate-b" time="0.3">
            <system-out>output b2</system-out>
        </testcase>
    </testsuite>
</testsuites>"#;

        let mut f = NamedTempFile::new()?;
        f.write_all(xml.as_bytes())?;

        let outputs = parse_junit_outputs(f.path())?;
        assert_eq!(outputs.len(), 3);
        assert_eq!(outputs["test_a"], "output a");
        assert_eq!(outputs["test_b1"], "output b1");
        assert_eq!(outputs["test_b2"], "output b2");
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_empty_system_out_skipped() -> TestResult<()> {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="1" failures="0">
    <testsuite name="my-crate" tests="1">
        <testcase name="test_quiet" classname="my-crate" time="0.1">
            <system-out>
            </system-out>
        </testcase>
    </testsuite>
</testsuites>"#;

        let mut f = NamedTempFile::new()?;
        f.write_all(xml.as_bytes())?;

        let outputs = parse_junit_outputs(f.path())?;
        assert_eq!(outputs.len(), 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_missing_file_returns_error() -> TestResult<()> {
        let result = parse_junit_outputs(Path::new("/nonexistent/junit.xml"));
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_malformed_xml_returns_error() -> TestResult<()> {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
    <testsuite>
        <testcase name="broken">
            <system-out>unterminated
        </testcase>
    </testsuite>
</testsuites>"#;

        let mut f = NamedTempFile::new()?;
        f.write_all(xml.as_bytes())?;

        let error = parse_junit_outputs(f.path()).expect_err("malformed XML must fail honestly");
        assert!(error.to_string().contains("JUnit XML parse error"));

        let summary_error =
            parse_junit_summary(f.path()).expect_err("malformed XML summary must fail honestly");
        assert!(summary_error
            .to_string()
            .contains("JUnit XML summary parse error"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_metadata_extracts_classname() -> TestResult<()> {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="2" failures="0">
    <testsuite name="sinex-db" tests="2">
        <testcase name="test_query" classname="sinex-db" time="0.5">
            <system-out>query output</system-out>
        </testcase>
        <testcase name="test_pool" classname="sinex-db" time="0.3" />
    </testsuite>
</testsuites>"#;

        let mut f = NamedTempFile::new()?;
        f.write_all(xml.as_bytes())?;

        let meta = parse_junit_metadata(f.path())?;

        // test_query has output and classname
        let query_meta = &meta["test_query"];
        assert_eq!(query_meta.classname.as_deref(), Some("sinex-db"));
        assert_eq!(query_meta.output.as_deref(), Some("query output"));
        assert!(query_meta.failure_message.is_none());

        // test_pool is self-closing with classname but no output
        let pool_meta = &meta["test_pool"];
        assert_eq!(pool_meta.classname.as_deref(), Some("sinex-db"));
        assert!(pool_meta.output.is_none());

        Ok(())
    }

    #[sinex_test]
    async fn test_parse_metadata_extracts_failure_info() -> TestResult<()> {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="1" failures="1">
    <testsuite name="my-crate" tests="1" failures="1">
        <testcase name="test_fails" classname="my-crate" time="2.0">
            <failure message="assertion `left == right` failed" type="test failure">
                full stack trace here
            </failure>
            <system-out>
[sandbox:INFO] event=slot_acquired slot=sinex_test_pool_7 duration_ms=150 pid=12345 clean=true
test output
            </system-out>
        </testcase>
    </testsuite>
</testsuites>"#;

        let mut f = NamedTempFile::new()?;
        f.write_all(xml.as_bytes())?;

        let meta = parse_junit_metadata(f.path())?;
        let test_meta = &meta["test_fails"];

        assert_eq!(
            test_meta.failure_message.as_deref(),
            Some("assertion `left == right` failed")
        );
        assert_eq!(test_meta.failure_type.as_deref(), Some("test failure"));
        assert_eq!(test_meta.classname.as_deref(), Some("my-crate"));
        assert!(test_meta.output.as_deref().unwrap().contains("test output"));

        Ok(())
    }

    #[sinex_test]
    async fn test_parse_junit_summary_counts_pass_fail_and_skip() -> TestResult<()> {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="3" failures="1">
    <testsuite name="my-crate" tests="3" failures="1" skipped="1">
        <testcase name="test_pass" classname="my-crate" time="0.1" />
        <testcase name="test_fail" classname="my-crate" time="0.2">
            <failure message="boom" type="test failure">stacktrace</failure>
        </testcase>
        <testcase name="test_skip" classname="my-crate" time="0.0">
            <skipped />
        </testcase>
    </testsuite>
</testsuites>"#;

        let mut f = NamedTempFile::new()?;
        f.write_all(xml.as_bytes())?;

        let summary = parse_junit_summary(f.path())?;
        assert_eq!(
            summary,
            JunitSummary {
                total: 3,
                passed: 1,
                failed: 1,
                ignored: 1,
            }
        );

        Ok(())
    }
}
