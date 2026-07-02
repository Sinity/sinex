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
    assert!(
        summary_error
            .to_string()
            .contains("JUnit XML summary parse error")
    );
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
