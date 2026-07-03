use super::{TestPhaseObserver, TestReporter};
use crate::sandbox::sinex_test;
use std::io::Cursor;

#[derive(Default)]
struct CountingPhaseObserver {
    suite_started_count: usize,
}

impl TestPhaseObserver for CountingPhaseObserver {
    fn suite_started(&mut self) {
        self.suite_started_count += 1;
    }
}

#[sinex_test]
async fn suite_totals_backfill_stream_parse_gaps() -> ::xtask::sandbox::TestResult<()> {
    let stdout = Cursor::new(
        concat!(
            "{\"type\":\"suite\",\"event\":\"started\",\"test_count\":1}\n",
            "{\"type\":\"suite\",\"event\":\"failed\",\"passed\":0,\"failed\":1,\"ignored\":0}\n",
        )
        .as_bytes(),
    );
    let stderr = Cursor::new(Vec::<u8>::new());

    let stats = TestReporter::new(false)
        .run(stdout, stderr, None, None)
        .expect("suite-only failure output should still produce stats");

    assert_eq!(stats.failed, 1);
    assert_eq!(stats.passed, 0);
    assert_eq!(stats.ignored, 0);
    assert_eq!(stats.total, 1);
    Ok(())
}

#[sinex_test]
async fn malformed_stdout_json_fails_honestly() -> ::xtask::sandbox::TestResult<()> {
    let stdout = Cursor::new(
        concat!(
            "{\"type\":\"suite\",\"event\":\"started\",\"test_count\":1}\n",
            "not-json\n",
        )
        .as_bytes(),
    );
    let stderr = Cursor::new(Vec::<u8>::new());

    let error = TestReporter::new(false)
        .run(stdout, stderr, None, None)
        .expect_err("malformed nextest stdout must fail honestly");
    let message = error.to_string();
    assert!(
        message.contains("failed to parse nextest stdout line: not-json"),
        "malformed stdout line was not preserved in error report: {message}"
    );
    Ok(())
}

#[sinex_test]
async fn missing_required_test_name_fails_honestly() -> ::xtask::sandbox::TestResult<()> {
    let stdout = Cursor::new(
        concat!(
            "{\"type\":\"suite\",\"event\":\"started\",\"test_count\":1}\n",
            "{\"type\":\"test\",\"event\":\"ok\"}\n",
        )
        .as_bytes(),
    );
    let stderr = Cursor::new(Vec::<u8>::new());

    let error = TestReporter::new(false)
        .run(stdout, stderr, None, None)
        .expect_err("missing nextest test name must fail honestly");
    let message = format!("{error:#}");
    assert!(
        message.contains("nextest test-finished message is missing required field 'name'"),
        "missing-field cause was not preserved in error chain: {message}"
    );
    Ok(())
}

#[sinex_test]
async fn phase_observer_fires_once_at_first_suite_start() -> ::xtask::sandbox::TestResult<()> {
    let stdout = Cursor::new(
        concat!(
            "{\"type\":\"suite\",\"event\":\"started\",\"test_count\":1}\n",
            "{\"type\":\"suite\",\"event\":\"ok\",\"passed\":1,\"failed\":0,\"ignored\":0}\n",
            "{\"type\":\"suite\",\"event\":\"started\",\"test_count\":1}\n",
            "{\"type\":\"suite\",\"event\":\"ok\",\"passed\":1,\"failed\":0,\"ignored\":0}\n",
        )
        .as_bytes(),
    );
    let stderr = Cursor::new(Vec::<u8>::new());
    let mut observer = CountingPhaseObserver::default();

    let stats = TestReporter::new(false)
        .run(stdout, stderr, None, Some(&mut observer))
        .expect("multi-suite output should parse");

    assert_eq!(observer.suite_started_count, 1);
    assert_eq!(stats.passed, 2);
    assert_eq!(stats.total, 2);
    Ok(())
}

#[sinex_test]
async fn pre_suite_sigterm_reports_compile_signal_not_no_tests(
) -> ::xtask::sandbox::TestResult<()> {
    let stdout = Cursor::new(Vec::<u8>::new());
    let stderr = Cursor::new(
        concat!(
            "error: could not compile `sinexd` (lib test)\n",
            "process didn't exit successfully: `rustc --crate-name sinexd ...` ",
            "(signal: 15, SIGTERM: termination signal)\n",
        )
        .as_bytes(),
    );

    let error = TestReporter::new(false)
        .run(stdout, stderr, None, None)
        .expect_err("pre-suite rustc SIGTERM must be classified as compile/resource failure");
    let message = error.to_string();

    assert!(
        message.contains("terminated by signal before nextest discovered tests"),
        "signal compile failure was not classified: {message}"
    );
    assert!(
        !message.contains("No tests discovered"),
        "signal compile failure should not report generic no-tests guidance: {message}"
    );
    Ok(())
}

#[sinex_test]
async fn pre_suite_compile_error_reports_compile_not_no_tests(
) -> ::xtask::sandbox::TestResult<()> {
    let stdout = Cursor::new(Vec::<u8>::new());
    let stderr = Cursor::new(
        concat!(
            "error[E0425]: cannot find value `missing` in this scope\n",
            "error: could not compile `sinexd` (lib test)\n",
        )
        .as_bytes(),
    );

    let error = TestReporter::new(false)
        .run(stdout, stderr, None, None)
        .expect_err("pre-suite compile error must be classified as compile failure");
    let message = error.to_string();

    assert!(
        message.contains("test binary compilation failed before nextest discovered tests"),
        "compile failure was not classified: {message}"
    );
    assert!(
        !message.contains("No tests discovered"),
        "compile failure should not report generic no-tests guidance: {message}"
    );
    Ok(())
}
