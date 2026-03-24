use color_eyre::eyre::{Result, bail};
use console::{Emoji, style};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Deserialize;
use std::io::BufRead;
use std::thread;

use crate::history::HistoryDb;
use crate::history::TestStatus;

/// Strict types for Nextest JSON messages (libtest-json-plus format)
///
/// The format is: {"type": "suite"|"test", "event": "started"|"ok"|"failed", ...}
/// With libtest-json-plus, failed tests include a "stdout" field with captured output.
#[derive(Deserialize, Debug)]
struct RawMessage {
    #[serde(rename = "type")]
    msg_type: String,
    event: String,
    #[serde(rename = "test_count")]
    test_count: Option<usize>,
    name: Option<String>,
    #[serde(rename = "exec_time")]
    exec_time: Option<f64>,
    stdout: Option<String>,
    passed: Option<usize>,
    failed: Option<usize>,
    ignored: Option<usize>,
}

impl RawMessage {
    fn into_message(self) -> Message {
        match (self.msg_type.as_str(), self.event.as_str()) {
            ("suite", "started") => Message::SuiteStarted(SuiteStarted {
                test_count: self.test_count.unwrap_or(0),
            }),
            ("suite", "ok" | "failed") => Message::SuiteFinished(SuiteFinished {
                passed: self.passed.unwrap_or(0),
                failed: self.failed.unwrap_or(0),
                ignored: self.ignored.unwrap_or(0),
            }),
            ("test", "started") => Message::TestStarted(TestStarted {
                name: self.name.unwrap_or_default(),
            }),
            ("test", "ok") => Message::TestFinished(TestFinished {
                name: self.name.unwrap_or_default(),
                result: TestStatus::Pass.as_str().to_string(),
                exec_time: self.exec_time,
                output: self.stdout, // Store output for ALL tests (not just failures)
            }),
            ("test", "failed") => Message::TestFinished(TestFinished {
                name: self.name.unwrap_or_default(),
                result: TestStatus::Fail.as_str().to_string(),
                exec_time: self.exec_time,
                output: self.stdout, // Capture failure output from libtest-json-plus
            }),
            ("test", "ignored") => Message::TestFinished(TestFinished {
                name: self.name.unwrap_or_default(),
                result: TestStatus::Skip.as_str().to_string(),
                exec_time: self.exec_time,
                output: None,
            }),
            _ => Message::Other,
        }
    }
}

enum Message {
    SuiteStarted(SuiteStarted),
    SuiteFinished(SuiteFinished),
    TestStarted(TestStarted),
    TestFinished(TestFinished),
    Other,
}

struct SuiteStarted {
    test_count: usize,
}

struct SuiteFinished {
    passed: usize,
    failed: usize,
    ignored: usize,
}

struct TestStarted {
    name: String,
}

struct TestFinished {
    name: String,
    result: String,
    exec_time: Option<f64>,
    /// Captured stdout/stderr from test (available via libtest-json-plus for failures)
    output: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct TestStats {
    pub passed: usize,
    pub failed: usize,
    pub ignored: usize,
    pub total: usize,
}

pub struct TestReporter {
    pb: ProgressBar,
    human: bool,
    interactive: bool,
}

impl TestReporter {
    const LINE_PROGRESS_EVERY: usize = 100;

    #[must_use]
    pub fn new(human: bool) -> Self {
        let interactive = human && crate::output::is_tty();

        // Use hidden progress bar when not in human mode or when stdout isn't a TTY.
        // ProgressBar::hidden() is a complete no-op — zero CPU, no output.
        let pb = if interactive {
            let mp = MultiProgress::new();
            let pb = mp.add(ProgressBar::new(0)); // Will update total when known
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
                    .expect("valid progress bar template")
                    .progress_chars("#>-"),
            );
            pb
        } else {
            ProgressBar::hidden()
        };

        Self {
            pb,
            human,
            interactive,
        }
    }

    fn emit_line(&self, msg: &str) {
        if self.interactive {
            self.pb.println(msg);
        } else {
            eprintln!("{msg}");
        }
    }

    /// Process the test execution stream
    pub fn run<R1, R2>(
        &self,
        stdout: R1,
        stderr: R2,
        history: Option<(&HistoryDb, i64)>,
    ) -> Result<TestStats>
    where
        R1: BufRead,
        R2: BufRead + Send + 'static,
    {
        if self.human {
            println!("{}", style("\n🚀 Launching tests...").bold());
            // Progress bar won't tick until suite-started; indicate compilation phase
            if self.interactive {
                self.pb.set_message("Compiling test binaries...");
            } else {
                eprintln!("  ▸ Compiling test binaries...");
            }
        }

        let update_progress_snapshot =
            |total: Option<usize>,
             passed: usize,
             failed: usize,
             ignored: usize,
             last_name: Option<&str>| {
                if let Some((db, invocation_id)) = history {
                    let completed = (passed + failed + ignored) as i64;
                    let total_i = total.map(|t| t as i64);
                    let pct = if let Some(t) = total
                        && t > 0
                    {
                        Some(100.0 * (passed + failed + ignored) as f64 / t as f64)
                    } else {
                        None
                    };
                    let _ = db.write_progress(
                        invocation_id,
                        Some("tests"),
                        last_name,
                        pct,
                        Some(completed),
                        total_i,
                    );
                }
            };

        update_progress_snapshot(None, 0, 0, 0, None);

        // Spawn stderr handler
        let pb_stderr = self.pb.clone();
        let interactive_stderr = self.interactive;
        thread::spawn(move || {
            for line in stderr.lines().map_while(Result::ok) {
                // Print stderr (build output) above the progress bar
                if interactive_stderr {
                    pb_stderr.println(style(line).yellow().dim().to_string());
                } else {
                    eprintln!("{line}");
                }
            }
        });

        let mut stats = TestStats::default();
        let mut suite_totals = TestStats::default();
        let mut suite_started = false;
        let mut suite_finished = false;

        for line_res in stdout.lines() {
            let line = line_res?;

            // Try to parse JSON line and convert to our message type
            if let Ok(raw) = serde_json::from_str::<RawMessage>(&line) {
                let msg = raw.into_message();
                match msg {
                    Message::SuiteStarted(s) => {
                        suite_started = true;
                        // Each test binary emits suite-started, so accumulate total
                        let new_total = stats.total + s.test_count;
                        self.pb.set_length(new_total as u64);
                        stats.total = new_total;
                        update_progress_snapshot(
                            Some(stats.total),
                            stats.passed,
                            stats.failed,
                            stats.ignored,
                            None,
                        );
                    }
                    Message::SuiteFinished(s) => {
                        suite_finished = true;
                        suite_totals.passed += s.passed;
                        suite_totals.failed += s.failed;
                        suite_totals.ignored += s.ignored;
                        suite_totals.total += s.passed + s.failed + s.ignored;

                        // Each test binary emits suite-finished with its own counts.
                        // Cross-validate: if nextest reports failures we missed via
                        // streaming, log a warning so the discrepancy is visible.
                        if suite_totals.failed > stats.failed {
                            let msg = format!(
                                "  ⚠ Suite reports {} failed but streaming saw {} — using suite totals as authoritative",
                                suite_totals.failed, stats.failed
                            );
                            self.emit_line(&msg);
                        }
                        // Log suite summary for diagnostics
                        if self.human && (s.passed > 0 || s.ignored > 0) {
                            let msg = format!(
                                "  {} Suite complete: {} passed, {} failed, {} ignored",
                                Emoji("📊", "-"),
                                s.passed,
                                s.failed,
                                s.ignored
                            );
                            self.emit_line(&msg);
                        }
                    }
                    Message::TestStarted(t) => {
                        self.pb.set_message(format!("Running {}", t.name));
                        if !self.human {
                            eprintln!("  ▸ {}", t.name);
                        }
                    }
                    Message::TestFinished(t) => {
                        let duration = t.exec_time.unwrap_or(0.0);

                        // Update stats and UI
                        match t.result.as_str() {
                            "passed" => {
                                stats.passed += 1;
                                self.pb.inc(1);
                                // Show slow tests (>5s) even in normal mode
                                if duration > 5.0 {
                                    let msg = format!(
                                        "  {} {} ({:.1}s)",
                                        Emoji("⚡", "~"),
                                        t.name,
                                        duration
                                    );
                                    self.emit_line(&msg);
                                }
                            }
                            "failed" => {
                                stats.failed += 1;
                                self.pb.inc(1);
                                // Log failure immediately above bar
                                let msg =
                                    format!("  {} {} ({:.1}s)", Emoji("❌", "x"), t.name, duration);
                                self.emit_line(&msg);
                            }
                            "ignored" => {
                                stats.ignored += 1;
                                self.pb.inc(1);
                            }
                            _ => {
                                self.pb.inc(1);
                            }
                        }

                        if self.human && !self.interactive {
                            let completed = stats.passed + stats.failed + stats.ignored;
                            if completed == 1
                                || completed % Self::LINE_PROGRESS_EVERY == 0
                                || (stats.total > 0 && completed == stats.total)
                                || t.result == "failed"
                            {
                                let total_display = if stats.total > 0 {
                                    stats.total.to_string()
                                } else {
                                    "?".to_string()
                                };
                                eprintln!(
                                    "  ▸ Progress: {completed}/{total_display} (passed {}, failed {}, ignored {})",
                                    stats.passed, stats.failed, stats.ignored
                                );
                            }
                        }

                        update_progress_snapshot(
                            Some(stats.total),
                            stats.passed,
                            stats.failed,
                            stats.ignored,
                            Some(&t.name),
                        );

                        // Record to DB (including failure output if available)
                        if let Some((db, invocation_id)) = history {
                            let output = t.output.as_deref();

                            // Extract package from test name (e.g. "sinex_db::repo::test_name"
                            // → "sinex_db", or "tests/e2e.rs::test_name" → "tests")
                            let package = t.name.split("::").next().unwrap_or("unknown");

                            // Log but don't fail — test recording shouldn't interrupt tests
                            if let Err(e) = db.record_test_result(
                                invocation_id,
                                &t.name,
                                package,
                                &t.result,
                                duration,
                                output,
                                "nextest",
                            ) {
                                eprintln!("⚠️  Failed to record test result for {}: {e}", t.name);
                            }
                        }
                    }
                    Message::Other => {}
                }
            }
        }

        if self.interactive {
            self.pb.finish_with_message("done");
        }

        if suite_finished {
            if stats.passed != suite_totals.passed
                || stats.failed != suite_totals.failed
                || stats.ignored != suite_totals.ignored
            {
                let msg = format!(
                    "  ⚠ Adjusted streamed test counts to suite totals: passed {}→{}, failed {}→{}, ignored {}→{}",
                    stats.passed,
                    suite_totals.passed,
                    stats.failed,
                    suite_totals.failed,
                    stats.ignored,
                    suite_totals.ignored
                );
                self.emit_line(&msg);
            }

            stats.passed = suite_totals.passed;
            stats.failed = suite_totals.failed;
            stats.ignored = suite_totals.ignored;
            stats.total = stats.total.max(suite_totals.total);
        }

        // Detect test discovery failures: if no suite-started message was received,
        // something went wrong (invalid profile, compilation error, etc.)
        if !suite_started {
            bail!(
                "No tests discovered. Possible causes:\n\
                 - Invalid nextest profile (check .config/nextest.toml)\n\
                 - Compilation errors (check stderr output above)\n\
                 - Filter expression matched no tests"
            );
        }

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::TestReporter;
    use std::io::Cursor;

    #[test]
    fn suite_totals_backfill_stream_parse_gaps() {
        let stdout = Cursor::new(
            concat!(
                "{\"type\":\"suite\",\"event\":\"started\",\"test_count\":1}\n",
                "{\"type\":\"suite\",\"event\":\"failed\",\"passed\":0,\"failed\":1,\"ignored\":0}\n",
            )
            .as_bytes(),
        );
        let stderr = Cursor::new(Vec::<u8>::new());

        let stats = TestReporter::new(false)
            .run(stdout, stderr, None)
            .expect("suite-only failure output should still produce stats");

        assert_eq!(stats.failed, 1);
        assert_eq!(stats.passed, 0);
        assert_eq!(stats.ignored, 0);
        assert_eq!(stats.total, 1);
    }
}
