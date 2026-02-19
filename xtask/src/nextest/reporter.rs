use color_eyre::eyre::{bail, Result};
use console::{style, Emoji};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Deserialize;
use std::io::BufRead;
use std::thread;

use crate::history::HistoryDb;

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
                result: "passed".to_string(),
                exec_time: self.exec_time,
                output: self.stdout, // Store output for ALL tests (not just failures)
            }),
            ("test", "failed") => Message::TestFinished(TestFinished {
                name: self.name.unwrap_or_default(),
                result: "failed".to_string(),
                exec_time: self.exec_time,
                output: self.stdout, // Capture failure output from libtest-json-plus
            }),
            ("test", "ignored") => Message::TestFinished(TestFinished {
                name: self.name.unwrap_or_default(),
                result: "ignored".to_string(),
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
    // mp: MultiProgress, // Removed unused field
    pb: ProgressBar,
    human: bool,
}

impl TestReporter {
    #[must_use]
    pub fn new(human: bool) -> Self {
        // Use hidden progress bar when not in human mode or when stdout isn't a TTY.
        // ProgressBar::hidden() is a complete no-op — zero CPU, no output.
        let pb = if human && crate::output::is_tty() {
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

        Self { pb, human }
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
            self.pb.set_message("Compiling test binaries...");
        }

        // Spawn stderr handler
        let pb_stderr = self.pb.clone();
        thread::spawn(move || {
            for line in stderr.lines().map_while(Result::ok) {
                // Print stderr (build output) above the progress bar
                pb_stderr.println(style(line).yellow().dim().to_string());
            }
        });

        let mut stats = TestStats::default();
        let mut suite_started = false;

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
                    }
                    Message::SuiteFinished(s) => {
                        // Each test binary emits suite-finished with its own counts.
                        // Cross-validate: if nextest reports failures we missed via
                        // streaming, log a warning so the discrepancy is visible.
                        if s.failed > 0 && stats.failed == 0 {
                            let msg = format!(
                                "  ⚠ Suite reports {} failed but streaming saw 0 — possible parse gap",
                                s.failed
                            );
                            self.pb.println(&msg);
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
                            self.pb.println(&msg);
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
                                    self.pb.println(&msg);
                                    if !self.human {
                                        eprintln!("{msg}");
                                    }
                                }
                            }
                            "failed" => {
                                stats.failed += 1;
                                self.pb.inc(1);
                                // Log failure immediately above bar
                                let msg =
                                    format!("  {} {} ({:.1}s)", Emoji("❌", "x"), t.name, duration);
                                self.pb.println(&msg);
                                if !self.human {
                                    eprintln!("{msg}");
                                }
                            }
                            "ignored" => {
                                stats.ignored += 1;
                                self.pb.inc(1);
                            }
                            _ => {
                                self.pb.inc(1);
                            }
                        }

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
                            ) {
                                eprintln!("⚠️  Failed to record test result for {}: {e}", t.name);
                            }
                        }
                    }
                    Message::Other => {}
                }
            }
        }

        if self.human {
            self.pb.finish_with_message("done");
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
