use anyhow::Result;
use console::{style, Emoji};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Deserialize;
use std::io::BufRead;
use std::thread;

use crate::history::HistoryDb;

/// Strict types for Nextest JSON messages (libtest-json format)
#[derive(Deserialize)]
#[serde(tag = "type")]
enum Message {
    #[serde(rename = "suite-started")]
    SuiteStarted(SuiteStarted),
    #[serde(rename = "test-event")]
    TestEvent(TestEvent),
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct SuiteStarted {
    #[serde(rename = "test-count")]
    test_count: usize,
}

#[derive(Deserialize)]
pub struct TestEvent {
    #[serde(rename = "test-event")]
    pub kind: String, // "test-started", "test-finished"
    pub name: String,
    pub package: Option<String>,
    pub result: Option<String>, // "passed", "failed", "ignored"
    #[serde(rename = "exec-time")]
    pub exec_time: Option<f64>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
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
        let mp = MultiProgress::new();
        let pb = mp.add(ProgressBar::new(0)); // Will update total when known

        // Match style from original test.rs
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
                .unwrap()
                .progress_chars("#>-"),
        );

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

        for line_res in stdout.lines() {
            let line = line_res?;

            // Try to parse parsing JSON line
            if let Ok(msg) = serde_json::from_str::<Message>(&line) {
                match msg {
                    Message::SuiteStarted(s) => {
                        self.pb.set_length(s.test_count as u64);
                        stats.total = s.test_count;
                    }
                    Message::TestEvent(event) => {
                        self.handle_event(event, &mut stats, history)?;
                    }
                    Message::Other => {}
                }
            }
        }

        if self.human {
            self.pb.finish_and_clear();
        }

        Ok(stats)
    }

    fn handle_event(
        &self,
        event: TestEvent,
        stats: &mut TestStats,
        history: Option<(&HistoryDb, i64)>,
    ) -> Result<()> {
        match event.kind.as_str() {
            "test-started" => {
                self.pb.set_message(format!("Running {}", event.name));
            }
            "test-finished" => {
                let result = event.result.as_deref().unwrap_or("unknown");
                let duration = event.exec_time.unwrap_or(0.0);

                // Update stats and UI
                match result {
                    "passed" => {
                        stats.passed += 1;
                        self.pb.inc(1);
                    }
                    "failed" => {
                        stats.failed += 1;
                        self.pb.inc(1);
                        // Log failure immediately above bar
                        let msg = format!("{} {} ({:.3}s)", Emoji("❌", "x"), event.name, duration);
                        self.pb.println(msg);
                    }
                    "ignored" => {
                        stats.ignored += 1;
                        self.pb.inc(1);
                        // Ignore implies successful skip
                    }
                    _ => {
                        self.pb.inc(1);
                    }
                }

                // Record to DB
                if let Some((db, invocation_id)) = history {
                    let mut output = String::new();
                    if let Some(s) = &event.stdout {
                        if !s.is_empty() {
                            output.push_str("STDOUT:\n");
                            output.push_str(s);
                            output.push('\n');
                        }
                    }
                    if let Some(s) = &event.stderr {
                        if !s.is_empty() {
                            output.push_str("STDERR:\n");
                            output.push_str(s);
                            output.push('\n');
                        }
                    }

                    let output_opt = if output.is_empty() {
                        None
                    } else {
                        Some(output.as_str())
                    };

                    // We ignore errors here to not interrupt testing flow if DB fails
                    let _ = db.record_test_result(
                        invocation_id,
                        &event.name,
                        event.package.as_deref().unwrap_or("unknown"),
                        result,
                        duration,
                        output_opt,
                    );
                }
            }
            _ => {}
        }
        Ok(())
    }
}
