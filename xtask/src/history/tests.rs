//! Parse and store nextest JSON output.

use super::db::{HistoryDb, InvocationStatus, ResourceUsage, StageTiming};
use color_eyre::eyre::{Result, WrapErr};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Status of a test execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TestStatus {
    Pass,
    Fail,
    Skip,
    Flaky,
}

impl TestStatus {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Skip => "skip",
            Self::Flaky => "flaky",
        }
    }

    /// All string representations that map to this status in the DB.
    /// Used to build resilient SQL `IN (...)` clauses that match both canonical
    /// and legacy/alias forms stored by different code paths.
    #[must_use]
    pub fn db_aliases(&self) -> &'static [&'static str] {
        match self {
            Self::Pass => &["pass", "ok", "passed"],
            Self::Fail => &["fail", "failed"],
            Self::Skip => &["skip", "ignored"],
            Self::Flaky => &["flaky"],
        }
    }

    pub fn try_from_str(s: &str) -> Result<Self> {
        match s {
            "pass" | "ok" | "passed" => Ok(Self::Pass),
            "fail" | "failed" => Ok(Self::Fail),
            "skip" | "ignored" => Ok(Self::Skip),
            "flaky" => Ok(Self::Flaky),
            _ => Err(color_eyre::eyre::eyre!(
                "invalid test status in history DB: {s}"
            )),
        }
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "called from rusqlite with String"
)]
pub(crate) fn parse_stored_test_status(status_str: String) -> rusqlite::Result<TestStatus> {
    TestStatus::try_from_str(&status_str).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid test status in history DB: {status_str}"),
            )),
        )
    })
}

/// A single test result from nextest output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    pub test_name: String,
    pub package: String,
    pub status: TestStatus,
    pub duration_secs: Option<f64>,
    pub attempt: i32,
    pub output: Option<String>,
}

/// Nextest libtest-json event types we care about.
#[cfg(test)]
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum NextestEvent {
    #[serde(rename = "suite")]
    Suite {
        event: String,
        test_count: Option<u32>,
        #[serde(default)]
        nextest: Option<NextestMeta>,
    },
    #[serde(rename = "test")]
    Test {
        event: String,
        name: String,
        #[serde(default)]
        exec_time: Option<f64>,
        #[serde(default)]
        stdout: Option<String>,
        #[serde(default)]
        stderr: Option<String>,
    },
}

#[cfg(test)]
#[derive(Debug, Deserialize, Default)]
struct NextestMeta {
    #[serde(rename = "crate")]
    crate_name: Option<String>,
}

/// Parse nextest libtest-json output and extract test results.
#[cfg(test)]
pub(super) fn parse_nextest_output(output: &str) -> Vec<TestResult> {
    let mut results = Vec::new();
    let mut current_package = String::from("unknown");

    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() || !line.starts_with('{') {
            continue;
        }

        let event: Result<NextestEvent, _> = serde_json::from_str(line);
        match event {
            Ok(NextestEvent::Suite {
                event,
                test_count,
                nextest,
            }) => {
                // Validate deserialized Suite fields
                assert!(!event.is_empty(), "suite event should not be empty");
                let _ = test_count; // Field is optional, just ensure it deserialized

                if let Some(meta) = nextest
                    && let Some(name) = meta.crate_name
                {
                    current_package = name;
                }
            }
            Ok(NextestEvent::Test {
                event,
                name,
                exec_time,
                stdout,
                stderr,
            }) => {
                // Only process completed tests (ok, failed, ignored)
                let status = match event.as_str() {
                    "ok" => TestStatus::Pass,
                    "failed" => TestStatus::Fail,
                    "ignored" => TestStatus::Skip,
                    "started" => continue, // Skip start events
                    _ => continue,
                };

                // Parse test name: "crate::binary$module::path::test_name"
                let (package, test_name) = parse_test_name(&name, &current_package);

                // Combine stdout/stderr for output
                let output = match (stdout, stderr) {
                    (Some(out), Some(err)) if !out.is_empty() || !err.is_empty() => {
                        Some(format!("stdout:\n{out}\nstderr:\n{err}"))
                    }
                    (Some(out), _) if !out.is_empty() => Some(out),
                    (_, Some(err)) if !err.is_empty() => Some(err),
                    _ => None,
                };

                results.push(TestResult {
                    test_name,
                    package,
                    status,
                    duration_secs: exec_time,
                    attempt: 1,
                    output,
                });
            }
            Err(_) => {} // Skip unparseable lines
        }
    }

    results
}

/// Parse nextest test name format to extract package and test name.
/// Format: "`crate::binary$module::path::test_name`"
#[cfg(test)]
fn parse_test_name(full_name: &str, default_package: &str) -> (String, String) {
    // Example: "xtask::xtask$bench::stats::tests::test_mean"
    // Package: "xtask", Test: "bench::stats::tests::test_mean"
    if let Some(dollar_pos) = full_name.find('$') {
        let package = full_name[..dollar_pos]
            .rsplit("::")
            .next()
            .unwrap_or(default_package)
            .to_string();
        let test_name = full_name[dollar_pos + 1..].to_string();
        (package, test_name)
    } else {
        // Fallback: use the whole name
        (default_package.to_string(), full_name.to_string())
    }
}

impl HistoryDb {
    /// Store test results for an invocation.
    pub fn store_test_results(&self, invocation_id: i64, results: &[TestResult]) -> Result<usize> {
        let mut stored = 0;
        for result in results {
            self.conn.execute(
                r"
                INSERT OR REPLACE INTO test_results
                    (invocation_id, test_name, package, status, duration_secs, attempt, output)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                ",
                rusqlite::params![
                    invocation_id,
                    result.test_name,
                    result.package,
                    result.status.as_str(),
                    result.duration_secs,
                    result.attempt,
                    result.output,
                ],
            )?;
            stored += 1;
        }
        Ok(stored)
    }

    /// Get test results for an invocation.
    pub fn get_test_results(&self, invocation_id: i64) -> Result<Vec<TestResult>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT test_name, package, status, duration_secs, attempt, output
            FROM test_results
            WHERE invocation_id = ?1
            ORDER BY package, test_name
            ",
        )?;

        let rows = stmt.query_map([invocation_id], |row| {
            let status_str: String = row.get(2)?;
            Ok(TestResult {
                test_name: row.get(0)?,
                package: row.get(1)?,
                status: parse_stored_test_status(status_str)?,
                duration_secs: row.get(3)?,
                attempt: row.get(4)?,
                output: row.get(5)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect test results")
    }

    /// Get flaky tests (tests that failed then passed on retry).
    pub fn get_flaky_tests(&self, limit: usize) -> Result<Vec<(String, String, i64)>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT DISTINCT t1.test_name, t1.package, t1.invocation_id
            FROM test_results t1
            WHERE t1.status = 'fail'
              AND EXISTS (
                SELECT 1 FROM test_results t2
                WHERE t2.invocation_id = t1.invocation_id
                  AND t2.test_name = t1.test_name
                  AND t2.attempt > t1.attempt
                  AND t2.status = 'pass'
              )
            ORDER BY t1.invocation_id DESC
            LIMIT ?1
            ",
        )?;

        let rows = stmt.query_map([limit], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect flaky tests")
    }

    /// Count flaky tests (tests that failed then passed on retry) up to `limit`.
    pub fn get_flaky_test_count(&self, limit: usize) -> Result<usize> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT t1.test_name, t1.package, t1.invocation_id
                FROM test_results t1
                WHERE t1.status = 'fail'
                  AND EXISTS (
                    SELECT 1 FROM test_results t2
                    WHERE t2.invocation_id = t1.invocation_id
                      AND t2.test_name = t1.test_name
                      AND t2.attempt > t1.attempt
                      AND t2.status = 'pass'
                  )
                ORDER BY t1.invocation_id DESC
                LIMIT ?1
            )
            ",
        )?;

        stmt.query_row([limit], |row| row.get(0))
            .context("failed to count flaky tests")
    }

    /// Get frequently failing tests.
    pub fn get_failing_tests(
        &self,
        invocation_id: i64,
        limit: usize,
    ) -> Result<Vec<(String, String, f64)>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT t.test_name, t.package, COALESCE(t.duration_secs, 0) as duration
            FROM test_results t
            WHERE t.invocation_id = ?1
              AND t.status = 'fail'
            ORDER BY t.test_name
            LIMIT ?2
            ",
        )?;

        let rows = stmt.query_map(rusqlite::params![invocation_id, limit as i64], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect failing tests")
    }

    /// Get failing tests from the most recent test run, with captured output.
    ///
    /// Includes failure_message and failure_type (populated from JUnit XML
    /// `<failure>` elements during metadata back-fill).
    pub fn get_failing_tests_with_output(
        &self,
        invocation_id: i64,
        limit: usize,
    ) -> Result<Vec<FailingTest>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT t.test_name, t.package, COALESCE(t.duration_secs, 0) as duration,
                   t.output, t.failure_message, t.failure_type, t.nats_context
            FROM test_results t
            WHERE t.invocation_id = ?1
              AND t.status = 'fail'
            ORDER BY t.test_name
            LIMIT ?2
            ",
        )?;

        let rows = stmt.query_map(rusqlite::params![invocation_id, limit as i64], |row| {
            Ok(FailingTest {
                test_name: row.get(0)?,
                package: row.get(1)?,
                duration_secs: row.get(2)?,
                output: row.get(3)?,
                failure_message: row.get(4)?,
                failure_type: row.get(5)?,
                nats_context: row.get(6)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect failing tests with output")
    }

    /// Get slowest tests by average duration.
    ///
    /// Only counts passing tests — failed/timed-out tests would inflate durations
    /// with timeout ceilings rather than reflecting real execution time.
    pub fn get_slowest_tests(&self, limit: usize) -> Result<Vec<HistoricalSlowTest>> {
        self.get_slowest_tests_filtered(limit, None, 1)
    }

    /// Get slowest tests by average duration with optional time/run filters.
    ///
    /// Only counts passing tests — failed/timed-out tests would inflate durations
    /// with timeout ceilings rather than reflecting real execution time.
    pub fn get_slowest_tests_filtered(
        &self,
        limit: usize,
        since: Option<&str>,
        min_runs: usize,
    ) -> Result<Vec<HistoricalSlowTest>> {
        let min_runs = i64::try_from(min_runs).unwrap_or(i64::MAX);
        let mut stmt = self.conn.prepare(
            r"
            SELECT test_name, package, AVG(test_results.duration_secs) as avg_duration, COUNT(*) as runs
            FROM test_results
            JOIN invocations ON invocations.id = test_results.invocation_id
            WHERE test_results.duration_secs IS NOT NULL
              AND test_results.status = 'pass'
              AND (?1 IS NULL OR invocations.started_at >= ?1)
            GROUP BY test_name, package
            HAVING COUNT(*) >= ?2
            ORDER BY avg_duration DESC
            LIMIT ?3
            ",
        )?;

        let rows = stmt.query_map(rusqlite::params![since, min_runs, limit as i64], |row| {
            Ok(HistoricalSlowTest {
                test_name: row.get(0)?,
                package: row.get(1)?,
                avg_duration_secs: row.get(2)?,
                passing_runs: row.get(3)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect slowest tests")
    }

    /// Get slowest tests by each test's latest passing result.
    ///
    /// This answers "what is currently slow?" after optimization work has
    /// changed a test's cost profile, while `get_slowest_tests_filtered`
    /// remains the historical-average view.
    pub fn get_slowest_latest_tests_filtered(
        &self,
        limit: usize,
        since: Option<&str>,
    ) -> Result<Vec<HistoricalSlowTest>> {
        let mut stmt = self.conn.prepare(
            r"
            WITH latest AS (
                SELECT
                    test_results.test_name,
                    test_results.package,
                    test_results.duration_secs,
                    ROW_NUMBER() OVER (
                        PARTITION BY test_results.test_name, test_results.package
                        ORDER BY invocations.started_at DESC, test_results.id DESC
                    ) as rn
                FROM test_results
                JOIN invocations ON invocations.id = test_results.invocation_id
                WHERE test_results.duration_secs IS NOT NULL
                  AND test_results.status = 'pass'
                  AND (?1 IS NULL OR invocations.started_at >= ?1)
            )
            SELECT test_name, package, duration_secs
            FROM latest
            WHERE rn = 1
            ORDER BY duration_secs DESC
            LIMIT ?2
            ",
        )?;

        let rows = stmt.query_map(rusqlite::params![since, limit as i64], |row| {
            Ok(HistoricalSlowTest {
                test_name: row.get(0)?,
                package: row.get(1)?,
                avg_duration_secs: row.get(2)?,
                passing_runs: 1,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect slowest latest tests")
    }

    /// Get the slowest concrete test results from one invocation.
    pub fn get_slowest_tests_for_invocation(
        &self,
        invocation_id: i64,
        limit: usize,
    ) -> Result<Vec<RunSlowTest>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT test_name, package, status, COALESCE(duration_secs, 0) as duration
            FROM test_results
            WHERE invocation_id = ?1
            ORDER BY duration DESC, package, test_name
            LIMIT ?2
            ",
        )?;

        let rows = stmt.query_map(rusqlite::params![invocation_id, limit as i64], |row| {
            Ok(RunSlowTest {
                test_name: row.get(0)?,
                package: row.get(1)?,
                status: row.get(2)?,
                duration_secs: row.get(3)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect slowest tests for invocation")
    }

    /// Get tests that are getting slower over time.
    ///
    /// Compares average duration of recent runs vs older runs within the window.
    /// Returns tests where `(recent_avg - older_avg) / older_avg * 100 > threshold_pct`.
    pub fn get_tests_getting_slower(
        &self,
        window: usize,
        threshold_pct: f64,
        limit: usize,
    ) -> Result<Vec<TestTrend>> {
        // We need to split the window in half: compare first half vs second half
        let half_window = window / 2;
        if half_window == 0 {
            return Ok(vec![]);
        }

        let mut stmt = self.conn.prepare(
            r"
            WITH ranked_results AS (
                SELECT
                    t.test_name,
                    t.package,
                    t.duration_secs,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.test_name, t.package
                        ORDER BY i.started_at DESC
                    ) as rn
                FROM test_results t
                JOIN invocations i ON t.invocation_id = i.id
                WHERE t.duration_secs IS NOT NULL
                  AND i.command = 'test'
                  AND t.status = 'pass'
            ),
            older_half AS (
                SELECT test_name, package, AVG(duration_secs) as avg_duration, COUNT(*) as cnt
                FROM ranked_results
                WHERE rn > ?1 AND rn <= ?2
                GROUP BY test_name, package
            ),
            recent_half AS (
                SELECT test_name, package, AVG(duration_secs) as avg_duration, COUNT(*) as cnt
                FROM ranked_results
                WHERE rn <= ?1
                GROUP BY test_name, package
            )
            SELECT
                r.test_name,
                r.package,
                o.avg_duration as older_avg,
                r.avg_duration as recent_avg,
                ((r.avg_duration - o.avg_duration) / o.avg_duration * 100) as pct_change,
                o.cnt + r.cnt as sample_count
            FROM recent_half r
            JOIN older_half o ON r.test_name = o.test_name AND r.package = o.package
            WHERE o.avg_duration > 0
              AND ((r.avg_duration - o.avg_duration) / o.avg_duration * 100) > ?3
            ORDER BY pct_change DESC
            LIMIT ?4
            ",
        )?;

        let rows = stmt.query_map(
            rusqlite::params![half_window, window, threshold_pct, limit],
            |row| {
                Ok(TestTrend {
                    test_name: row.get(0)?,
                    package: row.get(1)?,
                    older_avg_secs: row.get(2)?,
                    recent_avg_secs: row.get(3)?,
                    pct_change: row.get(4)?,
                    sample_count: row.get(5)?,
                })
            },
        )?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to get tests getting slower")
    }

    /// Get runtime trend for tests matching a pattern.
    ///
    /// Returns duration history per test, newest first.
    pub fn get_test_trends(
        &self,
        pattern: Option<&str>,
        package: Option<&str>,
        runs: usize,
    ) -> Result<Vec<TestTrendDetail>> {
        let pattern_like = pattern.map(|p| format!("%{p}%"));
        let package_like = package.map(|p| format!("%{p}%"));

        let mut stmt = self.conn.prepare(
            r"
            SELECT
                t.test_name,
                t.package,
                t.duration_secs,
                i.started_at
            FROM test_results t
            JOIN invocations i ON t.invocation_id = i.id
            WHERE t.duration_secs IS NOT NULL
              AND i.command = 'test'
              AND (?1 IS NULL OR t.test_name LIKE ?1)
              AND (?2 IS NULL OR t.package LIKE ?2)
            ORDER BY t.test_name, t.package, i.started_at DESC
            ",
        )?;

        let all_rows: Vec<(String, String, f64, String)> = stmt
            .query_map(rusqlite::params![pattern_like, package_like], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("failed to read stored test trend rows")?;

        // Group by test name and take first `runs` entries per test
        let mut grouped: std::collections::HashMap<(String, String), Vec<(f64, String)>> =
            std::collections::HashMap::new();

        for (test_name, package, duration, started_at) in all_rows {
            let key = (test_name, package);
            let entry = grouped.entry(key).or_default();
            if entry.len() < runs {
                entry.push((duration, started_at));
            }
        }

        let mut results: Vec<TestTrendDetail> = grouped
            .into_iter()
            .map(|((test_name, package), durations)| {
                let avg = if durations.is_empty() {
                    0.0
                } else {
                    durations.iter().map(|(d, _)| d).sum::<f64>() / durations.len() as f64
                };
                TestTrendDetail {
                    test_name,
                    package,
                    durations: durations.iter().map(|(d, _)| *d).collect(),
                    timestamps: durations.into_iter().map(|(_, t)| t).collect(),
                    avg_duration_secs: avg,
                }
            })
            .collect();

        // Sort by test name
        results.sort_by(|a, b| a.test_name.cmp(&b.test_name));

        Ok(results)
    }

    /// Estimate total runtime based on historical data.
    ///
    /// Returns estimated runtime and confidence level.
    pub fn estimate_runtime(&self) -> Result<RuntimeEstimate> {
        // Get average duration for all tests from recent runs
        let mut stmt = self.conn.prepare(
            r"
            SELECT
                t.package,
                AVG(t.duration_secs) as avg_duration,
                COUNT(DISTINCT t.test_name) as test_count,
                COUNT(*) as sample_count
            FROM test_results t
            JOIN invocations i ON t.invocation_id = i.id
            WHERE t.duration_secs IS NOT NULL
              AND i.command = 'test'
              AND i.started_at > datetime('now', '-7 days')
              AND t.status = 'pass'
            GROUP BY t.package
            ",
        )?;

        let mut total_secs = 0.0;
        let mut total_tests = 0usize;
        let mut total_samples = 0usize;
        let mut breakdown = Vec::new();

        let rows = stmt.query_map([], |row| {
            let package: String = row.get(0)?;
            let avg_duration: f64 = row.get(1)?;
            let test_count: usize = row.get(2)?;
            let sample_count: usize = row.get(3)?;
            Ok((package, avg_duration, test_count, sample_count))
        })?;

        for row in rows {
            let (package, avg_duration, test_count, sample_count) = row?;
            let package_total = avg_duration * test_count as f64;
            total_secs += package_total;
            total_tests += test_count;
            total_samples += sample_count;
            breakdown.push((package, package_total));
        }

        let confidence = if total_samples < 5 {
            Confidence::Low
        } else if total_samples < 20 {
            Confidence::Medium
        } else {
            Confidence::High
        };

        // Sort breakdown by time descending
        breakdown.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        Ok(RuntimeEstimate {
            estimated_secs: total_secs,
            test_count: total_tests,
            confidence,
            breakdown,
        })
    }
}

/// Comprehensive test suite analysis from the most recent run.
#[derive(Debug, Clone, Serialize)]
pub struct TestSuiteAnalysis {
    /// Duration distribution buckets
    pub duration_buckets: Vec<DurationBucket>,
    /// Slowest concrete tests in this invocation, ordered by observed duration
    pub slowest_tests: Vec<RunSlowTest>,
    /// Tests that appear to have timed out (failed with duration near a timeout ceiling)
    pub probable_timeouts: Vec<ProbableTimeout>,
    /// Failure summary grouped by package
    pub failure_summary: Vec<PackageFailureSummary>,
    /// Host-pressure context for timing-sensitive failures in this run.
    pub host_pressure: Option<HostPressureFailureClassification>,
    /// Invocation elapsed time compared with summed test-body duration.
    pub run_overhead: Option<TestRunOverhead>,
    /// Recorded pipeline stages for the invocation, grouped by stage name.
    pub stage_breakdown: Vec<TestRunStageBreakdown>,
    /// Invocation wall time not covered by recorded pipeline stages.
    pub unstaged_invocation_secs: Option<f64>,
    /// Total counts
    pub total_passed: usize,
    pub total_failed: usize,
    pub total_ignored: usize,
    pub total_duration_secs: f64,
    /// Invocation metadata
    pub invocation_id: i64,
    pub started_at: String,
}

/// Test-run overhead that is outside recorded test-body execution time.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TestRunOverhead {
    pub invocation_duration_secs: f64,
    pub test_body_duration_secs: f64,
    pub non_test_overhead_secs: f64,
    pub test_body_ratio: f64,
    pub classification: &'static str,
}

/// Recorded stage contribution for a single test invocation.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TestRunStageBreakdown {
    pub stage_name: String,
    pub runs: usize,
    pub total_duration_secs: f64,
    pub avg_duration_secs: f64,
    pub max_duration_secs: f64,
    pub success: bool,
}

/// Host-pressure context for interpreting timing-sensitive test failures.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct HostPressureFailureClassification {
    pub level: String,
    pub timing_failures_may_be_invalidated: bool,
    pub reason: String,
    pub host_io_pressure_full_avg10_max: Option<f64>,
    pub host_memory_pressure_full_avg10_max: Option<f64>,
    pub host_cpu_pressure_some_avg10_max: Option<f64>,
}

fn classify_test_run_overhead(
    invocation_duration_secs: Option<f64>,
    test_body_duration_secs: f64,
) -> Option<TestRunOverhead> {
    let invocation_duration_secs = invocation_duration_secs?;
    if invocation_duration_secs <= 0.0 || !invocation_duration_secs.is_finite() {
        return None;
    }

    let raw_test_body_ratio = test_body_duration_secs / invocation_duration_secs;
    let non_test_overhead_secs = (invocation_duration_secs - test_body_duration_secs).max(0.0);
    let test_body_ratio = raw_test_body_ratio.clamp(0.0, 1.0);
    let classification = if raw_test_body_ratio > 1.10 {
        "parallel_test_bodies"
    } else if non_test_overhead_secs < 1.0 {
        "negligible"
    } else if test_body_ratio < 0.25 {
        "runner_setup_dominated"
    } else if test_body_ratio < 0.75 {
        "mixed"
    } else {
        "test_body_dominated"
    };

    Some(TestRunOverhead {
        invocation_duration_secs,
        test_body_duration_secs,
        non_test_overhead_secs,
        test_body_ratio,
        classification,
    })
}

fn summarize_stage_breakdown(stages: &[StageTiming]) -> Vec<TestRunStageBreakdown> {
    let mut by_stage: std::collections::BTreeMap<String, (usize, f64, f64, bool)> =
        std::collections::BTreeMap::new();
    for stage in stages {
        let entry = by_stage
            .entry(stage.stage_name.clone())
            .or_insert((0, 0.0, 0.0, true));
        entry.0 += 1;
        entry.1 += stage.duration_secs;
        entry.2 = entry.2.max(stage.duration_secs);
        entry.3 &= stage.success;
    }

    let mut breakdown: Vec<TestRunStageBreakdown> = by_stage
        .into_iter()
        .map(
            |(stage_name, (runs, total_duration_secs, max_duration_secs, success))| {
                TestRunStageBreakdown {
                    stage_name,
                    runs,
                    total_duration_secs,
                    avg_duration_secs: if runs == 0 {
                        0.0
                    } else {
                        total_duration_secs / runs as f64
                    },
                    max_duration_secs,
                    success,
                }
            },
        )
        .collect();
    breakdown.sort_by(|left, right| {
        right
            .total_duration_secs
            .partial_cmp(&left.total_duration_secs)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.stage_name.cmp(&right.stage_name))
    });
    breakdown
}

fn unstaged_invocation_secs(
    invocation_duration_secs: Option<f64>,
    stage_breakdown: &[TestRunStageBreakdown],
) -> Option<f64> {
    let invocation_duration_secs = invocation_duration_secs?;
    let stage_secs: f64 = stage_breakdown
        .iter()
        .map(|stage| stage.total_duration_secs)
        .sum();
    Some((invocation_duration_secs - stage_secs).max(0.0))
}

fn invocation_duration_for_analysis(
    started_at: &str,
    stored_duration_secs: Option<f64>,
) -> Option<f64> {
    if stored_duration_secs.is_some() {
        return stored_duration_secs;
    }

    let started_at = OffsetDateTime::parse(started_at, &Rfc3339).ok()?;
    let elapsed = OffsetDateTime::now_utc() - started_at;
    let seconds = elapsed.as_seconds_f64();
    (seconds.is_finite() && seconds > 0.0).then_some(seconds)
}

fn is_probable_timeout_duration(duration_secs: f64) -> bool {
    const TIMEOUT_CEILINGS: [f64; 7] = [10.0, 30.0, 60.0, 90.0, 120.0, 180.0, 300.0];
    TIMEOUT_CEILINGS
        .iter()
        .any(|ceiling| (duration_secs - ceiling).abs() < 2.0)
}

fn timing_sensitive_failure_text(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("did not reach ready")
        || lower.contains("ready state")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("deadline")
}

fn classify_host_pressure_for_failures(
    usage: Option<&ResourceUsage>,
    has_timing_sensitive_failure: bool,
) -> Option<HostPressureFailureClassification> {
    let usage = usage?;
    let io_full = usage.host_io_pressure_full_avg10_max.unwrap_or(0.0);
    let memory_full = usage.host_memory_pressure_full_avg10_max.unwrap_or(0.0);
    let level = if io_full >= crate::resources::thresholds::PSI_IO_FULL_SEVERE
        || memory_full >= crate::resources::thresholds::PSI_MEMORY_FULL_SEVERE
    {
        "severe"
    } else if io_full >= crate::resources::thresholds::PSI_IO_FULL_WARN
        || memory_full >= crate::resources::thresholds::PSI_MEMORY_FULL_WARN
    {
        "elevated"
    } else {
        "clear"
    };

    if level == "clear" {
        return None;
    }

    Some(HostPressureFailureClassification {
        level: level.to_string(),
        timing_failures_may_be_invalidated: has_timing_sensitive_failure,
        reason: if has_timing_sensitive_failure {
            format!(
                "timing-sensitive failures occurred while host pressure was {level}; rerun under low contention before treating them as product regressions"
            )
        } else {
            format!(
                "host pressure was {level}, but the stored failures do not look timing-sensitive"
            )
        },
        host_io_pressure_full_avg10_max: usage.host_io_pressure_full_avg10_max,
        host_memory_pressure_full_avg10_max: usage.host_memory_pressure_full_avg10_max,
        host_cpu_pressure_some_avg10_max: usage.host_cpu_pressure_some_avg10_max,
    })
}

/// A duration distribution bucket.
#[derive(Debug, Clone, Serialize)]
pub struct DurationBucket {
    pub label: String,
    pub min_secs: f64,
    pub max_secs: f64,
    pub count: usize,
    pub tests: Vec<String>,
}

/// A test that probably timed out rather than doing real work.
#[derive(Debug, Clone, Serialize)]
pub struct ProbableTimeout {
    pub test_name: String,
    pub package: String,
    pub duration_secs: f64,
    pub status: String,
}

/// Failure summary for a package.
#[derive(Debug, Clone, Serialize)]
pub struct PackageFailureSummary {
    pub package: String,
    pub failed_count: usize,
    pub passed_count: usize,
    pub failure_rate_pct: f64,
    pub failed_tests: Vec<String>,
}

/// A historically slow test aggregated across passing runs.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct HistoricalSlowTest {
    pub test_name: String,
    pub package: String,
    pub avg_duration_secs: f64,
    pub passing_runs: i64,
}

/// A slow test result from one concrete invocation.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RunSlowTest {
    pub test_name: String,
    pub package: String,
    pub status: String,
    pub duration_secs: f64,
}

/// A concrete test-run invocation resolved from stored history.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResolvedTestRun {
    pub invocation_id: i64,
    pub started_at: String,
    pub job_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestRunSelector {
    Latest,
    Previous,
    LatestSuccess,
    LatestFailure,
    InvocationId(i64),
    BackgroundJobId(i64),
}

impl HistoryDb {
    fn parse_test_run_selector(selector: &str) -> Result<TestRunSelector> {
        if selector == "latest" {
            return Ok(TestRunSelector::Latest);
        }
        if selector == "previous" {
            return Ok(TestRunSelector::Previous);
        }
        if selector == "latest-success" {
            return Ok(TestRunSelector::LatestSuccess);
        }
        if selector == "latest-failure" {
            return Ok(TestRunSelector::LatestFailure);
        }

        let (kind, raw_id) = if let Some(value) = selector.strip_prefix("job:") {
            ("job", value)
        } else if let Some(value) = selector.strip_prefix("background-job:") {
            ("job", value)
        } else if let Some(value) = selector.strip_prefix("inv:") {
            ("invocation", value)
        } else if let Some(value) = selector.strip_prefix("invocation:") {
            ("invocation", value)
        } else {
            ("invocation", selector)
        };

        let id = raw_id.parse::<i64>().map_err(|_| {
            color_eyre::eyre::eyre!(
                "invalid test run selector: '{selector}' (expected 'latest', an invocation ID, 'inv:<id>', or 'job:<id>')"
            )
        })?;

        Ok(match kind {
            "job" => TestRunSelector::BackgroundJobId(id),
            _ => TestRunSelector::InvocationId(id),
        })
    }

    fn resolve_recent_test_run(
        &self,
        status_filter: Option<InvocationStatus>,
        offset: usize,
    ) -> Result<Option<ResolvedTestRun>> {
        let status_clause = match status_filter {
            Some(InvocationStatus::Success) => "AND i.status = 'success'",
            Some(InvocationStatus::Failed) => "AND i.status = 'failed'",
            _ => "AND i.status IN ('success', 'failed')",
        };
        let sql = format!(
            r"
            SELECT i.id, i.started_at
            FROM invocations i
            WHERE i.command = 'test'
              {status_clause}
              AND EXISTS (SELECT 1 FROM test_results tr WHERE tr.invocation_id = i.id)
            ORDER BY i.started_at DESC
            LIMIT 1 OFFSET ?1
            "
        );

        self.conn
            .query_row(&sql, [offset as i64], |row| {
                Ok(ResolvedTestRun {
                    invocation_id: row.get(0)?,
                    started_at: row.get(1)?,
                    job_id: None,
                })
            })
            .optional()
            .map_err(Into::into)
    }

    /// Recent completed test invocations that have stored test result rows.
    pub fn recent_test_runs(&self, limit: usize) -> Result<Vec<ResolvedTestRun>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut stmt = self.conn.prepare(
            r"
            SELECT i.id, i.started_at
            FROM invocations i
            WHERE i.command = 'test'
              AND i.status IN ('success', 'failed')
              AND EXISTS (SELECT 1 FROM test_results tr WHERE tr.invocation_id = i.id)
            ORDER BY i.started_at DESC, i.id DESC
            LIMIT ?1
            ",
        )?;

        stmt.query_map([limit], |row| {
            Ok(ResolvedTestRun {
                invocation_id: row.get(0)?,
                started_at: row.get(1)?,
                job_id: None,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("failed to collect recent test invocations with stored results")
    }

    fn resolve_test_run_invocation(&self, invocation_id: i64) -> Result<Option<ResolvedTestRun>> {
        self.conn
            .query_row(
                r"
                SELECT i.id, i.started_at
                FROM invocations i
                WHERE i.id = ?1
                  AND i.command = 'test'
                  AND EXISTS (SELECT 1 FROM test_results tr WHERE tr.invocation_id = i.id)
                LIMIT 1
                ",
                [invocation_id],
                |row| {
                    Ok(ResolvedTestRun {
                        invocation_id: row.get(0)?,
                        started_at: row.get(1)?,
                        job_id: None,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    fn resolve_test_run_background_job(&self, job_id: i64) -> Result<Option<ResolvedTestRun>> {
        self.conn
            .query_row(
                r"
                SELECT i.id, i.started_at
                FROM background_jobs bj
                JOIN invocations i ON i.id = bj.invocation_id
                WHERE bj.id = ?1
                  AND i.command = 'test'
                  AND EXISTS (SELECT 1 FROM test_results tr WHERE tr.invocation_id = i.id)
                LIMIT 1
                ",
                [job_id],
                |row| {
                    Ok(ResolvedTestRun {
                        invocation_id: row.get(0)?,
                        started_at: row.get(1)?,
                        job_id: Some(job_id),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    /// Resolve a test-run selector to a concrete invocation with stored test results.
    ///
    /// `None` and `"latest"` both select the most recent completed test invocation
    /// that actually recorded test results. Explicit selectors also accept
    /// `job:<id>`/`background-job:<id>`. Plain numeric selectors prefer
    /// invocation IDs, but fall back to matching background job IDs when the
    /// numeric invocation has no stored test results.
    pub fn resolve_test_run(&self, selector: Option<&str>) -> Result<Option<ResolvedTestRun>> {
        match selector {
            None | Some("latest") => self.resolve_recent_test_run(None, 0),
            Some(raw_selector) => match Self::parse_test_run_selector(raw_selector)? {
                TestRunSelector::Latest => self.resolve_recent_test_run(None, 0),
                TestRunSelector::Previous => self.resolve_recent_test_run(None, 1),
                TestRunSelector::LatestSuccess => {
                    self.resolve_recent_test_run(Some(InvocationStatus::Success), 0)
                }
                TestRunSelector::LatestFailure => {
                    self.resolve_recent_test_run(Some(InvocationStatus::Failed), 0)
                }
                TestRunSelector::BackgroundJobId(job_id) => self
                    .resolve_test_run_background_job(job_id)?
                    .ok_or_else(|| {
                        color_eyre::eyre::eyre!(
                            "Background job #{job_id} does not map to a completed test run with stored results"
                        )
                    })
                    .map(Some),
                TestRunSelector::InvocationId(invocation_id) => {
                    if let Some(resolved) = self.resolve_test_run_invocation(invocation_id)? {
                        return Ok(Some(resolved));
                    }

                    if raw_selector.chars().all(|ch| ch.is_ascii_digit())
                        && let Some(resolved) =
                            self.resolve_test_run_background_job(invocation_id)?
                    {
                        return Ok(Some(resolved));
                    }

                    Err(color_eyre::eyre::eyre!(
                        "Invocation #{invocation_id} has no stored test results"
                    ))
                }
            }
        }
    }

    /// Comprehensive analysis of the most recent test run.
    ///
    /// Produces bucketed duration distributions, probable timeout detection,
    /// and per-package failure summaries.
    pub fn analyze_test_run(&self, invocation_id: i64) -> Result<Option<TestSuiteAnalysis>> {
        let (started_at, invocation_duration_secs) = match self.conn.query_row(
            r"SELECT started_at, duration_secs FROM invocations WHERE id = ?1 AND command = 'test'",
            [invocation_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<f64>>(1)?)),
        ) {
            Ok(row) => row,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let invocation_duration_secs =
            invocation_duration_for_analysis(&started_at, invocation_duration_secs);

        // Get all test results for this invocation
        let mut stmt = self.conn.prepare(
            r"
            SELECT test_name,
                   package,
                   status,
                   COALESCE(duration_secs, 0) as duration,
                   output
            FROM test_results
            WHERE invocation_id = ?1
            ORDER BY duration DESC
            ",
        )?;

        struct Row {
            test_name: String,
            package: String,
            status: String,
            duration: f64,
            output: Option<String>,
        }

        let rows: Vec<Row> = stmt
            .query_map([invocation_id], |row| {
                Ok(Row {
                    test_name: row.get(0)?,
                    package: row.get(1)?,
                    status: row.get(2)?,
                    duration: row.get(3)?,
                    output: row.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .wrap_err_with(|| {
                format!("failed to read stored test rows for invocation {invocation_id}")
            })?;

        if rows.is_empty() {
            return Ok(None);
        }

        // Counts
        let total_passed = rows
            .iter()
            .filter(|r| matches!(r.status.as_str(), "passed" | "pass" | "ok"))
            .count();
        let total_failed = rows
            .iter()
            .filter(|r| matches!(r.status.as_str(), "failed" | "fail"))
            .count();
        let total_ignored = rows
            .iter()
            .filter(|r| matches!(r.status.as_str(), "ignored" | "skip"))
            .count();
        let total_duration_secs: f64 = rows.iter().map(|r| r.duration).sum();
        let slowest_tests: Vec<RunSlowTest> = rows
            .iter()
            .take(10)
            .map(|row| RunSlowTest {
                test_name: row.test_name.clone(),
                package: row.package.clone(),
                status: row.status.clone(),
                duration_secs: row.duration,
            })
            .collect();

        // Duration buckets
        let bucket_defs = [
            ("< 1s", 0.0, 1.0),
            ("1-5s", 1.0, 5.0),
            ("5-10s", 5.0, 10.0),
            ("10-30s", 10.0, 30.0),
            ("30-60s", 30.0, 60.0),
            ("60-120s", 60.0, 120.0),
            ("> 120s", 120.0, f64::MAX),
        ];

        let duration_buckets: Vec<DurationBucket> = bucket_defs
            .iter()
            .map(|(label, min, max)| {
                let tests: Vec<String> = rows
                    .iter()
                    .filter(|r| r.duration >= *min && r.duration < *max)
                    .map(|r| format!("{}::{} ({:.1}s)", r.package, r.test_name, r.duration))
                    .collect();
                DurationBucket {
                    label: label.to_string(),
                    min_secs: *min,
                    max_secs: if *max == f64::MAX { 999.0 } else { *max },
                    count: tests.len(),
                    tests,
                }
            })
            .collect();

        // Probable timeouts: failed tests with duration near common timeout ceilings
        // (10s, 30s, 60s, 90s, 120s, 180s, 300s)
        let probable_timeouts: Vec<ProbableTimeout> = rows
            .iter()
            .filter(|r| {
                matches!(r.status.as_str(), "failed" | "fail")
                    && is_probable_timeout_duration(r.duration)
            })
            .map(|r| ProbableTimeout {
                test_name: r.test_name.clone(),
                package: r.package.clone(),
                duration_secs: r.duration,
                status: r.status.clone(),
            })
            .collect();

        let resource_usage = self.get_resource_usage_for_invocation(invocation_id)?;
        let has_timing_sensitive_failure = rows.iter().any(|row| {
            matches!(row.status.as_str(), "failed" | "fail")
                && (is_probable_timeout_duration(row.duration)
                    || timing_sensitive_failure_text(&row.test_name)
                    || row
                        .output
                        .as_deref()
                        .is_some_and(timing_sensitive_failure_text))
        });
        let host_pressure = classify_host_pressure_for_failures(
            resource_usage.as_ref(),
            has_timing_sensitive_failure,
        );
        let run_overhead =
            classify_test_run_overhead(invocation_duration_secs, total_duration_secs);
        let stage_breakdown = summarize_stage_breakdown(
            &self
                .get_stage_timings_for_invocation(invocation_id)
                .wrap_err_with(|| {
                    format!("failed to load stage timings for test invocation {invocation_id}")
                })?,
        );
        let unstaged_invocation_secs =
            unstaged_invocation_secs(invocation_duration_secs, &stage_breakdown);

        // Per-package failure summary
        let mut pkg_map: std::collections::HashMap<String, (usize, usize, Vec<String>)> =
            std::collections::HashMap::new();
        for r in &rows {
            let entry = pkg_map.entry(r.package.clone()).or_default();
            if matches!(r.status.as_str(), "failed" | "fail") {
                entry.0 += 1;
                entry.2.push(r.test_name.clone());
            } else if matches!(r.status.as_str(), "passed" | "pass" | "ok") {
                entry.1 += 1;
            }
        }

        let mut failure_summary: Vec<PackageFailureSummary> = pkg_map
            .into_iter()
            .filter(|(_, (failed, _, _))| *failed > 0)
            .map(|(package, (failed, passed, tests))| {
                let total = failed + passed;
                PackageFailureSummary {
                    package,
                    failed_count: failed,
                    passed_count: passed,
                    failure_rate_pct: if total > 0 {
                        (failed as f64 / total as f64) * 100.0
                    } else {
                        0.0
                    },
                    failed_tests: tests,
                }
            })
            .collect();
        failure_summary.sort_by_key(|a| std::cmp::Reverse(a.failed_count));

        Ok(Some(TestSuiteAnalysis {
            duration_buckets,
            slowest_tests,
            probable_timeouts,
            failure_summary,
            host_pressure,
            run_overhead,
            stage_breakdown,
            unstaged_invocation_secs,
            total_passed,
            total_failed,
            total_ignored,
            total_duration_secs,
            invocation_id,
            started_at,
        }))
    }

    /// Comprehensive analysis of the most recent completed test run with stored results.
    pub fn analyze_last_run(&self) -> Result<Option<TestSuiteAnalysis>> {
        let Some(invocation) = self.resolve_test_run(None)? else {
            return Ok(None);
        };
        self.analyze_test_run(invocation.invocation_id)
    }

    /// Get test output for a specific test from the most recent run.
    pub fn get_test_output(
        &self,
        invocation_id: i64,
        test_pattern: &str,
    ) -> Result<Vec<TestOutputEntry>> {
        let pattern = format!("%{test_pattern}%");
        let mut stmt = self.conn.prepare(
            r"
            SELECT t.test_name, t.package, t.status, COALESCE(t.duration_secs, 0), t.output
            FROM test_results t
            WHERE t.invocation_id = ?1
              AND t.test_name LIKE ?2
            ORDER BY t.test_name
            ",
        )?;

        let rows = stmt.query_map(rusqlite::params![invocation_id, &pattern], |row| {
            Ok(TestOutputEntry {
                test_name: row.get(0)?,
                package: row.get(1)?,
                status: row.get(2)?,
                duration_secs: row.get(3)?,
                output: row.get(4)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect test output")
    }

    /// Get infrastructure timing summary from the most recent test run.
    ///
    /// Returns aggregated slot acquisition and cleanup timing from the metadata
    /// columns populated by slog event parsing. Returns `None` if no metadata
    /// columns exist or no data is available.
    pub fn get_infra_timing_summary(
        &self,
        invocation_id: i64,
    ) -> Result<Option<InfraTimingSummary>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT slot_name, slot_wait_ms, cleanup_ms
            FROM test_results t
            WHERE t.invocation_id = ?1
              AND t.slot_name IS NOT NULL
            ",
        )?;

        let rows: Vec<(String, i64, Option<i64>)> = stmt
            .query_map([invocation_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .wrap_err("failed to read stored infrastructure timing rows")?;

        if rows.is_empty() {
            return Ok(None);
        }

        let tests_with_metadata = rows.len();
        let total_wait: i64 = rows.iter().map(|(_, w, _)| w).sum();
        let max_slot_wait_ms = rows.iter().map(|(_, w, _)| *w).max().unwrap_or(0);
        let avg_slot_wait_ms = total_wait as f64 / tests_with_metadata as f64;

        let dirty_slots: Vec<&(String, i64, Option<i64>)> =
            rows.iter().filter(|(_, _, c)| c.is_some()).collect();
        let dirty_slot_count = dirty_slots.len();
        let avg_cleanup_ms = if dirty_slot_count > 0 {
            dirty_slots
                .iter()
                .map(|(_, _, c)| c.unwrap_or(0))
                .sum::<i64>() as f64
                / dirty_slot_count as f64
        } else {
            0.0
        };

        // Per-slot usage counts
        let mut slot_counts: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for (name, _, _) in &rows {
            *slot_counts.entry(name.clone()).or_default() += 1;
        }
        let mut slot_usage: Vec<(String, i64)> = slot_counts.into_iter().collect();
        slot_usage.sort_by_key(|(_, count)| std::cmp::Reverse(*count));

        Ok(Some(InfraTimingSummary {
            tests_with_metadata,
            avg_slot_wait_ms,
            max_slot_wait_ms,
            avg_cleanup_ms,
            dirty_slot_count,
            slot_usage,
        }))
    }
}

/// Test output entry from the most recent run.
#[derive(Debug, Clone, Serialize)]
pub struct TestOutputEntry {
    pub test_name: String,
    pub package: String,
    pub status: String,
    pub duration_secs: f64,
    pub output: Option<String>,
}

/// Infrastructure timing summary for test runs.
#[derive(Debug, Clone, Serialize)]
pub struct InfraTimingSummary {
    /// Total tests with slot metadata
    pub tests_with_metadata: usize,
    /// Average slot acquisition time in milliseconds
    pub avg_slot_wait_ms: f64,
    /// Maximum slot acquisition time in milliseconds
    pub max_slot_wait_ms: i64,
    /// Average cleanup time in milliseconds (dirty slots only)
    pub avg_cleanup_ms: f64,
    /// Number of tests that hit dirty slots
    pub dirty_slot_count: usize,
    /// Per-slot usage counts (slot_name -> test count)
    pub slot_usage: Vec<(String, i64)>,
}

/// A failing test from the most recent test run, with captured output.
#[derive(Debug, Clone, Serialize)]
pub struct FailingTest {
    pub test_name: String,
    pub package: String,
    pub duration_secs: f64,
    pub output: Option<String>,
    /// Extracted failure message from JUnit `<failure message="...">` (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_message: Option<String>,
    /// Extracted failure type from JUnit `<failure type="...">` (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_type: Option<String>,
    /// D8: NATS consumer snapshot JSON captured at failure time (if test used NATS sandbox)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nats_context: Option<String>,
}

/// Per-package test statistics from the most recent run (G7 --by-package).
#[derive(Debug, Clone, Serialize)]
pub struct PackageTestStats {
    pub package: String,
    pub total: i64,
    pub passed: i64,
    pub failed: i64,
    pub avg_duration_secs: f64,
    pub flaky_count: i64,
}

/// A test newly failing in recent runs that previously passed (G7 --regression).
#[derive(Debug, Clone, Serialize)]
pub struct RegressionTest {
    pub test_name: String,
    pub package: String,
    pub duration_secs: f64,
}

/// Test that is getting slower over time.
#[derive(Debug, Clone, Serialize)]
pub struct TestTrend {
    pub test_name: String,
    pub package: String,
    pub older_avg_secs: f64,
    pub recent_avg_secs: f64,
    pub pct_change: f64,
    pub sample_count: i64,
}

/// Detailed runtime trend for a single test.
#[derive(Debug, Clone, Serialize)]
pub struct TestTrendDetail {
    pub test_name: String,
    pub package: String,
    pub durations: Vec<f64>,
    pub timestamps: Vec<String>,
    pub avg_duration_secs: f64,
}

/// Confidence level for runtime estimates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    Low,
    Medium,
    High,
}

impl std::fmt::Display for Confidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Confidence::Low => write!(f, "low"),
            Confidence::Medium => write!(f, "medium"),
            Confidence::High => write!(f, "high"),
        }
    }
}

/// Estimated runtime for upcoming test run.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeEstimate {
    pub estimated_secs: f64,
    pub test_count: usize,
    pub confidence: Confidence,
    /// Package name -> estimated seconds
    pub breakdown: Vec<(String, f64)>,
}

// ─── G7: Test Analytics Extensions ───────────────────────────────────────────

impl HistoryDb {
    /// Full-text search across stored test output in the most recent invocation (G7 --grep).
    pub fn search_test_output(
        &self,
        invocation_id: i64,
        text: &str,
        limit: usize,
    ) -> Result<Vec<TestOutputEntry>> {
        let pattern = format!("%{text}%");
        let mut stmt = self.conn.prepare(
            r"
            SELECT t.test_name, t.package, t.status, COALESCE(t.duration_secs, 0.0), t.output
            FROM test_results t
            WHERE t.invocation_id = ?1
              AND t.output LIKE ?2
            ORDER BY t.test_name
            LIMIT ?3
            ",
        )?;
        let rows = stmt.query_map(
            rusqlite::params![invocation_id, &pattern, limit as i64],
            |row| {
                Ok(TestOutputEntry {
                    test_name: row.get(0)?,
                    package: row.get(1)?,
                    status: row.get(2)?,
                    duration_secs: row.get(3)?,
                    output: row.get(4)?,
                })
            },
        )?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Per-package pass rate, count, avg duration, and flaky count (G7 --by-package).
    pub fn get_tests_by_package(&self, invocation_id: i64) -> Result<Vec<PackageTestStats>> {
        let mut stmt = self.conn.prepare(
            r"
            WITH
            latest_tests AS (
                SELECT t.test_name, t.package, t.status, COALESCE(t.duration_secs, 0.0) as dur
                FROM test_results t
                WHERE t.invocation_id = ?1
            ),
            flaky_counts AS (
                SELECT t1.package, COUNT(DISTINCT t1.test_name) as flaky_count
                FROM test_results t1
                JOIN test_results t2 ON t1.test_name = t2.test_name
                    AND t1.package = t2.package
                    AND t1.invocation_id = t2.invocation_id
                    AND t1.status = 'pass' AND t2.status = 'fail'
                GROUP BY t1.package
            )
            SELECT lt.package,
                   COUNT(*) as total,
                   SUM(CASE WHEN lt.status = 'pass' THEN 1 ELSE 0 END) as passed,
                   SUM(CASE WHEN lt.status = 'fail' THEN 1 ELSE 0 END) as failed,
                   AVG(lt.dur) as avg_duration,
                   COALESCE(fc.flaky_count, 0) as flaky
            FROM latest_tests lt
            LEFT JOIN flaky_counts fc ON lt.package = fc.package
            GROUP BY lt.package
            ORDER BY failed DESC, total DESC
            ",
        )?;
        let rows = stmt.query_map([invocation_id], |row| {
            Ok(PackageTestStats {
                package: row.get(0)?,
                total: row.get(1)?,
                passed: row.get(2)?,
                failed: row.get(3)?,
                avg_duration_secs: row.get(4)?,
                flaky_count: row.get(5)?,
            })
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// P95 duration per test over recent invocations (G7 --duration-p95).
    ///
    /// Loads historical durations per test (last 30 invocations), sorts each test's
    /// durations, and computes the 95th percentile. Returns top `limit` tests by P95.
    pub fn get_test_duration_p95(&self, limit: usize) -> Result<Vec<(String, String, f64)>> {
        // Load all (test_name, package, duration) from recent passing runs
        let mut stmt = self.conn.prepare(
            r"
            SELECT t.test_name, t.package, COALESCE(t.duration_secs, 0.0)
            FROM test_results t
            INNER JOIN invocations i ON t.invocation_id = i.id
            WHERE i.command = 'test' AND t.status = 'pass'
            AND i.id IN (
                SELECT id FROM invocations
                WHERE command = 'test' ORDER BY started_at DESC LIMIT 30
            )
            ORDER BY t.test_name, t.package
            ",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
            ))
        })?;

        // Group by (test_name, package), compute P95 in Rust
        let mut by_test: std::collections::BTreeMap<(String, String), Vec<f64>> =
            std::collections::BTreeMap::new();
        for row in rows {
            let (name, pkg, dur) = row?;
            by_test.entry((name, pkg)).or_default().push(dur);
        }

        let mut p95_results: Vec<(String, String, f64)> = by_test
            .into_iter()
            .filter_map(|((name, pkg), mut durations)| {
                if durations.is_empty() {
                    return None;
                }
                durations.sort_by(f64::total_cmp);
                let idx = ((durations.len() as f64 * 0.95) as usize).min(durations.len() - 1);
                Some((name, pkg, durations[idx]))
            })
            .collect();
        p95_results.sort_by(|a, b| b.2.total_cmp(&a.2));
        p95_results.truncate(limit);
        Ok(p95_results)
    }

    /// Tests newly failing in the last N runs that previously passed (G7 --regression).
    pub fn get_tests_regressing(&self, recent_runs: usize) -> Result<Vec<RegressionTest>> {
        // Find tests failing in the most recent invocation that passed in any of the
        // N invocations before it.
        let limit_i64 = recent_runs as i64;
        let mut stmt = self.conn.prepare(
            r"
            WITH recent_inv AS (
                SELECT id FROM invocations
                WHERE command = 'test' AND status IN ('success', 'failed')
                ORDER BY started_at DESC LIMIT ?1
            ),
            latest_inv AS (
                SELECT id FROM invocations
                WHERE command = 'test' AND status IN ('success', 'failed')
                ORDER BY started_at DESC LIMIT 1
            ),
            currently_failing AS (
                SELECT DISTINCT t.test_name, t.package, COALESCE(t.duration_secs, 0.0) as dur
                FROM test_results t
                INNER JOIN latest_inv ON t.invocation_id = latest_inv.id
                WHERE t.status = 'fail'
            ),
            previously_passing AS (
                SELECT DISTINCT t.test_name, t.package
                FROM test_results t
                INNER JOIN recent_inv ON t.invocation_id = recent_inv.id
                LEFT JOIN latest_inv ON t.invocation_id = latest_inv.id
                WHERE latest_inv.id IS NULL AND t.status = 'pass'
            )
            SELECT cf.test_name, cf.package, cf.dur
            FROM currently_failing cf
            INNER JOIN previously_passing pp
                ON cf.test_name = pp.test_name AND cf.package = pp.package
            ORDER BY cf.package, cf.test_name
            ",
        )?;
        let rows = stmt.query_map(rusqlite::params![limit_i64], |row| {
            Ok(RegressionTest {
                test_name: row.get(0)?,
                package: row.get(1)?,
                duration_secs: row.get(2)?,
            })
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Test pass/fail/skip counts for a specific invocation (for G5 --with-tests).
    pub fn get_test_counts_for_invocation(&self, invocation_id: i64) -> Result<(i64, i64, i64)> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT
                COALESCE(SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status = 'skip' THEN 1 ELSE 0 END), 0)
            FROM test_results
            WHERE invocation_id = ?1
            ",
        )?;
        let (passed, failed, skipped) =
            stmt.query_row(rusqlite::params![invocation_id], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
        Ok((passed, failed, skipped))
    }
}

#[cfg(test)]
#[allow(clippy::module_inception)]
#[path = "tests_test.rs"]
mod tests;
