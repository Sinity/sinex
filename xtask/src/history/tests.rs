//! Parse and store nextest JSON output.

use super::db::HistoryDb;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

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
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Skip => "skip",
            Self::Flaky => "flaky",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "pass" | "ok" => Self::Pass,
            "fail" | "failed" => Self::Fail,
            "skip" | "ignored" => Self::Skip,
            "flaky" => Self::Flaky,
            _ => Self::Fail,
        }
    }
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

                if let Some(meta) = nextest {
                    if let Some(name) = meta.crate_name {
                        current_package = name;
                    }
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
                status: TestStatus::from_str(&status_str),
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
            WHERE t1.status = 'failed'
              AND EXISTS (
                SELECT 1 FROM test_results t2
                WHERE t2.invocation_id = t1.invocation_id
                  AND t2.test_name = t1.test_name
                  AND t2.attempt > t1.attempt
                  AND t2.status = 'passed'
              )
            ORDER BY t1.invocation_id DESC
            LIMIT ?1
            ",
        )?;

        let rows = stmt.query_map([limit], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect flaky tests")
    }

    /// Get frequently failing tests.
    pub fn get_failing_tests(&self, limit: usize) -> Result<Vec<(String, String, f64)>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT t.test_name, t.package, COALESCE(t.duration_secs, 0) as duration
            FROM test_results t
            INNER JOIN (
                SELECT MAX(i.id) as max_inv
                FROM invocations i
                WHERE i.command = 'test'
                  AND EXISTS (SELECT 1 FROM test_results tr WHERE tr.invocation_id = i.id)
            ) latest ON t.invocation_id = latest.max_inv
            WHERE t.status = 'failed'
            ORDER BY t.test_name
            LIMIT ?1
            ",
        )?;

        let rows = stmt.query_map([limit], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect failing tests")
    }

    /// Get failing tests from the most recent test run, with captured output.
    pub fn get_failing_tests_with_output(&self, limit: usize) -> Result<Vec<FailingTest>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT t.test_name, t.package, COALESCE(t.duration_secs, 0) as duration, t.output
            FROM test_results t
            INNER JOIN (
                SELECT MAX(i.id) as max_inv
                FROM invocations i
                WHERE i.command = 'test'
                  AND EXISTS (SELECT 1 FROM test_results tr WHERE tr.invocation_id = i.id)
            ) latest ON t.invocation_id = latest.max_inv
            WHERE t.status = 'failed'
            ORDER BY t.test_name
            LIMIT ?1
            ",
        )?;

        let rows = stmt.query_map([limit], |row| {
            Ok(FailingTest {
                test_name: row.get(0)?,
                package: row.get(1)?,
                duration_secs: row.get(2)?,
                output: row.get(3)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect failing tests with output")
    }

    /// Get slowest tests by average duration.
    ///
    /// Only counts passing tests — failed/timed-out tests would inflate durations
    /// with timeout ceilings rather than reflecting real execution time.
    pub fn get_slowest_tests(&self, limit: usize) -> Result<Vec<(String, String, f64, i64)>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT test_name, package, AVG(duration_secs) as avg_duration, COUNT(*) as runs
            FROM test_results
            WHERE duration_secs IS NOT NULL
              AND status IN ('passed', 'pass', 'ok')
            GROUP BY test_name, package
            ORDER BY avg_duration DESC
            LIMIT ?1
            ",
        )?;

        let rows = stmt.query_map([limit], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect slowest tests")
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
                  AND t.status IN ('passed', 'pass', 'ok')
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
            .filter_map(std::result::Result::ok)
            .collect();

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
              AND t.status IN ('passed', 'pass', 'ok')
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
    /// Tests that appear to have timed out (failed with duration near a timeout ceiling)
    pub probable_timeouts: Vec<ProbableTimeout>,
    /// Failure summary grouped by package
    pub failure_summary: Vec<PackageFailureSummary>,
    /// Total counts
    pub total_passed: usize,
    pub total_failed: usize,
    pub total_ignored: usize,
    pub total_duration_secs: f64,
    /// Invocation metadata
    pub invocation_id: i64,
    pub started_at: String,
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

impl HistoryDb {
    /// Comprehensive analysis of the most recent test run.
    ///
    /// Produces bucketed duration distributions, probable timeout detection,
    /// and per-package failure summaries.
    pub fn analyze_last_run(&self) -> Result<Option<TestSuiteAnalysis>> {
        // Get the latest test invocation
        let inv = self.conn.query_row(
            r"
            SELECT i.id, i.started_at
            FROM invocations i
            WHERE i.command = 'test'
              AND EXISTS (SELECT 1 FROM test_results tr WHERE tr.invocation_id = i.id)
            ORDER BY i.started_at DESC
            LIMIT 1
            ",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        );

        let (inv_id, started_at) = match inv {
            Ok(pair) => pair,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        // Get all test results for this invocation
        let mut stmt = self.conn.prepare(
            r"
            SELECT test_name, package, status, COALESCE(duration_secs, 0) as duration
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
        }

        let rows: Vec<Row> = stmt
            .query_map([inv_id], |row| {
                Ok(Row {
                    test_name: row.get(0)?,
                    package: row.get(1)?,
                    status: row.get(2)?,
                    duration: row.get(3)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

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
        let timeout_ceilings = [10.0, 30.0, 60.0, 90.0, 120.0, 180.0, 300.0];
        let probable_timeouts: Vec<ProbableTimeout> = rows
            .iter()
            .filter(|r| {
                matches!(r.status.as_str(), "failed" | "fail")
                    && timeout_ceilings
                        .iter()
                        .any(|c| (r.duration - c).abs() < 2.0)
            })
            .map(|r| ProbableTimeout {
                test_name: r.test_name.clone(),
                package: r.package.clone(),
                duration_secs: r.duration,
                status: r.status.clone(),
            })
            .collect();

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
        failure_summary.sort_by(|a, b| b.failed_count.cmp(&a.failed_count));

        Ok(Some(TestSuiteAnalysis {
            duration_buckets,
            probable_timeouts,
            failure_summary,
            total_passed,
            total_failed,
            total_ignored,
            total_duration_secs,
            invocation_id: inv_id,
            started_at,
        }))
    }

    /// Get test output for a specific test from the most recent run.
    pub fn get_test_output(&self, test_pattern: &str) -> Result<Vec<TestOutputEntry>> {
        let pattern = format!("%{test_pattern}%");
        let mut stmt = self.conn.prepare(
            r"
            SELECT t.test_name, t.package, t.status, COALESCE(t.duration_secs, 0), t.output
            FROM test_results t
            INNER JOIN (
                SELECT MAX(i.id) as max_inv
                FROM invocations i
                WHERE i.command = 'test'
                  AND EXISTS (SELECT 1 FROM test_results tr WHERE tr.invocation_id = i.id)
            ) latest ON t.invocation_id = latest.max_inv
            WHERE t.test_name LIKE ?1
            ORDER BY t.test_name
            ",
        )?;

        let rows = stmt.query_map([&pattern], |row| {
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

/// A failing test from the most recent test run, with captured output.
#[derive(Debug, Clone, Serialize)]
pub struct FailingTest {
    pub test_name: String,
    pub package: String,
    pub duration_secs: f64,
    pub output: Option<String>,
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

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_nextest_output() {
        let output = r#"
{"type":"suite","event":"started","test_count":2,"nextest":{"crate":"mypackage"}}
{"type":"test","event":"started","name":"mypackage::mypackage$module::test_one"}
{"type":"test","event":"ok","name":"mypackage::mypackage$module::test_one","exec_time":0.001}
{"type":"test","event":"started","name":"mypackage::mypackage$module::test_two"}
{"type":"test","event":"failed","name":"mypackage::mypackage$module::test_two","exec_time":0.5,"stdout":"test output"}
{"type":"suite","event":"finished","passed":1,"failed":1}
"#;

        let results = parse_nextest_output(output);
        assert_eq!(results.len(), 2);

        assert_eq!(results[0].package, "mypackage");
        assert_eq!(results[0].test_name, "module::test_one");
        assert_eq!(results[0].status, TestStatus::Pass);
        assert!(results[0].duration_secs.unwrap() < 0.01);

        assert_eq!(results[1].package, "mypackage");
        assert_eq!(results[1].test_name, "module::test_two");
        assert_eq!(results[1].status, TestStatus::Fail);
        assert!(results[1].output.is_some());
    }

    #[test]
    fn test_parse_test_name() {
        let (pkg, name) = parse_test_name("xtask::xtask$bench::stats::tests::test_mean", "default");
        assert_eq!(pkg, "xtask");
        assert_eq!(name, "bench::stats::tests::test_mean");

        let (pkg, name) = parse_test_name("no_dollar_sign", "fallback");
        assert_eq!(pkg, "fallback");
        assert_eq!(name, "no_dollar_sign");
    }
}
