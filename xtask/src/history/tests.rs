//! Parse and store nextest JSON output.

use super::db::HistoryDb;
use color_eyre::eyre::{Result, WrapErr};
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
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Skip => "skip",
            Self::Flaky => "flaky",
        }
    }

    pub fn try_from_str(s: &str) -> Result<Self> {
        match s {
            "pass" | "ok" => Ok(Self::Pass),
            "fail" | "failed" => Ok(Self::Fail),
            "skip" | "ignored" => Ok(Self::Skip),
            "flaky" => Ok(Self::Flaky),
            _ => Err(color_eyre::eyre::eyre!(
                "invalid test status in history DB: {s}"
            )),
        }
    }
}

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
            WHERE t.status = 'fail'
            ORDER BY t.test_name
            LIMIT ?1
            ",
        )?;

        let rows = stmt.query_map([limit], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect failing tests")
    }

    /// Get failing tests from the most recent test run, with captured output.
    ///
    /// Includes failure_message and failure_type (populated from JUnit XML
    /// `<failure>` elements during metadata back-fill).
    pub fn get_failing_tests_with_output(&self, limit: usize) -> Result<Vec<FailingTest>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT t.test_name, t.package, COALESCE(t.duration_secs, 0) as duration,
                   t.output, t.failure_message, t.failure_type, t.nats_context
            FROM test_results t
            INNER JOIN (
                SELECT MAX(i.id) as max_inv
                FROM invocations i
                WHERE i.command = 'test'
                  AND EXISTS (SELECT 1 FROM test_results tr WHERE tr.invocation_id = i.id)
            ) latest ON t.invocation_id = latest.max_inv
            WHERE t.status = 'fail'
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
    pub fn get_slowest_tests(&self, limit: usize) -> Result<Vec<(String, String, f64, i64)>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT test_name, package, AVG(duration_secs) as avg_duration, COUNT(*) as runs
            FROM test_results
            WHERE duration_secs IS NOT NULL
              AND status = 'pass'
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
            .filter_map(Result::ok)
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
        failure_summary.sort_by_key(|a| std::cmp::Reverse(a.failed_count));

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

    /// Get infrastructure timing summary from the most recent test run.
    ///
    /// Returns aggregated slot acquisition and cleanup timing from the metadata
    /// columns populated by slog event parsing. Returns `None` if no metadata
    /// columns exist or no data is available.
    pub fn get_infra_timing_summary(&self) -> Result<Option<InfraTimingSummary>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT slot_name, slot_wait_ms, cleanup_ms
            FROM test_results t
            INNER JOIN (
                SELECT MAX(i.id) as max_inv
                FROM invocations i
                WHERE i.command = 'test'
                  AND EXISTS (SELECT 1 FROM test_results tr WHERE tr.invocation_id = i.id)
            ) latest ON t.invocation_id = latest.max_inv
            WHERE t.slot_name IS NOT NULL
            ",
        )?;

        let rows: Vec<(String, i64, Option<i64>)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .filter_map(Result::ok)
            .collect();

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
    pub fn search_test_output(&self, text: &str, limit: usize) -> Result<Vec<TestOutputEntry>> {
        let pattern = format!("%{text}%");
        let mut stmt = self.conn.prepare(
            r"
            SELECT t.test_name, t.package, t.status, COALESCE(t.duration_secs, 0.0), t.output
            FROM test_results t
            INNER JOIN (
                SELECT id FROM invocations
                WHERE command = 'test' AND status IN ('success', 'failed')
                ORDER BY started_at DESC LIMIT 1
            ) latest ON t.invocation_id = latest.id
            WHERE t.output LIKE ?1
            ORDER BY t.test_name
            LIMIT ?2
            ",
        )?;
        let rows = stmt.query_map(rusqlite::params![&pattern, limit as i64], |row| {
            Ok(TestOutputEntry {
                test_name: row.get(0)?,
                package: row.get(1)?,
                status: row.get(2)?,
                duration_secs: row.get(3)?,
                output: row.get(4)?,
            })
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Per-package pass rate, count, avg duration, and flaky count (G7 --by-package).
    pub fn get_tests_by_package(&self) -> Result<Vec<PackageTestStats>> {
        // Aggregate from the most recent invocation
        let mut stmt = self.conn.prepare(
            r"
            WITH latest_inv AS (
                SELECT id FROM invocations
                WHERE command = 'test' AND status IN ('success', 'failed')
                ORDER BY started_at DESC LIMIT 1
            ),
            latest_tests AS (
                SELECT t.test_name, t.package, t.status, COALESCE(t.duration_secs, 0.0) as dur
                FROM test_results t
                INNER JOIN latest_inv ON t.invocation_id = latest_inv.id
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
        let rows = stmt.query_map([], |row| {
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
                SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'skip' THEN 1 ELSE 0 END)
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
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_parse_nextest_output() -> TestResult<()> {
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
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_test_name() -> TestResult<()> {
        let (pkg, name) = parse_test_name("xtask::xtask$bench::stats::tests::test_mean", "default");
        assert_eq!(pkg, "xtask");
        assert_eq!(name, "bench::stats::tests::test_mean");

        let (pkg, name) = parse_test_name("no_dollar_sign", "fallback");
        assert_eq!(pkg, "fallback");
        assert_eq!(name, "no_dollar_sign");
        Ok(())
    }

    #[sinex_test]
    async fn test_status_as_str_roundtrip() -> TestResult<()> {
        // Verify as_str and from_str are consistent
        for status in [
            TestStatus::Pass,
            TestStatus::Fail,
            TestStatus::Skip,
            TestStatus::Flaky,
        ] {
            let s = status.as_str();
            let roundtripped = TestStatus::try_from_str(s)?;
            assert_eq!(roundtripped, status, "Roundtrip failed for {s}");
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_status_from_str_aliases() -> TestResult<()> {
        // "ok" is an alias for Pass (nextest output format)
        assert_eq!(TestStatus::try_from_str("ok")?, TestStatus::Pass);
        // "failed" is an alias for Fail (nextest output format)
        assert_eq!(TestStatus::try_from_str("failed")?, TestStatus::Fail);
        // "ignored" is an alias for Skip (nextest output format)
        assert_eq!(TestStatus::try_from_str("ignored")?, TestStatus::Skip);
        // Unknown values are rejected rather than silently coerced.
        assert!(TestStatus::try_from_str("unknown").is_err());
        Ok(())
    }

    /// Helper: create a fresh in-memory-like HistoryDb with a test invocation.
    fn test_db_with_invocation() -> color_eyre::eyre::Result<(tempfile::TempDir, HistoryDb, i64)> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test-history.db");
        let db = HistoryDb::open(&db_path)?;
        let inv_id = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(
            inv_id,
            super::super::db::InvocationStatus::Success,
            Some(0),
            5.0,
        )?;
        Ok((dir, db, inv_id))
    }

    #[sinex_test]
    async fn test_store_and_get_test_results() -> TestResult<()> {
        let (_dir, db, inv_id) = test_db_with_invocation()?;

        let results = vec![
            TestResult {
                test_name: "test_alpha".into(),
                package: "pkg-a".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.5),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "test_beta".into(),
                package: "pkg-a".into(),
                status: TestStatus::Fail,
                duration_secs: Some(1.2),
                attempt: 1,
                output: Some("assertion failed".into()),
            },
        ];
        let stored = db.store_test_results(inv_id, &results)?;
        assert_eq!(stored, 2);

        let retrieved = db.get_test_results(inv_id)?;
        assert_eq!(retrieved.len(), 2);
        // Ordered by package, test_name
        assert_eq!(retrieved[0].test_name, "test_alpha");
        assert_eq!(retrieved[0].status, TestStatus::Pass);
        assert_eq!(retrieved[1].test_name, "test_beta");
        assert_eq!(retrieved[1].status, TestStatus::Fail);
        assert_eq!(retrieved[1].output.as_deref(), Some("assertion failed"));
        Ok(())
    }

    #[sinex_test]
    async fn test_get_flaky_tests_detects_retry_pass() -> TestResult<()> {
        let (_dir, db, inv_id) = test_db_with_invocation()?;

        // Simulate: test_flaky fails on attempt 1, passes on attempt 2
        let results = vec![
            TestResult {
                test_name: "test_flaky".into(),
                package: "pkg-a".into(),
                status: TestStatus::Fail,
                duration_secs: Some(0.3),
                attempt: 1,
                output: Some("timeout".into()),
            },
            TestResult {
                test_name: "test_flaky".into(),
                package: "pkg-a".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.2),
                attempt: 2,
                output: None,
            },
            // Non-flaky test: passes on first attempt
            TestResult {
                test_name: "test_stable".into(),
                package: "pkg-a".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.1),
                attempt: 1,
                output: None,
            },
        ];
        db.store_test_results(inv_id, &results)?;

        let flaky = db.get_flaky_tests(10)?;
        assert_eq!(flaky.len(), 1, "Should detect exactly one flaky test");
        assert_eq!(flaky[0].0, "test_flaky");
        assert_eq!(flaky[0].1, "pkg-a");
        assert_eq!(flaky[0].2, inv_id);
        Ok(())
    }

    #[sinex_test]
    async fn test_get_flaky_tests_no_false_positives() -> TestResult<()> {
        let (_dir, db, inv_id) = test_db_with_invocation()?;

        // Test that fails and stays failed — NOT flaky
        let results = vec![
            TestResult {
                test_name: "test_broken".into(),
                package: "pkg-a".into(),
                status: TestStatus::Fail,
                duration_secs: Some(0.3),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "test_broken".into(),
                package: "pkg-a".into(),
                status: TestStatus::Fail,
                duration_secs: Some(0.3),
                attempt: 2,
                output: None,
            },
        ];
        db.store_test_results(inv_id, &results)?;

        let flaky = db.get_flaky_tests(10)?;
        assert!(
            flaky.is_empty(),
            "Consistently failing test should not be flagged as flaky"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_get_failing_tests() -> TestResult<()> {
        let (_dir, db, inv_id) = test_db_with_invocation()?;

        let results = vec![
            TestResult {
                test_name: "test_ok".into(),
                package: "pkg-a".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.1),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "test_broken".into(),
                package: "pkg-a".into(),
                status: TestStatus::Fail,
                duration_secs: Some(0.8),
                attempt: 1,
                output: Some("panic!".into()),
            },
            TestResult {
                test_name: "test_also_broken".into(),
                package: "pkg-b".into(),
                status: TestStatus::Fail,
                duration_secs: Some(0.5),
                attempt: 1,
                output: None,
            },
        ];
        db.store_test_results(inv_id, &results)?;

        let failing = db.get_failing_tests(10)?;
        assert_eq!(failing.len(), 2);
        // Ordered by test_name
        assert_eq!(failing[0].0, "test_also_broken");
        assert_eq!(failing[1].0, "test_broken");
        Ok(())
    }

    #[sinex_test]
    async fn test_get_failing_tests_with_output() -> TestResult<()> {
        let (_dir, db, inv_id) = test_db_with_invocation()?;

        let results = vec![
            TestResult {
                test_name: "test_pass".into(),
                package: "pkg-a".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.1),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "test_fail".into(),
                package: "pkg-a".into(),
                status: TestStatus::Fail,
                duration_secs: Some(2.0),
                attempt: 1,
                output: Some("thread 'main' panicked at 'assertion failed'".into()),
            },
        ];
        db.store_test_results(inv_id, &results)?;

        let failing = db.get_failing_tests_with_output(10)?;
        assert_eq!(failing.len(), 1);
        assert_eq!(failing[0].test_name, "test_fail");
        assert!(failing[0].output.as_deref().unwrap().contains("panicked"));
        Ok(())
    }

    #[sinex_test]
    async fn test_get_slowest_tests() -> TestResult<()> {
        let (_dir, db, inv_id) = test_db_with_invocation()?;

        let results = vec![
            TestResult {
                test_name: "test_fast".into(),
                package: "pkg".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.01),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "test_slow".into(),
                package: "pkg".into(),
                status: TestStatus::Pass,
                duration_secs: Some(5.0),
                attempt: 1,
                output: None,
            },
            // Failed test should NOT appear in slowest (it inflates with timeout ceiling)
            TestResult {
                test_name: "test_failed_slow".into(),
                package: "pkg".into(),
                status: TestStatus::Fail,
                duration_secs: Some(60.0),
                attempt: 1,
                output: None,
            },
        ];
        db.store_test_results(inv_id, &results)?;

        let slowest = db.get_slowest_tests(10)?;
        assert_eq!(slowest.len(), 2, "Failed test should be excluded");
        assert_eq!(slowest[0].0, "test_slow");
        assert!(slowest[0].2 > 4.0); // avg duration > 4s
        assert_eq!(slowest[1].0, "test_fast");
        Ok(())
    }

    #[sinex_test]
    async fn test_analyze_last_run_basic() -> TestResult<()> {
        let (_dir, db, inv_id) = test_db_with_invocation()?;

        let results = vec![
            TestResult {
                test_name: "test_one".into(),
                package: "pkg-a".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.5),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "test_two".into(),
                package: "pkg-a".into(),
                status: TestStatus::Fail,
                duration_secs: Some(1.5),
                attempt: 1,
                output: Some("failed".into()),
            },
            TestResult {
                test_name: "test_three".into(),
                package: "pkg-b".into(),
                status: TestStatus::Skip,
                duration_secs: None,
                attempt: 1,
                output: None,
            },
        ];
        db.store_test_results(inv_id, &results)?;

        let analysis = db.analyze_last_run()?.expect("should have analysis");
        assert_eq!(analysis.total_passed, 1);
        assert_eq!(analysis.total_failed, 1);
        assert_eq!(analysis.total_ignored, 1);
        assert_eq!(analysis.invocation_id, inv_id);

        // Failure summary should have pkg-a with 1 failure
        assert_eq!(analysis.failure_summary.len(), 1);
        assert_eq!(analysis.failure_summary[0].package, "pkg-a");
        assert_eq!(analysis.failure_summary[0].failed_count, 1);
        assert_eq!(analysis.failure_summary[0].passed_count, 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_analyze_last_run_empty() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test-empty.db");
        let db = HistoryDb::open(&db_path)?;

        // No invocations at all
        let analysis = db.analyze_last_run()?;
        assert!(analysis.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_analyze_probable_timeouts() -> TestResult<()> {
        let (_dir, db, inv_id) = test_db_with_invocation()?;

        let results = vec![
            // Failed test at exactly 60s — probable timeout
            TestResult {
                test_name: "test_timeout".into(),
                package: "pkg".into(),
                status: TestStatus::Fail,
                duration_secs: Some(59.8),
                attempt: 1,
                output: None,
            },
            // Failed test at 3s — NOT a timeout
            TestResult {
                test_name: "test_real_fail".into(),
                package: "pkg".into(),
                status: TestStatus::Fail,
                duration_secs: Some(3.0),
                attempt: 1,
                output: None,
            },
        ];
        db.store_test_results(inv_id, &results)?;

        let analysis = db.analyze_last_run()?.expect("should have analysis");
        assert_eq!(analysis.probable_timeouts.len(), 1);
        assert_eq!(analysis.probable_timeouts[0].test_name, "test_timeout");
        Ok(())
    }

    #[sinex_test]
    async fn test_get_test_output() -> TestResult<()> {
        let (_dir, db, inv_id) = test_db_with_invocation()?;

        let results = vec![
            TestResult {
                test_name: "module::test_alpha".into(),
                package: "pkg-a".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.1),
                attempt: 1,
                output: Some("all good".into()),
            },
            TestResult {
                test_name: "module::test_beta".into(),
                package: "pkg-a".into(),
                status: TestStatus::Fail,
                duration_secs: Some(0.2),
                attempt: 1,
                output: Some("assertion failed".into()),
            },
        ];
        db.store_test_results(inv_id, &results)?;

        // Pattern match
        let output = db.get_test_output("alpha")?;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].test_name, "module::test_alpha");
        assert_eq!(output[0].output.as_deref(), Some("all good"));

        // Pattern matching multiple
        let output = db.get_test_output("test_")?;
        assert_eq!(output.len(), 2);
        Ok(())
    }

    #[sinex_test]
    async fn test_estimate_runtime() -> TestResult<()> {
        let (_dir, db, inv_id) = test_db_with_invocation()?;

        let results = vec![
            TestResult {
                test_name: "test_a".into(),
                package: "pkg-fast".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.1),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "test_b".into(),
                package: "pkg-fast".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.2),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "test_c".into(),
                package: "pkg-slow".into(),
                status: TestStatus::Pass,
                duration_secs: Some(5.0),
                attempt: 1,
                output: None,
            },
        ];
        db.store_test_results(inv_id, &results)?;

        let estimate = db.estimate_runtime()?;
        assert!(estimate.estimated_secs > 0.0);
        assert_eq!(estimate.test_count, 3);
        // Low confidence with < 5 samples
        assert_eq!(estimate.confidence, Confidence::Low);
        // Breakdown should have 2 packages
        assert_eq!(estimate.breakdown.len(), 2);
        Ok(())
    }

    #[sinex_test]
    async fn test_get_tests_getting_slower() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test-slower.db");
        let db = HistoryDb::open(&db_path)?;

        // Create multiple invocations to simulate time progression
        // Older runs: test takes ~1s
        for _ in 0..3 {
            let inv_id = db.start_invocation("test", None, None, None)?;
            db.finish_invocation(
                inv_id,
                super::super::db::InvocationStatus::Success,
                Some(0),
                5.0,
            )?;
            db.store_test_results(
                inv_id,
                &[TestResult {
                    test_name: "test_regressing".into(),
                    package: "pkg".into(),
                    status: TestStatus::Pass,
                    duration_secs: Some(1.0),
                    attempt: 1,
                    output: None,
                }],
            )?;
        }

        // Recent runs: test takes ~3s (200% slower)
        for _ in 0..3 {
            let inv_id = db.start_invocation("test", None, None, None)?;
            db.finish_invocation(
                inv_id,
                super::super::db::InvocationStatus::Success,
                Some(0),
                5.0,
            )?;
            db.store_test_results(
                inv_id,
                &[TestResult {
                    test_name: "test_regressing".into(),
                    package: "pkg".into(),
                    status: TestStatus::Pass,
                    duration_secs: Some(3.0),
                    attempt: 1,
                    output: None,
                }],
            )?;
        }

        let slower = db.get_tests_getting_slower(6, 50.0, 10)?;
        assert!(
            !slower.is_empty(),
            "Should detect test_regressing as getting slower"
        );
        assert_eq!(slower[0].test_name, "test_regressing");
        assert!(slower[0].pct_change > 100.0, "Should show >100% regression");
        Ok(())
    }

    #[sinex_test]
    async fn test_get_tests_getting_slower_zero_window() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test-zero-window.db");
        let db = HistoryDb::open(&db_path)?;

        // Window of 0 or 1 means half_window = 0 → early return
        let result = db.get_tests_getting_slower(0, 50.0, 10)?;
        assert!(result.is_empty());
        let result = db.get_tests_getting_slower(1, 50.0, 10)?;
        assert!(result.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_get_test_trends() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test-trends.db");
        let db = HistoryDb::open(&db_path)?;

        // Create 3 runs with varying durations
        for duration in [1.0, 1.5, 2.0] {
            let inv_id = db.start_invocation("test", None, None, None)?;
            db.finish_invocation(
                inv_id,
                super::super::db::InvocationStatus::Success,
                Some(0),
                5.0,
            )?;
            db.store_test_results(
                inv_id,
                &[TestResult {
                    test_name: "test_trending".into(),
                    package: "pkg".into(),
                    status: TestStatus::Pass,
                    duration_secs: Some(duration),
                    attempt: 1,
                    output: None,
                }],
            )?;
        }

        // Get trends for all tests
        let trends = db.get_test_trends(None, None, 10)?;
        assert_eq!(trends.len(), 1);
        assert_eq!(trends[0].test_name, "test_trending");
        assert_eq!(trends[0].durations.len(), 3);
        assert!(trends[0].avg_duration_secs > 1.0);

        // Filter by pattern
        let trends = db.get_test_trends(Some("trending"), None, 10)?;
        assert_eq!(trends.len(), 1);

        // Filter by non-matching pattern
        let trends = db.get_test_trends(Some("nonexistent"), None, 10)?;
        assert!(trends.is_empty());

        // Filter by package
        let trends = db.get_test_trends(None, Some("pkg"), 10)?;
        assert_eq!(trends.len(), 1);

        // Limit runs per test
        let trends = db.get_test_trends(None, None, 2)?;
        assert_eq!(trends[0].durations.len(), 2);
        Ok(())
    }

    #[sinex_test]
    async fn test_duration_buckets() -> TestResult<()> {
        let (_dir, db, inv_id) = test_db_with_invocation()?;

        let results = vec![
            TestResult {
                test_name: "test_instant".into(),
                package: "pkg".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.01),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "test_medium".into(),
                package: "pkg".into(),
                status: TestStatus::Pass,
                duration_secs: Some(7.0),
                attempt: 1,
                output: None,
            },
            TestResult {
                test_name: "test_long".into(),
                package: "pkg".into(),
                status: TestStatus::Pass,
                duration_secs: Some(45.0),
                attempt: 1,
                output: None,
            },
        ];
        db.store_test_results(inv_id, &results)?;

        let analysis = db.analyze_last_run()?.expect("should have analysis");

        // Check bucket distribution
        let sub_1s = analysis
            .duration_buckets
            .iter()
            .find(|b| b.label == "< 1s")
            .unwrap();
        assert_eq!(sub_1s.count, 1);
        let five_to_ten = analysis
            .duration_buckets
            .iter()
            .find(|b| b.label == "5-10s")
            .unwrap();
        assert_eq!(five_to_ten.count, 1);
        let thirty_to_sixty = analysis
            .duration_buckets
            .iter()
            .find(|b| b.label == "30-60s")
            .unwrap();
        assert_eq!(thirty_to_sixty.count, 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_confidence_display() -> TestResult<()> {
        assert_eq!(format!("{}", Confidence::Low), "low");
        assert_eq!(format!("{}", Confidence::Medium), "medium");
        assert_eq!(format!("{}", Confidence::High), "high");
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_nextest_output_empty() -> TestResult<()> {
        let results = parse_nextest_output("");
        assert!(results.is_empty());

        let results = parse_nextest_output("not json\njust text\n");
        assert!(results.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_nextest_output_ignores_started_events() -> TestResult<()> {
        let output = r#"
{"type":"suite","event":"started","test_count":1}
{"type":"test","event":"started","name":"pkg::pkg$test_one"}
"#;
        let results = parse_nextest_output(output);
        assert!(results.is_empty(), "Started events should be skipped");
        Ok(())
    }
}
