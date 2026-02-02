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
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Skip => "skip",
            Self::Flaky => "flaky",
        }
    }

    #[allow(dead_code)]
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
        #[allow(dead_code)]
        event: String,
        #[allow(dead_code)]
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
            Ok(NextestEvent::Suite { nextest, .. }) => {
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
                        Some(format!("stdout:\n{}\nstderr:\n{}", out, err))
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
            Err(_) => continue, // Skip unparseable lines
        }
    }

    results
}

/// Parse nextest test name format to extract package and test name.
/// Format: "crate::binary$module::path::test_name"
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
    #[allow(dead_code)]
    pub fn store_test_results(&self, invocation_id: i64, results: &[TestResult]) -> Result<usize> {
        let mut stored = 0;
        for result in results {
            self.conn.execute(
                r#"
                INSERT OR REPLACE INTO test_results
                    (invocation_id, test_name, package, status, duration_secs, attempt, output)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
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
    #[allow(dead_code)]
    pub fn get_test_results(&self, invocation_id: i64) -> Result<Vec<TestResult>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT test_name, package, status, duration_secs, attempt, output
            FROM test_results
            WHERE invocation_id = ?1
            ORDER BY package, test_name
            "#,
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
            r#"
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
            "#,
        )?;

        let rows = stmt.query_map([limit], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect flaky tests")
    }

    /// Get frequently failing tests.
    #[allow(dead_code)]
    pub fn get_failing_tests(&self, limit: usize) -> Result<Vec<(String, String, f64)>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT t.test_name, t.package, COALESCE(t.duration_secs, 0) as duration
            FROM test_results t
            INNER JOIN (
                SELECT MAX(invocation_id) as max_inv
                FROM invocations
                WHERE command = 'test'
            ) latest ON t.invocation_id = latest.max_inv
            WHERE t.status = 'fail'
            ORDER BY t.test_name
            LIMIT ?1
            "#,
        )?;

        let rows = stmt.query_map([limit], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect failing tests")
    }

    /// Get slowest tests by average duration.
    pub fn get_slowest_tests(&self, limit: usize) -> Result<Vec<(String, String, f64, i64)>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT test_name, package, AVG(duration_secs) as avg_duration, COUNT(*) as runs
            FROM test_results
            WHERE duration_secs IS NOT NULL
            GROUP BY test_name, package
            ORDER BY avg_duration DESC
            LIMIT ?1
            "#,
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
            r#"
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
            "#,
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
        let pattern_like = pattern.map(|p| format!("%{}%", p));
        let package_like = package.map(|p| format!("%{}%", p));

        let mut stmt = self.conn.prepare(
            r#"
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
            "#,
        )?;

        let all_rows: Vec<(String, String, f64, String)> = stmt
            .query_map(rusqlite::params![pattern_like, package_like], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .filter_map(|r| r.ok())
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
            r#"
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
            GROUP BY t.package
            "#,
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
