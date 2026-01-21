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

#[derive(Debug, Deserialize, Default)]
struct NextestMeta {
    #[serde(rename = "crate")]
    crate_name: Option<String>,
}

/// Parse nextest libtest-json output and extract test results.
pub fn parse_nextest_output(output: &str) -> Vec<TestResult> {
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
    pub fn store_test_results(
        &self,
        invocation_id: i64,
        results: &[TestResult],
    ) -> Result<usize> {
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

        let rows = stmt.query_map([limit], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect flaky tests")
    }

    /// Get currently failing tests (failed in most recent run).
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

        let rows = stmt.query_map([limit], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;

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
