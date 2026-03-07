//! Fluent query builders for xtask history database.
//!
//! Instead of proliferating bespoke query methods, these builders compose arbitrary filter
//! combinations into SQL WHERE clauses (no in-memory post-filtering).
//!
//! # Usage
//!
//! ```rust,no_run
//! # use xtask::history::{HistoryDb, query::*};
//! # let db = todo!();
//! // Diagnostics: current package-scoped view, fixable warnings in sinex-db
//! let diags = DiagnosticQuery::new()
//!     .package("sinex-db")
//!     .fixable()
//!     .level("warning")
//!     .limit(50)
//!     .run(&db)?;
//!
//! // Invocations: last 7 days of successful check runs
//! let invs = InvocationQuery::new()
//!     .command("check")
//!     .succeeded()
//!     .days(7)
//!     .run(&db)?;
//!
//! // Test results: failing tests in sinex-db from recent nextest run
//! let tests = TestResultQuery::new()
//!     .package("sinex-db")
//!     .failing()
//!     .with_output()
//!     .run(&db)?;
//! # Ok::<(), color_eyre::eyre::Error>(())
//! ```

use color_eyre::eyre::Result;
use serde::Serialize;
use time::OffsetDateTime;

use super::db::{HistoryDb, Invocation, InvocationStatus, StoredDiagnostic};
use super::tests::{TestResult, TestStatus};

// ─── Shared base ─────────────────────────────────────────────────────────────

/// Filter state shared by all query builders.
#[derive(Default, Clone)]
pub struct QueryBase {
    pub command_filter: Option<String>,
    pub package_filter: Option<String>,
    pub days: Option<u32>,
    pub limit: usize,
}

// ─── DiagnosticQuery ─────────────────────────────────────────────────────────

/// Which invocation scope to query diagnostics from.
#[derive(Default, Clone)]
pub enum DiagnosticScope {
    /// Package-scoped supersession: latest invocation per package (default).
    ///
    /// This is the most useful view — each package contributes only its most recent run.
    #[default]
    Current,
    /// All diagnostics from a specific invocation ID.
    Invocation(i64),
    /// Raw accumulated diagnostics across recent invocations (no supersession).
    Recent,
}

/// Fluent builder for querying stored compiler diagnostics.
///
/// Generates SQL WHERE clauses — no in-memory post-filtering.
#[derive(Clone)]
pub struct DiagnosticQuery {
    base: QueryBase,
    level_filter: Option<String>,
    file_pattern: Option<String>,
    fixable_only: bool,
    scope: DiagnosticScope,
}

impl Default for DiagnosticQuery {
    fn default() -> Self {
        Self {
            base: QueryBase {
                limit: 200,
                ..Default::default()
            },
            level_filter: None,
            file_pattern: None,
            fixable_only: false,
            scope: DiagnosticScope::Current,
        }
    }
}

impl DiagnosticQuery {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by diagnostic level (e.g. `"error"`, `"warning"`).
    pub fn level(mut self, level: impl Into<String>) -> Self {
        self.level_filter = Some(level.into());
        self
    }

    /// Filter by file path substring (SQL `LIKE %pattern%`).
    pub fn file(mut self, pattern: impl Into<String>) -> Self {
        self.file_pattern = Some(pattern.into());
        self
    }

    /// Filter to diagnostics from a specific package.
    pub fn package(mut self, package: impl Into<String>) -> Self {
        self.base.package_filter = Some(package.into());
        self
    }

    /// Filter by the command that produced the diagnostic (e.g. `"check"`, `"build"`).
    pub fn command(mut self, cmd: impl Into<String>) -> Self {
        self.base.command_filter = Some(cmd.into());
        self
    }

    /// Only return diagnostics with `MachineApplicable` fixes.
    #[must_use]
    pub fn fixable(mut self) -> Self {
        self.fixable_only = true;
        self
    }

    /// Maximum number of diagnostics to return (default: 200).
    #[must_use]
    pub fn limit(mut self, n: usize) -> Self {
        self.base.limit = n;
        self
    }

    /// Scope to a specific invocation by database ID.
    #[must_use]
    pub fn for_invocation(mut self, id: i64) -> Self {
        self.scope = DiagnosticScope::Invocation(id);
        self
    }

    /// Use package-scoped supersession — latest invocation per package (default).
    #[must_use]
    pub fn current(mut self) -> Self {
        self.scope = DiagnosticScope::Current;
        self
    }

    /// Query raw accumulated diagnostics across recent invocations (no supersession).
    #[must_use]
    pub fn recent(mut self) -> Self {
        self.scope = DiagnosticScope::Recent;
        self
    }

    /// Execute and return matching diagnostics.
    pub fn run(self, db: &HistoryDb) -> Result<Vec<StoredDiagnostic>> {
        db.run_diagnostic_query(&self)
    }

    /// Execute and return only the count.
    pub fn count(self, db: &HistoryDb) -> Result<usize> {
        db.count_diagnostic_query(&self)
    }
}

// ─── InvocationQuery ─────────────────────────────────────────────────────────

/// Fluent builder for querying command invocation history.
///
/// ```rust,no_run
/// # use xtask::history::{HistoryDb, query::*};
/// # let db = todo!();
/// let invs = InvocationQuery::new()
///     .command("check")
///     .succeeded()
///     .days(7)
///     .limit(20)
///     .run(&db)?;
/// # Ok::<(), color_eyre::eyre::Error>(())
/// ```
#[derive(Clone, Default)]
pub struct InvocationQuery {
    base: QueryBase,
    status_filter: Option<InvocationStatus>,
}

impl InvocationQuery {
    #[must_use]
    pub fn new() -> Self {
        Self {
            base: QueryBase {
                limit: 20,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Filter by xtask command (e.g. `"check"`, `"test"`, `"build"`).
    pub fn command(mut self, cmd: impl Into<String>) -> Self {
        self.base.command_filter = Some(cmd.into());
        self
    }

    /// Only return invocations that touched a specific package.
    pub fn package(mut self, package: impl Into<String>) -> Self {
        self.base.package_filter = Some(package.into());
        self
    }

    /// Only return invocations started within the last `n` days.
    #[must_use]
    pub fn days(mut self, n: u32) -> Self {
        self.base.days = Some(n);
        self
    }

    /// Maximum number of invocations to return (default: 20).
    #[must_use]
    pub fn limit(mut self, n: usize) -> Self {
        self.base.limit = n;
        self
    }

    /// Only successful invocations.
    #[must_use]
    pub fn succeeded(mut self) -> Self {
        self.status_filter = Some(InvocationStatus::Success);
        self
    }

    /// Only failed invocations.
    #[must_use]
    pub fn failed(mut self) -> Self {
        self.status_filter = Some(InvocationStatus::Failed);
        self
    }

    /// Filter by an explicit status.
    #[must_use]
    pub fn status(mut self, s: InvocationStatus) -> Self {
        self.status_filter = Some(s);
        self
    }

    /// Execute and return matching invocations (newest first).
    pub fn run(self, db: &HistoryDb) -> Result<Vec<Invocation>> {
        db.run_invocation_query(&self)
    }

    /// Execute and return the first (most recent) match, if any.
    pub fn first(self, db: &HistoryDb) -> Result<Option<Invocation>> {
        Ok(self.limit(1).run(db)?.into_iter().next())
    }

    /// Execute and return only the count.
    pub fn count(self, db: &HistoryDb) -> Result<usize> {
        db.count_invocation_query(&self)
    }
}

// ─── TestResultQuery ─────────────────────────────────────────────────────────

/// Fluent builder for querying stored nextest results.
///
/// ```rust,no_run
/// # use xtask::history::{HistoryDb, query::*};
/// # let db = todo!();
/// let tests = TestResultQuery::new()
///     .package("sinex-db")
///     .failing()
///     .with_output()
///     .limit(10)
///     .run(&db)?;
/// # Ok::<(), color_eyre::eyre::Error>(())
/// ```
#[derive(Clone, Default)]
pub struct TestResultQuery {
    base: QueryBase,
    status_filter: Option<TestStatus>,
    invocation_id: Option<i64>,
    with_output: bool,
    test_mode: Option<String>,
}

impl TestResultQuery {
    #[must_use]
    pub fn new() -> Self {
        Self {
            base: QueryBase {
                limit: 50,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Filter by package.
    pub fn package(mut self, package: impl Into<String>) -> Self {
        self.base.package_filter = Some(package.into());
        self
    }

    /// Filter by the xtask command that produced the test run.
    pub fn command(mut self, cmd: impl Into<String>) -> Self {
        self.base.command_filter = Some(cmd.into());
        self
    }

    /// Only results from invocations in the last `n` days.
    #[must_use]
    pub fn days(mut self, n: u32) -> Self {
        self.base.days = Some(n);
        self
    }

    /// Maximum number of results (default: 50).
    #[must_use]
    pub fn limit(mut self, n: usize) -> Self {
        self.base.limit = n;
        self
    }

    /// Filter by test status.
    #[must_use]
    pub fn status(mut self, s: TestStatus) -> Self {
        self.status_filter = Some(s);
        self
    }

    /// Only failing tests.
    #[must_use]
    pub fn failing(mut self) -> Self {
        self.status_filter = Some(TestStatus::Fail);
        self
    }

    /// Only passing tests.
    #[must_use]
    pub fn passing(mut self) -> Self {
        self.status_filter = Some(TestStatus::Pass);
        self
    }

    /// Scope to results from a specific invocation ID.
    #[must_use]
    pub fn for_invocation(mut self, id: i64) -> Self {
        self.invocation_id = Some(id);
        self
    }

    /// Include captured stdout/stderr output in results.
    ///
    /// When not set, the `output` field in returned `TestResult`s may be `None`
    /// for performance (avoids loading potentially large blobs).
    #[must_use]
    pub fn with_output(mut self) -> Self {
        self.with_output = true;
        self
    }

    /// Filter by test mode (e.g. `"nextest"`, `"bench"`, `"vm"`).
    pub fn mode(mut self, mode: impl Into<String>) -> Self {
        self.test_mode = Some(mode.into());
        self
    }

    /// Execute and return matching test results (newest first).
    pub fn run(self, db: &HistoryDb) -> Result<Vec<TestResult>> {
        db.run_test_result_query(&self)
    }

    /// Execute and return only the count.
    pub fn count(self, db: &HistoryDb) -> Result<usize> {
        db.count_test_result_query(&self)
    }
}

// ─── HistoryAnalysis ─────────────────────────────────────────────────────────

/// Package-level health snapshot across all three query dimensions.
#[derive(Debug, Clone, Serialize)]
pub struct PackageHealth {
    pub package: String,
    pub diagnostic_count: usize,
    pub fixable_count: usize,
    /// Pass rate over the last 7 days (0.0–1.0). `None` if no test data.
    pub test_pass_rate: Option<f64>,
    /// Average check invocation duration over the last 7 days. `None` if no data.
    pub avg_build_time_secs: Option<f64>,
}

/// A newly-appearing diagnostic correlated with recent test failures.
#[derive(Debug, Clone, Serialize)]
pub struct Regression {
    pub invocation_id: i64,
    pub package: Option<String>,
    pub level: String,
    pub message: String,
    /// Number of test failures in the same invocation's test output.
    pub test_failures: usize,
}

/// Cross-dimensional analysis facade that composes `DiagnosticQuery`,
/// `InvocationQuery`, and `TestResultQuery` into multi-dimensional views.
///
/// ```rust,no_run
/// # use xtask::history::{HistoryDb, query::HistoryAnalysis};
/// # let db = todo!();
/// let analysis = HistoryAnalysis::new(&db);
/// let health = analysis.package_health("sinex-db")?;
/// println!("{:.0}% tests passing, {} diagnostics", health.test_pass_rate.unwrap_or(0.0) * 100.0, health.diagnostic_count);
/// # Ok::<(), color_eyre::eyre::Error>(())
/// ```
pub struct HistoryAnalysis<'db> {
    db: &'db HistoryDb,
}

impl<'db> HistoryAnalysis<'db> {
    pub fn new(db: &'db HistoryDb) -> Self {
        Self { db }
    }

    /// Health snapshot for a package: diagnostics, test pass rate, average build time.
    pub fn package_health(&self, package: &str) -> Result<PackageHealth> {
        let diagnostic_count = DiagnosticQuery::new()
            .package(package)
            .current()
            .count(self.db)?;

        let fixable_count = DiagnosticQuery::new()
            .package(package)
            .fixable()
            .current()
            .count(self.db)?;

        let test_pass_rate = self.compute_test_pass_rate(package)?;
        let avg_build_time_secs = self.compute_avg_build_time(package)?;

        Ok(PackageHealth {
            package: package.to_string(),
            diagnostic_count,
            fixable_count,
            test_pass_rate,
            avg_build_time_secs,
        })
    }

    /// Health snapshot for all packages known to the diagnostics history (G4).
    ///
    /// Iterates over all packages that have appeared in diagnostics and calls
    /// `package_health` for each. Sorted by diagnostic count descending.
    pub fn all_packages_health(&self) -> Result<Vec<PackageHealth>> {
        let packages = self.db.get_known_packages()?;
        let mut results = Vec::with_capacity(packages.len());
        for pkg in &packages {
            results.push(self.package_health(pkg)?);
        }
        results.sort_by(|a, b| b.diagnostic_count.cmp(&a.diagnostic_count));
        Ok(results)
    }

    /// Scan for error-level diagnostics in failed invocations since `since`.
    ///
    /// Each returned `Regression` includes the number of co-occurring test failures,
    /// enabling correlation between new errors and test breakage.
    pub fn regression_scan(&self, since: OffsetDateTime) -> Result<Vec<Regression>> {
        let days_back = {
            let diff = OffsetDateTime::now_utc() - since;
            diff.whole_days().max(1) as u32
        };

        let failing_invocations = InvocationQuery::new()
            .failed()
            .days(days_back)
            .limit(50)
            .run(self.db)?;

        let mut regressions = Vec::new();
        for inv in failing_invocations {
            let diags = DiagnosticQuery::new()
                .for_invocation(inv.id)
                .level("error")
                .run(self.db)?;

            let test_failures = TestResultQuery::new()
                .for_invocation(inv.id)
                .failing()
                .count(self.db)?;

            for diag in diags {
                regressions.push(Regression {
                    invocation_id: inv.id,
                    package: diag.package,
                    level: diag.level,
                    message: diag.message,
                    test_failures,
                });
            }
        }

        Ok(regressions)
    }

    fn compute_test_pass_rate(&self, package: &str) -> Result<Option<f64>> {
        let total = TestResultQuery::new()
            .package(package)
            .days(7)
            .count(self.db)?;
        if total == 0 {
            return Ok(None);
        }
        let passed = TestResultQuery::new()
            .package(package)
            .passing()
            .days(7)
            .count(self.db)?;
        Ok(Some(passed as f64 / total as f64))
    }

    fn compute_avg_build_time(&self, package: &str) -> Result<Option<f64>> {
        // Approximate via successful check invocations (package-aware via invocation_packages).
        let invocations = InvocationQuery::new()
            .command("check")
            .package(package)
            .succeeded()
            .days(7)
            .limit(20)
            .run(self.db)?;

        let durations: Vec<f64> = invocations
            .iter()
            .filter_map(|inv| inv.duration_secs)
            .collect();

        if durations.is_empty() {
            return Ok(None);
        }
        Ok(Some(durations.iter().sum::<f64>() / durations.len() as f64))
    }
}

// ─── HistoryDb executor methods ──────────────────────────────────────────────
// These are package-private executor methods called by the query builders.
// They live here rather than in db.rs to keep db.rs focused on schema/CRUD,
// while this module owns the query composition logic.

impl HistoryDb {
    /// Execute a `DiagnosticQuery` and return results.
    pub(crate) fn run_diagnostic_query(
        &self,
        q: &DiagnosticQuery,
    ) -> Result<Vec<StoredDiagnostic>> {
        let conn = &self.conn;
        let mut where_clauses = Vec::<String>::new();
        let mut bound_params: Vec<String> = Vec::new();

        // Build SELECT + FROM based on scope
        let (cte, select_from) = match &q.scope {
            DiagnosticScope::Current => {
                let mut cte = String::from(
                    "WITH latest_per_package AS (\
                    SELECT ip.package, MAX(i.id) as latest_inv_id \
                    FROM invocation_packages ip \
                    JOIN invocations i ON ip.invocation_id = i.id \
                    WHERE i.status IN ('success', 'failed')",
                );
                if let Some(cmd) = &q.base.command_filter {
                    cte.push_str(" AND i.command = ?");
                    bound_params.push(cmd.clone());
                }
                cte.push_str(" GROUP BY ip.package)");
                let select = "SELECT bd.id, bd.level, bd.code, bd.message, bd.file_path, \
                    bd.line, bd.col, bd.rendered, bd.package, bd.fix_replacement, \
                    bd.fix_applicability, bd.fix_byte_start, bd.fix_byte_end, \
                    i.command as source_command, i.started_at as source_time \
                    FROM build_diagnostics bd \
                    JOIN latest_per_package lpp ON bd.package = lpp.package \
                        AND bd.invocation_id = lpp.latest_inv_id \
                    JOIN invocations i ON bd.invocation_id = i.id";
                (cte, select.to_string())
            }
            DiagnosticScope::Invocation(id) => {
                where_clauses.push("bd.invocation_id = ?".into());
                bound_params.push(id.to_string());
                let select = "SELECT bd.id, bd.level, bd.code, bd.message, bd.file_path, \
                    bd.line, bd.col, bd.rendered, bd.package, bd.fix_replacement, \
                    bd.fix_applicability, bd.fix_byte_start, bd.fix_byte_end, \
                    i.command as source_command, i.started_at as source_time \
                    FROM build_diagnostics bd \
                    JOIN invocations i ON bd.invocation_id = i.id";
                (String::new(), select.to_string())
            }
            DiagnosticScope::Recent => {
                let select = "SELECT bd.id, bd.level, bd.code, bd.message, bd.file_path, \
                    bd.line, bd.col, bd.rendered, bd.package, bd.fix_replacement, \
                    bd.fix_applicability, bd.fix_byte_start, bd.fix_byte_end, \
                    i.command as source_command, i.started_at as source_time \
                    FROM build_diagnostics bd \
                    JOIN invocations i ON bd.invocation_id = i.id";
                if let Some(cmd) = &q.base.command_filter {
                    where_clauses.push("i.command = ?".into());
                    bound_params.push(cmd.clone());
                }
                (String::new(), select.to_string())
            }
        };

        // Shared WHERE filters
        if let Some(level) = &q.level_filter {
            where_clauses.push("bd.level = ?".into());
            bound_params.push(level.clone());
        }
        if let Some(pattern) = &q.file_pattern {
            where_clauses.push("bd.file_path LIKE ?".into());
            bound_params.push(format!("%{pattern}%"));
        }
        if let Some(package) = &q.base.package_filter {
            where_clauses.push("bd.package = ?".into());
            bound_params.push(package.clone());
        }
        if q.fixable_only {
            where_clauses.push("bd.fix_applicability = 'MachineApplicable'".into());
        }

        let where_sql = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let limit_sql = format!(" ORDER BY bd.id LIMIT {}", q.base.limit);
        let sql = format!("{cte} {select_from}{where_sql}{limit_sql}");

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(bound_params.iter()), |row| {
            Ok(StoredDiagnostic {
                id: row.get(0)?,
                level: row.get(1)?,
                code: row.get(2)?,
                message: row.get(3)?,
                file_path: row.get(4)?,
                line: row.get(5)?,
                col: row.get(6)?,
                rendered: row.get(7)?,
                package: row.get(8)?,
                fix_replacement: row.get(9)?,
                fix_applicability: row.get(10)?,
                fix_byte_start: row.get(11)?,
                fix_byte_end: row.get(12)?,
                source_command: row.get(13)?,
                source_time: row.get(14)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub(crate) fn count_diagnostic_query(&self, q: &DiagnosticQuery) -> Result<usize> {
        // Reuse run and take len — simpler than duplicating SQL with COUNT(*)
        Ok(self.run_diagnostic_query(q)?.len())
    }

    /// Execute an `InvocationQuery` and return results.
    pub(crate) fn run_invocation_query(&self, q: &InvocationQuery) -> Result<Vec<Invocation>> {
        let conn = &self.conn;
        let mut where_clauses = Vec::<String>::new();
        let mut bound_params: Vec<String> = Vec::new();

        if let Some(cmd) = &q.base.command_filter {
            where_clauses.push("i.command = ?".into());
            bound_params.push(cmd.clone());
        }
        if let Some(status) = &q.status_filter {
            where_clauses.push("i.status = ?".into());
            bound_params.push(status.as_str().into());
        }
        if let Some(days) = q.base.days {
            where_clauses.push(format!("i.started_at > datetime('now', '-{days} days')"));
        }
        if let Some(pkg) = &q.base.package_filter {
            // Filter via invocation_packages join
            where_clauses.push(
                "EXISTS (SELECT 1 FROM invocation_packages ip WHERE ip.invocation_id = i.id AND ip.package = ?)".into()
            );
            bound_params.push(pkg.clone());
        }

        let where_sql = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let sql = format!(
            "SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty, \
            started_at, finished_at, duration_secs, exit_code, status, host, cwd, live_stage \
            FROM invocations i{where_sql} \
            ORDER BY i.id DESC LIMIT {}",
            q.base.limit
        );

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(bound_params.iter()), |row| {
            let started_str: String = row.get(7)?;
            let finished_str: Option<String> = row.get(8)?;
            let status_str: String = row.get(11)?;
            Ok(Invocation {
                id: row.get(0)?,
                command: row.get(1)?,
                subcommand: row.get(2)?,
                profile: row.get(3)?,
                args_json: row.get(4)?,
                git_commit: row.get(5)?,
                git_dirty: row.get::<_, i32>(6)? != 0,
                started_at: OffsetDateTime::parse(
                    &started_str,
                    &time::format_description::well_known::Rfc3339,
                )
                .unwrap_or_else(|_| OffsetDateTime::now_utc()),
                finished_at: finished_str.and_then(|s| {
                    OffsetDateTime::parse(&s, &time::format_description::well_known::Rfc3339).ok()
                }),
                duration_secs: row.get(9)?,
                exit_code: row.get(10)?,
                status: InvocationStatus::from_str(&status_str),
                host: row.get(12)?,
                cwd: row.get(13)?,
                live_stage: row.get(14)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub(crate) fn count_invocation_query(&self, q: &InvocationQuery) -> Result<usize> {
        Ok(self.run_invocation_query(q)?.len())
    }

    /// Execute a `TestResultQuery` and return results.
    pub(crate) fn run_test_result_query(&self, q: &TestResultQuery) -> Result<Vec<TestResult>> {
        let conn = &self.conn;
        let mut where_clauses = Vec::<String>::new();
        let mut bound_params: Vec<String> = Vec::new();

        if let Some(inv_id) = q.invocation_id {
            where_clauses.push("tr.invocation_id = ?".into());
            bound_params.push(inv_id.to_string());
        }
        if let Some(pkg) = &q.base.package_filter {
            where_clauses.push("tr.package = ?".into());
            bound_params.push(pkg.clone());
        }
        if let Some(status) = &q.status_filter {
            where_clauses.push("tr.status = ?".into());
            bound_params.push(status.as_str().into());
        }
        if let Some(cmd) = &q.base.command_filter {
            where_clauses.push("i.command = ?".into());
            bound_params.push(cmd.clone());
        }
        if let Some(days) = q.base.days {
            where_clauses.push(format!("i.started_at > datetime('now', '-{days} days')"));
        }
        if let Some(mode) = &q.test_mode {
            where_clauses.push("tr.test_mode = ?".into());
            bound_params.push(mode.clone());
        }

        let where_sql = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let output_col = if q.with_output {
            "tr.output"
        } else {
            "NULL as output"
        };

        let sql = format!(
            "SELECT tr.test_name, tr.package, tr.status, tr.duration_secs, tr.attempt, {output_col} \
            FROM test_results tr \
            JOIN invocations i ON tr.invocation_id = i.id{where_sql} \
            ORDER BY tr.id DESC LIMIT {}",
            q.base.limit
        );

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(bound_params.iter()), |row| {
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

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub(crate) fn count_test_result_query(&self, q: &TestResultQuery) -> Result<usize> {
        Ok(self.run_test_result_query(q)?.len())
    }
}
