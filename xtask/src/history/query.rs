//! Fluent query builders for xtask history database.
//!
//! These builders compose arbitrary filter combinations into SQL WHERE clauses
//! without in-memory post-filtering.
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

use super::db::{HistoryDb, Invocation, InvocationStatus, StoredDiagnostic, row_to_invocation};
use super::tests::{TestResult, TestStatus, parse_stored_test_status};

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
    invocation_id: Option<i64>,
    after_invocation_id: Option<i64>,
    before_invocation_id: Option<i64>,
    since_rfc3339: Option<String>,
    offset: usize,
    sort_by: InvocationSort,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum InvocationSort {
    #[default]
    Started,
    Duration,
    Status,
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

    /// Only return the invocation with this exact database ID.
    #[must_use]
    pub fn for_invocation(mut self, id: i64) -> Self {
        self.invocation_id = Some(id);
        self
    }

    /// Only return invocations recorded after this database ID.
    #[must_use]
    pub fn after_invocation(mut self, id: i64) -> Self {
        self.after_invocation_id = Some(id);
        self
    }

    /// Only return invocations recorded before this database ID.
    #[must_use]
    pub fn before_invocation(mut self, id: i64) -> Self {
        self.before_invocation_id = Some(id);
        self
    }

    /// Only return invocations started on or after this RFC3339 timestamp.
    pub fn since_rfc3339(mut self, timestamp: impl Into<String>) -> Self {
        self.since_rfc3339 = Some(timestamp.into());
        self
    }

    /// Skip N matching invocations after sorting.
    #[must_use]
    pub fn offset(mut self, n: usize) -> Self {
        self.offset = n;
        self
    }

    /// Sort by newest started time (default).
    #[must_use]
    pub fn sort_started(mut self) -> Self {
        self.sort_by = InvocationSort::Started;
        self
    }

    /// Sort by descending duration, leaving incomplete invocations last.
    #[must_use]
    pub fn sort_duration(mut self) -> Self {
        self.sort_by = InvocationSort::Duration;
        self
    }

    /// Sort by status then ID.
    #[must_use]
    pub fn sort_status(mut self) -> Self {
        self.sort_by = InvocationSort::Status;
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
        let mut bound_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(cmd) = &q.base.command_filter {
            where_clauses.push("i.command = ?".into());
            bound_params.push(Box::new(cmd.clone()));
        }
        if let Some(status) = &q.status_filter {
            where_clauses.push("i.status = ?".into());
            bound_params.push(Box::new(status.as_str().to_string()));
        }
        if let Some(days) = q.base.days {
            where_clauses.push(format!("i.started_at > datetime('now', '-{days} days')"));
        }
        if let Some(pkg) = &q.base.package_filter {
            // Filter via invocation_packages join
            where_clauses.push(
                "EXISTS (SELECT 1 FROM invocation_packages ip WHERE ip.invocation_id = i.id AND ip.package = ?)".into()
            );
            bound_params.push(Box::new(pkg.clone()));
        }
        if let Some(invocation_id) = q.invocation_id {
            where_clauses.push("i.id = ?".into());
            bound_params.push(Box::new(invocation_id));
        }
        if let Some(after_invocation_id) = q.after_invocation_id {
            where_clauses.push("i.id > ?".into());
            bound_params.push(Box::new(after_invocation_id));
        }
        if let Some(before_invocation_id) = q.before_invocation_id {
            where_clauses.push("i.id < ?".into());
            bound_params.push(Box::new(before_invocation_id));
        }
        if let Some(since_rfc3339) = &q.since_rfc3339 {
            where_clauses.push("datetime(i.started_at) >= datetime(?)".into());
            bound_params.push(Box::new(since_rfc3339.clone()));
        }

        let where_sql = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let order_sql = match q.sort_by {
            InvocationSort::Started => "i.started_at DESC, i.id DESC",
            InvocationSort::Duration => "i.duration_secs DESC NULLS LAST, i.id DESC",
            InvocationSort::Status => "i.status ASC, i.id DESC",
        };

        let sql = format!(
            "SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty, \
            started_at, finished_at, duration_secs, exit_code, status, host, cwd, live_stage \
            FROM invocations i{where_sql} \
            ORDER BY {order_sql} LIMIT ? OFFSET ?"
        );

        bound_params.push(Box::new(q.base.limit as i64));
        bound_params.push(Box::new(q.offset as i64));

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::ToSql> = bound_params
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect();
        let rows = stmt.query_map(rusqlite::params_from_iter(param_refs), row_to_invocation)?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub(crate) fn count_invocation_query(&self, q: &InvocationQuery) -> Result<usize> {
        let conn = &self.conn;
        let mut where_clauses = Vec::<String>::new();
        let mut bound_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(cmd) = &q.base.command_filter {
            where_clauses.push("i.command = ?".into());
            bound_params.push(Box::new(cmd.clone()));
        }
        if let Some(status) = &q.status_filter {
            where_clauses.push("i.status = ?".into());
            bound_params.push(Box::new(status.as_str().to_string()));
        }
        if let Some(days) = q.base.days {
            where_clauses.push(format!("i.started_at > datetime('now', '-{days} days')"));
        }
        if let Some(pkg) = &q.base.package_filter {
            where_clauses.push(
                "EXISTS (SELECT 1 FROM invocation_packages ip WHERE ip.invocation_id = i.id AND ip.package = ?)".into()
            );
            bound_params.push(Box::new(pkg.clone()));
        }
        if let Some(invocation_id) = q.invocation_id {
            where_clauses.push("i.id = ?".into());
            bound_params.push(Box::new(invocation_id));
        }
        if let Some(after_invocation_id) = q.after_invocation_id {
            where_clauses.push("i.id > ?".into());
            bound_params.push(Box::new(after_invocation_id));
        }
        if let Some(before_invocation_id) = q.before_invocation_id {
            where_clauses.push("i.id < ?".into());
            bound_params.push(Box::new(before_invocation_id));
        }
        if let Some(since_rfc3339) = &q.since_rfc3339 {
            where_clauses.push("datetime(i.started_at) >= datetime(?)".into());
            bound_params.push(Box::new(since_rfc3339.clone()));
        }

        let where_sql = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let sql = format!("SELECT COUNT(*) FROM invocations i{where_sql}");

        let param_refs: Vec<&dyn rusqlite::ToSql> = bound_params
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect();
        let count: i64 = conn.query_row(&sql, rusqlite::params_from_iter(param_refs), |row| {
            row.get(0)
        })?;
        Ok(count as usize)
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
            let aliases = status.db_aliases();
            let placeholders: Vec<&str> = aliases.iter().map(|_| "?").collect();
            where_clauses.push(format!("tr.status IN ({})", placeholders.join(",")));
            bound_params.extend(aliases.iter().map(|a| (*a).to_string()));
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
                status: parse_stored_test_status(status_str)?,
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
            let aliases = status.db_aliases();
            let placeholders: Vec<&str> = aliases.iter().map(|_| "?").collect();
            where_clauses.push(format!("tr.status IN ({})", placeholders.join(",")));
            bound_params.extend(aliases.iter().map(|a| (*a).to_string()));
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

        let sql = format!(
            "SELECT COUNT(*) FROM test_results tr \
            JOIN invocations i ON tr.invocation_id = i.id{where_sql}"
        );

        let mut stmt = conn.prepare(&sql)?;
        let count: usize = stmt
            .query_row(rusqlite::params_from_iter(bound_params.iter()), |row| {
                row.get(0)
            })?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    // Inline because these regressions exercise the private query execution path directly.
    use super::*;
    use crate::sandbox::prelude::*;
    use rusqlite::params;
    use tempfile::tempdir;

    #[sinex_test]
    async fn test_run_invocation_query_surfaces_invalid_started_at() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-query-invalid-started-at.db");
        let db = HistoryDb::open(&db_path)?;

        let id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)?;
        db.conn.execute(
            "UPDATE invocations SET started_at = ?1 WHERE id = ?2",
            params!["bad-query-started-at", id],
        )?;

        let error = InvocationQuery::new()
            .command("check")
            .run(&db)
            .expect_err("invalid started_at should surface from invocation queries");
        assert!(format!("{error:#}").contains("invalid invocation started_at"));
        Ok(())
    }

    #[sinex_test]
    async fn test_run_invocation_query_surfaces_invalid_finished_at() -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-query-invalid-finished-at.db");
        let db = HistoryDb::open(&db_path)?;

        let id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)?;
        db.conn.execute(
            "UPDATE invocations SET finished_at = ?1 WHERE id = ?2",
            params!["bad-query-finished-at", id],
        )?;

        let error = InvocationQuery::new()
            .command("check")
            .run(&db)
            .expect_err("invalid finished_at should surface from invocation queries");
        assert!(format!("{error:#}").contains("invalid invocation finished_at"));
        Ok(())
    }

    #[sinex_test]
    async fn test_invocation_query_supports_exact_and_bounded_scopes() -> TestResult<()> {
        let db = HistoryDb::open_in_memory()?;

        let oldest = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(oldest, InvocationStatus::Success, Some(0), 0.1)?;
        let middle = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(middle, InvocationStatus::Success, Some(0), 0.2)?;
        let newest = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(newest, InvocationStatus::Failed, Some(1), 0.3)?;

        let exact = InvocationQuery::new().for_invocation(middle).run(&db)?;
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].id, middle);

        let after_middle = InvocationQuery::new().after_invocation(middle).run(&db)?;
        assert_eq!(
            after_middle.iter().map(|inv| inv.id).collect::<Vec<_>>(),
            vec![newest]
        );

        let before_newest = InvocationQuery::new().before_invocation(newest).run(&db)?;
        assert_eq!(
            before_newest.iter().map(|inv| inv.id).collect::<Vec<_>>(),
            vec![middle, oldest]
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_invocation_query_offset_and_sort_controls() -> TestResult<()> {
        let db = HistoryDb::open_in_memory()?;

        let fast = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(fast, InvocationStatus::Success, Some(0), 0.1)?;
        let medium = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(medium, InvocationStatus::Success, Some(0), 0.5)?;
        let slow_fail = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(slow_fail, InvocationStatus::Failed, Some(1), 1.5)?;

        let duration_sorted = InvocationQuery::new().sort_duration().run(&db)?;
        assert_eq!(
            duration_sorted.iter().map(|inv| inv.id).collect::<Vec<_>>(),
            vec![slow_fail, medium, fast]
        );

        let paged = InvocationQuery::new()
            .limit(1)
            .offset(1)
            .sort_started()
            .run(&db)?;
        assert_eq!(paged.len(), 1);
        assert_eq!(paged[0].id, medium);

        Ok(())
    }
}
