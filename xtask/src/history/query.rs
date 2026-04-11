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
use serde::Serialize;
use time::OffsetDateTime;

use super::db::{
    DiagnosticCounts, HistoryDb, Invocation, InvocationStatus, StoredDiagnostic, row_to_invocation,
};
use super::tests::{TestResult, TestStatus, parse_stored_test_status};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VelocityView {
    Loop,
    Baseline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VelocityScopeKind {
    Workspace,
    Packages,
    Affected,
    Unknown,
}

#[derive(Debug, Clone)]
struct VelocityWorkload {
    identity: String,
    scope_label: Option<String>,
    baseline_candidate: bool,
}

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

    fn compute_global_test_pass_rate(&self) -> Result<Option<f64>> {
        let total = TestResultQuery::new().days(7).count(self.db)?;
        if total == 0 {
            return Ok(None);
        }

        let passed = TestResultQuery::new().passing().days(7).count(self.db)?;
        Ok(Some(passed as f64 / total as f64))
    }

    // ── Analytics subsystem ──────────────────────────────────────────────────

    /// Composite workspace health report (score 0-100).
    ///
    /// Score = build (50%) + test (30%) + velocity (20%).
    /// Build score = 100 - errors×10 - warnings×1, clamped.
    /// Test score = avg pass rate across packages (75 if no data).
    /// Velocity score = 75 adjusted by avg duration delta % (slower → lower).
    pub fn workspace_health_report(&self) -> Result<WorkspaceHealthReport> {
        let packages = self.all_packages_health()?;
        let counts = self.db.get_current_diagnostic_counts()?;
        let baseline_velocity = self.workspace_baseline_velocity_trends()?;
        Ok(self.build_workspace_health_report(packages, counts, &baseline_velocity))
    }

    pub fn analytics_snapshot(
        &self,
    ) -> Result<(
        WorkspaceHealthReport,
        Vec<VelocityTrend>,
        Vec<VelocityTrend>,
        Vec<Recommendation>,
    )> {
        let packages = self.all_packages_health()?;
        let counts = self.db.get_current_diagnostic_counts()?;
        let loop_velocity = self.loop_velocity_trends()?;
        let baseline_velocity = self.workspace_baseline_velocity_trends()?;
        let health = self.build_workspace_health_report(packages, counts, &baseline_velocity);
        let recommendations =
            self.build_recommendations(&health, &baseline_velocity, &loop_velocity)?;
        Ok((health, loop_velocity, baseline_velocity, recommendations))
    }

    pub fn status_summary_snapshot(
        &self,
    ) -> Result<(
        WorkspaceHealthReport,
        Vec<VelocityTrend>,
        Vec<VelocityTrend>,
        Vec<Recommendation>,
    )> {
        let counts = self.db.get_current_diagnostic_counts()?;
        let loop_velocity = self.loop_velocity_trends()?;
        let baseline_velocity = self.workspace_baseline_velocity_trends()?;
        let avg_test_pass_rate = self.compute_global_test_pass_rate()?;
        let health = self.build_workspace_health_report_from_scalars(
            counts,
            &baseline_velocity,
            avg_test_pass_rate,
            0,
            0,
            Vec::new(),
        );
        let recommendations =
            self.build_status_recommendations(&health, &baseline_velocity, &loop_velocity);
        Ok((health, loop_velocity, baseline_velocity, recommendations))
    }

    fn build_workspace_health_report(
        &self,
        packages: Vec<PackageHealth>,
        counts: DiagnosticCounts,
        velocity_trends: &[VelocityTrend],
    ) -> WorkspaceHealthReport {
        let packages_with_tests: Vec<_> = packages
            .iter()
            .filter(|p| p.test_pass_rate.is_some())
            .collect();
        let avg_test_pass_rate = if packages_with_tests.is_empty() {
            None
        } else {
            Some(
                packages_with_tests
                    .iter()
                    .filter_map(|p| p.test_pass_rate)
                    .sum::<f64>()
                    / packages_with_tests.len() as f64,
            )
        };

        self.build_workspace_health_report_from_scalars(
            counts,
            velocity_trends,
            avg_test_pass_rate,
            packages.iter().filter(|p| p.diagnostic_count > 0).count(),
            packages_with_tests.len(),
            packages,
        )
    }

    fn build_workspace_health_report_from_scalars(
        &self,
        counts: DiagnosticCounts,
        velocity_trends: &[VelocityTrend],
        avg_test_pass_rate: Option<f64>,
        packages_with_errors: usize,
        test_packages: usize,
        packages: Vec<PackageHealth>,
    ) -> WorkspaceHealthReport {
        let error_count = counts.errors;
        let warning_count = counts.warnings;
        let fixable_count = counts.fixable;

        let build_score = ((100i32 - (error_count as i32 * 10) - (warning_count as i32 / 5))
            .clamp(0, 100)) as u32;
        let test_score = avg_test_pass_rate
            .map(|avg| (avg * 100.0).round() as u32)
            .unwrap_or(75);
        let velocity_score = Self::compute_velocity_score(velocity_trends);
        let score =
            (build_score as f64 * 0.5 + test_score as f64 * 0.3 + velocity_score as f64 * 0.2)
                .round() as u32;

        WorkspaceHealthReport {
            score,
            build_score,
            test_score,
            velocity_score,
            error_count,
            warning_count,
            fixable_count,
            packages_with_errors,
            test_packages,
            avg_test_pass_rate,
            packages,
        }
    }

    fn compute_velocity_score(velocity_trends: &[VelocityTrend]) -> u32 {
        if velocity_trends.is_empty() {
            return 75;
        }

        let measurable: Vec<f64> = velocity_trends.iter().filter_map(|v| v.delta_pct).collect();
        if measurable.is_empty() {
            return 75;
        }

        let avg_delta = measurable.iter().sum::<f64>() / measurable.len() as f64;
        ((75.0 - avg_delta * 0.5).clamp(0.0, 100.0)) as u32
    }

    fn parse_invocation_args(args_json: Option<&str>) -> Vec<String> {
        args_json
            .and_then(|json| serde_json::from_str::<Vec<String>>(json).ok())
            .unwrap_or_default()
    }

    fn has_flag(args: &[String], flag: &str) -> bool {
        args.iter().any(|arg| arg == flag)
    }

    fn has_prefix(args: &[String], prefix: &str) -> bool {
        args.iter().any(|arg| arg.starts_with(prefix))
    }

    fn compact_packages(prefix: Option<&str>, packages: &[String]) -> String {
        match packages {
            [] => prefix.unwrap_or("scope").to_string(),
            [single] if prefix.is_none() => format!("-p {single}"),
            [single] => format!("{} {single}", prefix.unwrap_or("scope")),
            [first, second] if prefix.is_none() => format!("{first},{second}"),
            [first, second] => format!("{} {first},{second}", prefix.unwrap_or("scope")),
            many if prefix.is_none() => format!("packages {}", many.len()),
            many => format!("{} {} pkgs", prefix.unwrap_or("scope"), many.len()),
        }
    }

    fn extract_package_flags(args: &[String]) -> Vec<String> {
        let mut packages = Vec::new();
        let mut iter = args.iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "-p" | "--package" => {
                    if let Some(package) = iter.next() {
                        packages.push(package.clone());
                    }
                }
                _ => {
                    if let Some(package) = arg.strip_prefix("--package=") {
                        packages.push(package.to_string());
                    }
                }
            }
        }
        packages.sort();
        packages.dedup();
        packages
    }

    fn parse_scope_marker(marker: &str) -> (VelocityScopeKind, String) {
        fn parse_packages(raw: &str) -> Vec<String> {
            raw.split(',')
                .filter(|entry| !entry.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        }

        let raw = marker.strip_prefix("--scope=").unwrap_or(marker);
        if raw == "workspace" {
            (VelocityScopeKind::Workspace, "workspace".to_string())
        } else if let Some(packages) = raw.strip_prefix("packages:") {
            (
                VelocityScopeKind::Packages,
                Self::compact_packages(None, &parse_packages(packages)),
            )
        } else if let Some(packages) = raw.strip_prefix("affected:") {
            (
                VelocityScopeKind::Affected,
                Self::compact_packages(Some("affected"), &parse_packages(packages)),
            )
        } else {
            (VelocityScopeKind::Unknown, raw.to_string())
        }
    }

    fn legacy_scope_from_args(args: &[String]) -> Option<(VelocityScopeKind, String, String)> {
        if Self::has_flag(args, "--workspace") || Self::has_flag(args, "--all") {
            return Some((
                VelocityScopeKind::Workspace,
                "workspace".to_string(),
                "--scope=workspace".to_string(),
            ));
        }

        let packages = Self::extract_package_flags(args);
        if packages.is_empty() {
            return None;
        }

        Some((
            VelocityScopeKind::Packages,
            Self::compact_packages(None, &packages),
            format!("--scope=packages:{}", packages.join(",")),
        ))
    }

    fn fallback_scope_from_history(
        &self,
        command: &str,
        invocation_id: i64,
    ) -> Result<Option<(VelocityScopeKind, String, String)>> {
        if command != "test" {
            return Ok(None);
        }

        let mut stmt = self.db.conn.prepare(
            "SELECT DISTINCT package FROM test_results WHERE invocation_id = ?1 ORDER BY package",
        )?;
        let packages: Vec<String> = stmt
            .query_map(rusqlite::params![invocation_id], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        if packages.is_empty() {
            return Ok(None);
        }

        Ok(Some((
            VelocityScopeKind::Packages,
            Self::compact_packages(None, &packages),
            format!("--scope=packages:{}", packages.join(",")),
        )))
    }

    fn velocity_mode_tokens(command: &str, args: &[String]) -> Vec<String> {
        let mut tokens = Vec::new();
        match command {
            "build" => {
                if Self::has_flag(args, "--release") {
                    tokens.push("release".to_string());
                }
            }
            "check" => {
                for flag in [
                    "--fix",
                    "--full",
                    "--lint",
                    "--fmt",
                    "--forbidden",
                    "--nix",
                    "--heavy",
                    "--skip-tests",
                ] {
                    if Self::has_flag(args, flag) {
                        tokens.push(flag.trim_start_matches("--").to_string());
                    }
                }
            }
            "test" => {
                for flag in [
                    "--debug",
                    "--fail-fast",
                    "--heavy",
                    "--include-ignored",
                    "--update-snapshots",
                ] {
                    if Self::has_flag(args, flag) {
                        tokens.push(flag.trim_start_matches("--").to_string());
                    }
                }
                for prefix in ["--filter=", "--threads=", "--retries=", "--timeout="] {
                    if let Some(value) = args.iter().find(|arg| arg.starts_with(prefix)) {
                        tokens.push(value.clone());
                    }
                }
            }
            _ => {}
        }
        tokens
    }

    fn velocity_workload(
        &self,
        command: &str,
        invocation: &Invocation,
    ) -> Result<VelocityWorkload> {
        let args = Self::parse_invocation_args(invocation.args_json.as_deref());
        let semantic_scope_marker = args.iter().find(|arg| arg.starts_with("--scope=")).cloned();
        let (scope_kind, scope_label, canonical_scope_marker) =
            match semantic_scope_marker.as_deref() {
                Some(marker) => {
                    let (kind, label) = Self::parse_scope_marker(marker);
                    (kind, Some(label), Some(marker.to_string()))
                }
                None => {
                    if let Some((kind, label, marker)) = Self::legacy_scope_from_args(&args) {
                        (kind, Some(label), Some(marker))
                    } else if let Some((kind, label, marker)) =
                        self.fallback_scope_from_history(command, invocation.id)?
                    {
                        (kind, Some(label), Some(marker))
                    } else {
                        (VelocityScopeKind::Unknown, None, None)
                    }
                }
            };

        let mut identity_tokens = Self::velocity_mode_tokens(command, &args);
        if let Some(subcommand) = invocation.subcommand.as_deref() {
            identity_tokens.push(format!("subcommand={subcommand}"));
        }
        if let Some(marker) = canonical_scope_marker {
            identity_tokens.push(marker);
        }
        if identity_tokens.is_empty() {
            identity_tokens.push("default".to_string());
        }
        identity_tokens.sort();

        let mut label_parts = Vec::new();
        if let Some(subcommand) = invocation.subcommand.as_deref() {
            label_parts.push(subcommand.to_string());
        }
        if let Some(scope) = scope_label.clone() {
            label_parts.push(scope);
        }
        let mode_tokens = Self::velocity_mode_tokens(command, &args);
        if !mode_tokens.is_empty() {
            label_parts.push(format!("+{}", mode_tokens.join("+")));
        }

        let baseline_candidate = match command {
            "build" => scope_kind == VelocityScopeKind::Workspace,
            "check" => {
                scope_kind == VelocityScopeKind::Workspace && !Self::has_flag(&args, "--fix")
            }
            "test" => {
                scope_kind == VelocityScopeKind::Workspace
                    && invocation.subcommand.is_none()
                    && !Self::has_flag(&args, "--debug")
                    && !Self::has_flag(&args, "--fail-fast")
                    && !Self::has_flag(&args, "--heavy")
                    && !Self::has_flag(&args, "--include-ignored")
                    && !Self::has_flag(&args, "--update-snapshots")
                    && !Self::has_prefix(&args, "--filter=")
                    && !Self::has_prefix(&args, "--threads=")
                    && !Self::has_prefix(&args, "--retries=")
                    && !Self::has_prefix(&args, "--timeout=")
            }
            _ => false,
        };

        Ok(VelocityWorkload {
            identity: identity_tokens.join("|"),
            scope_label: (!label_parts.is_empty()).then(|| label_parts.join(" ")),
            baseline_candidate,
        })
    }

    fn velocity_trends_for(&self, view: VelocityView) -> Result<Vec<VelocityTrend>> {
        let mut trends = Vec::new();
        for command in ["check", "test", "build"] {
            let invocations = InvocationQuery::new()
                .command(command)
                .succeeded()
                .days(14)
                .limit(60)
                .run(self.db)?;

            let mut scopes: Vec<(String, Option<String>, Vec<f64>)> = Vec::new();
            for invocation in &invocations {
                let Some(duration) = invocation.duration_secs else {
                    continue;
                };
                let workload = self.velocity_workload(command, invocation)?;
                if view == VelocityView::Baseline && !workload.baseline_candidate {
                    continue;
                }

                if let Some((_, _, durations)) = scopes
                    .iter_mut()
                    .find(|(identity, _, _)| *identity == workload.identity)
                {
                    durations.push(duration);
                } else {
                    scopes.push((workload.identity, workload.scope_label, vec![duration]));
                }
            }

            let fallback_scope = scopes.first();
            let comparable_scope = scopes.iter().find(|(_, _, durations)| durations.len() >= 4);

            let Some((_, scope_label, durations)) = comparable_scope.or(fallback_scope) else {
                trends.push(VelocityTrend {
                    command: command.to_string(),
                    scope_label: None,
                    recent_avg_secs: None,
                    older_avg_secs: None,
                    delta_pct: None,
                    trend: "no_data".to_string(),
                    sample_count: 0,
                });
                continue;
            };

            if durations.len() < 4 {
                trends.push(VelocityTrend {
                    command: command.to_string(),
                    scope_label: scope_label.clone(),
                    recent_avg_secs: durations.first().copied(),
                    older_avg_secs: None,
                    delta_pct: None,
                    trend: "no_data".to_string(),
                    sample_count: durations.len(),
                });
                continue;
            }

            let mid = durations.len() / 2;
            let recent_avg = durations[..mid].iter().sum::<f64>() / mid as f64;
            let older_avg = durations[mid..].iter().sum::<f64>() / (durations.len() - mid) as f64;
            let delta_pct = if older_avg.abs() < f64::EPSILON {
                0.0
            } else {
                ((recent_avg - older_avg) / older_avg) * 100.0
            };

            let trend = if delta_pct.abs() < 5.0 {
                "stable"
            } else if delta_pct < 0.0 {
                "faster"
            } else {
                "slower"
            };

            trends.push(VelocityTrend {
                command: command.to_string(),
                scope_label: scope_label.clone(),
                recent_avg_secs: Some(recent_avg),
                older_avg_secs: Some(older_avg),
                delta_pct: Some(delta_pct),
                trend: trend.to_string(),
                sample_count: durations.len(),
            });
        }
        Ok(trends)
    }

    pub fn loop_velocity_trends(&self) -> Result<Vec<VelocityTrend>> {
        self.velocity_trends_for(VelocityView::Loop)
    }

    /// J2: Diagnostic hotspots — most active (churning) diagnostics.
    ///
    /// Uses lifecycle classification: chronic and recurring diagnostics are
    /// ordered by occurrence count to surface the most persistent issues.
    pub fn diagnostic_hotspots(&self, limit: usize) -> Result<Vec<DiagnosticHotspot>> {
        use super::db::LifecycleStatus;
        let lifecycle = self
            .db
            .get_diagnostic_lifecycle(None, None, None, None, limit * 3)?;
        let mut hotspots: Vec<DiagnosticHotspot> = lifecycle
            .into_iter()
            .filter(|d| {
                matches!(
                    d.status,
                    LifecycleStatus::Chronic | LifecycleStatus::Recurring
                ) || (d.status == LifecycleStatus::New && d.occurrence_count > 1)
            })
            .map(|d| DiagnosticHotspot {
                package: d.package,
                level: d.level,
                code: d.code,
                message: d.message,
                occurrences: d.occurrence_count,
                status: match d.status {
                    LifecycleStatus::New => "new".to_string(),
                    LifecycleStatus::Chronic => "chronic".to_string(),
                    LifecycleStatus::Recurring => "recurring".to_string(),
                    LifecycleStatus::Resolved => "resolved".to_string(),
                },
            })
            .collect();
        hotspots.sort_by(|a, b| b.occurrences.cmp(&a.occurrences));
        hotspots.truncate(limit);
        Ok(hotspots)
    }

    /// J3: Per-package test reliability (pass rate + flakiness, 7d vs 30d trend).
    pub fn package_reliability(&self, limit: usize) -> Result<Vec<PackageReliability>> {
        let packages = self.db.get_known_packages()?;
        let flaky_tests = self.db.get_flaky_tests(200)?;

        let mut results = Vec::new();
        for pkg in packages.iter() {
            let total_7d = TestResultQuery::new().package(pkg).days(7).count(self.db)?;
            if total_7d == 0 {
                continue;
            }
            let passed_7d = TestResultQuery::new()
                .package(pkg)
                .passing()
                .days(7)
                .count(self.db)?;
            let total_30d = TestResultQuery::new()
                .package(pkg)
                .days(30)
                .count(self.db)?;
            let passed_30d = TestResultQuery::new()
                .package(pkg)
                .passing()
                .days(30)
                .count(self.db)?;

            let pass_rate_7d = passed_7d as f64 / total_7d as f64;
            let pass_rate_30d = if total_30d > 0 {
                passed_30d as f64 / total_30d as f64
            } else {
                pass_rate_7d
            };

            let trend = if (pass_rate_7d - pass_rate_30d).abs() < 0.02 {
                "stable"
            } else if pass_rate_7d > pass_rate_30d {
                "improving"
            } else {
                "degrading"
            };

            let flaky_count = flaky_tests
                .iter()
                .filter(|(_test, package, _)| package == pkg)
                .count();

            results.push(PackageReliability {
                package: pkg.clone(),
                pass_rate: pass_rate_7d,
                total_runs: total_7d,
                flaky_count,
                trend: trend.to_string(),
            });
        }

        results.sort_by(|a, b| {
            a.pass_rate
                .partial_cmp(&b.pass_rate)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);
        Ok(results)
    }

    pub fn velocity_trends(&self) -> Result<Vec<VelocityTrend>> {
        self.loop_velocity_trends()
    }

    pub fn workspace_baseline_velocity_trends(&self) -> Result<Vec<VelocityTrend>> {
        self.velocity_trends_for(VelocityView::Baseline)
    }

    /// J5: Actionable heuristic recommendations derived from J1-J4 data.
    ///
    /// Each recommendation includes the exact `xtask` command to run next.
    /// Sorted: critical → warning → info.
    pub fn recommendations(&self) -> Result<Vec<Recommendation>> {
        let (_, _, _, recommendations) = self.analytics_snapshot()?;
        Ok(recommendations)
    }

    fn build_recommendations(
        &self,
        health: &WorkspaceHealthReport,
        baseline_velocity: &[VelocityTrend],
        loop_velocity: &[VelocityTrend],
    ) -> Result<Vec<Recommendation>> {
        let mut recs = Vec::new();

        if health.error_count > 0 {
            recs.push(Recommendation {
                severity: "critical".to_string(),
                category: "build".to_string(),
                description: format!(
                    "{} compiler error(s) in current workspace",
                    health.error_count
                ),
                action: "xtask check --lint".to_string(),
            });
        }

        if health.fixable_count > 0 {
            recs.push(Recommendation {
                severity: "warning".to_string(),
                category: "build".to_string(),
                description: format!("{} diagnostic(s) can be auto-fixed", health.fixable_count),
                action: "xtask fix --smart".to_string(),
            });
        }

        let failing_pkgs: Vec<_> = health
            .packages
            .iter()
            .filter(|p| p.test_pass_rate.map(|r| r < 0.9).unwrap_or(false))
            .collect();

        let packages_with_tests = health
            .packages
            .iter()
            .filter(|p| p.test_pass_rate.is_some())
            .count();

        // Consolidate when most packages are failing — individual per-package
        // recommendations become noise; a single "run full suite" is more actionable.
        let consolidation_threshold = (packages_with_tests / 2).max(3);
        if failing_pkgs.len() >= consolidation_threshold && failing_pkgs.len() > 1 {
            let worst_rate = failing_pkgs
                .iter()
                .filter_map(|p| p.test_pass_rate)
                .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or(0.0);
            recs.push(Recommendation {
                severity: "critical".to_string(),
                category: "tests".to_string(),
                description: format!(
                    "{}/{} packages below 90% pass rate (worst: {:.0}%)",
                    failing_pkgs.len(),
                    packages_with_tests,
                    worst_rate * 100.0
                ),
                action: "xtask test --debug".to_string(),
            });
        } else {
            for pkg in &failing_pkgs {
                recs.push(Recommendation {
                    severity: if pkg.test_pass_rate.unwrap_or(1.0) < 0.7 {
                        "critical".to_string()
                    } else {
                        "warning".to_string()
                    },
                    category: "tests".to_string(),
                    description: format!(
                        "{}: {:.0}% pass rate (last 7 days)",
                        pkg.package,
                        pkg.test_pass_rate.unwrap_or(0.0) * 100.0
                    ),
                    action: format!("xtask test -p {} --debug", pkg.package),
                });
            }
        }

        let flaky = self.db.get_flaky_tests(5)?;
        if !flaky.is_empty() {
            recs.push(Recommendation {
                severity: "warning".to_string(),
                category: "tests".to_string(),
                description: format!("{} flaky test(s) detected", flaky.len()),
                action: "xtask history tests flaky".to_string(),
            });
        }

        for trend in baseline_velocity
            .iter()
            .filter(|v| v.trend == "slower" && v.delta_pct.unwrap_or(0.0) > 20.0)
        {
            recs.push(Recommendation {
                severity: "warning".to_string(),
                category: "performance".to_string(),
                description: format!(
                    "workspace `{}` is {:.0}% slower than the prior week",
                    trend.display_label(),
                    trend.delta_pct.unwrap_or(0.0)
                ),
                action: "xtask history timeline".to_string(),
            });
        }

        for trend in loop_velocity
            .iter()
            .filter(|v| v.trend == "slower" && v.delta_pct.unwrap_or(0.0) > 20.0)
        {
            recs.push(Recommendation {
                severity: "info".to_string(),
                category: "performance".to_string(),
                description: format!(
                    "recent loop `{}` is {:.0}% slower than its prior samples",
                    trend.display_label(),
                    trend.delta_pct.unwrap_or(0.0)
                ),
                action: "xtask analytics velocity".to_string(),
            });
        }

        let hotspots = self.diagnostic_hotspots(10)?;
        let chronic_count = hotspots.iter().filter(|h| h.status == "chronic").count();
        if chronic_count > 0 {
            recs.push(Recommendation {
                severity: "info".to_string(),
                category: "build".to_string(),
                description: format!(
                    "{} chronic diagnostic(s) have persisted across 3+ builds",
                    chronic_count
                ),
                action: "xtask history diagnostics --lifecycle --lifecycle-status chronic"
                    .to_string(),
            });
        }

        if recs.is_empty() {
            recs.push(Recommendation {
                severity: "info".to_string(),
                category: "general".to_string(),
                description: "Workspace health looks good — no critical issues detected"
                    .to_string(),
                action: "xtask history view workspace-timeline".to_string(),
            });
        }

        recs.sort_by_key(|r| match r.severity.as_str() {
            "critical" => 0u8,
            "warning" => 1,
            _ => 2,
        });
        Ok(recs)
    }

    fn build_status_recommendations(
        &self,
        health: &WorkspaceHealthReport,
        baseline_velocity: &[VelocityTrend],
        loop_velocity: &[VelocityTrend],
    ) -> Vec<Recommendation> {
        let mut recs = Vec::new();

        if health.error_count > 0 {
            recs.push(Recommendation {
                severity: "critical".to_string(),
                category: "build".to_string(),
                description: format!(
                    "{} compiler error(s) in current workspace",
                    health.error_count
                ),
                action: "xtask check --lint".to_string(),
            });
        }

        if health.fixable_count > 0 {
            recs.push(Recommendation {
                severity: "warning".to_string(),
                category: "build".to_string(),
                description: format!("{} diagnostic(s) can be auto-fixed", health.fixable_count),
                action: "xtask fix --smart".to_string(),
            });
        }

        if let Some(pass_rate) = health.avg_test_pass_rate
            && pass_rate < 0.9
        {
            recs.push(Recommendation {
                severity: if pass_rate < 0.75 {
                    "critical".to_string()
                } else {
                    "warning".to_string()
                },
                category: "tests".to_string(),
                description: format!(
                    "Workspace tests are {:.1}% green over the last 7 days",
                    pass_rate * 100.0
                ),
                action: "xtask history tests analyze".to_string(),
            });
        }

        for trend in baseline_velocity
            .iter()
            .filter(|v| v.trend == "slower" && v.delta_pct.unwrap_or(0.0) > 20.0)
        {
            recs.push(Recommendation {
                severity: "warning".to_string(),
                category: "performance".to_string(),
                description: format!(
                    "workspace `{}` is {:.0}% slower than the prior week",
                    trend.display_label(),
                    trend.delta_pct.unwrap_or(0.0)
                ),
                action: "xtask history timeline".to_string(),
            });
        }

        for trend in loop_velocity
            .iter()
            .filter(|v| v.trend == "slower" && v.delta_pct.unwrap_or(0.0) > 20.0)
        {
            recs.push(Recommendation {
                severity: "warning".to_string(),
                category: "performance".to_string(),
                description: format!(
                    "recent loop `{}` is {:.0}% slower than its prior samples",
                    trend.display_label(),
                    trend.delta_pct.unwrap_or(0.0)
                ),
                action: "xtask analytics velocity".to_string(),
            });
        }

        recs.sort_by_key(|r| match r.severity.as_str() {
            "critical" => 0u8,
            "warning" => 1,
            _ => 2,
        });
        recs
    }
}

// ─── Analytics output types ──────────────────────────────────────────────────

/// Composite workspace health report.
#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceHealthReport {
    pub score: u32,
    pub build_score: u32,
    pub test_score: u32,
    pub velocity_score: u32,
    pub error_count: usize,
    pub warning_count: usize,
    pub fixable_count: usize,
    pub packages_with_errors: usize,
    pub test_packages: usize,
    pub avg_test_pass_rate: Option<f64>,
    pub packages: Vec<PackageHealth>,
}

/// A diagnostic that actively churns across invocations (J2).
#[derive(Debug, Clone, Serialize)]
pub struct DiagnosticHotspot {
    pub package: Option<String>,
    pub level: String,
    pub code: Option<String>,
    pub message: String,
    pub occurrences: usize,
    pub status: String,
}

/// Test reliability summary for one package (J3).
#[derive(Debug, Clone, Serialize)]
pub struct PackageReliability {
    pub package: String,
    pub pass_rate: f64,
    pub total_runs: usize,
    pub flaky_count: usize,
    pub trend: String,
}

/// Build/test time trend for one command (J4).
#[derive(Debug, Clone, Serialize)]
pub struct VelocityTrend {
    pub command: String,
    pub scope_label: Option<String>,
    pub recent_avg_secs: Option<f64>,
    pub older_avg_secs: Option<f64>,
    /// Positive = slower, negative = faster (%)
    pub delta_pct: Option<f64>,
    pub trend: String,
    pub sample_count: usize,
}

impl VelocityTrend {
    #[must_use]
    pub fn display_label(&self) -> String {
        match self.scope_label.as_deref() {
            Some(scope) if !scope.is_empty() => format!("{} [{}]", self.command, scope),
            _ => self.command.clone(),
        }
    }
}

/// An actionable recommendation with the exact command to run (J5).
#[derive(Debug, Clone, Serialize)]
pub struct Recommendation {
    pub severity: String,
    pub category: String,
    pub description: String,
    pub action: String,
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
        let rows = stmt.query_map(
            rusqlite::params_from_iter(bound_params.iter()),
            row_to_invocation,
        )?;

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
    use crate::history::{TestResult, TestStatus};
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
    async fn test_status_summary_snapshot_uses_global_test_rate_without_package_fanout()
    -> TestResult<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test-status-summary-snapshot.db");
        let db = HistoryDb::open(&db_path)?;

        let check_id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(check_id, InvocationStatus::Success, Some(0), 1.0)?;

        let test_id = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(test_id, InvocationStatus::Success, Some(0), 2.0)?;
        db.store_test_results(
            test_id,
            &[TestResult {
                test_name: "status_summary_smoke".into(),
                package: "sinex-status".into(),
                status: TestStatus::Pass,
                duration_secs: Some(0.2),
                attempt: 1,
                output: None,
            }],
        )?;

        let analysis = HistoryAnalysis::new(&db);
        let (health, _loop_velocity, _baseline_velocity, recommendations) =
            analysis.status_summary_snapshot()?;

        assert!(health.packages.is_empty());
        assert_eq!(health.avg_test_pass_rate, Some(1.0));
        assert!(health.score > 0);
        assert!(recommendations.is_empty());
        Ok(())
    }
}
