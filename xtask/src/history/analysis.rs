//! Cross-dimensional history analytics and recommendations.

use color_eyre::eyre::Result;
use serde::Serialize;
use time::OffsetDateTime;

use super::db::{DiagnosticCounts, HistoryDb, Invocation, LifecycleStatus};
use super::query::{DiagnosticQuery, InvocationQuery, TestResultQuery};

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
/// # use xtask::history::{HistoryAnalysis, HistoryDb};
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
        results.sort_by_key(|p| std::cmp::Reverse(p.diagnostic_count));
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

    pub fn analytics_snapshot(&self) -> Result<AnalyticsSnapshot> {
        let packages = self.all_packages_health()?;
        let counts = self.db.get_current_diagnostic_counts()?;
        let loop_velocity = self.loop_velocity_trends()?;
        let baseline_velocity = self.workspace_baseline_velocity_trends()?;
        let health = self.build_workspace_health_report(packages, counts, &baseline_velocity);
        let recommendations =
            self.build_recommendations(&health, &baseline_velocity, &loop_velocity)?;
        Ok(AnalyticsSnapshot {
            health,
            loop_velocity,
            baseline_velocity,
            recommendations,
        })
    }

    pub fn status_summary_snapshot(&self) -> Result<AnalyticsSnapshot> {
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
        Ok(AnalyticsSnapshot {
            health,
            loop_velocity,
            baseline_velocity,
            recommendations,
        })
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

    #[allow(
        clippy::needless_pass_by_value,
        reason = "counts fields extracted immediately"
    )]
    fn build_workspace_health_report_from_scalars(
        &self,
        counts: DiagnosticCounts,
        velocity_trends: &[VelocityTrend],
        avg_test_pass_rate: Option<f64>,
        packages_with_errors: usize,
        test_packages: usize,
        #[allow(clippy::needless_pass_by_value)] packages: Vec<PackageHealth>,
    ) -> WorkspaceHealthReport {
        let error_count = counts.errors;
        let warning_count = counts.warnings;
        let fixable_count = counts.fixable;

        let build_score = ((100i32 - (error_count as i32 * 10) - (warning_count as i32 / 5))
            .clamp(0, 100)) as u32;
        let test_score = avg_test_pass_rate.map_or(75, |avg| (avg * 100.0).round() as u32);
        let velocity_score = Self::compute_velocity_score(velocity_trends);
        let score = (f64::from(build_score) * 0.5
            + f64::from(test_score) * 0.3
            + f64::from(velocity_score) * 0.2)
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
                scope_kind == VelocityScopeKind::Workspace
                    && !Self::has_flag(&args, "--fix")
                    && !Self::has_flag(&args, "--lint")
                    && !Self::has_flag(&args, "--fmt")
                    && !Self::has_flag(&args, "--forbidden")
                    && !Self::has_flag(&args, "--nix")
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

            // Prefer scoped workloads (non-None scope_label) over workspace-wide ones so that
            // per-package velocity wins when the history contains both.
            let fallback_scope = scopes
                .iter()
                .find(|(_, label, _)| label.is_some())
                .or_else(|| scopes.first());
            let comparable_scope = scopes
                .iter()
                .find(|(_, label, durations)| label.is_some() && durations.len() >= 4)
                .or_else(|| scopes.iter().find(|(_, _, durations)| durations.len() >= 4));

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
        hotspots.sort_by_key(|h| std::cmp::Reverse(h.occurrences));
        hotspots.truncate(limit);
        Ok(hotspots)
    }

    /// J3: Per-package test reliability (pass rate + flakiness, 7d vs 30d trend).
    pub fn package_reliability(&self, limit: usize) -> Result<Vec<PackageReliability>> {
        let packages = self.db.get_known_packages()?;
        let flaky_tests = self.db.get_flaky_tests(200)?;

        let mut results = Vec::new();
        for pkg in &packages {
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
        Ok(self.analytics_snapshot()?.recommendations)
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
            .filter(|p| p.test_pass_rate.is_some_and(|r| r < 0.9))
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
                    "{chronic_count} chronic diagnostic(s) have persisted across 3+ builds"
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

/// Snapshot of workspace analytics: health, recent/baseline velocity, and derived recommendations.
///
/// Returned by `HistoryAnalysis::analytics_snapshot` and `status_summary_snapshot`. The two
/// methods compute the same shape via different paths: `analytics_snapshot` is the full fan-out
/// that walks every package; `status_summary_snapshot` is the fast path used by `xtask status`.
#[derive(Debug, Clone, Serialize)]
pub struct AnalyticsSnapshot {
    pub health: WorkspaceHealthReport,
    pub loop_velocity: Vec<VelocityTrend>,
    pub baseline_velocity: Vec<VelocityTrend>,
    pub recommendations: Vec<Recommendation>,
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{InvocationStatus, TestResult, TestStatus};
    use crate::sandbox::prelude::*;
    use tempfile::tempdir;

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
        let snapshot = analysis.status_summary_snapshot()?;

        assert!(snapshot.health.packages.is_empty());
        assert_eq!(snapshot.health.avg_test_pass_rate, Some(1.0));
        assert!(snapshot.health.score > 0);
        assert!(snapshot.recommendations.is_empty());
        Ok(())
    }
}
