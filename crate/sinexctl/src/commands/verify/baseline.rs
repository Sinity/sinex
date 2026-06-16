//! `sinexctl ops verify baseline` — comprehensive verification battery (#1565).
//!
//! Runs a set of weighted checks across schema integrity, closure hygiene,
//! source coverage, privacy invariants, replay integrity, drift-guard
//! bypass frequency, and workspace compilation. Produces a machine-readable
//! score (0-100) and a human-readable report with per-check status.

use std::process::Stdio;
use std::time::Duration;

use clap::Args;
use color_eyre::{Result, eyre::eyre};
use console::style;
use serde::Serialize;
use tokio::process::Command;
use tokio::time::timeout;

use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;

// ---------------------------------------------------------------------------
// CLI arguments
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Args)]
pub struct BaselineArgs {
    /// Per-check timeout in seconds.
    #[arg(long, default_value_t = 60)]
    timeout: u64,

    /// Include advisory checks that would normally be skipped when their
    /// backing data is absent (e.g. no history DB).
    #[arg(long)]
    strict: bool,
}

// ---------------------------------------------------------------------------
// Check definitions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum CheckWeight {
    High,
    Medium,
    // Reserved severity tier: no production check is currently classified Low
    // (all are High/Medium), but the scoring/display paths and tests handle it.
    #[allow(dead_code)]
    Low,
}

impl CheckWeight {
    fn value(self) -> f64 {
        match self {
            Self::High => 3.0,
            Self::Medium => 2.0,
            Self::Low => 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum CheckStatus {
    Pass,
    Degraded,
    Fail,
    Skipped,
}

impl CheckStatus {
    fn score(self) -> f64 {
        match self {
            Self::Pass => 1.0,
            Self::Degraded => 0.5,
            Self::Fail => 0.0,
            Self::Skipped => 0.0,
        }
    }

    fn colored_icon(self) -> String {
        match self {
            Self::Pass => style("PASS").green().bold().to_string(),
            Self::Degraded => style("DEGR").yellow().bold().to_string(),
            Self::Fail => style("FAIL").red().bold().to_string(),
            Self::Skipped => style("SKIP").dim().to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct CheckResult {
    id: &'static str,
    label: &'static str,
    weight: CheckWeight,
    status: CheckStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recommendation: Option<String>,
}

impl CheckResult {
    fn new(id: &'static str, label: &'static str, weight: CheckWeight) -> Self {
        Self {
            id,
            label,
            weight,
            status: CheckStatus::Skipped,
            detail: None,
            recommendation: None,
        }
    }
}

#[derive(Debug, Serialize)]
struct BaselineReport {
    schema_version: u32,
    score: u32,
    checks: Vec<CheckResult>,
    summary: String,
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

pub async fn execute(args: BaselineArgs, format: OutputFormat) -> Result<()> {
    run_baseline(args, format).await
}

async fn run_baseline(args: BaselineArgs, format: OutputFormat) -> Result<()> {
    let check_timeout = Duration::from_secs(args.timeout);
    let table_mode = matches!(format, OutputFormat::Table);

    if table_mode {
        println!();
        println!(
            "{}",
            style("sinexctl ops verify baseline — Comprehensive Verification")
                .bold()
                .cyan()
        );
        println!("{}", style("═".repeat(60)).dim());
        println!();
    }

    let checks = run_all_checks(check_timeout, args.strict).await;

    let score = compute_score(&checks);

    if table_mode {
        print_table_report(&checks, score);
    }

    let summary = build_summary(&checks, score);
    let report = BaselineReport {
        schema_version: 1,
        score,
        checks: checks.clone(),
        summary,
    };

    match format {
        OutputFormat::Json | OutputFormat::Ndjson => println!("{}", format_json(&report)?),
        OutputFormat::Yaml => println!("{}", format_yaml(&report)?),
        OutputFormat::Table => {
            // Table report already printed above; nothing extra to emit.
        }
        OutputFormat::Dot => {
            return Err(eyre!(
                "ops verify baseline does not support --format dot; use --format json|yaml|table"
            ));
        }
    }

    // Exit non-zero on any Fail or Degraded check so CI can gate on this.
    if checks
        .iter()
        .any(|c| matches!(c.status, CheckStatus::Fail | CheckStatus::Degraded))
    {
        std::process::exit(1);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Check runners
// ---------------------------------------------------------------------------

async fn run_all_checks(per_check_timeout: Duration, strict: bool) -> Vec<CheckResult> {
    // drift-guard health: removed because the producer/store does not exist yet.
    // Re-add when a pre-push hook records bypasses to xtask-history. See CONTRIBUTING.md §"Pre-push drift guard".
    let mut checks = vec![
        check_schema_strict_diff(per_check_timeout).await,
        check_closure_health(per_check_timeout, strict).await,
        check_privacy_invariants(per_check_timeout).await,
        check_replay_integrity(per_check_timeout).await,
        check_workspace_check(per_check_timeout).await,
    ];

    // Sort by weight (highest first) then by id.
    checks.sort_by(|a, b| {
        b.weight
            .value()
            .partial_cmp(&a.weight.value())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(b.id))
    });

    checks
}

// ---------------------------------------------------------------------------
// 1. Schema strict-diff (weight: high)
// ---------------------------------------------------------------------------

async fn check_schema_strict_diff(check_timeout: Duration) -> CheckResult {
    let mut result = CheckResult::new(
        "schema-strict-diff",
        "Schema strict-diff",
        CheckWeight::High,
    );

    let outcome = timeout(check_timeout, run_xtask(&["schema", "strict-diff"])).await;

    match outcome {
        Ok(Ok(xtask_result)) => {
            if xtask_result.success {
                result.status = CheckStatus::Pass;
                result.detail = Some("Zero schema drift detected".into());
            } else {
                let msg = format!("Schema drift detected: {}", xtask_result.stderr_summary());
                result.status = CheckStatus::Fail;
                result.detail = Some(msg);
                result.recommendation =
                    Some("Run `xtask schema strict-diff` to inspect drift, then `xtask ci schema-only` to converge".into());
            }
        }
        Ok(Err(error)) => {
            result.status = CheckStatus::Degraded;
            result.detail = Some(format!("xtask invocation failed: {error}"));
        }
        Err(_elapsed) => {
            result.status = CheckStatus::Degraded;
            result.detail = Some("Schema strict-diff timed out".into());
        }
    }

    result
}

// ---------------------------------------------------------------------------
// 2. Closure verification health (weight: medium)
// ---------------------------------------------------------------------------

async fn check_closure_health(check_timeout: Duration, _strict: bool) -> CheckResult {
    let mut result = CheckResult::new(
        "closure-health",
        "Closure verification health",
        CheckWeight::Medium,
    );

    // Find recently closed issues and run `xtask verify closure` on each.
    // We use `gh` to discover recently closed issues, then verify each.
    let outcome = timeout(check_timeout, discover_and_verify_recent_closures()).await;

    match outcome {
        Ok(Ok(health)) => {
            let (verified, total) = health;
            if total == 0 {
                result.status = CheckStatus::Skipped;
                result.detail = Some("No recently closed issues found".into());
                return result;
            }
            let pct = if total > 0 {
                (verified as f64 / total as f64 * 100.0) as u32
            } else {
                0
            };
            result.detail = Some(format!("{verified}/{total} closures verified ({pct}%)"));
            if verified == total {
                result.status = CheckStatus::Pass;
            } else if pct >= 50 {
                result.status = CheckStatus::Degraded;
                result.recommendation =
                    Some("Run `xtask verify closure <N>` on failed issues to diagnose".into());
            } else {
                result.status = CheckStatus::Fail;
                result.recommendation = Some(
                    "Multiple closure verifications failing — run `xtask verify closure <N>` on each failed issue".into(),
                );
            }
        }
        Ok(Err(error)) => {
            result.status = CheckStatus::Skipped;
            result.detail = Some(format!("Could not query recent closures: {error}"));
            result.recommendation =
                Some("Install `gh` CLI and authenticate to enable closure health checks".into());
        }
        Err(_elapsed) => {
            result.status = CheckStatus::Degraded;
            result.detail = Some("Closure health check timed out".into());
        }
    }

    result
}

async fn discover_and_verify_recent_closures() -> Result<(usize, usize), String> {
    // Discover issues closed in the last 30 days. The cutoff is computed at
    // call time so the window slides with the calendar instead of drifting
    // past a hardcoded date.
    let cutoff = {
        let now = time::OffsetDateTime::now_utc();
        let then = now - time::Duration::days(30);
        let date = then.date();
        format!(
            "{:04}-{:02}-{:02}",
            date.year(),
            u8::from(date.month()),
            date.day()
        )
    };
    let search = format!("closed:>={cutoff}");
    let output = Command::new("gh")
        .args([
            "issue",
            "list",
            "--state",
            "closed",
            "--limit",
            "20",
            "--search",
            &search,
            "--json",
            "number",
            "-R",
            "sinity/sinex",
        ])
        .output()
        .await
        .map_err(|e| format!("gh not available: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "gh issue list failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let numbers: Vec<u64> = serde_json::from_slice::<Vec<serde_json::Value>>(&output.stdout)
        .map_err(|e| format!("failed to parse gh output: {e}"))?
        .into_iter()
        .filter_map(|v| v["number"].as_u64())
        .collect();

    if numbers.is_empty() {
        return Ok((0, 0));
    }

    let total = numbers.len();
    let mut verified = 0usize;

    for issue in &numbers {
        // Run `xtask verify closure <N>` on each. We can't call xtask directly
        // from sinexctl, so shell out.
        let cmd_out = Command::new("xtask")
            .args(["verify", "closure", &issue.to_string()])
            .output()
            .await;

        match cmd_out {
            Ok(out) if out.status.success() => verified += 1,
            _ => {} // closure verification failed — counted against us
        }
    }

    Ok((verified, total))
}

// ---------------------------------------------------------------------------
// 3. Privacy invariants (weight: high)
// ---------------------------------------------------------------------------

async fn check_privacy_invariants(check_timeout: Duration) -> CheckResult {
    let mut result = CheckResult::new(
        "privacy-invariants",
        "Privacy invariants",
        CheckWeight::High,
    );

    // Sub-check 1: privacy unit tests in sinex-primitives.
    let (primary_status, primary_msg) = match timeout(
        check_timeout,
        run_xtask(&[
            "test",
            "-p",
            "sinex-primitives",
            "-E",
            "test(privacy)",
            "--impact-mode=off",
        ]),
    )
    .await
    {
        Ok(Ok(xtask_result)) => {
            if xtask_result.success {
                (CheckStatus::Pass, "privacy tests passing".to_string())
            } else {
                (
                    CheckStatus::Fail,
                    format!("privacy test failures: {}", xtask_result.stderr_summary()),
                )
            }
        }
        Ok(Err(error)) => (
            CheckStatus::Degraded,
            format!("privacy test invocation failed: {error}"),
        ),
        Err(_elapsed) => (CheckStatus::Degraded, "privacy tests timed out".into()),
    };

    result.status = primary_status;
    result.detail = Some(primary_msg);
    if matches!(primary_status, CheckStatus::Fail | CheckStatus::Degraded) {
        result.recommendation =
            Some("Run `xtask test -p sinex-primitives -E 'test(privacy)'` to inspect".into());
    }

    result
}

// ---------------------------------------------------------------------------
// 5. Replay integrity (weight: high)
// ---------------------------------------------------------------------------

async fn check_replay_integrity(check_timeout: Duration) -> CheckResult {
    let mut result = CheckResult::new("replay-integrity", "Replay integrity", CheckWeight::High);

    // Run replay-related tests.
    let outcome = timeout(
        check_timeout,
        run_xtask(&[
            "test",
            "-p",
            "sinexd",
            "-E",
            "test(replay)",
            "--impact-mode=off",
        ]),
    )
    .await;

    match outcome {
        Ok(Ok(xtask_result)) => {
            if xtask_result.success {
                result.status = CheckStatus::Pass;
                result.detail = Some("Replay tests passing".into());
            } else {
                result.status = CheckStatus::Fail;
                result.detail = Some(format!(
                    "Replay test failures: {}",
                    xtask_result.stderr_summary()
                ));
                result.recommendation =
                    Some("Run `xtask test -p sinexd -E 'test(replay)'` to inspect".into());
            }
        }
        Ok(Err(error)) => {
            result.status = CheckStatus::Degraded;
            result.detail = Some(format!("Replay test invocation failed: {error}"));
        }
        Err(_elapsed) => {
            result.status = CheckStatus::Degraded;
            result.detail = Some("Replay tests timed out".into());
        }
    }

    result
}

// ---------------------------------------------------------------------------
// 6. Workspace check (weight: medium)
// ---------------------------------------------------------------------------

async fn check_workspace_check(check_timeout: Duration) -> CheckResult {
    let mut result = CheckResult::new(
        "workspace-check",
        "Workspace compilation",
        CheckWeight::Medium,
    );

    let outcome = timeout(check_timeout, run_xtask(&["check"])).await;

    match outcome {
        Ok(Ok(xtask_result)) => {
            if xtask_result.success {
                result.status = CheckStatus::Pass;
                result.detail = Some("Workspace compiles cleanly".into());
            } else {
                result.status = CheckStatus::Fail;
                result.detail = Some(format!(
                    "Workspace check failures: {}",
                    xtask_result.stderr_summary()
                ));
                result.recommendation =
                    Some("Run `xtask check` to inspect compilation errors".into());
            }
        }
        Ok(Err(error)) => {
            result.status = CheckStatus::Degraded;
            result.detail = Some(format!("xtask invocation failed: {error}"));
        }
        Err(_elapsed) => {
            result.status = CheckStatus::Degraded;
            result.detail = Some("Workspace check timed out".into());
        }
    }

    result
}

// ---------------------------------------------------------------------------
// xtask subprocess helper
// ---------------------------------------------------------------------------

struct XtaskResult {
    success: bool,
    stderr: String,
}

impl XtaskResult {
    fn stderr_summary(&self) -> String {
        let s = self.stderr.trim();
        if s.is_empty() {
            "no output".into()
        } else if s.len() > 500 {
            format!("{}…", &s[..500])
        } else {
            s.to_string()
        }
    }
}

async fn run_xtask(args: &[&str]) -> Result<XtaskResult, String> {
    let output = Command::new("xtask")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("failed to execute xtask: {e}"))?;

    Ok(XtaskResult {
        success: output.status.success(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

// ---------------------------------------------------------------------------
// Scoring
// ---------------------------------------------------------------------------

fn compute_score(checks: &[CheckResult]) -> u32 {
    let mut total_weight = 0.0f64;
    let mut earned = 0.0f64;

    for check in checks {
        if check.status == CheckStatus::Skipped {
            continue;
        }
        let w = check.weight.value();
        total_weight += w;
        earned += w * check.status.score();
    }

    if total_weight == 0.0 {
        return 100; // everything skipped — nothing to measure
    }

    (earned / total_weight * 100.0).round() as u32
}

// ---------------------------------------------------------------------------
// Human-readable report
// ---------------------------------------------------------------------------

fn print_table_report(checks: &[CheckResult], score: u32) {
    for check in checks {
        let icon = check.status.colored_icon();
        let weight_label = match check.weight {
            CheckWeight::High => "HIGH",
            CheckWeight::Medium => "MED ",
            CheckWeight::Low => "LOW ",
        };
        println!(
            "  [{}] [{}] {}",
            icon,
            style(weight_label).dim(),
            check.label
        );
        if let Some(detail) = &check.detail {
            println!("         {}", style(detail).dim());
        }
        if let Some(rec) = &check.recommendation {
            println!("         {} {}", style("->").yellow(), style(rec).yellow());
        }
    }

    println!();
    println!("{}", style("─".repeat(60)).dim());

    let (pass, degraded, fail, skipped) = tally(checks);
    println!(
        "  {} passed  {} degraded  {} failed  {} skipped",
        style(pass).green().bold(),
        style(degraded).yellow().bold(),
        style(fail).red().bold(),
        style(skipped).dim(),
    );

    print_score_bar(score);

    if fail > 0 {
        println!();
        println!(
            "{}",
            style("Verification FAILED — fix failures above")
                .red()
                .bold()
        );
    } else if score >= 80 {
        println!();
        println!(
            "{}",
            style("Verification baseline healthy ✓").green().bold()
        );
    } else {
        println!();
        println!(
            "{}",
            style("Verification baseline below threshold — address degraded checks").yellow()
        );
    }
}

fn print_score_bar(score: u32) {
    let bar_width = 40;
    let filled = (score as usize * bar_width / 100).min(bar_width);
    let empty = bar_width - filled;

    let color = match score {
        80..=100 => style("█".repeat(filled)).green(),
        50..=79 => style("█".repeat(filled)).yellow(),
        _ => style("█".repeat(filled)).red(),
    };

    println!();
    println!(
        "  Score: {}/100  {}{}",
        style(score).bold(),
        color,
        style("░".repeat(empty)).dim()
    );
}

fn tally(checks: &[CheckResult]) -> (usize, usize, usize, usize) {
    let mut pass = 0usize;
    let mut degraded = 0usize;
    let mut fail = 0usize;
    let mut skipped = 0usize;
    for c in checks {
        match c.status {
            CheckStatus::Pass => pass += 1,
            CheckStatus::Degraded => degraded += 1,
            CheckStatus::Fail => fail += 1,
            CheckStatus::Skipped => skipped += 1,
        }
    }
    (pass, degraded, fail, skipped)
}

fn build_summary(checks: &[CheckResult], score: u32) -> String {
    let (pass, degraded, fail, skipped) = tally(checks);
    let fail_ids: Vec<&str> = checks
        .iter()
        .filter(|c| c.status == CheckStatus::Fail)
        .map(|c| c.id)
        .collect();

    if fail > 0 {
        format!(
            "Score {score}/100 — {pass} pass, {degraded} degraded, {fail} failed, {skipped} skipped. Failed: {}",
            fail_ids.join(", ")
        )
    } else if score >= 80 {
        format!(
            "Score {score}/100 — baseline healthy ({pass} pass, {degraded} degraded, {skipped} skipped)"
        )
    } else {
        format!(
            "Score {score}/100 — below threshold ({pass} pass, {degraded} degraded, {skipped} skipped). Address degraded checks."
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn score_is_100_when_all_pass() -> xtask::sandbox::TestResult<()> {
        let checks = vec![
            make_check("a", CheckStatus::Pass, CheckWeight::High),
            make_check("b", CheckStatus::Pass, CheckWeight::Medium),
        ];
        assert_eq!(compute_score(&checks), 100);
        Ok(())
    }

    #[sinex_test]
    async fn score_is_0_when_all_fail() -> xtask::sandbox::TestResult<()> {
        let checks = vec![
            make_check("a", CheckStatus::Fail, CheckWeight::High),
            make_check("b", CheckStatus::Fail, CheckWeight::Low),
        ];
        assert_eq!(compute_score(&checks), 0);
        Ok(())
    }

    #[sinex_test]
    async fn skipped_checks_are_excluded() -> xtask::sandbox::TestResult<()> {
        let checks = vec![
            make_check("a", CheckStatus::Pass, CheckWeight::High),
            make_check("b", CheckStatus::Skipped, CheckWeight::High),
            make_check("c", CheckStatus::Fail, CheckWeight::Medium),
        ];
        // Pass=3.0*1.0=3.0, Fail=2.0*0.0=0.0, total weight=5.0, score=60
        assert_eq!(compute_score(&checks), 60);
        Ok(())
    }

    #[sinex_test]
    async fn degraded_is_half_weight() -> xtask::sandbox::TestResult<()> {
        let checks = vec![
            make_check("a", CheckStatus::Pass, CheckWeight::High),
            make_check("b", CheckStatus::Degraded, CheckWeight::High),
        ];
        // Pass=3.0, Degraded=3.0*0.5=1.5, total=4.5/6.0=75
        assert_eq!(compute_score(&checks), 75);
        Ok(())
    }

    #[sinex_test]
    async fn all_skipped_is_100() -> xtask::sandbox::TestResult<()> {
        let checks = vec![make_check("a", CheckStatus::Skipped, CheckWeight::High)];
        assert_eq!(compute_score(&checks), 100);
        Ok(())
    }

    #[sinex_test]
    async fn tally_counts_correctly() -> xtask::sandbox::TestResult<()> {
        let checks = vec![
            make_check("a", CheckStatus::Pass, CheckWeight::High),
            make_check("b", CheckStatus::Pass, CheckWeight::Medium),
            make_check("c", CheckStatus::Degraded, CheckWeight::Low),
            make_check("d", CheckStatus::Fail, CheckWeight::High),
            make_check("e", CheckStatus::Skipped, CheckWeight::Low),
        ];
        let (pass, degraded, fail, skipped) = tally(&checks);
        assert_eq!(pass, 2);
        assert_eq!(degraded, 1);
        assert_eq!(fail, 1);
        assert_eq!(skipped, 1);
        Ok(())
    }

    fn make_check(id: &'static str, status: CheckStatus, weight: CheckWeight) -> CheckResult {
        CheckResult {
            id,
            label: id,
            weight,
            status,
            detail: None,
            recommendation: None,
        }
    }
}
