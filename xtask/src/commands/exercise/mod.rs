//! Surface area validation for the xtask command suite.
//!
//! Exercises every major xtask capability via real subprocess invocations,
//! validates results, and saves all outputs for inspection.
//!
//! ```bash
//! xtask exercise              # Run tier 1 (default, ~30s)
//! xtask exercise --all        # Run all tiers
//! xtask exercise --tier 2     # Specific tier
//! xtask exercise --list       # Show catalog
//! xtask exercise --id t4.bg_job_lifecycle  # Specific exercise
//! ```

use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use color_eyre::eyre::{Result, WrapErr};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::workspace_root;

pub mod builders;
pub mod catalog;
pub mod custom;
pub mod runner;
pub mod types;

// Re-export the builder helpers used from tests
pub use builders::{
    extract_json_field, json_path, parse_last_json, v_arr_min, v_contains, v_empty, v_eq, v_has,
    v_json, v_lines, v_stderr,
};
pub use catalog::build_catalog;
pub use runner::{
    build_report, exec_step, print_human_summary, run_custom_exercise, run_declarative_exercise,
    run_xtask, save_output, setup_output_dir, validate_step,
};
pub use types::{
    ExerciseDef, ExerciseKind, ExerciseOutcome, ExerciseReport, ExpectedExit, InfraReq, QaManifest,
    QaManifestEntry, ReportEntry, StepEntry, StepOutcome, StepOutput, Tier, Validation,
};

// ═══════════════════════════════════════════════════════════════════════════════
// CLI struct
// ═══════════════════════════════════════════════════════════════════════════════

/// Full surface area validation for xtask commands.
///
/// Runs real subprocess invocations of `xtask` commands across four tiers
/// of increasing scope, validates outputs, and saves results for inspection.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct ExerciseCommand {
    /// Run all tiers (default: tier 1 only)
    #[arg(long)]
    pub all: bool,

    /// Specific tier(s) to run (1-4, repeatable)
    #[arg(long = "tier", value_name = "TIER")]
    pub tiers: Vec<Tier>,

    /// Run specific exercise(s) by ID
    #[arg(long = "id", value_name = "ID")]
    pub exercises: Vec<String>,

    /// List exercises without running
    #[arg(long)]
    pub list: bool,

    /// Alias for --list
    #[arg(long)]
    pub dry_run: bool,

    /// Skip exercises needing infrastructure (Postgres/NATS)
    #[arg(long)]
    pub skip_infra: bool,

    /// Stream exercise output in real-time
    #[arg(long)]
    pub verbose: bool,

    /// Stop on first failure (default: continue all)
    #[arg(long)]
    pub fail_fast: bool,

    /// Seed a temporary history database before running exercises (T1).
    ///
    /// Creates an ephemeral SQLite file with synthetic invocation history,
    /// sets `SINEX_STATE_DIR` for all subprocess xtask invocations so they
    /// see rich history output. The temporary database is cleaned up when the
    /// exercise run completes; the real history is never touched.
    #[arg(long)]
    pub seed: bool,

    /// Days of history to generate for --seed (default: 30)
    #[arg(long, default_value = "30", requires = "seed")]
    pub seed_days: u32,

    /// Number of invocations to generate for --seed (default: 100)
    #[arg(long, default_value = "100", requires = "seed")]
    pub seed_invocations: u32,

    /// After seeding, print the DB path as an export statement for shell activation (T4)
    #[arg(long, requires = "seed")]
    pub activate: bool,

    /// Write a deterministic QA manifest (exercise IDs + pass/fail) to this path.
    ///
    /// The manifest is small and stable — no timings, no paths, just behavioral
    /// outcomes. Commit it as `xtask/config/exercise-baseline.json` to create a
    /// regression gate. Use `--ci-check` to enforce it against the baseline.
    #[arg(long, value_name = "PATH")]
    pub audit_file: Option<std::path::PathBuf>,

    /// Diff results against the committed baseline, fail on regressions.
    ///
    /// Reads `xtask/config/exercise-baseline.json` (or the path given by
    /// `--baseline`). Any exercise that was passing in the baseline and is now
    /// failing is a regression — exits non-zero with a clear report.
    ///
    /// Baseline gate: `xtask exercise --tier 1 --seed --ci-check`
    #[arg(long)]
    pub ci_check: bool,

    /// Override the baseline path used by `--ci-check`.
    #[arg(long, value_name = "PATH", requires = "ci_check")]
    pub baseline: Option<std::path::PathBuf>,

    /// Update the committed baseline with the current run results.
    ///
    /// Writes to `xtask/config/exercise-baseline.json` (or `--baseline` path).
    /// Requires `--ci-check` so the update is intentional.
    #[arg(long, requires = "ci_check")]
    pub update_baseline: bool,
}

// ═══════════════════════════════════════════════════════════════════════════════
// XtaskCommand implementation
// ═══════════════════════════════════════════════════════════════════════════════

impl ExerciseCommand {
    /// Render the `--list` / `--dry-run` catalog view (JSON or human) and return
    /// the resulting command result. Extracted from `execute` to keep it within
    /// the cognitive-complexity budget.
    fn render_exercise_listing(
        &self,
        ctx: &CommandContext,
        exercises: &[&ExerciseDef],
    ) -> CommandResult {
        if ctx.is_json() {
            let entries: Vec<serde_json::Value> = exercises
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "id": e.id,
                        "tier": e.tier.label(),
                        "description": e.description,
                        "infra": format!("{:?}", e.infra),
                        "kind": match &e.kind {
                            ExerciseKind::Declarative(s) => format!("declarative ({} steps)", s.len()),
                            ExerciseKind::Custom => "custom".to_string(),
                        },
                    })
                })
                .collect();
            return CommandResult::success()
                .with_message(format!("{} exercises", entries.len()))
                .with_data(serde_json::json!({
                    "exercises": entries,
                    "count": entries.len()
                }));
        }

        println!("Exercise catalog ({} exercises):\n", exercises.len());
        let mut current_tier = None;
        for e in exercises {
            if current_tier != Some(e.tier) {
                current_tier = Some(e.tier);
                println!("  {} ─────────────────────────────────────", e.tier.label());
            }
            let infra_tag = match e.infra {
                InfraReq::None => "",
                InfraReq::Postgres => " [pg]",
                InfraReq::Nats => " [nats]",
                InfraReq::Both => " [pg+nats]",
            };
            println!("    {:<40} {}{}", e.id, e.description, infra_tag);
        }
        println!();

        CommandResult::success()
            .with_message(format!("{} exercises listed", exercises.len()))
            .with_duration(ctx.elapsed())
    }

    /// Resolve the baseline path: explicit `--baseline`, else workspace default.
    fn baseline_path(&self) -> PathBuf {
        self.baseline
            .clone()
            .unwrap_or_else(|| workspace_root().join("xtask/config/exercise-baseline.json"))
    }

    fn background_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        if self.all {
            args.push("--all".to_string());
        }
        for t in &self.tiers {
            args.push("--tier".to_string());
            args.push(t.as_arg().to_string());
        }
        for e in &self.exercises {
            args.push("--id".to_string());
            args.push(e.clone());
        }
        if self.list {
            args.push("--list".to_string());
        }
        if self.dry_run {
            args.push("--dry-run".to_string());
        }
        if self.skip_infra {
            args.push("--skip-infra".to_string());
        }
        if self.verbose {
            args.push("--verbose".to_string());
        }
        if self.fail_fast {
            args.push("--fail-fast".to_string());
        }
        if self.seed {
            args.push("--seed".to_string());
            args.push("--seed-days".to_string());
            args.push(self.seed_days.to_string());
            args.push("--seed-invocations".to_string());
            args.push(self.seed_invocations.to_string());
        }
        if self.activate {
            args.push("--activate".to_string());
        }
        if let Some(audit_path) = &self.audit_file {
            args.push("--audit-file".to_string());
            args.push(audit_path.display().to_string());
        }
        if self.ci_check {
            args.push("--ci-check".to_string());
        }
        if let Some(baseline) = &self.baseline {
            args.push("--baseline".to_string());
            args.push(baseline.display().to_string());
        }
        if self.update_baseline {
            args.push("--update-baseline".to_string());
        }
        args
    }
}

impl XtaskCommand for ExerciseCommand {
    fn name(&self) -> &'static str {
        "exercise"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle background execution
        if ctx.is_background() {
            let args = self.background_args();
            return ctx.spawn_background("exercise", &args);
        }

        // T1: --seed mode — create ephemeral history database for this exercise run.
        // All child xtask subprocesses will see the synthetic DB via SINEX_STATE_DIR.
        // The guard cleans up (restores env + removes temp dir) when it drops at end of scope.
        let seed_guard = if self.seed {
            if ctx.is_human() {
                println!("Preparing ephemeral history database…");
            }
            Some(SeedGuard::new(
                self.seed_days,
                self.seed_invocations,
                ctx.is_human(),
            )?)
        } else {
            None
        };

        let catalog = build_catalog();

        // Determine which tiers to run
        let tiers: HashSet<Tier> = if !self.exercises.is_empty() {
            // When specific exercises requested, include all tiers
            [Tier::T1, Tier::T2, Tier::T3, Tier::T4]
                .into_iter()
                .collect()
        } else if self.all {
            [Tier::T1, Tier::T2, Tier::T3, Tier::T4]
                .into_iter()
                .collect()
        } else if self.tiers.is_empty() {
            [Tier::T1].into_iter().collect() // Default: T1 only
        } else {
            self.tiers.iter().copied().collect()
        };

        // Filter exercises
        let exercises: Vec<&ExerciseDef> = catalog
            .iter()
            .filter(|e| {
                if !tiers.contains(&e.tier) {
                    return false;
                }
                if !self.exercises.is_empty() && !self.exercises.contains(&e.id) {
                    return false;
                }
                if self.skip_infra && e.infra != InfraReq::None {
                    return false;
                }
                true
            })
            .collect();

        // Handle --list / --dry-run
        if self.list || self.dry_run {
            return Ok(self.render_exercise_listing(ctx, &exercises));
        }

        // Count infra-skipped exercises (for reporting)
        let skipped_count = if self.skip_infra {
            catalog
                .iter()
                .filter(|e| {
                    tiers.contains(&e.tier)
                        && (self.exercises.is_empty() || self.exercises.contains(&e.id))
                        && e.infra != InfraReq::None
                })
                .count()
        } else {
            0
        };

        // Setup output directory
        let output_dir = setup_output_dir()?;

        if ctx.is_human() {
            println!(
                "Running {} exercises (skipped: {})...\n",
                exercises.len(),
                skipped_count
            );
        }

        // Run exercises
        let start = Instant::now();
        let mut outcomes = Vec::new();

        for (i, ex) in exercises.iter().enumerate() {
            let ex_dir = output_dir.join(&ex.id);

            if ctx.is_human() {
                print!("  [{}/{}] {} ...", i + 1, exercises.len(), ex.id);
                if let Err(error) = std::io::stdout().flush() {
                    eprintln!("⚠ failed to flush exercise progress output: {error}");
                }
            }

            if let Err(error) = create_exercise_dir(&ex_dir) {
                let message = format!("{error:#}");
                if ctx.is_human() {
                    println!(" \x1b[31m✗\x1b[0m");
                    println!("      {message}");
                }
                outcomes.push(ExerciseOutcome {
                    id: ex.id.clone(),
                    passed: false,
                    duration: Duration::ZERO,
                    steps: vec![],
                    error: Some(message),
                });
                continue;
            }

            let outcome = match &ex.kind {
                ExerciseKind::Declarative(_) => run_declarative_exercise(ex, &ex_dir, self.verbose),
                ExerciseKind::Custom => run_custom_exercise(ex, &ex_dir, self.verbose),
            };

            if ctx.is_human() {
                print_exercise_outcome(i, exercises.len(), &ex.id, &outcome);
            }

            let failed = !outcome.passed;
            outcomes.push(outcome);

            if self.fail_fast && failed {
                if ctx.is_human() {
                    println!("\n  ⚡ Stopping early (--fail-fast)");
                }
                break;
            }
        }

        let total_duration = start.elapsed();

        // Build and save report
        let report = build_report(
            &outcomes,
            &catalog,
            skipped_count,
            total_duration,
            &output_dir,
        );
        let report_json = serde_json::to_string_pretty(&report)?;
        fs::write(output_dir.join("summary.json"), &report_json)?;

        // Record detailed exercise run in history DB (best-effort, non-fatal).
        if let Some(inv_id) = ctx.invocation_id() {
            ctx.with_history_db(|db| db.record_exercise_run(inv_id, &report));
        }

        // Build QA manifest (used by --audit-file and --ci-check).
        let manifest = QaManifest::from_report(&report);

        // --audit-file: write deterministic manifest to the specified path.
        if let Some(audit_path) = &self.audit_file {
            let json = serde_json::to_string_pretty(&manifest)?;
            if let Some(parent) = audit_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(audit_path, &json)?;
            if ctx.is_human() {
                println!("  Manifest: {}", audit_path.display());
            }
        }

        // --ci-check: diff against committed baseline, fail on regressions.
        let (ci_regressions, _) = if self.ci_check {
            check_ci_baseline(
                &manifest,
                &self.baseline_path(),
                self.update_baseline,
                ctx.is_human(),
            )?
        } else {
            (vec![], vec![])
        };

        // Print human summary
        if ctx.is_human() {
            print_human_summary(&outcomes, skipped_count, total_duration);
            println!("  Outputs: {}", output_dir.display());
            println!();
        }

        // Build execution result
        let passed = outcomes.iter().filter(|o| o.passed).count();
        let failed = outcomes.iter().filter(|o| !o.passed).count();

        // Baseline regressions override a clean run to a failure.
        let mut result = if !ci_regressions.is_empty() {
            CommandResult::failure(crate::output::StructuredError::new(
                "BASELINE_REGRESSION",
                format!(
                    "{} exercise(s) regressed vs baseline: {}",
                    ci_regressions.len(),
                    ci_regressions.join(", ")
                ),
            ))
        } else if failed == 0 {
            CommandResult::success()
        } else if passed == 0 {
            CommandResult::failure(crate::output::StructuredError::new(
                "EXERCISE_FAILED",
                format!("All {failed} exercises failed"),
            ))
        } else {
            CommandResult::partial()
        };

        result = result
            .with_message(format!("{passed}/{} exercises passed", outcomes.len()))
            .with_data(serde_json::to_value(&report).wrap_err("serialize exercise report")?)
            .with_duration(ctx.elapsed());

        if failed > 0 {
            let failed_ids: Vec<String> = outcomes
                .iter()
                .filter(|o| !o.passed)
                .map(|o| o.id.clone())
                .collect();
            result = result.with_details(failed_ids);
        }

        if !ci_regressions.is_empty() {
            result = result.with_details(ci_regressions.clone());
        }

        // T1/T4: --activate prints the DB path as a shell export so users can
        // point their shell session at the synthetic DB for manual exploration.
        // The guard's temp dir is cleaned up when it drops after this block.
        if self.activate
            && let Some(ref guard) = seed_guard
        {
            println!("export XTASK_HISTORY_DB={}", guard.db_path.display());
            eprintln!(
                "  ⚡ DB path printed. To activate: eval $(xtask exercise --seed --activate)"
            );
        }

        // Drop guard explicitly to make scope visible; restores env + deletes temp dir.
        drop(seed_guard);

        Ok(result)
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("test"),
            timeout: Some(Duration::from_mins(30)),
            modifies_state: false,
            track_in_history: true,
            history_access: crate::command::HistoryAccessMode::ReadWrite,
        }
    }
}

/// Diff the current manifest against a committed baseline, returning (regressions, new_passes).
/// Writes the baseline if `update_baseline` is true or no baseline exists.
/// Print the human-readable outcome line for one exercise.
fn print_exercise_outcome(idx: usize, total: usize, id: &str, outcome: &ExerciseOutcome) {
    let symbol = if outcome.passed {
        "\x1b[32m✓\x1b[0m"
    } else {
        "\x1b[31m✗\x1b[0m"
    };
    println!(
        "\r  [{}/{}] {} {} ({:.1}s)",
        idx + 1,
        total,
        symbol,
        id,
        outcome.duration.as_secs_f64()
    );
    if !outcome.passed {
        for s in &outcome.steps {
            for err in &s.validation_errors {
                println!("           └─ {}: {err}", s.label);
            }
        }
        if let Some(e) = &outcome.error {
            println!("           └─ {e}");
        }
    }
}

fn check_ci_baseline(
    manifest: &QaManifest,
    baseline_path: &std::path::Path,
    update_baseline: bool,
    is_human: bool,
) -> Result<(Vec<String>, Vec<String>)> {
    if update_baseline {
        let json = serde_json::to_string_pretty(manifest)?;
        if let Some(parent) = baseline_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(baseline_path, &json)?;
        if is_human {
            println!("  Baseline updated: {}", baseline_path.display());
        }
        return Ok((vec![], vec![]));
    }

    if !baseline_path.exists() {
        let json = serde_json::to_string_pretty(manifest)?;
        if let Some(parent) = baseline_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(baseline_path, &json)?;
        if is_human {
            println!(
                "  Baseline created: {} (no prior baseline found)",
                baseline_path.display()
            );
        }
        return Ok((vec![], vec![]));
    }

    let baseline_raw = fs::read_to_string(baseline_path)?;
    let baseline: QaManifest = serde_json::from_str(&baseline_raw).map_err(|e| {
        color_eyre::eyre::eyre!("Failed to parse baseline {}: {e}", baseline_path.display())
    })?;
    let regressions = manifest.regressions(&baseline);
    let new_passes = manifest.new_passes(&baseline);

    if is_human {
        if !regressions.is_empty() {
            println!("\n  ⚡ Baseline regressions ({}):", regressions.len());
            for id in &regressions {
                println!("       ✗  {id}  (was passing in baseline)");
            }
        }
        if !new_passes.is_empty() {
            println!("  🎉 Newly passing ({}):", new_passes.len());
            for id in &new_passes {
                println!("       ✓  {id}");
            }
        }
        if regressions.is_empty() {
            println!(
                "  ✓ No regressions vs baseline ({})",
                baseline_path.display()
            );
        }
    }
    Ok((regressions, new_passes))
}

fn create_exercise_dir(path: &std::path::Path) -> Result<()> {
    fs::create_dir_all(path)
        .wrap_err_with(|| format!("create exercise output directory {}", path.display()))
}

// ═══════════════════════════════════════════════════════════════════════════════
// T1: Ephemeral history seed guard
// ═══════════════════════════════════════════════════════════════════════════════

/// RAII guard that provides an ephemeral seeded history database for exercise runs.
///
/// On construction: creates a temp directory, seeds a SQLite history DB inside it,
/// and sets `SINEX_STATE_DIR` so all child `xtask` invocations use the synthetic DB.
/// Sets `XTASK_SYNTHETIC_HISTORY=allow` to suppress the synthetic-data warning in
/// subprocess output.
///
/// On drop: restores the original `SINEX_STATE_DIR`, removes `XTASK_SYNTHETIC_HISTORY`,
/// and deletes the temporary directory.
struct SeedGuard {
    _temp_dir: tempfile::TempDir,
    old_state_dir: Option<String>,
    /// Path to the seeded DB (for --activate reporting)
    pub db_path: std::path::PathBuf,
}

impl SeedGuard {
    fn new(days: u32, invocations: u32, verbose: bool) -> color_eyre::eyre::Result<Self> {
        use crate::history::HistoryDb;
        use crate::history::seed::{SeedOptions, seed_history};

        let temp_dir = tempfile::tempdir()?;
        let state_dir = temp_dir.path().join("state");
        std::fs::create_dir_all(&state_dir)?;
        let db_path = state_dir.join("xtask-history.db");

        let db = HistoryDb::open(&db_path)?;
        seed_history(&db, &SeedOptions { days, invocations })?;

        let old_state_dir = std::env::var("SINEX_STATE_DIR").ok();

        // Safety: exercises run sequentially via blocking Command::output() calls;
        // no concurrent thread reads env during this window.
        unsafe {
            std::env::set_var("SINEX_STATE_DIR", &state_dir);
            std::env::set_var("XTASK_SYNTHETIC_HISTORY", "allow");
        }

        if verbose {
            println!(
                "  Seeded ephemeral history: {} ({days}d, {invocations} invocations)",
                db_path.display()
            );
        }

        Ok(Self {
            _temp_dir: temp_dir,
            old_state_dir,
            db_path,
        })
    }
}

impl Drop for SeedGuard {
    fn drop(&mut self) {
        // Safety: same single-threaded sequential context as construction.
        unsafe {
            match &self.old_state_dir {
                Some(v) => std::env::set_var("SINEX_STATE_DIR", v),
                None => std::env::remove_var("SINEX_STATE_DIR"),
            }
            std::env::remove_var("XTASK_SYNTHETIC_HISTORY");
        }
        // _temp_dir drops here, deleting the ephemeral directory.
    }
}

#[cfg(test)]
#[path = "../exercise_test.rs"]
mod tests;
