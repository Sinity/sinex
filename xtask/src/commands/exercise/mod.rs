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
//! xtask exercise -E t4.bg_job_lifecycle  # Specific exercise
//! ```

use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::time::{Duration, Instant};

use color_eyre::eyre::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

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
    ExerciseDef, ExerciseKind, ExerciseOutcome, ExerciseReport, ExpectedExit, InfraReq,
    ReportEntry, StepEntry, StepOutcome, StepOutput, Tier, Validation,
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
}

// ═══════════════════════════════════════════════════════════════════════════════
// XtaskCommand implementation
// ═══════════════════════════════════════════════════════════════════════════════

impl XtaskCommand for ExerciseCommand {
    fn name(&self) -> &'static str {
        "exercise"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle background execution
        if ctx.is_background() {
            let mut args = Vec::new();
            if self.all {
                args.push("--all".to_string());
            }
            for t in &self.tiers {
                args.push("--tier".to_string());
                args.push(t.as_arg().to_string());
            }
            for e in &self.exercises {
                args.push("-E".to_string());
                args.push(e.clone());
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
                return Ok(CommandResult::success()
                    .with_message(format!("{} exercises", entries.len()))
                    .with_data(serde_json::json!({
                        "exercises": entries,
                        "count": entries.len()
                    })));
            }

            println!("Exercise catalog ({} exercises):\n", exercises.len());
            let mut current_tier = None;
            for e in &exercises {
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

            return Ok(CommandResult::success()
                .with_message(format!("{} exercises listed", exercises.len()))
                .with_duration(ctx.elapsed()));
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
            let _ = fs::create_dir_all(&ex_dir);

            if ctx.is_human() {
                print!("  [{}/{}] {} ...", i + 1, exercises.len(), ex.id);
                let _ = std::io::stdout().flush();
            }

            let outcome = match &ex.kind {
                ExerciseKind::Declarative(_) => run_declarative_exercise(ex, &ex_dir, self.verbose),
                ExerciseKind::Custom => run_custom_exercise(ex, &ex_dir, self.verbose),
            };

            if ctx.is_human() {
                let symbol = if outcome.passed {
                    "\x1b[32m✓\x1b[0m"
                } else {
                    "\x1b[31m✗\x1b[0m"
                };
                println!(
                    "\r  [{}/{}] {} {} ({:.1}s)",
                    i + 1,
                    exercises.len(),
                    symbol,
                    ex.id,
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

        // Print human summary
        if ctx.is_human() {
            print_human_summary(&outcomes, skipped_count, total_duration);
            println!("  Outputs: {}", output_dir.display());
            println!();
        }

        // Build execution result
        let passed = outcomes.iter().filter(|o| o.passed).count();
        let failed = outcomes.iter().filter(|o| !o.passed).count();

        let mut result = if failed == 0 {
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
            .with_data(serde_json::to_value(&report).unwrap_or_default())
            .with_duration(ctx.elapsed());

        if failed > 0 {
            let failed_ids: Vec<String> = outcomes
                .iter()
                .filter(|o| !o.passed)
                .map(|o| o.id.clone())
                .collect();
            result = result.with_details(failed_ids);
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
        }
    }
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
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    // ── Tier enum ─────────────────────────────────────────────────────────────

    #[sinex_test]
    async fn test_tier_label() -> ::xtask::sandbox::TestResult<()> {
        assert_eq!(Tier::T1.label(), "T1");
        assert_eq!(Tier::T2.label(), "T2");
        assert_eq!(Tier::T3.label(), "T3");
        assert_eq!(Tier::T4.label(), "T4");
        Ok(())
    }

    #[sinex_test]
    async fn test_tier_as_arg() -> ::xtask::sandbox::TestResult<()> {
        assert_eq!(Tier::T1.as_arg(), "1");
        assert_eq!(Tier::T2.as_arg(), "2");
        assert_eq!(Tier::T3.as_arg(), "3");
        assert_eq!(Tier::T4.as_arg(), "4");
        Ok(())
    }

    #[sinex_test]
    async fn test_tier_display() -> ::xtask::sandbox::TestResult<()> {
        assert_eq!(Tier::T1.to_string(), "T1");
        assert_eq!(Tier::T4.to_string(), "T4");
        Ok(())
    }

    // ── json_path helper ──────────────────────────────────────────────────────

    #[sinex_test]
    async fn test_json_path_top_level() -> ::xtask::sandbox::TestResult<()> {
        let val = serde_json::json!({"status": "success", "count": 3});
        assert_eq!(
            json_path(&val, "status"),
            Some(&serde_json::json!("success"))
        );
        assert_eq!(json_path(&val, "count"), Some(&serde_json::json!(3)));
        assert_eq!(json_path(&val, "missing"), None);
        Ok(())
    }

    #[sinex_test]
    async fn test_json_path_nested() -> ::xtask::sandbox::TestResult<()> {
        let val = serde_json::json!({"data": {"job_id": 42}});
        assert_eq!(json_path(&val, "data.job_id"), Some(&serde_json::json!(42)));
        assert_eq!(json_path(&val, "data.missing"), None);
        assert_eq!(json_path(&val, "nope.job_id"), None);
        Ok(())
    }

    // ── parse_last_json ───────────────────────────────────────────────────────

    #[sinex_test]
    async fn test_parse_last_json_single() -> ::xtask::sandbox::TestResult<()> {
        let result = parse_last_json(r#"{"status":"ok"}"#);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), serde_json::json!({"status": "ok"}));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_last_json_multiple_returns_last() -> ::xtask::sandbox::TestResult<()> {
        // Two concatenated JSON objects — last wins
        let result = parse_last_json(r#"{"first":1}{"second":2}"#);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), serde_json::json!({"second": 2}));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_last_json_empty_string() -> ::xtask::sandbox::TestResult<()> {
        let result = parse_last_json("");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no JSON object found"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_last_json_invalid() -> ::xtask::sandbox::TestResult<()> {
        let result = parse_last_json("not json at all");
        assert!(result.is_err());
        Ok(())
    }

    // ── Validation::check ─────────────────────────────────────────────────────

    fn make_output(stdout: &str, stderr: &str, exit_code: i32) -> StepOutput {
        StepOutput {
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
            exit_code,
            duration: Duration::ZERO,
        }
    }

    #[sinex_test]
    async fn test_validation_json_valid() -> ::xtask::sandbox::TestResult<()> {
        let out = make_output(r#"{"ok":true}"#, "", 0);
        assert!(Validation::JsonValid.check(&out).is_ok());

        let bad = make_output("not json", "", 0);
        assert!(Validation::JsonValid.check(&bad).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_validation_json_has_fields() -> ::xtask::sandbox::TestResult<()> {
        let out = make_output(r#"{"status":"ok","data":{}}"#, "", 0);
        let v = v_has(&["status", "data"]);
        assert!(v.check(&out).is_ok());

        let v_missing = v_has(&["status", "missing_field"]);
        assert!(v_missing.check(&out).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_validation_json_field_equals() -> ::xtask::sandbox::TestResult<()> {
        let out = make_output(r#"{"status":"success"}"#, "", 0);
        let v = v_eq("status", serde_json::json!("success"));
        assert!(v.check(&out).is_ok());

        let v_wrong = v_eq("status", serde_json::json!("failure"));
        assert!(v_wrong.check(&out).is_err());

        let v_missing = v_eq("nonexistent", serde_json::json!("x"));
        assert!(v_missing.check(&out).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_validation_json_array_min_len() -> ::xtask::sandbox::TestResult<()> {
        let out = make_output(r#"{"items":[1,2,3]}"#, "", 0);
        let v = v_arr_min("items", 2);
        assert!(v.check(&out).is_ok());

        let v_too_few = v_arr_min("items", 5);
        assert!(v_too_few.check(&out).is_err());

        // Field missing
        let v_missing = v_arr_min("no_such_field", 1);
        assert!(v_missing.check(&out).is_err());

        // Not an array
        let not_arr = make_output(r#"{"items":"hello"}"#, "", 0);
        assert!(v_arr_min("items", 1).check(&not_arr).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_validation_stdout_contains() -> ::xtask::sandbox::TestResult<()> {
        let out = make_output("hello world", "", 0);
        assert!(v_contains("hello").check(&out).is_ok());
        assert!(v_contains("missing").check(&out).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_validation_stdout_not_contains() -> ::xtask::sandbox::TestResult<()> {
        let out = make_output("hello world", "", 0);
        let v = Validation::StdoutNotContains("absent".to_string());
        assert!(v.check(&out).is_ok());

        let v_present = Validation::StdoutNotContains("hello".to_string());
        assert!(v_present.check(&out).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_validation_stderr_contains() -> ::xtask::sandbox::TestResult<()> {
        let out = make_output("", "No command specified", 1);
        assert!(v_stderr("No command").check(&out).is_ok());
        assert!(v_stderr("missing phrase").check(&out).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_validation_stdout_empty() -> ::xtask::sandbox::TestResult<()> {
        let empty = make_output("   \n  ", "", 0);
        assert!(v_empty().check(&empty).is_ok());

        let non_empty = make_output("some output", "", 0);
        assert!(v_empty().check(&non_empty).is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_validation_stdout_line_count() -> ::xtask::sandbox::TestResult<()> {
        let three_lines = make_output("a\nb\nc", "", 0);

        assert!(v_lines(Some(1), Some(5)).check(&three_lines).is_ok());
        assert!(v_lines(Some(3), Some(3)).check(&three_lines).is_ok());
        assert!(v_lines(Some(4), None).check(&three_lines).is_err());
        assert!(v_lines(None, Some(2)).check(&three_lines).is_err());
        assert!(v_lines(None, None).check(&three_lines).is_ok());
        Ok(())
    }

    // ── validate_step ─────────────────────────────────────────────────────────

    #[sinex_test]
    async fn test_validate_step_exit_success_passes() -> ::xtask::sandbox::TestResult<()> {
        let out = make_output("", "", 0);
        let errs = validate_step(&out, &ExpectedExit::Success, &[]);
        assert!(errs.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_validate_step_exit_success_fails_on_nonzero() -> ::xtask::sandbox::TestResult<()>
    {
        let out = make_output("", "", 1);
        let errs = validate_step(&out, &ExpectedExit::Success, &[]);
        assert_eq!(errs.len(), 1);
        assert!(errs[0].contains("expected exit 0"));
        Ok(())
    }

    #[sinex_test]
    async fn test_validate_step_exit_failure_passes_on_nonzero() -> ::xtask::sandbox::TestResult<()>
    {
        let out = make_output("", "", 2);
        let errs = validate_step(&out, &ExpectedExit::Failure, &[]);
        assert!(errs.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_validate_step_exit_failure_fails_on_zero() -> ::xtask::sandbox::TestResult<()> {
        let out = make_output("", "", 0);
        let errs = validate_step(&out, &ExpectedExit::Failure, &[]);
        assert_eq!(errs.len(), 1);
        assert!(errs[0].contains("expected non-zero exit"));
        Ok(())
    }

    #[sinex_test]
    async fn test_validate_step_any_accepts_any_exit() -> ::xtask::sandbox::TestResult<()> {
        for code in [0, 1, 2, 127] {
            let out = make_output("", "", code);
            let errs = validate_step(&out, &ExpectedExit::Any, &[]);
            assert!(
                errs.is_empty(),
                "exit code {code} should be accepted by Any"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_validate_step_collects_multiple_errors() -> ::xtask::sandbox::TestResult<()> {
        let out = make_output("", "", 1); // non-zero exit
        let errs = validate_step(
            &out,
            &ExpectedExit::Success,           // exit error
            &[v_contains("expected phrase")], // validation error
        );
        assert_eq!(errs.len(), 2);
        Ok(())
    }

    // ── build_catalog ─────────────────────────────────────────────────────────

    #[sinex_test]
    async fn test_catalog_has_exercises_in_all_tiers() -> ::xtask::sandbox::TestResult<()> {
        let catalog = build_catalog();
        let t1: Vec<_> = catalog.iter().filter(|e| e.tier == Tier::T1).collect();
        let t2: Vec<_> = catalog.iter().filter(|e| e.tier == Tier::T2).collect();
        let t3: Vec<_> = catalog.iter().filter(|e| e.tier == Tier::T3).collect();
        let t4: Vec<_> = catalog.iter().filter(|e| e.tier == Tier::T4).collect();
        assert!(!t1.is_empty(), "T1 should have exercises");
        assert!(!t2.is_empty(), "T2 should have exercises");
        assert!(!t3.is_empty(), "T3 should have exercises");
        assert!(!t4.is_empty(), "T4 should have exercises");
        Ok(())
    }

    #[sinex_test]
    async fn test_catalog_ids_are_unique() -> ::xtask::sandbox::TestResult<()> {
        let catalog = build_catalog();
        let mut seen = std::collections::HashSet::new();
        for ex in &catalog {
            assert!(
                seen.insert(ex.id.clone()),
                "duplicate exercise ID: {}",
                ex.id
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_catalog_ids_match_tier_prefix() -> ::xtask::sandbox::TestResult<()> {
        let catalog = build_catalog();
        for ex in &catalog {
            let expected_prefix = match ex.tier {
                Tier::T1 => "t1.",
                Tier::T2 => "t2.",
                Tier::T3 => "t3.",
                Tier::T4 => "t4.",
            };
            assert!(
                ex.id.starts_with(expected_prefix),
                "exercise '{}' has tier {:?} but id doesn't start with '{}'",
                ex.id,
                ex.tier,
                expected_prefix
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_catalog_descriptions_non_empty() -> ::xtask::sandbox::TestResult<()> {
        let catalog = build_catalog();
        for ex in &catalog {
            assert!(
                !ex.description.is_empty(),
                "exercise '{}' has an empty description",
                ex.id
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_catalog_declarative_exercises_have_steps() -> ::xtask::sandbox::TestResult<()> {
        let catalog = build_catalog();
        for ex in &catalog {
            if let ExerciseKind::Declarative(steps) = &ex.kind {
                assert!(
                    !steps.is_empty(),
                    "declarative exercise '{}' has no steps",
                    ex.id
                );
            }
        }
        Ok(())
    }

    // ── Command metadata ──────────────────────────────────────────────────────

    #[sinex_test]
    async fn test_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = ExerciseCommand {
            all: false,
            tiers: vec![],
            exercises: vec![],
            list: false,
            dry_run: false,
            skip_infra: false,
            verbose: false,
            fail_fast: false,
            ..ExerciseCommand::default()
        };
        assert_eq!(cmd.name(), "exercise");
        Ok(())
    }

    #[sinex_test]
    async fn test_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = ExerciseCommand {
            all: true,
            ..ExerciseCommand::default()
        };
        let meta = cmd.metadata();
        assert_eq!(meta.category, Some("test"));
        assert!(!meta.modifies_state);
        assert!(meta.track_in_history);
        assert!(meta.timeout.is_some());
        Ok(())
    }

    // ── Builder helpers ───────────────────────────────────────────────────────

    #[sinex_test]
    async fn test_def_builder_defaults() -> ::xtask::sandbox::TestResult<()> {
        use crate::commands::exercise::builders::def;
        let ex = def("t1.test_id", "A test exercise", Tier::T1);
        assert_eq!(ex.id, "t1.test_id");
        assert_eq!(ex.description, "A test exercise");
        assert_eq!(ex.tier, Tier::T1);
        assert_eq!(ex.infra, InfraReq::None);
        // Declarative with no steps by default
        assert!(matches!(ex.kind, ExerciseKind::Declarative(ref s) if s.is_empty()));
        Ok(())
    }

    #[sinex_test]
    async fn test_def_builder_custom() -> ::xtask::sandbox::TestResult<()> {
        use crate::commands::exercise::builders::def;
        let ex = def("t4.custom", "Custom exercise", Tier::T4).custom();
        assert!(matches!(ex.kind, ExerciseKind::Custom));
        Ok(())
    }

    #[sinex_test]
    async fn test_def_builder_infra() -> ::xtask::sandbox::TestResult<()> {
        use crate::commands::exercise::builders::def;
        let ex = def("t3.infra", "With infra", Tier::T3).infra(InfraReq::Postgres);
        assert_eq!(ex.infra, InfraReq::Postgres);
        Ok(())
    }

    #[sinex_test]
    async fn test_step_builder() -> ::xtask::sandbox::TestResult<()> {
        use crate::commands::exercise::builders::step;
        let s = step("my step", &["check", "--json"]);
        assert_eq!(s.label, "my step");
        assert_eq!(s.args, vec!["check", "--json"]);
        assert!(s.validations.is_empty());
        assert!(matches!(s.expected_exit, ExpectedExit::Success));
        Ok(())
    }

    #[sinex_test]
    async fn test_step_builder_with_exit_and_validations() -> ::xtask::sandbox::TestResult<()> {
        use crate::commands::exercise::builders::step;
        let s = step("bad", &["check"])
            .exit(ExpectedExit::Failure)
            .v(v_contains("error"));
        assert!(matches!(s.expected_exit, ExpectedExit::Failure));
        assert_eq!(s.validations.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_def_builder_step_accumulates() -> ::xtask::sandbox::TestResult<()> {
        use crate::commands::exercise::builders::{def, step};
        let ex = def("t1.multi", "Multi-step", Tier::T1)
            .step(step("step1", &["check"]))
            .step(step("step2", &["test"]));
        match &ex.kind {
            ExerciseKind::Declarative(steps) => {
                assert_eq!(steps.len(), 2);
                assert_eq!(steps[0].label, "step1");
                assert_eq!(steps[1].label, "step2");
            }
            ExerciseKind::Custom => panic!("expected Declarative"),
        }
        Ok(())
    }

    // ── build_report ─────────────────────────────────────────────────────────

    #[sinex_test]
    async fn test_build_report_all_passed() -> ::xtask::sandbox::TestResult<()> {
        use crate::commands::exercise::builders::def;
        let catalog = vec![def("t1.a", "Exercise A", Tier::T1)];
        let outcomes = vec![ExerciseOutcome {
            id: "t1.a".to_string(),
            passed: true,
            duration: Duration::from_secs(1),
            steps: vec![],
            error: None,
        }];
        let report = build_report(
            &outcomes,
            &catalog,
            0,
            Duration::from_secs(1),
            std::path::Path::new("/tmp"),
        );
        assert_eq!(report.status, "success");
        assert_eq!(report.passed, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(report.total, 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_build_report_all_failed() -> ::xtask::sandbox::TestResult<()> {
        use crate::commands::exercise::builders::def;
        let catalog = vec![def("t1.a", "Exercise A", Tier::T1)];
        let outcomes = vec![ExerciseOutcome {
            id: "t1.a".to_string(),
            passed: false,
            duration: Duration::from_millis(500),
            steps: vec![],
            error: Some("it broke".to_string()),
        }];
        let report = build_report(
            &outcomes,
            &catalog,
            0,
            Duration::from_secs(1),
            std::path::Path::new("/tmp"),
        );
        assert_eq!(report.status, "failed");
        assert_eq!(report.passed, 0);
        assert_eq!(report.failed, 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_build_report_partial() -> ::xtask::sandbox::TestResult<()> {
        use crate::commands::exercise::builders::def;
        let catalog = vec![
            def("t1.a", "Exercise A", Tier::T1),
            def("t1.b", "Exercise B", Tier::T1),
        ];
        let outcomes = vec![
            ExerciseOutcome {
                id: "t1.a".to_string(),
                passed: true,
                duration: Duration::ZERO,
                steps: vec![],
                error: None,
            },
            ExerciseOutcome {
                id: "t1.b".to_string(),
                passed: false,
                duration: Duration::ZERO,
                steps: vec![],
                error: None,
            },
        ];
        let report = build_report(
            &outcomes,
            &catalog,
            0,
            Duration::from_secs(2),
            std::path::Path::new("/tmp"),
        );
        assert_eq!(report.status, "partial");
        assert_eq!(report.passed, 1);
        assert_eq!(report.failed, 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_build_report_skipped_counted_in_total() -> ::xtask::sandbox::TestResult<()> {
        use crate::commands::exercise::builders::def;
        let catalog = vec![def("t1.a", "A", Tier::T1)];
        let outcomes = vec![ExerciseOutcome {
            id: "t1.a".to_string(),
            passed: true,
            duration: Duration::ZERO,
            steps: vec![],
            error: None,
        }];
        let report = build_report(
            &outcomes,
            &catalog,
            3, // 3 skipped
            Duration::from_secs(1),
            std::path::Path::new("/tmp"),
        );
        // total = outcomes (1) + skipped (3)
        assert_eq!(report.total, 4);
        assert_eq!(report.skipped, 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_build_report_entries_have_tier() -> ::xtask::sandbox::TestResult<()> {
        use crate::commands::exercise::builders::def;
        let catalog = vec![def("t2.foo", "Foo", Tier::T2)];
        let outcomes = vec![ExerciseOutcome {
            id: "t2.foo".to_string(),
            passed: true,
            duration: Duration::from_millis(100),
            steps: vec![StepOutcome {
                label: "step1".to_string(),
                passed: true,
                exit_code: 0,
                duration: Duration::from_millis(50),
                validation_errors: vec![],
            }],
            error: None,
        }];
        let report = build_report(
            &outcomes,
            &catalog,
            0,
            Duration::from_millis(100),
            std::path::Path::new("/tmp"),
        );
        assert_eq!(report.results.len(), 1);
        assert_eq!(report.results[0].tier, "T2");
        assert_eq!(report.results[0].steps.len(), 1);
        assert_eq!(report.results[0].steps[0].label, "step1");
        Ok(())
    }
}
