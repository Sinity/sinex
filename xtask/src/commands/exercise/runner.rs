use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use color_eyre::eyre::{Result, WrapErr};

use super::custom::*;
use super::types::{
    ExerciseDef, ExerciseKind, ExerciseOutcome, ExerciseReport, ExpectedExit, ReportEntry,
    StepEntry, StepOutcome, StepOutput, Validation,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Git state guard (RAII for T4 affected exercises)
// ═══════════════════════════════════════════════════════════════════════════════

pub struct GitStateGuard {
    pub stash_created: bool,
    pub touched_files: Vec<PathBuf>,
}

impl GitStateGuard {
    pub fn new() -> Result<Self> {
        let status = Command::new("git")
            .args(["status", "--porcelain"])
            .output()
            .context("git status")?;
        let has_changes = !status.stdout.is_empty();

        let stash_created = if has_changes {
            Command::new("git")
                .args(["stash", "push", "-m", "xtask-exercise-guard"])
                .status()
                .is_ok_and(|s| s.success())
        } else {
            false
        };

        Ok(Self {
            stash_created,
            touched_files: Vec::new(),
        })
    }

    pub fn touch_file(&mut self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(path)
            .with_context(|| format!("touch {}", path.display()))?;
        file.write_all(b"\n")?;
        self.touched_files.push(path.to_path_buf());
        Ok(())
    }
}

impl Drop for GitStateGuard {
    fn drop(&mut self) {
        for file in &self.touched_files {
            let _ = Command::new("git")
                .args(["checkout", "--"])
                .arg(file)
                .status();
        }
        if self.stash_created {
            let _ = Command::new("git").args(["stash", "pop"]).status();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Runner functions
// ═══════════════════════════════════════════════════════════════════════════════

pub fn run_xtask(args: &[&str], env: &[(&str, &str)], verbose: bool) -> StepOutput {
    let start = Instant::now();
    let mut cmd = Command::new("xtask");
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let result = cmd.output();
    let duration = start.elapsed();

    let output = match result {
        Ok(out) => StepOutput {
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            exit_code: out.status.code().unwrap_or(-1),
            duration,
        },
        Err(e) => StepOutput {
            stdout: String::new(),
            stderr: format!("Failed to execute xtask: {e}"),
            exit_code: -1,
            duration,
        },
    };

    if verbose {
        if !output.stdout.is_empty() {
            print!("{}", output.stdout);
        }
        if !output.stderr.is_empty() {
            eprint!("{}", output.stderr);
        }
    }

    output
}

pub fn save_output(dir: &Path, prefix: &str, output: &StepOutput) {
    let _ = fs::write(dir.join(format!("{prefix}.stdout.log")), &output.stdout);
    let _ = fs::write(dir.join(format!("{prefix}.stderr.log")), &output.stderr);
}

pub fn validate_step(
    output: &StepOutput,
    expected_exit: &ExpectedExit,
    validations: &[Validation],
) -> Vec<String> {
    let mut errors = Vec::new();

    match expected_exit {
        ExpectedExit::Success if output.exit_code != 0 => {
            errors.push(format!("expected exit 0, got {}", output.exit_code));
        }
        ExpectedExit::Failure if output.exit_code == 0 => {
            errors.push("expected non-zero exit, got 0".to_string());
        }
        _ => {}
    }

    for v in validations {
        if let Err(e) = v.check(output) {
            errors.push(e);
        }
    }

    errors
}

/// Run and validate a single step, returning both the outcome and raw output.
pub fn exec_step(
    dir: &Path,
    idx: usize,
    label: &str,
    args: &[&str],
    expected: ExpectedExit,
    validations: &[Validation],
    verbose: bool,
) -> (StepOutcome, StepOutput) {
    let output = run_xtask(args, &[], verbose);
    let prefix = format!("step_{}_{}", idx, label.replace(' ', "_"));
    save_output(dir, &prefix, &output);
    let errors = validate_step(&output, &expected, validations);
    let outcome = StepOutcome {
        label: label.to_string(),
        passed: errors.is_empty(),
        exit_code: output.exit_code,
        duration: output.duration,
        validation_errors: errors,
    };
    (outcome, output)
}

pub fn run_declarative_exercise(
    def: &ExerciseDef,
    output_dir: &Path,
    verbose: bool,
) -> ExerciseOutcome {
    let start = Instant::now();
    let steps = match &def.kind {
        ExerciseKind::Declarative(steps) => steps,
        ExerciseKind::Custom => unreachable!("called run_declarative on custom exercise"),
    };

    let mut outcomes = Vec::new();
    for (i, step) in steps.iter().enumerate() {
        let args: Vec<&str> = step.args.iter().map(String::as_str).collect();
        let env: Vec<(&str, &str)> = step
            .env
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let output = run_xtask(&args, &env, verbose);
        let prefix = format!("step_{}_{}", i, step.label.replace(' ', "_"));
        save_output(output_dir, &prefix, &output);
        let errors = validate_step(&output, &step.expected_exit, &step.validations);
        outcomes.push(StepOutcome {
            label: step.label.clone(),
            passed: errors.is_empty(),
            exit_code: output.exit_code,
            duration: output.duration,
            validation_errors: errors,
        });
    }

    let passed = outcomes.iter().all(|s| s.passed);
    ExerciseOutcome {
        id: def.id.clone(),
        passed,
        duration: start.elapsed(),
        steps: outcomes,
        error: None,
    }
}

pub fn run_custom_exercise(def: &ExerciseDef, output_dir: &Path, verbose: bool) -> ExerciseOutcome {
    let start = Instant::now();
    let steps = match def.id.as_str() {
        "t4.bg_job_lifecycle" => custom_bg_job_lifecycle(output_dir, verbose),
        "t4.affected_clean" => custom_affected_clean(output_dir, verbose),
        "t4.affected_leaf" => custom_affected_leaf(output_dir, verbose),
        "t4.affected_foundation" => custom_affected_foundation(output_dir, verbose),
        "t4.affected_workspace" => custom_affected_workspace(output_dir, verbose),
        "t4.history_roundtrip" => custom_history_roundtrip(output_dir, verbose),
        "t4.output_format_matrix" => custom_output_format_matrix(output_dir, verbose),
        "t4.jobs_prune" => custom_jobs_prune(output_dir, verbose),
        "t4.coord_fresh_check" => custom_coord_fresh_check(output_dir, verbose),
        "t4.coord_attach_check" => custom_coord_attach_check(output_dir, verbose),
        "t4.coord_scope_isolation" => custom_coord_scope_isolation(output_dir, verbose),
        "t4.coord_state_update" => custom_coord_state_update(output_dir, verbose),
        "t4.coord_supersede" => custom_coord_supersede(output_dir, verbose),
        "t4.coord_queue_no_overwrite" => custom_coord_queue_no_overwrite(output_dir, verbose),
        "t4.affected_transitive" => custom_affected_transitive(output_dir, verbose),
        "t4.jobs_output_while_running" => custom_jobs_output_while_running(output_dir, verbose),
        "t4.preflight_stages_in_history" => custom_preflight_stages_in_history(output_dir, verbose),
        "t4.live_stage_visible_during_run" => {
            custom_live_stage_visible_during_run(output_dir, verbose)
        }
        "t4.diagnostic_delta_roundtrip" => custom_diagnostic_delta_roundtrip(output_dir, verbose),
        "t4.history_stages_populated" => custom_history_stages_populated(output_dir, verbose),
        "t4.analytics_recommend_runs" => custom_analytics_recommend_runs(output_dir, verbose),
        other => {
            return ExerciseOutcome {
                id: def.id.clone(),
                passed: false,
                duration: start.elapsed(),
                steps: vec![],
                error: Some(format!("unknown custom exercise: {other}")),
            };
        }
    };

    let passed = steps.iter().all(|s| s.passed);
    ExerciseOutcome {
        id: def.id.clone(),
        passed,
        duration: start.elapsed(),
        steps,
        error: None,
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Output directory & reporting
// ═══════════════════════════════════════════════════════════════════════════════

pub fn setup_output_dir() -> Result<PathBuf> {
    let timestamp = Command::new("date")
        .arg("+%Y%m%d-%H%M%S")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map_or_else(
            || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
                    .to_string()
            },
            |s| s.trim().to_string(),
        );

    let base = PathBuf::from("target/exercise");
    let run_dir = base.join(format!("run-{timestamp}"));
    fs::create_dir_all(&run_dir).context("create exercise output directory")?;

    // Symlink target/exercise/latest → run-<timestamp>
    let latest = base.join("latest");
    let _ = fs::remove_file(&latest);
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        // Use just the directory name (not full path) since the symlink lives in the same parent
        let _ = symlink(run_dir.file_name().unwrap(), &latest);
    }

    Ok(run_dir)
}

pub fn build_report(
    outcomes: &[ExerciseOutcome],
    catalog: &[ExerciseDef],
    skipped: usize,
    total_duration: Duration,
    output_dir: &Path,
) -> ExerciseReport {
    let passed = outcomes.iter().filter(|o| o.passed).count();
    let failed = outcomes.iter().filter(|o| !o.passed).count();
    let status = if failed == 0 {
        "success"
    } else if passed == 0 {
        "failed"
    } else {
        "partial"
    };

    ExerciseReport {
        status: status.to_string(),
        total: outcomes.len() + skipped,
        passed,
        failed,
        skipped,
        duration_secs: total_duration.as_secs_f64(),
        output_dir: output_dir.display().to_string(),
        results: outcomes
            .iter()
            .map(|o| {
                let tier = catalog
                    .iter()
                    .find(|d| d.id == o.id)
                    .map(|d| d.tier.label().to_string())
                    .unwrap_or_default();
                ReportEntry {
                    id: o.id.clone(),
                    tier,
                    passed: o.passed,
                    duration_secs: o.duration.as_secs_f64(),
                    error: o.error.clone(),
                    steps: o
                        .steps
                        .iter()
                        .map(|s| StepEntry {
                            label: s.label.clone(),
                            passed: s.passed,
                            exit_code: s.exit_code,
                            duration_secs: s.duration.as_secs_f64(),
                            validation_errors: s.validation_errors.clone(),
                        })
                        .collect(),
                }
            })
            .collect(),
    }
}

pub fn print_human_summary(outcomes: &[ExerciseOutcome], skipped: usize, total_duration: Duration) {
    let passed = outcomes.iter().filter(|o| o.passed).count();
    let failed = outcomes.iter().filter(|o| !o.passed).count();

    println!();
    println!("═══════════════════════════════════════════");
    println!("  Exercise Summary");
    println!("═══════════════════════════════════════════");
    println!();

    if skipped > 0 {
        println!("  Skipped: {skipped} (infrastructure required)");
    }
    println!(
        "  Total: {}  Passed: \x1b[32m{passed}\x1b[0m  Failed: {}  ({:.1}s)",
        outcomes.len(),
        if failed > 0 {
            format!("\x1b[31m{failed}\x1b[0m")
        } else {
            "0".to_string()
        },
        total_duration.as_secs_f64()
    );
    println!();
}

pub fn run_affected_exercise(
    dir: &Path,
    verbose: bool,
    file_to_touch: &str,
    expected_present: &[&str],
    expected_absent: &[&str],
) -> Vec<StepOutcome> {
    use super::builders::v_json;

    let mut steps = Vec::new();

    let mut guard = match GitStateGuard::new() {
        Ok(g) => g,
        Err(e) => {
            steps.push(StepOutcome {
                label: "setup".into(),
                passed: false,
                exit_code: -1,
                duration: Duration::ZERO,
                validation_errors: vec![format!("git guard setup failed: {e}")],
            });
            return steps;
        }
    };

    if let Err(e) = guard.touch_file(file_to_touch) {
        steps.push(StepOutcome {
            label: "touch".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec![format!("touch file failed: {e}")],
        });
        return steps;
    }

    let (mut outcome, output) = exec_step(
        dir,
        0,
        "build_affected",
        &["build", "--affected=true", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );

    // Combine stdout + stderr for searching (affected info may appear in either)
    let combined = format!("{}{}", output.stdout, output.stderr);
    for pkg in expected_present {
        if !combined.contains(pkg) {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("expected '{pkg}' in affected output"));
        }
    }
    for pkg in expected_absent {
        if combined.contains(pkg) {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("'{pkg}' should not appear in affected output"));
        }
    }
    steps.push(outcome);

    drop(guard);
    steps
}
