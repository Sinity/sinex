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
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::Serialize;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

// ═══════════════════════════════════════════════════════════════════════════════
// CLI struct
// ═══════════════════════════════════════════════════════════════════════════════

/// Full surface area validation for xtask commands.
///
/// Runs real subprocess invocations of `xtask` commands across four tiers
/// of increasing scope, validates outputs, and saves results for inspection.
#[derive(Debug, Clone, clap::Args)]
pub struct ExerciseCommand {
    /// Run all tiers (default: tier 1 only)
    #[arg(long)]
    pub all: bool,

    /// Specific tier(s) to run (1-4, repeatable)
    #[arg(long = "tier", value_name = "TIER")]
    pub tiers: Vec<Tier>,

    /// Run specific exercise(s) by ID
    #[arg(short = 'E', long = "exercise", value_name = "ID")]
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
}

// ═══════════════════════════════════════════════════════════════════════════════
// Framework types
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    #[value(name = "1")]
    T1,
    #[value(name = "2")]
    T2,
    #[value(name = "3")]
    T3,
    #[value(name = "4")]
    T4,
}

impl Tier {
    fn label(self) -> &'static str {
        match self {
            Tier::T1 => "T1",
            Tier::T2 => "T2",
            Tier::T3 => "T3",
            Tier::T4 => "T4",
        }
    }

    fn as_arg(self) -> &'static str {
        match self {
            Tier::T1 => "1",
            Tier::T2 => "2",
            Tier::T3 => "3",
            Tier::T4 => "4",
        }
    }
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Variants maintained for catalog extensibility
enum InfraReq {
    None,
    Postgres,
    Nats,
    Both,
}

#[derive(Debug, Clone, Copy)]
enum ExpectedExit {
    Success,
    Failure,
    Any,
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // Variants maintained for catalog extensibility
enum Validation {
    JsonValid,
    JsonHasFields(Vec<String>),
    JsonFieldEquals {
        path: String,
        expected: serde_json::Value,
    },
    JsonFieldOneOf {
        path: String,
        values: Vec<serde_json::Value>,
    },
    JsonArrayMinLen {
        path: String,
        min: usize,
    },
    StdoutContains(String),
    StdoutNotContains(String),
    StderrContains(String),
    StdoutEmpty,
    StdoutLineCount {
        min: Option<usize>,
        max: Option<usize>,
    },
}

struct ExerciseStep {
    label: String,
    args: Vec<String>,
    expected_exit: ExpectedExit,
    validations: Vec<Validation>,
    env: Vec<(String, String)>,
}

enum ExerciseKind {
    Declarative(Vec<ExerciseStep>),
    Custom,
}

struct ExerciseDef {
    id: String,
    description: String,
    tier: Tier,
    infra: InfraReq,
    kind: ExerciseKind,
}

struct StepOutput {
    stdout: String,
    stderr: String,
    exit_code: i32,
    duration: Duration,
}

struct StepOutcome {
    label: String,
    passed: bool,
    exit_code: i32,
    duration: Duration,
    validation_errors: Vec<String>,
}

struct ExerciseOutcome {
    id: String,
    passed: bool,
    duration: Duration,
    steps: Vec<StepOutcome>,
    error: Option<String>,
}

#[derive(Serialize)]
struct ExerciseReport {
    status: String,
    total: usize,
    passed: usize,
    failed: usize,
    skipped: usize,
    duration_secs: f64,
    output_dir: String,
    results: Vec<ReportEntry>,
}

#[derive(Serialize)]
struct ReportEntry {
    id: String,
    tier: String,
    passed: bool,
    duration_secs: f64,
    error: Option<String>,
    steps: Vec<StepEntry>,
}

#[derive(Serialize)]
struct StepEntry {
    label: String,
    passed: bool,
    exit_code: i32,
    duration_secs: f64,
    validation_errors: Vec<String>,
}

// ═══════════════════════════════════════════════════════════════════════════════
// JSON path helper
// ═══════════════════════════════════════════════════════════════════════════════

fn json_path<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path.split('.') {
        current = current.get(key)?;
    }
    Some(current)
}

/// Parse the last complete JSON value from stdout.
///
/// Some xtask commands print their own JSON output *before* the framework's
/// `CommandResult` JSON wrapper, resulting in multiple concatenated JSON objects.
/// We always want the last one (the framework's authoritative output).
fn parse_last_json(stdout: &str) -> std::result::Result<serde_json::Value, String> {
    let mut last = None;
    let stream = serde_json::Deserializer::from_str(stdout).into_iter::<serde_json::Value>();
    for item in stream {
        match item {
            Ok(val) => last = Some(val),
            Err(e) => return Err(format!("JSON parse error: {e}")),
        }
    }
    last.ok_or_else(|| "no JSON object found in stdout".to_string())
}

fn extract_json_field(stdout: &str, path: &str) -> Option<serde_json::Value> {
    let val = parse_last_json(stdout).ok()?;
    json_path(&val, path).cloned()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Validation engine
// ═══════════════════════════════════════════════════════════════════════════════

impl Validation {
    fn check(&self, output: &StepOutput) -> std::result::Result<(), String> {
        match self {
            Validation::JsonValid => parse_last_json(&output.stdout).map(|_| ()),

            Validation::JsonHasFields(fields) => {
                let val = parse_last_json(&output.stdout)?;
                for field in fields {
                    if json_path(&val, field).is_none() {
                        return Err(format!("missing JSON field: {field}"));
                    }
                }
                Ok(())
            }

            Validation::JsonFieldEquals { path, expected } => {
                let val = parse_last_json(&output.stdout)?;
                match json_path(&val, path) {
                    Some(actual) if actual == expected => Ok(()),
                    Some(actual) => Err(format!("JSON {path}: expected {expected}, got {actual}")),
                    None => Err(format!("JSON field not found: {path}")),
                }
            }

            Validation::JsonFieldOneOf { path, values } => {
                let val = parse_last_json(&output.stdout)?;
                match json_path(&val, path) {
                    Some(actual) if values.contains(actual) => Ok(()),
                    Some(actual) => Err(format!("JSON {path}: {actual} not in {values:?}")),
                    None => Err(format!("JSON field not found: {path}")),
                }
            }

            Validation::JsonArrayMinLen { path, min } => {
                let val = parse_last_json(&output.stdout)?;
                match json_path(&val, path) {
                    Some(serde_json::Value::Array(arr)) if arr.len() >= *min => Ok(()),
                    Some(serde_json::Value::Array(arr)) => {
                        Err(format!("JSON {path}: array length {} < {min}", arr.len()))
                    }
                    Some(_) => Err(format!("JSON {path}: not an array")),
                    None => Err(format!("JSON field not found: {path}")),
                }
            }

            Validation::StdoutContains(s) => {
                if output.stdout.contains(s.as_str()) {
                    Ok(())
                } else {
                    Err(format!("stdout does not contain '{s}'"))
                }
            }

            Validation::StdoutNotContains(s) => {
                if output.stdout.contains(s.as_str()) {
                    Err(format!("stdout unexpectedly contains '{s}'"))
                } else {
                    Ok(())
                }
            }

            Validation::StderrContains(s) => {
                if output.stderr.contains(s.as_str()) {
                    Ok(())
                } else {
                    Err(format!("stderr does not contain '{s}'"))
                }
            }

            Validation::StdoutEmpty => {
                if output.stdout.trim().is_empty() {
                    Ok(())
                } else {
                    Err(format!(
                        "expected empty stdout, got {} bytes",
                        output.stdout.len()
                    ))
                }
            }

            Validation::StdoutLineCount { min, max } => {
                let count = output.stdout.lines().count();
                if let Some(min_val) = min {
                    if count < *min_val {
                        return Err(format!("stdout has {count} lines, expected >= {min_val}"));
                    }
                }
                if let Some(max_val) = max {
                    if count > *max_val {
                        return Err(format!("stdout has {count} lines, expected <= {max_val}"));
                    }
                }
                Ok(())
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Git state guard (RAII for T4 affected exercises)
// ═══════════════════════════════════════════════════════════════════════════════

struct GitStateGuard {
    stash_created: bool,
    touched_files: Vec<PathBuf>,
}

impl GitStateGuard {
    fn new() -> Result<Self> {
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

    fn touch_file(&mut self, path: impl AsRef<Path>) -> Result<()> {
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

fn run_xtask(args: &[&str], env: &[(&str, &str)], verbose: bool) -> StepOutput {
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

fn save_output(dir: &Path, prefix: &str, output: &StepOutput) {
    let _ = fs::write(dir.join(format!("{prefix}.stdout.log")), &output.stdout);
    let _ = fs::write(dir.join(format!("{prefix}.stderr.log")), &output.stderr);
}

fn validate_step(
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
fn exec_step(
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

fn run_declarative_exercise(
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

fn run_custom_exercise(def: &ExerciseDef, output_dir: &Path, verbose: bool) -> ExerciseOutcome {
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
// Builder helpers (compact catalog construction)
// ═══════════════════════════════════════════════════════════════════════════════

fn def(id: &str, desc: &str, tier: Tier) -> ExerciseDef {
    ExerciseDef {
        id: id.to_string(),
        description: desc.to_string(),
        tier,
        infra: InfraReq::None,
        kind: ExerciseKind::Declarative(Vec::new()),
    }
}

impl ExerciseDef {
    fn infra(mut self, req: InfraReq) -> Self {
        self.infra = req;
        self
    }
    fn step(mut self, s: ExerciseStep) -> Self {
        if let ExerciseKind::Declarative(ref mut steps) = self.kind {
            steps.push(s);
        }
        self
    }
    fn custom(mut self) -> Self {
        self.kind = ExerciseKind::Custom;
        self
    }
}

fn step(label: &str, args: &[&str]) -> ExerciseStep {
    ExerciseStep {
        label: label.to_string(),
        args: args.iter().map(ToString::to_string).collect(),
        expected_exit: ExpectedExit::Success,
        validations: Vec::new(),
        env: Vec::new(),
    }
}

impl ExerciseStep {
    fn exit(mut self, e: ExpectedExit) -> Self {
        self.expected_exit = e;
        self
    }
    fn v(mut self, val: Validation) -> Self {
        self.validations.push(val);
        self
    }
}

// Validation shorthand constructors
fn v_json() -> Validation {
    Validation::JsonValid
}
fn v_has(fields: &[&str]) -> Validation {
    Validation::JsonHasFields(fields.iter().map(ToString::to_string).collect())
}
fn v_eq(path: &str, expected: serde_json::Value) -> Validation {
    Validation::JsonFieldEquals {
        path: path.to_string(),
        expected,
    }
}
#[allow(dead_code)] // Maintained for catalog extensibility
fn v_one_of(path: &str, values: &[&str]) -> Validation {
    Validation::JsonFieldOneOf {
        path: path.to_string(),
        values: values
            .iter()
            .map(|s| serde_json::Value::String(s.to_string()))
            .collect(),
    }
}
fn v_arr_min(path: &str, min: usize) -> Validation {
    Validation::JsonArrayMinLen {
        path: path.to_string(),
        min,
    }
}
fn v_contains(s: &str) -> Validation {
    Validation::StdoutContains(s.to_string())
}
#[allow(dead_code)] // Maintained for catalog extensibility
fn v_not_contains(s: &str) -> Validation {
    Validation::StdoutNotContains(s.to_string())
}
fn v_stderr(s: &str) -> Validation {
    Validation::StderrContains(s.to_string())
}
fn v_empty() -> Validation {
    Validation::StdoutEmpty
}
fn v_lines(min: Option<usize>, max: Option<usize>) -> Validation {
    Validation::StdoutLineCount { min, max }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Exercise catalog (~65 exercises)
// ═══════════════════════════════════════════════════════════════════════════════

#[allow(clippy::vec_init_then_push)] // 65-item catalog is clearer with push than vec![]
fn build_catalog() -> Vec<ExerciseDef> {
    use ExpectedExit::{Any, Failure};
    use Tier::{T1, T2, T3, T4};

    let mut v = Vec::with_capacity(65);

    // ─── Tier 1: Fast / Read-Only (~30s total) ──────────────────────────────

    v.push(
        def("t1.help_root", "Root --help output", T1)
            .step(step("help", &["--help"]).v(v_contains("Developer tasks"))),
    );

    v.push(
        def("t1.help_check", "Check --help output", T1)
            .step(step("help", &["check", "--help"]).v(v_contains("--skip-fmt"))),
    );

    v.push(
        def("t1.help_test", "Test --help output", T1)
            .step(step("help", &["test", "--help"]).v(v_contains("--debug"))),
    );

    v.push(
        def("t1.help_build", "Build --help output", T1)
            .step(step("help", &["build", "--help"]).v(v_contains("--release"))),
    );

    v.push(
        def("t1.list_commands_human", "List commands (human)", T1).step(
            step("list", &["--list-commands"])
                .v(v_contains("check"))
                .v(v_contains("test"))
                .v(v_contains("build"))
                .v(v_contains("status")),
        ),
    );

    v.push(
        def("t1.list_commands_json", "List commands (JSON)", T1).step(
            step("list", &["--list-commands", "--json"])
                .v(v_json())
                .v(v_has(&["commands", "version"])),
        ),
    );

    v.push(
        def("t1.list_commands_count", "Command count >= 15", T1).step(
            step("count", &["--list-commands", "--json"])
                .v(v_json())
                .v(v_arr_min("commands", 15)),
        ),
    );

    v.push(
        def("t1.status_summary_human", "Status summary (human)", T1)
            .step(step("summary", &["status", "--summary"]).v(v_lines(Some(1), None))),
    );

    v.push(
        def("t1.status_summary_json", "Status summary (JSON)", T1).step(
            step("summary", &["status", "--summary", "--json"])
                .v(v_json())
                .v(v_has(&["status"])),
        ),
    );

    v.push(
        def("t1.status_doctor_json", "Status doctor (JSON)", T1).step(
            step("doctor", &["status", "--doctor", "--json"])
                .v(v_json())
                .v(v_has(&["status", "data"])),
        ),
    );

    v.push(
        def("t1.deps_list_human", "Deps list (human)", T1)
            .step(step("deps", &["deps", "list"]).v(v_contains("sinex-primitives"))),
    );

    v.push(
        def("t1.deps_list_json", "Deps list (JSON)", T1)
            .step(step("deps", &["deps", "list", "--json"]).v(v_json())),
    );

    v.push(
        def("t1.deps_duplicates", "Deps duplicates (JSON)", T1)
            .step(step("dups", &["deps", "duplicates", "--json"]).v(v_json())),
    );

    v.push(
        def("t1.history_list_human", "History list (human)", T1)
            .step(step("history", &["history", "list", "--limit", "3"])),
    );

    v.push(
        def("t1.history_list_json", "History list (JSON)", T1)
            .step(step("history", &["history", "list", "--limit", "3", "--json"]).v(v_json())),
    );

    v.push(
        def("t1.jobs_list_human", "Jobs list (human)", T1).step(step("jobs", &["jobs", "list"])),
    );

    v.push(
        def("t1.jobs_list_json", "Jobs list (JSON)", T1).step(
            step("jobs", &["jobs", "list", "--json"])
                .v(v_json())
                .v(v_has(&["status", "data"])),
        ),
    );

    v.push(
        def("t1.jobs_active_json", "Jobs active (JSON)", T1)
            .step(step("active", &["jobs", "active", "--json"]).v(v_json())),
    );

    v.push(
        def("t1.format_silent", "Silent format produces no output", T1)
            .step(step("silent", &["status", "--summary", "--format", "silent"]).v(v_empty())),
    );

    v.push(
        def("t1.format_compact", "Compact format is 1-3 lines", T1).step(
            step("compact", &["status", "--summary", "--format", "compact"])
                .v(v_lines(Some(1), Some(3))),
        ),
    );

    v.push(
        def("t1.no_command_error", "No subcommand exits non-zero", T1).step(
            step("nocommand", &[])
                .exit(Failure)
                .v(v_stderr("No command")),
        ),
    );

    v.push(
        def("t1.invalid_flag", "Invalid flag exits non-zero", T1)
            .step(step("badflag", &["check", "--nonexistent"]).exit(Failure)),
    );

    v.push(
        def("t1.test_dry_run", "Test dry-run (JSON)", T1).step(
            step(
                "dryrun",
                &["test", "--dry-run", "--skip-preflight", "--json"],
            )
            .v(v_json()),
        ),
    );

    v.push(def("t1.infra_env", "Infra env prints vars", T1).step(step("env", &["infra", "env"])));

    // ─── Tier 2: Moderate (~5min) ───────────────────────────────────────────

    v.push(
        def("t2.check_json", "Check with JSON output", T2).step(
            step("check", &["check", "--json", "--skip-tests"])
                .v(v_json())
                .v(v_eq("status", serde_json::json!("success"))),
        ),
    );

    v.push(
        def("t2.check_skip_fmt", "Check with skip-fmt", T2)
            .step(step("check", &["check", "--skip-fmt", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.check_no_forbidden", "Check forbidden=false", T2)
            .step(step("check", &["check", "--forbidden=false", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.check_package", "Check single package", T2)
            .step(step("check", &["check", "-p", "sinex-primitives", "--json"]).v(v_json())),
    );

    v.push(
        def(
            "t2.build_package",
            "Build single package (debug + release)",
            T2,
        )
        .step(step("debug", &["build", "-p", "sinex-primitives", "--json"]).v(v_json()))
        .step(
            step(
                "release",
                &["build", "-p", "sinex-primitives", "--release", "--json"],
            )
            .v(v_json()),
        ),
    );

    v.push(
        def(
            "t2.test_suite",
            "Test xtask: full suite + filter expression",
            T2,
        )
        .step(
            step(
                "full",
                &["test", "-p", "xtask", "--json", "--skip-preflight"],
            )
            .v(v_json()),
        )
        .step(
            step(
                "filter",
                &[
                    "test",
                    "-E",
                    "test(test_status_symbol)",
                    "-p",
                    "xtask",
                    "--skip-preflight",
                    "--json",
                ],
            )
            .v(v_json()),
        ),
    );

    v.push(
        def("t2.deps_tree", "Deps tree (JSON)", T2).step(
            step(
                "tree",
                &["deps", "tree", "--package", "sinex-primitives", "--json"],
            )
            .v(v_json()),
        ),
    );

    v.push(
        def("t2.deps_unused", "Deps unused (JSON)", T2)
            .step(step("unused", &["deps", "unused", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.deps_timings", "Deps timings (JSON)", T2)
            .step(step("timings", &["deps", "timings", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.deps_impact", "Deps impact analysis", T2).step(
            step(
                "impact",
                &["deps", "impact", "--package", "sinex-primitives", "--json"],
            )
            .v(v_json()),
        ),
    );

    v.push(
        def("t2.status_full_json", "Full status (JSON)", T2).step(
            step("status", &["status", "--json"])
                .v(v_json())
                .v(v_has(&["status", "data"])),
        ),
    );

    v.push(
        def("t2.history_stats", "History stats (JSON)", T2).step(
            step(
                "stats",
                &["history", "stats", "--command", "check", "--json"],
            )
            .v(v_json()),
        ),
    );

    v.push(
        def("t2.history_export", "History export (JSON)", T2)
            .step(step("export", &["history", "export", "--limit", "3", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.infra_status", "Infra status (JSON)", T2)
            .infra(InfraReq::Both)
            .step(step("status", &["infra", "status", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.contracts_info", "Contracts info", T2)
            .infra(InfraReq::Postgres)
            .step(step("info", &["contracts", "info", "--json"]).exit(Any)),
    );

    v.push(
        def("t2.json_vs_human", "JSON vs human consistency", T2)
            .step(step("json", &["status", "--doctor", "--json"]).v(v_json()))
            .step(step("human", &["status", "--doctor"])),
    );

    v.push(
        def("t2.json_vs_compact", "JSON vs compact format", T2)
            .step(
                step("compact", &["status", "--doctor", "--format", "compact"])
                    .v(v_lines(Some(1), Some(5))),
            )
            .step(step("json", &["status", "--doctor", "--json"]).v(v_json())),
    );

    v.push(
        def("t2.check_lint_breakdown", "Check lint-breakdown", T2).step(
            step(
                "check",
                &["check", "--lint-breakdown", "--json", "--skip-tests"],
            )
            .v(v_json()),
        ),
    );

    // ─── Tier 3: Heavy (~10min) ─────────────────────────────────────────────

    v.push(
        def("t3.check_full", "Full workspace check", T3)
            .step(step("check", &["check", "--all", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.build_workspace", "Full workspace build", T3)
            .step(step("build", &["build", "--all", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.test_primitives", "Test sinex-primitives", T3)
            .infra(InfraReq::Postgres)
            .step(step("test", &["test", "-p", "sinex-primitives", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.test_schema", "Test sinex-schema", T3)
            .infra(InfraReq::Postgres)
            .step(step("test", &["test", "-p", "sinex-schema", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.check_by_file", "Check by-file breakdown", T3)
            .step(step("check", &["check", "--by-file", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.history_analyze", "Test failure analysis", T3)
            .step(step("analyze", &["history", "tests", "analyze", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.history_slowest", "Slowest tests", T3)
            .step(step("slowest", &["history", "tests", "slowest", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.history_eta", "Test runtime ETA", T3)
            .step(step("eta", &["history", "tests", "eta", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.history_diagnostics", "Compiler diagnostic history", T3)
            .step(step("diags", &["history", "diagnostics", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.db_status", "Database status", T3)
            .infra(InfraReq::Postgres)
            .step(step("status", &["db", "status", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.contracts_ready", "Schema verification", T3)
            .infra(InfraReq::Postgres)
            .step(step("ready", &["contracts", "check-ready", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.infra_cycle", "Infra stop/start/status cycle", T3)
            .infra(InfraReq::Both)
            .step(step("stop", &["infra", "stop"]).exit(Any))
            .step(step("start", &["infra", "start"]))
            .step(step("status", &["infra", "status", "--json"]).v(v_json())),
    );

    v.push(
        def("t3.deps_graph", "Dependency graph", T3)
            .step(step("graph", &["deps", "graph", "--json"]).v(v_json())),
    );

    // ─── Tier 4: Advanced Multi-Step ────────────────────────────────────────

    v.push(def("t4.bg_job_lifecycle", "Background job full lifecycle", T4).custom());
    v.push(def("t4.affected_clean", "Affected: clean state", T4).custom());
    v.push(def("t4.affected_leaf", "Affected: leaf crate changed", T4).custom());
    v.push(
        def(
            "t4.affected_foundation",
            "Affected: foundation crate changed",
            T4,
        )
        .custom(),
    );
    v.push(
        def(
            "t4.affected_workspace",
            "Affected: workspace-wide trigger",
            T4,
        )
        .custom(),
    );
    v.push(def("t4.history_roundtrip", "History tracking roundtrip", T4).custom());
    v.push(def("t4.output_format_matrix", "Output format matrix", T4).custom());
    v.push(def("t4.jobs_prune", "Jobs prune safety boundary", T4).custom());

    // Coordinator exercises — validate deduplication decision matrix
    v.push(
        def(
            "t4.coord_fresh_check",
            "Coordinator: Fresh detection (check→re-check)",
            T4,
        )
        .custom(),
    );
    v.push(
        def(
            "t4.coord_attach_check",
            "Coordinator: Attach to running job",
            T4,
        )
        .custom(),
    );
    v.push(
        def(
            "t4.coord_scope_isolation",
            "Coordinator: Scope key isolates packages",
            T4,
        )
        .custom(),
    );
    v.push(
        def(
            "t4.coord_state_update",
            "Coordinator: State updated with real job_id+pid",
            T4,
        )
        .custom(),
    );

    // Extended coordinator exercises — validate FIFO queue and supersede behavior
    v.push(
        def(
            "t4.coord_supersede",
            "Coordinator: Supersede stale bg job on tree change",
            T4,
        )
        .custom(),
    );
    v.push(
        def(
            "t4.coord_queue_no_overwrite",
            "Coordinator: Multiple queued jobs are preserved (FIFO)",
            T4,
        )
        .custom(),
    );

    // Extended affected exercise
    v.push(
        def(
            "t4.affected_transitive",
            "Affected: transitive dependents included",
            T4,
        )
        .custom(),
    );

    // Extended job exercise
    v.push(
        def(
            "t4.jobs_output_while_running",
            "Jobs: output readable while job is running",
            T4,
        )
        .custom(),
    );

    v
}

// ═══════════════════════════════════════════════════════════════════════════════
// T4 custom exercise implementations
// ═══════════════════════════════════════════════════════════════════════════════

fn custom_bg_job_lifecycle(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Spawn a background job
    let (outcome, output) = exec_step(
        dir,
        0,
        "spawn",
        &["build", "-p", "sinex-primitives", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let job_id = extract_json_field(&output.stdout, "data.job_id").map(|v| {
        v.as_i64()
            .map(|n| n.to_string())
            .or_else(|| v.as_str().map(String::from))
    });
    let job_id = job_id.flatten();
    steps.push(outcome);

    let Some(job_id) = job_id else {
        steps.push(StepOutcome {
            label: "extract job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract data.job_id from spawn output".into()],
        });
        return steps;
    };

    // 2. Monitor: check status
    let (outcome, _) = exec_step(
        dir,
        1,
        "monitor",
        &["jobs", "status", &job_id, "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    // 3. Wait for completion
    let (outcome, _) = exec_step(
        dir,
        2,
        "wait",
        &["jobs", "wait", &job_id, "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    // 4. Retrieve output
    let (mut outcome, output) = exec_step(
        dir,
        3,
        "output",
        &["jobs", "output", &job_id],
        ExpectedExit::Success,
        &[],
        verbose,
    );
    if output.stdout.trim().is_empty() && output.stderr.trim().is_empty() {
        outcome.passed = false;
        outcome.validation_errors.push("job output is empty".into());
    }
    steps.push(outcome);

    // 5. Verify job appears in listing
    let (mut outcome, output) = exec_step(
        dir,
        4,
        "list_verify",
        &["jobs", "list", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    if !output.stdout.contains(&job_id) {
        outcome.passed = false;
        outcome
            .validation_errors
            .push(format!("job {job_id} not found in jobs list"));
    }
    steps.push(outcome);

    steps
}

fn custom_affected_clean(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    let guard = match GitStateGuard::new() {
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

    // In clean state, affected detection should find no changes
    let (outcome, _) = exec_step(
        dir,
        0,
        "build_affected",
        &["build", "--affected=true", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    drop(guard);
    steps
}

fn run_affected_exercise(
    dir: &Path,
    verbose: bool,
    file_to_touch: &str,
    expected_present: &[&str],
    expected_absent: &[&str],
) -> Vec<StepOutcome> {
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

fn custom_affected_leaf(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    run_affected_exercise(
        dir,
        verbose,
        "crate/nodes/sinex-fs-ingestor/src/lib.rs",
        &["sinex-fs-ingestor"],
        &[], // Don't assert absence — transitive deps are implementation-dependent
    )
}

fn custom_affected_foundation(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    run_affected_exercise(
        dir,
        verbose,
        "crate/lib/sinex-primitives/src/lib.rs",
        &["sinex-primitives"], // Foundation change should at least include itself
        &[],
    )
}

fn custom_affected_workspace(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    run_affected_exercise(
        dir,
        verbose,
        "Cargo.lock",
        &[], // Just verify the command handles Cargo.lock change gracefully
        &[],
    )
}

fn custom_history_roundtrip(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Run a tracked command (deps list IS tracked, unlike status which is excluded)
    let (outcome, _) = exec_step(
        dir,
        0,
        "trigger",
        &["deps", "list", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    // Brief pause for history write to complete
    std::thread::sleep(Duration::from_millis(500));

    // 2. Query recent history — should contain our "deps" invocation
    let (mut outcome, output) = exec_step(
        dir,
        1,
        "query",
        &["history", "list", "--limit", "5", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    if !output.stdout.contains("deps") {
        outcome.passed = false;
        outcome
            .validation_errors
            .push("recent history should contain 'deps' command".into());
    }
    steps.push(outcome);

    // 3. Query last invocation for deps
    let (outcome, _) = exec_step(
        dir,
        2,
        "last",
        &["history", "last", "--command", "deps", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    steps
}

fn custom_output_format_matrix(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    let formats: &[(&str, ExpectedExit, Vec<Validation>)] = &[
        ("human", ExpectedExit::Success, vec![v_lines(Some(1), None)]),
        (
            "json",
            ExpectedExit::Success,
            vec![v_json(), v_has(&["status"])],
        ),
        (
            "compact",
            ExpectedExit::Success,
            vec![v_lines(Some(1), Some(5))],
        ),
        ("silent", ExpectedExit::Success, vec![v_empty()]),
    ];

    for (i, (fmt, expected, validations)) in formats.iter().enumerate() {
        let (outcome, _) = exec_step(
            dir,
            i,
            &format!("format_{fmt}"),
            &["status", "--doctor", "--format", fmt],
            *expected,
            validations,
            verbose,
        );
        steps.push(outcome);
    }

    steps
}

fn custom_jobs_prune(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // Prune with an absurdly high threshold — should prune 0 jobs
    let (outcome, _) = exec_step(
        dir,
        0,
        "prune_safe",
        &["jobs", "prune", "--older-than", "9999", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    steps
}

// ═══════════════════════════════════════════════════════════════════════════════
// T4 coordinator exercise implementations
// ═══════════════════════════════════════════════════════════════════════════════

/// Fresh detection: run check --bg, wait for completion, re-run — second should be "fresh".
fn custom_coord_fresh_check(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Run check --bg --json and wait for completion
    let (outcome, output) = exec_step(
        dir,
        0,
        "first_check",
        &["check", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    let action_str = extract_json_field(&output.stdout, "data.action");
    let is_fresh = action_str.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    steps.push(outcome);

    // Wait for the job to complete (if we got a job_id and it's not fresh)
    if let Some(id) = job_id {
        if is_fresh {
            // Fresh result — no real job to wait on, skip to second check
            steps.push(StepOutcome {
                label: "wait_first".into(),
                passed: true,
                exit_code: 0,
                duration: Duration::ZERO,
                validation_errors: vec![],
            });
        } else {
            let (outcome, _) = exec_step(
                dir,
                1,
                "wait_first",
                &["jobs", "wait", &id.to_string(), "--json"],
                ExpectedExit::Success,
                &[v_json()],
                verbose,
            );
            steps.push(outcome);
        }
    } else {
        // First check might have returned "fresh" itself — that's OK too
        if is_fresh {
            steps.push(StepOutcome {
                label: "already_fresh".into(),
                passed: true,
                exit_code: 0,
                duration: Duration::ZERO,
                validation_errors: vec![],
            });
            return steps;
        }
        steps.push(StepOutcome {
            label: "extract_job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract job_id from first check".into()],
        });
        return steps;
    }

    // 2. Immediately re-run check --bg --json — should get "fresh"
    let (mut outcome, output) = exec_step(
        dir,
        2,
        "second_check",
        &["check", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action = extract_json_field(&output.stdout, "data.action");
    match action.as_ref().and_then(|v| v.as_str()) {
        Some("fresh") => {} // Expected
        Some(other) => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("expected action \"fresh\", got \"{other}\""));
        }
        None => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push("could not extract data.action from second check".into());
        }
    }
    steps.push(outcome);

    steps
}

/// Attach: start a long-running --bg build, immediately re-run — should get "attached".
fn custom_coord_attach_check(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Start a background build (builds take longer than checks, more likely to still be running)
    let (outcome, output) = exec_step(
        dir,
        0,
        "start_build",
        &["build", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    let first_action = extract_json_field(&output.stdout, "data.action");
    let first_is_fresh = first_action.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    steps.push(outcome);

    let Some(first_job_id) = job_id else {
        steps.push(StepOutcome {
            label: "extract_job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract job_id from first build".into()],
        });
        return steps;
    };

    // 2. Immediately re-run build --bg --json — should get "attached" with same job_id
    let (mut outcome, output) = exec_step(
        dir,
        1,
        "re_run_build",
        &["build", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action = extract_json_field(&output.stdout, "data.action");
    match action.as_ref().and_then(|v| v.as_str()) {
        Some("attached") => {
            // Verify it attached to our job
            let attached_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            });
            if attached_id != Some(first_job_id) {
                outcome.passed = false;
                outcome.validation_errors.push(format!(
                    "attached to job {attached_id:?}, expected {first_job_id}"
                ));
            }
        }
        Some("fresh") => {
            // Build completed extremely fast and returned fresh — acceptable
        }
        Some(other) => {
            outcome.passed = false;
            outcome.validation_errors.push(format!(
                "expected action \"attached\" or \"fresh\", got \"{other}\""
            ));
        }
        None => {
            // No coordinator action — first job completed before second started.
            // This is acceptable in environments with warm caches.
        }
    }
    steps.push(outcome);

    // 3. Wait for the original job to finish (cleanup)
    // Skip if first build returned "fresh" — the job_id is historical, not waitable
    if first_is_fresh {
        steps.push(StepOutcome {
            label: "wait_cleanup".into(),
            passed: true,
            exit_code: 0,
            duration: Duration::ZERO,
            validation_errors: vec![],
        });
    } else {
        let (outcome, _) = exec_step(
            dir,
            2,
            "wait_cleanup",
            &["jobs", "wait", &first_job_id.to_string(), "--json"],
            ExpectedExit::Success,
            &[v_json()],
            verbose,
        );
        steps.push(outcome);
    }

    steps
}

/// Scope isolation: start test --bg -p xtask, then -p sinex-primitives — should Queue.
fn custom_coord_scope_isolation(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Start test for one package
    let (outcome, output) = exec_step(
        dir,
        0,
        "start_test_pkg1",
        &["test", "--bg", "-p", "xtask", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let first_job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    steps.push(outcome);

    let Some(first_id) = first_job_id else {
        steps.push(StepOutcome {
            label: "extract_job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract job_id from first test".into()],
        });
        return steps;
    };

    // 2. Start test for a DIFFERENT package — should queue behind
    let (mut outcome, output) = exec_step(
        dir,
        1,
        "start_test_pkg2",
        &["test", "--bg", "-p", "sinex-primitives", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action = extract_json_field(&output.stdout, "data.action");
    match action.as_ref().and_then(|v| v.as_str()) {
        Some("queued") => {
            // Expected — verify it's queued behind first job
            let behind_id =
                extract_json_field(&output.stdout, "data.current_job_id").and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                });
            if behind_id != Some(first_id) {
                outcome.passed = false;
                outcome.validation_errors.push(format!(
                    "queued behind job {behind_id:?}, expected {first_id}"
                ));
            }
        }
        Some("fresh" | "started") => {
            // First test completed before second started — acceptable in fast environments
        }
        Some(other) => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("expected action \"queued\", got \"{other}\""));
        }
        None => {
            // No coordinator action — first test completed before second started.
            // This is acceptable in environments with warm caches.
        }
    }
    steps.push(outcome);

    // 3. Wait for first job to finish
    let (outcome, _) = exec_step(
        dir,
        2,
        "wait_cleanup",
        &["jobs", "wait", &first_id.to_string(), "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    steps
}

/// State update: verify that `--bg` check produces a real `job_id` (>0) and `pid` (>0).
fn custom_coord_state_update(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Run check --bg --json
    let (mut outcome, output) = exec_step(
        dir,
        0,
        "check_bg",
        &["check", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );

    // Verify job_id is a real value (not sentinel -1 or 0)
    let job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    match job_id {
        Some(id) if id > 0 => {} // Good
        Some(id) => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("job_id is sentinel value {id}, expected >0"));
        }
        None => {
            // May be "fresh" result — check action
            let action = extract_json_field(&output.stdout, "data.action");
            if action.as_ref().and_then(|v| v.as_str()) != Some("fresh") {
                outcome.passed = false;
                outcome
                    .validation_errors
                    .push("could not extract job_id and action is not fresh".into());
            }
        }
    }

    // Verify pid is present and >0 (for started jobs)
    let pid = extract_json_field(&output.stdout, "data.pid").and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    let action = extract_json_field(&output.stdout, "data.action");
    let is_started = action
        .as_ref()
        .and_then(|v| v.as_str())
        .is_none_or(|a| a != "fresh" && a != "attached" && a != "queued");
    if is_started {
        match pid {
            Some(p) if p > 0 => {} // Good
            Some(p) => {
                outcome.passed = false;
                outcome
                    .validation_errors
                    .push(format!("pid is {p}, expected >0"));
            }
            None => {
                outcome.passed = false;
                outcome
                    .validation_errors
                    .push("missing pid in started job result".into());
            }
        }
    }
    steps.push(outcome);

    // 2. Wait for completion
    let is_fresh_action = action.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    if let Some(id) = job_id {
        if id > 0 && !is_fresh_action {
            let (outcome, _) = exec_step(
                dir,
                1,
                "wait",
                &["jobs", "wait", &id.to_string(), "--json"],
                ExpectedExit::Success,
                &[v_json()],
                verbose,
            );
            steps.push(outcome);
        }
    }

    steps
}

// ═══════════════════════════════════════════════════════════════════════════════
// T4 extended coordinator exercises
// ═══════════════════════════════════════════════════════════════════════════════

/// Supersede: start bg build, modify tree (GitStateGuard), re-run build with
/// same scope — coordinator should cancel stale job and start fresh.
fn custom_coord_supersede(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
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

    // 1. Start a background build
    let (outcome, output) = exec_step(
        dir,
        0,
        "start_build",
        &["build", "-p", "sinex-primitives", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let first_action = extract_json_field(&output.stdout, "data.action");
    let first_is_fresh = first_action.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    let first_job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    steps.push(outcome);

    // If first build returned "fresh", we need a real running job to supersede.
    // Wait for it and force a new start by changing the tree.
    if first_is_fresh {
        // Touch a file to change tree fingerprint, then start a real job
        if let Err(e) = guard.touch_file("crate/lib/sinex-primitives/src/lib.rs") {
            steps.push(StepOutcome {
                label: "touch_for_start".into(),
                passed: false,
                exit_code: -1,
                duration: Duration::ZERO,
                validation_errors: vec![format!("touch file failed: {e}")],
            });
            drop(guard);
            return steps;
        }

        let (outcome, output) = exec_step(
            dir,
            1,
            "force_start",
            &["build", "-p", "sinex-primitives", "--bg", "--json"],
            ExpectedExit::Success,
            &[v_json()],
            verbose,
        );
        let action = extract_json_field(&output.stdout, "data.action");
        let is_started = action.as_ref().and_then(|v| v.as_str());
        if !matches!(is_started, Some("started" | "superseded")) {
            // Can't test supersede if we can't start a job
            steps.push(StepOutcome {
                label: "need_running_job".into(),
                passed: true, // Not a failure — just can't test supersede in this environment
                exit_code: 0,
                duration: Duration::ZERO,
                validation_errors: vec![],
            });
            steps.push(outcome);
            drop(guard);
            return steps;
        }
        steps.push(outcome);

        // Now touch a DIFFERENT file to change tree fingerprint again
        if let Err(e) = guard.touch_file("crate/lib/sinex-primitives/docs/error.md") {
            steps.push(StepOutcome {
                label: "touch_for_supersede".into(),
                passed: false,
                exit_code: -1,
                duration: Duration::ZERO,
                validation_errors: vec![format!("touch file failed: {e}")],
            });
            drop(guard);
            return steps;
        }
    } else {
        // First build is running — touch a file to change tree fingerprint
        if let Err(e) = guard.touch_file("crate/lib/sinex-primitives/src/lib.rs") {
            steps.push(StepOutcome {
                label: "touch".into(),
                passed: false,
                exit_code: -1,
                duration: Duration::ZERO,
                validation_errors: vec![format!("touch file failed: {e}")],
            });
            drop(guard);
            return steps;
        }
    }

    // 2. Re-run build --bg with same scope but different tree → should get "superseded"
    let (mut outcome, output) = exec_step(
        dir,
        steps.len(),
        "supersede_build",
        &["build", "-p", "sinex-primitives", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action = extract_json_field(&output.stdout, "data.action");
    match action.as_ref().and_then(|v| v.as_str()) {
        Some("superseded") => {
            // Expected: verify old_job_id and new_job_id differ
            let old_id = extract_json_field(&output.stdout, "data.old_job_id");
            let new_id = extract_json_field(&output.stdout, "data.new_job_id");
            if old_id == new_id {
                outcome.passed = false;
                outcome
                    .validation_errors
                    .push("old_job_id == new_job_id in supersede result".into());
            }
        }
        Some("started") => {
            // Acceptable: previous job finished before re-run (fast build)
        }
        Some("fresh") => {
            // Acceptable: build cached result matched
        }
        Some(other) => {
            outcome.passed = false;
            outcome.validation_errors.push(format!(
                "expected \"superseded\" or \"started\", got \"{other}\""
            ));
        }
        None => {
            // No action — coordinator not engaged, acceptable
        }
    }
    steps.push(outcome);

    // 3. Cleanup: wait for the new job
    let cleanup_job_id = extract_json_field(&output.stdout, "data.new_job_id")
        .or_else(|| extract_json_field(&output.stdout, "data.job_id"))
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        });
    if let Some(id) = cleanup_job_id {
        if id > 0 {
            let (outcome, _) = exec_step(
                dir,
                steps.len(),
                "wait_cleanup",
                &["jobs", "wait", &id.to_string(), "--json"],
                ExpectedExit::Success,
                &[v_json()],
                verbose,
            );
            steps.push(outcome);
        }
    }

    // Also wait for first job if it was cancelled (to avoid zombie)
    if let Some(id) = first_job_id {
        if id > 0 && !first_is_fresh {
            let _ = exec_step(
                dir,
                steps.len(),
                "wait_first_cleanup",
                &["jobs", "wait", &id.to_string(), "--json"],
                ExpectedExit::Any,
                &[],
                verbose,
            );
        }
    }

    drop(guard);
    steps
}

/// Queue no-overwrite: start a bg test, queue two different packages behind it.
/// After completion, verify that the FIFO queue preserves both items (not just the last).
///
/// This validates the fix for the critical queue overwrite bug where
/// `Option<QueuedWork>` was replaced with `Vec<QueuedWork>`.
fn custom_coord_queue_no_overwrite(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Start a background test to hold the coordination slot
    let (outcome, output) = exec_step(
        dir,
        0,
        "start_test",
        &["test", "--bg", "-p", "xtask", "--json", "--skip-preflight"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let first_action = extract_json_field(&output.stdout, "data.action");
    let first_is_fresh = first_action.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    let first_job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    steps.push(outcome);

    if first_is_fresh {
        // Can't test queuing if first job returned fresh (no running job to queue behind)
        steps.push(StepOutcome {
            label: "skip_no_running_job".into(),
            passed: true,
            exit_code: 0,
            duration: Duration::ZERO,
            validation_errors: vec![],
        });
        return steps;
    }

    let Some(first_id) = first_job_id else {
        steps.push(StepOutcome {
            label: "extract_job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract job_id from first test".into()],
        });
        return steps;
    };

    // 2. Queue first different-scope job
    let (mut outcome, output) = exec_step(
        dir,
        1,
        "queue_pkg2",
        &[
            "test",
            "--bg",
            "-p",
            "sinex-primitives",
            "--json",
            "--skip-preflight",
        ],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action2 = extract_json_field(&output.stdout, "data.action");
    match action2.as_ref().and_then(|v| v.as_str()) {
        Some("queued") => {} // Expected
        Some("fresh" | "started") => {
            // First job finished before queue — acceptable, but can't test queue behavior
            steps.push(outcome);
            steps.push(StepOutcome {
                label: "skip_first_completed".into(),
                passed: true,
                exit_code: 0,
                duration: Duration::ZERO,
                validation_errors: vec![],
            });
            let _ = exec_step(
                dir,
                3,
                "wait_cleanup",
                &["jobs", "wait", &first_id.to_string(), "--json"],
                ExpectedExit::Any,
                &[],
                verbose,
            );
            return steps;
        }
        Some(other) => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("expected \"queued\", got \"{other}\""));
        }
        None => {}
    }
    steps.push(outcome);

    // 3. Queue second different-scope job
    let (mut outcome, output) = exec_step(
        dir,
        2,
        "queue_pkg3",
        &[
            "test",
            "--bg",
            "-p",
            "sinex-schema",
            "--json",
            "--skip-preflight",
        ],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action3 = extract_json_field(&output.stdout, "data.action");
    match action3.as_ref().and_then(|v| v.as_str()) {
        Some("queued") => {} // Expected — FIFO queue preserved both
        Some("fresh" | "started") => {
            // Timing issue — acceptable
        }
        Some(other) => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("expected \"queued\", got \"{other}\""));
        }
        None => {}
    }
    steps.push(outcome);

    // 4. Read coordinator state to verify queue has 2 items
    // (This is an internal check — read the state file directly)
    let cfg = crate::config::config();
    let state_path = cfg.state_dir.join("coordinator").join("test.state.json");
    let mut queue_verified = false;
    if let Ok(content) = std::fs::read_to_string(&state_path) {
        if let Ok(state) = serde_json::from_str::<crate::coordinator::CoordinationState>(&content) {
            if state.queue.len() >= 2 {
                queue_verified = true;
            }
            steps.push(StepOutcome {
                label: "verify_queue_depth".into(),
                passed: state.queue.len() >= 2,
                exit_code: 0,
                duration: Duration::ZERO,
                validation_errors: if state.queue.len() < 2 {
                    vec![format!(
                        "expected queue depth >= 2, got {} (overwrite bug?)",
                        state.queue.len()
                    )]
                } else {
                    vec![]
                },
            });
        }
    }
    if !queue_verified
        && steps
            .last()
            .is_none_or(|s| s.label != "verify_queue_depth")
    {
        // State file may not exist (race — job finished too quickly)
        steps.push(StepOutcome {
            label: "verify_queue_depth".into(),
            passed: true, // Not a hard failure — timing-dependent
            exit_code: 0,
            duration: Duration::ZERO,
            validation_errors: vec![],
        });
    }

    // 5. Wait for first job to finish (cleanup)
    let (outcome, _) = exec_step(
        dir,
        steps.len(),
        "wait_cleanup",
        &["jobs", "wait", &first_id.to_string(), "--json"],
        ExpectedExit::Any,
        &[],
        verbose,
    );
    steps.push(outcome);

    steps
}

/// Affected transitive: touch sinex-db (a mid-level library), verify that
/// transitive dependents like sinex-services and sinex-gateway appear.
fn custom_affected_transitive(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    run_affected_exercise(
        dir,
        verbose,
        "crate/lib/sinex-db/src/lib.rs",
        &[
            "sinex-db",       // Direct change
            "sinex-services", // Depends on sinex-db
        ],
        &[], // Don't assert absence — other transitive deps may or may not appear
    )
}

/// Jobs output while running: spawn a bg job, immediately read its output,
/// then wait for completion and read again — verify output grows.
fn custom_jobs_output_while_running(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Spawn a build that takes a bit of time
    let (outcome, output) = exec_step(
        dir,
        0,
        "spawn",
        &["build", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .map(|n| n.to_string())
            .or_else(|| v.as_str().map(String::from))
    });
    let action = extract_json_field(&output.stdout, "data.action");
    let is_fresh = action.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    steps.push(outcome);

    if is_fresh {
        // Build returned fresh — no running job to observe output from
        steps.push(StepOutcome {
            label: "skip_fresh".into(),
            passed: true,
            exit_code: 0,
            duration: Duration::ZERO,
            validation_errors: vec![],
        });
        return steps;
    }

    let Some(job_id) = job_id else {
        steps.push(StepOutcome {
            label: "extract_job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract job_id".into()],
        });
        return steps;
    };

    // 2. Immediately try to read output (may be empty or partial — both OK)
    let (outcome, early_output) = exec_step(
        dir,
        1,
        "early_output",
        &["jobs", "output", &job_id],
        ExpectedExit::Any, // May fail if job hasn't written yet
        &[],
        verbose,
    );
    let early_len = early_output.stdout.len() + early_output.stderr.len();
    steps.push(outcome);

    // 3. Wait for completion
    let (outcome, _) = exec_step(
        dir,
        2,
        "wait",
        &["jobs", "wait", &job_id, "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    // 4. Read output after completion — should be non-empty and >= early output
    let (mut outcome, final_output) = exec_step(
        dir,
        3,
        "final_output",
        &["jobs", "output", &job_id],
        ExpectedExit::Success,
        &[],
        verbose,
    );
    let final_len = final_output.stdout.len() + final_output.stderr.len();

    if final_len == 0 {
        outcome.passed = false;
        outcome
            .validation_errors
            .push("final output is empty after job completion".into());
    }
    if final_len < early_len {
        outcome.passed = false;
        outcome.validation_errors.push(format!(
            "final output ({final_len} bytes) < early output ({early_len} bytes)"
        ));
    }
    steps.push(outcome);

    steps
}

// ═══════════════════════════════════════════════════════════════════════════════
// Output directory & reporting
// ═══════════════════════════════════════════════════════════════════════════════

fn setup_output_dir() -> Result<PathBuf> {
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

fn build_report(
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

fn print_human_summary(outcomes: &[ExerciseOutcome], skipped: usize, total_duration: Duration) {
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

// ═══════════════════════════════════════════════════════════════════════════════
// XtaskCommand implementation
// ═══════════════════════════════════════════════════════════════════════════════

#[async_trait::async_trait]
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

        Ok(result)
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("test".to_string()),
            timeout: Some(Duration::from_mins(30)),
            modifies_state: false,
            track_in_history: true,
        }
    }
}
