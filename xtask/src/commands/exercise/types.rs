use std::time::Duration;

use clap::ValueEnum;
use serde::Serialize;

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
    pub fn label(self) -> &'static str {
        match self {
            Tier::T1 => "T1",
            Tier::T2 => "T2",
            Tier::T3 => "T3",
            Tier::T4 => "T4",
        }
    }

    pub fn as_arg(self) -> &'static str {
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
pub enum InfraReq {
    None,
    Postgres,
    Nats,
    Both,
}

#[derive(Debug, Clone, Copy)]
pub enum ExpectedExit {
    Success,
    Failure,
    Any,
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // Variants maintained for catalog extensibility
pub enum Validation {
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

pub struct ExerciseStep {
    pub label: String,
    pub args: Vec<String>,
    pub expected_exit: ExpectedExit,
    pub validations: Vec<Validation>,
    pub env: Vec<(String, String)>,
}

pub enum ExerciseKind {
    Declarative(Vec<ExerciseStep>),
    Custom,
}

pub struct ExerciseDef {
    pub id: String,
    pub description: String,
    pub tier: Tier,
    pub infra: InfraReq,
    pub kind: ExerciseKind,
}

pub struct StepOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub duration: Duration,
}

pub struct StepOutcome {
    pub label: String,
    pub passed: bool,
    pub exit_code: i32,
    pub duration: Duration,
    pub validation_errors: Vec<String>,
}

pub struct ExerciseOutcome {
    pub id: String,
    pub passed: bool,
    pub duration: Duration,
    pub steps: Vec<StepOutcome>,
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct ExerciseReport {
    pub status: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub duration_secs: f64,
    pub output_dir: String,
    pub results: Vec<ReportEntry>,
}

#[derive(Serialize)]
pub struct ReportEntry {
    pub id: String,
    pub tier: String,
    pub passed: bool,
    pub duration_secs: f64,
    pub error: Option<String>,
    pub steps: Vec<StepEntry>,
}

#[derive(Serialize)]
pub struct StepEntry {
    pub label: String,
    pub passed: bool,
    pub exit_code: i32,
    pub duration_secs: f64,
    pub validation_errors: Vec<String>,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Validation engine
// ═══════════════════════════════════════════════════════════════════════════════

impl Validation {
    pub fn check(&self, output: &StepOutput) -> std::result::Result<(), String> {
        use super::builders::{json_path, parse_last_json};
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
                if let Some(min_val) = min
                    && count < *min_val
                {
                    return Err(format!("stdout has {count} lines, expected >= {min_val}"));
                }
                if let Some(max_val) = max
                    && count > *max_val
                {
                    return Err(format!("stdout has {count} lines, expected <= {max_val}"));
                }
                Ok(())
            }
        }
    }
}
