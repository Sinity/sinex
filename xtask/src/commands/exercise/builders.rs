use super::types::{ExerciseDef, ExerciseKind, ExerciseStep, ExpectedExit, InfraReq, Validation};

// ═══════════════════════════════════════════════════════════════════════════════
// JSON path helpers
// ═══════════════════════════════════════════════════════════════════════════════

#[must_use]
pub fn json_path<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
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
pub fn parse_last_json(stdout: &str) -> std::result::Result<serde_json::Value, String> {
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

#[must_use]
pub fn extract_json_field(stdout: &str, path: &str) -> Option<serde_json::Value> {
    let val = parse_last_json(stdout).ok()?;
    json_path(&val, path).cloned()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Builder helpers (compact catalog construction)
// ═══════════════════════════════════════════════════════════════════════════════

#[must_use]
pub fn def(id: &str, desc: &str, tier: super::types::Tier) -> ExerciseDef {
    ExerciseDef {
        id: id.to_string(),
        description: desc.to_string(),
        tier,
        infra: InfraReq::None,
        kind: ExerciseKind::Declarative(Vec::new()),
    }
}

impl ExerciseDef {
    #[must_use]
    pub fn infra(mut self, req: InfraReq) -> Self {
        self.infra = req;
        self
    }
    #[must_use]
    pub fn step(mut self, s: ExerciseStep) -> Self {
        if let ExerciseKind::Declarative(ref mut steps) = self.kind {
            steps.push(s);
        }
        self
    }
    #[must_use]
    pub fn custom(mut self) -> Self {
        self.kind = ExerciseKind::Custom;
        self
    }
}

pub fn step(label: &str, args: &[&str]) -> ExerciseStep {
    ExerciseStep {
        label: label.to_string(),
        args: args.iter().map(ToString::to_string).collect(),
        expected_exit: ExpectedExit::Success,
        validations: Vec::new(),
        env: Vec::new(),
    }
}

impl ExerciseStep {
    #[must_use]
    pub fn exit(mut self, e: ExpectedExit) -> Self {
        self.expected_exit = e;
        self
    }
    #[must_use]
    pub fn v(mut self, val: Validation) -> Self {
        self.validations.push(val);
        self
    }
}

// Validation shorthand constructors
#[must_use]
pub fn v_json() -> Validation {
    Validation::JsonValid
}
pub fn v_has(fields: &[&str]) -> Validation {
    Validation::JsonHasFields(fields.iter().map(ToString::to_string).collect())
}
#[must_use]
pub fn v_eq(path: &str, expected: serde_json::Value) -> Validation {
    Validation::JsonFieldEquals {
        path: path.to_string(),
        expected,
    }
}
#[allow(dead_code)] // Maintained for catalog extensibility
#[must_use]
pub fn v_one_of(path: &str, values: &[&str]) -> Validation {
    Validation::JsonFieldOneOf {
        path: path.to_string(),
        values: values
            .iter()
            .map(|s| serde_json::Value::String(s.to_string()))
            .collect(),
    }
}
#[must_use]
pub fn v_arr_min(path: &str, min: usize) -> Validation {
    Validation::JsonArrayMinLen {
        path: path.to_string(),
        min,
    }
}
#[must_use]
pub fn v_contains(s: &str) -> Validation {
    Validation::StdoutContains(s.to_string())
}
#[allow(dead_code)] // Maintained for catalog extensibility
#[must_use]
pub fn v_not_contains(s: &str) -> Validation {
    Validation::StdoutNotContains(s.to_string())
}
#[must_use]
pub fn v_stderr(s: &str) -> Validation {
    Validation::StderrContains(s.to_string())
}
#[must_use]
pub fn v_empty() -> Validation {
    Validation::StdoutEmpty
}
#[must_use]
pub fn v_lines(min: Option<usize>, max: Option<usize>) -> Validation {
    Validation::StdoutLineCount { min, max }
}
