//! Requests and the inputs a result read. A request holds everything that
//! determines a computation, frozen before it runs; its identity is a keyed
//! BLAKE3 hash of its canonical bytes, so the same question at the same commit
//! position is the same request, and the second asking returns the first's
//! result.
use crate::coverage::Coverage;
use crate::span::Span;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Intent {
    /// Fixed after (or without) seeing outcomes; findings over it say so.
    Exploratory,
    /// Pre-registered: family and method fixed in a request recorded before
    /// the outcome period ended.
    PreRegistered,
}

/// What a request reads: a relation over a time range, read at the
/// request's commit position.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadPlan {
    pub relation: String,
    /// RFC 3339 bounds of the half-open range.
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub recipe: String,
    pub recipe_version: u32,
    pub params: Value,
    /// `(method id, version)` of every method the recipe calls.
    pub methods: Vec<(String, u32)>,
    pub plans: Vec<ReadPlan>,
    /// The commit position every plan is read at.
    pub as_of: i64,
    /// The day setting the units were computed under (`analysis.md` §4.3).
    pub day_setting: String,
    /// The full hypothesis family, fixed before results are seen.
    pub family: Option<Vec<Value>>,
    pub intent: Intent,
}

impl Request {
    /// `blake3::derive_key("sinex analysis request v1", canonical bytes)`.
    pub fn id(&self) -> [u8; 32] {
        let value = serde_json::to_value(self).expect("requests serialize");
        blake3::derive_key("sinex analysis request v1", canonical(&value).as_bytes())
    }

    pub fn id_hex(&self) -> String {
        hex(&self.id())
    }
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// RFC 8785-style canonical JSON: object keys sorted by UTF-16 code units,
/// no whitespace, and every number written as a string, so an integer and a
/// float that print alike never hash differently by type. Non-finite floats
/// cannot occur in `serde_json` values.
pub fn canonical(v: &Value) -> String {
    let mut out = String::new();
    write_canonical(v, &mut out);
    out
}

fn write_canonical(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&Value::String(n.to_string()).to_string()),
        Value::String(s) => out.push_str(&Value::String(s.clone()).to_string()),
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(x, out);
            }
            out.push(']');
        }
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(k.clone()).to_string());
                out.push(':');
                write_canonical(&m[k], out);
            }
            out.push('}');
        }
    }
}

/// The exact input of one computation: which rows a plan selected at a commit
/// position, and the coverage it relied on.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InputsRead {
    /// Hex BLAKE3 of the canonical plan.
    pub plan: String,
    pub as_of: i64,
    pub rows: u64,
    /// Hex BLAKE3 over the sorted identities of every row read.
    pub digest: String,
    pub coverage: Vec<Coverage>,
    /// The cells read, for plans over maintained metric values (which keep no
    /// history, so re-evaluation could differ).
    pub membership: Option<Value>,
}

impl InputsRead {
    /// The record of what `plan` read: the identities of its rows.
    pub fn new(
        plan: &ReadPlan,
        as_of: i64,
        mut row_ids: Vec<String>,
        coverage: Vec<Coverage>,
    ) -> Self {
        row_ids.sort_unstable();
        let mut h = blake3::Hasher::new();
        for id in &row_ids {
            h.update(id.as_bytes());
            h.update(&[0]);
        }
        let plan_value = serde_json::to_value(plan).expect("plans serialize");
        Self {
            plan: hex(blake3::hash(canonical(&plan_value).as_bytes()).as_bytes()),
            as_of,
            rows: row_ids.len() as u64,
            digest: hex(h.finalize().as_bytes()),
            coverage,
            membership: None,
        }
    }
}

/// What a result's dependency is: a relation over a time range. The writer
/// marks every current result stale whose dependency overlaps a change.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Dependency {
    pub relation: String,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Estimated,
    NotEstimable,
    Failed,
}

/// The output hash of a result: BLAKE3 over its canonical tables.
pub fn output_hash(tables: &Value) -> [u8; 32] {
    *blake3::hash(canonical(tables).as_bytes()).as_bytes()
}

/// Sort the keys of every object in a value, for stored output that compares
/// equal across runs.
pub fn sorted(v: Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<_> = m.into_iter().collect();
            keys.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(
                keys.into_iter()
                    .map(|(k, v)| (k, sorted(v)))
                    .collect::<Map<_, _>>(),
            )
        }
        Value::Array(a) => Value::Array(a.into_iter().map(sorted).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests;
