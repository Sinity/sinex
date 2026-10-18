//! The analysis layer as pure code (architecture §6.5): results that depend on
//! a whole series, stored as products with the exact inputs they were computed
//! from. Filled by the analysis lane.
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A stored analysis result over many events. Not an event.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Product {
    pub plan: String,
    pub method: String,
    /// Exactly which inputs, by plan and commit position.
    pub readset: Value,
    pub status: ProductStatus,
    pub result: Value,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProductStatus {
    Current,
    Stale,
    Superseded,
    Withdrawn,
}

/// A typed statement about a product result, with its caveats. The operator
/// accepting it is an ordinary event.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Finding {
    pub statement: String,
    pub caveats: Vec<String>,
    pub product: String,
}

/// A cell of a metric: zero is not missing (whether the source was capturing).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Cell {
    Value { value: f64 },
    NotCapturing,
    Unknown,
}

/// Run a registered plan over its inputs.
pub fn run_plan(_plan: &str, _inputs: &[Value]) -> Product {
    todo!("architecture §6.5: relations, metric algebra (k4c.2), plans and registered methods")
}
