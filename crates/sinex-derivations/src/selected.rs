//! Query-backed inputs selected by the daemon through the shared query compiler.
use serde_json::Value;
use sinex_model::{Class, EventId};

/// Complete selected query results and their provenance at one read position.
/// Presentation pages never restrict these inputs.
#[derive(Clone, Debug)]
pub struct SelectedInputs {
    pub value: Value,
    pub parents: Vec<EventId>,
    pub class: Class,
    pub read_as: i64,
    pub signature: String,
}
