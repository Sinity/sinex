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

/// A retained public question or a current internal computational selector.
#[derive(Clone,Debug)]
pub enum SelectedQuery {
    Retained(sinex_model::event_types::analysis::QuerySelection),
    Referenced {
        text:String,
        reference:sinex_model::ReferenceInput,
        target:EventId,
    },
}

pub type InputBytes = std::collections::BTreeMap<EventId,Result<Vec<u8>,String>>;
