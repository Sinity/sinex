use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
#[event_payload(source = "test-source")]
pub struct MissingEventType {
    pub data: String,
}

fn main() {}
