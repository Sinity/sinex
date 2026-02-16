use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
#[event_payload(event_type = "test.event")]
pub struct MissingSource {
    pub data: String,
}

fn main() {}
