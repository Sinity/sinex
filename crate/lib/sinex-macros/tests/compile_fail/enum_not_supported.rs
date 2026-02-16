use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, sinex_macros::EventPayload)]
#[event_payload(source = "test-source", event_type = "test.event")]
pub enum NotAStruct {
    VariantA,
    VariantB(String),
}

fn main() {}
