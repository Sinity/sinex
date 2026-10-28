//! Metadata about intervals when a stream could not capture records.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "capture.gap", key = location, replace = repeat, time = start, quality = captured)]
pub struct Gap {
    pub stream: String,
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
    pub reason: String,
}
