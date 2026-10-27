//! Captured document observations anchored at their stored file records.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "document.captured", replace = newer,
    time = modified_at, quality = file_mtime, removed_when = removed_at)]
pub struct DocumentCaptured {
    pub host: String,
    pub root: String,
    pub path: String,
    pub native_path_bytes: Option<String>,
    pub size: Option<u64>,
    pub modified_at: Option<Timestamp>,
    pub media_type: Option<String>,
    pub content: Option<String>,
    pub removed_at: Option<Timestamp>,
}

#[cfg(test)]
mod tests;
