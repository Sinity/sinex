//! Captured documents and hints derived from library filenames.
use crate::{EventId, Timestamp};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "document.captured", key = ["host", "root", "path"], replace = newer,
    time = modified_at, quality = file_mtime, removed_when = removed_at)]
pub struct DocumentCaptured {
    pub host: String,
    pub root: String,
    pub path: String,
    pub size: u64,
    pub modified_at: Option<Timestamp>,
    pub media_type: String,
    pub content: Option<String>,
    pub removed_at: Option<Timestamp>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "library.filename_hints", key = ["copy"], replace = newer)]
pub struct FilenameHints {
    pub copy: EventId,
    pub path: String,
    pub content: String,
    pub author_hint: Option<String>,
    pub title_hint: Option<String>,
    pub year_hint: Option<u16>,
    pub external_id_hint: Option<String>,
    pub rule_version: u32,
}

#[cfg(test)]
mod tests;
