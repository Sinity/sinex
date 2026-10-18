//! The media library (architecture §6.6): items, renditions, curation records
//! and handoff to players and viewers, built on materials and admission.
//! Intake is ordinary staging; curation is written through admission; removal
//! is hide and excision. Filled by the library lane once its record design is
//! checked against the operator's library requirements (architecture §15).
use serde::{Deserialize, Serialize};
use sinex_model::MaterialId;

/// A continuing reference whose revisions can point at changed bytes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Item {
    pub content: MaterialId,
    /// Where the file was last seen; found again by hash if it moved.
    pub last_known_path: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Rendition {
    Original,
    Crop,
    Preview,
    Transcode,
}

/// Stage a file into the library.
pub fn intake(_path: &std::path::Path) -> Item {
    todo!("architecture §6.6: library intake through ordinary staging")
}
