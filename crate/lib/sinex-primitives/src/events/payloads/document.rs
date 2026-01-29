//! Document ingestion event payloads

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "document-ingestor", event_type = "document.ingested")]
pub struct DocumentIngestedPayload {
    pub file_path: String,
    pub source_material_id: String,
    pub size_bytes: u64,
    pub mime_type: Option<String>,
    pub encoding: Option<String>,
}
