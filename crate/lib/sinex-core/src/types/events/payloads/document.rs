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

impl DocumentIngestedPayload {
    /// Builder-style method for size
    pub fn with_size_bytes(mut self, size: u64) -> Self {
        self.size_bytes = size;
        self
    }

    /// Builder-style method for MIME type
    pub fn with_mime_type(mut self, mime_type: impl Into<String>) -> Self {
        self.mime_type = Some(mime_type.into());
        self
    }

    /// Builder-style method for encoding
    pub fn with_encoding(mut self, encoding: impl Into<String>) -> Self {
        self.encoding = Some(encoding.into());
        self
    }
}
