//! Clipboard event payloads

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "clipboard", event_type = "clipboard.copied")]
pub struct ClipboardCopiedPayload {
    pub operation: String,
    pub content_type: String,
    pub content_size: usize,
    pub text_preview: Option<String>,
    pub file_count: Option<usize>,
    pub file_paths: Option<Vec<String>>,
    pub source_app: Option<String>,
    pub window_title: Option<String>,
    pub content_hash: String,
    pub original_hash: Option<String>,
    pub annex_key: Option<String>,
    pub blob_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "clipboard", event_type = "clipboard.selected")]
pub struct ClipboardSelectedPayload {
    pub selection_type: String, // primary, clipboard
    pub content_type: String,
    pub content_size: usize,
    pub text_preview: Option<String>,
    pub source_app: Option<String>,
    pub content_hash: String,
    pub original_hash: Option<String>,
    pub annex_key: Option<String>,
    pub blob_id: Option<String>,
}

impl ClipboardCopiedPayload {
    /// Create a test instance with the given content hash
    #[cfg(any(test, feature = "testing"))]
    pub fn test_default(content_hash: impl Into<String>) -> Self {
        Self {
            operation: "copy".to_string(),
            content_type: "text/plain".to_string(),
            content_size: 0,
            text_preview: None,
            file_count: None,
            file_paths: None,
            source_app: None,
            window_title: None,
            content_hash: content_hash.into(),
            original_hash: None,
            annex_key: None,
            blob_id: None,
        }
    }
}

impl ClipboardSelectedPayload {
    /// Create a test instance with the given content hash
    #[cfg(any(test, feature = "testing"))]
    pub fn test_default(content_hash: impl Into<String>) -> Self {
        Self {
            selection_type: "primary".to_string(),
            content_type: "text/plain".to_string(),
            content_size: 0,
            text_preview: None,
            source_app: None,
            content_hash: content_hash.into(),
            original_hash: None,
            annex_key: None,
            blob_id: None,
        }
    }
}
