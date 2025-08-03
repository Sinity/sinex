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
    /// Create a test payload with sensible defaults
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
    
    /// Builder-style method for operation
    pub fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.operation = operation.into();
        self
    }
    
    /// Builder-style method for content type
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }
    
    /// Builder-style method for content size
    pub fn with_content_size(mut self, size: usize) -> Self {
        self.content_size = size;
        self
    }
    
    /// Builder-style method for text preview
    pub fn with_text_preview(mut self, preview: impl Into<String>) -> Self {
        self.text_preview = Some(preview.into());
        self
    }
    
    /// Builder-style method for source app
    pub fn with_source_app(mut self, app: impl Into<String>) -> Self {
        self.source_app = Some(app.into());
        self
    }
    
    /// Builder-style method for file paths
    pub fn with_file_paths(mut self, paths: Vec<String>) -> Self {
        self.file_count = Some(paths.len());
        self.file_paths = Some(paths);
        self
    }
}

impl ClipboardSelectedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(content_hash: impl Into<String>) -> Self {
        Self {
            selection_type: "clipboard".to_string(),
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
    
    /// Builder-style method for selection type
    pub fn with_selection_type(mut self, selection_type: impl Into<String>) -> Self {
        self.selection_type = selection_type.into();
        self
    }
    
    /// Builder-style method for content type
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }
    
    /// Builder-style method for content size
    pub fn with_content_size(mut self, size: usize) -> Self {
        self.content_size = size;
        self
    }
    
    /// Builder-style method for text preview
    pub fn with_text_preview(mut self, preview: impl Into<String>) -> Self {
        self.text_preview = Some(preview.into());
        self
    }
    
    /// Builder-style method for source app
    pub fn with_source_app(mut self, app: impl Into<String>) -> Self {
        self.source_app = Some(app.into());
        self
    }
}
