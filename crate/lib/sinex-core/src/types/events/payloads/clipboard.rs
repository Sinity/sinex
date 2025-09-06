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

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::prelude::*;

    /// Test ClipboardCopiedPayload creation and serialization
    #[sinex_test]
    async fn test_clipboard_copied_payload(_ctx: TestContext) -> Result<()> {
        let payload = ClipboardCopiedPayload::test_default("abc123")
            .with_operation("copy")
            .with_content_type("text")
            .with_content_size(100)
            .with_text_preview("Hello world")
            .with_source_app("firefox");

        // Verify the payload can be serialized to JSON
        let json_value = serde_json::to_value(&payload)?;

        // Check the essential fields match the expected structure
        assert_eq!(json_value["operation"], "copy");
        assert_eq!(json_value["content_type"], "text");
        assert_eq!(json_value["content_size"], 100);
        assert_eq!(json_value["text_preview"], "Hello world");
        assert_eq!(json_value["content_hash"], "abc123");
        assert_eq!(json_value["source_app"], "firefox");

        Ok(())
    }

    /// Test ClipboardSelectedPayload creation and serialization
    #[sinex_test]
    async fn test_clipboard_selected_payload(_ctx: TestContext) -> Result<()> {
        let payload = ClipboardSelectedPayload::test_default("def456")
            .with_selection_type("primary")
            .with_content_type("text")
            .with_content_size(50)
            .with_text_preview("Selected text");

        // Verify the payload can be serialized to JSON
        let json_value = serde_json::to_value(&payload)?;

        // Check the essential fields match the expected structure
        assert_eq!(json_value["content_type"], "text");
        assert_eq!(json_value["content_size"], 50);
        assert_eq!(json_value["text_preview"], "Selected text");
        assert_eq!(json_value["selection_type"], "primary");
        assert_eq!(json_value["content_hash"], "def456");

        Ok(())
    }

    /// Test payload structure compatibility with current architecture
    #[sinex_test]
    async fn test_payload_structure_compatibility(_ctx: TestContext) -> Result<()> {
        // Test that our payloads have the expected structure
        let copied_payload = ClipboardCopiedPayload::test_default("hash123")
            .with_operation("copy")
            .with_content_type("text")
            .with_content_size(100)
            .with_text_preview("Test content")
            .with_source_app("test_app");

        let copied_json = serde_json::to_value(&copied_payload)?;

        // Verify all expected fields are present in current architecture
        assert!(copied_json.get("operation").is_some());
        assert!(copied_json.get("content_type").is_some());
        assert!(copied_json.get("content_size").is_some());
        assert!(copied_json.get("text_preview").is_some());
        assert!(copied_json.get("content_hash").is_some());
        assert!(copied_json.get("source_app").is_some());

        // Verify extended fields exist (current architecture includes these)
        assert!(copied_json.get("file_count").is_some());
        assert!(copied_json.get("file_paths").is_some());
        assert!(copied_json.get("window_title").is_some());
        assert!(copied_json.get("original_hash").is_some());
        assert!(copied_json.get("annex_key").is_some());
        assert!(copied_json.get("blob_id").is_some());

        Ok(())
    }

    /// Test clipboard payload with file operations
    #[sinex_test]
    async fn test_clipboard_file_operations(_ctx: TestContext) -> Result<()> {
        let file_paths = vec!["/tmp/file1.txt".to_string(), "/tmp/file2.txt".to_string()];
        let payload = ClipboardCopiedPayload::test_default("file_hash")
            .with_content_type("application/x-file-list")
            .with_file_paths(file_paths.clone())
            .with_source_app("file_manager");

        let json_value = serde_json::to_value(&payload)?;

        // Verify file-specific fields
        assert_eq!(json_value["content_type"], "application/x-file-list");
        assert_eq!(json_value["file_count"], 2);
        assert_eq!(json_value["file_paths"], serde_json::to_value(&file_paths)?);
        assert_eq!(json_value["source_app"], "file_manager");

        Ok(())
    }

    /// Test clipboard selection types
    #[sinex_test]
    async fn test_clipboard_selection_types(_ctx: TestContext) -> Result<()> {
        // Test primary selection
        let primary_payload = ClipboardSelectedPayload::test_default("primary_hash")
            .with_selection_type("primary")
            .with_content_type("text/plain")
            .with_text_preview("Primary selection");

        let primary_json = serde_json::to_value(&primary_payload)?;
        assert_eq!(primary_json["selection_type"], "primary");

        // Test clipboard selection
        let clipboard_payload = ClipboardSelectedPayload::test_default("clipboard_hash")
            .with_selection_type("clipboard")
            .with_content_type("text/plain")
            .with_text_preview("Clipboard selection");

        let clipboard_json = serde_json::to_value(&clipboard_payload)?;
        assert_eq!(clipboard_json["selection_type"], "clipboard");

        Ok(())
    }

    /// Test builder method chaining
    #[sinex_test]
    async fn test_builder_method_chaining(_ctx: TestContext) -> Result<()> {
        // Test that all builder methods can be chained fluently
        let payload = ClipboardCopiedPayload::test_default("builder_test")
            .with_operation("paste")
            .with_content_type("image/png")
            .with_content_size(2048)
            .with_text_preview("Image preview")
            .with_source_app("image_editor")
            .with_file_paths(vec!["/tmp/image.png".to_string()]);

        let json_value = serde_json::to_value(&payload)?;

        // Verify all chained values are set correctly
        assert_eq!(json_value["operation"], "paste");
        assert_eq!(json_value["content_type"], "image/png");
        assert_eq!(json_value["content_size"], 2048);
        assert_eq!(json_value["text_preview"], "Image preview");
        assert_eq!(json_value["source_app"], "image_editor");
        assert_eq!(json_value["file_count"], 1);

        Ok(())
    }
}
