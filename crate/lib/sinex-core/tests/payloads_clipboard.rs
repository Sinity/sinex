use sinex_core::types::events::payloads::clipboard::{
    ClipboardCopiedPayload, ClipboardSelectedPayload,
};
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn clipboard_copied_payload_serializes_expected_fields() -> Result<()> {
    let payload = ClipboardCopiedPayload::test_default("abc123")
        .with_operation("copy")
        .with_content_type("text")
        .with_content_size(100usize)
        .with_text_preview("Hello world")
        .with_source_app("firefox");

    let json_value = serde_json::to_value(&payload)?;
    assert_eq!(json_value["operation"], "copy");
    assert_eq!(json_value["content_type"], "text");
    assert_eq!(json_value["content_size"], 100);
    assert_eq!(json_value["text_preview"], "Hello world");
    assert_eq!(json_value["content_hash"], "abc123");
    assert_eq!(json_value["source_app"], "firefox");
    Ok(())
}

#[sinex_test]
async fn clipboard_selected_payload_serializes_expected_fields() -> Result<()> {
    let payload = ClipboardSelectedPayload::test_default("def456")
        .with_selection_type("primary")
        .with_content_type("text")
        .with_content_size(50usize)
        .with_text_preview("Selected text");

    let json_value = serde_json::to_value(&payload)?;
    assert_eq!(json_value["content_type"], "text");
    assert_eq!(json_value["content_size"], 50);
    assert_eq!(json_value["text_preview"], "Selected text");
    assert_eq!(json_value["selection_type"], "primary");
    assert_eq!(json_value["content_hash"], "def456");
    Ok(())
}

#[sinex_test]
async fn payload_structure_matches_architecture() -> Result<()> {
    let payload = ClipboardCopiedPayload::test_default("hash123")
        .with_operation("copy")
        .with_content_type("text")
        .with_content_size(100usize)
        .with_text_preview("Test content")
        .with_source_app("test_app");

    let json_value = serde_json::to_value(&payload)?;
    for field in [
        "operation",
        "content_type",
        "content_size",
        "text_preview",
        "content_hash",
        "source_app",
        "file_count",
        "file_paths",
        "window_title",
        "original_hash",
        "annex_key",
        "blob_id",
    ] {
        assert!(json_value.get(field).is_some(), "missing field {field}");
    }
    Ok(())
}

#[sinex_test]
async fn clipboard_file_operations_capture_paths() -> Result<()> {
    let file_paths = vec!["/tmp/file1.txt".to_string(), "/tmp/file2.txt".to_string()];
    let payload = ClipboardCopiedPayload::test_default("file_hash")
        .with_content_type("application/x-file-list")
        .with_file_paths(file_paths.clone())
        .with_file_count(file_paths.len())
        .with_source_app("file_manager");

    let json_value = serde_json::to_value(&payload)?;
    assert_eq!(json_value["content_type"], "application/x-file-list");
    assert_eq!(json_value["file_count"], 2);
    assert_eq!(json_value["file_paths"], serde_json::to_value(&file_paths)?);
    assert_eq!(json_value["source_app"], "file_manager");
    Ok(())
}

#[sinex_test]
async fn clipboard_selection_types_round_trip() -> Result<()> {
    let primary = ClipboardSelectedPayload::test_default("primary_hash")
        .with_selection_type("primary")
        .with_content_type("text/plain")
        .with_text_preview("Primary selection");
    let primary_json = serde_json::to_value(&primary)?;
    assert_eq!(primary_json["selection_type"], "primary");

    let clipboard = ClipboardSelectedPayload::test_default("clipboard_hash")
        .with_selection_type("clipboard")
        .with_content_type("text/plain")
        .with_text_preview("Clipboard selection");
    let clipboard_json = serde_json::to_value(&clipboard)?;
    assert_eq!(clipboard_json["selection_type"], "clipboard");
    Ok(())
}

#[sinex_test]
async fn builder_method_chaining_sets_all_fields() -> Result<()> {
    let payload = ClipboardCopiedPayload::test_default("builder_test")
        .with_operation("paste")
        .with_content_type("image/png")
        .with_content_size(2048usize)
        .with_text_preview("Image preview")
        .with_source_app("image_editor")
        .with_file_paths(vec!["/tmp/image.png".to_string()])
        .with_file_count(1usize);

    let json_value = serde_json::to_value(&payload)?;
    assert_eq!(json_value["operation"], "paste");
    assert_eq!(json_value["content_type"], "image/png");
    assert_eq!(json_value["content_size"], 2048);
    assert_eq!(json_value["text_preview"], "Image preview");
    assert_eq!(json_value["source_app"], "image_editor");
    assert_eq!(json_value["file_count"], 1);
    Ok(())
}
