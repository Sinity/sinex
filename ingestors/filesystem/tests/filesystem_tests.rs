use sinex_shared::{RawEventBuilder, sources, event_type_constants};
use serde_json::json;
use tempfile::TempDir;
use std::fs::{File, create_dir_all};
use std::io::Write;

#[test]
fn test_filesystem_event_creation() {
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/tmp/test.txt",
            "size": 1024
        })
    ).build();

    assert_eq!(event.source, sources::FILESYSTEM);
    assert_eq!(event.event_type, event_type_constants::filesystem::FILE_CREATED);
    assert!(!event.host.is_empty());
}

#[test]
fn test_file_event_payloads() {
    let create_event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/home/user/document.txt",
            "size": 2048,
            "permissions": "644"
        })
    ).build();

    let modify_event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_MODIFIED,
        json!({
            "path": "/home/user/document.txt",
            "old_size": 2048,
            "new_size": 2560,
            "modification_type": "content_change"
        })
    ).build();

    let delete_event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_DELETED,
        json!({
            "path": "/home/user/document.txt",
            "was_directory": false
        })
    ).build();

    assert_eq!(create_event.event_type, event_type_constants::filesystem::FILE_CREATED);
    assert_eq!(modify_event.event_type, event_type_constants::filesystem::FILE_MODIFIED);
    assert_eq!(delete_event.event_type, event_type_constants::filesystem::FILE_DELETED);
    
    // Verify payload structure
    assert!(create_event.payload["path"].is_string());
    assert!(create_event.payload["size"].is_number());
    assert!(modify_event.payload["old_size"].is_number());
    assert!(delete_event.payload["was_directory"].is_boolean());
}

#[test]
fn test_event_builder_features() {
    use chrono::Utc;
    use sinex_ulid::Ulid;
    
    let now = Utc::now();
    let schema_id = Ulid::new();
    
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({"path": "/test"})
    )
    .with_orig_timestamp(now)
    .with_schema_id(schema_id)
    .with_ingestor_version("test-1.0.0")
    .build();
    
    assert_eq!(event.source, sources::FILESYSTEM);
    assert_eq!(event.event_type, event_type_constants::filesystem::FILE_CREATED);
    assert_eq!(event.ts_orig, Some(now));
    assert!(event.ingestor_version.as_ref().unwrap().contains("test-1.0.0"));
    assert_eq!(event.payload["path"], "/test");
}

#[test]
fn test_rename_event_payload() {
    let rename_event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_RENAMED,
        json!({
            "old_path": "/home/user/old_name.txt",
            "new_path": "/home/user/new_name.txt",
            "is_directory": false
        })
    ).build();

    assert_eq!(rename_event.event_type, event_type_constants::filesystem::FILE_RENAMED);
    assert_eq!(rename_event.payload["old_path"], "/home/user/old_name.txt");
    assert_eq!(rename_event.payload["new_path"], "/home/user/new_name.txt");
    assert_eq!(rename_event.payload["is_directory"], false);
}