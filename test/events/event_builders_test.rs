use sinex_events::{
    filesystem::{FileCreated, FileModified, FileDeleted},
    terminal::CommandExecuted,
    window_manager::{WindowFocused, WorkspaceChanged},
};
use serde_json::json;

#[test]
fn test_file_created_event() {
    let event = FileCreated::new("/test/file.txt", 1024);
    assert_eq!(event.path, "/test/file.txt");
    assert_eq!(event.size, 1024);
    
    // Test serialization
    let json = serde_json::to_value(&event).unwrap();
    assert_eq!(json["path"], "/test/file.txt");
    assert_eq!(json["size"], 1024);
}

#[test]
fn test_file_modified_event() {
    let event = FileModified::content_modified("/test/file.txt", 1024, 2048);
    assert_eq!(event.path, "/test/file.txt");
    assert_eq!(event.old_size, Some(1024));
    assert_eq!(event.new_size, Some(2048));
    assert_eq!(event.modification_type, Some("content".to_string()));
}

#[test]
fn test_command_executed_event() {
    let event = CommandExecuted::new("ls -la", "/home/user");
    assert_eq!(event.command, "ls -la");
    assert_eq!(event.working_directory, Some("/home/user".to_string()));
    assert!(event.start_time.is_some());
}

#[test]
fn test_window_focused_event() {
    let window_data = json!({
        "title": "Test Window",
        "class": "TestApp"
    });
    let event = WindowFocused::new(window_data);
    assert!(event.window.is_object());
    assert!(event.timestamp.is_some());
}

#[test]
fn test_workspace_changed_event() {
    let event = WorkspaceChanged::new("2".to_string());
    assert_eq!(event.workspace, "2");
    assert!(event.timestamp.is_some());
}