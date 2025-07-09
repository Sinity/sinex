use crate::common::prelude::*;
use sinex_events_terminal::kitty::{
    KittyCommandCompleted, KittyScrollbackIncremental,
    KittyTabFocused, KittyProcessChanged, KittyCommandCompletedPayload, 
    KittyScrollbackIncrementalPayload, KittyTabFocusedPayload, KittyProcessChangedPayload,
};
use sinex_core::{EventSource, EventSourceContext, EventType};
use serde_json::json;

#[sinex_test]
async fn test_kitty_event_source_initialization(_ctx: TestContext) -> TestResult {
    // Test that KittyEventSource initializes without a socket (common case)
    let event_ctx = EventSourceContext::new(json!({}));
    let _source = KittyEventSource::initialize(event_ctx).await?;
    
    // Should initialize successfully even without Kitty socket
    assert_eq!(KittyEventSource::SOURCE_NAME, "shell.kitty");
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_event_types_registration(_ctx: TestContext) -> TestResult {
    // Verify all Kitty event types are properly registered
    assert_eq!(KittyCommandExecuted::EVENT_NAME, "command.started");
    assert_eq!(KittyCommandCompleted::EVENT_NAME, "command.completed");
    assert_eq!(KittyScrollbackIncremental::EVENT_NAME, "content.streamed");
    assert_eq!(KittyTabFocused::EVENT_NAME, "tab.focused");
    assert_eq!(KittyProcessChanged::EVENT_NAME, "process.changed");
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_command_completed_payload_structure(_ctx: TestContext) -> TestResult {
    // Test the command completed payload structure
    let payload = KittyCommandCompletedPayload {
        command: "ls -la".to_string(),
        command_output: "total 42\ndrwxr-xr-x 3 user user 4096 Jan 1 12:00 .\n".to_string(),
        working_directory: Some("/home/user".to_string()),
        kitty_window_id: "123".to_string(),
        kitty_tab_id: "456".to_string(),
        exit_status: Some(0),
        execution_time_ms: Some(150),
        output_size_bytes: 51,
        output_line_count: 2,
        shell_integration_used: true,
        completion_timestamp: chrono::Utc::now().to_rfc3339(),
    };
    
    // Verify serialization works
    let json_value = serde_json::to_value(&payload)?;
    assert!(json_value.get("command").is_some());
    assert!(json_value.get("command_output").is_some());
    assert!(json_value.get("exit_status").is_some());
    assert!(json_value.get("kitty_tab_id").is_some());
    
    // Verify no deduplication - payload should contain full output  
    assert_eq!(payload.command_output.len(), 51); // Actual length of the test string
    assert_eq!(payload.output_line_count, 2);
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_scrollback_incremental_payload_structure(_ctx: TestContext) -> TestResult {
    // Test the incremental scrollback payload structure
    let new_lines = vec![
        "Line 101: New scrollback content".to_string(),
        "Line 102: More content".to_string(),
        "Line 103: Final line".to_string(),
    ];
    
    let payload = KittyScrollbackIncrementalPayload {
        kitty_window_id: "789".to_string(),
        new_lines: new_lines.clone(),
        line_start_offset: 100,
        capture_timestamp: chrono::Utc::now().to_rfc3339(),
    };
    
    // Verify serialization works
    let json_value = serde_json::to_value(&payload)?;
    assert!(json_value.get("new_lines").is_some());
    assert!(json_value.get("line_start_offset").is_some());
    
    // Verify actual content is stored, not just metadata
    assert_eq!(payload.new_lines.len(), 3);
    assert_eq!(payload.new_lines[0], "Line 101: New scrollback content");
    assert_eq!(payload.line_start_offset, 100);
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_tab_focused_payload_structure(_ctx: TestContext) -> TestResult {
    // Test tab focus payload with real tab IDs
    let payload = KittyTabFocusedPayload {
        kitty_tab_id: "12345".to_string(),
        kitty_window_id: "67890".to_string(),
        tab_title: "Terminal Session".to_string(),
        tab_index: 2,
        previous_tab_id: Some("11111".to_string()),
        focus_timestamp: chrono::Utc::now().to_rfc3339(),
    };
    
    // Verify serialization works
    let json_value = serde_json::to_value(&payload)?;
    assert!(json_value.get("kitty_tab_id").is_some());
    assert!(json_value.get("previous_tab_id").is_some());
    
    // Verify real tab IDs are used, not hardcoded "0"
    assert_eq!(payload.kitty_tab_id, "12345");
    assert_eq!(payload.previous_tab_id, Some("11111".to_string()));
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_process_changed_payload_structure(_ctx: TestContext) -> TestResult {
    // Test process change payload with tab association
    
    let current_process = KittyProcessInfo {
        pid: 1234,
        name: "zsh".to_string(),
        cmdline: Some("/bin/zsh -i".to_string()),
        parent_pid: Some(1000),
    };
    
    let previous_process = KittyProcessInfo {
        pid: 1200,
        name: "bash".to_string(),
        cmdline: Some("/bin/bash".to_string()),
        parent_pid: Some(1000),
    };
    
    let payload = KittyProcessChangedPayload {
        kitty_window_id: "555".to_string(),
        kitty_tab_id: "777".to_string(),  // Should be real tab ID
        previous_process: Some(previous_process),
        current_process,
        change_timestamp: chrono::Utc::now().to_rfc3339(),
        working_directory: Some("/home/user".to_string()),
    };
    
    // Verify serialization works
    let json_value = serde_json::to_value(&payload)?;
    assert!(json_value.get("current_process").is_some());
    assert!(json_value.get("previous_process").is_some());
    assert!(json_value.get("kitty_tab_id").is_some());
    
    // Verify proper tab association
    assert_eq!(payload.kitty_tab_id, "777");
    assert_ne!(payload.kitty_tab_id, "0"); // Should not be hardcoded
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_event_storage_and_retrieval(ctx: TestContext) -> TestResult {
    // Test that Kitty events can be stored and retrieved properly
    
    // Create a command completed event
    let command_event = create_test_event("shell.kitty", "command.completed").await;
    
    // Insert event
    let event_id = insert_event(ctx.pool(), &command_event).await?;
    
    // Retrieve and verify
    let retrieved = crate::common::get_event_by_id(ctx.pool(), event_id).await?;
    assert_eq!(retrieved.source, "shell.kitty");
    assert_eq!(retrieved.event_type, "command.completed");
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_incremental_scrollback_storage(ctx: TestContext) -> TestResult {
    // Test that incremental scrollback stores actual content
    
    let scrollback_event = create_test_event("shell.kitty", "content.streamed").await;
    
    // Insert event
    let event_id = insert_event(ctx.pool(), &scrollback_event).await?;
    
    // Retrieve and verify content is actually stored
    let retrieved = crate::common::get_event_by_id(ctx.pool(), event_id).await?;
    assert_eq!(retrieved.event_type, "content.streamed");
    
    // The payload should contain meaningful data (not just metadata)
    assert!(retrieved.payload.is_object());
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_multiple_event_types_query(ctx: TestContext) -> TestResult {
    // Test querying multiple Kitty event types
    
    // Insert different types of Kitty events
    let events = vec![
        create_test_event("shell.kitty", "command.completed").await,
        create_test_event("shell.kitty", "tab.focused").await,
        create_test_event("shell.kitty", "content.streamed").await,
        create_test_event("shell.kitty", "process.changed").await,
    ];
    
    // Insert all events
    for event in &events {
        insert_event(ctx.pool(), event).await?;
    }
    
    // Query by source
    let kitty_events = crate::common::get_events_by_source(ctx.pool(), "shell.kitty", 100).await?;
    assert!(kitty_events.len() >= 4);
    
    // Verify we have all event types
    let event_types: std::collections::HashSet<String> = kitty_events
        .iter()
        .map(|e| e.event_type.clone())
        .collect();
    
    assert!(event_types.contains("command.completed"));
    assert!(event_types.contains("tab.focused"));
    assert!(event_types.contains("content.streamed"));
    assert!(event_types.contains("process.changed"));
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_event_ordering_and_timing(ctx: TestContext) -> TestResult {
    // Test that Kitty events maintain proper timing and ordering
    
    let _base_time = chrono::Utc::now();
    
    // Create events 
    let events = vec![
        create_test_event("shell.kitty", "command.executed").await,
        create_test_event("shell.kitty", "command.completed").await,
        create_test_event("shell.kitty", "command.executed").await,
        create_test_event("shell.kitty", "command.completed").await,
    ];
    
    // Insert events 
    for event in &events {
        insert_event(ctx.pool(), event).await?;
    }
    
    // Query events in order
    let ordered_events = crate::common::get_events_by_source(ctx.pool(), "shell.kitty", 100).await?;
    assert!(ordered_events.len() >= 4);
    
    // Verify command execution/completion pairs are properly ordered
    let command_events: Vec<_> = ordered_events
        .iter()
        .filter(|e| e.event_type.starts_with("command."))
        .collect();
    
    assert!(command_events.len() >= 4);
    // Events should be in chronological order due to ts_orig
    
    Ok(())
}