use sinex_events_terminal::kitty::{
    KittyEventSource, KittyConfig, KittyCommandExecuted, KittyCommandCompleted, KittyScrollbackIncremental,
    KittyTabCreated, KittyTabFocused, KittyTabClosed, KittyProcessChanged,
    KittyCommandExecutedPayload, KittyCommandCompletedPayload,
    KittyTabCreatedPayload, KittyProcessChangedPayload,
    KittyProcessInfo,
};
use sinex_core::{EventSource, EventSourceContext, EventType};

#[tokio::test]
async fn test_kitty_event_source_creation() {
    // Test that we can create a KittyEventSource - this will likely fail to find socket but should not panic
    let ctx = EventSourceContext::for_test(); // Use test constructor
    
    // This should not panic and create the source (socket discovery may fail but that's expected)
    let result = KittyEventSource::initialize(ctx).await;
    assert!(result.is_ok(), "Should be able to create KittyEventSource even without socket");
}

#[test]
fn test_kitty_config_serialization() {
    let config = KittyConfig {
        poll_interval_seconds: 5,
        socket_path: Some("/tmp/kitty.sock".to_string()),
        enabled: true,
    };
    
    // Should serialize/deserialize properly
    let serialized = serde_json::to_string(&config).expect("Should serialize");
    let deserialized: KittyConfig = serde_json::from_str(&serialized).expect("Should deserialize");
    
    assert_eq!(config.poll_interval_seconds, deserialized.poll_interval_seconds);
    assert_eq!(config.socket_path, deserialized.socket_path);
    assert_eq!(config.enabled, deserialized.enabled);
}

#[test]
fn test_kitty_event_types() {
    // Verify event type constants
    assert_eq!(KittyCommandExecuted::EVENT_NAME, "command.started");
    assert_eq!(KittyCommandCompleted::EVENT_NAME, "command.completed");
    assert_eq!(KittyScrollbackIncremental::EVENT_NAME, "content.streamed");
    assert_eq!(KittyTabCreated::EVENT_NAME, "tab.created");
    assert_eq!(KittyTabFocused::EVENT_NAME, "tab.focused");
    assert_eq!(KittyTabClosed::EVENT_NAME, "tab.closed");
    assert_eq!(KittyProcessChanged::EVENT_NAME, "process.changed");
    assert_eq!(KittyEventSource::SOURCE_NAME, "terminal.kitty");
}

#[test]
fn test_kitty_payload_serialization() {
    // Test KittyCommandExecutedPayload
    let command_payload = KittyCommandExecutedPayload {
        command: "ls -la".to_string(),
        working_directory: Some("/home/user".to_string()),
        kitty_window_id: "123".to_string(),
        kitty_tab_id: "456".to_string(),
        exit_status: Some(0),
        execution_time_ms: Some(250),
        prompt_detected: true,
        scrollback_hash: Some("abc123".to_string()),
    };
    
    let serialized = serde_json::to_string(&command_payload).expect("Should serialize");
    let deserialized: KittyCommandExecutedPayload = serde_json::from_str(&serialized).expect("Should deserialize");
    
    assert_eq!(command_payload.command, deserialized.command);
    assert_eq!(command_payload.working_directory, deserialized.working_directory);
    assert_eq!(command_payload.exit_status, deserialized.exit_status);
    
    // Test KittyTabCreatedPayload
    let tab_payload = KittyTabCreatedPayload {
        kitty_tab_id: "tab123".to_string(),
        kitty_window_id: "win456".to_string(),
        tab_title: "Terminal 1".to_string(),
        tab_index: 0,
        is_active: true,
        creation_timestamp: "2024-01-01T12:00:00Z".to_string(),
    };
    
    let serialized = serde_json::to_string(&tab_payload).expect("Should serialize");
    let deserialized: KittyTabCreatedPayload = serde_json::from_str(&serialized).expect("Should deserialize");
    
    assert_eq!(tab_payload.kitty_tab_id, deserialized.kitty_tab_id);
    assert_eq!(tab_payload.tab_index, deserialized.tab_index);
    assert_eq!(tab_payload.is_active, deserialized.is_active);
    
    // Test KittyProcessChangedPayload
    let process_info = KittyProcessInfo {
        pid: 1234,
        name: "bash".to_string(),
        cmdline: Some("/bin/bash".to_string()),
        parent_pid: Some(5678),
    };
    
    let process_payload = KittyProcessChangedPayload {
        kitty_window_id: "win123".to_string(),
        kitty_tab_id: "tab456".to_string(),
        previous_process: None,
        current_process: process_info.clone(),
        change_timestamp: "2024-01-01T12:00:00Z".to_string(),
        working_directory: Some("/home/user".to_string()),
    };
    
    let serialized = serde_json::to_string(&process_payload).expect("Should serialize");
    let deserialized: KittyProcessChangedPayload = serde_json::from_str(&serialized).expect("Should deserialize");
    
    assert_eq!(process_payload.current_process.pid, deserialized.current_process.pid);
    assert_eq!(process_payload.current_process.name, deserialized.current_process.name);
}


#[test]
fn test_kitty_command_completed_payload() {
    let completion_payload = KittyCommandCompletedPayload {
        command: "git status".to_string(),
        command_output: "On branch main\nnothing to commit, working tree clean".to_string(),
        working_directory: Some("/home/user/project".to_string()),
        kitty_window_id: "win123".to_string(),
        kitty_tab_id: "tab456".to_string(),
        exit_status: Some(0),
        execution_time_ms: Some(150),
        output_size_bytes: 42,
        output_line_count: 2,
        shell_integration_used: true,
        completion_timestamp: "2024-01-01T12:00:00Z".to_string(),
    };
    
    let serialized = serde_json::to_string(&completion_payload).expect("Should serialize");
    let deserialized: KittyCommandCompletedPayload = serde_json::from_str(&serialized).expect("Should deserialize");
    
    assert_eq!(completion_payload.command, deserialized.command);
    assert_eq!(completion_payload.command_output, deserialized.command_output);
    assert_eq!(completion_payload.working_directory, deserialized.working_directory);
    assert_eq!(completion_payload.kitty_window_id, deserialized.kitty_window_id);
    assert_eq!(completion_payload.kitty_tab_id, deserialized.kitty_tab_id);
    assert_eq!(completion_payload.exit_status, deserialized.exit_status);
    assert_eq!(completion_payload.execution_time_ms, deserialized.execution_time_ms);
    assert_eq!(completion_payload.output_size_bytes, deserialized.output_size_bytes);
    assert_eq!(completion_payload.output_line_count, deserialized.output_line_count);
    assert_eq!(completion_payload.shell_integration_used, deserialized.shell_integration_used);
    assert_eq!(completion_payload.completion_timestamp, deserialized.completion_timestamp);
}