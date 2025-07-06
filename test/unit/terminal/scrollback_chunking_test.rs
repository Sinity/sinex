use sinex_events_terminal::scrollback::{
    ScrollbackCapture, ScrollbackConfig, TerminalScrollbackCaptured,
    TerminalScrollbackCapturedPayload, CommandOutputCaptured,
    CommandOutputCapturedPayload,
};
use sinex_core::{EventSource, EventSourceContext, EventType, chunking::ChunkingService};
use std::path::PathBuf;
use chrono::Utc;

#[test]
fn test_scrollback_config_default() {
    let config = ScrollbackConfig::default();
    
    assert_eq!(config.capture_interval_secs, 60);
    assert_eq!(config.max_scrollback_lines, 10000);
    assert_eq!(config.include_ansi_codes, false);
    assert_eq!(config.capture_command_output, true);
    assert_eq!(config.save_to_files, false);
    assert_eq!(config.capture_on_command, true);
    assert_eq!(config.command_capture_delay_ms, 500);
    assert_eq!(config.chunking_threshold_bytes, 32_768); // 32KB
    assert_eq!(config.enable_chunking, true);
}

#[test]
fn test_scrollback_config_serialization() {
    let config = ScrollbackConfig {
        kitty_socket_path: "/tmp/test_kitty.sock".to_string(),
        capture_interval_secs: 120,
        max_scrollback_lines: 5000,
        include_ansi_codes: true,
        capture_command_output: false,
        save_to_files: true,
        scrollback_dir: PathBuf::from("/tmp/scrollback"),
        capture_on_command: false,
        command_capture_delay_ms: 1000,
        chunking_threshold_bytes: 16_384, // 16KB
        enable_chunking: false,
    };
    
    let serialized = serde_json::to_string(&config).expect("Should serialize");
    let deserialized: ScrollbackConfig = serde_json::from_str(&serialized).expect("Should deserialize");
    
    assert_eq!(config.kitty_socket_path, deserialized.kitty_socket_path);
    assert_eq!(config.capture_interval_secs, deserialized.capture_interval_secs);
    assert_eq!(config.max_scrollback_lines, deserialized.max_scrollback_lines);
    assert_eq!(config.include_ansi_codes, deserialized.include_ansi_codes);
    assert_eq!(config.capture_command_output, deserialized.capture_command_output);
    assert_eq!(config.save_to_files, deserialized.save_to_files);
    assert_eq!(config.scrollback_dir, deserialized.scrollback_dir);
    assert_eq!(config.capture_on_command, deserialized.capture_on_command);
    assert_eq!(config.command_capture_delay_ms, deserialized.command_capture_delay_ms);
    assert_eq!(config.chunking_threshold_bytes, deserialized.chunking_threshold_bytes);
    assert_eq!(config.enable_chunking, deserialized.enable_chunking);
}

#[test]
fn test_terminal_scrollback_payload_small_content() {
    // Test payload with small content (below chunking threshold)
    let small_text = "This is a small scrollback content.";
    
    let payload = TerminalScrollbackCapturedPayload {
        window_id: 123,
        terminal_type: "kitty".to_string(),
        cwd: "/home/user".to_string(),
        window_title: "Terminal".to_string(),
        scrollback_text: Some(small_text.to_string()),
        scrollback_chunks: None,
        scrollback_lines: 1,
        scrollback_size_bytes: small_text.len() as u64,
        is_chunked: false,
        chunk_count: None,
        includes_screen: true,
        has_ansi_codes: false,
        timestamp: Utc::now(),
    };
    
    let serialized = serde_json::to_string(&payload).expect("Should serialize");
    let deserialized: TerminalScrollbackCapturedPayload = serde_json::from_str(&serialized).expect("Should deserialize");
    
    assert_eq!(payload.window_id, deserialized.window_id);
    assert_eq!(payload.terminal_type, deserialized.terminal_type);
    assert_eq!(payload.scrollback_text, deserialized.scrollback_text);
    assert_eq!(payload.scrollback_chunks, deserialized.scrollback_chunks);
    assert_eq!(payload.is_chunked, deserialized.is_chunked);
    assert_eq!(payload.chunk_count, deserialized.chunk_count);
    assert_eq!(payload.scrollback_size_bytes, deserialized.scrollback_size_bytes);
}

#[test]
fn test_terminal_scrollback_payload_chunked_content() {
    // Test payload with chunked content (above threshold)
    let chunking_service = ChunkingService::with_default_config();
    let large_text = "A".repeat(50_000); // 50KB of 'A' characters
    let chunks = chunking_service.chunk_string(&large_text).expect("Should chunk successfully");
    
    let chunk_jsons: Vec<serde_json::Value> = chunks
        .into_iter()
        .map(|chunk| serde_json::to_value(chunk).unwrap())
        .collect();
    
    let payload = TerminalScrollbackCapturedPayload {
        window_id: 456,
        terminal_type: "kitty".to_string(),
        cwd: "/home/user".to_string(),
        window_title: "Terminal".to_string(),
        scrollback_text: None, // No raw text for chunked content
        scrollback_chunks: Some(chunk_jsons.clone()),
        scrollback_lines: 1,
        scrollback_size_bytes: large_text.len() as u64,
        is_chunked: true,
        chunk_count: Some(chunk_jsons.len() as u32),
        includes_screen: true,
        has_ansi_codes: false,
        timestamp: Utc::now(),
    };
    
    let serialized = serde_json::to_string(&payload).expect("Should serialize");
    let deserialized: TerminalScrollbackCapturedPayload = serde_json::from_str(&serialized).expect("Should deserialize");
    
    assert_eq!(payload.window_id, deserialized.window_id);
    assert_eq!(payload.terminal_type, deserialized.terminal_type);
    assert_eq!(payload.scrollback_text, deserialized.scrollback_text); // Should be None
    assert!(deserialized.scrollback_chunks.is_some());
    assert_eq!(payload.is_chunked, deserialized.is_chunked);
    assert_eq!(payload.chunk_count, deserialized.chunk_count);
    assert_eq!(payload.scrollback_size_bytes, deserialized.scrollback_size_bytes);
    
    // Verify chunk count matches
    assert_eq!(
        payload.chunk_count.unwrap() as usize,
        deserialized.scrollback_chunks.as_ref().unwrap().len()
    );
}

#[test]
fn test_chunking_threshold_logic() {
    let chunking_service = ChunkingService::with_default_config();
    
    // Test data below threshold (32KB default)
    let small_data = "A".repeat(16_000); // 16KB
    assert!(small_data.len() < 32_768, "Small data should be below threshold");
    
    // Test data above threshold
    let large_data = "A".repeat(40_000); // 40KB
    assert!(large_data.len() > 32_768, "Large data should be above threshold");
    
    // Verify chunking works for large data
    let chunks = chunking_service.chunk_string(&large_data).expect("Should chunk large data");
    assert!(chunks.len() > 1, "Large data should produce multiple chunks");
    
    // Verify chunk reconstruction
    let mut reconstructed = Vec::new();
    for chunk in &chunks {
        reconstructed.extend_from_slice(&chunk.data);
    }
    let reconstructed_string = String::from_utf8(reconstructed).expect("Should reconstruct as valid UTF-8");
    assert_eq!(large_data, reconstructed_string, "Reconstructed data should match original");
}

#[test]
fn test_event_type_constants() {
    assert_eq!(TerminalScrollbackCaptured::EVENT_NAME, "scrollback.full");
    assert_eq!(CommandOutputCaptured::EVENT_NAME, "command.output");
    assert_eq!(ScrollbackCapture::SOURCE_NAME, "shell.scrollback");
}

#[test]
fn test_command_output_payload() {
    let payload = CommandOutputCapturedPayload {
        window_id: 789,
        command_text: Some("git status".to_string()),
        output_text: "On branch main\nnothing to commit, working tree clean".to_string(),
        output_type: "last_cmd_output".to_string(),
        cwd: "/home/user/project".to_string(),
        timestamp: Utc::now(),
    };
    
    let serialized = serde_json::to_string(&payload).expect("Should serialize");
    let deserialized: CommandOutputCapturedPayload = serde_json::from_str(&serialized).expect("Should deserialize");
    
    assert_eq!(payload.window_id, deserialized.window_id);
    assert_eq!(payload.command_text, deserialized.command_text);
    assert_eq!(payload.output_text, deserialized.output_text);
    assert_eq!(payload.output_type, deserialized.output_type);
    assert_eq!(payload.cwd, deserialized.cwd);
}

#[tokio::test]
async fn test_scrollback_capture_initialization() {
    let config = ScrollbackConfig {
        kitty_socket_path: "/tmp/nonexistent_kitty.sock".to_string(),
        capture_interval_secs: 60,
        max_scrollback_lines: 1000,
        include_ansi_codes: false,
        capture_command_output: true,
        save_to_files: false,
        scrollback_dir: PathBuf::from("/tmp"),
        capture_on_command: true,
        command_capture_delay_ms: 500,
        chunking_threshold_bytes: 16_384,
        enable_chunking: true,
    };
    
    let ctx = EventSourceContext {
        config: serde_json::to_value(config).expect("Should serialize config"),
        annex_repo_path: None,
        db_pool: None,
    };
    
    // Should initialize successfully even without a kitty socket
    let result = ScrollbackCapture::initialize(ctx).await;
    assert!(result.is_ok(), "ScrollbackCapture should initialize successfully");
}

#[test]
fn test_chunking_enabled_vs_disabled() {
    // Test with chunking enabled
    let config_with_chunking = ScrollbackConfig {
        enable_chunking: true,
        chunking_threshold_bytes: 1000, // Low threshold for testing
        ..ScrollbackConfig::default()
    };
    
    // Test with chunking disabled
    let config_without_chunking = ScrollbackConfig {
        enable_chunking: false,
        chunking_threshold_bytes: 1000,
        ..ScrollbackConfig::default()
    };
    
    // Verify configs serialize properly
    let serialized_with = serde_json::to_string(&config_with_chunking).expect("Should serialize");
    let serialized_without = serde_json::to_string(&config_without_chunking).expect("Should serialize");
    
    let deserialized_with: ScrollbackConfig = serde_json::from_str(&serialized_with).expect("Should deserialize");
    let deserialized_without: ScrollbackConfig = serde_json::from_str(&serialized_without).expect("Should deserialize");
    
    assert_eq!(config_with_chunking.enable_chunking, deserialized_with.enable_chunking);
    assert_eq!(config_without_chunking.enable_chunking, deserialized_without.enable_chunking);
    assert_ne!(deserialized_with.enable_chunking, deserialized_without.enable_chunking);
}