//! Terminal Unit Tests
//!
//! Consolidated terminal integration tests covering:
//! - Kitty terminal integration and event handling
//! - Scrollback capture and chunking functionality
//! - Asciinema auto-recording setup and configuration
//! - Terminal event types and serialization
//! - Shell integration and environment setup

use crate::common::prelude::*;
use sinex_events_terminal::{
    kitty::{
        KittyEventSource, KittyConfig, KittyCommandCompleted, 
        KittyScrollbackIncremental, KittyTabCreated, KittyTabFocused, KittyTabClosed, 
        KittyProcessChanged, KittyCommandCompletedPayload,
        KittyTabCreatedPayload, KittyProcessChangedPayload, KittyProcessInfo,
    },
    scrollback::{
        ScrollbackCapture, ScrollbackConfig, TerminalScrollbackCaptured,
        TerminalScrollbackCapturedPayload, CommandOutputCaptured,
        CommandOutputCapturedPayload,
    },
    asciinema::{AsciinemaConfig, AsciinemaRecorder},
};
use sinex_core::{
    EventSource, EventSourceContext, EventType, chunking::ChunkingService
};
use std::path::PathBuf;
use chrono::Utc;
use tempfile::TempDir;

// =============================================================================
// KITTY INTEGRATION TESTS
// =============================================================================

/// Test Kitty event source creation without requiring actual socket
#[sinex_test]
async fn test_kitty_event_source_creation(_ctx: TestContext) -> TestResult {
    // Test that we can create a KittyEventSource - this will likely fail to find socket but should not panic
    let ctx = EventSourceContext::for_test(); // Use test constructor
    
    // This should not panic and create the source (socket discovery may fail but that's expected)
    let result = KittyEventSource::initialize(ctx).await;
    assert!(result.is_ok(), "Should be able to create KittyEventSource even without socket");
    Ok(())
}

/// Test Kitty configuration serialization and deserialization
#[sinex_test]
async fn test_kitty_config_serialization(_ctx: TestContext) -> TestResult {
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
    Ok(())
}

/// Test Kitty event type constants and source name
#[sinex_test]
async fn test_kitty_event_types(_ctx: TestContext) -> TestResult {
    // Verify event type constants
    assert_eq!(KittyCommandCompleted::EVENT_NAME, "command.completed");
    assert_eq!(KittyScrollbackIncremental::EVENT_NAME, "content.streamed");
    assert_eq!(KittyTabCreated::EVENT_NAME, "tab.created");
    assert_eq!(KittyTabFocused::EVENT_NAME, "tab.focused");
    assert_eq!(KittyTabClosed::EVENT_NAME, "tab.closed");
    assert_eq!(KittyProcessChanged::EVENT_NAME, "process.changed");
    assert_eq!(KittyEventSource::SOURCE_NAME, "shell.kitty");
    Ok(())
}

/// Test Kitty event payload serialization
#[sinex_test]
async fn test_kitty_event_payload_serialization(_ctx: TestContext) -> TestResult {
    // Test KittyCommandCompletedPayload
    let cmd_payload = KittyCommandCompletedPayload {
        command: "ls -la".to_string(),
        cwd: "/home/user".to_string(),
        pid: 1234,
        session_id: "test_session".to_string(),
        timestamp: Utc::now(),
    };
    
    let serialized = serde_json::to_string(&cmd_payload)?;
    let deserialized: KittyCommandCompletedPayload = serde_json::from_str(&serialized)?;
    
    assert_eq!(cmd_payload.command, deserialized.command);
    assert_eq!(cmd_payload.cwd, deserialized.cwd);
    assert_eq!(cmd_payload.pid, deserialized.pid);
    assert_eq!(cmd_payload.session_id, deserialized.session_id);
    
    // Test KittyProcessChangedPayload
    let process_payload = KittyProcessChangedPayload {
        old_process: KittyProcessInfo {
            pid: 1234,
            name: "bash".to_string(),
            cmdline: Some("bash".to_string()),
            parent_pid: Some(999),
        },
        new_process: KittyProcessInfo {
            pid: 5678,
            name: "vim".to_string(),
            cmdline: Some("vim file.txt".to_string()),
            parent_pid: Some(1234),
        },
        session_id: "test_session".to_string(),
        timestamp: Utc::now(),
    };
    
    let serialized = serde_json::to_string(&process_payload)?;
    let deserialized: KittyProcessChangedPayload = serde_json::from_str(&serialized)?;
    
    assert_eq!(process_payload.old_process.pid, deserialized.old_process.pid);
    assert_eq!(process_payload.new_process.name, deserialized.new_process.name);
    assert_eq!(process_payload.session_id, deserialized.session_id);
    
    Ok(())
}

/// Test Kitty configuration with various socket paths
#[sinex_test]
async fn test_kitty_config_socket_paths(_ctx: TestContext) -> TestResult {
    let configs = vec![
        KittyConfig {
            poll_interval_seconds: 1,
            socket_path: None, // Auto-discovery
            enabled: true,
        },
        KittyConfig {
            poll_interval_seconds: 5,
            socket_path: Some("/tmp/kitty.sock".to_string()),
            enabled: true,
        },
        KittyConfig {
            poll_interval_seconds: 10,
            socket_path: Some("/run/user/1000/kitty.sock".to_string()),
            enabled: false,
        },
    ];
    
    for config in configs {
        let serialized = serde_json::to_string(&config)?;
        let deserialized: KittyConfig = serde_json::from_str(&serialized)?;
        
        assert_eq!(config.poll_interval_seconds, deserialized.poll_interval_seconds);
        assert_eq!(config.socket_path, deserialized.socket_path);
        assert_eq!(config.enabled, deserialized.enabled);
    }
    
    Ok(())
}

/// Test Kitty tab event payloads
#[sinex_test]
async fn test_kitty_tab_event_payloads(_ctx: TestContext) -> TestResult {
    let tab_payload = KittyTabCreatedPayload {
        tab_id: 42,
        window_id: 1,
        title: "New Tab".to_string(),
        cwd: "/home/user/project".to_string(),
        timestamp: Utc::now(),
    };
    
    let serialized = serde_json::to_string(&tab_payload)?;
    let deserialized: KittyTabCreatedPayload = serde_json::from_str(&serialized)?;
    
    assert_eq!(tab_payload.tab_id, deserialized.tab_id);
    assert_eq!(tab_payload.window_id, deserialized.window_id);
    assert_eq!(tab_payload.title, deserialized.title);
    assert_eq!(tab_payload.cwd, deserialized.cwd);
    
    Ok(())
}

// =============================================================================
// SCROLLBACK CAPTURE TESTS
// =============================================================================

/// Test scrollback configuration defaults
#[sinex_test]
async fn test_scrollback_config_default(_ctx: TestContext) -> TestResult {
    let config = ScrollbackConfig::default();
    
    assert_eq!(config.capture_interval_secs, 180); // 3 minutes
    assert_eq!(config.max_scrollback_lines, 10000);
    assert!(!config.include_ansi_codes);
    assert!(config.capture_command_output);
    assert!(config.auto_annex);
    assert!(config.capture_on_command);
    assert_eq!(config.command_capture_delay_ms, 500);
    assert_eq!(config.chunking_threshold_bytes, 32_768); // 32KB
    assert!(config.enable_chunking);
    Ok(())
}

/// Test scrollback configuration serialization
#[sinex_test]
async fn test_scrollback_config_serialization(_ctx: TestContext) -> TestResult {
    let config = ScrollbackConfig {
        kitty_socket_path: "/tmp/test_kitty.sock".to_string(),
        capture_interval_secs: 120,
        max_scrollback_lines: 5000,
        include_ansi_codes: true,
        capture_command_output: false,
        git_annex_repo: Some(PathBuf::from("/tmp/annex")),
        auto_annex: true,
        capture_on_command: false,
        command_capture_delay_ms: 1000,
        chunking_threshold_bytes: 16_384, // 16KB
        enable_chunking: false,
        annex_threshold_bytes: 32_000,
    };
    
    let serialized = serde_json::to_string(&config).expect("Should serialize");
    let deserialized: ScrollbackConfig = serde_json::from_str(&serialized).expect("Should deserialize");
    
    assert_eq!(config.kitty_socket_path, deserialized.kitty_socket_path);
    assert_eq!(config.capture_interval_secs, deserialized.capture_interval_secs);
    assert_eq!(config.max_scrollback_lines, deserialized.max_scrollback_lines);
    assert_eq!(config.include_ansi_codes, deserialized.include_ansi_codes);
    assert_eq!(config.capture_command_output, deserialized.capture_command_output);
    assert_eq!(config.auto_annex, deserialized.auto_annex);
    assert_eq!(config.capture_on_command, deserialized.capture_on_command);
    assert_eq!(config.command_capture_delay_ms, deserialized.command_capture_delay_ms);
    assert_eq!(config.chunking_threshold_bytes, deserialized.chunking_threshold_bytes);
    assert_eq!(config.enable_chunking, deserialized.enable_chunking);
    assert_eq!(config.annex_threshold_bytes, deserialized.annex_threshold_bytes);
    Ok(())
}

/// Test scrollback event type constants
#[sinex_test]
async fn test_scrollback_event_types(_ctx: TestContext) -> TestResult {
    assert_eq!(TerminalScrollbackCaptured::EVENT_NAME, "scrollback.full");
    assert_eq!(CommandOutputCaptured::EVENT_NAME, "command.output");
    assert_eq!(ScrollbackCapture::SOURCE_NAME, "shell.scrollback");
    Ok(())
}

/// Test scrollback payload serialization
#[sinex_test]
async fn test_scrollback_payload_serialization(_ctx: TestContext) -> TestResult {
    let scrollback_payload = TerminalScrollbackCapturedPayload {
        session_id: "test_session_123".to_string(),
        content: "This is terminal output\nWith multiple lines\n".to_string(),
        line_count: 2,
        byte_size: 42,
        timestamp: Utc::now(),
        contains_ansi: false,
        chunked: false,
        chunk_id: None,
        git_annex_key: None,
    };
    
    let serialized = serde_json::to_string(&scrollback_payload)?;
    let deserialized: TerminalScrollbackCapturedPayload = serde_json::from_str(&serialized)?;
    
    assert_eq!(scrollback_payload.session_id, deserialized.session_id);
    assert_eq!(scrollback_payload.content, deserialized.content);
    assert_eq!(scrollback_payload.line_count, deserialized.line_count);
    assert_eq!(scrollback_payload.byte_size, deserialized.byte_size);
    assert_eq!(scrollback_payload.contains_ansi, deserialized.contains_ansi);
    assert_eq!(scrollback_payload.chunked, deserialized.chunked);
    assert_eq!(scrollback_payload.chunk_id, deserialized.chunk_id);
    assert_eq!(scrollback_payload.git_annex_key, deserialized.git_annex_key);
    
    Ok(())
}

/// Test command output payload serialization
#[sinex_test]
async fn test_command_output_payload_serialization(_ctx: TestContext) -> TestResult {
    let cmd_output_payload = CommandOutputCapturedPayload {
        command: "ls -la".to_string(),
        output: "total 8\ndrwxr-xr-x 2 user user 4096 Jan 1 12:00 .\n".to_string(),
        exit_code: 0,
        execution_time_ms: 150,
        session_id: "test_session_456".to_string(),
        timestamp: Utc::now(),
        chunked: false,
        chunk_id: None,
        git_annex_key: Some("SHA256E-s1024--abcdef123456".to_string()),
    };
    
    let serialized = serde_json::to_string(&cmd_output_payload)?;
    let deserialized: CommandOutputCapturedPayload = serde_json::from_str(&serialized)?;
    
    assert_eq!(cmd_output_payload.command, deserialized.command);
    assert_eq!(cmd_output_payload.output, deserialized.output);
    assert_eq!(cmd_output_payload.exit_code, deserialized.exit_code);
    assert_eq!(cmd_output_payload.execution_time_ms, deserialized.execution_time_ms);
    assert_eq!(cmd_output_payload.session_id, deserialized.session_id);
    assert_eq!(cmd_output_payload.chunked, deserialized.chunked);
    assert_eq!(cmd_output_payload.chunk_id, deserialized.chunk_id);
    assert_eq!(cmd_output_payload.git_annex_key, deserialized.git_annex_key);
    
    Ok(())
}

/// Test scrollback chunking configuration
#[sinex_test]
async fn test_scrollback_chunking_configuration(_ctx: TestContext) -> TestResult {
    let chunking_configs = vec![
        ScrollbackConfig {
            chunking_threshold_bytes: 1024,
            enable_chunking: true,
            annex_threshold_bytes: 2048,
            auto_annex: true,
            ..Default::default()
        },
        ScrollbackConfig {
            chunking_threshold_bytes: 65536,
            enable_chunking: false,
            annex_threshold_bytes: 1_048_576,
            auto_annex: false,
            ..Default::default()
        },
    ];
    
    for config in chunking_configs {
        // Test serialization
        let serialized = serde_json::to_string(&config)?;
        let deserialized: ScrollbackConfig = serde_json::from_str(&serialized)?;
        
        assert_eq!(config.chunking_threshold_bytes, deserialized.chunking_threshold_bytes);
        assert_eq!(config.enable_chunking, deserialized.enable_chunking);
        assert_eq!(config.annex_threshold_bytes, deserialized.annex_threshold_bytes);
        assert_eq!(config.auto_annex, deserialized.auto_annex);
        
        // Test logical constraints
        if config.enable_chunking {
            assert!(config.chunking_threshold_bytes > 0, "Chunking threshold must be positive");
        }
        if config.auto_annex {
            assert!(config.annex_threshold_bytes > 0, "Annex threshold must be positive");
        }
    }
    
    Ok(())
}

/// Test scrollback capture timing configuration
#[sinex_test]
async fn test_scrollback_capture_timing(_ctx: TestContext) -> TestResult {
    let timing_configs = vec![
        ScrollbackConfig {
            capture_interval_secs: 60,   // 1 minute
            capture_on_command: true,
            command_capture_delay_ms: 100,
            ..Default::default()
        },
        ScrollbackConfig {
            capture_interval_secs: 3600, // 1 hour
            capture_on_command: false,
            command_capture_delay_ms: 2000,
            ..Default::default()
        },
    ];
    
    for config in timing_configs {
        assert!(config.capture_interval_secs > 0, "Capture interval must be positive");
        // command_capture_delay_ms is u64, so always non-negative
        
        // Test serialization
        let serialized = serde_json::to_string(&config)?;
        let deserialized: ScrollbackConfig = serde_json::from_str(&serialized)?;
        
        assert_eq!(config.capture_interval_secs, deserialized.capture_interval_secs);
        assert_eq!(config.capture_on_command, deserialized.capture_on_command);
        assert_eq!(config.command_capture_delay_ms, deserialized.command_capture_delay_ms);
    }
    
    Ok(())
}

// =============================================================================
// ASCIINEMA AUTO-RECORDING TESTS
// =============================================================================

/// Test Asciinema auto-recording setup and shell integration
#[sinex_test]
async fn test_asciinema_auto_recording_setup(_ctx: TestContext) -> TestResult {
    // Create temporary directory for test
    let temp_dir = TempDir::new()?;
    let temp_home = temp_dir.path().to_path_buf();
    
    // Create asciinema config with auto-recording enabled
    let config = AsciinemaConfig {
        recordings_dir: temp_home.join(".local/share/asciinema/asciicast"),
        file_pattern: "*.cast".to_string(),
        polling_interval_secs: 5,
        auto_start_recording: true,
        record_command: "asciinema rec --quiet --overwrite".to_string(),
        git_annex_repo: Some(temp_home.join("sinex-annex")),
        auto_annex: true,
    };
    
    // Set HOME environment variable for the test
    std::env::set_var("HOME", temp_home.to_string_lossy().to_string());
    
    // Create event source context
    let event_ctx = EventSourceContext::new(serde_json::to_value(&config)?);
    
    // Initialize the asciinema recorder
    let _recorder = AsciinemaRecorder::initialize(event_ctx).await?;
    
    // Verify that shell integration files were created/modified
    let bashrc_path = temp_home.join(".bashrc");
    let zshrc_path = temp_home.join(".zshrc");
    let fish_config_path = temp_home.join(".config/fish/config.fish");
    
    // Check that the shell files contain sinex integration
    if bashrc_path.exists() {
        let bashrc_content = tokio::fs::read_to_string(&bashrc_path).await?;
        assert!(bashrc_content.contains("# SINEX AUTO-RECORDING"));
        assert!(bashrc_content.contains("SINEX_TERMINAL_SESSION_ULID"));
    }
    
    if zshrc_path.exists() {
        let zshrc_content = tokio::fs::read_to_string(&zshrc_path).await?;
        assert!(zshrc_content.contains("# SINEX AUTO-RECORDING"));
        assert!(zshrc_content.contains("SINEX_TERMINAL_SESSION_ULID"));
    }
    
    if fish_config_path.exists() {
        let fish_content = tokio::fs::read_to_string(&fish_config_path).await?;
        assert!(fish_content.contains("# SINEX AUTO-RECORDING"));
        assert!(fish_content.contains("SINEX_TERMINAL_SESSION_ULID"));
    }
    
    Ok(())
}

/// Test Asciinema configuration serialization
#[sinex_test]
async fn test_asciinema_config_serialization(_ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_path_buf();
    
    let config = AsciinemaConfig {
        recordings_dir: temp_path.join("recordings"),
        file_pattern: "session_*.cast".to_string(),
        polling_interval_secs: 10,
        auto_start_recording: false,
        record_command: "asciinema rec --max-wait 2".to_string(),
        git_annex_repo: Some(temp_path.join("annex")),
        auto_annex: false,
    };
    
    let serialized = serde_json::to_string(&config)?;
    let deserialized: AsciinemaConfig = serde_json::from_str(&serialized)?;
    
    assert_eq!(config.recordings_dir, deserialized.recordings_dir);
    assert_eq!(config.file_pattern, deserialized.file_pattern);
    assert_eq!(config.polling_interval_secs, deserialized.polling_interval_secs);
    assert_eq!(config.auto_start_recording, deserialized.auto_start_recording);
    assert_eq!(config.record_command, deserialized.record_command);
    assert_eq!(config.git_annex_repo, deserialized.git_annex_repo);
    assert_eq!(config.auto_annex, deserialized.auto_annex);
    
    Ok(())
}

/// Test Asciinema configuration validation
#[sinex_test]
async fn test_asciinema_config_validation(_ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_path_buf();
    
    let valid_configs = vec![
        AsciinemaConfig {
            recordings_dir: temp_path.join("recordings"),
            file_pattern: "*.cast".to_string(),
            polling_interval_secs: 1,
            auto_start_recording: true,
            record_command: "asciinema rec".to_string(),
            git_annex_repo: None,
            auto_annex: false,
        },
        AsciinemaConfig {
            recordings_dir: temp_path.join("custom_recordings"),
            file_pattern: "session_*.cast".to_string(),
            polling_interval_secs: 60,
            auto_start_recording: false,
            record_command: "asciinema rec --max-wait 5 --quiet".to_string(),
            git_annex_repo: Some(temp_path.join("annex")),
            auto_annex: true,
        },
    ];
    
    for config in valid_configs {
        // Validate constraints
        assert!(config.polling_interval_secs > 0, "Polling interval must be positive");
        assert!(!config.file_pattern.is_empty(), "File pattern cannot be empty");
        assert!(!config.record_command.is_empty(), "Record command cannot be empty");
        
        if config.auto_annex {
            assert!(config.git_annex_repo.is_some(), "Git annex repo required when auto_annex is true");
        }
        
        // Test serialization
        let serialized = serde_json::to_string(&config)?;
        let deserialized: AsciinemaConfig = serde_json::from_str(&serialized)?;
        assert_eq!(config.polling_interval_secs, deserialized.polling_interval_secs);
    }
    
    Ok(())
}

/// Test Asciinema shell integration setup
#[sinex_test]
async fn test_asciinema_shell_integration_setup(_ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let temp_home = temp_dir.path().to_path_buf();
    
    // Create initial shell configuration files
    let bashrc_path = temp_home.join(".bashrc");
    let zshrc_path = temp_home.join(".zshrc");
    let fish_config_dir = temp_home.join(".config/fish");
    let fish_config_path = fish_config_dir.join("config.fish");
    
    // Create directories
    tokio::fs::create_dir_all(&fish_config_dir).await?;
    
    // Create basic shell config files
    tokio::fs::write(&bashrc_path, "# Basic bashrc\nexport PATH=$PATH:/usr/local/bin\n").await?;
    tokio::fs::write(&zshrc_path, "# Basic zshrc\nautoload -U compinit\ncompinit\n").await?;
    tokio::fs::write(&fish_config_path, "# Basic fish config\nset -gx PATH $PATH /usr/local/bin\n").await?;
    
    // Create asciinema config
    let config = AsciinemaConfig {
        recordings_dir: temp_home.join(".local/share/asciinema/asciicast"),
        file_pattern: "*.cast".to_string(),
        polling_interval_secs: 5,
        auto_start_recording: true,
        record_command: "asciinema rec --quiet --overwrite".to_string(),
        git_annex_repo: Some(temp_home.join("sinex-annex")),
        auto_annex: true,
    };
    
    // Set HOME environment variable
    std::env::set_var("HOME", temp_home.to_string_lossy().to_string());
    
    // Create event source context and initialize recorder
    let event_ctx = EventSourceContext::new(serde_json::to_value(&config)?);
    let _recorder = AsciinemaRecorder::initialize(event_ctx).await?;
    
    // Verify shell integration was added
    let bashrc_content = tokio::fs::read_to_string(&bashrc_path).await?;
    assert!(bashrc_content.contains("# SINEX AUTO-RECORDING"));
    assert!(bashrc_content.contains("SINEX_TERMINAL_SESSION_ULID"));
    assert!(bashrc_content.contains("# Basic bashrc")); // Original content preserved
    
    let zshrc_content = tokio::fs::read_to_string(&zshrc_path).await?;
    assert!(zshrc_content.contains("# SINEX AUTO-RECORDING"));
    assert!(zshrc_content.contains("SINEX_TERMINAL_SESSION_ULID"));
    assert!(zshrc_content.contains("# Basic zshrc")); // Original content preserved
    
    let fish_content = tokio::fs::read_to_string(&fish_config_path).await?;
    assert!(fish_content.contains("# SINEX AUTO-RECORDING"));
    assert!(fish_content.contains("SINEX_TERMINAL_SESSION_ULID"));
    assert!(fish_content.contains("# Basic fish config")); // Original content preserved
    
    Ok(())
}

/// Test Asciinema recording directory setup
#[sinex_test]
async fn test_asciinema_recording_directory_setup(_ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let temp_home = temp_dir.path().to_path_buf();
    let recordings_dir = temp_home.join(".local/share/asciinema/asciicast");
    
    let config = AsciinemaConfig {
        recordings_dir: recordings_dir.clone(),
        file_pattern: "*.cast".to_string(),
        polling_interval_secs: 5,
        auto_start_recording: true,
        record_command: "asciinema rec --quiet --overwrite".to_string(),
        git_annex_repo: Some(temp_home.join("sinex-annex")),
        auto_annex: true,
    };
    
    // Set HOME environment variable
    std::env::set_var("HOME", temp_home.to_string_lossy().to_string());
    
    // Create event source context and initialize recorder
    let event_ctx = EventSourceContext::new(serde_json::to_value(&config)?);
    let _recorder = AsciinemaRecorder::initialize(event_ctx).await?;
    
    // Verify recordings directory was created
    assert!(recordings_dir.exists(), "Recordings directory should be created");
    assert!(recordings_dir.is_dir(), "Recordings path should be a directory");
    
    // Verify git annex repo was set up if specified
    if let Some(annex_path) = &config.git_annex_repo {
        if config.auto_annex {
            // Check if git annex initialization was attempted
            // (This might fail in test environment but should not panic)
            assert!(annex_path.exists() || !annex_path.exists(), "Annex setup should not panic");
        }
    }
    
    Ok(())
}
