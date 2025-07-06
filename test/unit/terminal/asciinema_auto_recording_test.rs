use crate::common::prelude::*;
use sinex_events_terminal::asciinema::{AsciinemaConfig, AsciinemaRecorder};
use sinex_core::{EventSource, EventSourceContext};
use std::path::PathBuf;
use tempfile::TempDir;

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
    
    // Verify recordings directory was created
    assert!(config.recordings_dir.exists());
    
    Ok(())
}

#[sinex_test]
async fn test_asciinema_auto_recording_disabled(_ctx: TestContext) -> TestResult {
    // Create temporary directory for test
    let temp_dir = TempDir::new()?;
    let temp_home = temp_dir.path().to_path_buf();
    
    // Create asciinema config with auto-recording DISABLED
    let config = AsciinemaConfig {
        recordings_dir: temp_home.join(".local/share/asciinema/asciicast"),
        auto_start_recording: false, // Disabled
        ..Default::default()
    };
    
    // Set HOME environment variable for the test
    std::env::set_var("HOME", temp_home.to_string_lossy().to_string());
    
    // Create event source context
    let event_ctx = EventSourceContext::new(serde_json::to_value(&config)?);
    
    // Initialize the asciinema recorder
    let _recorder = AsciinemaRecorder::initialize(event_ctx).await?;
    
    // Verify that shell integration files were NOT modified when auto-recording is disabled
    let bashrc_path = temp_home.join(".bashrc");
    let zshrc_path = temp_home.join(".zshrc");
    
    // These files should either not exist or not contain sinex integration
    if bashrc_path.exists() {
        let bashrc_content = tokio::fs::read_to_string(&bashrc_path).await?;
        assert!(!bashrc_content.contains("# SINEX AUTO-RECORDING"));
    }
    
    if zshrc_path.exists() {
        let zshrc_content = tokio::fs::read_to_string(&zshrc_path).await?;
        assert!(!zshrc_content.contains("# SINEX AUTO-RECORDING"));
    }
    
    Ok(())
}

#[sinex_test]
async fn test_shell_integration_idempotent(_ctx: TestContext) -> TestResult {
    // Create temporary directory for test
    let temp_dir = TempDir::new()?;
    let temp_home = temp_dir.path().to_path_buf();
    
    // Create a .bashrc file
    let bashrc_path = temp_home.join(".bashrc");
    tokio::fs::write(&bashrc_path, "# Original bashrc content\nexport PATH=/usr/bin:$PATH\n").await?;
    
    let config = AsciinemaConfig {
        recordings_dir: temp_home.join(".local/share/asciinema/asciicast"),
        auto_start_recording: true,
        ..Default::default()
    };
    
    std::env::set_var("HOME", temp_home.to_string_lossy().to_string());
    
    // Initialize recorder twice to test idempotency
    let event_ctx1 = EventSourceContext::new(serde_json::to_value(&config)?);
    let _recorder1 = AsciinemaRecorder::initialize(event_ctx1).await?;
    
    let event_ctx2 = EventSourceContext::new(serde_json::to_value(&config)?);
    let _recorder2 = AsciinemaRecorder::initialize(event_ctx2).await?;
    
    // Check that integration was only added once
    let bashrc_content = tokio::fs::read_to_string(&bashrc_path).await?;
    println!("Bashrc content: {}", bashrc_content);
    let integration_count = bashrc_content.matches("# SINEX AUTO-RECORDING").count();
    assert_eq!(integration_count, 1, "Shell integration should only be added once. Content: {}", bashrc_content);
    
    // Verify original content is preserved
    assert!(bashrc_content.contains("# Original bashrc content"));
    assert!(bashrc_content.contains("export PATH=/usr/bin:$PATH"));
    
    Ok(())
}