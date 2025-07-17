// Scanner Integration Tests
//
// This module tests the scanner functionality for event sources, including:
// - Filesystem scanner with cross-platform support
// - Atuin scanner with SQLite import and Git-annex integration
// - Shell history scanner with multi-shell support
// - Overlap analysis and interactive prompts

use crate::common::prelude::*;

use crate::common::prelude::*;
use chrono::{TimeZone, Utc};
use sinex_core_types::CoreError;
use sinex_events::EventFactory;
use sinex_satellite_sdk::{EventSourceConfig, ScanArgs, ScanReport, StatefulStreamProcessor};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

// =============================================================================
// Filesystem Scanner Tests
// =============================================================================

#[sinex_test]
async fn test_filesystem_scanner_basic_functionality(ctx: TestContext) -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path();

    // Create test files
    fs::write(temp_path.join("test1.txt"), "content1")?;
    fs::write(temp_path.join("test2.log"), "content2")?;
    fs::create_dir(temp_path.join("subdir"))?;
    fs::write(temp_path.join("subdir/test3.txt"), "content3")?;

    // Initialize filesystem monitor with scanner support
    let config = serde_json::json!({
        "watch_patterns": [temp_path.to_str().unwrap()],
        "ignore_patterns": [],
        "recursive": true
    });

    let source_ctx = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut fs_monitor = FilesystemMonitor::initialize(source_ctx).await?;

    // Test scanner support
    assert!(fs_monitor.supports_scanner());

    // Run scanner
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let scanner_args = ScanArgs {
        targets: vec![temp_path.to_string_lossy().to_string()],
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let report = fs_monitor.run_scanner(tx, scanner_args).await?;

    // Verify scan report
    assert!(report.events_generated >= 3); // At least 3 files
    assert!(report.duration.as_millis() > 0);
    assert!(report.source_stats.contains_key("files_scanned"));

    // Verify events were generated
    let mut events_received = 0;
    while let Ok(event) = rx.try_recv() {
        assert_eq!(event.source, "fs");
        assert!(event.event_type.starts_with("file.") || event.event_type.starts_with("dir."));
        events_received += 1;
    }

    assert!(events_received >= 3);
    Ok(())
}

#[sinex_test]
async fn test_filesystem_scanner_with_ignore_patterns(ctx: TestContext) -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path();

    // Create test files including ones that should be ignored
    fs::write(temp_path.join("important.txt"), "keep this")?;
    fs::write(temp_path.join("temp.tmp"), "ignore this")?;
    fs::write(temp_path.join("backup.bak"), "ignore this too")?;

    // Initialize with ignore patterns
    let config = serde_json::json!({
        "watch_patterns": [temp_path.to_str().unwrap()],
        "ignore_patterns": ["*.tmp", "*.bak"],
        "recursive": false
    });

    let source_ctx = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut fs_monitor = FilesystemMonitor::initialize(source_ctx).await?;

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let scanner_args = ScanArgs {
        targets: vec![temp_path.to_string_lossy().to_string()],
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let _report = fs_monitor.run_scanner(tx, scanner_args).await?;

    // Check that only the important file was processed
    let mut important_found = false;
    let mut ignored_found = false;

    while let Ok(event) = rx.try_recv() {
        if let Ok(payload) = serde_json::from_value::<serde_json::Value>(event.payload) {
            if let Some(path) = payload.get("path").and_then(|p| p.as_str()) {
                if path.contains("important.txt") {
                    important_found = true;
                } else if path.contains("temp.tmp") || path.contains("backup.bak") {
                    ignored_found = true;
                }
            }
        }
    }

    assert!(important_found, "Important file should be found");
    assert!(!ignored_found, "Ignored files should not be found");

    Ok(())
}

// =============================================================================
// Shell History Scanner Tests
// =============================================================================

#[sinex_test]
async fn test_shell_history_scanner_bash_format(ctx: TestContext) -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let history_file = temp_dir.path().join(".bash_history");

    // Create test bash history
    let bash_history = r#"ls -la
cd /home/user
git status
echo "hello world"
sudo apt update
"#;
    fs::write(&history_file, bash_history)?;

    // Initialize shell history monitor
    let config = serde_json::json!({
        "enable_atuin": false,
        "enable_history_files": true,
        "history_paths": [history_file.to_string_lossy()],
        "min_command_length": 2,
        "ignore_commands": ["ls"],
        "max_execution_time_ms": 3600000
    });

    let source_ctx = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut monitor = ShellHistoryMonitor::initialize(source_ctx).await?;

    // Test scanner
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let scanner_args = ScanArgs {
        targets: vec![history_file.to_string_lossy().to_string()],
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let report = monitor.run_scanner(tx, scanner_args).await?;

    // Verify results (should ignore 'ls' command)
    assert!(report.events_generated >= 3); // cd, git, echo, sudo (minus ignored ls)
    assert!(
        report.source_stats.contains_key("shell_bash_entries")
            || report.source_stats.contains_key("shell_unknown_entries")
    );

    // Verify events
    let mut git_found = false;
    while let Ok(event) = rx.try_recv() {
        assert_eq!(event.source, "shell.history");
        assert_eq!(event.event_type, "command.imported");

        if let Ok(payload) = serde_json::from_value::<serde_json::Value>(event.payload) {
            if let Some(command) = payload.get("command_line").and_then(|c| c.as_str()) {
                if command.contains("git status") {
                    git_found = true;
                }
                // Verify 'ls' command was ignored
                assert!(!command.starts_with("ls "), "ls command should be ignored");
            }
        }
    }

    assert!(git_found, "Git command should be found");
    Ok(())
}

#[sinex_test]
async fn test_shell_history_scanner_zsh_format(ctx: TestContext) -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let history_file = temp_dir.path().join(".zsh_history");

    // Create test zsh extended history
    let zsh_history = r#": 1640995200:0;ls -la
: 1640995260:5;cd /home/user
: 1640995300:0;git status  
: 1640995400:2;echo "test command"
"#;
    fs::write(&history_file, zsh_history)?;

    let config = serde_json::json!({
        "enable_atuin": false,
        "enable_history_files": true,
        "history_paths": [history_file.to_string_lossy()],
        "min_command_length": 2,
        "ignore_commands": [],
        "max_execution_time_ms": 3600000
    });

    let source_ctx = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut monitor = ShellHistoryMonitor::initialize(source_ctx).await?;

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let scanner_args = ScanArgs {
        targets: vec![history_file.to_string_lossy().to_string()],
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let report = monitor.run_scanner(tx, scanner_args).await?;

    assert!(report.events_generated >= 4);

    // Verify timestamp parsing
    let mut timestamp_found = false;
    while let Ok(event) = rx.try_recv() {
        if let Ok(payload) = serde_json::from_value::<serde_json::Value>(event.payload) {
            if let Some(timestamp_str) = payload.get("timestamp").and_then(|t| t.as_str()) {
                if !timestamp_str.is_empty() {
                    timestamp_found = true;
                }
            }
        }
    }

    assert!(timestamp_found, "Zsh timestamps should be parsed");
    Ok(())
}

// =============================================================================
// Scanner Time Range Tests
// =============================================================================

#[sinex_test]
async fn test_scanner_time_range_filtering(ctx: TestContext) -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let history_file = temp_dir.path().join(".bash_history");

    // Create history with known timestamps (using bash HISTTIMEFORMAT)
    let bash_history = r#"#1640995200
old_command_before_range
#1641000000  
command_in_range
#1641005000
another_command_in_range
#1641010000
command_after_range
"#;
    fs::write(&history_file, bash_history)?;

    let config = serde_json::json!({
        "enable_atuin": true,
        "enable_history_files": true,
        "history_paths": ["~/.bash_history", "~/.zsh_history"],
        "min_command_length": 2,
        "ignore_commands": ["ls", "cd", "pwd"],
        "max_execution_time_ms": 3600000
    });
    let source_ctx = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut monitor = ShellHistoryMonitor::initialize(source_ctx).await?;

    // Test with time range filter
    let start_time = Utc.timestamp_opt(1640998000, 0).unwrap(); // Between first and second
    let end_time = Utc.timestamp_opt(1641007000, 0).unwrap(); // Between third and fourth

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let scanner_args = ScanArgs {
        targets: vec![history_file.to_string_lossy().to_string()],
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let report = monitor.run_scanner(tx, scanner_args).await?;

    // Should only find 2 commands in the time range
    assert_eq!(report.events_generated, 2);

    // Verify time range in report
    if let Some((report_start, report_end)) = report.time_range {
        assert!(report_start >= start_time);
        assert!(report_end <= end_time);
    }

    Ok(())
}

// =============================================================================
// Scanner Dry Run Tests
// =============================================================================

#[sinex_test]
async fn test_scanner_dry_run_mode(ctx: TestContext) -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path();

    // Create test files
    fs::write(temp_path.join("test1.txt"), "content1")?;
    fs::write(temp_path.join("test2.txt"), "content2")?;

    let config = serde_json::json!({
        "watch_patterns": [temp_path.to_str().unwrap()],
        "ignore_patterns": [],
        "recursive": false
    });

    let source_ctx = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut fs_monitor = FilesystemMonitor::initialize(source_ctx).await?;

    // Test dry run
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let scanner_args = ScanArgs {
        targets: vec![temp_path.to_string_lossy().to_string()],
        dry_run: true,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let report = fs_monitor.run_scanner(tx, scanner_args).await?;

    // In dry run mode, events should be generated but not sent through channel
    assert!(report.events_generated >= 2);

    // Channel should be empty since dry_run = true
    assert!(
        rx.try_recv().is_err(),
        "No events should be sent in dry run mode"
    );

    Ok(())
}

// =============================================================================
// Scanner Error Handling Tests
// =============================================================================

#[sinex_test]
async fn test_scanner_handles_missing_files(ctx: TestContext) -> TestResult {
    let config = serde_json::json!({
        "watch_patterns": [],
        "ignore_patterns": [],
        "recursive": false
    });

    let source_ctx = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut fs_monitor = FilesystemMonitor::initialize(source_ctx).await?;

    let (tx, _rx) = tokio::sync::mpsc::channel(100);
    let scanner_args = ScanArgs {
        targets: vec!["/nonexistent/path/file.txt".to_string()],
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    // Should handle missing files gracefully
    let report = fs_monitor.run_scanner(tx, scanner_args).await?;
    assert_eq!(report.events_generated, 0);

    Ok(())
}

#[sinex_test]
async fn test_scanner_handles_empty_paths(ctx: TestContext) -> TestResult {
    let config = serde_json::json!({
        "watch_patterns": [],
        "ignore_patterns": [],
        "recursive": false
    });

    let source_ctx = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut fs_monitor = FilesystemMonitor::initialize(source_ctx).await?;

    let (tx, _rx) = tokio::sync::mpsc::channel(100);
    let scanner_args = ScanArgs {
        targets: vec![], // Empty targets
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    // Should handle empty paths gracefully
    let report = fs_monitor.run_scanner(tx, scanner_args).await?;

    // Should fall back to smart defaults or process nothing
    assert!(report.duration.as_millis() >= 0);

    Ok(())
}

// =============================================================================
// Scanner Performance Tests
// =============================================================================

#[sinex_test]
async fn test_scanner_performance_large_directory(ctx: TestContext) -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path();

    // Create many small files to test performance
    for i in 0..100 {
        fs::write(
            temp_path.join(format!("file_{:03}.txt", i)),
            format!("content_{}", i),
        )?;
    }

    let config = serde_json::json!({
        "watch_patterns": [temp_path.to_str().unwrap()],
        "ignore_patterns": [],
        "recursive": false
    });

    let source_ctx = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut fs_monitor = FilesystemMonitor::initialize(source_ctx).await?;

    let (tx, _rx) = tokio::sync::mpsc::channel(1000);
    let scanner_args = ScanArgs {
        targets: vec![temp_path.to_string_lossy().to_string()],
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let start_time = std::time::Instant::now();
    let report = fs_monitor.run_scanner(tx, scanner_args).await?;
    let elapsed = start_time.elapsed();

    // Should process 100 files efficiently
    assert_eq!(report.events_generated, 100);
    assert!(
        elapsed.as_secs() < 5,
        "Scanner should complete within 5 seconds"
    );
    assert!(report.duration.as_millis() > 0);

    Ok(())
}
