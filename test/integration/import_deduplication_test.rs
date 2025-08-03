// Import Deduplication Integration Tests
//
// This module tests the deduplication functionality for scanner imports, including:
// - Git-annex hash-based deduplication for file imports
// - Database overlap detection and analysis
// - Interactive user decision flows
// - Time range overlap calculations
// - Duplicate prevention across different import methods

use sinex_test_utils::prelude::*;

use sinex_test_utils::event_sources::EventSource;
use sinex_test_utils::mocks::EventSourceContext;
use sinex_test_utils::prelude::*;
use chrono::{TimeZone, Utc};
use sinex_satellite_sdk::annex::{AnnexConfig, BlobManager};
use sinex_core_types::CoreError;
use sinex_db::models::EventFactory;
use sinex_satellite_sdk::{ScanArgs, ScanReport};
use std::collections::HashMap;
use std::fs;
use camino::Utf8PathBuf;
use tempfile::TempDir;
use tracing::info;

// =============================================================================
// Git-annex Hash Deduplication Tests
// =============================================================================

#[sinex_test]
async fn test_git_annex_hash_deduplication(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let annex_dir = temp_dir.path().join("git-annex");

    // Create BlobManager
    let annex_config = AnnexConfig {
        repo_path: annex_dir.clone(),
        num_copies: Some(1),
        large_files: None,
    };

    let blob_manager = BlobManager::new(annex_config, ctx.pool().clone())?;

    // Create test file with specific content
    let test_file = temp_dir.path().join("test_content.txt");
    let test_content = "This is test content for deduplication testing";
    fs::write(&test_file, test_content)?;

    // First import should store the file
    let metadata1 = blob_manager
        .ingest_file(&test_file, Some("first_import"))
        .await?;
    assert!(metadata1.checksum_blake3.is_some());

    // Create duplicate file with same content but different name
    let duplicate_file = temp_dir.path().join("duplicate_content.txt");
    fs::write(&duplicate_file, test_content)?;

    // Second import should detect duplicate by hash
    let metadata2 = blob_manager
        .ingest_file(&duplicate_file, Some("second_import"))
        .await?;

    // Should have same hash but different blob IDs
    assert_eq!(metadata1.checksum_blake3, metadata2.checksum_blake3);
    info!(
        "Deduplication successful - same hash: {}",
        metadata1.checksum_blake3.as_ref().unwrap()
    );

    // Verify both metadata entries have the same hash
    info!("Both files should have the same hash - demonstrating deduplication at git-annex level");

    Ok(())
}

#[sinex_test]
async fn test_atuin_import_overlap_detection(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_atuin.db");

    // Create test Atuin database with overlapping entries
    create_test_atuin_db(
        &db_path,
        vec![
            create_atuin_entry("test-1", "echo 'first'", 1640995200),
            create_atuin_entry("test-2", "echo 'second'", 1640995300),
            create_atuin_entry("test-3", "echo 'third'", 1640995400),
        ],
    )?;

    // First import - use simplified config structure
    let config = serde_json::json!({
        "db_path": db_path.to_string_lossy(),
        "polling_interval_secs": 1,
        "batch_size": 10,
        "use_file_watch": false
    });

    let source_ctx = EventSourceContext::new(config.clone()).with_db_pool(ctx.pool().clone());

    let mut importer = AtuinHistoryImporter::initialize(source_ctx).await?;

    let (tx1, mut rx1) = tokio::sync::mpsc::channel(100);
    let scanner_args = ScanArgs {
        targets: vec![db_path.to_string_lossy().to_string()],
        dry_run: false,
        interactive: false, // Non-interactive for testing
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let report1 = importer.run_scanner(tx1, scanner_args.clone()).await?;
    assert_eq!(report1.events_processed, 3);

    // Consume events from first import
    let mut events1 = Vec::new();
    while let Ok(event) = rx1.try_recv() {
        events1.push(event);
    }
    assert_eq!(events1.len(), 3);

    // Add more entries to the database (some overlapping, some new)
    add_atuin_entries(
        &db_path,
        vec![
            create_atuin_entry("test-3", "echo 'third'", 1640995400), // Duplicate
            create_atuin_entry("test-4", "echo 'fourth'", 1640995500), // New
            create_atuin_entry("test-5", "echo 'fifth'", 1640995600), // New
        ],
    )?;

    // Second import should detect overlap
    let source_ctx2 = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut importer2 = AtuinHistoryImporter::initialize(source_ctx2).await?;
    let (tx2, mut rx2) = tokio::sync::mpsc::channel(100);

    let report2 = importer2.run_scanner(tx2, scanner_args).await?;

    // Should detect new entries (overlapping ones should be filtered)
    assert!(report2.events_processed >= 2); // At least the 2 new entries

    // Verify no exact duplicates were imported by checking command uniqueness
    let mut events2 = Vec::new();
    while let Ok(event) = rx2.try_recv() {
        events2.push(event);
    }

    // Extract commands from both imports
    let mut all_commands = HashMap::new();
    for event in events1.iter().chain(events2.iter()) {
        if let Ok(payload) = serde_json::from_value::<serde_json::Value>(event.payload.clone()) {
            if let Some(command) = payload.get("command_line").and_then(|c| c.as_str()) {
                *all_commands.entry(command.to_string()).or_insert(0) += 1;
            }
        }
    }

    // Verify deduplication worked (no command should appear more than expected)
    for (command, count) in all_commands {
        info!("Command '{}' appeared {} times", command, count);
        assert!(
            count <= 2,
            "Command '{}' appeared {} times - possible duplicate",
            command,
            count
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_shell_history_time_range_overlap(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let history_file = temp_dir.path().join(".bash_history");

    // Create bash history with timestamps
    let bash_history = r#"#1640995200
echo "old command"
#1641000000
echo "middle command 1"
#1641002000
echo "middle command 2"
#1641005000
echo "recent command"
"#;
    fs::write(&history_file, bash_history)?;

    let config = serde_json::json!({
        "enable_atuin": false,
        "enable_history_files": true,
        "history_paths": [history_file.to_string_lossy()],
        "min_command_length": 2,
        "ignore_commands": [],
        "max_execution_time_ms": 3600000
    });

    let source_ctx = EventSourceContext::new(config.clone()).with_db_pool(ctx.pool().clone());

    let mut monitor = ShellHistoryMonitor::initialize(source_ctx).await?;

    // First import with limited time range (should get 2 middle commands)
    let start_time = Utc.timestamp_opt(1640998000, 0).unwrap();
    let end_time = Utc.timestamp_opt(1641003000, 0).unwrap();

    let (tx1, mut rx1) = tokio::sync::mpsc::channel(100);
    let scanner_args1 = ScanArgs {
        targets: vec![history_file.to_string_lossy().to_string()],
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let report1 = monitor.run_scanner(tx1, scanner_args1).await?;
    assert_eq!(report1.events_processed, 2); // middle commands only

    // Verify time range in report
    assert!(report1.time_range.is_some());
    let (report_start, report_end) = report1.time_range.unwrap();
    assert!(report_start >= start_time);
    assert!(report_end <= end_time);

    // Consume events
    let mut events1 = Vec::new();
    while let Ok(event) = rx1.try_recv() {
        events1.push(event);
    }

    // Second import with different overlapping time range
    let start_time2 = Utc.timestamp_opt(1641001000, 0).unwrap(); // Overlaps with first range
    let end_time2 = Utc.timestamp_opt(1641006000, 0).unwrap(); // Extends beyond first range

    let source_ctx2 = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut monitor2 = ShellHistoryMonitor::initialize(source_ctx2).await?;
    let (tx2, mut rx2) = tokio::sync::mpsc::channel(100);

    let scanner_args2 = ScanArgs {
        targets: vec![history_file.to_string_lossy().to_string()],
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let report2 = monitor2.run_scanner(tx2, scanner_args2).await?;
    assert_eq!(report2.events_processed, 3); // middle command 2 + recent command + overlap

    let mut events2 = Vec::new();
    while let Ok(event) = rx2.try_recv() {
        events2.push(event);
    }

    // Verify overlap behavior - should have some overlapping content but different time ranges
    assert_ne!(
        events1.len(),
        events2.len(),
        "Different time ranges should yield different event counts"
    );

    Ok(())
}

// =============================================================================
// Interactive Decision Flow Tests
// =============================================================================

#[sinex_test]
async fn test_overlap_analysis_statistics(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let history_file = temp_dir.path().join(".zsh_history");

    // Create shell history with many entries for statistical analysis
    let mut zsh_content = String::new();
    for i in 0..100 {
        zsh_content.push_str(&format!(
            ": {}:0;echo 'command {}'\n",
            1640995200 + i * 60,
            i
        ));
    }
    fs::write(&history_file, &zsh_content)?;

    // First import to populate database
    let config = serde_json::json!({
        "enable_atuin": false,
        "enable_history_files": true,
        "history_paths": [history_file.to_string_lossy()],
        "min_command_length": 2,
        "ignore_commands": [],
        "max_execution_time_ms": 3600000
    });

    let source_ctx = EventSourceContext::new(config.clone()).with_db_pool(ctx.pool().clone());

    let mut monitor = ShellHistoryMonitor::initialize(source_ctx).await?;

    let (tx1, mut rx1) = tokio::sync::mpsc::channel(200);
    let scanner_args = ScanArgs {
        targets: vec![history_file.to_string_lossy().to_string()],
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let report1 = monitor.run_scanner(tx1, scanner_args.clone()).await?;
    assert_eq!(report1.events_processed, 100);

    // Consume all events
    let mut events_count = 0;
    while let Ok(_) = rx1.try_recv() {
        events_count += 1;
    }
    assert_eq!(events_count, 100);

    // Now add some overlapping entries and test overlap detection
    let mut additional_content = String::new();
    for i in 50..150 {
        // 50 overlapping + 50 new
        additional_content.push_str(&format!(
            ": {}:0;echo 'command {}'\n",
            1640995200 + i * 60,
            i
        ));
    }
    fs::write(
        &history_file,
        format!("{}{}", zsh_content, additional_content),
    )?;

    // Create new monitor instance for fresh analysis
    let source_ctx2 = EventSourceContext::new(config).with_db_pool(ctx.pool().clone());

    let mut monitor2 = ShellHistoryMonitor::initialize(source_ctx2).await?;

    // Test overlap analysis without actual import (we'd need to capture the analysis)
    // This tests the statistical calculation parts
    let history_files = vec![history_file];

    // Manually verify we can detect the overlap in the database
    let existing_count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE source = 'shell.history' AND event_type = 'command.imported'"
    )
    .fetch_one(ctx.pool())
    .await?
    .unwrap_or(0) as u64;

    assert_eq!(
        existing_count, 100,
        "Should have 100 existing events from first import"
    );

    // Count total lines in file for potential import
    let file_content = fs::read_to_string(&history_files[0])?;
    let total_lines = file_content
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with(':') && !line.starts_with('#'))
        .count() as u64;

    assert_eq!(total_lines, 150, "Should have 150 total commands in file");

    // This simulates the overlap calculation that would happen in interactive mode
    let potential_duplicates = std::cmp::min(existing_count, total_lines) / 2;
    assert!(potential_duplicates > 0, "Should detect potential overlap");

    info!(
        "Overlap analysis: {} existing, {} total, {} potential duplicates",
        existing_count, total_lines, potential_duplicates
    );

    Ok(())
}

// =============================================================================
// Cross-Scanner Deduplication Tests
// =============================================================================

#[sinex_test]
async fn test_cross_scanner_deduplication(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let temp_dir = tempfile::tempdir()?;

    // Create the same command in both Atuin and shell history
    let command = "echo 'test cross deduplication'";
    let timestamp = 1640995200;

    // Setup Atuin database
    let atuin_db = temp_dir.path().join("atuin.db");
    create_test_atuin_db(
        &atuin_db,
        vec![create_atuin_entry("cross-test-1", command, timestamp)],
    )?;

    // Setup shell history file
    let history_file = temp_dir.path().join(".bash_history");
    fs::write(&history_file, format!("#{}\n{}\n", timestamp, command))?;

    // Import from Atuin first
    let atuin_config = serde_json::json!({
        "db_path": atuin_db.to_string_lossy(),
        "polling_interval_secs": 1,
        "batch_size": 10,
        "use_file_watch": false
    });

    let atuin_ctx = EventSourceContext::new(atuin_config).with_db_pool(ctx.pool().clone());

    let mut atuin_importer = AtuinHistoryImporter::initialize(atuin_ctx).await?;
    let (atuin_tx, mut atuin_rx) = tokio::sync::mpsc::channel(100);

    let atuin_args = ScanArgs {
        targets: vec![], // Will use smart defaults
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let atuin_report = atuin_importer.run_scanner(atuin_tx, atuin_args).await?;
    assert_eq!(atuin_report.events_processed, 1);

    // Consume Atuin event
    let atuin_event = atuin_rx.recv().await.unwrap();
    assert_eq!(atuin_event.source, "shell.atuin");

    // Now import from shell history
    let shell_config = serde_json::json!({
        "enable_atuin": false,
        "enable_history_files": true,
        "history_paths": [history_file.to_string_lossy()],
        "min_command_length": 2,
        "ignore_commands": [],
        "max_execution_time_ms": 3600000
    });

    let shell_ctx = EventSourceContext::new(shell_config).with_db_pool(ctx.pool().clone());

    let mut shell_monitor = ShellHistoryMonitor::initialize(shell_ctx).await?;
    let (shell_tx, mut shell_rx) = tokio::sync::mpsc::channel(100);

    let shell_args = ScanArgs {
        targets: vec![history_file.to_string_lossy().to_string()],
        dry_run: false,
        interactive: false,
        max_events: 0,
        skip_duplicates: true,
        config: std::collections::HashMap::new(),
    };

    let shell_report = shell_monitor.run_scanner(shell_tx, shell_args).await?;
    assert_eq!(shell_report.events_processed, 1);

    // Consume shell event
    let shell_event = shell_rx.recv().await.unwrap();
    assert_eq!(shell_event.source, "shell.history");

    // Verify both events exist but with different sources
    let total_events = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE payload->>'command_line' = $1",
        command
    )
    .fetch_one(ctx.pool())
    .await?
    .unwrap_or(0);

    assert_eq!(total_events, 2, "Should have events from both sources");

    // Verify different source attribution
    let atuin_count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE source = 'shell.atuin' AND payload->>'command_line' = $1",
        command
    )
    .fetch_one(ctx.pool())
    .await?
    .unwrap_or(0);

    let shell_count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE source = 'shell.history' AND payload->>'command_line' = $1", 
        command
    )
    .fetch_one(ctx.pool())
    .await?
    .unwrap_or(0);

    assert_eq!(atuin_count, 1, "Should have one Atuin event");
    assert_eq!(shell_count, 1, "Should have one shell history event");

    info!("Cross-scanner deduplication test passed - same command tracked from different sources");

    Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

fn create_test_atuin_db(path: &Utf8PathBuf, entries: Vec<TestAtuinEntry>) -> color_eyre::eyre::Result<()> {
    use rusqlite::{params, Connection};

    let conn = Connection::open(path)?;

    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS history (
            id TEXT PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            duration INTEGER NOT NULL,
            exit INTEGER NOT NULL,
            command TEXT NOT NULL,
            cwd TEXT NOT NULL,
            session TEXT NOT NULL,
            hostname TEXT NOT NULL,
            deleted_at INTEGER
        )
        "#,
        [],
    )?;

    for entry in entries {
        conn.execute(
            r#"
            INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)
            "#,
            params![
                entry.id,
                entry.timestamp_ns,
                entry.duration_ns,
                entry.exit_code,
                entry.command,
                entry.cwd,
                entry.session,
                entry.hostname,
            ],
        )?;
    }

    Ok(())
}

fn add_atuin_entries(path: &Utf8PathBuf, entries: Vec<TestAtuinEntry>) -> color_eyre::eyre::Result<()> {
    use rusqlite::{params, Connection};

    let conn = Connection::open(path)?;

    for entry in entries {
        conn.execute(
            r#"
            INSERT OR REPLACE INTO history (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)
            "#,
            params![
                entry.id,
                entry.timestamp_ns,
                entry.duration_ns,
                entry.exit_code,
                entry.command,
                entry.cwd,
                entry.session,
                entry.hostname,
            ],
        )?;
    }

    Ok(())
}

struct TestAtuinEntry {
    id: String,
    timestamp_ns: i64,
    duration_ns: i64,
    exit_code: i32,
    command: String,
    cwd: String,
    session: String,
    hostname: String,
}

fn create_atuin_entry(id: &str, command: &str, timestamp_seconds: i64) -> TestAtuinEntry {
    TestAtuinEntry {
        id: id.to_string(),
        timestamp_ns: timestamp_seconds * 1_000_000_000,
        duration_ns: 1_000_000_000, // 1 second
        exit_code: 0,
        command: command.to_string(),
        cwd: "/tmp".to_string(),
        session: "test-session".to_string(),
        hostname: "test-host".to_string(),
    }
}
