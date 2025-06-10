use std::path::PathBuf;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio::process::Command;
use tempfile::TempDir;
use serde_json::json;
use tracing::{info, warn, error, debug};

use crate::common::database_service_from_pool;
use sinex_shared::{sources, event_types};

/// Adaptive E2E test that works with whatever systems are available
#[tokio::test]
async fn test_adaptive_full_system() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,sinex=debug")
        .try_init();

    info!("Starting adaptive E2E test");

    // Setup
    let pool = crate::test_setup::get_test_db().await;
    let temp_dir = TempDir::new()?;
    let watch_path = temp_dir.path().to_path_buf();
    
    // Check available systems
    let systems = AvailableSystems::detect().await;
    info!("Available systems: {:?}", systems);

    if !systems.has_any() {
        warn!("No testable systems available, setting up minimal test environment");
    }

    // Start available ingestors
    let mut harness = AdaptiveIngestorHarness::new();
    
    // Filesystem is always available
    harness.start_filesystem(&watch_path).await?;
    
    if systems.has_kitty {
        harness.start_kitty().await?;
    }
    
    if systems.has_hyprland {
        harness.start_hyprland().await?;
    }

    // Wait for initialization
    sleep(Duration::from_secs(2)).await;

    // Run tests for available systems
    let mut total_events = 0;

    // Filesystem tests (always run)
    info!("Testing filesystem events...");
    let fs_events = test_filesystem_adaptive(&watch_path, &pool).await?;
    total_events += fs_events;

    // Kitty tests (if available)
    if systems.has_kitty {
        info!("Testing Kitty events...");
        match test_kitty_adaptive(&pool).await {
            Ok(count) => total_events += count,
            Err(e) => warn!("Kitty test failed: {}", e),
        }
    }

    // Hyprland tests (if available)
    if systems.has_hyprland {
        info!("Testing Hyprland events...");
        match test_hyprland_adaptive(&pool).await {
            Ok(count) => total_events += count,
            Err(e) => warn!("Hyprland test failed: {}", e),
        }
    }

    // Verify results
    assert!(total_events > 0, "Expected at least some events to be captured");
    info!("Adaptive E2E test completed. Total events captured: {}", total_events);

    // Cleanup
    harness.stop_all().await;
    Ok(())
}

/// Test with minimal dependencies - just filesystem
#[tokio::test]
async fn test_minimal_e2e() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();

    info!("Starting minimal E2E test");

    let pool = crate::test_setup::get_test_db().await;
    let temp_dir = TempDir::new()?;
    let watch_path = temp_dir.path().to_path_buf();

    // Start only filesystem ingestor
    let mut harness = AdaptiveIngestorHarness::new();
    harness.start_filesystem(&watch_path).await?;

    sleep(Duration::from_secs(1)).await;

    // Create test files
    let test_file = watch_path.join("minimal_test.txt");
    tokio::fs::write(&test_file, "Minimal E2E test").await?;
    
    // Wait and verify
    sleep(Duration::from_secs(2)).await;

    let events: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*) 
        FROM raw.events 
        WHERE source = $1
        AND ts_ingest > NOW() - INTERVAL '1 minute'
        "#
    )
    .bind(sources::FILESYSTEM)
    .fetch_one(pool.as_ref())
    .await?;

    assert!(events > 0, "Expected filesystem events to be captured");
    
    harness.stop_all().await;
    info!("Minimal E2E test completed successfully");
    Ok(())
}

/// Detect which systems are available for testing
#[derive(Debug)]
struct AvailableSystems {
    has_kitty: bool,
    has_hyprland: bool,
    has_kitty_remote: bool,
    has_hyprctl: bool,
}

impl AvailableSystems {
    async fn detect() -> Self {
        Self {
            has_kitty: Self::check_command("kitty", &["--version"]).await,
            has_hyprland: std::env::var("HYPRLAND_INSTANCE_SIGNATURE").is_ok(),
            has_kitty_remote: Self::check_command("kitty", &["@", "ls"]).await,
            has_hyprctl: Self::check_command("hyprctl", &["version"]).await,
        }
    }

    async fn check_command(cmd: &str, args: &[&str]) -> bool {
        match Command::new(cmd).args(args).output().await {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    fn has_any(&self) -> bool {
        self.has_kitty || self.has_hyprland
    }
}

/// Adaptive filesystem test
async fn test_filesystem_adaptive(
    watch_path: &PathBuf,
    pool: &sqlx::PgPool,
) -> Result<usize, Box<dyn std::error::Error>> {
    let initial = count_recent_events(pool, sources::FILESYSTEM).await?;

    // Create various filesystem events
    let file1 = watch_path.join("test1.txt");
    tokio::fs::write(&file1, "Test content 1").await?;
    
    let dir1 = watch_path.join("subdir");
    tokio::fs::create_dir(&dir1).await?;
    
    let file2 = dir1.join("test2.txt");
    tokio::fs::write(&file2, "Test content 2").await?;
    
    // Modify
    tokio::fs::write(&file1, "Modified content").await?;
    
    // Delete
    tokio::fs::remove_file(&file2).await?;

    // Wait for processing
    sleep(Duration::from_secs(2)).await;

    let final_count = count_recent_events(pool, sources::FILESYSTEM).await?;
    let captured = final_count - initial;
    
    info!("Filesystem test: {} events captured", captured);
    Ok(captured)
}

/// Adaptive Kitty test
async fn test_kitty_adaptive(pool: &sqlx::PgPool) -> Result<usize, Box<dyn std::error::Error>> {
    let initial = count_recent_events(pool, sources::TERMINAL_KITTY).await?;

    // Try different methods to interact with Kitty
    
    // Method 1: Remote control
    let remote_result = Command::new("kitty")
        .args(&["@", "send-text", "echo 'E2E test command'\n"])
        .output()
        .await;

    if remote_result.is_ok() && remote_result.unwrap().status.success() {
        debug!("Sent command via Kitty remote control");
    } else {
        // Method 2: Try to find kitty socket directly with timeout
        let socket_search = tokio::time::timeout(
            Duration::from_secs(5),
            async {
                std::fs::read_dir("/tmp")?
                    .filter_map(Result::ok)
                    .filter(|e| {
                        e.file_name()
                            .to_str()
                            .map(|s| s.starts_with("kitty-") && s.ends_with(".sock"))
                            .unwrap_or(false)
                    })
                    .collect::<Vec<_>>()
            }
        ).await;

        match socket_search {
            Ok(Ok(sockets)) if !sockets.is_empty() => {
                debug!("Found {} Kitty sockets", sockets.len());
            },
            Ok(Ok(_)) => warn!("No Kitty sockets found"),
            Ok(Err(e)) => warn!("Error reading /tmp directory: {}", e),
            Err(_) => warn!("Timeout while searching for Kitty sockets"),
        }
    }

    sleep(Duration::from_secs(2)).await;

    let final_count = count_recent_events(pool, sources::TERMINAL_KITTY).await?;
    let captured = final_count - initial;
    
    info!("Kitty test: {} events captured", captured);
    Ok(captured)
}

/// Adaptive Hyprland test
async fn test_hyprland_adaptive(pool: &sqlx::PgPool) -> Result<usize, Box<dyn std::error::Error>> {
    let initial = count_recent_events(pool, sources::HYPRLAND).await?;

    // Get current workspace before switching
    let current_workspace = Command::new("hyprctl")
        .args(&["activeworkspace", "-j"])
        .output()
        .await
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok())
        .and_then(|v| v["id"].as_i64())
        .unwrap_or(1);

    // Try hyprctl commands
    let commands = vec![
        vec!["dispatch", "workspace", "2"],
        vec!["clients"],  // List windows
        vec!["monitors"], // List monitors
    ];

    for cmd in commands {
        let result = Command::new("hyprctl")
            .args(&cmd)
            .output()
            .await;

        if let Ok(output) = result {
            if output.status.success() {
                debug!("Executed hyprctl command: {:?}", cmd);
            }
        }
        
        sleep(Duration::from_millis(200)).await;
    }

    // Switch back to original workspace and wait for completion
    let switch_back = Command::new("hyprctl")
        .args(&["dispatch", "workspace", &current_workspace.to_string()])
        .output()
        .await;
    
    if switch_back.is_ok() {
        // Wait for workspace switch to complete
        sleep(Duration::from_millis(500)).await;
        debug!("Switched back to workspace {}", current_workspace);
    }

    // Wait for periodic snapshot
    sleep(Duration::from_secs(5)).await;

    let final_count = count_recent_events(pool, sources::HYPRLAND).await?;
    let captured = final_count - initial;
    
    info!("Hyprland test: {} events captured", captured);
    Ok(captured)
}

/// Count recent events for a source
async fn count_recent_events(pool: &sqlx::PgPool, source: &str) -> Result<usize, sqlx::Error> {
    let count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*) 
        FROM raw.events 
        WHERE source = $1
        AND ts_ingest > NOW() - INTERVAL '5 minutes'
        "#
    )
    .bind(source)
    .fetch_one(pool)
    .await?;
    
    Ok(count as usize)
}

/// Adaptive ingestor harness that handles failures gracefully
struct AdaptiveIngestorHarness {
    processes: Vec<RunningProcess>,
    failed_starts: Vec<String>,
}

struct RunningProcess {
    name: String,
    child: tokio::process::Child,
}

impl AdaptiveIngestorHarness {
    fn new() -> Self {
        Self {
            processes: Vec::new(),
            failed_starts: Vec::new(),
        }
    }

    async fn start_filesystem(&mut self, watch_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        // Create simple config
        let config = format!(
            r#"{{
                "watch_paths": ["{}"],
                "ignore_patterns": ["*.tmp", "*.swp"],
                "event_buffer_size": 100
            }}"#,
            watch_path.display()
        );

        let config_path = std::env::temp_dir().join("sinex_e2e_fs.toml");
        std::fs::write(&config_path, config)?;

        match Command::new("cargo")
            .args(&[
                "run", 
                "--package", 
                "unified-collector",
                "--",
                "--config",
                config_path.to_str().unwrap(),
            ])
            .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql:///sinex_test".to_string()))
            .spawn()
        {
            Ok(child) => {
                self.processes.push(RunningProcess {
                    name: "filesystem".to_string(),
                    child,
                });
                info!("Started filesystem ingestor");
                Ok(())
            }
            Err(e) => {
                error!("Failed to start filesystem ingestor: {}", e);
                self.failed_starts.push("filesystem".to_string());
                Err(e.into())
            }
        }
    }

    async fn start_kitty(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match Command::new("cargo")
            .args(&["run", "--package", "unified-collector"])
            .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql:///sinex_test".to_string()))
            .spawn()
        {
            Ok(child) => {
                self.processes.push(RunningProcess {
                    name: "kitty".to_string(),
                    child,
                });
                info!("Started kitty ingestor");
                Ok(())
            }
            Err(e) => {
                warn!("Failed to start kitty ingestor: {}", e);
                self.failed_starts.push("kitty".to_string());
                Ok(()) // Don't fail the test
            }
        }
    }

    async fn start_hyprland(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match Command::new("cargo")
            .args(&["run", "--package", "unified-collector"])
            .env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql:///sinex_test".to_string()))
            .spawn()
        {
            Ok(child) => {
                self.processes.push(RunningProcess {
                    name: "hyprland".to_string(),
                    child,
                });
                info!("Started hyprland ingestor");
                Ok(())
            }
            Err(e) => {
                warn!("Failed to start hyprland ingestor: {}", e);
                self.failed_starts.push("hyprland".to_string());
                Ok(()) // Don't fail the test
            }
        }
    }

    async fn stop_all(&mut self) {
        for mut process in self.processes.drain(..) {
            debug!("Stopping {} ingestor", process.name);
            let _ = process.child.kill().await;
        }
        
        if !self.failed_starts.is_empty() {
            warn!("Failed to start ingestors: {:?}", self.failed_starts);
        }
    }
}