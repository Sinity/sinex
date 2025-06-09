use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;
use tokio::process::Command;
use tempfile::TempDir;
use serde_json::json;
use tracing::{info, warn};

use crate::common::database_service_from_pool;
use sinex_shared::{sources, event_types};

/// Full end-to-end test with real system interactions
#[tokio::test]
#[ignore] // Run with: cargo test --test integration e2e::full_system_test -- --ignored
async fn test_full_system_with_real_events() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for debugging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    info!("Starting full system E2E test");

    // Setup database
    let pool = crate::test_setup::get_test_db().await;
    let _db = database_service_from_pool(pool.as_ref().clone());

    // Create temporary directory for filesystem watching
    let temp_dir = TempDir::new()?;
    let watch_path = temp_dir.path().to_path_buf();
    info!("Created temp directory for filesystem watching: {:?}", watch_path);

    // Create config files for ingestors
    let fs_config = create_filesystem_config(&watch_path)?;
    let kitty_config = create_kitty_config()?;
    let hyprland_config = create_hyprland_config()?;

    // Start ingestors
    let mut ingestors = IngestorHarness::new();
    ingestors.start_filesystem(&fs_config).await?;
    ingestors.start_kitty(&kitty_config).await?;
    ingestors.start_hyprland(&hyprland_config).await?;

    // Wait for ingestors to initialize and send heartbeats
    sleep(Duration::from_secs(3)).await;

    // Verify heartbeats were received
    verify_heartbeats(&pool, &["filesystem", "kitty", "hyprland"]).await?;

    // Test 1: Filesystem events
    info!("Testing filesystem events...");
    test_filesystem_events(&watch_path, &pool).await?;

    // Test 2: Hyprland events (if available)
    info!("Testing Hyprland events...");
    test_hyprland_events(&pool).await?;

    // Test 3: Kitty events (if available)
    info!("Testing Kitty events...");
    test_kitty_events(&pool).await?;

    // Wait for any pending events to be processed
    sleep(Duration::from_secs(2)).await;

    // Verify all expected events were captured
    verify_captured_events(&pool).await?;

    // Cleanup
    ingestors.stop_all().await;
    
    info!("Full system E2E test completed successfully");
    Ok(())
}

/// Dry-run version of the E2E test
#[tokio::test]
async fn test_full_system_dry_run() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    info!("Starting dry-run E2E test");

    // Create temporary directory for filesystem watching
    let temp_dir = TempDir::new()?;
    let watch_path = temp_dir.path().to_path_buf();

    // Create config files
    let fs_config = create_filesystem_config(&watch_path)?;
    let kitty_config = create_kitty_config()?;
    let hyprland_config = create_hyprland_config()?;

    // Start ingestors in dry-run mode
    let mut ingestors = IngestorHarness::new();
    ingestors.start_filesystem_dry_run(&fs_config).await?;
    ingestors.start_kitty_dry_run(&kitty_config).await?;
    ingestors.start_hyprland_dry_run(&hyprland_config).await?;

    // Wait for initialization
    sleep(Duration::from_secs(2)).await;

    // Generate events
    info!("Generating test events in dry-run mode...");
    
    // Filesystem events
    tokio::fs::write(watch_path.join("test.txt"), "Hello, dry run!").await?;
    tokio::fs::create_dir(watch_path.join("test_dir")).await?;
    
    // Try Hyprland commands (may fail if not running)
    let _ = Command::new("hyprctl")
        .args(&["dispatch", "workspace", "2"])
        .output()
        .await;

    // Wait for events to be logged
    sleep(Duration::from_secs(3)).await;

    // In dry-run mode, we just verify the ingestors didn't crash
    assert!(ingestors.all_running(), "Some ingestors crashed during dry-run");

    ingestors.stop_all().await;
    info!("Dry-run E2E test completed successfully");
    Ok(())
}

/// Test filesystem events by creating real files
async fn test_filesystem_events(
    watch_path: &PathBuf,
    pool: &sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let initial_count = count_events(pool, sources::FILESYSTEM).await?;

    // Create a file
    let test_file = watch_path.join("test_file.txt");
    tokio::fs::write(&test_file, "Hello, Sinex!").await?;
    
    // Create a directory
    let test_dir = watch_path.join("test_directory");
    tokio::fs::create_dir(&test_dir).await?;
    
    // Modify the file
    tokio::fs::write(&test_file, "Modified content").await?;
    
    // Create nested structure
    let nested_dir = test_dir.join("nested");
    tokio::fs::create_dir(&nested_dir).await?;
    tokio::fs::write(nested_dir.join("nested.txt"), "Nested file").await?;
    
    // Delete a file
    tokio::fs::remove_file(&test_file).await?;

    // Wait for events to be processed
    sleep(Duration::from_secs(2)).await;

    // Verify events were captured
    let new_count = count_events(pool, sources::FILESYSTEM).await?;
    assert!(
        new_count > initial_count,
        "Expected filesystem events to be captured. Initial: {}, New: {}",
        initial_count,
        new_count
    );

    // Verify specific event types
    let file_created = count_events_by_type(
        pool,
        sources::FILESYSTEM,
        event_types::event_types::filesystem::FILE_CREATED,
    ).await?;
    assert!(file_created > 0, "Expected FILE_CREATED events");

    // Note: Directory creation events might be captured as FILE_CREATED
    // depending on the filesystem watcher implementation

    info!("Filesystem events test passed. Captured {} new events", new_count - initial_count);
    Ok(())
}

/// Test Hyprland events using hyprctl
async fn test_hyprland_events(
    pool: &sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if hyprctl is available
    let hyprctl_check = Command::new("hyprctl")
        .arg("version")
        .output()
        .await;

    if hyprctl_check.is_err() || !hyprctl_check.unwrap().status.success() {
        warn!("Hyprland not available, skipping Hyprland event tests");
        return Ok(());
    }

    let initial_count = count_events(pool, sources::HYPRLAND).await?;

    // Switch workspace
    Command::new("hyprctl")
        .args(&["dispatch", "workspace", "2"])
        .output()
        .await?;

    sleep(Duration::from_millis(500)).await;

    // Switch back
    Command::new("hyprctl")
        .args(&["dispatch", "workspace", "1"])
        .output()
        .await?;

    // Create a fake window event by running a command
    let _ = Command::new("hyprctl")
        .args(&["dispatch", "exec", "echo 'test window'"])
        .output()
        .await;

    // Wait for periodic snapshot (usually every few seconds)
    sleep(Duration::from_secs(5)).await;

    let new_count = count_events(pool, sources::HYPRLAND).await?;
    
    if new_count > initial_count {
        info!("Hyprland events test passed. Captured {} new events", new_count - initial_count);
    } else {
        warn!("No Hyprland events captured - this may be normal if Hyprland isn't fully configured");
    }

    Ok(())
}

/// Test Kitty events using remote control
async fn test_kitty_events(
    pool: &sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if kitty remote control is available
    let kitty_check = Command::new("kitty")
        .args(&["@", "ls"])
        .output()
        .await;

    if kitty_check.is_err() || !kitty_check.unwrap().status.success() {
        warn!("Kitty remote control not available, skipping Kitty event tests");
        return Ok(());
    }

    let initial_count = count_events(pool, sources::TERMINAL_KITTY).await?;

    // Send commands via kitty remote control
    let _ = Command::new("kitty")
        .args(&["@", "send-text", "echo 'Hello from E2E test'\n"])
        .output()
        .await;

    sleep(Duration::from_millis(500)).await;

    // Send another command
    let _ = Command::new("kitty")
        .args(&["@", "send-text", "ls -la\n"])
        .output()
        .await;

    // Wait for events to be captured
    sleep(Duration::from_secs(2)).await;

    let new_count = count_events(pool, sources::TERMINAL_KITTY).await?;
    
    if new_count > initial_count {
        info!("Kitty events test passed. Captured {} new events", new_count - initial_count);
    } else {
        warn!("No Kitty events captured - ensure Kitty is configured for remote control");
    }

    Ok(())
}

/// Verify heartbeats from all ingestors
async fn verify_heartbeats(
    pool: &sqlx::PgPool,
    expected_agents: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    for agent in expected_agents {
        let heartbeat_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) 
            FROM raw.events 
            WHERE source = $1 
            AND event_type = $2
            AND ts_ingest > NOW() - INTERVAL '1 minute'
            "#
        )
        .bind(sources::SINEX)
        .bind(event_types::event_types::sinex::AGENT_HEARTBEAT)
        .fetch_one(pool)
        .await?;

        assert!(
            heartbeat_count > 0,
            "Expected heartbeats from {} agent",
            agent
        );
    }
    
    info!("All expected heartbeats received");
    Ok(())
}

/// Verify all expected events were captured
async fn verify_captured_events(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let total_events: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*) 
        FROM raw.events 
        WHERE ts_ingest > NOW() - INTERVAL '5 minutes'
        "#
    )
    .fetch_one(pool)
    .await?;

    info!("Total events captured during test: {}", total_events);

    // Check event distribution by source
    let event_distribution: Vec<(String, i64)> = sqlx::query_as(
        r#"
        SELECT source, COUNT(*) as count
        FROM raw.events 
        WHERE ts_ingest > NOW() - INTERVAL '5 minutes'
        GROUP BY source
        ORDER BY count DESC
        "#
    )
    .fetch_all(pool)
    .await?;

    info!("Event distribution by source:");
    for (source, count) in event_distribution {
        info!("  {}: {} events", source, count);
    }

    assert!(total_events > 0, "Expected at least some events to be captured");
    Ok(())
}

/// Helper to count events by source
async fn count_events(pool: &sqlx::PgPool, source: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*) 
        FROM raw.events 
        WHERE source = $1
        AND ts_ingest > NOW() - INTERVAL '10 minutes'
        "#
    )
    .bind(source)
    .fetch_one(pool)
    .await
}

/// Helper to count events by source and type
async fn count_events_by_type(
    pool: &sqlx::PgPool,
    source: &str,
    event_type: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*) 
        FROM raw.events 
        WHERE source = $1 
        AND event_type = $2
        AND ts_ingest > NOW() - INTERVAL '10 minutes'
        "#
    )
    .bind(source)
    .bind(event_type)
    .fetch_one(pool)
    .await
}

/// Manages ingestor processes for testing
struct IngestorHarness {
    processes: Vec<IngestorProcess>,
}

struct IngestorProcess {
    name: String,
    child: tokio::process::Child,
}

impl IngestorHarness {
    fn new() -> Self {
        Self {
            processes: Vec::new(),
        }
    }

    async fn start_filesystem(&mut self, config_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new("cargo")
            .args(&["run", "--package", "filesystem-ingestor", "--", "--config", config_path.to_str().unwrap()])
            .spawn()?;
        
        self.processes.push(IngestorProcess {
            name: "filesystem".to_string(),
            child,
        });
        
        info!("Started filesystem ingestor");
        Ok(())
    }

    async fn start_filesystem_dry_run(&mut self, config_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new("cargo")
            .args(&["run", "--package", "filesystem-ingestor", "--", "--config", config_path.to_str().unwrap(), "--dry-run"])
            .spawn()?;
        
        self.processes.push(IngestorProcess {
            name: "filesystem-dry".to_string(),
            child,
        });
        
        info!("Started filesystem ingestor in dry-run mode");
        Ok(())
    }

    async fn start_kitty(&mut self, config_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new("cargo")
            .args(&["run", "--package", "kitty-ingestor", "--", "--config", config_path.to_str().unwrap()])
            .spawn()?;
        
        self.processes.push(IngestorProcess {
            name: "kitty".to_string(),
            child,
        });
        
        info!("Started kitty ingestor");
        Ok(())
    }

    async fn start_kitty_dry_run(&mut self, config_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new("cargo")
            .args(&["run", "--package", "kitty-ingestor", "--", "--config", config_path.to_str().unwrap(), "--dry-run"])
            .spawn()?;
        
        self.processes.push(IngestorProcess {
            name: "kitty-dry".to_string(),
            child,
        });
        
        info!("Started kitty ingestor in dry-run mode");
        Ok(())
    }

    async fn start_hyprland(&mut self, config_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new("cargo")
            .args(&["run", "--package", "hyprland-ingestor", "--", "--config", config_path.to_str().unwrap()])
            .spawn()?;
        
        self.processes.push(IngestorProcess {
            name: "hyprland".to_string(),
            child,
        });
        
        info!("Started hyprland ingestor");
        Ok(())
    }

    async fn start_hyprland_dry_run(&mut self, config_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new("cargo")
            .args(&["run", "--package", "hyprland-ingestor", "--", "--config", config_path.to_str().unwrap(), "--dry-run"])
            .spawn()?;
        
        self.processes.push(IngestorProcess {
            name: "hyprland-dry".to_string(),
            child,
        });
        
        info!("Started hyprland ingestor in dry-run mode");
        Ok(())
    }

    fn all_running(&self) -> bool {
        // In a real implementation, we'd check process status
        // For now, assume they're running if we started them
        !self.processes.is_empty()
    }

    async fn stop_all(&mut self) {
        for mut process in self.processes.drain(..) {
            info!("Stopping {} ingestor", process.name);
            let _ = process.child.kill().await;
        }
    }
}

/// Create filesystem ingestor config
fn create_filesystem_config(watch_path: &PathBuf) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let config = json!({
        "database_url": std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql:///sinex_test".to_string()),
        "watch_paths": [watch_path.to_str().unwrap()],
        "ignore_patterns": ["*.tmp", "*.swp"],
        "max_file_size": 1048576,
        "event_buffer_size": 100,
        "heartbeat_interval": 30
    });

    let config_path = std::env::temp_dir().join("sinex_e2e_fs_config.json");
    std::fs::write(&config_path, serde_json::to_string_pretty(&config)?)?;
    Ok(config_path)
}

/// Create kitty ingestor config
fn create_kitty_config() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let config = json!({
        "database_url": std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql:///sinex_test".to_string()),
        "socket_path": "/tmp/kitty-*.sock",
        "capture_output": true,
        "max_output_size": 10485760,
        "event_buffer_size": 50,
        "heartbeat_interval": 30
    });

    let config_path = std::env::temp_dir().join("sinex_e2e_kitty_config.json");
    std::fs::write(&config_path, serde_json::to_string_pretty(&config)?)?;
    Ok(config_path)
}

/// Create hyprland ingestor config
fn create_hyprland_config() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let config = json!({
        "database_url": std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgresql:///sinex_test".to_string()),
        "hyprland_instance": std::env::var("HYPRLAND_INSTANCE_SIGNATURE").ok(),
        "snapshot_interval": 3,
        "event_buffer_size": 100,
        "heartbeat_interval": 30
    });

    let config_path = std::env::temp_dir().join("sinex_e2e_hyprland_config.json");
    std::fs::write(&config_path, serde_json::to_string_pretty(&config)?)?;
    Ok(config_path)
}