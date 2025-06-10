use anyhow::Result;
use sinex_core::{event_type_constants, sources};
use sinex_db::create_pool;
use sqlx::PgPool;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::process::Command as TokioCommand;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

struct IngestorProcess {
    name: &'static str,
    child: Child,
    #[allow(dead_code)]
    started: Instant,
}

impl Drop for IngestorProcess {
    fn drop(&mut self) {
        info!("Terminating {} ingestor", self.name);
        if let Err(e) = self.child.kill() {
            error!("Failed to kill {} process: {}", self.name, e);
        }
    }
}

struct TestHarness {
    pool: PgPool,
    ingestors: Vec<IngestorProcess>,
    test_dir: std::path::PathBuf,
    shutdown: Arc<AtomicBool>,
}

impl TestHarness {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL")?;
        let pool = create_pool(&database_url).await?;
        
        // Create a temporary directory
        let test_dir = std::env::temp_dir().join(format!("sinex_test_{}", std::process::id()));
        std::fs::create_dir_all(&test_dir)?;
        
        Ok(Self {
            pool,
            ingestors: Vec::new(),
            test_dir,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn start_filesystem_ingestor(&mut self) -> Result<()> {
        info!("Starting filesystem ingestor monitoring: {}", self.test_dir.display());
        
        // Build the binary first
        let check = std::process::Command::new("cargo")
            .args(&["build", "-p", "unified-collector"])
            .output()?;
        
        if !check.status.success() {
            error!("Failed to build unified-collector: {:?}", String::from_utf8_lossy(&check.stderr));
            return Err(anyhow::anyhow!("Could not build unified-collector"));
        }
        
        // Create a temporary config file
        let config_content = format!(r#"
[database]
url = "{}"

[logging]
level = "info"

[filesystem]
watch_directories = ["{}"]
exclude_patterns = []
debounce_ms = 100
batch_size_events = 5
batch_timeout_ms = 1000
hash_files = false
heartbeat_interval_secs = 60
"#, std::env::var("DATABASE_URL")?, self.test_dir.display());

        let config_path = self.test_dir.join("filesystem-config.toml");
        std::fs::write(&config_path, config_content)?;

        let mut cmd = Command::new("cargo");
        cmd.args(&["run", "-p", "unified-collector", "--", "--config", &config_path.to_string_lossy()])
            .env("DATABASE_URL", std::env::var("DATABASE_URL")?)
            .env("RUST_LOG", "info,sinex=debug")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        match cmd.spawn() {
            Ok(child) => {
                info!("Filesystem ingestor process started with PID: {:?}", child.id());
                self.ingestors.push(IngestorProcess {
                    name: "filesystem",
                    child,
                    started: Instant::now(),
                });
            }
            Err(e) => {
                error!("Failed to start filesystem ingestor: {}", e);
                return Err(e.into());
            }
        }

        // Give it time to start and register
        sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    async fn start_hyprland_ingestor(&mut self) -> Result<()> {
        // Check if hyprland is running first
        let check = Command::new("hyprctl")
            .arg("version")
            .output();

        if check.is_err() {
            warn!("Hyprland not running, skipping hyprland ingestor");
            return Ok(());
        }

        info!("Starting hyprland ingestor");
        
        let mut cmd = Command::new("cargo");
        cmd.args(&["run", "-p", "unified-collector"])
            .env("DATABASE_URL", std::env::var("DATABASE_URL")?)
            .env("RUST_LOG", "info,sinex=debug")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let child = cmd.spawn()?;
        
        self.ingestors.push(IngestorProcess {
            name: "hyprland",
            child,
            started: Instant::now(),
        });

        sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    async fn start_kitty_ingestor(&mut self) -> Result<()> {
        // Check if kitty is available
        let check = Command::new("kitty")
            .arg("--version")
            .output();

        if check.is_err() {
            warn!("Kitty not available, skipping kitty ingestor");
            return Ok(());
        }

        info!("Starting kitty ingestor");
        
        let mut cmd = Command::new("cargo");
        cmd.args(&["run", "-p", "unified-collector"])
            .env("DATABASE_URL", std::env::var("DATABASE_URL")?)
            .env("RUST_LOG", "info,sinex=debug")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let child = cmd.spawn()?;
        
        self.ingestors.push(IngestorProcess {
            name: "kitty",
            child,
            started: Instant::now(),
        });

        sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    async fn trigger_filesystem_events(&self) -> Result<()> {
        info!("Triggering filesystem events");
        
        let test_file = self.test_dir.join("test_file.txt");
        let test_dir = self.test_dir.join("test_subdir");
        
        // Create file
        fs::write(&test_file, "initial content").await?;
        sleep(Duration::from_millis(500)).await;
        
        // Modify file
        fs::write(&test_file, "modified content").await?;
        sleep(Duration::from_millis(500)).await;
        
        // Create directory
        fs::create_dir(&test_dir).await?;
        sleep(Duration::from_millis(500)).await;
        
        // Create file in subdirectory
        fs::write(test_dir.join("nested.txt"), "nested file").await?;
        sleep(Duration::from_millis(500)).await;
        
        // Delete file
        fs::remove_file(&test_file).await?;
        sleep(Duration::from_millis(500)).await;
        
        Ok(())
    }

    async fn trigger_hyprland_events(&self) -> Result<()> {
        // Check if hyprland is running
        let check = TokioCommand::new("hyprctl")
            .arg("version")
            .output()
            .await;

        if check.is_err() {
            info!("Hyprland not running, skipping hyprland events");
            return Ok(());
        }

        info!("Triggering hyprland events");
        
        // Create a temporary workspace to avoid disrupting user
        TokioCommand::new("hyprctl")
            .args(&["dispatch", "workspace", "99"])
            .output()
            .await?;
        
        sleep(Duration::from_millis(500)).await;
        
        // Move focus (safe operation)
        TokioCommand::new("hyprctl")
            .args(&["dispatch", "movefocus", "r"])
            .output()
            .await?;
        
        sleep(Duration::from_millis(500)).await;
        
        // Switch back to original workspace
        TokioCommand::new("hyprctl")
            .args(&["dispatch", "workspace", "1"])
            .output()
            .await?;
        
        sleep(Duration::from_millis(500)).await;
        
        Ok(())
    }

    async fn trigger_kitty_events(&self) -> Result<()> {
        // Try to connect to kitty's control socket
        let socket_path = std::env::var("KITTY_LISTEN_ON")
            .unwrap_or_else(|_| "/tmp/kitty".to_string());
        
        if !socket_path.starts_with("unix:") {
            info!("Kitty control socket not available, trying to find running instance");
            
            // Try to use kitty @ command with timeout
            let output = tokio::time::timeout(
                Duration::from_secs(5),
                TokioCommand::new("kitty")
                    .args(&["@", "ls"])
                    .output()
            ).await;
                
            match output {
                Ok(Ok(result)) if result.status.success() => {
                    info!("Found accessible kitty instance");
                },
                Ok(Ok(_)) => {
                    info!("Kitty command failed, skipping kitty events");
                    return Ok(());
                },
                Ok(Err(_)) | Err(_) => {
                    info!("No accessible kitty instance or timeout, skipping kitty events");
                    return Ok(());
                }
            }
        }

        info!("Triggering kitty events");
        
        // Send a safe command that won't disrupt user
        TokioCommand::new("kitty")
            .args(&["@", "send-text", "--match", "title:e2e-test", "echo 'E2E test command'\\n"])
            .output()
            .await
            .ok();
        
        sleep(Duration::from_millis(500)).await;
        
        Ok(())
    }

    async fn verify_events(&self, source: &str, min_events: usize) -> Result<()> {
        info!("Verifying {} events (expecting at least {})", source, min_events);
        
        let events = sqlx::query!(
            r#"
            SELECT 
                id::TEXT as id,
                source,
                event_type,
                ts_orig,
                host,
                payload,
                ts_ingest
            FROM raw.events
            WHERE source = $1
              AND ts_ingest >= NOW() - INTERVAL '5 minutes'
            ORDER BY ts_ingest DESC
            "#,
            source
        )
        .fetch_all(&self.pool)
        .await?;
        
        info!("Found {} {} events", events.len(), source);
        
        for event in &events {
            debug!("{} event: type={}, payload={:?}", 
                source, 
                event.event_type, 
                event.payload
            );
        }
        
        // Also check total events from this source
        let total_events = sqlx::query!(
            r#"
            SELECT COUNT(*) as count
            FROM raw.events
            WHERE source = $1
            "#,
            source
        )
        .fetch_one(&self.pool)
        .await?;
        
        info!("Total {} events in database: {}", source, total_events.count.unwrap_or(0));
        
        assert!(
            events.len() >= min_events,
            "Expected at least {} {} events, but found {} (total in DB: {})",
            min_events,
            source,
            events.len(),
            total_events.count.unwrap_or(0)
        );
        
        Ok(())
    }

    async fn verify_heartbeats(&self) -> Result<()> {
        info!("Verifying agent heartbeats");
        
        let heartbeats = sqlx::query!(
            r#"
            SELECT 
                agent_name,
                last_heartbeat_ts,
                status
            FROM sinex_schemas.agent_manifests
            WHERE last_heartbeat_ts >= NOW() - INTERVAL '5 minutes'
            "#
        )
        .fetch_all(&self.pool)
        .await?;
        
        info!("Found {} active agents with recent heartbeats", heartbeats.len());
        
        for heartbeat in &heartbeats {
            info!("Agent {} last seen: {:?}", 
                heartbeat.agent_name, 
                heartbeat.last_heartbeat_ts
            );
        }
        
        // Also check if any agents are registered at all
        let all_agents = sqlx::query!(
            r#"
            SELECT COUNT(*) as count
            FROM sinex_schemas.agent_manifests
            "#
        )
        .fetch_one(&self.pool)
        .await?;
        
        info!("Total agents registered: {}", all_agents.count.unwrap_or(0));
        
        assert!(
            !heartbeats.is_empty(),
            "No recent heartbeats found. Total agents: {}",
            all_agents.count.unwrap_or(0)
        );
        
        Ok(())
    }

    async fn shutdown(mut self) -> Result<()> {
        info!("Shutting down test harness");
        self.shutdown.store(true, Ordering::SeqCst);
        
        // Terminate all ingestors
        for mut ingestor in self.ingestors.drain(..) {
            info!("Stopping {} ingestor", ingestor.name);
            ingestor.child.kill()?;
        }
        
        // Clean up test directory
        if self.test_dir.exists() {
            std::fs::remove_dir_all(&self.test_dir)?;
        }
        
        Ok(())
    }
}

#[tokio::test]
#[ignore = "Requires running services and may affect system state"]
async fn test_full_system_end_to_end() -> Result<()> {
    // Logging is already initialized by the test framework

    info!("Starting full system end-to-end test");
    
    let mut harness = TestHarness::new().await?;
    
    // Start all ingestors
    info!("Starting ingestors...");
    harness.start_filesystem_ingestor().await?;
    harness.start_hyprland_ingestor().await?;
    harness.start_kitty_ingestor().await?;
    
    // Check initial event count
    let initial_count = sqlx::query!(
        r#"SELECT COUNT(*) as count FROM raw.events"#
    )
    .fetch_one(&harness.pool)
    .await?;
    info!("Initial event count: {}", initial_count.count.unwrap_or(0));
    
    // Wait for ingestors to fully initialize and send initial heartbeats
    info!("Waiting for ingestors to initialize...");
    sleep(Duration::from_secs(5)).await;
    
    // Verify initial heartbeats
    harness.verify_heartbeats().await?;
    
    // Trigger events from each source
    harness.trigger_filesystem_events().await?;
    harness.trigger_hyprland_events().await?;
    harness.trigger_kitty_events().await?;
    
    // Wait for events to be processed
    info!("Waiting for events to be captured and stored...");
    sleep(Duration::from_secs(3)).await;
    
    // Verify events were captured
    harness.verify_events(sources::FILESYSTEM, 3).await?;
    
    // Only verify hyprland/kitty if they're running
    if Command::new("hyprctl").arg("version").output().is_ok() {
        harness.verify_events(sources::HYPRLAND, 1).await?;
    }
    
    if std::env::var("KITTY_LISTEN_ON").is_ok() || 
       Command::new("kitty").arg("--version").output().is_ok() {
        harness.verify_events(sources::TERMINAL_KITTY, 1).await?;
    }
    
    // Verify heartbeats are still coming
    info!("Waiting for additional heartbeat cycle...");
    sleep(Duration::from_secs(5)).await;
    harness.verify_heartbeats().await?;
    
    // Verify periodic snapshots if enough time has passed
    let snapshots = sqlx::query!(
        r#"
        SELECT COUNT(*) as count
        FROM raw.events
        WHERE event_type IN ($1, $2)
          AND ts_ingest >= NOW() - INTERVAL '5 minutes'
        "#,
        event_type_constants::sinex::AGENT_HEARTBEAT,
        "workspace_snapshot" // No constant defined for this yet
    )
    .fetch_one(&harness.pool)
    .await?;
    
    info!("Found {} snapshot/heartbeat events", snapshots.count.unwrap_or(0));
    
    // Clean shutdown
    harness.shutdown().await?;
    
    info!("Full system end-to-end test completed successfully");
    Ok(())
}