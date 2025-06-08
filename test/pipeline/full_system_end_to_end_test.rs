use anyhow::Result;
use serde_json::json;
use sinex_core::{event_type_constants, sources, RawEvent};
use sinex_db::{models_no_ts_ingest::*, pool::create_pool};
use sqlx::PgPool;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::process::Command as TokioCommand;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

struct IngestorProcess {
    name: &'static str,
    child: Child,
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
    test_dir: TempDir,
    shutdown: Arc<AtomicBool>,
}

impl TestHarness {
    async fn new() -> Result<Self> {
        let pool = create_pool(None).await?;
        let test_dir = TempDir::new()?;
        
        Ok(Self {
            pool,
            ingestors: Vec::new(),
            test_dir,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn start_filesystem_ingestor(&mut self) -> Result<()> {
        info!("Starting filesystem ingestor monitoring: {}", self.test_dir.path().display());
        
        let mut cmd = Command::new("cargo");
        cmd.args(&["run", "--bin", "filesystem-ingestor", "--"])
            .arg("--watch-dir")
            .arg(self.test_dir.path())
            .env("DATABASE_URL", std::env::var("DATABASE_URL")?)
            .env("RUST_LOG", "info,sinex=debug")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let child = cmd.spawn()?;
        
        self.ingestors.push(IngestorProcess {
            name: "filesystem",
            child,
            started: Instant::now(),
        });

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
        cmd.args(&["run", "--bin", "hyprland-ingestor"])
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
        cmd.args(&["run", "--bin", "kitty-ingestor"])
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
        
        let test_file = self.test_dir.path().join("test_file.txt");
        let test_dir = self.test_dir.path().join("test_subdir");
        
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
            
            // Try to use kitty @ command
            let output = TokioCommand::new("kitty")
                .args(&["@", "ls"])
                .output()
                .await;
                
            if output.is_err() || !output.unwrap().status.success() {
                info!("No accessible kitty instance, skipping kitty events");
                return Ok(());
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
        
        let events = sqlx::query_as!(
            RawEventDto,
            r#"
            SELECT 
                id,
                event_time,
                source,
                event_type,
                event_payload,
                correlation_id,
                causation_id,
                agent_id,
                version,
                created_at,
                processing_status as "processing_status: ProcessingStatus",
                processing_error,
                promotion_status as "promotion_status: PromotionStatus"
            FROM raw.events
            WHERE source = $1
              AND created_at >= NOW() - INTERVAL '5 minutes'
            ORDER BY event_time DESC
            "#,
            source
        )
        .fetch_all(&self.pool)
        .await?;
        
        info!("Found {} {} events", events.len(), source);
        
        for event in &events {
            debug!("{} event: type={}, payload={}", 
                source, 
                event.event_type, 
                serde_json::to_string(&event.event_payload)?
            );
        }
        
        assert!(
            events.len() >= min_events,
            "Expected at least {} {} events, but found {}",
            min_events,
            source,
            events.len()
        );
        
        Ok(())
    }

    async fn verify_heartbeats(&self) -> Result<()> {
        info!("Verifying agent heartbeats");
        
        let heartbeats = sqlx::query!(
            r#"
            SELECT 
                agent_id,
                agent_name,
                last_seen,
                is_active
            FROM sinex_schemas.agent_manifests
            WHERE last_seen >= NOW() - INTERVAL '5 minutes'
            "#
        )
        .fetch_all(&self.pool)
        .await?;
        
        info!("Found {} active agents with recent heartbeats", heartbeats.len());
        
        for heartbeat in &heartbeats {
            info!("Agent {} last seen: {:?}", 
                heartbeat.agent_name, 
                heartbeat.last_seen
            );
        }
        
        assert!(
            !heartbeats.is_empty(),
            "No recent heartbeats found"
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
        
        Ok(())
    }
}

#[tokio::test]
#[ignore = "Requires running services and may affect system state"]
async fn test_full_system_end_to_end() -> Result<()> {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,sinex=debug")
        .try_init();

    info!("Starting full system end-to-end test");
    
    let mut harness = TestHarness::new().await?;
    
    // Start all ingestors
    harness.start_filesystem_ingestor().await?;
    harness.start_hyprland_ingestor().await?;
    harness.start_kitty_ingestor().await?;
    
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
          AND created_at >= NOW() - INTERVAL '5 minutes'
        "#,
        event_type_constants::heartbeat::AGENT_HEARTBEAT,
        event_type_constants::hyprland::WORKSPACE_SNAPSHOT
    )
    .fetch_one(&harness.pool)
    .await?;
    
    info!("Found {} snapshot/heartbeat events", snapshots.count.unwrap_or(0));
    
    // Clean shutdown
    harness.shutdown().await?;
    
    info!("Full system end-to-end test completed successfully");
    Ok(())
}