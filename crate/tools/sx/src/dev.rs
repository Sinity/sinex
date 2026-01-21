//! Development mode with hot reload support.
//!
//! This module implements the `sx dev` command which provides:
//! - File watching for source changes
//! - Automatic rebuild on change
//! - Process restart with state handoff
//! - Signal handling for graceful shutdown

use crate::watcher::FileWatcher;
use camino::Utf8PathBuf;
use clap::Args;
use color_eyre::eyre::{eyre, Result};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Arguments for the dev command
#[derive(Args)]
pub struct DevArgs {
    /// Path to the processor crate
    #[arg(default_value = ".")]
    pub path: String,

    /// Binary name (defaults to crate name)
    #[arg(long)]
    pub bin: Option<String>,

    /// Additional arguments to pass to the processor
    #[arg(last = true)]
    pub args: Vec<String>,

    /// Don't watch for changes (just run)
    #[arg(long)]
    pub no_watch: bool,

    /// Build in release mode
    #[arg(long)]
    pub release: bool,

    /// Connect to production for test data (The Tether)
    #[arg(long)]
    pub tether: Option<String>,

    /// Checkpoint file path (for state continuity)
    #[arg(long)]
    pub checkpoint: Option<PathBuf>,
}

/// Orchestrator for development mode
pub struct DevOrchestrator {
    args: DevArgs,
    crate_path: Utf8PathBuf,
    binary_name: String,
    child: Option<Child>,
    shutdown_requested: Arc<AtomicBool>,
}

impl DevOrchestrator {
    pub fn new(args: DevArgs) -> Result<Self> {
        let crate_path = Utf8PathBuf::from(&args.path);

        // Determine binary name from Cargo.toml if not specified
        let binary_name = if let Some(ref bin) = args.bin {
            bin.clone()
        } else {
            Self::get_crate_name(&crate_path)?
        };

        Ok(Self {
            args,
            crate_path,
            binary_name,
            child: None,
            shutdown_requested: Arc::new(AtomicBool::new(false)),
        })
    }

    fn get_crate_name(path: &Utf8PathBuf) -> Result<String> {
        let cargo_toml = path.join("Cargo.toml");
        let contents = std::fs::read_to_string(&cargo_toml)
            .map_err(|e| eyre!("Failed to read {}: {}", cargo_toml, e))?;

        let parsed: toml::Value = contents
            .parse()
            .map_err(|e| eyre!("Failed to parse {}: {}", cargo_toml, e))?;

        parsed
            .get("package")
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str())
            .map(String::from)
            .ok_or_else(|| eyre!("No package.name found in {}", cargo_toml))
    }

    /// Build the processor
    async fn build(&self) -> Result<PathBuf> {
        info!("Building {}...", self.binary_name);

        let mut cmd = Command::new("cargo");
        cmd.arg("build")
            .arg("--bin")
            .arg(&self.binary_name)
            .current_dir(&self.crate_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if self.args.release {
            cmd.arg("--release");
        }

        // Stream output in real-time
        let mut child = cmd.spawn()?;

        // Handle stdout
        if let Some(stdout) = child.stdout.take() {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    println!("[build] {}", line);
                }
            });
        }

        // Handle stderr
        if let Some(stderr) = child.stderr.take() {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("[build] {}", line);
                }
            });
        }

        let status = child.wait().await?;
        if !status.success() {
            return Err(eyre!("Build failed with status: {}", status));
        }

        // Determine binary path
        let profile = if self.args.release {
            "release"
        } else {
            "debug"
        };
        let target_dir = self.crate_path.join("target").join(profile);
        let binary_path = target_dir.join(&self.binary_name);

        info!("Build complete: {}", binary_path);
        Ok(binary_path.into())
    }

    /// Start the processor
    async fn start(&mut self, binary_path: &PathBuf) -> Result<()> {
        info!("Starting {}...", self.binary_name);

        let mut cmd = Command::new(binary_path);
        cmd.args(&self.args.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        // Add checkpoint path if specified
        if let Some(ref checkpoint) = self.args.checkpoint {
            cmd.env("SINEX_CHECKPOINT_FILE", checkpoint);
        }

        // Add tether configuration if specified
        if let Some(ref tether) = self.args.tether {
            cmd.env("SINEX_TETHER_TARGET", tether);
        }

        let mut child = cmd.spawn()?;

        // Stream stdout
        if let Some(stdout) = child.stdout.take() {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            let name = self.binary_name.clone();
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    println!("[{}] {}", name, line);
                }
            });
        }

        // Stream stderr
        if let Some(stderr) = child.stderr.take() {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            let name = self.binary_name.clone();
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("[{}] {}", name, line);
                }
            });
        }

        self.child = Some(child);
        info!("{} started", self.binary_name);
        Ok(())
    }

    /// Stop the processor gracefully
    async fn stop(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            info!("Stopping {}...", self.binary_name);

            // Send SIGTERM for graceful shutdown
            #[cfg(unix)]
            {
                if let Some(id) = child.id() {
                    let _ = nix::sys::signal::kill(
                        nix::unistd::Pid::from_raw(id as i32),
                        nix::sys::signal::Signal::SIGTERM,
                    );
                }
            }

            // Wait for graceful shutdown with timeout
            tokio::select! {
                result = child.wait() => {
                    match result {
                        Ok(status) => info!("{} exited with: {}", self.binary_name, status),
                        Err(e) => warn!("Error waiting for {}: {}", self.binary_name, e),
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                    warn!("Timeout waiting for graceful shutdown, killing...");
                    let _ = child.kill().await;
                }
            }
        }
        Ok(())
    }

    /// Restart the processor (build first, then stop + start)
    async fn restart(&mut self) -> Result<()> {
        // Build first - don't stop if build fails
        let binary_path = match self.build().await {
            Ok(path) => path,
            Err(e) => {
                error!("Build failed: {}. Keeping current process running...", e);
                return Ok(()); // Don't crash, just wait for next file change
            }
        };

        // Build succeeded, now safe to restart
        self.stop().await?;
        self.start(&binary_path).await?;

        Ok(())
    }

    /// Run the development loop
    pub async fn run(&mut self) -> Result<()> {
        // Initial build and start
        let binary_path = self.build().await?;
        self.start(&binary_path).await?;

        if self.args.no_watch {
            // Just wait for the process to exit
            if let Some(ref mut child) = self.child {
                child.wait().await?;
            }
            return Ok(());
        }

        // Set up file watcher
        let (tx, mut rx) = mpsc::channel(32);
        let watcher = FileWatcher::new(&self.crate_path, tx)?;

        // Set up signal handler
        let shutdown = self.shutdown_requested.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Ctrl+C received, shutting down...");
            shutdown.store(true, Ordering::SeqCst);
        });

        info!(
            "Watching {} for changes. Press Ctrl+C to stop.",
            self.crate_path
        );

        // Main event loop
        loop {
            if self.shutdown_requested.load(Ordering::SeqCst) {
                break;
            }

            tokio::select! {
                Some(event) = rx.recv() => {
                    debug!("File change detected: {:?}", event);
                    info!("Rebuilding...");
                    self.restart().await?;
                }
                status = async {
                    if let Some(ref mut child) = self.child {
                        child.wait().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    match status {
                        Ok(s) if !s.success() => {
                            warn!("{} exited with: {}. Waiting for file changes...", self.binary_name, s);
                            self.child = None;
                        }
                        Ok(s) => {
                            info!("{} exited with: {}", self.binary_name, s);
                            break;
                        }
                        Err(e) => {
                            error!("Process error: {}", e);
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    // Periodic check for shutdown
                }
            }
        }

        self.stop().await?;
        drop(watcher);
        Ok(())
    }
}

/// Run the dev command
pub async fn run(args: DevArgs) -> Result<()> {
    let mut orchestrator = DevOrchestrator::new(args)?;
    orchestrator.run().await
}
