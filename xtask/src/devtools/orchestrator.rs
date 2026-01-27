//! Development orchestrator with hot reload support.
//!
//! This module implements the hot reload loop for running sinex binaries
//! in development mode with automatic rebuilding on source changes.

use crate::devtools::watcher::{FileWatcher, WatchEvent};
use anyhow::{bail, Result};
use camino::Utf8PathBuf;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

/// Arguments for running a binary with hot reload
#[derive(Debug, Clone)]
pub struct RunArgs {
    /// Binary name (e.g., "sinex-ingestd")
    pub binary: String,
    /// Build in release mode
    pub release: bool,
    /// Disable file watching (just run once)
    pub no_watch: bool,
    /// Tether mode (e.g., "prod" to stream production events)
    pub tether: Option<String>,
    /// Checkpoint file path for state continuity
    pub checkpoint: Option<PathBuf>,
    /// Additional arguments to pass to the binary
    pub args: Vec<String>,
    /// Environment variables from stack config
    pub env_vars: Vec<(String, String)>,
}

/// Orchestrator for development mode with hot reload
pub struct DevOrchestrator {
    args: RunArgs,
    workspace_root: Utf8PathBuf,
    child: Option<Child>,
    shutdown_requested: Arc<AtomicBool>,
}

impl DevOrchestrator {
    /// Create a new orchestrator
    pub fn new(args: RunArgs, workspace_root: Utf8PathBuf) -> Self {
        Self {
            args,
            workspace_root,
            child: None,
            shutdown_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Build the binary
    async fn build(&self) -> Result<PathBuf> {
        println!("[build] Building {}...", self.args.binary);

        let mut cmd = Command::new("cargo");
        cmd.arg("build")
            .arg("-p")
            .arg(&self.args.binary)
            .arg("--bin")
            .arg(&self.args.binary)
            .current_dir(&self.workspace_root)
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
            bail!("Build failed with status: {}", status);
        }

        // Determine binary path
        let profile = if self.args.release {
            "release"
        } else {
            "debug"
        };
        let binary_path = self
            .workspace_root
            .join("target")
            .join(profile)
            .join(&self.args.binary);

        println!("[build] Build complete: {}", binary_path);
        Ok(binary_path.into())
    }

    /// Start the binary process
    async fn start(&mut self, binary_path: &PathBuf) -> Result<()> {
        println!("[run] Starting {}...", self.args.binary);

        let mut cmd = Command::new(binary_path);
        cmd.args(&self.args.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        // Set stack environment variables
        for (key, value) in &self.args.env_vars {
            cmd.env(key, value);
        }

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
            let name = self.args.binary.clone();
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
            let name = self.args.binary.clone();
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("[{}] {}", name, line);
                }
            });
        }

        self.child = Some(child);
        println!("[run] {} started", self.args.binary);
        Ok(())
    }

    /// Stop the binary gracefully
    async fn stop(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            println!("[run] Stopping {}...", self.args.binary);

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
                        Ok(status) => println!("[run] {} exited with: {}", self.args.binary, status),
                        Err(e) => eprintln!("[run] Error waiting for {}: {}", self.args.binary, e),
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                    eprintln!("[run] Timeout waiting for graceful shutdown, killing...");
                    let _ = child.kill().await;
                }
            }
        }
        Ok(())
    }

    /// Restart the binary (build first, then handoff)
    async fn restart(&mut self) -> Result<()> {
        // Build first - don't stop if build fails
        let binary_path = match self.build().await {
            Ok(path) => path,
            Err(e) => {
                eprintln!(
                    "[build] Build failed: {}. Keeping current process running...",
                    e
                );
                return Ok(()); // Don't crash, just wait for next file change
            }
        };

        // Build succeeded - now do seamless handoff
        if self.child.is_some() {
            // Start new process while old still running
            println!("[handoff] Starting new instance...");
            let new_child = self.spawn_new_instance(&binary_path).await?;

            // Wait for new instance to initialize (connect to NATS, start coordination)
            println!("[handoff] Waiting for new instance to initialize...");
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;

            // At this point, if both processes have coordination enabled:
            // - New instance detects old leader
            // - New sends handoff request (has newer build_timestamp per our Ord impl)
            // - Old drains work, saves checkpoint, releases leadership
            // - Old exits gracefully
            // If coordination is disabled, just wait for old to exit

            println!("[handoff] Waiting for old instance to complete handoff...");

            // Wait for old process to exit (handoff complete) or timeout
            if let Some(mut old_child) = self.child.take() {
                tokio::select! {
                    result = old_child.wait() => {
                        match result {
                            Ok(status) => println!("[handoff] Old instance exited: {}", status),
                            Err(e) => eprintln!("[handoff] Error waiting for old instance: {}", e),
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                        eprintln!("[handoff] Timeout waiting for old instance, force killing...");
                        let _ = old_child.kill().await;
                    }
                }
            }

            println!("[handoff] Handoff complete");
            self.child = Some(new_child);
        } else {
            // No running process, just start fresh
            self.start(&binary_path).await?;
        }

        Ok(())
    }

    /// Spawn a new instance without replacing self.child
    async fn spawn_new_instance(&self, binary_path: &PathBuf) -> Result<Child> {
        let mut cmd = Command::new(binary_path);
        cmd.args(&self.args.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        // Set stack environment variables
        for (key, value) in &self.args.env_vars {
            cmd.env(key, value);
        }

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
            let name = self.args.binary.clone();
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
            let name = self.args.binary.clone();
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("[{}] {}", name, line);
                }
            });
        }

        Ok(child)
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
        let _watcher = FileWatcher::for_workspace(self.workspace_root.as_std_path(), tx)?;

        // Set up signal handler
        let shutdown = self.shutdown_requested.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            println!("\n[ctrl+c] Shutting down...");
            shutdown.store(true, Ordering::SeqCst);
        });

        println!(
            "[watch] Watching {} for changes. Press Ctrl+C to stop.",
            self.workspace_root
        );

        // Main event loop
        loop {
            if self.shutdown_requested.load(Ordering::SeqCst) {
                break;
            }

            tokio::select! {
                Some(event) = rx.recv() => {
                    match event {
                        WatchEvent::FileChanged(path) => {
                            println!("[watch] Change detected: {:?}", path);
                            println!("[watch] Rebuilding...");
                            self.restart().await?;
                        }
                    }
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
                            eprintln!("[run] {} exited with: {}. Waiting for file changes...", self.args.binary, s);
                            self.child = None;
                        }
                        Ok(s) => {
                            println!("[run] {} exited with: {}", self.args.binary, s);
                            break;
                        }
                        Err(e) => {
                            eprintln!("[run] Process error: {}", e);
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
        Ok(())
    }

    /// Request shutdown
    #[allow(dead_code)]
    pub fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
    }
}

/// Run a binary with optional hot reload
pub async fn run_binary(args: RunArgs, workspace_root: Utf8PathBuf) -> Result<()> {
    let mut orchestrator = DevOrchestrator::new(args, workspace_root);
    orchestrator.run().await
}
