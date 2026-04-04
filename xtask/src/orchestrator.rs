//! Canonical hot-reload orchestrator for development binary execution.
//!
//! `DevOrchestrator` and `RunArgs` are the single source of truth for running
//! sinex binaries with optional file-watching and hot-reload. The sandbox module
//! re-exports these types from here.

use camino::Utf8PathBuf;
use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use crate::watcher::{FileWatcher, WatchEvent};

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

pub(crate) fn get_target_dir(workspace_root: &std::path::Path) -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        return std::path::PathBuf::from(dir);
    }
    let custom_target = workspace_root.join(".sinex/target");
    if custom_target.exists() {
        return custom_target;
    }
    workspace_root.join("target")
}

/// Orchestrator for development mode with hot reload
pub struct DevOrchestrator {
    args: RunArgs,
    workspace_root: Utf8PathBuf,
    child: Option<Child>,
    shutdown_requested: Arc<AtomicBool>,
}

async fn stream_reader_lines<R, F>(reader: R, context_label: &str, mut emit: F) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    F: FnMut(String),
{
    let mut lines = BufReader::new(reader).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => emit(line),
            Ok(None) => return Ok(()),
            Err(error) => bail!("failed to read {context_label}: {error}"),
        }
    }
}

fn child_running(child: &mut Child, label: &str) -> Result<bool> {
    child
        .try_wait()
        .with_context(|| format!("failed to poll {label} process state"))?
        .map_or(Ok(true), |_| Ok(false))
}

#[cfg(unix)]
fn send_sigterm_if_running(child: &mut Child, label: &str) -> Result<()> {
    let Some(id) = child.id() else {
        return Ok(());
    };
    match nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(id as i32),
        nix::sys::signal::Signal::SIGTERM,
    ) {
        Ok(()) => Ok(()),
        Err(error) => {
            if child_running(child, label)? {
                bail!("failed to send SIGTERM to {label} (pid {id}): {error}");
            }
            Ok(())
        }
    }
}

#[cfg(not(unix))]
fn send_sigterm_if_running(_child: &mut Child, _label: &str) -> Result<()> {
    Ok(())
}

async fn kill_child_if_running(child: &mut Child, label: &str, context: &str) -> Result<()> {
    if !child_running(child, label)? {
        return Ok(());
    }
    match child.kill().await {
        Ok(()) => {}
        Err(error) => {
            if child_running(child, label)? {
                return Err(eyre!(error).wrap_err(format!("failed to kill {label} {context}")));
            }
        }
    }
    child
        .wait()
        .await
        .with_context(|| format!("failed to wait for {label} after kill {context}"))?;
    Ok(())
}

impl DevOrchestrator {
    /// Create a new orchestrator
    #[must_use]
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
            tokio::spawn(async move {
                if let Err(error) =
                    stream_reader_lines(stdout, "build stdout", |line| println!("[build] {line}"))
                        .await
                {
                    eprintln!("[build] {error}");
                }
            });
        }

        // Handle stderr
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                if let Err(error) =
                    stream_reader_lines(stderr, "build stderr", |line| eprintln!("[build] {line}"))
                        .await
                {
                    eprintln!("[build] {error}");
                }
            });
        }

        let status = child.wait().await?;
        if !status.success() {
            bail!("Build failed with status: {status}");
        }

        // Determine binary path
        let profile = if self.args.release {
            "release"
        } else {
            "debug"
        };
        let target_dir = get_target_dir(self.workspace_root.as_std_path());
        let binary_path = target_dir.join(profile).join(&self.args.binary);

        println!("[build] Build complete: {}", binary_path.display());
        Ok(binary_path)
    }

    /// Build a `Command` for the target binary with args, env vars, and piped stdio.
    fn build_process_command(&self, binary_path: &PathBuf) -> Command {
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

        cmd
    }

    /// Spawn a command and attach streaming tasks for stdout/stderr.
    ///
    /// Each output line is prefixed with `[{label}]` so interleaved output
    /// from multiple instances remains distinguishable.
    fn spawn_with_streaming(&self, mut cmd: Command, label: &str) -> Result<Child> {
        let mut child = cmd.spawn()?;

        // Stream stdout
        if let Some(stdout) = child.stdout.take() {
            let tag = label.to_owned();
            tokio::spawn(async move {
                if let Err(error) = stream_reader_lines(stdout, &format!("{tag} stdout"), |line| {
                    println!("[{tag}] {line}");
                })
                .await
                {
                    eprintln!("[run] {error}");
                }
            });
        }

        // Stream stderr
        if let Some(stderr) = child.stderr.take() {
            let tag = label.to_owned();
            tokio::spawn(async move {
                if let Err(error) = stream_reader_lines(stderr, &format!("{tag} stderr"), |line| {
                    eprintln!("[{tag}] {line}");
                })
                .await
                {
                    eprintln!("[run] {error}");
                }
            });
        }

        Ok(child)
    }

    /// Start the binary process
    fn start(&mut self, binary_path: &PathBuf) -> Result<()> {
        println!("[run] Starting {}...", self.args.binary);

        let cmd = self.build_process_command(binary_path);
        let child = self.spawn_with_streaming(cmd, &self.args.binary)?;

        self.child = Some(child);
        println!("[run] {} started", self.args.binary);
        Ok(())
    }

    /// Stop the binary gracefully
    async fn stop(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            println!("[run] Stopping {}...", self.args.binary);

            send_sigterm_if_running(&mut child, &self.args.binary)?;

            // Wait for graceful shutdown with timeout
            tokio::select! {
                result = child.wait() => {
                    match result {
                        Ok(status) => println!("[run] {} exited with: {status}", self.args.binary),
                        Err(e) => eprintln!("[run] Error waiting for {}: {e}", self.args.binary),
                    }
                }
                () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                    eprintln!("[run] Timeout waiting for graceful shutdown, killing...");
                    kill_child_if_running(&mut child, &self.args.binary, "after shutdown timeout").await?;
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
                eprintln!("[build] Build failed: {e}. Keeping current process running...");
                return Ok(()); // Don't crash, just wait for next file change
            }
        };

        // Build succeeded - now do seamless handoff
        if self.child.is_some() {
            // Start new process while old still running
            println!("[handoff] Starting new instance...");
            let mut new_child = self.spawn_new_instance(&binary_path)?;

            // Wait for new instance to initialize: poll for up to 5s.
            // If the process crashes immediately, abort the handoff rather than wasting 3s.
            println!("[handoff] Waiting for new instance to initialize...");
            let new_crashed = tokio::select! {
                status = new_child.wait() => {
                    match status {
                        Ok(s) => {
                            eprintln!("[handoff] New instance exited immediately (status: {s}). Handoff aborted.");
                        }
                        Err(e) => {
                            eprintln!("[handoff] Error waiting for new instance: {e}. Handoff aborted.");
                        }
                    }
                    true
                }
                () = tokio::time::sleep(std::time::Duration::from_secs(5)) => false
            };
            if new_crashed {
                return Ok(());
            }
            println!("[handoff] New instance initialized");

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
                            Ok(status) => println!("[handoff] Old instance exited: {status}"),
                            Err(e) => eprintln!("[handoff] Error waiting for old instance: {e}"),
                        }
                    }
                    () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                        eprintln!("[handoff] Timeout waiting for old instance, force killing...");
                        kill_child_if_running(
                            &mut old_child,
                            &format!("old {}", self.args.binary),
                            "after handoff timeout",
                        )
                        .await?;
                    }
                }
            }

            println!("[handoff] Handoff complete");
            self.child = Some(new_child);
        } else {
            // No running process, just start fresh
            self.start(&binary_path)?;
        }

        Ok(())
    }

    /// Spawn a new instance without replacing `self.child`.
    fn spawn_new_instance(&self, binary_path: &PathBuf) -> Result<Child> {
        let cmd = self.build_process_command(binary_path);
        self.spawn_with_streaming(cmd, &self.args.binary)
    }

    /// Run the development loop
    pub async fn run(&mut self) -> Result<()> {
        // Initial build and start
        let binary_path = self.build().await?;
        self.start(&binary_path)?;

        if self.args.no_watch {
            // Just wait for the process to exit
            if let Some(ref mut child) = self.child {
                child.wait().await?;
            }
            return Ok(());
        }

        // Set up file watcher
        let (tx, mut rx) = mpsc::channel(32);
        let _watcher = FileWatcher::for_workspace(self.workspace_root.as_std_path(), tx)
            .map_err(|e| eyre!(e.to_string()))?;

        let shutdown_signal = tokio::signal::ctrl_c();
        tokio::pin!(shutdown_signal);

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
                result = &mut shutdown_signal => {
                    result.wrap_err("failed to wait for Ctrl+C in dev orchestrator")?;
                    println!("\n[ctrl+c] Shutting down...");
                    self.shutdown_requested.store(true, Ordering::SeqCst);
                }
                Some(event) = rx.recv() => {
                    match event {
                        WatchEvent::FileChanged(path) => {
                            println!("[watch] Change detected: {}", path.display());
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
                            eprintln!("[run] {} exited with: {s}. Waiting for file changes...", self.args.binary);
                            self.child = None;
                        }
                        Ok(s) => {
                            println!("[run] {} exited with: {s}", self.args.binary);
                            break;
                        }
                        Err(e) => {
                            eprintln!("[run] Process error: {e}");
                            break;
                        }
                    }
                }
                () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    // Periodic check for shutdown
                }
            }
        }

        self.stop().await?;
        Ok(())
    }

    /// Request a graceful shutdown from outside the event loop.
    ///
    /// Sets the shutdown flag that the `run()` event loop checks each iteration.
    /// Useful for programmatic callers that hold a reference to the orchestrator
    /// (e.g., test harnesses or embedding contexts).
    pub fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
    }
}

/// Run a binary with optional hot reload
pub async fn run_binary(args: RunArgs, workspace_root: Utf8PathBuf) -> Result<()> {
    let mut orchestrator = DevOrchestrator::new(args, workspace_root);
    orchestrator.run().await
}

#[cfg(test)]
mod tests {
    use super::{child_running, stream_reader_lines};
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_stream_reader_lines_collects_utf8_lines() -> ::xtask::sandbox::TestResult<()> {
        use parking_lot::Mutex;
        use tokio::io::AsyncWriteExt;

        let (reader, mut writer) = tokio::io::duplex(64);
        writer.write_all(b"alpha\nbeta\n").await?;
        drop(writer);

        let collected = std::sync::Arc::new(Mutex::new(Vec::new()));
        let collected_clone = collected.clone();
        stream_reader_lines(reader, "test stdout", move |line| {
            collected_clone.lock().push(line);
        })
        .await?;

        assert_eq!(
            *collected.lock(),
            vec!["alpha".to_string(), "beta".to_string()]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_stream_reader_lines_surfaces_invalid_utf8() -> ::xtask::sandbox::TestResult<()> {
        use tokio::io::AsyncWriteExt;

        let (reader, mut writer) = tokio::io::duplex(64);
        writer.write_all(&[0xff, b'\n']).await?;
        drop(writer);

        let error = stream_reader_lines(reader, "build stdout", |_| {})
            .await
            .expect_err("invalid utf8 should surface");
        let message = format!("{error:#}");
        assert!(message.contains("failed to read build stdout"));
        assert!(message.contains("valid UTF-8"));
        Ok(())
    }

    #[sinex_test]
    async fn test_child_running_reports_exited_process() -> ::xtask::sandbox::TestResult<()> {
        let mut child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg("exit 0")
            .spawn()?;
        child.wait().await?;

        assert!(!child_running(&mut child, "test child")?);
        Ok(())
    }
}
