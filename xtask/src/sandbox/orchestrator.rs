//! Development orchestrator with hot reload support.
//!
//! This module implements the hot reload loop for running sinex binaries
//! in development mode with automatic rebuilding on source changes.

use crate::sandbox::prelude::*;
use crate::watcher::{FileWatcher, WatchEvent};
use camino::Utf8PathBuf;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

fn get_target_dir(workspace_root: &std::path::Path) -> std::path::PathBuf {
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
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    println!("[build] {line}");
                }
            });
        }

        // Handle stderr
        if let Some(stderr) = child.stderr.take() {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("[build] {line}");
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
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            let tag = label.to_owned();
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    println!("[{tag}] {line}");
                }
            });
        }

        // Stream stderr
        if let Some(stderr) = child.stderr.take() {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            let tag = label.to_owned();
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("[{tag}] {line}");
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
                        Ok(status) => println!("[run] {} exited with: {status}", self.args.binary),
                        Err(e) => eprintln!("[run] Error waiting for {}: {e}", self.args.binary),
                    }
                }
                () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
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
                eprintln!("[build] Build failed: {e}. Keeping current process running...");
                return Ok(()); // Don't crash, just wait for next file change
            }
        };

        // Build succeeded - now do seamless handoff
        if self.child.is_some() {
            // Start new process while old still running
            println!("[handoff] Starting new instance...");
            let new_child = self.spawn_new_instance(&binary_path)?;

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
                            Ok(status) => println!("[handoff] Old instance exited: {status}"),
                            Err(e) => eprintln!("[handoff] Error waiting for old instance: {e}"),
                        }
                    }
                    () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                        eprintln!("[handoff] Timeout waiting for old instance, force killing...");
                        let _ = old_child.kill().await;
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

/// Configuration for test ingestd instance
#[derive(Debug, Clone)]
pub struct TestIngestdConfig {
    pub nats: sinex_primitives::nats::NatsConnectionConfig,
    pub database_url: String,
    pub work_dir: Option<std::path::PathBuf>,
    pub namespace: Option<String>,
    pub consumer_fetch_max_messages: usize,
    pub consumer_fetch_timeout_ms: u64,
    /// Database connection pool size for the spawned ingestd.
    /// Defaults to 4 (test-appropriate; production default is 50).
    pub database_pool_size: u32,
}

impl Default for TestIngestdConfig {
    fn default() -> Self {
        Self {
            nats: sinex_primitives::nats::NatsConnectionConfig::default(),
            database_url: "postgresql:///sinex_test?host=/run/postgresql".to_string(),
            work_dir: None,
            namespace: None,
            consumer_fetch_max_messages: 100,
            consumer_fetch_timeout_ms: 50,
            database_pool_size: 4,
        }
    }
}

pub struct TestIngestdHandle {
    child: tokio::process::Child,
    pub stream_name: String,
    stderr_reader: Option<tokio::task::JoinHandle<String>>,
}

impl TestIngestdHandle {
    pub async fn stop(&mut self) -> Result<()> {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
        // Dump debug log file
        let debug_log = format!("/tmp/sinex-ingestd-{}.log", std::process::id());
        if let Ok(content) = std::fs::read_to_string(&debug_log) {
            if content.is_empty() {
                eprintln!("📋 ingestd log: EMPTY");
            } else {
                let end = content.floor_char_boundary(3000);
                let truncated = &content[..end];
                eprintln!("📋 ingestd log ({} bytes):\n{truncated}", content.len());
            }
        }
        if let Some(reader) = self.stderr_reader.take() {
            let _ = reader.await;
        }
        Ok(())
    }
}

impl Drop for TestIngestdHandle {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Find the workspace root by traversing up from current directory
fn find_workspace_root() -> Result<PathBuf> {
    let mut current = std::env::current_dir()?;
    loop {
        if current.join("Cargo.toml").exists() {
            // Check if it's a workspace root by reading content roughly
            // This is a heuristic; simpler than parsing TOML but usually sufficient for dev tools
            let content = std::fs::read_to_string(current.join("Cargo.toml")).unwrap_or_default();
            if content.contains("[workspace]") {
                return Ok(current);
            }
        }
        if !current.pop() {
            bail!("Could not find workspace root (Cargo.toml with [workspace])");
        }
    }
}

pub async fn start_test_ingestd_with_config(
    config: TestIngestdConfig,
    ctx: Option<&crate::sandbox::context::Sandbox>,
) -> Result<TestIngestdHandle> {
    let workspace_root = find_workspace_root()?;
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let target_dir = get_target_dir(&workspace_root);
    let binary_path = target_dir.join(profile).join("sinex-ingestd");

    if !binary_path.exists() {
        bail!(
            "sinex-ingestd binary not found at {:?}. Please build it first.",
            binary_path
        );
    }

    // Capture both stdout and stderr to a debug log file.
    // tracing_subscriber::fmt() defaults to stdout in 0.3.x, so we need >{file} 2>&1.
    let debug_log = format!("/tmp/sinex-ingestd-{}.log", std::process::id());
    let mut cmd = Command::new("bash");
    #[cfg(target_os = "linux")]
    unsafe {
        cmd.pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            Ok(())
        });
    }
    cmd.arg("-c").arg(format!(
        "exec {} --pool-size {} >{} 2>&1",
        binary_path.display(),
        config.database_pool_size,
        debug_log,
    ));
    cmd.env("DATABASE_URL", &config.database_url);
    cmd.env("SINEX_NATS_URL", &config.nats.url);
    if config.nats.require_tls {
        cmd.env("SINEX_NATS_REQUIRE_TLS", "true");
    }
    if let Some(ca) = &config.nats.ca_cert {
        cmd.env("SINEX_NATS_CA_CERT", ca);
    }
    if let Some(cert) = &config.nats.client_cert {
        cmd.env("SINEX_NATS_CLIENT_CERT", cert);
    }
    if let Some(key) = &config.nats.client_key {
        cmd.env("SINEX_NATS_CLIENT_KEY", key);
    }
    if let Some(ns) = &config.namespace {
        cmd.env("SINEX_NAMESPACE", ns);
    }
    if let Some(wd) = &config.work_dir {
        // Set assembler state dir and annex path to per-test work directory.
        // NOTE: We use SINEX_ASSEMBLER_STATE_DIR and SINEX_ANNEX_PATH which are
        // clap-level env vars (read directly by the binary's Args struct).
        // Do NOT use SINEX_INGESTD_WORK_DIR — Figment's .split('_') maps it to
        // "work.dir" (nested key) which doesn't match the flat "work_dir" field.
        cmd.env("SINEX_ASSEMBLER_STATE_DIR", wd.join("assembler_state"));
        cmd.env("SINEX_ANNEX_PATH", wd.join("annex"));
    }
    cmd.env(
        "SINEX_INGESTD_CONSUMER_FETCH_MAX_MESSAGES",
        config.consumer_fetch_max_messages.to_string(),
    );
    cmd.env(
        "SINEX_INGESTD_CONSUMER_FETCH_TIMEOUT_MS",
        config.consumer_fetch_timeout_ms.to_string(),
    );
    // Disable schema validation and schema sync for test instances.
    // Test events use DynamicPayload with arbitrary payloads that don't conform
    // to registered JSON schemas. Without this, events fail validation and get
    // routed to the DLQ instead of being persisted.
    cmd.env("SINEX_VALIDATE_SCHEMAS", "false");
    cmd.env("SINEX_SKIP_SCHEMA_SYNC", "true");
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let child = cmd.spawn()?;
    let stderr_reader = None;

    // Compute the stream name using the same logic as ingestd:
    // environment-prefixed base name, with optional namespace suffix.
    let env = sinex_primitives::environment::environment();
    let stream_name = env.nats_stream_name_with_namespace(
        config.namespace.as_deref(),
        &env.nats_stream_name("SINEX_RAW_EVENTS"),
    );

    // Wait for ingestd to create the JetStream stream AND attach a consumer.
    // Without stream wait: tests publish before the stream exists → silent message loss.
    // Without consumer wait: stream exists but ingestd isn't pulling yet → events
    // pile up in NATS and never reach the database before test timeout.
    if let Some(sandbox) = ctx {
        // Only wait for stream if sandbox has NATS initialized via with_nats().
        // Tests that create their own EphemeralNats pass ctx for the DB pool
        // but don't initialize NATS on the sandbox.
        if let Ok(nats) = sandbox.nats_handle() {
            let client = sandbox.nats_client();
            let js = nats.jetstream_with_client(client);
            nats.wait_for_stream(&js, &stream_name, Duration::from_secs(Timeouts::STANDARD))
                .await
                .wrap_err_with(|| format!("ingestd failed to create stream {stream_name}"))?;

            // Wait for ingestd to create a consumer on the stream. This proves
            // the process has completed startup and is actively pulling messages.
            nats.wait_for_consumer_on_stream(
                &js,
                &stream_name,
                Duration::from_secs(Timeouts::STANDARD),
            )
            .await
            .wrap_err_with(|| format!("ingestd consumer not ready on stream {stream_name}"))?;
        }
    }

    Ok(TestIngestdHandle {
        child,
        stream_name,
        stderr_reader,
    })
}
