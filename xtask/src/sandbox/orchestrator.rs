//! Development orchestrator — test-infrastructure items only.
//!
//! `DevOrchestrator`, `RunArgs`, and `run_binary` live in `crate::orchestrator`
//! (the canonical, non-sandbox location). This module re-exports them and adds
//! sandbox-only helpers: `TestIngestdConfig`, `TestIngestdHandle`, and
//! `start_test_ingestd_with_config`.

use crate::sandbox::prelude::*;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::Command;

pub use crate::orchestrator::{DevOrchestrator, RunArgs, run_binary};

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
        let database_url = crate::infra::stack::StackConfig::for_current_checkout().map_or_else(
            |_| "postgresql:///sinex_test?host=/run/postgresql".to_string(),
            |cfg| cfg.database_url(),
        );
        Self {
            nats: sinex_primitives::nats::NatsConnectionConfig::default(),
            database_url,
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

/// Structured output captured from a binary invocation.
///
/// Provides helpers for parsing structured (JSON-line) log output from
/// binaries started with `--log-format json`.
pub struct CapturedOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

impl CapturedOutput {
    /// Parse stderr as JSON lines, returning only lines that are valid JSON objects.
    pub fn stderr_json_lines(&self) -> Vec<serde_json::Value> {
        self.stderr
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }

    /// Parse stdout as JSON lines, returning only lines that are valid JSON objects.
    pub fn stdout_json_lines(&self) -> Vec<serde_json::Value> {
        self.stdout
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }
}

/// Configuration for a test gateway instance
#[derive(Debug, Clone)]
pub struct TestGatewayConfig {
    /// TCP address to listen on (e.g., `127.0.0.1:0` for OS-assigned port)
    pub listen_addr: std::net::SocketAddr,
    pub database_url: String,
    pub nats_url: String,
    /// Path to TLS server certificate (PEM)
    pub tls_cert: PathBuf,
    /// Path to TLS server private key (PEM)
    pub tls_key: PathBuf,
    /// RPC bearer token for authentication. If set, the gateway requires this
    /// token on every request. Format: `<secret>:<role>` (e.g. `test-token:admin`).
    pub rpc_token: Option<String>,
    /// Make replay control optional (degrade gracefully without NATS replay).
    pub replay_control_optional: bool,
    /// Disable RPC rate limiting (default: true — rate limiting disabled in tests).
    pub rpc_rate_limit_disabled: bool,
}

impl TestGatewayConfig {
    /// Create a config using devshell TLS certs from `.sinex/tls/`.
    pub fn with_devshell_tls(
        listen_addr: std::net::SocketAddr,
        database_url: String,
        nats_url: String,
    ) -> Result<Self> {
        let workspace = find_workspace_root()?;
        let tls_dir = workspace.join(".sinex/tls");
        if !tls_dir.exists() {
            bail!(
                "TLS certs not found at {}. Run `xtask doctor --fix` to generate them.",
                tls_dir.display()
            );
        }
        Ok(Self {
            listen_addr,
            database_url,
            nats_url,
            tls_cert: tls_dir.join("server.pem"),
            tls_key: tls_dir.join("server-key.pem"),
            rpc_token: None,
            replay_control_optional: false,
            rpc_rate_limit_disabled: true,
        })
    }
}

/// Handle to a running test gateway instance
pub struct TestGatewayHandle {
    /// The actual bound address (useful when port 0 was specified)
    pub addr: std::net::SocketAddr,
    child: tokio::process::Child,
}

impl TestGatewayHandle {
    pub async fn stop(&mut self) -> Result<()> {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
        Ok(())
    }
}

impl Drop for TestGatewayHandle {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Allocate a free TCP port by briefly binding to `:0` and releasing.
///
/// There's a small TOCTOU window between release and process bind, but
/// it's acceptable for tests since port exhaustion is extremely unlikely.
pub fn allocate_free_port() -> Result<std::net::SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(addr)
}

/// Spawn a gateway instance for use in integration tests.
///
/// The gateway binary must be pre-built. When `wait_ready` is true (default),
/// this polls the TCP port until the gateway accepts connections before returning.
pub async fn start_test_gateway(config: TestGatewayConfig) -> Result<TestGatewayHandle> {
    start_test_gateway_inner(config, true).await
}

async fn start_test_gateway_inner(
    config: TestGatewayConfig,
    wait_ready: bool,
) -> Result<TestGatewayHandle> {
    let workspace = find_workspace_root()?;
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let target_dir = crate::orchestrator::get_target_dir(&workspace);
    let binary_path = target_dir.join(profile).join("sinex-gateway");

    if !binary_path.exists() {
        bail!(
            "sinex-gateway binary not found at {:?}. Please build it first.",
            binary_path
        );
    }

    // If port 0 was requested, allocate a real port before spawning
    let actual_addr = if config.listen_addr.port() == 0 {
        allocate_free_port()?
    } else {
        config.listen_addr
    };

    let listen_str = actual_addr.to_string();

    let mut cmd = tokio::process::Command::new(&binary_path);
    #[cfg(target_os = "linux")]
    unsafe {
        cmd.pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            Ok(())
        });
    }
    cmd.args(["rpc-server", "--tcp-listen", &listen_str])
        .env("DATABASE_URL", &config.database_url)
        .env("SINEX_NATS_URL", &config.nats_url)
        .env(
            "SINEX_GATEWAY_TLS_CERT",
            config.tls_cert.to_string_lossy().as_ref(),
        )
        .env(
            "SINEX_GATEWAY_TLS_KEY",
            config.tls_key.to_string_lossy().as_ref(),
        )
        // Clear mTLS client CA so the subprocess doesn't inherit it from the
        // parent environment (NixOS, other tests) and unexpectedly require
        // client certificates.
        .env_remove("SINEX_GATEWAY_TLS_CLIENT_CA");
    if config.rpc_rate_limit_disabled {
        cmd.env("SINEX_RPC_RATE_LIMIT_ENABLED", "false");
    }
    if let Some(token) = &config.rpc_token {
        cmd.env("SINEX_RPC_TOKEN", token);
    }
    if config.replay_control_optional {
        cmd.env("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    }
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let child = cmd.spawn()?;

    let mut handle = TestGatewayHandle {
        addr: actual_addr,
        child,
    };

    if wait_ready {
        if let Err(e) = wait_for_gateway_tcp(&actual_addr).await {
            let _ = handle.stop().await;
            return Err(e).wrap_err("Gateway failed to become ready");
        }
    }

    Ok(handle)
}

/// Poll until the gateway's TCP socket accepts connections.
async fn wait_for_gateway_tcp(addr: &std::net::SocketAddr) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(_) => return Ok(()),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => {
                return Err(eyre!(
                    "Gateway at {} did not accept TCP connections within 30s: {e}",
                    addr
                ));
            }
        }
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
    let target_dir = crate::orchestrator::get_target_dir(&workspace_root);
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
        // Set assembler state dir and annex path to the per-test work directory.
        // These env vars are part of the canonical env-first runtime contract;
        // the binary reads them directly into its typed config.
        // Do NOT use SINEX_INGESTD_WORK_DIR here: ingestd's effective config
        // surface is SINEX_ASSEMBLER_STATE_DIR plus SINEX_ANNEX_PATH.
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
