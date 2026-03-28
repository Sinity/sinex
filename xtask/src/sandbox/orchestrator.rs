//! Development orchestrator — test-infrastructure items only.
//!
//! `DevOrchestrator`, `RunArgs`, and `run_binary` live in `crate::orchestrator`
//! (the canonical, non-sandbox location). This module re-exports them and adds
//! sandbox-only helpers: `TestIngestdConfig`, `TestIngestdHandle`, and
//! `start_test_ingestd_with_config`.

use crate::sandbox::prelude::*;
use color_eyre::eyre::WrapErr;
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
}

async fn terminate_test_child(child: &mut tokio::process::Child, process_name: &str) -> Result<()> {
    if let Some(status) = child
        .try_wait()
        .wrap_err_with(|| format!("failed to inspect {process_name} child status before stop"))?
    {
        eprintln!("📋 {process_name} exited before explicit stop: {status}");
        return Ok(());
    }

    child
        .kill()
        .await
        .wrap_err_with(|| format!("failed to kill {process_name} child process"))?;
    child
        .wait()
        .await
        .wrap_err_with(|| format!("failed to wait for {process_name} child process after kill"))?;
    Ok(())
}

impl TestIngestdHandle {
    pub async fn stop(&mut self) -> Result<()> {
        let stop_result = terminate_test_child(&mut self.child, "test ingestd").await;
        // Dump debug log file
        let debug_log = ingestd_debug_log_path_for_test_process();
        match read_ingestd_debug_log(&debug_log) {
            Ok(None) => {
                eprintln!("📋 ingestd log: EMPTY");
            }
            Ok(Some(content)) => {
                let end = content.floor_char_boundary(3000);
                let truncated = &content[..end];
                eprintln!("📋 ingestd log ({} bytes):\n{truncated}", content.len());
            }
            Err(error) => eprintln!("📋 ingestd log unavailable: {error:#}"),
        }
        stop_result
    }
}

impl Drop for TestIngestdHandle {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

pub(crate) fn ingestd_debug_log_path_for_test_process() -> PathBuf {
    PathBuf::from(format!("/tmp/sinex-ingestd-{}.log", std::process::id()))
}

pub(crate) fn read_ingestd_debug_log(path: &std::path::Path) -> Result<Option<String>> {
    let content = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read ingestd debug log '{}'", path.display()))?;
    if content.is_empty() {
        Ok(None)
    } else {
        Ok(Some(content))
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
    /// Parse stderr as JSON lines, failing if any non-empty line is invalid.
    pub fn stderr_json_lines(&self) -> Result<Vec<serde_json::Value>> {
        parse_json_lines(&self.stderr, "stderr")
    }

    /// Parse stdout as JSON lines, failing if any non-empty line is invalid.
    pub fn stdout_json_lines(&self) -> Result<Vec<serde_json::Value>> {
        parse_json_lines(&self.stdout, "stdout")
    }
}

fn parse_json_lines(output: &str, stream_name: &str) -> Result<Vec<serde_json::Value>> {
    output
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            let value = serde_json::from_str::<serde_json::Value>(line).wrap_err_with(|| {
                format!(
                    "failed to parse {stream_name} JSON line {}: {line}",
                    index + 1
                )
            })?;
            if !value.is_object() {
                bail!(
                    "{stream_name} JSON line {} is not an object: {line}",
                    index + 1
                );
            }
            Ok(value)
        })
        .collect()
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
        terminate_test_child(&mut self.child, "test gateway").await
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
            if let Err(stop_error) = handle.stop().await {
                return Err(e).wrap_err(format!(
                    "Gateway failed to become ready and cleanup failed: {stop_error:#}"
                ));
            }
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
    find_workspace_root_from(std::env::current_dir()?)
}

fn find_workspace_root_from(mut current: PathBuf) -> Result<PathBuf> {
    loop {
        let manifest_path = current.join("Cargo.toml");
        if manifest_path.exists() {
            // Check if it's a workspace root by reading content roughly
            // This is a heuristic; simpler than parsing TOML but usually sufficient for dev tools
            let content = std::fs::read_to_string(&manifest_path).wrap_err_with(|| {
                format!(
                    "failed to read workspace candidate manifest at {}",
                    manifest_path.display()
                )
            })?;
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
    let debug_log = ingestd_debug_log_path_for_test_process();
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
        debug_log.display(),
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
    cmd.stdin(Stdio::null()).kill_on_drop(true);

    let child = cmd.spawn()?;

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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tokio::process::Command;

    #[sinex_test]
    async fn captured_output_stdout_json_lines_surfaces_invalid_json() -> TestResult<()> {
        let output = CapturedOutput {
            stdout: "{\"ok\":true}\nnot-json\n".to_string(),
            stderr: String::new(),
            exit_code: 0,
        };

        let error = output
            .stdout_json_lines()
            .expect_err("invalid JSON line should surface");
        let message = format!("{error:#}");
        assert!(message.contains("failed to parse stdout JSON line 2"));
        Ok(())
    }

    #[sinex_test]
    async fn captured_output_stderr_json_lines_rejects_non_object_values() -> TestResult<()> {
        let output = CapturedOutput {
            stdout: String::new(),
            stderr: "[]\n".to_string(),
            exit_code: 0,
        };

        let error = output
            .stderr_json_lines()
            .expect_err("non-object JSON line should surface");
        let message = format!("{error:#}");
        assert!(message.contains("stderr JSON line 1 is not an object"));
        Ok(())
    }

    #[sinex_test]
    async fn find_workspace_root_from_surfaces_unreadable_manifest() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let workspace_root = tempdir.path().join("workspace");
        std::fs::create_dir_all(&workspace_root)?;
        std::fs::create_dir(workspace_root.join("Cargo.toml"))?;

        let error = find_workspace_root_from(workspace_root.clone())
            .expect_err("directory manifest should surface");
        let message = format!("{error:#}");
        assert!(message.contains("failed to read workspace candidate manifest"));
        assert!(message.contains(workspace_root.join("Cargo.toml").display().to_string().as_str()));
        Ok(())
    }

    #[sinex_test]
    async fn read_ingestd_debug_log_reports_missing_file() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let error = read_ingestd_debug_log(&tempdir.path().join("missing.log")).unwrap_err();
        assert!(format!("{error:#}").contains("failed to read ingestd debug log"));
        Ok(())
    }

    #[sinex_test]
    async fn read_ingestd_debug_log_treats_empty_file_as_empty() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let debug_log = tempdir.path().join("ingestd.log");
        fs::write(&debug_log, "")?;
        assert!(read_ingestd_debug_log(&debug_log)?.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn read_ingestd_debug_log_preserves_non_empty_content() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let debug_log = tempdir.path().join("ingestd.log");
        fs::write(&debug_log, "line one\nline two\n")?;
        assert_eq!(
            read_ingestd_debug_log(&debug_log)?,
            Some("line one\nline two\n".to_string())
        );
        Ok(())
    }

    #[sinex_test]
    async fn terminate_test_child_accepts_exited_process() -> TestResult<()> {
        let mut child = Command::new("true").spawn()?;
        child.wait().await?;
        terminate_test_child(&mut child, "unit-test child").await?;
        Ok(())
    }

    #[sinex_test]
    async fn terminate_test_child_kills_running_process() -> TestResult<()> {
        let mut child = Command::new("sleep").arg("30").spawn()?;
        terminate_test_child(&mut child, "unit-test child").await?;
        Ok(())
    }
}
