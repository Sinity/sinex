//! Development orchestrator — test-infrastructure items only.
//!
//! `DevOrchestrator`, `RunArgs`, and `run_binary` live in `crate::orchestrator`
//! (the canonical, non-sandbox location). This module re-exports them and adds
//! sandbox-only helpers: `TestIngestdConfig`, `TestIngestdHandle`, and
//! `start_test_ingestd_with_config`.

use crate::sandbox::prelude::*;
use color_eyre::eyre::WrapErr;
use guppy::MetadataCommand;
use guppy::graph::PackageGraph;
use serde_json::json;
use std::collections::HashSet;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::UnixDatagram;
use tokio::process::Command;
use walkdir::WalkDir;

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

/// Configuration for a test `sinex-source-worker` instance.
#[derive(Debug, Clone)]
pub struct TestSourceWorkerConfig {
    pub source_unit_id: String,
    pub nats: sinex_primitives::nats::NatsConnectionConfig,
    pub database_url: String,
    pub work_dir: Option<std::path::PathBuf>,
    pub namespace: Option<String>,
    pub node_config: Option<String>,
    pub service_name: Option<String>,
}

impl TestSourceWorkerConfig {
    #[must_use]
    pub fn new(source_unit_id: impl Into<String>) -> Self {
        Self {
            source_unit_id: source_unit_id.into(),
            nats: sinex_primitives::nats::NatsConnectionConfig::default(),
            database_url: crate::infra::stack::StackConfig::for_current_checkout().map_or_else(
                |_| "postgresql:///sinex_test?host=/run/postgresql".to_string(),
                |cfg| cfg.database_url(),
            ),
            work_dir: None,
            namespace: None,
            node_config: None,
            service_name: None,
        }
    }
}

pub struct TestIngestdHandle {
    child: tokio::process::Child,
    pub stream_name: String,
}

/// Handle to a running test `sinex-source-worker` instance.
pub struct TestSourceWorkerHandle {
    child: tokio::process::Child,
    pub source_unit_id: String,
}

async fn terminate_test_child(child: &mut tokio::process::Child, process_name: &str) -> Result<()> {
    // Attempt kill first; treat ESRCH / no-such-process as "already gone".
    // This avoids the TOCTOU race of is_alive() → kill() where the process can
    // exit between the two calls and the kill is applied to a recycled PID.
    if let Err(kill_err) =
        crate::process::terminate_tokio_child_process_group(child, process_name, "sandbox stop")
    {
        // If the process already exited, try_wait() will confirm it.
        if let Ok(Some(status)) = child.try_wait() {
            eprintln!("📋 {process_name} exited before explicit stop: {status}");
            return Ok(());
        }
        return Err(kill_err)
            .wrap_err_with(|| format!("failed to terminate {process_name} process group"));
    }

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

impl TestSourceWorkerHandle {
    pub async fn stop(&mut self) -> Result<()> {
        let stop_result = terminate_test_child(&mut self.child, "test source-worker").await;
        let debug_log = source_worker_debug_log_path_for_test_process(&self.source_unit_id);
        match std::fs::read_to_string(&debug_log) {
            Ok(content) if content.is_empty() => eprintln!("📋 source-worker log: EMPTY"),
            Ok(content) => {
                let end = content.floor_char_boundary(3000);
                let truncated = &content[..end];
                eprintln!(
                    "📋 source-worker log ({} bytes):\n{truncated}",
                    content.len()
                );
            }
            Err(error) => eprintln!("📋 source-worker log unavailable: {error:#}"),
        }
        stop_result
    }
}

impl Drop for TestSourceWorkerHandle {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

pub(crate) fn ingestd_debug_log_path_for_test_process() -> PathBuf {
    PathBuf::from(format!("/tmp/sinex-ingestd-{}.log", std::process::id()))
}

pub(crate) fn source_worker_debug_log_path_for_test_process(source_unit_id: &str) -> PathBuf {
    let safe_unit = source_unit_id.replace(['/', ':'], "_");
    PathBuf::from(format!(
        "/tmp/sinex-source-worker-{safe_unit}-{}.log",
        std::process::id()
    ))
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

fn trailing_log_excerpt(content: &str, max_bytes: usize) -> (&str, bool) {
    if content.len() <= max_bytes {
        return (content, false);
    }

    let min_start = content.len() - max_bytes;
    let start = content
        .char_indices()
        .map(|(index, _)| index)
        .find(|index| *index >= min_start)
        .unwrap_or(content.len());
    let start = content[start..]
        .find('\n')
        .map(|offset| start + offset + 1)
        .unwrap_or(start);
    (&content[start..], true)
}

fn format_ingestd_debug_context(debug_log: &std::path::Path) -> String {
    match read_ingestd_debug_log(debug_log) {
        Ok(Some(content)) => {
            let (excerpt, truncated) = trailing_log_excerpt(&content, 3000);
            if truncated {
                format!(
                    "ingestd debug log at {} ({} bytes, trailing excerpt):\n{}",
                    debug_log.display(),
                    content.len(),
                    excerpt
                )
            } else {
                format!(
                    "ingestd debug log at {} ({} bytes):\n{}",
                    debug_log.display(),
                    content.len(),
                    excerpt
                )
            }
        }
        Ok(None) => format!("ingestd debug log at {} was empty", debug_log.display()),
        Err(log_error) => format!(
            "ingestd debug log at {} unavailable: {log_error:#}",
            debug_log.display()
        ),
    }
}

fn notify_socket_path(prefix: &str) -> Result<PathBuf> {
    let base = PathBuf::from("/tmp");
    std::fs::create_dir_all(&base)
        .wrap_err_with(|| format!("failed to create notify socket dir '{}'", base.display()))?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| eyre!("system clock is before UNIX_EPOCH: {err}"))?
        .as_nanos();
    Ok(base.join(format!(
        "sx-{prefix}-{}-{timestamp}.sock",
        std::process::id()
    )))
}

async fn wait_for_ready_notify(
    process_name: &str,
    listener: &UnixDatagram,
    child: &mut tokio::process::Child,
    timeout_duration: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    let mut buf = [0_u8; 256];

    loop {
        if let Some(status) = child.try_wait()? {
            return Err(eyre!(
                "{process_name} exited before READY=1 notification (status: {status})"
            ));
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(eyre!(
                "{process_name} did not send READY=1 within {timeout_duration:?}"
            ));
        }

        let remaining = deadline - now;
        let poll_window = remaining.min(Duration::from_millis(250));
        match tokio::time::timeout(poll_window, listener.recv(&mut buf)).await {
            Ok(Ok(len)) => {
                let message = std::str::from_utf8(&buf[..len])
                    .wrap_err_with(|| format!("{process_name} sent non-UTF-8 sd_notify payload"))?;
                if message.lines().any(|line| line == "READY=1") {
                    return Ok(());
                }
            }
            Ok(Err(error)) => return Err(error).wrap_err("failed to receive sd_notify payload"),
            Err(_) => {}
        }
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
/// this waits for the gateway's `sd_notify(READY=1)` signal before returning.
pub async fn start_test_gateway(config: TestGatewayConfig) -> Result<TestGatewayHandle> {
    start_test_gateway_inner(config, true).await
}

async fn start_test_gateway_inner(
    config: TestGatewayConfig,
    wait_ready: bool,
) -> Result<TestGatewayHandle> {
    let workspace = find_workspace_root()?;
    let freshness = check_runtime_binary_freshness(&workspace, "sinex-gateway", "sinex-gateway")?;
    freshness.ensure_fresh()?;
    let binary_path = freshness.binary_path;

    // If port 0 was requested, allocate a real port before spawning
    let actual_addr = if config.listen_addr.port() == 0 {
        allocate_free_port()?
    } else {
        config.listen_addr
    };

    let listen_str = actual_addr.to_string();
    let notify_socket_path = notify_socket_path("gw")?;
    let _ = std::fs::remove_file(&notify_socket_path);
    let notify_listener = UnixDatagram::bind(&notify_socket_path)
        .wrap_err_with(|| format!("failed to bind {}", notify_socket_path.display()))?;

    let mut cmd = tokio::process::Command::new(&binary_path);
    crate::process::configure_managed_child_tokio(&mut cmd);
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
        .env_remove("SINEX_GATEWAY_TLS_CLIENT_CA")
        .env("NOTIFY_SOCKET", &notify_socket_path);
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
    crate::process::register_tokio_child_process_group(&child, "sandbox gateway");

    let mut handle = TestGatewayHandle {
        addr: actual_addr,
        child,
    };

    if wait_ready
        && let Err(e) = wait_for_ready_notify(
            "sinex-gateway",
            &notify_listener,
            &mut handle.child,
            Duration::from_secs(Timeouts::STANDARD),
        )
        .await
    {
        let _ = std::fs::remove_file(&notify_socket_path);
        if let Err(stop_error) = handle.stop().await {
            return Err(e).wrap_err(format!(
                "Gateway failed to become ready and cleanup failed: {stop_error:#}"
            ));
        }
        return Err(e).wrap_err("Gateway failed to become ready");
    }
    let _ = std::fs::remove_file(&notify_socket_path);

    Ok(handle)
}

/// Find the workspace root by traversing up from current directory
pub(crate) fn find_workspace_root() -> Result<PathBuf> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeBinaryFreshnessStatus {
    Fresh,
    Missing,
    Stale,
}

impl RuntimeBinaryFreshnessStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Missing => "missing",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeBinaryFreshnessReport {
    pub(crate) package: String,
    pub(crate) binary_name: String,
    pub(crate) binary_path: PathBuf,
    pub(crate) status: RuntimeBinaryFreshnessStatus,
    pub(crate) binary_modified_at: Option<SystemTime>,
    pub(crate) newest_input_path: Option<PathBuf>,
    pub(crate) newest_input_modified_at: Option<SystemTime>,
    pub(crate) input_count: usize,
    pub(crate) build_command: String,
}

impl RuntimeBinaryFreshnessReport {
    pub(crate) fn is_fresh(&self) -> bool {
        self.status == RuntimeBinaryFreshnessStatus::Fresh
    }

    pub(crate) fn ensure_fresh(&self) -> Result<()> {
        if self.is_fresh() {
            return Ok(());
        }
        color_eyre::eyre::bail!("{}", self.error_message())
    }

    pub(crate) fn error_message(&self) -> String {
        match self.status {
            RuntimeBinaryFreshnessStatus::Fresh => {
                format!("{} is fresh", self.binary_name)
            }
            RuntimeBinaryFreshnessStatus::Missing => format!(
                "{} binary not found at {}. Run `{}` before launching tests that spawn this runtime binary.",
                self.binary_name,
                self.binary_path.display(),
                self.build_command,
            ),
            RuntimeBinaryFreshnessStatus::Stale => {
                let newest = self.newest_input_path.as_ref().map_or_else(
                    || "<unknown>".to_string(),
                    |path| path.display().to_string(),
                );
                format!(
                    "{} binary at {} is stale: newest source input {} is newer than the binary. Run `{}` before launching tests that spawn this runtime binary.",
                    self.binary_name,
                    self.binary_path.display(),
                    newest,
                    self.build_command,
                )
            }
        }
    }

    pub(crate) fn to_json(&self) -> serde_json::Value {
        json!({
            "package": self.package,
            "binary_name": self.binary_name,
            "binary_path": self.binary_path.display().to_string(),
            "status": self.status.as_str(),
            "binary_modified_at_epoch_secs": system_time_epoch_secs(self.binary_modified_at),
            "newest_input_path": self.newest_input_path.as_ref().map(|path| path.display().to_string()),
            "newest_input_modified_at_epoch_secs": system_time_epoch_secs(self.newest_input_modified_at),
            "input_count": self.input_count,
            "build_command": self.build_command,
        })
    }
}

pub(crate) fn runtime_binary_path(workspace_root: &std::path::Path, binary_name: &str) -> PathBuf {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    crate::orchestrator::get_target_dir(workspace_root)
        .join(profile)
        .join(binary_name)
}

/// Return the expected `sinex-source-worker` binary path for this workspace.
///
/// This mirrors the runtime-binary path convention used by gateway and ingestd
/// launchers while giving production-path tests a source-worker-specific
/// public helper.
#[must_use]
pub fn source_worker_binary_path(workspace_root: &std::path::Path) -> PathBuf {
    runtime_binary_path(workspace_root, "sinex-source-worker")
}

pub(crate) fn check_runtime_binary_freshness(
    workspace_root: &std::path::Path,
    package: &str,
    binary_name: &str,
) -> Result<RuntimeBinaryFreshnessReport> {
    let binary_path = runtime_binary_path(workspace_root, binary_name);
    let build_command = format!("xtask build -p {package}");
    let input_paths = collect_runtime_binary_input_paths(workspace_root, package)?;
    runtime_binary_freshness_from_inputs(
        package,
        binary_name,
        binary_path,
        input_paths,
        build_command,
    )
}

pub(crate) fn runtime_binary_freshness_from_inputs(
    package: &str,
    binary_name: &str,
    binary_path: PathBuf,
    input_paths: Vec<PathBuf>,
    build_command: String,
) -> Result<RuntimeBinaryFreshnessReport> {
    let binary_modified_at = std::fs::metadata(&binary_path)
        .and_then(|metadata| metadata.modified())
        .ok();
    let newest_input = newest_modified_input(&input_paths)?;
    let status = match (binary_modified_at, newest_input.as_ref()) {
        (None, _) => RuntimeBinaryFreshnessStatus::Missing,
        (Some(_), None) => RuntimeBinaryFreshnessStatus::Fresh,
        (Some(binary_mtime), Some((_, input_mtime))) if binary_mtime >= *input_mtime => {
            RuntimeBinaryFreshnessStatus::Fresh
        }
        (Some(_), Some(_)) => RuntimeBinaryFreshnessStatus::Stale,
    };
    Ok(RuntimeBinaryFreshnessReport {
        package: package.to_string(),
        binary_name: binary_name.to_string(),
        binary_path,
        status,
        binary_modified_at,
        newest_input_path: newest_input.as_ref().map(|(path, _)| path.clone()),
        newest_input_modified_at: newest_input.map(|(_, modified_at)| modified_at),
        input_count: input_paths.len(),
        build_command,
    })
}

fn collect_runtime_binary_input_paths(
    workspace_root: &std::path::Path,
    package: &str,
) -> Result<Vec<PathBuf>> {
    // The input set drives the binary-staleness check. It must be conservative
    // enough to catch real changes but tight enough that unrelated workspace
    // edits don't force a rebuild.
    //
    // Intentionally NOT included:
    //   - workspace `Cargo.toml`: edited whenever ANY workspace member is added,
    //     removed, or renamed — including changes unrelated to the runtime
    //     binary's dependency closure.
    //   - workspace `Cargo.lock`: touched by `cargo build` / `cargo update`
    //     for any package in the workspace; a `cargo build -p sinex-db` run
    //     bumps the lockfile mtime past the `sinex-ingestd` binary's mtime,
    //     marking ingestd "stale" even though nothing in its dependency
    //     closure changed.
    //
    // If a real dependency-graph change does happen, the per-crate
    // `Cargo.toml` files in the dep closure (collected below via
    // `workspace_dependency_roots`) capture it. Cargo's incremental compile
    // is the second line of defence: if our cache check falsely concludes
    // "fresh", cargo will rebuild whatever its own staleness check finds.
    //
    // See #1220 — pre-fix this stage ran 168 times/week consuming 55min/week
    // largely because of the lockfile / workspace-Cargo.toml mtime tail.
    let mut paths = Vec::new();
    for root in workspace_dependency_roots(workspace_root, package)? {
        paths.push(root.join("Cargo.toml"));
        let build_rs = root.join("build.rs");
        if build_rs.exists() {
            paths.push(build_rs);
        }
        let src = root.join("src");
        if src.exists() {
            collect_source_files(&src, &mut paths);
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn workspace_dependency_roots(
    workspace_root: &std::path::Path,
    package: &str,
) -> Result<Vec<PathBuf>> {
    let mut command = MetadataCommand::new();
    command
        .current_dir(workspace_root.to_path_buf())
        .manifest_path(workspace_root.join("Cargo.toml"));
    let metadata = command
        .exec()
        .context("failed to execute cargo metadata for runtime binary freshness")?;
    let graph =
        PackageGraph::from_metadata(metadata).context("failed to build cargo package graph")?;
    let root = workspace_root
        .canonicalize()
        .unwrap_or_else(|_| workspace_root.to_path_buf());
    let package_metadata = graph
        .packages()
        .find(|candidate| candidate.name() == package)
        .ok_or_else(|| color_eyre::eyre::eyre!("package '{package}' not found in workspace"))?;
    let mut roots = Vec::new();
    let mut seen_roots = HashSet::new();
    let mut visited = HashSet::new();
    let mut stack = vec![package_metadata];
    while let Some(metadata) = stack.pop() {
        if !visited.insert(metadata.id().clone()) {
            continue;
        }
        if !push_workspace_package_root(&root, metadata, &mut seen_roots, &mut roots) {
            continue;
        }
        for link in metadata.direct_links() {
            if link.normal().is_present() || link.build().is_present() {
                stack.push(link.to());
            }
        }
    }
    Ok(roots)
}

fn push_workspace_package_root(
    workspace_root: &std::path::Path,
    package: guppy::graph::PackageMetadata<'_>,
    seen: &mut HashSet<PathBuf>,
    roots: &mut Vec<PathBuf>,
) -> bool {
    let manifest = package.manifest_path().as_std_path();
    let Some(package_root) = manifest.parent() else {
        return false;
    };
    let normalized = package_root
        .canonicalize()
        .unwrap_or_else(|_| package_root.to_path_buf());
    if !normalized.starts_with(workspace_root) {
        return false;
    }
    if seen.insert(normalized.clone()) {
        roots.push(normalized);
    }
    true
}

fn collect_source_files(root: &std::path::Path, paths: &mut Vec<PathBuf>) {
    for entry in WalkDir::new(root)
        .into_iter()
        .filter_map(std::result::Result::ok)
    {
        if entry.file_type().is_file()
            && entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "rs")
        {
            paths.push(entry.path().to_path_buf());
        }
    }
}

fn newest_modified_input(paths: &[PathBuf]) -> Result<Option<(PathBuf, SystemTime)>> {
    let mut newest = None;
    for path in paths {
        let Ok(metadata) = std::fs::metadata(path) else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        let modified_at = metadata
            .modified()
            .wrap_err_with(|| format!("failed to inspect mtime for {}", path.display()))?;
        if newest
            .as_ref()
            .is_none_or(|(_, newest_mtime)| modified_at > *newest_mtime)
        {
            newest = Some((path.clone(), modified_at));
        }
    }
    Ok(newest)
}

fn system_time_epoch_secs(time: Option<SystemTime>) -> Option<u64> {
    time.and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
}

pub async fn start_test_source_worker(
    config: TestSourceWorkerConfig,
    ctx: Option<&crate::sandbox::context::Sandbox>,
) -> Result<TestSourceWorkerHandle> {
    let workspace_root = find_workspace_root()?;
    let freshness = check_runtime_binary_freshness(
        &workspace_root,
        "sinex-source-worker",
        "sinex-source-worker",
    )?;
    if let Some(sandbox) = ctx {
        sandbox.record_evidence_event(
            "runtime_binary.freshness",
            "checked runtime binary freshness before launching test source-worker",
            freshness.to_json(),
        );
    }
    freshness.ensure_fresh()?;
    let binary_path = freshness.binary_path.clone();

    let debug_log = source_worker_debug_log_path_for_test_process(&config.source_unit_id);
    let notify_socket_path = notify_socket_path("sw")?;
    let _ = std::fs::remove_file(&notify_socket_path);
    let notify_listener = UnixDatagram::bind(&notify_socket_path)
        .wrap_err_with(|| format!("failed to bind {}", notify_socket_path.display()))?;

    let log_file = std::fs::File::create(&debug_log)
        .wrap_err_with(|| format!("failed to create {}", debug_log.display()))?;
    let log_file_for_stderr = log_file
        .try_clone()
        .wrap_err_with(|| format!("failed to clone {}", debug_log.display()))?;

    let mut cmd = Command::new(&binary_path);
    crate::process::configure_managed_child_tokio(&mut cmd);
    cmd.args([
        "--source-unit",
        &config.source_unit_id,
        "--runner-pack",
        "source-worker",
    ]);
    cmd.env("DATABASE_URL", &config.database_url);
    cmd.env("SINEX_NATS_URL", &config.nats.url);
    cmd.env("SINEX_SOURCE_UNIT", &config.source_unit_id);
    cmd.env("SINEX_RUNNER_PACK", "source-worker");
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
        cmd.arg("--work-dir").arg(wd);
    }
    if let Some(service_name) = &config.service_name {
        cmd.arg("--service-name").arg(service_name);
    }
    if let Some(node_config) = &config.node_config {
        cmd.arg("--node-config").arg(node_config);
    }
    cmd.env("NOTIFY_SOCKET", &notify_socket_path);
    cmd.arg("service");
    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_for_stderr))
        .kill_on_drop(true);

    let mut child = cmd.spawn()?;
    crate::process::register_tokio_child_process_group(&child, "sandbox source-worker");

    if let Some(sandbox) = ctx
        && sandbox.nats_handle().is_ok()
        && let Err(error) = wait_for_ready_notify(
            "sinex-source-worker",
            &notify_listener,
            &mut child,
            Duration::from_secs(Timeouts::STANDARD),
        )
        .await
    {
        let _ = std::fs::remove_file(&notify_socket_path);
        let mut handle = TestSourceWorkerHandle {
            child,
            source_unit_id: config.source_unit_id,
        };
        if let Err(stop_error) = handle.stop().await {
            return Err(error).wrap_err(format!(
                "source-worker failed to become ready and cleanup failed: {stop_error:#}"
            ));
        }
        return Err(error).wrap_err("source-worker failed to become ready");
    }
    let _ = std::fs::remove_file(&notify_socket_path);

    Ok(TestSourceWorkerHandle {
        child,
        source_unit_id: config.source_unit_id,
    })
}

/// Run a one-shot `sinex-source-worker scan` subprocess and capture its output.
///
/// Use this for production-path tests that need to exercise the real binary
/// path without keeping a long-running service process alive.
pub async fn run_test_source_worker_scan(
    config: TestSourceWorkerConfig,
    targets: &[PathBuf],
    ctx: Option<&crate::sandbox::context::Sandbox>,
) -> Result<CapturedOutput> {
    let workspace_root = find_workspace_root()?;
    let freshness = check_runtime_binary_freshness(
        &workspace_root,
        "sinex-source-worker",
        "sinex-source-worker",
    )?;
    if let Some(sandbox) = ctx {
        sandbox.record_evidence_event(
            "runtime_binary.freshness",
            "checked runtime binary freshness before running test source-worker scan",
            freshness.to_json(),
        );
    }
    freshness.ensure_fresh()?;

    let mut cmd = Command::new(&freshness.binary_path);
    crate::process::configure_managed_child_tokio(&mut cmd);
    cmd.args([
        "--source-unit",
        &config.source_unit_id,
        "--runner-pack",
        "source-worker",
    ]);
    if let Some(wd) = &config.work_dir {
        cmd.arg("--work-dir").arg(wd);
    }
    if let Some(service_name) = &config.service_name {
        cmd.arg("--service-name").arg(service_name);
    }
    if let Some(node_config) = &config.node_config {
        cmd.arg("--node-config").arg(node_config);
    }
    cmd.arg("scan").arg("--until").arg("snapshot");
    for target in targets {
        cmd.arg("--targets").arg(target);
    }

    cmd.env("DATABASE_URL", &config.database_url);
    cmd.env("SINEX_NATS_URL", &config.nats.url);
    cmd.env("SINEX_SOURCE_UNIT", &config.source_unit_id);
    cmd.env("SINEX_RUNNER_PACK", "source-worker");
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
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let child = cmd.spawn()?;
    crate::process::register_tokio_child_process_group(&child, "sandbox source-worker scan");
    let output = tokio::time::timeout(
        Duration::from_secs(Timeouts::STANDARD),
        child.wait_with_output(),
    )
    .await
    .wrap_err("source-worker scan timed out")?
    .wrap_err("failed to wait for source-worker scan")?;

    let captured = CapturedOutput {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        exit_code: output.status.code().unwrap_or(-1),
    };
    if !output.status.success() {
        bail!(
            "source-worker scan for '{}' exited with {}.\nstdout:\n{}\nstderr:\n{}",
            config.source_unit_id,
            captured.exit_code,
            captured.stdout,
            captured.stderr
        );
    }
    Ok(captured)
}

pub async fn start_test_ingestd_with_config(
    config: TestIngestdConfig,
    ctx: Option<&crate::sandbox::context::Sandbox>,
) -> Result<TestIngestdHandle> {
    let workspace_root = find_workspace_root()?;
    let freshness =
        check_runtime_binary_freshness(&workspace_root, "sinex-ingestd", "sinex-ingestd")?;
    if let Some(sandbox) = ctx {
        sandbox.record_evidence_event(
            "runtime_binary.freshness",
            "checked runtime binary freshness before launching test ingestd",
            freshness.to_json(),
        );
    }
    freshness.ensure_fresh()?;
    let binary_path = freshness.binary_path.clone();

    // Capture both stdout and stderr to a debug log file.
    // tracing_subscriber::fmt() defaults to stdout in 0.3.x, so we need >{file} 2>&1.
    let debug_log = ingestd_debug_log_path_for_test_process();
    let notify_socket_path = notify_socket_path("in")?;
    let _ = std::fs::remove_file(&notify_socket_path);
    let notify_listener = UnixDatagram::bind(&notify_socket_path)
        .wrap_err_with(|| format!("failed to bind {}", notify_socket_path.display()))?;
    let mut cmd = Command::new("bash");
    crate::process::configure_managed_child_tokio(&mut cmd);
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
        // Set assembler state and content-store roots to the per-test work directory.
        // These env vars are part of the canonical env-first runtime contract;
        // the binary reads them directly into its typed config.
        // Do NOT use SINEX_INGESTD_WORK_DIR here: ingestd's effective config
        // surface is SINEX_ASSEMBLER_STATE_DIR plus SINEX_CONTENT_STORE_PATH.
        cmd.env("SINEX_ASSEMBLER_STATE_DIR", wd.join("assembler_state"));
        cmd.env("SINEX_CONTENT_STORE_PATH", wd.join("content-store"));
        cmd.env(
            "SINEX_CONTENT_STORE_PROCESS_COUNTERS_PATH",
            wd.join("content-store-process-counters.json"),
        );
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
    cmd.env("NOTIFY_SOCKET", &notify_socket_path);
    cmd.stdin(Stdio::null()).kill_on_drop(true);

    let mut child = cmd.spawn()?;
    crate::process::register_tokio_child_process_group(&child, "sandbox ingestd");

    // Compute the stream name using the same logic as ingestd:
    // environment-prefixed base name, with optional namespace suffix.
    let env = sinex_primitives::environment::environment();
    let stream_name = env.nats_stream_name_with_namespace(
        config.namespace.as_deref(),
        &env.nats_stream_name("SINEX_RAW_EVENTS"),
    );

    // Wait for ingestd's own readiness signal. The binary emits READY=1 only
    // after the JetStream consumer and MaterialAssembler have both completed
    // setup, which is the same readiness contract production systemd uses.
    if let Some(sandbox) = ctx {
        // Only wait for stream if sandbox has NATS initialized via with_nats().
        // Tests that create their own EphemeralNats pass ctx for the DB pool
        // but don't initialize NATS on the sandbox.
        if let Ok(nats) = sandbox.nats_handle() {
            let _ = nats;
            if let Err(error) = wait_for_ready_notify(
                "sinex-ingestd",
                &notify_listener,
                &mut child,
                Duration::from_secs(Timeouts::STANDARD),
            )
            .await
            {
                let _ = std::fs::remove_file(&notify_socket_path);
                let _ = child.start_kill();
                return Err(error)
                    .wrap_err(format_ingestd_debug_context(&debug_log))
                    .wrap_err("ingestd did not reach systemd READY state");
            }
        }
    }
    let _ = std::fs::remove_file(&notify_socket_path);

    Ok(TestIngestdHandle { child, stream_name })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;
    use tokio::process::Command;

    fn write_file_at(
        path: &std::path::Path,
        content: &str,
        modified_at: SystemTime,
    ) -> TestResult<()> {
        fs::write(path, content)?;
        let file = fs::OpenOptions::new().write(true).open(path)?;
        file.set_times(std::fs::FileTimes::new().set_modified(modified_at))?;
        Ok(())
    }

    #[sinex_test]
    async fn source_worker_binary_path_uses_runtime_target_dir() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let path = source_worker_binary_path(tempdir.path());

        assert!(path.ends_with("sinex-source-worker"));
        assert!(path.starts_with(crate::orchestrator::get_target_dir(tempdir.path())));
        Ok(())
    }

    #[sinex_test]
    async fn source_worker_debug_log_path_includes_sanitized_unit() -> TestResult<()> {
        let path = source_worker_debug_log_path_for_test_process("browser.history:test");
        let rendered = path.display().to_string();

        assert!(rendered.contains("browser.history_test"));
        assert!(!rendered.contains(':'));
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_freshness_reports_missing_binary() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let report = runtime_binary_freshness_from_inputs(
            "sinex-ingestd",
            "sinex-ingestd",
            tempdir.path().join("target/debug/sinex-ingestd"),
            Vec::new(),
            "xtask build -p sinex-ingestd".to_string(),
        )?;

        assert_eq!(report.status, RuntimeBinaryFreshnessStatus::Missing);
        let message = report.error_message();
        assert!(message.contains("sinex-ingestd binary not found"));
        assert!(message.contains("xtask build -p sinex-ingestd"));
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_freshness_reports_stale_binary() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let binary = tempdir.path().join("sinex-ingestd");
        let source = tempdir.path().join("src.rs");
        write_file_at(&binary, "binary", UNIX_EPOCH + Duration::from_secs(1_000))?;
        write_file_at(&source, "source", UNIX_EPOCH + Duration::from_secs(2_000))?;

        let report = runtime_binary_freshness_from_inputs(
            "sinex-ingestd",
            "sinex-ingestd",
            binary,
            vec![source.clone()],
            "xtask build -p sinex-ingestd".to_string(),
        )?;

        assert_eq!(report.status, RuntimeBinaryFreshnessStatus::Stale);
        assert_eq!(report.newest_input_path.as_deref(), Some(source.as_path()));
        let message = report.error_message();
        assert!(message.contains("is stale"));
        assert!(message.contains(source.display().to_string().as_str()));
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_freshness_accepts_newer_binary() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let binary = tempdir.path().join("sinex-ingestd");
        let source = tempdir.path().join("src.rs");
        write_file_at(&source, "source", UNIX_EPOCH + Duration::from_secs(1_000))?;
        write_file_at(&binary, "binary", UNIX_EPOCH + Duration::from_secs(2_000))?;

        let report = runtime_binary_freshness_from_inputs(
            "sinex-ingestd",
            "sinex-ingestd",
            binary,
            vec![source],
            "xtask build -p sinex-ingestd".to_string(),
        )?;

        assert_eq!(report.status, RuntimeBinaryFreshnessStatus::Fresh);
        report.ensure_fresh()?;
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_inputs_exclude_dev_only_xtask_sources() -> TestResult<()> {
        let workspace = find_workspace_root()?;
        let inputs = collect_runtime_binary_input_paths(&workspace, "sinex-ingestd")?;
        let ingestd_main = workspace.join("crate/core/sinex-ingestd/src/main.rs");

        assert!(
            inputs.iter().any(|path| path == &ingestd_main),
            "runtime binary inputs should include the target binary source"
        );
        assert!(
            inputs.iter().all(|path| {
                let relative = path.strip_prefix(&workspace).unwrap_or(path);
                !relative.starts_with("xtask/src")
            }),
            "runtime binary inputs must not include xtask dev-dependency sources: {inputs:#?}"
        );
        Ok(())
    }

    /// Regression: workspace Cargo.toml and Cargo.lock must not appear in the
    /// runtime-binary input set. Touching either (cargo update for an
    /// unrelated package, adding a workspace member) used to mark every
    /// runtime binary stale and trigger a full pre-test rebuild. See #1220.
    #[sinex_test]
    async fn runtime_binary_inputs_exclude_workspace_manifest_and_lockfile() -> TestResult<()> {
        let workspace = find_workspace_root()?;
        let inputs = collect_runtime_binary_input_paths(&workspace, "sinex-ingestd")?;
        let workspace_manifest = workspace.join("Cargo.toml");
        let lockfile = workspace.join("Cargo.lock");

        assert!(
            !inputs.iter().any(|path| path == &workspace_manifest),
            "runtime binary inputs must NOT include workspace Cargo.toml; \
             edits there (members/shared deps) over-invalidate every runtime binary (#1220)"
        );
        assert!(
            !inputs.iter().any(|path| path == &lockfile),
            "runtime binary inputs must NOT include workspace Cargo.lock; \
             `cargo build -p <other>` bumps lockfile mtime and falsely marks \
             this binary stale (#1220). cargo's own incremental compile remains \
             the safety net for real dep-graph changes"
        );
        Ok(())
    }

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
        assert!(
            message.contains(
                workspace_root
                    .join("Cargo.toml")
                    .display()
                    .to_string()
                    .as_str()
            )
        );
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
    async fn format_ingestd_debug_context_includes_path_size_and_content() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let debug_log = tempdir.path().join("ingestd.log");
        fs::write(&debug_log, "startup failed\nmissing stream\n")?;
        let context = format_ingestd_debug_context(&debug_log);

        assert!(context.contains(debug_log.display().to_string().as_str()));
        assert!(context.contains("(30 bytes)"));
        assert!(context.contains("startup failed\nmissing stream\n"));
        Ok(())
    }

    #[sinex_test]
    async fn format_ingestd_debug_context_uses_tail_for_long_logs() -> TestResult<()> {
        let tempdir = tempfile::tempdir()?;
        let debug_log = tempdir.path().join("ingestd.log");
        let content = format!("{}\nFINAL ROOT CAUSE\n", "startup chatter\n".repeat(400));
        fs::write(&debug_log, &content)?;
        let context = format_ingestd_debug_context(&debug_log);

        assert!(context.contains(debug_log.display().to_string().as_str()));
        assert!(context.contains("trailing excerpt"));
        assert!(context.contains("FINAL ROOT CAUSE"));
        assert!(
            !context.contains("startup chatte\n"),
            "excerpt should start on a line boundary: {context}"
        );
        assert!(!context.contains(content.as_str()));
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
