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

pub struct TestIngestdHandle {
    child: tokio::process::Child,
    pub stream_name: String,
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
    crate::process::register_tokio_child_process_group(&child, "sandbox gateway");

    let mut handle = TestGatewayHandle {
        addr: actual_addr,
        child,
    };

    if wait_ready && let Err(e) = wait_for_gateway_tcp(&actual_addr).await {
        if let Err(stop_error) = handle.stop().await {
            return Err(e).wrap_err(format!(
                "Gateway failed to become ready and cleanup failed: {stop_error:#}"
            ));
        }
        return Err(e).wrap_err("Gateway failed to become ready");
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
    let workspace_manifest = workspace_root.join("Cargo.toml");
    if workspace_manifest.exists() {
        paths.push(workspace_manifest);
    }
    let lockfile = workspace_root.join("Cargo.lock");
    if lockfile.exists() {
        paths.push(lockfile);
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
    cmd.stdin(Stdio::null()).kill_on_drop(true);

    let child = cmd.spawn()?;
    crate::process::register_tokio_child_process_group(&child, "sandbox ingestd");

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
