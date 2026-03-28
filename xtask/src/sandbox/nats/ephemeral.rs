use std::collections::HashMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_nats::{
    Client,
    jetstream::{
        self, ErrorCode,
        consumer::PullConsumer,
        consumer::{AckPolicy, pull::Config as ConsumerConfig},
        context::{CreateStreamError, CreateStreamErrorKind},
    },
};
use color_eyre::eyre::{Result, WrapErr, eyre};
use rand::RngExt;
use sinex_primitives::nats::NatsConnectionConfig;
use tempfile::TempDir;
use tokio::{
    process::{Child, Command},
    sync::Mutex as AsyncMutex,
    time::{Instant, sleep, timeout},
};
use tokio_stream::StreamExt;
use tracing::warn;
use which::which;

static SHARED_NATS_REGISTRY: std::sync::LazyLock<AsyncMutex<HashMap<String, Arc<EphemeralNats>>>> =
    std::sync::LazyLock::new(|| AsyncMutex::new(HashMap::new()));

/// Ephemeral JetStream-enabled NATS server spawned for tests.
pub struct EphemeralNats {
    process: Arc<AsyncMutex<Option<Child>>>,
    url: String,
    _store: TempDir,
    log_path: Option<PathBuf>,
    chaos: Option<ChaosConfig>,
    stream_prefix: Option<String>,
    tls: Option<TlsConfig>,
    token: Option<String>,
}

impl EphemeralNats {
    /// Start a fresh NATS server using default configuration.
    pub async fn start() -> Result<Self> {
        EphemeralNatsBuilder::default().start().await
    }

    /// Start a fresh NATS server with optional config file.
    pub async fn start_with_config(config_file: Option<PathBuf>) -> Result<Self> {
        let mut builder = EphemeralNatsBuilder::default();
        builder.config_file = config_file;
        builder.start().await
    }

    #[must_use]
    pub fn builder() -> EphemeralNatsBuilder {
        EphemeralNatsBuilder::default()
    }
}

#[derive(Debug, Clone, Default, bon::Builder)]
pub struct EphemeralNatsBuilder {
    pub config_file: Option<PathBuf>,
    pub tls: Option<TlsConfig>,
    pub token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub ca_cert: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
}

impl EphemeralNatsBuilder {
    pub fn with_tls_fixtures_path(mut self, path: impl AsRef<Path>) -> Self {
        let p = path.as_ref();
        self.tls = Some(TlsConfig {
            ca_cert: p.join("ca.pem"),
            server_cert: p.join("server.pem"),
            server_key: p.join("server-key.pem"),
            client_cert: p.join("client.pem"),
            client_key: p.join("client-key.pem"),
        });
        self
    }

    pub fn with_config_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config_file = Some(path.into());
        self
    }

    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }
}

impl EphemeralNatsBuilder {
    pub async fn start(self) -> Result<EphemeralNats> {
        let binary = EphemeralNats::resolve_binary()?;

        let store_dir = TempDir::new()?;
        tokio::fs::create_dir_all(store_dir.path()).await?;

        // Nextest runs tests concurrently in separate processes.
        let log_path = store_dir.path().join("nats.log");
        let log_file = std::fs::File::create(&log_path)?;
        let log_err = log_file.try_clone()?;

        let (url, child) = {
            const MAX_ATTEMPTS: usize = 10;
            let mut attempt = 0usize;
            loop {
                attempt += 1;
                let port = EphemeralNats::reserve_port()?;
                // If TLS is enabled, we might need a different scheme,
                // but for raw TCP connection checks "127.0.0.1:port" is usually fine.
                // However, the client URL exposed to tests needs to match scheme.
                let url = if self.tls.is_some() {
                    format!("tls://127.0.0.1:{port}")
                } else {
                    format!("127.0.0.1:{port}")
                };

                let mut cmd = Command::new(&binary);
                // Auto-kill the nats-server when the parent test process exits.
                // Without this, shared NATS instances (held in a static registry)
                // become orphans because Rust doesn't guarantee static destructors.
                #[cfg(target_os = "linux")]
                unsafe {
                    cmd.pre_exec(|| {
                        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
                        Ok(())
                    });
                }
                cmd.arg("--jetstream")
                    .arg("--store_dir")
                    .arg(store_dir.path())
                    .arg("--port")
                    .arg(port.to_string())
                    .arg("--http_port")
                    .arg("0")
                    .stdout(std::process::Stdio::from(log_file.try_clone()?))
                    .stderr(std::process::Stdio::from(log_err.try_clone()?));

                if let Some(cfg) = &self.config_file {
                    cmd.arg("--config").arg(cfg);
                }

                if let Some(tls) = &self.tls {
                    cmd.arg("--tls")
                        .arg("--tlscert")
                        .arg(&tls.server_cert)
                        .arg("--tlskey")
                        .arg(&tls.server_key)
                        .arg("--tlscacert")
                        .arg(&tls.ca_cert)
                        .arg("--tlsverify");
                }

                if let Some(token) = &self.token {
                    cmd.arg("--auth").arg(token);
                }

                let mut child = cmd.spawn()?;

                // We pass the raw port for the connectivity check.
                match EphemeralNats::wait_for_ready(port, &mut child).await {
                    Ok(()) => break (url, child),
                    Err(err) => {
                        if let Err(error) = child.start_kill() {
                            warn!(error = %error, "Failed to start-kill NATS child after readiness failure");
                        }
                        match timeout(Duration::from_secs(1), child.wait()).await {
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => {
                                warn!(error = %error, "Failed waiting for NATS child after readiness failure");
                            }
                            Err(_) => {
                                warn!("Timed out waiting for NATS child after readiness failure");
                            }
                        }

                        if attempt >= MAX_ATTEMPTS {
                            return Err(err);
                        }
                    }
                }
            }
        };

        Ok(EphemeralNats {
            process: Arc::new(AsyncMutex::new(Some(child))),
            url,
            _store: store_dir,
            log_path: Some(log_path),
            chaos: None,
            stream_prefix: None,
            tls: self.tls,
            token: self.token,
        })
    }
}

impl EphemeralNats {
    /// Return the client URL (e.g. `127.0.0.1:4222`).
    #[must_use]
    pub fn client_url(&self) -> &str {
        &self.url
    }

    /// Return a `NatsConnectionConfig` suitable for connecting to this server.
    /// Includes TLS certificates if the server was started with TLS.
    #[must_use]
    pub fn connection_config(&self) -> NatsConnectionConfig {
        let mut config = NatsConnectionConfig::default();
        config.url.clone_from(&self.url);
        config.require_tls = self.tls.is_some();
        if let Some(tls) = &self.tls {
            config.ca_cert = Some(tls.ca_cert.clone());
            config.client_cert = Some(tls.client_cert.clone());
            config.client_key = Some(tls.client_key.clone());
        }
        if let Some(token) = &self.token {
            config.token = Some(token.clone());
        }
        config
    }

    /// Return the tail of the NATS log file, if logging is enabled.
    pub fn log_tail(&self, max_lines: usize) -> Result<Option<String>> {
        let Some(path) = self.log_path.as_ref() else {
            return Ok(None);
        };
        let contents = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("failed to read NATS log {}", path.display()))?;
        let mut lines: Vec<&str> = contents.lines().collect();
        if lines.len() > max_lines {
            lines = lines.split_off(lines.len().saturating_sub(max_lines));
        }
        Ok(Some(lines.join("\n")))
    }

    /// Assert the NATS log does not contain any of the provided needles.
    pub fn assert_log_does_not_contain(&self, needles: &[&str], max_lines: usize) -> Result<()> {
        let Some(tail) = self.log_tail(max_lines)? else {
            return Ok(());
        };
        for needle in needles {
            if tail.contains(needle) {
                return Err(eyre!(
                    "nats-server log contains unexpected entry '{needle}':\n{tail}"
                ));
            }
        }
        Ok(())
    }

    /// Expose underlying process for managed shutdown.
    #[must_use]
    pub fn process_handle(&self) -> Arc<AsyncMutex<Option<Child>>> {
        self.process.clone()
    }

    /// Stop the underlying NATS process (best-effort).
    pub async fn shutdown(&self) -> Result<()> {
        let mut guard = self.process.lock().await;
        if let Some(mut child) = guard.take() {
            if let Err(error) = child.start_kill() {
                warn!(error = %error, "Failed to start-kill ephemeral NATS child during shutdown");
            }
            match timeout(Duration::from_secs(2), child.wait()).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    warn!(error = %error, "Failed waiting for ephemeral NATS child during shutdown");
                }
                Err(_) => {
                    warn!("Timed out waiting for ephemeral NATS child during shutdown");
                }
            }
        }
        Ok(())
    }

    /// Connect an async-nats client to this server.
    pub async fn connect(&self) -> Result<Client> {
        if let Some(cfg) = &self.chaos {
            cfg.simulate_latency().await;
            cfg.maybe_fail("simulated connection failure")?;
        }

        let mut config = NatsConnectionConfig::default();
        config.url.clone_from(&self.url);
        config.require_tls = self.tls.is_some();
        if let Some(tls) = &self.tls {
            config.ca_cert = Some(tls.ca_cert.clone());
            config.client_cert = Some(tls.client_cert.clone());
            config.client_key = Some(tls.client_key.clone());
        }
        if let Some(token) = &self.token {
            config.token = Some(token.clone());
        }
        let opts = config
            .to_options()
            .await
            .map_err(|e| eyre!("failed to build NATS connect options: {e}"))?;

        timeout(Duration::from_secs(5), opts.connect(&self.url))
            .await
            .map_err(|_| eyre!("timed out connecting to NATS at {}", self.url))?
            .map_err(|err| eyre!("failed to connect to NATS at {}: {err}", self.url))
    }

    /// Attach chaos settings (latency + failure rate) to this server instance.
    #[must_use]
    pub fn with_chaos(mut self, latency: Duration, failure_rate: f64) -> Self {
        self.chaos = Some(ChaosConfig {
            latency,
            failure_rate: failure_rate.clamp(0.0, 1.0),
        });
        self
    }

    /// Override the stream/subject prefix tests should use when creating streams.
    /// Useful for isolating multiple test suites sharing a single NATS instance.
    pub fn with_stream_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.stream_prefix = Some(prefix.into());
        self
    }

    /// Return the active stream prefix (if any).
    #[must_use]
    pub fn stream_prefix(&self) -> Option<&str> {
        self.stream_prefix.as_deref()
    }

    /// Apply the configured stream prefix (if any) to a name.
    #[must_use]
    pub fn qualify(&self, name: &str) -> String {
        if let Some(prefix) = &self.stream_prefix {
            format!("{prefix}{name}")
        } else {
            name.to_string()
        }
    }

    /// Create a `JetStream` context bound to this server.
    pub async fn jetstream(&self) -> Result<jetstream::Context> {
        let client = self.connect().await?;
        Ok(jetstream::new(client))
    }

    /// Create a `JetStream` context using an existing client connection.
    /// This keeps tests coupled to `EphemeralNats` while avoiding extra connections.
    #[must_use]
    pub fn jetstream_with_client(&self, client: Client) -> jetstream::Context {
        jetstream::new(client)
    }

    /// Create or update a `JetStream` stream with the provided subjects.
    pub async fn create_stream(&self, name: &str, subjects: &[&str]) -> Result<()> {
        let js = self.jetstream().await?;
        let config = jetstream::stream::Config {
            name: name.to_string(),
            subjects: subjects
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
            ..Default::default()
        };
        js.get_or_create_stream(config)
            .await
            .map_err(color_eyre::Report::from)
            .wrap_err_with(|| format!("failed to create stream {name}"))?;
        Ok(())
    }

    /// Create or reuse a stream, then create (or reuse) a durable pull consumer.
    /// Returns the final stream name (including any prefix) and the consumer.
    pub async fn ensure_stream_with_consumer(
        &self,
        stream: &str,
        subjects: &[&str],
        mut consumer_cfg: ConsumerConfig,
    ) -> Result<(String, PullConsumer)> {
        let qualified_stream = self.qualify(stream);
        let qualified_subjects: Vec<String> = subjects.iter().map(|s| self.qualify(s)).collect();
        let subject_refs: Vec<&str> = qualified_subjects.iter().map(String::as_str).collect();

        self.ensure_stream_allowing_overlap(&qualified_stream, &subject_refs)
            .await?;

        if consumer_cfg.durable_name.is_none() {
            consumer_cfg.durable_name =
                Some(format!("{}-consumer", qualified_stream.to_lowercase()));
        }

        let consumer = self
            .create_consumer(&qualified_stream, consumer_cfg)
            .await?;

        Ok((qualified_stream, consumer))
    }

    /// Create a stream + durable consumer with sensible defaults (explicit ack, 30s wait,
    /// `max_deliver=10`) for simple test setups.
    pub async fn ensure_default_stream_with_consumer(
        &self,
        stream: &str,
        subject: &str,
    ) -> Result<(String, PullConsumer)> {
        let mut cfg = ConsumerConfig::default();
        cfg.ack_policy = AckPolicy::Explicit;
        cfg.ack_wait = Duration::from_secs(30);
        cfg.max_deliver = 10;

        self.ensure_stream_with_consumer(stream, &[subject], cfg)
            .await
    }

    /// Create a stream, skipping if the existing stream has overlapping subjects.
    /// Useful for concurrent test runs where streams may already exist with a slightly
    /// different config.
    pub async fn ensure_stream_allowing_overlap(
        &self,
        name: &str,
        subjects: &[&str],
    ) -> Result<()> {
        match self.create_stream(name, subjects).await {
            Ok(()) => Ok(()),
            Err(err) => {
                if Self::is_stream_overlap_or_exists_error(&err) {
                    // Treat overlapping config as non-fatal in shared NATS instances.
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }

    fn is_stream_overlap_or_exists_error(err: &color_eyre::Report) -> bool {
        err.chain().any(|cause| {
            cause
                .downcast_ref::<CreateStreamError>()
                .is_some_and(|stream_err| match stream_err.kind() {
                    CreateStreamErrorKind::JetStream(js_err) => matches!(
                        js_err.error_code(),
                        ErrorCode::STREAM_NAME_EXIST | ErrorCode::STREAM_SUBJECT_OVERLAP
                    ),
                    _ => false,
                })
        })
    }

    /// Create a durable pull consumer for the given stream.
    pub async fn create_consumer(
        &self,
        stream: &str,
        config: ConsumerConfig,
    ) -> Result<PullConsumer> {
        let js = self.jetstream().await?;
        let stream_handle = js
            .get_stream(stream)
            .await
            .map_err(|err| eyre!("failed to load stream {stream}: {err}"))?;
        let durable = config
            .durable_name
            .clone()
            .unwrap_or_else(|| "test-consumer".to_string());
        stream_handle
            .get_or_create_consumer(&durable, config)
            .await
            .map_err(|err| eyre!("failed to create consumer on {stream}: {err}"))
    }

    /// Wait until at least `expected` messages have been published to `subject`.
    pub async fn wait_for_subject_messages(
        &self,
        subject: &str,
        expected: usize,
        timeout_duration: Duration,
    ) -> Result<()> {
        let client = self.connect().await?;
        let mut subscriber = client
            .subscribe(subject.to_string())
            .await
            .map_err(|err| eyre!("failed to subscribe to {subject}: {err}"))?;

        let deadline = Instant::now() + timeout_duration;
        let mut remaining = expected;
        while remaining > 0 {
            let now = Instant::now();
            if now >= deadline {
                return Err(eyre!(
                    "timed out waiting for {} messages on subject {}",
                    expected,
                    subject
                ));
            }
            let wait_for = deadline - now;
            let next = timeout(wait_for, subscriber.next())
                .await
                .map_err(|_| eyre!("timed out waiting for {subject} messages"))?;

            match next {
                Some(_) => remaining -= 1,
                None => return Err(eyre!("subscription for {subject} ended unexpectedly")),
            }
        }
        Ok(())
    }

    /// Wait for a stream to become available on this `JetStream` server.
    pub async fn wait_for_stream(
        &self,
        js: &jetstream::Context,
        name: &str,
        timeout_duration: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            match js.get_stream(name).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    if Instant::now() >= deadline {
                        return Err(eyre!("stream {name} not ready: {err}"));
                    }
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// Wait until at least one consumer exists on the given JetStream stream.
    ///
    /// This ensures that the process creating consumers (e.g. ingestd) has fully
    /// started and is actively pulling messages. Without this, tests may publish
    /// events to a stream before anyone is consuming them.
    pub async fn wait_for_consumer_on_stream(
        &self,
        js: &jetstream::Context,
        stream_name: &str,
        timeout_duration: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout_duration;
        loop {
            if let Ok(mut stream) = js.get_stream(stream_name).await
                && let Ok(info) = stream.info().await
                && info.state.consumer_count > 0
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(eyre!(
                    "no consumer found on stream {stream_name} within {timeout_duration:?}"
                ));
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    fn resolve_binary() -> Result<PathBuf> {
        if let Ok(explicit) = std::env::var("NATS_SERVER_BIN") {
            let path = Path::new(&explicit);
            if path.exists() {
                return Ok(path.to_path_buf());
            }
            return Err(eyre!(
                "NATS_SERVER_BIN points to missing binary: {explicit}"
            ));
        }
        which("nats-server").map_err(|_| {
            eyre!(
                "nats-server binary not found on PATH. Install it (e.g. `nix-env -iA nixpkgs.nats-server` or `brew install nats-server`) or set NATS_SERVER_BIN to the binary path."
            )
        })
    }

    fn reserve_port() -> Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        drop(listener);
        Ok(port)
    }

    async fn wait_for_ready(port: u16, child: &mut Child) -> Result<()> {
        let addr = format!("127.0.0.1:{port}");
        let mut last_err = None;
        for _ in 0..50 {
            if let Ok(Some(status)) = child.try_wait() {
                return Err(eyre!(
                    "nats-server process exited before becoming ready (status: {status})"
                ));
            }

            match timeout(
                Duration::from_millis(250),
                tokio::net::TcpStream::connect(&addr),
            )
            .await
            {
                Ok(Ok(_stream)) => {
                    // Connected successfully
                    return Ok(());
                }
                Ok(Err(err)) => {
                    last_err = Some(err);
                    sleep(Duration::from_millis(100)).await;
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Err(eyre!(
            "nats-server at {addr} did not become ready: {:?}",
            last_err
        ))
    }
}

impl Drop for EphemeralNats {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.process.try_lock() {
            if let Some(mut child) = guard.take() {
                let _ = child.start_kill();
                // Synchronously reap the child to prevent zombie processes.
                // try_wait() is non-blocking on tokio::process::Child.
                // If the process hasn't exited yet, spawn an OS thread (NOT a
                // tokio task — the runtime may be shutting down) to poll until reaped.
                match child.try_wait() {
                    Ok(Some(_)) => {} // Already exited, reaped
                    _ => {
                        // SIGKILL sent but process not yet exited — poll in OS thread
                        std::thread::spawn(move || {
                            for _ in 0..40 {
                                match child.try_wait() {
                                    Ok(Some(_)) => return,
                                    _ => std::thread::sleep(std::time::Duration::from_millis(50)),
                                }
                            }
                            // After 2s of polling, give up. Process will be reaped
                            // when the test process exits (init inherits zombies).
                        });
                    }
                }
            }
        } else {
            // Lock is contended (e.g., shutdown hook running concurrently).
            // Spawn an OS thread (not tokio task) to kill the process, so it
            // works even when the tokio runtime is shutting down.
            let process = Arc::clone(&self.process);
            std::thread::spawn(move || {
                // Build a small runtime just for this cleanup
                if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    rt.block_on(async move {
                        let mut guard = process.lock().await;
                        if let Some(mut child) = guard.take() {
                            let _ = child.start_kill();
                            let _ =
                                tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
                        }
                    });
                }
            });
        }
    }
}

#[derive(Clone, Copy)]
struct ChaosConfig {
    latency: Duration,
    failure_rate: f64,
}

impl ChaosConfig {
    async fn simulate_latency(&self) {
        if !self.latency.is_zero() {
            sleep(self.latency).await;
        }
    }

    fn maybe_fail(&self, message: &str) -> Result<()> {
        if self.failure_rate > 0.0 {
            let mut rng = rand::rng();
            if rng.random_bool(self.failure_rate) {
                return Err(eyre!("{}", message));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SharedNatsProfile {
    Default,
    SecureTls,
}

impl SharedNatsProfile {
    fn key(self) -> &'static str {
        match self {
            SharedNatsProfile::Default => "default",
            SharedNatsProfile::SecureTls => "secure-tls",
        }
    }

    pub(crate) fn builder(self) -> EphemeralNatsBuilder {
        match self {
            SharedNatsProfile::Default => EphemeralNats::builder(),
            SharedNatsProfile::SecureTls => {
                let fixture_dir =
                    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/fixtures/tls");
                let fixture_dir = fixture_dir.canonicalize().unwrap_or(fixture_dir);
                EphemeralNats::builder().with_tls_fixtures_path(fixture_dir)
            }
        }
    }
}

async fn get_or_init_shared(id: &str, builder: EphemeralNatsBuilder) -> Result<Arc<EphemeralNats>> {
    // Hold the lock across check + spawn + insert to prevent TOCTOU race.
    // The spawn takes a few seconds, but shared NATS init is rare (once per profile per test run)
    // and correctness is more important than parallelism here.
    let mut guard = SHARED_NATS_REGISTRY.lock().await;
    if let Some(existing) = guard.get(id).cloned() {
        return Ok(existing);
    }

    let instance = Arc::new(builder.start().await?);
    guard.insert(id.to_string(), instance.clone());
    Ok(instance)
}

/// Obtain (or lazily start) a shared `EphemeralNats` instance for the given profile.
pub async fn shared_ephemeral_nats(profile: SharedNatsProfile) -> Result<Arc<EphemeralNats>> {
    let builder = profile.builder();
    get_or_init_shared(profile.key(), builder).await
}

/// Obtain (or lazily start) a shared `EphemeralNats` instance with a custom key.
/// Use `reset_shared_ephemeral_nats` if you need to replace an existing key.
pub async fn shared_ephemeral_nats_with_key(
    key: &str,
    builder: EphemeralNatsBuilder,
) -> Result<Arc<EphemeralNats>> {
    let key = key.trim();
    if key.is_empty() {
        return Err(eyre!("shared NATS key must be non-empty"));
    }
    get_or_init_shared(key, builder).await
}

/// Clear cached shared NATS instances so tests can start with fresh configs.
pub async fn reset_shared_ephemeral_nats() -> Result<()> {
    let instances = {
        let mut guard = SHARED_NATS_REGISTRY.lock().await;
        guard.drain().map(|(_, v)| v).collect::<Vec<_>>()
    };

    for instance in instances {
        instance.shutdown().await?;
    }

    Ok(())
}

/// Ensure the default coordination buckets exist for tests.
pub async fn ensure_coordination_buckets(js: &jetstream::Context) -> Result<()> {
    const LEADERSHIP_TTL_SECS: u64 = 15;

    let env = sinex_primitives::environment::environment();
    super::create_or_open_kv_store(
        js,
        jetstream::kv::Config {
            bucket: format!("KV_{}", env.nats_kv_bucket_name("sinex_instances")),
            history: 1,
            ..Default::default()
        },
    )
    .await?;

    super::create_or_open_kv_store(
        js,
        jetstream::kv::Config {
            bucket: format!("KV_{}", env.nats_kv_bucket_name("sinex_leadership")),
            history: 5,
            max_age: Duration::from_secs(LEADERSHIP_TTL_SECS),
            ..Default::default()
        },
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_log_tail_surfaces_read_failures() -> Result<()> {
        let store = TempDir::new()?;
        let missing = store.path().join("missing.log");
        let nats = EphemeralNats {
            process: Arc::new(AsyncMutex::new(None)),
            url: "127.0.0.1:4222".to_string(),
            _store: store,
            log_path: Some(missing.clone()),
            chaos: None,
            stream_prefix: None,
            tls: None,
            token: None,
        };

        let err = nats.log_tail(20).expect_err("missing log should surface an error");
        assert!(
            err.to_string().contains("failed to read NATS log"),
            "unexpected error: {err:#}"
        );
        Ok(())
    }
}
