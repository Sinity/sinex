use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_nats::{
    jetstream::{
        self,
        consumer::PullConsumer,
        consumer::{pull::Config as ConsumerConfig, AckPolicy},
    },
    Client,
};
use color_eyre::eyre::{eyre, Result};
use rand::Rng;
use tempfile::TempDir;
use tokio::{
    process::{Child, Command},
    time::{sleep, timeout, Instant},
};
use tokio_stream::StreamExt;
use which::which;

/// Ephemeral JetStream-enabled NATS server spawned for tests.
pub struct EphemeralNats {
    process: Option<Child>,
    url: String,
    _store: TempDir,
    chaos: Option<ChaosConfig>,
    stream_prefix: Option<String>,
}

impl EphemeralNats {
    /// Start a fresh NATS server with JetStream enabled on a random localhost port.
    pub async fn start() -> Result<Self> {
        let binary = Self::resolve_binary()?;

        let port = Self::reserve_port()?;
        let url = format!("127.0.0.1:{port}");

        let store_dir = TempDir::new()?;
        tokio::fs::create_dir_all(store_dir.path()).await?;

        let mut child = Command::new(binary)
            .arg("--jetstream")
            .arg("--store_dir")
            .arg(store_dir.path())
            .arg("--port")
            .arg(port.to_string())
            .arg("--http_port")
            .arg("0")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?;

        Self::wait_for_ready(&url).await.map_err(|err| {
            let _ = child.start_kill();
            err
        })?;

        Ok(Self {
            process: Some(child),
            url,
            _store: store_dir,
            chaos: None,
            stream_prefix: None,
        })
    }

    /// Return the client URL (e.g. `127.0.0.1:4222`).
    pub fn client_url(&self) -> &str {
        &self.url
    }

    /// Connect an async-nats client to this server.
    pub async fn connect(&self) -> Result<Client> {
        if let Some(cfg) = &self.chaos {
            cfg.simulate_latency().await;
            cfg.maybe_fail("simulated connection failure")?;
        }
        async_nats::connect(&self.url)
            .await
            .map_err(|err| eyre!("failed to connect to NATS at {}: {err}", self.url))
    }

    /// Attach chaos settings (latency + failure rate) to this server instance.
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
    pub fn stream_prefix(&self) -> Option<&str> {
        self.stream_prefix.as_deref()
    }

    /// Apply the configured stream prefix (if any) to a name.
    pub fn qualify(&self, name: &str) -> String {
        if let Some(prefix) = &self.stream_prefix {
            format!("{prefix}{name}")
        } else {
            name.to_string()
        }
    }

    /// Create a JetStream context bound to this server.
    pub async fn jetstream(&self) -> Result<jetstream::Context> {
        let client = self.connect().await?;
        Ok(jetstream::new(client))
    }

    /// Create a JetStream context using an existing client connection.
    /// This keeps tests coupled to `EphemeralNats` while avoiding extra connections.
    pub fn jetstream_with_client(&self, client: Client) -> jetstream::Context {
        jetstream::new(client)
    }

    /// Create or update a JetStream stream with the provided subjects.
    pub async fn create_stream(&self, name: &str, subjects: &[&str]) -> Result<()> {
        let js = self.jetstream().await?;
        let config = jetstream::stream::Config {
            name: name.to_string(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        };
        js.get_or_create_stream(config)
            .await
            .map_err(|err| eyre!("failed to create stream {name}: {err}"))?;
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
    /// max_deliver=10) for simple test setups.
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
                let msg = err.to_string();
                if msg.contains("stream name already in use") || msg.contains("subjects overlap") {
                    // Treat overlapping config as non-fatal in shared NATS instances.
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
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

    /// Wait for a stream to become available on this JetStream server.
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

    async fn wait_for_ready(url: &str) -> Result<()> {
        let mut last_err = None;
        for _ in 0..50 {
            match async_nats::connect(url).await {
                Ok(client) => {
                    drop(client);
                    return Ok(());
                }
                Err(err) => {
                    last_err = Some(err);
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Err(eyre!(
            "nats-server at {url} did not become ready: {:?}",
            last_err
        ))
    }
}

impl Drop for EphemeralNats {
    fn drop(&mut self) {
        if let Some(mut child) = self.process.take() {
            let _ = child.start_kill();
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
            let mut rng = rand::thread_rng();
            if rng.gen_bool(self.failure_rate) {
                return Err(eyre!("{}", message));
            }
        }
        Ok(())
    }
}
