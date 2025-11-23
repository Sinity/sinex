use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_nats::{
    jetstream::{self, consumer::pull::Config as ConsumerConfig, consumer::PullConsumer},
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

    /// Create a JetStream context bound to this server.
    pub async fn jetstream(&self) -> Result<jetstream::Context> {
        let client = self.connect().await?;
        Ok(jetstream::new(client))
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
