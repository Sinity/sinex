use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_nats::Client;
use color_eyre::eyre::{eyre, Result};
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tokio::time::sleep;
use which::which;

/// Ephemeral JetStream-enabled NATS server spawned for tests.
pub struct EphemeralNats {
    process: Option<Child>,
    url: String,
    _store: TempDir,
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
        })
    }

    /// Return the client URL (e.g. `127.0.0.1:4222`).
    pub fn client_url(&self) -> &str {
        &self.url
    }

    /// Connect an async-nats client to this server.
    pub async fn connect(&self) -> Result<Client> {
        async_nats::connect(&self.url)
            .await
            .map_err(|err| eyre!("failed to connect to NATS at {}: {err}", self.url))
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
        which("nats-server").map_err(|_| eyre!("nats-server binary not found on PATH"))
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
