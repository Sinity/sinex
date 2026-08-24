use color_eyre::eyre::{Result, WrapErr, bail};
use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

const NATS_JETSTREAM_MAX_MEM: &str = "64MB";
const NATS_JETSTREAM_MAX_FILE: &str = "16GB";

#[derive(Debug, Clone)]
pub struct NatsConfig {
    pub port: u16,
    pub config_file: PathBuf,
    pub data_dir: PathBuf,
    pub log_file: PathBuf,
}

pub struct NatsManager {
    config: NatsConfig,
}

impl NatsManager {
    #[must_use]
    pub fn new(config: NatsConfig) -> Self { Self { config } }

    pub fn generate_config(&self) -> Result<()> {
        let store_dir = self.config.data_dir.join("jetstream");
        let config = format!(
            "# AgentCTL lease-owned Sinex development NATS\nhost = \"127.0.0.1\"\nport = {}\njetstream {{\n    store_dir = \"{}\"\n    max_mem = {}\n    max_file = {}\n}}\n",
            self.config.port, store_dir.display(), NATS_JETSTREAM_MAX_MEM, NATS_JETSTREAM_MAX_FILE,
        );
        if self.config.config_file.exists() && fs::read_to_string(&self.config.config_file).ok().as_deref() == Some(&config) {
            return Ok(());
        }
        fs::create_dir_all(&store_dir)?;
        if let Some(parent) = self.config.config_file.parent() { fs::create_dir_all(parent)?; }
        fs::write(&self.config.config_file, config)?;
        Ok(())
    }

    pub fn start(&self, verbose: bool) -> Result<()> {
        if self.is_ready() {
            bail!("AgentCTL leased NATS port {} is already accepting connections", self.config.port);
        }
        if let Some(parent) = self.config.log_file.parent() { fs::create_dir_all(parent)?; }
        let log = fs::File::create(&self.config.log_file)?;
        let mut command = match std::env::var("NATS_SERVER_BIN") {
            Ok(path) => Command::new(path),
            Err(_) => Command::new("nats-server"),
        };
        let mut child = command.arg("-js").arg("-c").arg(&self.config.config_file)
            .stdout(log.try_clone()?).stderr(log).spawn().wrap_err("start lease-owned NATS")?;
        for _ in 0..30 {
            if self.is_ready() {
                if verbose { println!("NATS is ready on lease port {}", self.config.port); }
                return Ok(());
            }
            if let Some(status) = child.try_wait()? {
                bail!("NATS exited before readiness ({status}); inspect {}", self.config.log_file.display());
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        bail!("NATS did not bind lease port {} within 15 seconds; inspect {}", self.config.port, self.config.log_file.display())
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        TcpStream::connect_timeout(&SocketAddr::from(([127, 0, 0, 1], self.config.port)), Duration::from_millis(200)).is_ok()
    }

}

#[cfg(test)]
#[path = "nats_test.rs"]
mod tests;
