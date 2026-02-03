use anyhow::{bail, Context, Result};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

#[derive(Debug, Clone)]
pub struct NatsConfig {
    pub port: u16,
    pub config_file: PathBuf,
    pub data_dir: PathBuf,
    pub pid_file: PathBuf,
    pub log_file: PathBuf,
}

pub struct NatsManager {
    config: NatsConfig,
}

impl NatsManager {
    #[must_use]
    pub fn new(config: NatsConfig) -> Self {
        Self { config }
    }

    pub fn generate_config(&self) -> Result<()> {
        if self.config.config_file.exists() {
            return Ok(());
        }

        if let Some(parent) = self.config.config_file.parent() {
            fs::create_dir_all(parent)?;
        }

        let store_dir = self.config.data_dir.join("jetstream");
        // Ensure store dir exists, though NATS might create it
        fs::create_dir_all(&store_dir)?;

        let nats_conf = format!(
            r#"# sinex-dev / sandbox NATS configuration
port = {}
jetstream {{
    store_dir = "{}"
    max_mem = 256MB
    max_file = 1GB
}}
"#,
            self.config.port,
            store_dir.display()
        );

        fs::write(&self.config.config_file, nats_conf)?;
        Ok(())
    }

    pub fn start(&self, verbose: bool) -> Result<()> {
        if self.is_running() {
            if verbose {
                println!("NATS already running");
            }
            return Ok(());
        }

        if verbose {
            println!("Starting NATS on port {}...", self.config.port);
        }

        if let Some(parent) = self.config.log_file.parent() {
            fs::create_dir_all(parent)?;
        }
        let log_file = fs::File::create(&self.config.log_file)?;

        let child = self
            .nats_command()
            .args(["-js", "-c", self.config.config_file.to_str().unwrap()])
            .stdout(log_file.try_clone()?)
            .stderr(log_file)
            .spawn()
            .context("Failed to start NATS")?;

        if let Some(parent) = self.config.pid_file.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.config.pid_file, child.id().to_string())?;

        // Wait for port
        for _ in 0..30 {
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", self.config.port)).is_ok() {
                if verbose {
                    println!("NATS started");
                }
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }

        bail!("NATS failed to start within 15 seconds")
    }

    pub fn stop(&self, verbose: bool) -> Result<()> {
        if !self.is_running() {
            if verbose {
                println!("NATS not running");
            }
            // Cleanup pid file if it exists but process doesn't
            if self.config.pid_file.exists() {
                let _ = fs::remove_file(&self.config.pid_file);
            }
            return Ok(());
        }

        if verbose {
            println!("Stopping NATS...");
        }

        if let Some(pid) = self.read_pid() {
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
            // Wait for exit
            for _ in 0..40 {
                if !self.is_running_pid(pid) {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
        }

        let _ = fs::remove_file(&self.config.pid_file);

        if verbose {
            println!("NATS stopped");
        }

        Ok(())
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        if let Some(pid) = self.read_pid() {
            self.is_running_pid(pid)
        } else {
            false
        }
    }

    fn read_pid(&self) -> Option<u32> {
        fs::read_to_string(&self.config.pid_file)
            .ok()
            .and_then(|s| s.trim().parse().ok())
    }

    fn is_running_pid(&self, pid: u32) -> bool {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }

    fn nats_command(&self) -> Command {
        if let Ok(path) = std::env::var("NATS_SERVER_BIN") {
            Command::new(path)
        } else {
            Command::new("nats-server")
        }
    }
}
