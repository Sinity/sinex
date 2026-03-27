//! Stack configuration and status tracking.

use color_eyre::eyre::{Result, WrapErr, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::infra::services::nats::{NatsConfig as SharedNatsConfig, NatsManager};
use crate::infra::services::postgres::{PostgresConfig as SharedPgConfig, PostgresManager};
use crate::infra::state::CheckoutState;

/// Stack configuration, uses per-checkout state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackConfig {
    pub state_dir: PathBuf,
    pub postgres: PostgresConfig,
    pub nats: NatsConfig,
    pub annex: AnnexConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub port: u16,
    pub database: String,
    pub user: String,
    pub superuser: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsConfig {
    pub port: u16,
    pub jetstream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnexConfig {
    pub enable: bool,
    pub backend: String,
}

impl StackConfig {
    /// Create config for the current checkout with per-checkout state
    pub fn for_current_checkout() -> Result<Self> {
        let checkout_state = CheckoutState::for_current_checkout()?;
        Ok(Self::from_checkout_state(&checkout_state))
    }

    /// Create config from a `CheckoutState`
    #[must_use]
    pub fn from_checkout_state(state: &CheckoutState) -> Self {
        // Use fixed ports - no conflicts between checkouts because each has isolated
        // Unix socket directory. TCP is disabled (listen_addresses='') so port is
        // only used in socket filename (.s.PGSQL.5432)
        let nats_port = Self::nats_port_for_checkout(state.checkout_root());

        Self {
            state_dir: state.state_dir().to_path_buf(),
            postgres: PostgresConfig {
                port: 5432, // PostgreSQL default - only used in Unix socket filename (TCP disabled)
                database: "sinex_dev".to_string(),
                user: std::env::var("USER").unwrap_or_else(|_| "sinity".to_string()),
                superuser: "postgres".to_string(),
            },
            nats: NatsConfig {
                port: nats_port,
                jetstream: true,
            },
            annex: AnnexConfig {
                enable: true,
                backend: "SHA256E".to_string(),
            },
        }
    }

    /// Generate a port offset based on checkout path hash (0-99)
    fn port_offset_for_checkout(checkout_root: &Path) -> u16 {
        let digest = Sha256::digest(checkout_root.to_string_lossy().as_bytes());
        u16::from(digest[0]) % 100
    }

    fn nats_port_for_checkout(checkout_root: &Path) -> u16 {
        4222 + Self::port_offset_for_checkout(checkout_root)
    }

    /// Derived paths
    #[must_use]
    pub fn data_dir(&self) -> PathBuf {
        self.state_dir.join("data")
    }
    #[must_use]
    pub fn run_dir(&self) -> PathBuf {
        self.state_dir.join("run")
    }
    #[must_use]
    pub fn logs_dir(&self) -> PathBuf {
        self.run_dir().join("logs")
    }
    #[must_use]
    pub fn snapshots_dir(&self) -> PathBuf {
        self.state_dir.join("snapshots")
    }
    #[must_use]
    pub fn config_dir(&self) -> PathBuf {
        self.state_dir.join("config")
    }
    #[must_use]
    pub fn pg_data(&self) -> PathBuf {
        self.data_dir().join("postgres")
    }
    #[must_use]
    pub fn nats_data(&self) -> PathBuf {
        self.data_dir().join("nats")
    }
    #[must_use]
    pub fn annex_data(&self) -> PathBuf {
        self.data_dir().join("annex")
    }
    #[must_use]
    pub fn pg_pid_file(&self) -> PathBuf {
        self.pg_data().join("postmaster.pid")
    }
    #[must_use]
    pub fn nats_pid_file(&self) -> PathBuf {
        self.run_dir().join("nats.pid")
    }
    #[must_use]
    pub fn nats_config(&self) -> PathBuf {
        self.config_dir().join("nats").join("nats.conf")
    }

    #[must_use]
    pub fn database_url(&self) -> String {
        format!(
            "postgresql:///{}?host={}",
            self.postgres.database,
            self.run_dir().display()
        )
    }

    #[must_use]
    pub fn nats_url(&self) -> String {
        let port = self.nats.port;
        format!("nats://localhost:{port}")
    }

    #[must_use]
    pub fn to_shared_pg(&self) -> SharedPgConfig {
        SharedPgConfig {
            port: self.postgres.port,
            data_dir: self.pg_data(),
            run_dir: self.run_dir(),
            logs_dir: self.logs_dir(),
            database: self.postgres.database.clone(),
            superuser: self.postgres.superuser.clone(),
            app_user: self.postgres.user.clone(),
        }
    }

    #[must_use]
    pub fn to_shared_nats(&self) -> SharedNatsConfig {
        SharedNatsConfig {
            port: self.nats.port,
            config_file: self.nats_config(),
            data_dir: self.nats_data(),
            pid_file: self.nats_pid_file(),
            log_file: self.logs_dir().join("nats.log"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stack Status
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct StackStatus {
    pub initialized: bool,
    pub postgres: ServiceStatus,
    pub nats: ServiceStatus,
    pub annex: AnnexStatus,
    pub data_sizes: DataSizes,
    pub snapshots: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ServiceStatus {
    pub running: bool,
    pub pid: Option<u32>,
    pub port: u16,
}

#[derive(Debug, Serialize)]
pub struct AnnexStatus {
    pub initialized: bool,
    pub path: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct DataSizes {
    pub postgres_bytes: u64,
    pub nats_bytes: u64,
    pub annex_bytes: u64,
}

impl StackStatus {
    #[must_use]
    pub fn gather(config: &StackConfig) -> Self {
        let initialized =
            config.state_dir.exists() && (config.pg_data().exists() || config.nats_data().exists());

        let pg_mgr = PostgresManager::new(config.to_shared_pg());
        let nats_mgr = NatsManager::new(config.to_shared_nats());

        let postgres = ServiceStatus {
            running: pg_mgr.is_running(),
            pid: if pg_mgr.is_running() {
                // Read actual PID from postmaster.pid (first line is the postmaster PID)
                std::fs::read_to_string(config.pg_data().join("postmaster.pid"))
                    .ok()
                    .and_then(|c| c.lines().next().and_then(|l| l.trim().parse::<u32>().ok()))
            } else {
                None
            },
            port: config.postgres.port,
        };

        let nats = ServiceStatus {
            running: nats_mgr.is_running(),
            pid: nats_mgr.read_pid(),
            port: config.nats.port,
        };

        let annex = AnnexStatus {
            initialized: config.annex_data().join(".git").exists(),
            path: config.annex_data(),
        };

        let data_sizes = DataSizes {
            postgres_bytes: dir_size(&config.pg_data()),
            nats_bytes: dir_size(&config.nats_data()),
            annex_bytes: dir_size(&config.annex_data()),
        };

        let snapshots = list_snapshots(&config.snapshots_dir());

        Self {
            initialized,
            postgres,
            nats,
            annex,
            data_sizes,
            snapshots,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stack Operations (Helpers)
// ─────────────────────────────────────────────────────────────────────────────

pub fn ensure_directories(config: &StackConfig) -> Result<()> {
    fs::create_dir_all(config.config_dir().join("nats"))?;
    fs::create_dir_all(config.pg_data())?;
    fs::create_dir_all(config.nats_data())?;
    fs::create_dir_all(config.nats_data().join("jetstream"))?;
    fs::create_dir_all(config.annex_data())?;
    fs::create_dir_all(config.run_dir())?;
    fs::create_dir_all(config.logs_dir())?;
    fs::create_dir_all(config.snapshots_dir())?;
    Ok(())
}

fn summarize_command_output(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        return stderr;
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        return stdout;
    }

    format!("exit status {}", output.status)
}

fn probe_annex_available(output: std::io::Result<std::process::Output>) -> Result<bool> {
    match output {
        Ok(output) if output.status.success() => Ok(true),
        Ok(output) => {
            bail!(
                "git-annex version probe failed: {}",
                summarize_command_output(&output)
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).wrap_err("failed to run git-annex version probe"),
    }
}

fn require_successful_command(
    description: &str,
    output: std::io::Result<std::process::Output>,
) -> Result<()> {
    let output = output.wrap_err_with(|| format!("failed to run {description}"))?;
    if !output.status.success() {
        bail!(
            "{description} failed: {}",
            summarize_command_output(&output)
        );
    }
    Ok(())
}

pub fn annex_init(config: &StackConfig, verbose: bool) -> Result<()> {
    if config.annex_data().join(".git").exists() {
        if verbose {
            println!("Git-annex repository already initialized");
        }
        return Ok(());
    }

    if !probe_annex_available(Command::new("git-annex").arg("version").output())? {
        if verbose {
            println!("git-annex not found, skipping annex initialization");
        }
        return Ok(());
    }

    if verbose {
        println!("Initializing git-annex repository...");
    }

    fs::create_dir_all(config.annex_data())?;

    require_successful_command(
        "git init for annex repository",
        Command::new("git")
            .args(["init"])
            .current_dir(config.annex_data())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output(),
    )?;

    require_successful_command(
        "git-annex init for annex repository",
        Command::new("git-annex")
            .args(["init", "sinex-dev-isolated"])
            .current_dir(config.annex_data())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output(),
    )?;

    require_successful_command(
        "git config annex.thin",
        Command::new("git")
            .args(["config", "annex.thin", "true"])
            .current_dir(config.annex_data())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output(),
    )?;

    require_successful_command(
        "git config annex.backend",
        Command::new("git")
            .args(["config", "annex.backend", &config.annex.backend])
            .current_dir(config.annex_data())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output(),
    )?;

    if verbose {
        println!("Git-annex initialized");
    }

    Ok(())
}

#[must_use]
pub fn pg_bin(binary: &str) -> PathBuf {
    if let Ok(prefix) = std::env::var("SINEX_PG_BIN") {
        PathBuf::from(prefix).join(binary)
    } else {
        PathBuf::from(binary)
    }
}

pub fn pg_init(config: &StackConfig, verbose: bool) -> Result<()> {
    let mgr = PostgresManager::new(config.to_shared_pg());
    mgr.init(verbose)
}

pub fn pg_start(config: &StackConfig, verbose: bool) -> Result<()> {
    let mgr = PostgresManager::new(config.to_shared_pg());
    mgr.start(verbose)
}

pub fn pg_stop(config: &StackConfig, verbose: bool) -> Result<()> {
    let mgr = PostgresManager::new(config.to_shared_pg());
    mgr.stop(verbose)
}

pub fn pg_setup_database(config: &StackConfig, verbose: bool) -> Result<()> {
    let mgr = PostgresManager::new(config.to_shared_pg());
    // Always use "postgres" as the initial user — initdb creates this superuser via -U postgres,
    // regardless of which OS user is running the process (root, sinity, etc.)
    let initial_user = config.postgres.superuser.clone();

    mgr.ensure_user(&config.postgres.superuser, true, &initial_user)?;
    mgr.ensure_user(&config.postgres.user, true, &config.postgres.superuser)?;
    mgr.ensure_db(
        &config.postgres.database,
        &config.postgres.user,
        &config.postgres.superuser,
    )?;

    if verbose {
        println!("Enabling PostgreSQL extensions...");
    }

    mgr.install_extensions(&config.postgres.database, &config.postgres.superuser)?;

    if verbose {
        println!("Database setup complete");
    }

    Ok(())
}

/// Apply declarative database schema to an explicit database URL.
///
/// Uses `block_in_place` since this is called from sync command contexts
/// but needs to call async `apply_schema_for_url`.
pub fn apply_schema_for_database_url(database_url: &str, verbose: bool) -> Result<()> {
    if verbose {
        println!("Applying declarative database schema...");
    }

    let handle = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| handle.block_on(sinex_db::apply_schema_for_url(database_url)))
        .map_err(|e| color_eyre::eyre::eyre!("{e}"))
        .context("Failed to apply declarative schema")?;

    if verbose {
        println!("Schema apply complete");
    }

    Ok(())
}

/// Apply declarative database schema using the current stack configuration.
pub fn pg_apply_schema(config: &StackConfig, verbose: bool) -> Result<()> {
    apply_schema_for_database_url(&config.database_url(), verbose)
}

#[must_use]
pub fn nats_bin() -> PathBuf {
    if let Ok(path) = std::env::var("NATS_SERVER_BIN") {
        PathBuf::from(path)
    } else {
        PathBuf::from("nats-server")
    }
}

pub fn nats_generate_config(config: &StackConfig, _verbose: bool) -> Result<()> {
    let mgr = NatsManager::new(config.to_shared_nats());
    mgr.generate_config()
}

pub fn nats_start(config: &StackConfig, verbose: bool) -> Result<()> {
    let mgr = NatsManager::new(config.to_shared_nats());
    mgr.start(verbose)
}

pub fn nats_stop(config: &StackConfig, verbose: bool) -> Result<()> {
    let mgr = NatsManager::new(config.to_shared_nats());
    mgr.stop(verbose)
}

// ─────────────────────────────────────────────────────────────────────────────
// Utility Functions (Local copies to avoid import cycles / shared utils)
// ─────────────────────────────────────────────────────────────────────────────

#[must_use]
pub fn dir_size(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    walkdir::WalkDir::new(path)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter_map(|e| e.metadata().ok())
        .filter(std::fs::Metadata::is_file)
        .map(|m| m.len())
        .sum()
}

// Re-export list_snapshots if needed by commands (it was used in Status)
// or move it to crate::utils if it's generic enough. It seems specific to stack layout.
#[must_use]
pub fn list_snapshots(dir: &Path) -> Vec<String> {
    if !dir.exists() {
        return vec![];
    }
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(std::result::Result::ok)
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().to_string();
                    if name.ends_with(".tar.zst") {
                        Some(name.trim_end_matches(".tar.zst").to_string())
                    } else if name.ends_with(".sql.zst") {
                        Some(name.trim_end_matches(".sql.zst").to_string())
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::StackConfig;
    use super::{probe_annex_available, require_successful_command};
    use std::path::Path;
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn nats_port_matches_flake_hash_for_sinex_checkout() {
        let checkout = Path::new("/realm/project/sinex");
        assert_eq!(StackConfig::port_offset_for_checkout(checkout), 86);
        assert_eq!(StackConfig::nats_port_for_checkout(checkout), 4308);
    }

    #[test]
    fn probe_annex_available_treats_missing_binary_as_absent() {
        let available = probe_annex_available(Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "missing",
        )))
        .unwrap();
        assert!(!available);
    }

    #[test]
    fn probe_annex_available_reports_nonzero_status() {
        let error = probe_annex_available(Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: b"git-annex broken".to_vec(),
        }))
        .unwrap_err();
        assert!(format!("{error:#}").contains("git-annex broken"));
    }

    #[test]
    fn require_successful_command_reports_failure_output() {
        let error = require_successful_command(
            "git init for annex repository",
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(1 << 8),
                stdout: Vec::new(),
                stderr: b"permission denied".to_vec(),
            }),
        )
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("permission denied"));
        assert!(message.contains("git init for annex repository"));
    }
}
