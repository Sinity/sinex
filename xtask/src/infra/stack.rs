//! Stack configuration and status tracking.

use color_eyre::eyre::{Result, WrapErr};
use serde::{Deserialize, Serialize};
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

        // Prefer NATS port from Nix (flake.nix computes it), fallback to hash computation
        // This ensures port is available even when xtask doesn't compile
        let nats_port = std::env::var("SINEX_DEV_NATS_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or_else(|| {
                let offset = Self::port_offset_for_checkout(state.checkout_root());
                4222 + offset
            });

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
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        checkout_root.hash(&mut hasher);
        (hasher.finish() % 100) as u16
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
            "postgresql:///{}?host={}&port={}",
            self.postgres.database,
            self.run_dir().display(),
            self.postgres.port
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
            pid: if pg_mgr.is_running() { Some(1) } else { None }, // Simplified PID check
            port: config.postgres.port,
        };

        let nats = ServiceStatus {
            running: nats_mgr.is_running(),
            pid: if nats_mgr.is_running() { Some(1) } else { None }, // Simplified PID check
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

pub fn annex_init(config: &StackConfig, verbose: bool) -> Result<()> {
    if config.annex_data().join(".git").exists() {
        if verbose {
            println!("Git-annex repository already initialized");
        }
        return Ok(());
    }

    if Command::new("git-annex").arg("version").output().is_err() {
        if verbose {
            println!("git-annex not found, skipping annex initialization");
        }
        return Ok(());
    }

    if verbose {
        println!("Initializing git-annex repository...");
    }

    fs::create_dir_all(config.annex_data())?;

    let _ = Command::new("git")
        .args(["init"])
        .current_dir(config.annex_data())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    let _ = Command::new("git-annex")
        .args(["init", "sinex-dev-isolated"])
        .current_dir(config.annex_data())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    let _ = Command::new("git")
        .args(["config", "annex.thin", "true"])
        .current_dir(config.annex_data())
        .status();

    let _ = Command::new("git")
        .args(["config", "annex.backend", &config.annex.backend])
        .current_dir(config.annex_data())
        .status();

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

/// Run database migrations using sinex-db's in-process migrator.
///
/// Uses `block_in_place` since this is called from sync infra start context
/// but needs to call async `run_migrations_for_url`.
pub fn pg_run_migrations(config: &StackConfig, verbose: bool) -> Result<()> {
    if verbose {
        println!("Running database migrations...");
    }

    let handle = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| {
        handle.block_on(sinex_db::run_migrations_for_url(&config.database_url()))
    })
    .map_err(|e| color_eyre::eyre::eyre!("{e}"))
    .context("Failed to run migrations")?;

    if verbose {
        println!("Migrations complete");
    }

    Ok(())
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
