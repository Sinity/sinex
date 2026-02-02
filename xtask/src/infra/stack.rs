//! Stack configuration and status tracking.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

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
        self.run_dir().join("postgres.pid")
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
        format!("nats://localhost:{}", self.nats.port)
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

        let postgres = ServiceStatus {
            running: is_process_running(&config.pg_pid_file()),
            pid: read_pid(&config.pg_pid_file()),
            port: config.postgres.port,
        };

        let nats = ServiceStatus {
            running: is_process_running(&config.nats_pid_file()),
            pid: read_pid(&config.nats_pid_file()),
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
    if config.pg_data().join("PG_VERSION").exists() {
        if verbose {
            println!("PostgreSQL data directory already initialized");
        }
        return Ok(());
    }

    if verbose {
        println!("Initializing PostgreSQL data directory...");
    }

    let status = Command::new(pg_bin("initdb"))
        .args(["--auth=trust", "--no-locale", "--encoding=UTF8", "-D"])
        .arg(config.pg_data())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("Failed to run initdb")?;

    if !status.success() {
        bail!("initdb failed with status {status}");
    }

    let conf_path = config.pg_data().join("postgresql.conf");
    let mut conf = fs::OpenOptions::new()
        .append(true)
        .open(conf_path)
        .context("Failed to open postgresql.conf")?;

    writeln!(conf, "\n# sinex-dev isolated configuration")?;
    writeln!(
        conf,
        "unix_socket_directories = '{}'",
        config.run_dir().display()
    )?;
    writeln!(conf, "listen_addresses = ''")?;
    writeln!(conf, "port = {}", config.postgres.port)?;
    writeln!(conf, "max_connections = 200")?;
    writeln!(conf, "shared_preload_libraries = 'timescaledb'")?;
    writeln!(conf, "log_destination = 'stderr'")?;
    writeln!(conf, "logging_collector = on")?;
    writeln!(conf, "log_directory = '{}'", config.logs_dir().display())?;
    writeln!(conf, "log_filename = 'postgres.log'")?;

    if verbose {
        println!("PostgreSQL initialized");
    }

    Ok(())
}

pub fn pg_start(config: &StackConfig, verbose: bool) -> Result<()> {
    if is_process_running(&config.pg_pid_file()) {
        if verbose {
            println!("PostgreSQL already running");
        }
        return Ok(());
    }

    if verbose {
        println!("Starting PostgreSQL on port {}...", config.postgres.port);
    }

    let log_path = config.logs_dir().join("postgres.log");

    let status = Command::new(pg_bin("pg_ctl"))
        .args(["-D", config.pg_data().to_str().unwrap(), "start", "-w"])
        .arg("-l")
        .arg(log_path)
        .arg("-o")
        .arg(format!(
            "-k {} -p {}",
            config.run_dir().display(),
            config.postgres.port
        ))
        .status()
        .context("Failed to start PostgreSQL")?;

    if !status.success() {
        bail!("pg_ctl start failed with status {status}");
    }

    if let Ok(content) = fs::read_to_string(config.pg_data().join("postmaster.pid")) {
        if let Some(first_line) = content.lines().next() {
            fs::write(config.pg_pid_file(), first_line)?;
        }
    }

    for _ in 0..60 {
        let check = Command::new(pg_bin("pg_isready"))
            .args(["-h", config.run_dir().to_str().unwrap()])
            .args(["-p", config.postgres.port.to_string().as_str()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        if check.is_ok_and(|s| s.success()) {
            if verbose {
                println!("PostgreSQL started");
            }
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    bail!("PostgreSQL failed to start within 30 seconds")
}

pub fn pg_stop(config: &StackConfig, verbose: bool) -> Result<()> {
    if !is_process_running(&config.pg_pid_file()) {
        if verbose {
            println!("PostgreSQL not running");
        }
        let _ = fs::remove_file(config.pg_pid_file());
        return Ok(());
    }

    if verbose {
        println!("Stopping PostgreSQL...");
    }

    let _ = Command::new(pg_bin("pg_ctl"))
        .args([
            "-D",
            config.pg_data().to_str().unwrap(),
            "stop",
            "-m",
            "fast",
        ])
        .status();

    let _ = fs::remove_file(config.pg_pid_file());

    if verbose {
        println!("PostgreSQL stopped");
    }

    Ok(())
}

pub fn pg_setup_database(config: &StackConfig, verbose: bool) -> Result<()> {
    let initial_user = std::env::var("USER").unwrap_or_else(|_| config.postgres.superuser.clone());

    let psql = |user: &str, db: &str, sql: &str| -> Result<String> {
        let output = Command::new(pg_bin("psql"))
            .args(["-h", config.run_dir().to_str().unwrap()])
            .args(["-p", config.postgres.port.to_string().as_str()])
            .args(["-U", user])
            .args(["-d", db])
            .args(["-tAc", sql])
            .output()
            .context("Failed to run psql")?;

        if !output.status.success() {
            bail!("psql failed: {}", String::from_utf8_lossy(&output.stderr));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    };

    let exists = psql(
        initial_user.as_str(),
        "postgres",
        &format!(
            "SELECT 1 FROM pg_roles WHERE rolname = '{}'",
            config.postgres.superuser
        ),
    )?;
    if exists.is_empty() {
        if verbose {
            println!("Creating superuser role: {}", config.postgres.superuser);
        }
        psql(
            initial_user.as_str(),
            "postgres",
            &format!(
                "CREATE ROLE {} LOGIN SUPERUSER CREATEDB",
                config.postgres.superuser
            ),
        )?;
    }

    let exists = psql(
        config.postgres.superuser.as_str(),
        "postgres",
        &format!(
            "SELECT 1 FROM pg_roles WHERE rolname = '{}'",
            config.postgres.user
        ),
    )?;
    if exists.is_empty() {
        if verbose {
            println!("Creating application role: {}", config.postgres.user);
        }
        psql(
            config.postgres.superuser.as_str(),
            "postgres",
            &format!(
                "CREATE ROLE {} LOGIN SUPERUSER CREATEDB",
                config.postgres.user
            ),
        )?;
    }

    let exists = psql(
        config.postgres.superuser.as_str(),
        "postgres",
        &format!(
            "SELECT 1 FROM pg_database WHERE datname = '{}'",
            config.postgres.database
        ),
    )?;
    if exists.is_empty() {
        if verbose {
            println!("Creating database: {}", config.postgres.database);
        }
        psql(
            config.postgres.superuser.as_str(),
            "postgres",
            &format!(
                "CREATE DATABASE {} OWNER {}",
                config.postgres.database, config.postgres.user
            ),
        )?;
    }

    if verbose {
        println!("Enabling PostgreSQL extensions...");
    }
    for ext in &["timescaledb", "vector", "pg_jsonschema"] {
        let _ = psql(
            &config.postgres.superuser,
            &config.postgres.database,
            &format!("CREATE EXTENSION IF NOT EXISTS {ext}"),
        );
    }
    let _ = psql(
        &config.postgres.superuser,
        &config.postgres.database,
        "CREATE EXTENSION IF NOT EXISTS pgx_ulid",
    )
    .or_else(|_| {
        psql(
            &config.postgres.superuser,
            &config.postgres.database,
            "CREATE EXTENSION IF NOT EXISTS ulid",
        )
    });

    if verbose {
        println!("Database setup complete");
    }

    Ok(())
}

pub fn pg_run_migrations(config: &StackConfig, verbose: bool) -> Result<()> {
    if verbose {
        println!("Running database migrations...");
    }

    let status = Command::new("cargo")
        .args([
            "run",
            "--manifest-path",
            "crate/lib/sinex-schema/Cargo.toml",
            "--bin",
            "sinex-schema",
            "--",
            "up",
        ])
        .env("DATABASE_URL", config.database_url())
        .status()
        .context("Failed to run migrations")?;

    if !status.success() {
        bail!("Migrations failed with status {status}");
    }

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

pub fn nats_generate_config(config: &StackConfig, verbose: bool) -> Result<()> {
    if config.nats_config().exists() {
        if verbose {
            println!("NATS config already exists");
        }
        return Ok(());
    }

    if verbose {
        println!("Generating NATS configuration...");
    }

    fs::create_dir_all(config.nats_config().parent().unwrap())?;

    let nats_conf = format!(
        r#"# sinex-dev isolated NATS configuration
port = {}
jetstream {{
    store_dir = "{}"
    max_mem = 256MB
    max_file = 1GB
}}
"#,
        config.nats.port,
        config.nats_data().join("jetstream").display()
    );

    fs::write(config.nats_config(), nats_conf)?;

    if verbose {
        println!("NATS configuration generated");
    }

    Ok(())
}

pub fn nats_start(config: &StackConfig, verbose: bool) -> Result<()> {
    if is_process_running(&config.nats_pid_file()) {
        if verbose {
            println!("NATS already running");
        }
        return Ok(());
    }

    if verbose {
        println!("Starting NATS on port {}...", config.nats.port);
    }

    let log_path = config.logs_dir().join("nats.log");
    let log_file = fs::File::create(&log_path)?;

    let child = Command::new(nats_bin())
        .args(["-js", "-c", config.nats_config().to_str().unwrap()])
        .stdout(log_file.try_clone()?)
        .stderr(log_file)
        .spawn()
        .context("Failed to start NATS")?;

    fs::write(config.nats_pid_file(), child.id().to_string())?;

    for _ in 0..30 {
        let check = std::net::TcpStream::connect(format!("127.0.0.1:{}", config.nats.port));
        if check.is_ok() {
            if verbose {
                println!("NATS started");
            }
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    bail!("NATS failed to start within 15 seconds")
}

pub fn nats_stop(config: &StackConfig, verbose: bool) -> Result<()> {
    if !is_process_running(&config.nats_pid_file()) {
        if verbose {
            println!("NATS not running");
        }
        let _ = fs::remove_file(config.nats_pid_file());
        return Ok(());
    }

    if verbose {
        println!("Stopping NATS...");
    }

    if let Some(pid) = read_pid(&config.nats_pid_file()) {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
        for _ in 0..40 {
            if !is_process_running(&config.nats_pid_file()) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
    }

    let _ = fs::remove_file(config.nats_pid_file());

    if verbose {
        println!("NATS stopped");
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Utility Functions (Local copies to avoid import cycles / shared utils)
// ─────────────────────────────────────────────────────────────────────────────

#[must_use]
pub fn is_process_running(pid_file: &Path) -> bool {
    read_pid(pid_file).is_some_and(|pid| unsafe { libc::kill(pid as i32, 0) == 0 })
}

#[must_use]
pub fn read_pid(pid_file: &Path) -> Option<u32> {
    fs::read_to_string(pid_file)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

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
