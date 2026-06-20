//! Stack configuration and status tracking.

use color_eyre::eyre::{Result, WrapErr, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sinex_db::repositories::schema_management::{SchemaManagementRepository, SchemaSyncResult};
use sinex_db::schema::apply::SHARED_ACCESS_ROLES;
use sinex_primitives::events::schema_registry::generate_schema_bundle;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::infra::services::nats::{NatsConfig as SharedNatsConfig, NatsManager};
use crate::infra::services::postgres::{
    PostgresConfig as SharedPgConfig, PostgresDurabilityMode, PostgresManager,
};
use crate::infra::state::{CheckoutInventoryRoot, CheckoutState, LockInspection};

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
        Self::from_state_dir(
            state.state_dir().to_path_buf(),
            state.checkout_root(),
            Some(Self::nats_port_for_checkout(state.checkout_root())),
        )
    }

    /// Create config for a discovered dev-state root without mutating it.
    #[must_use]
    pub fn from_inventory_root(root: &CheckoutInventoryRoot) -> Self {
        let checkout = root.checkout_path.as_deref().unwrap_or(&root.cache_root);
        Self::from_state_dir(
            root.dev_state_dir.clone(),
            checkout,
            discover_nats_port(&root.dev_state_dir),
        )
    }

    fn from_state_dir(state_dir: PathBuf, checkout_root: &Path, nats_port: Option<u16>) -> Self {
        // Use fixed ports - no conflicts between checkouts because each has isolated
        // Unix socket directory. TCP is disabled (listen_addresses='') so port is
        // only used in socket filename (.s.PGSQL.5432)
        let nats_port = nats_port.unwrap_or_else(|| Self::nats_port_for_checkout(checkout_root));

        Self {
            state_dir,
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
            // Dev infra connects via Unix socket (DATABASE_URL=postgresql:///...?host=...).
            // Disable TCP to avoid contending with a system Postgres on port 5432.
            listen_addresses: String::new(),
            durability: PostgresDurabilityMode::Durable,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub data_size_issues: Vec<String>,
    pub snapshots: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_issue: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ServiceStatus {
    pub running: bool,
    pub pid: Option<u32>,
    pub pid_state: ServicePidState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_bytes: Option<u64>,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServicePidState {
    Missing,
    Running,
    Stale,
    Malformed,
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

#[derive(Debug, Serialize)]
pub struct AllCheckoutsStatus {
    pub base_dir: PathBuf,
    pub checkouts: Vec<CheckoutInfraStatus>,
    pub totals: AllCheckoutsTotals,
    pub issues: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct CheckoutInfraStatus {
    pub cache_root: PathBuf,
    pub dev_state_dir: PathBuf,
    pub checkout_path: Option<PathBuf>,
    pub checkout_path_exists: Option<bool>,
    pub lock: LockStatus,
    pub initialized: bool,
    pub postgres: ServiceStatus,
    pub nats: ServiceStatus,
    pub sinexd: RuntimeProcessStatus,
    pub data_sizes: DataSizes,
    pub logs_bytes: u64,
    pub total_state_bytes: u64,
    pub data_size_issues: Vec<String>,
    pub remediation: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct LockStatus {
    pub present: bool,
    pub state: LockState,
    pub pid: Option<u32>,
    pub checkout_path: Option<PathBuf>,
    pub description: Option<String>,
    pub issue: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LockState {
    Missing,
    Live,
    Stale,
    Malformed,
}

#[derive(Debug, Serialize)]
pub struct RuntimeProcessStatus {
    pub running: bool,
    pub pids: Vec<u32>,
    pub rss_bytes: u64,
    pub issue: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AllCheckoutsTotals {
    pub checkout_count: usize,
    pub running_postgres: usize,
    pub stale_postgres_pid_files: usize,
    pub running_nats: usize,
    pub stale_nats_pid_files: usize,
    pub running_sinexd: usize,
    pub rss_bytes: u64,
    pub state_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct AllCheckoutsCleanup {
    pub base_dir: PathBuf,
    pub dry_run: bool,
    pub stale_only: bool,
    pub checkouts: Vec<CheckoutCleanup>,
    pub totals: CleanupTotals,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct CheckoutCleanup {
    pub cache_root: PathBuf,
    pub dev_state_dir: PathBuf,
    pub checkout_path: Option<PathBuf>,
    pub actions: Vec<CleanupAction>,
    pub skipped: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct CleanupAction {
    pub action: CleanupActionKind,
    pub target: PathBuf,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CleanupActionKind {
    StopPostgres,
    StopNats,
    RemoveStalePostgresPid,
    RemoveMalformedPostgresPid,
    RemoveStaleNatsPid,
    RemoveMalformedNatsPid,
    RemoveStaleLock,
    RemoveMalformedLock,
}

#[derive(Debug, Default, Serialize)]
pub struct CleanupTotals {
    pub checkouts: usize,
    pub actions: usize,
    pub skipped: usize,
    pub stopped_postgres: usize,
    pub stopped_nats: usize,
    pub removed_files: usize,
}

#[derive(Debug)]
pub struct DirectorySizeProbe {
    pub bytes: u64,
    pub issue: Option<String>,
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
            pid: pg_mgr.read_pid(),
            pid_state: service_pid_state(&config.pg_pid_file()),
            rss_bytes: service_rss_bytes(&config.pg_pid_file(), true),
            port: config.postgres.port,
        };

        let nats = ServiceStatus {
            running: nats_mgr.is_running(),
            pid: nats_mgr.read_pid(),
            pid_state: service_pid_state(&config.nats_pid_file()),
            rss_bytes: service_rss_bytes(&config.nats_pid_file(), false),
            port: config.nats.port,
        };

        let annex = AnnexStatus {
            initialized: config.annex_data().join(".git").exists(),
            path: config.annex_data(),
        };

        let postgres_size = dir_size(&config.pg_data());
        let nats_size = dir_size(&config.nats_data());
        let annex_size = dir_size(&config.annex_data());
        let data_sizes = DataSizes {
            postgres_bytes: postgres_size.bytes,
            nats_bytes: nats_size.bytes,
            annex_bytes: annex_size.bytes,
        };
        let data_size_issues = [postgres_size.issue, nats_size.issue, annex_size.issue]
            .into_iter()
            .flatten()
            .collect();

        let snapshots = list_snapshots(&config.snapshots_dir());

        Self {
            initialized,
            postgres,
            nats,
            annex,
            data_sizes,
            data_size_issues,
            snapshots: snapshots.snapshots,
            snapshot_issue: snapshots.issue,
        }
    }
}

impl AllCheckoutsStatus {
    #[must_use]
    pub fn gather(base_dir: PathBuf, roots: Vec<CheckoutInventoryRoot>) -> Self {
        let mut checkouts: Vec<_> = roots.into_iter().map(CheckoutInfraStatus::gather).collect();
        checkouts.sort_by(|left, right| left.cache_root.cmp(&right.cache_root));

        let totals = AllCheckoutsTotals {
            checkout_count: checkouts.len(),
            running_postgres: checkouts.iter().filter(|c| c.postgres.running).count(),
            stale_postgres_pid_files: checkouts
                .iter()
                .filter(|c| c.postgres.pid_state == ServicePidState::Stale)
                .count(),
            running_nats: checkouts.iter().filter(|c| c.nats.running).count(),
            stale_nats_pid_files: checkouts
                .iter()
                .filter(|c| c.nats.pid_state == ServicePidState::Stale)
                .count(),
            running_sinexd: checkouts.iter().filter(|c| c.sinexd.running).count(),
            rss_bytes: checkouts
                .iter()
                .map(|c| {
                    c.postgres.rss_bytes.unwrap_or(0)
                        + c.nats.rss_bytes.unwrap_or(0)
                        + c.sinexd.rss_bytes
                })
                .sum(),
            state_bytes: checkouts.iter().map(|c| c.total_state_bytes).sum(),
        };

        Self {
            base_dir,
            checkouts,
            totals,
            issues: Vec::new(),
        }
    }
}

impl AllCheckoutsCleanup {
    pub fn run(
        base_dir: PathBuf,
        roots: Vec<CheckoutInventoryRoot>,
        dry_run: bool,
        stale_only: bool,
    ) -> Result<Self> {
        let mut checkouts = Vec::new();
        let mut warnings = Vec::new();
        let mut totals = CleanupTotals {
            checkouts: roots.len(),
            ..CleanupTotals::default()
        };

        for root in roots {
            let cleanup = CheckoutCleanup::run(root, dry_run, stale_only)?;
            totals.actions += cleanup.actions.len();
            totals.skipped += cleanup.skipped.len();
            for action in &cleanup.actions {
                match action.action {
                    CleanupActionKind::StopPostgres => totals.stopped_postgres += 1,
                    CleanupActionKind::StopNats => totals.stopped_nats += 1,
                    CleanupActionKind::RemoveStalePostgresPid
                    | CleanupActionKind::RemoveMalformedPostgresPid
                    | CleanupActionKind::RemoveStaleNatsPid
                    | CleanupActionKind::RemoveMalformedNatsPid
                    | CleanupActionKind::RemoveStaleLock
                    | CleanupActionKind::RemoveMalformedLock => totals.removed_files += 1,
                }
            }
            warnings.extend(cleanup.skipped.iter().cloned());
            checkouts.push(cleanup);
        }

        checkouts.sort_by(|left, right| left.cache_root.cmp(&right.cache_root));
        Ok(Self {
            base_dir,
            dry_run,
            stale_only,
            checkouts,
            totals,
            warnings,
        })
    }
}

impl CheckoutCleanup {
    fn run(root: CheckoutInventoryRoot, dry_run: bool, stale_only: bool) -> Result<Self> {
        let config = StackConfig::from_inventory_root(&root);
        let status = CheckoutInfraStatus::gather(root.clone());
        let mut actions = Vec::new();
        let mut skipped = Vec::new();

        cleanup_lock(
            &config.state_dir.join(".lock"),
            &root.lock,
            dry_run,
            &mut actions,
        )?;
        cleanup_pid_file(
            &config.pg_pid_file(),
            status.postgres.pid_state,
            CleanupActionKind::RemoveStalePostgresPid,
            CleanupActionKind::RemoveMalformedPostgresPid,
            dry_run,
            &mut actions,
        )?;
        cleanup_pid_file(
            &config.nats_pid_file(),
            status.nats.pid_state,
            CleanupActionKind::RemoveStaleNatsPid,
            CleanupActionKind::RemoveMalformedNatsPid,
            dry_run,
            &mut actions,
        )?;

        if !stale_only {
            if let Some(pid) = status.postgres.pid
                && status.postgres.pid_state == ServicePidState::Running
            {
                if postgres_pid_is_dev_owned(pid, &config.pg_data()) {
                    if !dry_run {
                        pg_stop(&config, false)?;
                    }
                    actions.push(CleanupAction {
                        action: CleanupActionKind::StopPostgres,
                        target: config.pg_data(),
                        dry_run,
                    });
                } else {
                    skipped.push(format!(
                        "skipped postgres pid {pid}: /proc cmdline does not prove ownership of {}",
                        config.pg_data().display()
                    ));
                }
            }
            if let Some(pid) = status.nats.pid
                && status.nats.pid_state == ServicePidState::Running
            {
                if nats_pid_is_dev_owned(pid, &config.nats_config()) {
                    if !dry_run {
                        nats_stop(&config, false)?;
                    }
                    actions.push(CleanupAction {
                        action: CleanupActionKind::StopNats,
                        target: config.nats_config(),
                        dry_run,
                    });
                } else {
                    skipped.push(format!(
                        "skipped nats pid {pid}: /proc cmdline does not prove ownership of {}",
                        config.nats_config().display()
                    ));
                }
            }
        }

        Ok(Self {
            cache_root: status.cache_root,
            dev_state_dir: status.dev_state_dir,
            checkout_path: status.checkout_path,
            actions,
            skipped,
        })
    }
}

impl CheckoutInfraStatus {
    #[must_use]
    pub fn gather(root: CheckoutInventoryRoot) -> Self {
        let config = StackConfig::from_inventory_root(&root);
        let initialized =
            config.state_dir.exists() && (config.pg_data().exists() || config.nats_data().exists());

        let postgres = ServiceStatus {
            running: service_pid_state(&config.pg_pid_file()) == ServicePidState::Running,
            pid: read_pid_file(&config.pg_pid_file()).ok().flatten(),
            pid_state: service_pid_state(&config.pg_pid_file()),
            rss_bytes: service_rss_bytes(&config.pg_pid_file(), true),
            port: config.postgres.port,
        };
        let nats = ServiceStatus {
            running: service_pid_state(&config.nats_pid_file()) == ServicePidState::Running,
            pid: read_pid_file(&config.nats_pid_file()).ok().flatten(),
            pid_state: service_pid_state(&config.nats_pid_file()),
            rss_bytes: service_rss_bytes(&config.nats_pid_file(), false),
            port: config.nats.port,
        };

        let postgres_size = dir_size(&config.pg_data());
        let nats_size = dir_size(&config.nats_data());
        let annex_size = dir_size(&config.annex_data());
        let logs_size = dir_size(&config.logs_dir());
        let state_size = dir_size(&config.state_dir);
        let data_size_issues = [
            postgres_size.issue,
            nats_size.issue,
            annex_size.issue,
            logs_size.issue,
            state_size.issue,
        ]
        .into_iter()
        .flatten()
        .collect();

        let remediation = remediation_for_checkout(&root, &postgres, &nats);
        let sinexd = inspect_sinexd_processes(root.checkout_path.as_deref());

        Self {
            cache_root: root.cache_root,
            dev_state_dir: root.dev_state_dir,
            checkout_path_exists: root.checkout_path.as_ref().map(|path| path.exists()),
            checkout_path: root.checkout_path,
            lock: lock_status(root.lock),
            initialized,
            postgres,
            nats,
            sinexd,
            data_sizes: DataSizes {
                postgres_bytes: postgres_size.bytes,
                nats_bytes: nats_size.bytes,
                annex_bytes: annex_size.bytes,
            },
            logs_bytes: logs_size.bytes,
            total_state_bytes: state_size.bytes,
            data_size_issues,
            remediation,
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

const GIT_REPOSITORY_ENV_KEYS: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_COMMON_DIR",
    "GIT_INDEX_FILE",
    "GIT_NAMESPACE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CONFIG",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_PARAMETERS",
];

fn git_subprocess(program: &str) -> Command {
    let mut command = Command::new(program);
    // Git hooks export repository-scoped variables such as GIT_DIR. If xtask
    // inherits them while initializing the isolated annex store, git ignores
    // current_dir() and mutates the caller's repository metadata instead.
    for key in GIT_REPOSITORY_ENV_KEYS {
        command.env_remove(key);
    }
    command
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

    if !probe_annex_available(git_subprocess("git-annex").arg("version").output())? {
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
        git_subprocess("git")
            .args(["init"])
            .current_dir(config.annex_data())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output(),
    )?;

    require_successful_command(
        "git-annex init for annex repository",
        git_subprocess("git-annex")
            .args(["init", "sinex-dev-isolated"])
            .current_dir(config.annex_data())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output(),
    )?;

    require_successful_command(
        "git config annex.thin",
        git_subprocess("git")
            .args(["config", "annex.thin", "true"])
            .current_dir(config.annex_data())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output(),
    )?;

    require_successful_command(
        "git config annex.backend",
        git_subprocess("git")
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
    for role in SHARED_ACCESS_ROLES {
        mgr.ensure_role(role, false, false, &config.postgres.superuser)?;
    }
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
/// Runs on the current multithreaded runtime when available, otherwise falls back
/// to a dedicated current-thread runtime so tests and sync contexts behave the same.
pub fn apply_schema_for_database_url(database_url: &str, verbose: bool) -> Result<()> {
    if verbose {
        println!("Applying declarative database schema...");
    }

    let database_url = database_url.to_string();
    run_async_from_sync(async move {
        sinex_db::apply_schema_for_url(&database_url)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))
    })
    .context("Failed to apply declarative schema")?;

    if verbose {
        println!("Schema apply complete");
    }

    Ok(())
}

/// Synchronize discovered event payload schemas into the database.
///
/// Uses the same in-process schema registry inventory that event_engine uses at startup.
pub fn sync_event_payload_schemas_for_database_url(
    database_url: &str,
    verbose: bool,
) -> Result<SchemaSyncResult> {
    if verbose {
        println!("Synchronizing event payload schemas...");
    }

    let database_url = database_url.to_string();
    let result = run_async_from_sync(async move {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .wrap_err("Failed to connect for event payload schema synchronization")?;

        let repo = SchemaManagementRepository::new(&pool);
        let schema_bundle = generate_schema_bundle()
            .map_err(|error| color_eyre::eyre::eyre!("{error}"))
            .wrap_err("Failed to generate discovered event payload schema bundle")?;
        let result = repo
            .sync_schema_bundle(schema_bundle.into_entries())
            .await
            .wrap_err("Failed to synchronize discovered event payload schema bundle")?;
        pool.close().await;
        Ok::<_, color_eyre::Report>(result)
    })?;

    if verbose {
        println!(
            "Schema synchronization complete (discovered={}, created={}, updated={}, unchanged={})",
            result.discovered, result.created, result.updated, result.unchanged
        );
    }

    Ok(result)
}

/// Apply declarative database schema using the current stack configuration.
pub fn pg_apply_schema(config: &StackConfig, verbose: bool) -> Result<()> {
    apply_schema_for_database_url(&config.database_url(), verbose)
}

fn run_async_from_sync<F, T>(fut: F) -> Result<T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(fut))
        }
        Ok(_) => run_async_on_dedicated_thread(fut),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .wrap_err("failed to build runtime for stack operation")?
            .block_on(fut),
    }
}

fn run_async_on_dedicated_thread<F, T>(fut: F) -> Result<T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .wrap_err("failed to build dedicated runtime for stack operation")?
            .block_on(fut)
    })
    .join()
    .map_err(|payload| {
        let message = if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else if let Some(message) = payload.downcast_ref::<&'static str>() {
            (*message).to_string()
        } else {
            "non-string panic payload".to_string()
        };
        color_eyre::eyre::eyre!("stack operation thread panicked: {message}")
    })?
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

fn discover_nats_port(dev_state_dir: &Path) -> Option<u16> {
    let config = dev_state_dir.join("config/nats/nats.conf");
    let contents = fs::read_to_string(config).ok()?;
    contents.lines().find_map(|line| {
        let line = line.trim();
        let value = line.strip_prefix("port")?.trim_start();
        let value = value.strip_prefix('=')?.trim();
        value.parse().ok()
    })
}

fn read_pid_file(path: &Path) -> Result<Option<u32>> {
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read pid file {}", path.display()))?;
    let Some(line) = contents
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    else {
        bail!("pid file {} is empty", path.display());
    };
    let pid = line
        .parse::<u32>()
        .wrap_err_with(|| format!("failed to parse pid from {}", path.display()))?;
    Ok(Some(pid))
}

fn service_pid_state(path: &Path) -> ServicePidState {
    match read_pid_file(path) {
        Ok(Some(pid)) if pid_is_alive(pid) => ServicePidState::Running,
        Ok(Some(_)) => ServicePidState::Stale,
        Ok(None) => ServicePidState::Missing,
        Err(_) => ServicePidState::Malformed,
    }
}

fn service_rss_bytes(path: &Path, include_children: bool) -> Option<u64> {
    let pid = read_pid_file(path).ok().flatten()?;
    if !pid_is_alive(pid) {
        return None;
    }
    Some(if include_children {
        process_tree_rss_bytes(pid)
    } else {
        process_rss_bytes(pid).unwrap_or(0)
    })
}

fn pid_is_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

fn process_rss_bytes(pid: u32) -> Option<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?.trim();
        let kib = value.split_whitespace().next()?.parse::<u64>().ok()?;
        Some(kib * 1024)
    })
}

fn process_tree_rss_bytes(root_pid: u32) -> u64 {
    let children = proc_parent_map();
    let mut stack = vec![root_pid];
    let mut rss = 0;
    while let Some(pid) = stack.pop() {
        rss += process_rss_bytes(pid).unwrap_or(0);
        for child in children
            .iter()
            .filter_map(|(candidate, parent)| (*parent == pid).then_some(*candidate))
        {
            stack.push(child);
        }
    }
    rss
}

fn proc_parent_map() -> HashMap<u32, u32> {
    let mut parents = HashMap::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return parents;
    };
    for entry in entries.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        if let Some(ppid) = parse_proc_stat_ppid(&stat) {
            parents.insert(pid, ppid);
        }
    }
    parents
}

fn parse_proc_stat_ppid(stat: &str) -> Option<u32> {
    let close = stat.rfind(") ")?;
    let after = &stat[close + 2..];
    let mut fields = after.split_whitespace();
    let _state = fields.next()?;
    fields.next()?.parse().ok()
}

fn inspect_sinexd_processes(checkout_path: Option<&Path>) -> RuntimeProcessStatus {
    let Some(checkout_path) = checkout_path else {
        return RuntimeProcessStatus {
            running: false,
            pids: Vec::new(),
            rss_bytes: 0,
            issue: Some("checkout path unknown; cannot classify dev-local sinexd".to_string()),
        };
    };

    let mut pids = Vec::new();
    let mut issues = Vec::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return RuntimeProcessStatus {
            running: false,
            pids,
            rss_bytes: 0,
            issue: Some("failed to read /proc while inspecting sinexd".to_string()),
        };
    };

    for entry in entries.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let proc_dir = entry.path();
        if !proc_cmdline_contains(&proc_dir.join("cmdline"), "sinexd") {
            continue;
        }
        match fs::read_link(proc_dir.join("cwd")) {
            Ok(cwd) if cwd.starts_with(checkout_path) => pids.push(pid),
            Ok(_) => {}
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                issues.push(format!("failed to read cwd for pid {pid}: {error}"));
            }
            Err(_) => {}
        }
    }

    pids.sort_unstable();
    let rss_bytes = pids
        .iter()
        .map(|pid| process_rss_bytes(*pid).unwrap_or(0))
        .sum();

    RuntimeProcessStatus {
        running: !pids.is_empty(),
        pids,
        rss_bytes,
        issue: if issues.is_empty() {
            None
        } else {
            Some(issues.join("; "))
        },
    }
}

fn proc_cmdline_contains(path: &Path, needle: &str) -> bool {
    let Ok(mut file) = fs::File::open(path) else {
        return false;
    };
    let mut bytes = Vec::new();
    if file.read_to_end(&mut bytes).is_err() {
        return false;
    }
    bytes
        .split(|byte| *byte == 0)
        .filter_map(|part| std::str::from_utf8(part).ok())
        .any(|part| Path::new(part).file_name().and_then(|name| name.to_str()) == Some(needle))
}

fn lock_status(lock: LockInspection) -> LockStatus {
    match lock {
        LockInspection::Missing => LockStatus {
            present: false,
            state: LockState::Missing,
            pid: None,
            checkout_path: None,
            description: None,
            issue: None,
        },
        LockInspection::Live(info) => LockStatus {
            present: true,
            state: LockState::Live,
            pid: Some(info.pid),
            checkout_path: Some(info.checkout_path),
            description: info.description,
            issue: None,
        },
        LockInspection::Stale(info) => LockStatus {
            present: true,
            state: LockState::Stale,
            pid: Some(info.pid),
            checkout_path: Some(info.checkout_path),
            description: info.description,
            issue: Some(format!("lock pid {} is not running", info.pid)),
        },
        LockInspection::Malformed(issue) => LockStatus {
            present: true,
            state: LockState::Malformed,
            pid: None,
            checkout_path: None,
            description: None,
            issue: Some(issue),
        },
    }
}

fn remediation_for_checkout(
    root: &CheckoutInventoryRoot,
    postgres: &ServiceStatus,
    nats: &ServiceStatus,
) -> Vec<String> {
    let mut commands = Vec::new();
    if root
        .checkout_path
        .as_ref()
        .is_some_and(|path| path.exists())
    {
        if postgres.running || nats.running {
            if let Some(path) = &root.checkout_path {
                commands.push(format!("cd {} && xtask infra stop", path.display()));
            }
        }
    }
    if matches!(
        postgres.pid_state,
        ServicePidState::Stale | ServicePidState::Malformed
    ) {
        commands.push(format!(
            "rm {}",
            root.dev_state_dir
                .join("data/postgres/postmaster.pid")
                .display()
        ));
    }
    if matches!(
        nats.pid_state,
        ServicePidState::Stale | ServicePidState::Malformed
    ) {
        commands.push(format!(
            "rm {}",
            root.dev_state_dir.join("run/nats.pid").display()
        ));
    }
    commands
}

fn cleanup_lock(
    path: &Path,
    lock: &LockInspection,
    dry_run: bool,
    actions: &mut Vec<CleanupAction>,
) -> Result<()> {
    let action = match lock {
        LockInspection::Missing | LockInspection::Live(_) => return Ok(()),
        LockInspection::Stale(_) => CleanupActionKind::RemoveStaleLock,
        LockInspection::Malformed(_) => CleanupActionKind::RemoveMalformedLock,
    };
    remove_file_if_present(path, "checkout lock", dry_run)?;
    actions.push(CleanupAction {
        action,
        target: path.to_path_buf(),
        dry_run,
    });
    Ok(())
}

fn cleanup_pid_file(
    path: &Path,
    state: ServicePidState,
    stale_action: CleanupActionKind,
    malformed_action: CleanupActionKind,
    dry_run: bool,
    actions: &mut Vec<CleanupAction>,
) -> Result<()> {
    let action = match state {
        ServicePidState::Missing | ServicePidState::Running => return Ok(()),
        ServicePidState::Stale => stale_action,
        ServicePidState::Malformed => malformed_action,
    };
    remove_file_if_present(path, "pid file", dry_run)?;
    actions.push(CleanupAction {
        action,
        target: path.to_path_buf(),
        dry_run,
    });
    Ok(())
}

fn remove_file_if_present(path: &Path, label: &str, dry_run: bool) -> Result<()> {
    if dry_run || !path.exists() {
        return Ok(());
    }
    fs::remove_file(path).wrap_err_with(|| format!("failed to remove {label} {}", path.display()))
}

fn postgres_pid_is_dev_owned(pid: u32, data_dir: &Path) -> bool {
    let args = proc_cmdline_args(pid);
    args.iter().any(|arg| arg.ends_with("postgres"))
        && args.iter().any(|arg| Path::new(arg) == data_dir)
}

fn nats_pid_is_dev_owned(pid: u32, config_path: &Path) -> bool {
    let args = proc_cmdline_args(pid);
    args.iter().any(|arg| arg.ends_with("nats-server"))
        && args.iter().any(|arg| Path::new(arg) == config_path)
}

fn proc_cmdline_args(pid: u32) -> Vec<String> {
    let path = PathBuf::from(format!("/proc/{pid}/cmdline"));
    proc_cmdline_args_from_path(&path)
}

fn proc_cmdline_args_from_path(path: &Path) -> Vec<String> {
    let Ok(mut file) = fs::File::open(path) else {
        return Vec::new();
    };
    let mut bytes = Vec::new();
    if file.read_to_end(&mut bytes).is_err() {
        return Vec::new();
    }
    parse_cmdline_bytes(&bytes)
}

fn parse_cmdline_bytes(bytes: &[u8]) -> Vec<String> {
    bytes
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .filter_map(|part| std::str::from_utf8(part).ok())
        .map(str::to_string)
        .collect()
}

#[must_use]
pub fn dir_size(path: &Path) -> DirectorySizeProbe {
    if !path.exists() {
        return DirectorySizeProbe {
            bytes: 0,
            issue: None,
        };
    }
    if !path.is_dir() {
        return DirectorySizeProbe {
            bytes: 0,
            issue: Some(format!(
                "expected directory while sizing stack data path {}, found non-directory entry",
                path.display()
            )),
        };
    }

    let mut bytes = 0;
    let mut issues = Vec::new();
    for entry in walkdir::WalkDir::new(path) {
        match entry {
            Ok(entry) => match entry.metadata() {
                Ok(metadata) if metadata.is_file() => {
                    bytes += metadata.len();
                }
                Ok(_) => {}
                Err(error) => issues.push(format!(
                    "failed to read metadata while sizing {}: {error}",
                    entry.path().display()
                )),
            },
            Err(error) => issues.push(format!(
                "failed to walk stack data path {}: {error}",
                path.display()
            )),
        }
    }

    DirectorySizeProbe {
        bytes,
        issue: if issues.is_empty() {
            None
        } else {
            Some(issues.join("; "))
        },
    }
}

fn collect_snapshot_names<I>(dir: &Path, entries: I) -> SnapshotListProbe
where
    I: IntoIterator<Item = std::io::Result<std::ffi::OsString>>,
{
    let mut snapshots = Vec::new();
    let mut issues = Vec::new();

    for entry in entries {
        match entry {
            Ok(name) => {
                let Ok(name) = name.into_string() else {
                    issues.push(format!(
                        "failed to read snapshot entry in {}: entry name is not valid UTF-8",
                        dir.display()
                    ));
                    continue;
                };
                if name.ends_with(".tar.zst") {
                    snapshots.push(name.trim_end_matches(".tar.zst").to_string());
                } else if name.ends_with(".sql.zst") {
                    snapshots.push(name.trim_end_matches(".sql.zst").to_string());
                }
            }
            Err(error) => issues.push(format!(
                "failed to read snapshot entry in {}: {error}",
                dir.display()
            )),
        }
    }

    snapshots.sort();
    SnapshotListProbe {
        snapshots,
        issue: if issues.is_empty() {
            None
        } else {
            Some(issues.join("; "))
        },
    }
}

// Re-export list_snapshots if needed by commands (it was used in Status)
// or move it to crate::utils if it's generic enough. It seems specific to stack layout.
pub struct SnapshotListProbe {
    pub snapshots: Vec<String>,
    pub issue: Option<String>,
}

#[must_use]
pub fn list_snapshots(dir: &Path) -> SnapshotListProbe {
    if !dir.exists() {
        return SnapshotListProbe {
            snapshots: vec![],
            issue: None,
        };
    }
    match fs::read_dir(dir) {
        Ok(entries) => collect_snapshot_names(
            dir,
            entries.map(|entry| entry.map(|entry| entry.file_name())),
        ),
        Err(error) => SnapshotListProbe {
            snapshots: Vec::new(),
            issue: Some(format!(
                "failed to read snapshots directory {}: {error:#}",
                dir.display()
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::StackConfig;
    use super::{
        AllCheckoutsCleanup, AllCheckoutsStatus, CleanupActionKind, GIT_REPOSITORY_ENV_KEYS,
        collect_snapshot_names, dir_size, discover_nats_port, git_subprocess, list_snapshots,
        parse_cmdline_bytes, parse_proc_stat_ppid, probe_annex_available,
        require_successful_command, service_pid_state, sync_event_payload_schemas_for_database_url,
    };
    use crate::infra::state::{CheckoutInventoryRoot, LockInfo, LockInspection};
    use crate::sandbox::prelude::*;
    use sinex_primitives::temporal::Timestamp;
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::process::ExitStatusExt;
    use std::path::Path;

    #[test]
    fn nats_port_matches_flake_hash_for_sinex_checkout() {
        let checkout = Path::new("/realm/project/sinex");
        assert_eq!(StackConfig::port_offset_for_checkout(checkout), 86);
        assert_eq!(StackConfig::nats_port_for_checkout(checkout), 4308);
    }

    #[test]
    fn discover_nats_port_reads_generated_config() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let config_dir = temp.path().join("config/nats");
        fs::create_dir_all(&config_dir)?;
        fs::write(
            config_dir.join("nats.conf"),
            r#"
host = "127.0.0.1"
port = 4310
"#,
        )?;

        assert_eq!(discover_nats_port(temp.path()), Some(4310));
        Ok(())
    }

    #[test]
    fn service_pid_state_classifies_stale_pid_files() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let pid_file = temp.path().join("service.pid");
        fs::write(&pid_file, "999999999\n")?;

        assert_eq!(service_pid_state(&pid_file), super::ServicePidState::Stale);
        Ok(())
    }

    #[test]
    fn all_checkouts_status_totals_stale_pid_files_and_sizes() -> Result<()> {
        let base = tempfile::tempdir()?;
        let cache_root = base.path().join("hash123");
        let dev_state = cache_root.join("dev-state");
        fs::create_dir_all(dev_state.join("data/postgres"))?;
        fs::create_dir_all(dev_state.join("run"))?;
        fs::write(
            dev_state.join("data/postgres/postmaster.pid"),
            "999999999\n",
        )?;
        fs::write(dev_state.join("run/nats.pid"), "999999998\n")?;
        fs::write(dev_state.join("run/example.log"), "hello")?;

        let status = AllCheckoutsStatus::gather(
            base.path().to_path_buf(),
            vec![CheckoutInventoryRoot {
                cache_root,
                dev_state_dir: dev_state,
                checkout_path: None,
                lock: LockInspection::Missing,
            }],
        );

        assert_eq!(status.totals.checkout_count, 1);
        assert_eq!(status.totals.stale_postgres_pid_files, 1);
        assert_eq!(status.totals.stale_nats_pid_files, 1);
        assert!(status.totals.state_bytes >= 5);
        assert_eq!(
            status.checkouts[0].postgres.pid_state,
            super::ServicePidState::Stale
        );
        assert_eq!(
            status.checkouts[0].nats.pid_state,
            super::ServicePidState::Stale
        );
        assert!(!status.checkouts[0].remediation.is_empty());
        Ok(())
    }

    #[test]
    fn all_checkouts_cleanup_removes_stale_lock_and_pid_files() -> Result<()> {
        let base = tempfile::tempdir()?;
        let checkout = tempfile::tempdir()?;
        let cache_root = base.path().join("hash123");
        let dev_state = cache_root.join("dev-state");
        let pg_pid = dev_state.join("data/postgres/postmaster.pid");
        let nats_pid = dev_state.join("run/nats.pid");
        let lock_file = dev_state.join(".lock");
        fs::create_dir_all(pg_pid.parent().unwrap())?;
        fs::create_dir_all(nats_pid.parent().unwrap())?;
        fs::write(&pg_pid, "999999999\n")?;
        fs::write(&nats_pid, "999999998\n")?;
        fs::write(&lock_file, "{}")?;

        let cleanup = AllCheckoutsCleanup::run(
            base.path().to_path_buf(),
            vec![CheckoutInventoryRoot {
                cache_root,
                dev_state_dir: dev_state,
                checkout_path: Some(checkout.path().to_path_buf()),
                lock: LockInspection::Stale(LockInfo {
                    pid: 999_999_997,
                    checkout_path: checkout.path().to_path_buf(),
                    acquired_at: Timestamp::now(),
                    description: Some("test stale lock".to_string()),
                }),
            }],
            false,
            true,
        )?;

        assert!(!pg_pid.exists());
        assert!(!nats_pid.exists());
        assert!(!lock_file.exists());
        assert_eq!(cleanup.totals.removed_files, 3);
        assert!(
            cleanup.checkouts[0]
                .actions
                .iter()
                .any(|action| action.action == CleanupActionKind::RemoveStaleLock)
        );
        Ok(())
    }

    #[test]
    fn all_checkouts_cleanup_dry_run_leaves_stale_files() -> Result<()> {
        let base = tempfile::tempdir()?;
        let cache_root = base.path().join("hash123");
        let dev_state = cache_root.join("dev-state");
        let nats_pid = dev_state.join("run/nats.pid");
        fs::create_dir_all(nats_pid.parent().unwrap())?;
        fs::write(&nats_pid, "999999998\n")?;

        let cleanup = AllCheckoutsCleanup::run(
            base.path().to_path_buf(),
            vec![CheckoutInventoryRoot {
                cache_root,
                dev_state_dir: dev_state,
                checkout_path: None,
                lock: LockInspection::Missing,
            }],
            true,
            true,
        )?;

        assert!(nats_pid.exists());
        assert_eq!(cleanup.totals.removed_files, 1);
        assert!(cleanup.checkouts[0].actions[0].dry_run);
        Ok(())
    }

    #[test]
    fn parse_cmdline_bytes_ignores_empty_nul_segments() {
        assert_eq!(
            parse_cmdline_bytes(b"postgres\0-D\0/tmp/dev-state/data/postgres\0\0"),
            vec![
                "postgres".to_string(),
                "-D".to_string(),
                "/tmp/dev-state/data/postgres".to_string()
            ]
        );
    }

    #[test]
    fn parse_proc_stat_ppid_handles_comm_with_spaces() {
        assert_eq!(
            parse_proc_stat_ppid("123 (postgres: checkpointer) S 42 1 1 0"),
            Some(42)
        );
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

    #[test]
    fn annex_git_subprocess_clears_hook_repository_environment() {
        let command = git_subprocess("git");
        for key in GIT_REPOSITORY_ENV_KEYS {
            let is_removed = command
                .get_envs()
                .any(|(name, value)| name == *key && value.is_none());
            assert!(
                is_removed,
                "{key} must be removed so annex initialization cannot mutate the hook caller repo"
            );
        }
    }

    #[sinex_test]
    async fn list_snapshots_reports_directory_read_failures() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let not_a_dir = temp.path().join("snapshots");
        fs::write(&not_a_dir, "blocked")?;

        let probe = list_snapshots(&not_a_dir);
        assert!(probe.snapshots.is_empty());
        assert!(
            probe
                .issue
                .unwrap_or_default()
                .contains("failed to read snapshots directory")
        );
        Ok(())
    }

    #[sinex_test]
    async fn list_snapshots_collects_known_extensions_sorted() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        fs::write(temp.path().join("b.tar.zst"), "")?;
        fs::write(temp.path().join("a.sql.zst"), "")?;
        fs::write(temp.path().join("ignore.txt"), "")?;

        let probe = list_snapshots(temp.path());
        assert_eq!(probe.snapshots, vec!["a".to_string(), "b".to_string()]);
        assert!(probe.issue.is_none());
        Ok(())
    }

    #[test]
    fn collect_snapshot_names_reports_entry_failures_without_dropping_snapshots() {
        let probe = collect_snapshot_names(
            Path::new("/tmp/snapshots"),
            [
                Ok(OsString::from("b.tar.zst")),
                Err(std::io::Error::other("entry read failed")),
                Ok(OsString::from("a.sql.zst")),
                Ok(OsString::from("ignore.txt")),
            ],
        );

        assert_eq!(probe.snapshots, vec!["a".to_string(), "b".to_string()]);
        assert!(
            probe
                .issue
                .unwrap_or_default()
                .contains("failed to read snapshot entry")
        );
    }

    #[cfg(unix)]
    #[test]
    fn collect_snapshot_names_reports_non_utf8_entry_names() {
        use std::os::unix::ffi::OsStringExt;

        let probe = collect_snapshot_names(
            Path::new("/tmp/snapshots"),
            [
                Ok(OsString::from_vec(vec![
                    b'b', 0xff, b'.', b't', b'a', b'r', b'.', b'z', b's', b't',
                ])),
                Ok(OsString::from("a.sql.zst")),
            ],
        );

        assert_eq!(probe.snapshots, vec!["a".to_string()]);
        assert!(
            probe
                .issue
                .unwrap_or_default()
                .contains("entry name is not valid UTF-8")
        );
    }

    #[sinex_test]
    async fn dir_size_reports_non_directory_paths() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let file_path = temp.path().join("postgres");
        fs::write(&file_path, "blocked")?;

        let probe = dir_size(&file_path);
        assert_eq!(probe.bytes, 0);
        assert!(
            probe
                .issue
                .unwrap_or_default()
                .contains("expected directory while sizing stack data path")
        );
        Ok(())
    }

    #[sinex_test]
    async fn sync_event_payload_schemas_uses_in_process_registry(
        ctx: TestContext,
    ) -> TestResult<()> {
        let result = sync_event_payload_schemas_for_database_url(ctx.database_url(), false)?;
        assert!(result.discovered > 0);
        assert_eq!(
            result.discovered,
            result.created + result.updated + result.unchanged
        );
        Ok(())
    }
}
