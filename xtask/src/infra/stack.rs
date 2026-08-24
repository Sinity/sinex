//! Stack configuration and status tracking.

use color_eyre::eyre::{Result, WrapErr, bail};
use serde::{Deserialize, Serialize};
use sinex_db::repositories::schema_management::{SchemaManagementRepository, SchemaSyncResult};
use sinex_db::schema::apply::SHARED_ACCESS_ROLES;
use sinex_primitives::events::schema_registry::generate_schema_bundle;
use sqlx::postgres::PgPoolOptions;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use crate::infra::services::nats::{NatsConfig as SharedNatsConfig, NatsManager};
use crate::infra::services::postgres::{
    PostgresConfig as SharedPgConfig, PostgresDurabilityMode, PostgresManager,
};
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
        Self::from_state_dir(state.state_dir().to_path_buf())
    }

    fn from_state_dir(state_dir: PathBuf) -> Self {

        Self {
            state_dir,
            postgres: PostgresConfig {
                port: std::env::var("SINEX_DEV_POSTGRES_PORT")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .or_else(|| std::env::var("PGPORT").ok().and_then(|value| value.parse().ok()))
                    .unwrap_or(5432),
                database: "sinex_dev".to_string(),
                user: std::env::var("USER").unwrap_or_else(|_| "sinity".to_string()),
                superuser: "postgres".to_string(),
            },
            nats: NatsConfig {
                port: std::env::var("SINEX_DEV_NATS_PORT")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(4222),
                jetstream: true,
            },
            annex: AnnexConfig {
                enable: true,
                backend: "SHA256E".to_string(),
            },
        }
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
            // AgentCTL assigns a private loopback lease for this foreground job.
            listen_addresses: "127.0.0.1".to_string(),
            durability: PostgresDurabilityMode::Durable,
        }
    }

    #[must_use]
    pub fn to_shared_nats(&self) -> SharedNatsConfig {
        SharedNatsConfig {
            port: self.nats.port,
            config_file: self.nats_config(),
            data_dir: self.nats_data(),
            log_file: self.logs_dir().join("nats.log"),
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
