use color_eyre::eyre::{eyre, Result, WrapErr};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use crate::infra::services::postgres::{PostgresConfig as SharedPgConfig, PostgresManager};

/// Configuration for an ephemeral Postgres instance
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub port: u16,
    pub data_dir: PathBuf,
    pub socket_dir: PathBuf,
    pub keep_data: bool,
    pub app_user: String,
    pub superuser: String,
    pub database: String,
    pub operation_id: String,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            port: 5433,
            data_dir: PathBuf::from(".sinex/ci-pgdata"),
            socket_dir: env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            keep_data: false,
            app_user: "sinex_app".to_string(),
            superuser: "sinex_superuser".to_string(),
            database: "sinex_dev".to_string(),
            operation_id: "default-op".to_string(),
        }
    }
}

/// RAII guard for Postgres instance cleanup
pub struct PgInstance {
    manager: PostgresManager,
}

impl Drop for PgInstance {
    fn drop(&mut self) {
        // Best effort stop
        let _ = self.manager.stop(false);
    }
}

/// Context for running queries against the instance
#[derive(Clone)]
pub struct PgEnv {
    pub host: String,
    pub port: u16,
    pub superuser: String,
    pub app_user: String,
    pub database: String,
    pub operation_id: String,
}

/// Setup an ephemeral Postgres instance
pub fn setup_ephemeral(config: &PostgresConfig) -> Result<(PgInstance, PgEnv)> {
    let host = "127.0.0.1".to_string();

    if config.data_dir.exists() && !config.keep_data {
        fs::remove_dir_all(&config.data_dir).context("failed to remove existing data dir")?;
    }

    // Construct shared config
    let shared_config = SharedPgConfig {
        port: config.port,
        data_dir: config.data_dir.clone(),
        run_dir: config.socket_dir.clone(),
        logs_dir: config.data_dir.clone(), // or a logs subdir? Default logic uses logs_dir
        database: config.database.clone(),
        superuser: config.superuser.clone(),
        app_user: config.app_user.clone(),
    };

    let manager = PostgresManager::new(shared_config.clone());

    // Init and start
    manager.init(false)?;
    manager.start(false)?;

    let pg_guard = PgInstance { manager };

    let env = PgEnv {
        host: host.clone(),
        port: config.port,
        superuser: config.superuser.clone(),
        app_user: config.app_user.clone(),
        database: config.database.clone(),
        operation_id: config.operation_id.clone(),
    };

    // Initialize roles and DB
    let mgr = &pg_guard.manager;
    let initial_user = env::var("USER").unwrap_or_else(|_| config.superuser.clone());

    mgr.ensure_user(&config.superuser, true, &initial_user)?;
    mgr.ensure_user(&config.app_user, true, &config.superuser)?;

    // Set operation ID default for the role
    let stmt = format!(
        "ALTER ROLE {} SET sinex.operation_id = '{}';",
        config.app_user, config.operation_id
    );
    mgr.psql(&config.superuser, "postgres", &stmt)?;

    mgr.ensure_db(&config.database, &config.app_user, &config.superuser)?;
    mgr.install_extensions(&config.database, &config.superuser)?;

    // Grant schema usage
    mgr.psql(
        &config.superuser,
        &config.database,
        &format!("GRANT ALL ON SCHEMA public TO {};", config.app_user),
    )?;

    Ok((pg_guard, env))
}

// Keeping this helper as it might be used by tests directly
pub fn psql(env: &PgEnv, user: &str, database: &str, sql: &str) -> Result<String> {
    let mut cmd = Command::new("psql");
    cmd.arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-h")
        .arg(&env.host)
        .arg("-p")
        .arg(env.port.to_string())
        .arg("-d")
        .arg(database)
        .arg("-tAc")
        .arg(sql)
        .env("PGUSER", user);

    let output = cmd
        .output()
        .with_context(|| format!("failed to run psql for query {sql}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(eyre!(
            "psql exited with status {} for query {sql}\n{}",
            output.status,
            stderr.trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}
