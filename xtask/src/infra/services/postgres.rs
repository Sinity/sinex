use color_eyre::eyre::{Result, WrapErr, bail};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub port: u16,
    pub data_dir: PathBuf,
    pub run_dir: PathBuf,
    pub logs_dir: PathBuf,
    pub database: String,
    pub superuser: String,
    pub app_user: String,
}

pub struct PostgresManager {
    config: PostgresConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostmasterPidState {
    Missing,
    Running(i32),
    Stale(i32),
}

fn remove_service_file(path: &Path, label: &str) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .wrap_err_with(|| format!("failed to remove {label} {}", path.display())),
    }
}

fn read_postmaster_pid(path: &Path) -> Result<Option<i32>> {
    if !path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read postmaster pid file {}", path.display()))?;
    let Some(first_line) = content.lines().next().map(str::trim).filter(|line| !line.is_empty())
    else {
        bail!("postmaster pid file {} is empty", path.display());
    };

    let pid = first_line
        .parse::<i32>()
        .wrap_err_with(|| format!("failed to parse postmaster pid from {}", path.display()))?;
    Ok(Some(pid))
}

impl PostgresManager {
    #[must_use]
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }

    pub fn init(&self, verbose: bool) -> Result<()> {
        if self.config.data_dir.join("PG_VERSION").exists() {
            if verbose {
                println!("PostgreSQL data directory already initialized");
            }
            return Ok(());
        }

        if verbose {
            println!("Initializing PostgreSQL data directory...");
        }

        fs::create_dir_all(&self.config.data_dir).context("failed to create data dir")?;
        fs::create_dir_all(&self.config.run_dir).context("failed to create run dir")?;
        fs::create_dir_all(&self.config.logs_dir).context("failed to create logs dir")?;

        let status = self
            .pg_command("initdb")
            .args([
                "--auth=trust",
                "--no-locale",
                "--encoding=UTF8",
                "-U",
                "postgres",
                "-D",
            ])
            .arg(&self.config.data_dir)
            .stdout(if verbose {
                Stdio::inherit()
            } else {
                Stdio::null()
            })
            .stderr(if verbose {
                Stdio::inherit()
            } else {
                Stdio::null()
            })
            .status()
            .context("Failed to run initdb")?;

        if !status.success() {
            bail!("initdb failed with status {status}");
        }

        let conf_path = self.config.data_dir.join("postgresql.conf");
        let mut conf = fs::OpenOptions::new()
            .append(true)
            .open(conf_path)
            .context("Failed to open postgresql.conf")?;

        writeln!(conf, "\n# sinex-dev configuration")?;
        writeln!(
            conf,
            "unix_socket_directories = '{}'",
            self.config.run_dir.display()
        )?;
        writeln!(conf, "listen_addresses = ''")?; // TCP disabled, use Unix sockets only
        writeln!(conf, "port = {}", self.config.port)?;
        writeln!(conf, "max_connections = 800")?;
        writeln!(conf, "shared_preload_libraries = 'timescaledb'")?;
        writeln!(conf, "log_destination = 'stderr'")?;
        writeln!(conf, "logging_collector = on")?;
        writeln!(conf, "log_directory = '{}'", self.config.logs_dir.display())?;
        writeln!(conf, "log_filename = 'postgres.log'")?;

        if verbose {
            println!("PostgreSQL initialized");
        }

        Ok(())
    }

    pub fn start(&self, verbose: bool) -> Result<()> {
        match self.postmaster_pid_state()? {
            PostmasterPidState::Missing => {}
            PostmasterPidState::Running(_) => {
                if self.is_accepting_connections() {
                    if verbose {
                        println!("PostgreSQL already running");
                    }
                    return Ok(());
                }

                eprintln!(
                    "⚠️  Stale PostgreSQL detected (PID alive but not accepting connections). Recovering..."
                );
                self.force_cleanup(verbose)?;
            }
            PostmasterPidState::Stale(pid) => {
                eprintln!("⚠️  Stale PostgreSQL pid file detected for dead PID {pid}. Recovering...");
                self.force_cleanup(verbose)?;
            }
        }

        if verbose {
            println!("Starting PostgreSQL on port {}...", self.config.port);
        }

        let log_path = self.config.logs_dir.join("postgres.log");

        let status = self
            .pg_command("pg_ctl")
            .args([
                "-D",
                self.config
                    .data_dir
                    .to_str()
                    .expect("data dir must be valid UTF-8"),
                "start",
                "-w",
            ])
            .arg("-l")
            .arg(&log_path)
            .arg("-o")
            .arg(format!(
                "-k {} -p {}",
                self.config.run_dir.display(),
                self.config.port
            ))
            .status()
            .context("Failed to start PostgreSQL")?;

        if !status.success() {
            bail!("pg_ctl start failed with status {status}");
        }

        // Wait for readiness
        for _ in 0..60 {
            let check = self
                .pg_command("pg_isready")
                .args([
                    "-h",
                    self.config
                        .run_dir
                        .to_str()
                        .expect("run dir must be valid UTF-8"),
                ])
                .arg("-p")
                .arg(self.config.port.to_string())
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

    pub fn stop(&self, verbose: bool) -> Result<()> {
        match self.postmaster_pid_state()? {
            PostmasterPidState::Missing => {
                if verbose {
                    println!("PostgreSQL not running");
                }
                return Ok(());
            }
            PostmasterPidState::Stale(pid) => {
                if verbose {
                    println!("Cleaning up stale PostgreSQL state for dead PID {pid}");
                }
                self.force_cleanup(verbose)?;
                return Ok(());
            }
            PostmasterPidState::Running(_) => {}
        }

        if verbose {
            println!("Stopping PostgreSQL...");
        }

        self.pg_command("pg_ctl")
            .args([
                "-D",
                self.config
                    .data_dir
                    .to_str()
                    .expect("data dir must be valid UTF-8"),
                "stop",
                "-m",
                "fast",
            ])
            .status()
            .context("pg_ctl stop failed")?;

        if verbose {
            println!("PostgreSQL stopped");
        }

        Ok(())
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        match self.postmaster_pid_state() {
            Ok(PostmasterPidState::Running(_)) => true,
            Ok(PostmasterPidState::Missing | PostmasterPidState::Stale(_)) => false,
            Err(error) => {
                tracing::warn!(path = %self.config.data_dir.join("postmaster.pid").display(), error = %error, "failed to inspect postgres pid file");
                false
            }
        }
    }

    #[must_use]
    pub fn read_pid(&self) -> Option<u32> {
        match read_postmaster_pid(&self.config.data_dir.join("postmaster.pid")) {
            Ok(Some(pid)) => Some(pid as u32),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(path = %self.config.data_dir.join("postmaster.pid").display(), error = %error, "failed to read postgres pid file");
                None
            }
        }
    }

    /// Check if PostgreSQL is accepting connections via pg_isready.
    pub fn is_accepting_connections(&self) -> bool {
        self.pg_command("pg_isready")
            .args([
                "-h",
                self.config
                    .run_dir
                    .to_str()
                    .expect("run dir must be valid UTF-8"),
            ])
            .arg("-p")
            .arg(self.config.port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    }

    /// Force-clean a stale PostgreSQL: kill the process, remove PID file and socket.
    fn force_cleanup(&self, verbose: bool) -> Result<()> {
        let pid_file = self.config.data_dir.join("postmaster.pid");
        if let Some(pid) = read_postmaster_pid(&pid_file)?
            && unsafe { libc::kill(pid, 0) == 0 }
        {
            if verbose {
                eprintln!("  Sending SIGKILL to stale PID {pid}");
            }
            unsafe { libc::kill(pid, libc::SIGKILL) };
            // Brief pause for kernel to reap
            std::thread::sleep(std::time::Duration::from_millis(500));
        }

        // Clean up stale files so pg_ctl start succeeds
        remove_service_file(&pid_file, "postgres pid file")?;
        let socket = self
            .config
            .run_dir
            .join(format!(".s.PGSQL.{}", self.config.port));
        let lock = self
            .config
            .run_dir
            .join(format!(".s.PGSQL.{}.lock", self.config.port));
        remove_service_file(&socket, "postgres socket")?;
        remove_service_file(&lock, "postgres socket lock")?;

        if verbose {
            eprintln!("  Cleaned up stale PID file and sockets");
        }

        Ok(())
    }

    pub fn psql(&self, user: &str, db: &str, sql: &str) -> Result<String> {
        let output = self
            .pg_command("psql")
            .args([
                "-h",
                self.config
                    .run_dir
                    .to_str()
                    .expect("run dir must be valid UTF-8"),
            ])
            .arg("-p")
            .arg(self.config.port.to_string())
            .args(["-U", user])
            .args(["-d", db])
            .args(["-tAc", sql])
            .output()
            .context("Failed to run psql")?;

        if !output.status.success() {
            bail!("psql failed: {}", String::from_utf8_lossy(&output.stderr));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    pub fn ensure_user(&self, user: &str, is_superuser: bool, creator: &str) -> Result<()> {
        let exists = self.psql(
            creator,
            "postgres",
            &format!("SELECT 1 FROM pg_roles WHERE rolname = '{user}'"),
        )?;
        if exists.is_empty() {
            let mut sql = format!("CREATE ROLE {user} LOGIN");
            if is_superuser {
                sql.push_str(" SUPERUSER CREATEDB");
            }
            self.psql(creator, "postgres", &sql)?;
        }
        Ok(())
    }

    pub fn drop_db(&self, db: &str, creator: &str) -> Result<()> {
        // WITH (FORCE) terminates any remaining connections before dropping (PG 13+)
        self.psql(
            creator,
            "postgres",
            &format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)"),
        )?;
        Ok(())
    }

    pub fn ensure_db(&self, db: &str, owner: &str, creator: &str) -> Result<()> {
        let exists = self.psql(
            creator,
            "postgres",
            &format!("SELECT 1 FROM pg_database WHERE datname = '{db}'"),
        )?;
        if exists.is_empty() {
            self.psql(
                creator,
                "postgres",
                &format!("CREATE DATABASE {db} OWNER {owner}"),
            )?;
        }
        Ok(())
    }

    pub fn install_extensions(&self, db: &str, superuser: &str) -> Result<()> {
        Self::install_extensions_with(superuser, db, |user, target_db, sql| {
            self.psql(user, target_db, sql)
        })
    }

    fn pg_command(&self, binary: &str) -> Command {
        if let Ok(prefix) = env::var("SINEX_PG_BIN") {
            let path = PathBuf::from(prefix).join(binary);
            Command::new(path)
        } else {
            Command::new(binary)
        }
    }

    fn postmaster_pid_state(&self) -> Result<PostmasterPidState> {
        let pid_file = self.config.data_dir.join("postmaster.pid");
        let Some(pid) = read_postmaster_pid(&pid_file)? else {
            return Ok(PostmasterPidState::Missing);
        };

        if unsafe { libc::kill(pid, 0) == 0 } {
            Ok(PostmasterPidState::Running(pid))
        } else {
            Ok(PostmasterPidState::Stale(pid))
        }
    }

    fn install_extensions_with<F>(superuser: &str, db: &str, mut psql: F) -> Result<()>
    where
        F: FnMut(&str, &str, &str) -> Result<String>,
    {
        for ext in &["timescaledb", "vector", "pg_jsonschema", "pg_trgm"] {
            let check = psql(
                superuser,
                db,
                &format!("SELECT 1 FROM pg_available_extensions WHERE name = '{ext}'"),
            )?;
            if !check.is_empty() {
                psql(
                    superuser,
                    db,
                    &format!("CREATE EXTENSION IF NOT EXISTS \"{ext}\" CASCADE"),
                )
                .wrap_err_with(|| format!("failed to install postgres extension {ext}"))?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    fn test_manager(root: &tempfile::TempDir) -> PostgresManager {
        PostgresManager::new(PostgresConfig {
            port: 55432,
            data_dir: root.path().join("data"),
            run_dir: root.path().join("run"),
            logs_dir: root.path().join("logs"),
            database: "sinex".to_string(),
            superuser: "postgres".to_string(),
            app_user: "sinex".to_string(),
        })
    }

    #[sinex_test]
    async fn test_postmaster_pid_state_reports_malformed_pid_file() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let manager = test_manager(&temp);
        fs::create_dir_all(&manager.config.data_dir)?;
        fs::write(manager.config.data_dir.join("postmaster.pid"), "not-a-pid\n")?;

        let error = manager.postmaster_pid_state().unwrap_err();
        assert!(format!("{error:#}").contains("failed to parse postmaster pid"));
        Ok(())
    }

    #[sinex_test]
    async fn test_force_cleanup_reports_socket_cleanup_failure() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let manager = test_manager(&temp);
        fs::create_dir_all(&manager.config.data_dir)?;
        fs::create_dir_all(&manager.config.run_dir)?;
        fs::write(manager.config.data_dir.join("postmaster.pid"), "999999\n")?;
        fs::create_dir(manager.config.run_dir.join(format!(".s.PGSQL.{}", manager.config.port)))?;

        let error = manager.force_cleanup(false).unwrap_err();
        assert!(format!("{error:#}").contains("failed to remove postgres socket"));
        Ok(())
    }

    #[sinex_test]
    async fn test_read_pid_returns_parsed_postmaster_pid() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let manager = test_manager(&temp);
        fs::create_dir_all(&manager.config.data_dir)?;
        fs::write(manager.config.data_dir.join("postmaster.pid"), "4321\n")?;

        assert_eq!(manager.read_pid(), Some(4321));
        Ok(())
    }

    #[sinex_test]
    async fn test_read_pid_returns_none_for_missing_postmaster_pid() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let manager = test_manager(&temp);

        assert_eq!(manager.read_pid(), None);
        Ok(())
    }

    #[sinex_test]
    async fn test_install_extensions_reports_create_failures() -> TestResult<()> {
        let error = PostgresManager::install_extensions_with("postgres", "sinex", |_, _, sql| {
            if sql.contains("SELECT 1 FROM pg_available_extensions") {
                Ok("1".to_string())
            } else {
                Err(color_eyre::eyre::eyre!("create extension failed"))
            }
        })
        .unwrap_err();

        assert!(format!("{error:#}").contains("failed to install postgres extension timescaledb"));
        Ok(())
    }

    #[sinex_test]
    async fn test_install_extensions_skips_unavailable_extensions() -> TestResult<()> {
        let mut statements = Vec::new();

        PostgresManager::install_extensions_with("postgres", "sinex", |_, _, sql| {
            statements.push(sql.to_string());
            if sql.contains("timescaledb") || sql.contains("pg_trgm") {
                Ok("1".to_string())
            } else {
                Ok(String::new())
            }
        })?;

        assert!(statements.iter().any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"timescaledb\" CASCADE"));
        assert!(statements.iter().any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"pg_trgm\" CASCADE"));
        assert!(!statements.iter().any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"vector\" CASCADE"));
        assert!(!statements.iter().any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"pg_jsonschema\" CASCADE"));
        Ok(())
    }
}
