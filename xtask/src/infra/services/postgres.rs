use color_eyre::eyre::{Result, WrapErr, bail};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Validate a PostgreSQL identifier against the strict ASCII allowlist defined in
/// `sinex_primitives::validation::validate_pg_identifier`, adapted to `eyre::Result`.
pub(crate) fn validate_pg_identifier(ident: &str, kind: &str) -> Result<()> {
    sinex_primitives::validation::validate_pg_identifier(ident, kind)
        .map_err(|e| color_eyre::eyre::eyre!("{e}"))
}

fn pg_identifier(ident: &str, kind: &str) -> Result<String> {
    validate_pg_identifier(ident, kind)?;
    Ok(format!("\"{ident}\""))
}

fn pg_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

const MANAGED_CONFIG_BEGIN: &str = "# >>> sinex-dev managed configuration >>>";
const MANAGED_CONFIG_END: &str = "# <<< sinex-dev managed configuration <<<";
const LEGACY_CONFIG_MARKER: &str = "# sinex-dev configuration";
const TIMESCALEDB_MAX_BACKGROUND_WORKERS: u16 = 16;
const POSTGRES_WORKER_PROCESS_HEADROOM: u16 = 8;
const POSTGRES_MAX_WORKER_PROCESSES: u16 =
    TIMESCALEDB_MAX_BACKGROUND_WORKERS + POSTGRES_WORKER_PROCESS_HEADROOM;

#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub port: u16,
    pub data_dir: PathBuf,
    pub run_dir: PathBuf,
    pub logs_dir: PathBuf,
    pub database: String,
    pub superuser: String,
    pub app_user: String,
    /// `listen_addresses` value for postgresql.conf. Empty disables TCP and
    /// forces clients onto the Unix socket (the default for dev infra). CI /
    /// sandbox sets `"127.0.0.1"` so sqlx clients connecting via
    /// `postgresql://...@127.0.0.1:port/...` can reach the cluster.
    pub listen_addresses: String,
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
        Err(error) => {
            Err(error).wrap_err_with(|| format!("failed to remove {label} {}", path.display()))
        }
    }
}

fn read_postmaster_pid(path: &Path) -> Result<Option<i32>> {
    if !path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read postmaster pid file {}", path.display()))?;
    let Some(first_line) = content
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    else {
        bail!("postmaster pid file {} is empty", path.display());
    };

    let pid = first_line
        .parse::<i32>()
        .wrap_err_with(|| format!("failed to parse postmaster pid from {}", path.display()))?;
    Ok(Some(pid))
}

fn format_command_output(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => format!(" stdout: {stdout}"),
        (true, false) => format!(" stderr: {stderr}"),
        (false, false) => format!(" stdout: {stdout}; stderr: {stderr}"),
    }
}

/// Read the tail of the postgres log so failures surface the FATAL line that
/// pg_ctl's "stopped waiting" stderr message asks the operator to consult.
fn format_postgres_log_tail(log_path: &Path) -> String {
    const TAIL_LINES: usize = 40;
    match fs::read_to_string(log_path) {
        Ok(contents) if !contents.is_empty() => {
            let lines: Vec<&str> = contents.lines().collect();
            let start = lines.len().saturating_sub(TAIL_LINES);
            format!(
                "\n--- postgres.log tail ({} of {} lines) ---\n{}\n--- end postgres.log ---",
                lines.len() - start,
                lines.len(),
                lines[start..].join("\n")
            )
        }
        Ok(_) => format!(
            "\n(postgres log at {} is empty — no FATAL line was written before the failure)",
            log_path.display()
        ),
        Err(err) => format!(
            "\n(could not read postgres log at {}: {err})",
            log_path.display()
        ),
    }
}

/// Resolve `path` to an absolute form without requiring it to exist on disk.
/// `Path::canonicalize` requires existence and will fail when called before
/// initdb has populated the data directory. This helper falls back to joining
/// the current working directory.
fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    if let Ok(canonical) = path.canonicalize() {
        return Ok(canonical);
    }
    let cwd =
        env::current_dir().context("failed to read current dir while resolving postgres path")?;
    Ok(cwd.join(path))
}

fn utf8_path<'a>(path: &'a Path, label: &str) -> Result<&'a str> {
    path.to_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("{label} must be valid UTF-8: {}", path.display()))
}

impl PostgresManager {
    #[must_use]
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }

    pub fn init(&self, verbose: bool) -> Result<()> {
        fs::create_dir_all(&self.config.run_dir).context("failed to create run dir")?;
        fs::create_dir_all(&self.config.logs_dir).context("failed to create logs dir")?;

        if self.config.data_dir.join("PG_VERSION").exists() {
            self.ensure_runtime_config()?;
            if verbose {
                println!("PostgreSQL data directory already initialized");
            }
            return Ok(());
        }

        if verbose {
            println!("Initializing PostgreSQL data directory...");
        }

        fs::create_dir_all(&self.config.data_dir).context("failed to create data dir")?;
        let mut initdb = self.pg_command("initdb");
        initdb.args([
            "--auth=trust",
            "--no-locale",
            "--encoding=UTF8",
            "-U",
            "postgres",
            "-D",
        ]);
        initdb.arg(&self.config.data_dir);

        if verbose {
            let status = initdb
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
                .context("Failed to run initdb")?;
            if !status.success() {
                bail!("initdb failed with status {status}");
            }
        } else {
            let output = initdb
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .context("Failed to run initdb")?;
            if !output.status.success() {
                bail!(
                    "initdb failed with status {}{}",
                    output.status,
                    format_command_output(&output)
                );
            }
        }

        self.ensure_runtime_config()?;

        if verbose {
            println!("PostgreSQL initialized");
        }

        Ok(())
    }

    pub fn start(&self, verbose: bool) -> Result<()> {
        fs::create_dir_all(&self.config.run_dir).context("failed to create run dir")?;
        fs::create_dir_all(&self.config.logs_dir).context("failed to create logs dir")?;
        self.ensure_runtime_config()?;

        match self.postmaster_pid_state()? {
            PostmasterPidState::Missing => {}
            PostmasterPidState::Running(_) => {
                if self.accepting_connections_probe()? {
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
                eprintln!(
                    "⚠️  Stale PostgreSQL pid file detected for dead PID {pid}. Recovering..."
                );
                self.force_cleanup(verbose)?;
            }
        }

        if verbose {
            println!("Starting PostgreSQL on port {}...", self.config.port);
        }

        let log_path = self.config.logs_dir.join("postgres.log");

        let mut pg_ctl = self.pg_ctl_start_command(&log_path)?;

        if verbose {
            let status = pg_ctl
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
                .context("Failed to start PostgreSQL")?;
            if !status.success() {
                bail!("pg_ctl start failed with status {status}");
            }
        } else {
            let output = pg_ctl
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .context("Failed to start PostgreSQL")?;
            if !output.status.success() {
                bail!(
                    "pg_ctl start failed with status {}{}{}",
                    output.status,
                    format_command_output(&output),
                    format_postgres_log_tail(&log_path)
                );
            }
        }

        // Wait for readiness
        for _ in 0..60 {
            if self.accepting_connections_probe()? {
                if verbose {
                    println!("PostgreSQL started");
                }
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }

        bail!(
            "PostgreSQL failed to start within 30 seconds{}",
            format_postgres_log_tail(&log_path)
        )
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

        self.pg_ctl_stop_command()
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
    pub fn accepting_connections_probe(&self) -> Result<bool> {
        pg_isready_probe(self.pg_isready_command().output())
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
            .psql_command(user, db, sql)
            .output()
            .context("Failed to run psql")?;

        if !output.status.success() {
            bail!("psql failed: {}", String::from_utf8_lossy(&output.stderr));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    pub fn ensure_user(&self, user: &str, is_superuser: bool, creator: &str) -> Result<()> {
        Self::ensure_role_with(user, true, is_superuser, creator, |actor, db, sql| {
            self.psql(actor, db, sql)
        })
    }

    pub fn ensure_role(
        &self,
        role: &str,
        login: bool,
        is_superuser: bool,
        creator: &str,
    ) -> Result<()> {
        Self::ensure_role_with(role, login, is_superuser, creator, |actor, db, sql| {
            self.psql(actor, db, sql)
        })
    }

    pub fn drop_db(&self, db: &str, creator: &str) -> Result<()> {
        let db_ident = pg_identifier(db, "database")?;
        // WITH (FORCE) terminates any remaining connections before dropping (PG 13+)
        self.psql(
            creator,
            "postgres",
            &format!("DROP DATABASE IF EXISTS {db_ident} WITH (FORCE)"),
        )?;
        Ok(())
    }

    pub fn ensure_db(&self, db: &str, owner: &str, creator: &str) -> Result<()> {
        let db_ident = pg_identifier(db, "database")?;
        let owner_ident = pg_identifier(owner, "role")?;
        let db_literal = pg_literal(db);
        let exists = self.psql(
            creator,
            "postgres",
            &format!("SELECT 1 FROM pg_database WHERE datname = {db_literal}"),
        )?;
        if exists.is_empty() {
            self.psql(
                creator,
                "postgres",
                &format!("CREATE DATABASE {db_ident} OWNER {owner_ident}"),
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

    fn pg_ctl_start_command(&self, log_path: &Path) -> Result<Command> {
        let run_dir = utf8_path(&self.config.run_dir, "postgres run dir")?;
        let abs_log_path = absolute_path(log_path)?;
        let mut pg_ctl = self.pg_command("pg_ctl");
        pg_ctl
            .arg("-D")
            .arg(&self.config.data_dir)
            .arg("start")
            .arg("-w")
            .arg("-l")
            .arg(&abs_log_path)
            .arg("-o")
            .arg(format!("-k {run_dir} -p {}", self.config.port));
        Ok(pg_ctl)
    }

    fn pg_ctl_stop_command(&self) -> Command {
        let mut pg_ctl = self.pg_command("pg_ctl");
        pg_ctl
            .arg("-D")
            .arg(&self.config.data_dir)
            .arg("stop")
            .arg("-m")
            .arg("fast");
        pg_ctl
    }

    fn pg_isready_command(&self) -> Command {
        let mut cmd = self.pg_command("pg_isready");
        cmd.arg("-h")
            .arg(&self.config.run_dir)
            .arg("-p")
            .arg(self.config.port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        cmd
    }

    fn psql_command(&self, user: &str, db: &str, sql: &str) -> Command {
        let mut cmd = self.pg_command("psql");
        cmd.arg("-h")
            .arg(&self.config.run_dir)
            .arg("-p")
            .arg(self.config.port.to_string())
            .args(["-U", user])
            .args(["-d", db])
            .args(["-tAc", sql]);
        cmd
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
            let ext_ident = pg_identifier(ext, "extension")?;
            let ext_literal = pg_literal(ext);
            let check = psql(
                superuser,
                db,
                &format!("SELECT 1 FROM pg_available_extensions WHERE name = {ext_literal}"),
            )?;
            if !check.is_empty() {
                psql(
                    superuser,
                    db,
                    &format!("CREATE EXTENSION IF NOT EXISTS {ext_ident} CASCADE"),
                )
                .wrap_err_with(|| format!("failed to install postgres extension {ext}"))?;
            }
        }

        Ok(())
    }

    fn ensure_role_with<F>(
        role: &str,
        login: bool,
        is_superuser: bool,
        creator: &str,
        mut psql: F,
    ) -> Result<()>
    where
        F: FnMut(&str, &str, &str) -> Result<String>,
    {
        let role_ident = pg_identifier(role, "role")?;
        let role_literal = pg_literal(role);
        let exists = psql(
            creator,
            "postgres",
            &format!("SELECT 1 FROM pg_roles WHERE rolname = {role_literal}"),
        )?;
        if exists.is_empty() {
            let mut sql = format!("CREATE ROLE {role_ident}");
            if login {
                sql.push_str(" LOGIN");
            } else {
                sql.push_str(" NOLOGIN");
            }
            if is_superuser {
                sql.push_str(" SUPERUSER CREATEDB");
            }
            psql(creator, "postgres", &sql)?;
        }
        Ok(())
    }

    fn render_runtime_config(&self) -> Result<String> {
        // Postgres interprets relative `unix_socket_directories` and `log_directory`
        // against the cluster data directory, not the process cwd. A repo-relative
        // logs_dir like `.sinex/ci-pgdata` therefore double-nests as
        // `<data_dir>/.sinex/ci-pgdata`, which doesn't exist — the postmaster bails
        // with `could not open log file ... No such file or directory` and pg_ctl
        // reports only the unhelpful "stopped waiting" stderr. Resolve to absolute
        // before rendering postgresql.conf.
        let abs_run_dir = absolute_path(&self.config.run_dir)?;
        let abs_logs_dir = absolute_path(&self.config.logs_dir)?;
        let run_dir = utf8_path(&abs_run_dir, "postgres run dir")?;
        let logs_dir = utf8_path(&abs_logs_dir, "postgres logs dir")?;
        Ok(format!(
            "{MANAGED_CONFIG_BEGIN}
unix_socket_directories = '{}'
listen_addresses = '{}'
port = {}
max_connections = 800
max_worker_processes = {}
shared_preload_libraries = 'timescaledb'
timescaledb.max_background_workers = {}
log_destination = 'stderr'
logging_collector = on
log_directory = '{}'
log_filename = 'postgres.log'
{MANAGED_CONFIG_END}",
            run_dir,
            self.config.listen_addresses,
            self.config.port,
            POSTGRES_MAX_WORKER_PROCESSES,
            TIMESCALEDB_MAX_BACKGROUND_WORKERS,
            logs_dir
        ))
    }

    fn ensure_runtime_config(&self) -> Result<()> {
        let conf_path = self.config.data_dir.join("postgresql.conf");
        let existing = fs::read_to_string(&conf_path)
            .wrap_err_with(|| format!("failed to read {}", conf_path.display()))?;
        let managed_block = self.render_runtime_config()?;

        let updated = if let Some(start) = existing.find(MANAGED_CONFIG_BEGIN) {
            let end = existing
                .find(MANAGED_CONFIG_END)
                .map(|offset| offset + MANAGED_CONFIG_END.len())
                .ok_or_else(|| {
                    color_eyre::eyre::eyre!(
                        "managed postgres config marker is missing closing delimiter in {}",
                        conf_path.display()
                    )
                })?;
            format!(
                "{}{}{}",
                &existing[..start],
                managed_block,
                &existing[end..]
            )
        } else if let Some(start) = existing.find(LEGACY_CONFIG_MARKER) {
            let prefix = existing[..start].trim_end();
            if prefix.is_empty() {
                managed_block
            } else {
                format!("{prefix}\n\n{managed_block}")
            }
        } else {
            let prefix = existing.trim_end();
            if prefix.is_empty() {
                managed_block
            } else {
                format!("{prefix}\n\n{managed_block}")
            }
        };

        fs::write(&conf_path, format!("{updated}\n"))
            .wrap_err_with(|| format!("failed to write {}", conf_path.display()))?;
        Ok(())
    }
}

fn pg_isready_probe(output: std::io::Result<std::process::Output>) -> Result<bool> {
    let output = output.wrap_err("failed to run pg_isready")?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1 | 2) => Ok(false),
        _ => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let detail = stderr.trim();
            let suffix = if detail.is_empty() {
                String::new()
            } else {
                format!(" ({detail})")
            };
            bail!("pg_isready exited unexpectedly{suffix}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    #[cfg(unix)]
    use std::ffi::{OsStr, OsString};
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;

    fn test_manager(root: &tempfile::TempDir) -> PostgresManager {
        PostgresManager::new(PostgresConfig {
            port: 55432,
            data_dir: root.path().join("data"),
            run_dir: root.path().join("run"),
            logs_dir: root.path().join("logs"),
            database: "sinex".to_string(),
            superuser: "postgres".to_string(),
            app_user: "sinex".to_string(),
            listen_addresses: String::new(),
        })
    }

    #[sinex_test]
    async fn test_postmaster_pid_state_reports_malformed_pid_file() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let manager = test_manager(&temp);
        fs::create_dir_all(&manager.config.data_dir)?;
        fs::write(
            manager.config.data_dir.join("postmaster.pid"),
            "not-a-pid\n",
        )?;

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
        fs::create_dir(
            manager
                .config
                .run_dir
                .join(format!(".s.PGSQL.{}", manager.config.port)),
        )?;

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
    async fn postgres_identifier_rendering_rejects_sql_fragments() -> TestResult<()> {
        assert_eq!(
            pg_identifier("sinex_test_db", "database")?,
            "\"sinex_test_db\""
        );
        assert!(pg_identifier("sinex-test-db", "database").is_err());
        assert!(pg_identifier("sinex;DROP_DATABASE", "database").is_err());
        Ok(())
    }

    #[sinex_test]
    async fn postgres_literal_rendering_escapes_single_quotes() -> TestResult<()> {
        assert_eq!(pg_literal("sinex"), "'sinex'");
        assert_eq!(pg_literal("sin'ex"), "'sin''ex'");
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

        assert!(
            statements
                .iter()
                .any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"timescaledb\" CASCADE")
        );
        assert!(
            statements
                .iter()
                .any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"pg_trgm\" CASCADE")
        );
        assert!(
            !statements
                .iter()
                .any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"vector\" CASCADE")
        );
        assert!(
            !statements
                .iter()
                .any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"pg_jsonschema\" CASCADE")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_role_creates_nologin_role() -> TestResult<()> {
        let mut statements = Vec::new();

        PostgresManager::ensure_role_with(
            "sinex_gateway",
            false,
            false,
            "postgres",
            |actor, db, sql| {
                statements.push((actor.to_string(), db.to_string(), sql.to_string()));
                Ok(String::new())
            },
        )?;

        assert_eq!(
            statements,
            vec![
                (
                    "postgres".to_string(),
                    "postgres".to_string(),
                    "SELECT 1 FROM pg_roles WHERE rolname = 'sinex_gateway'".to_string(),
                ),
                (
                    "postgres".to_string(),
                    "postgres".to_string(),
                    "CREATE ROLE \"sinex_gateway\" NOLOGIN".to_string(),
                ),
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_role_skips_existing_role() -> TestResult<()> {
        let mut statements = Vec::new();

        PostgresManager::ensure_role_with(
            "sinex_readonly",
            false,
            false,
            "postgres",
            |actor, db, sql| {
                statements.push((actor.to_string(), db.to_string(), sql.to_string()));
                if sql.contains("SELECT 1 FROM pg_roles") {
                    Ok("1".to_string())
                } else {
                    Ok(String::new())
                }
            },
        )?;

        assert_eq!(
            statements,
            vec![(
                "postgres".to_string(),
                "postgres".to_string(),
                "SELECT 1 FROM pg_roles WHERE rolname = 'sinex_readonly'".to_string(),
            )]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_role_rejects_invalid_role_identifiers() -> TestResult<()> {
        let error = PostgresManager::ensure_role_with(
            "sinex;drop role postgres",
            false,
            false,
            "postgres",
            |_, _, _| Ok(String::new()),
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("invalid PostgreSQL role identifier"));
        Ok(())
    }

    #[sinex_test]
    async fn pg_isready_probe_reports_spawn_failures() -> TestResult<()> {
        let error =
            pg_isready_probe(Err(std::io::Error::other("pg_isready exploded"))).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("failed to run pg_isready"));
        assert!(message.contains("pg_isready exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn pg_isready_probe_treats_exit_two_as_not_accepting() -> TestResult<()> {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            let accepting = pg_isready_probe(Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(512),
                stdout: Vec::new(),
                stderr: Vec::new(),
            }))?;
            assert!(!accepting);
        }
        Ok(())
    }

    #[sinex_test]
    async fn pg_isready_probe_reports_unexpected_exit_failures() -> TestResult<()> {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            let error = pg_isready_probe(Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(768),
                stdout: Vec::new(),
                stderr: b"invalid option".to_vec(),
            }))
            .unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains("pg_isready exited unexpectedly"));
            assert!(message.contains("invalid option"));
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_runtime_config_rewrites_legacy_tail_block() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let manager = test_manager(&temp);
        fs::create_dir_all(&manager.config.data_dir)?;
        fs::write(
            manager.config.data_dir.join("postgresql.conf"),
            "shared_buffers = '128MB'\n\n# sinex-dev configuration\nport = 1111\n",
        )?;

        manager.ensure_runtime_config()?;

        let config = fs::read_to_string(manager.config.data_dir.join("postgresql.conf"))?;
        assert!(config.contains(MANAGED_CONFIG_BEGIN));
        assert!(config.contains(&format!(
            "max_worker_processes = {POSTGRES_MAX_WORKER_PROCESSES}"
        )));
        assert!(config.contains(&format!(
            "timescaledb.max_background_workers = {TIMESCALEDB_MAX_BACKGROUND_WORKERS}"
        )));
        assert!(!config.contains("port = 1111"));
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_runtime_config_replaces_existing_managed_block() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let manager = test_manager(&temp);
        fs::create_dir_all(&manager.config.data_dir)?;
        fs::write(
            manager.config.data_dir.join("postgresql.conf"),
            format!(
                "shared_buffers = '128MB'\n\n{MANAGED_CONFIG_BEGIN}\nport = 1111\n{MANAGED_CONFIG_END}\n"
            ),
        )?;

        manager.ensure_runtime_config()?;

        let config = fs::read_to_string(manager.config.data_dir.join("postgresql.conf"))?;
        assert_eq!(config.matches(MANAGED_CONFIG_BEGIN).count(), 1);
        assert!(config.contains("port = 55432"));
        assert!(!config.contains("port = 1111"));
        Ok(())
    }

    #[sinex_test]
    async fn test_postgres_worker_process_budget_leaves_headroom() -> TestResult<()> {
        const {
            assert!(POSTGRES_MAX_WORKER_PROCESSES > TIMESCALEDB_MAX_BACKGROUND_WORKERS);
        }
        assert_eq!(
            POSTGRES_MAX_WORKER_PROCESSES - TIMESCALEDB_MAX_BACKGROUND_WORKERS,
            POSTGRES_WORKER_PROCESS_HEADROOM
        );
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn test_pg_commands_preserve_non_utf8_paths() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let data_dir = PathBuf::from(OsString::from_vec(b"/tmp/pg-data-\xff".to_vec()));
        let run_dir = PathBuf::from(OsString::from_vec(b"/tmp/pg-run-\xfe".to_vec()));
        let manager = PostgresManager::new(PostgresConfig {
            port: 55432,
            data_dir: data_dir.clone(),
            run_dir: run_dir.clone(),
            logs_dir: temp.path().join("logs"),
            database: "sinex".to_string(),
            superuser: "postgres".to_string(),
            app_user: "sinex".to_string(),
            listen_addresses: String::new(),
        });

        let stop_args: Vec<OsString> = manager
            .pg_ctl_stop_command()
            .get_args()
            .map(OsStr::to_os_string)
            .collect();
        assert!(stop_args.iter().any(|arg| arg == data_dir.as_os_str()));

        let ready_args: Vec<OsString> = manager
            .pg_isready_command()
            .get_args()
            .map(OsStr::to_os_string)
            .collect();
        assert!(ready_args.iter().any(|arg| arg == run_dir.as_os_str()));

        let psql_args: Vec<OsString> = manager
            .psql_command("postgres", "sinex", "SELECT 1")
            .get_args()
            .map(OsStr::to_os_string)
            .collect();
        assert!(psql_args.iter().any(|arg| arg == run_dir.as_os_str()));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn test_pg_runtime_config_rejects_non_utf8_run_dir() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let manager = PostgresManager::new(PostgresConfig {
            port: 55432,
            data_dir: temp.path().join("data"),
            run_dir: PathBuf::from(OsString::from_vec(b"/tmp/pg-run-\xfe".to_vec())),
            logs_dir: temp.path().join("logs"),
            database: "sinex".to_string(),
            superuser: "postgres".to_string(),
            app_user: "sinex".to_string(),
            listen_addresses: String::new(),
        });

        let error = manager.render_runtime_config().unwrap_err();
        assert!(format!("{error:#}").contains("postgres run dir must be valid UTF-8"));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn test_pg_ctl_start_command_rejects_non_utf8_run_dir() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let manager = PostgresManager::new(PostgresConfig {
            port: 55432,
            data_dir: temp.path().join("data"),
            run_dir: PathBuf::from(OsString::from_vec(b"/tmp/pg-run-\xfe".to_vec())),
            logs_dir: temp.path().join("logs"),
            database: "sinex".to_string(),
            superuser: "postgres".to_string(),
            app_user: "sinex".to_string(),
            listen_addresses: String::new(),
        });

        let error = manager
            .pg_ctl_start_command(&temp.path().join("postgres.log"))
            .unwrap_err();
        assert!(format!("{error:#}").contains("postgres run dir must be valid UTF-8"));
        Ok(())
    }
}
