use color_eyre::eyre::{Result, WrapErr, bail};
use std::env;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
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
        if self.is_running() {
            // PID exists — but verify it's actually accepting connections
            if self.is_accepting_connections() {
                if verbose {
                    println!("PostgreSQL already running");
                }
                return Ok(());
            }
            // PID alive but not accepting connections — stale/zombie Postgres
            eprintln!(
                "⚠️  Stale PostgreSQL detected (PID alive but not accepting connections). Recovering..."
            );
            self.force_cleanup(verbose);
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
        if !self.is_running() {
            if verbose {
                println!("PostgreSQL not running");
            }
            return Ok(());
        }

        if verbose {
            println!("Stopping PostgreSQL...");
        }

        let _ = self
            .pg_command("pg_ctl")
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
            .status();

        if verbose {
            println!("PostgreSQL stopped");
        }

        Ok(())
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        let pid_file = self.config.data_dir.join("postmaster.pid");
        if let Ok(content) = fs::read_to_string(&pid_file)
            && let Some(first_line) = content.lines().next()
            && let Ok(pid) = first_line.parse::<i32>()
        {
            return unsafe { libc::kill(pid, 0) == 0 };
        }
        false
    }

    /// Check if PostgreSQL is accepting connections via pg_isready.
    fn is_accepting_connections(&self) -> bool {
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
    fn force_cleanup(&self, verbose: bool) {
        let pid_file = self.config.data_dir.join("postmaster.pid");
        if let Ok(content) = fs::read_to_string(&pid_file)
            && let Some(first_line) = content.lines().next()
            && let Ok(pid) = first_line.parse::<i32>()
        {
            if verbose {
                eprintln!("  Sending SIGKILL to stale PID {pid}");
            }
            unsafe { libc::kill(pid, libc::SIGKILL) };
            // Brief pause for kernel to reap
            std::thread::sleep(std::time::Duration::from_millis(500));
        }

        // Clean up stale files so pg_ctl start succeeds
        let _ = fs::remove_file(&pid_file);
        let socket = self
            .config
            .run_dir
            .join(format!(".s.PGSQL.{}", self.config.port));
        let lock = self
            .config
            .run_dir
            .join(format!(".s.PGSQL.{}.lock", self.config.port));
        let _ = fs::remove_file(&socket);
        let _ = fs::remove_file(&lock);

        if verbose {
            eprintln!("  Cleaned up stale PID file and sockets");
        }
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
        // Common extensions
        for ext in &["timescaledb", "vector", "pg_jsonschema", "pg_trgm"] {
            // Check availability first to avoid error spam if not installed in system
            let check = self.psql(
                superuser,
                db,
                &format!("SELECT 1 FROM pg_available_extensions WHERE name = '{ext}'"),
            )?;
            if !check.is_empty() {
                let _ = self.psql(
                    superuser,
                    db,
                    &format!("CREATE EXTENSION IF NOT EXISTS \"{ext}\" CASCADE"),
                );
            }
        }

        Ok(())
    }

    fn pg_command(&self, binary: &str) -> Command {
        if let Ok(prefix) = env::var("SINEX_PG_BIN") {
            let path = PathBuf::from(prefix).join(binary);
            Command::new(path)
        } else {
            Command::new(binary)
        }
    }
}
