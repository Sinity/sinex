//! A throwaway PostgreSQL cluster with the canonical schema, for tests and
//! scenarios. It lives in a temporary directory and is stopped and removed on drop.
//! It never fsyncs: on `/realm` a synced initdb alone takes a minute, and the
//! tests kill processes, not machines.
use crate::Result;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Cluster {
    dir: PathBuf,
    /// Short tmpfs path for the socket (Unix socket paths are limited to about
    /// 100 bytes) and for executables (the daemon hashes its own executable at
    /// startup, which is slow from a busy disk).
    fast: PathBuf,
    port: u16,
    admin: PathBuf,
    databases: AtomicUsize,
}

impl Cluster {
    /// Start a cluster, create the roles and a `sinex` database with the
    /// schema applied by `admin` (a `sinexd` binary, copied into the cluster dir).
    pub fn start_with_schema(admin: &Path) -> Result<Self> {
        let unique = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.subsec_nanos())
        );
        let dir = std::env::temp_dir().join(format!("sinex-pg-{unique}"));
        let tmpfs = if Path::new("/dev/shm").is_dir() {
            "/dev/shm"
        } else {
            "/tmp"
        };
        let fast = PathBuf::from(format!("{tmpfs}/sinex-pg-{unique}"));
        for path in [&dir, &fast] {
            std::fs::create_dir_all(path).map_err(|e| format!("{}: {e}", path.display()))?;
        }
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .map_err(|e| format!("pick a port: {e}"))?
            .port();
        let copied = fast.join("admin-sinexd");
        crate::copy_file(admin, &copied)?;
        let cluster = Self {
            dir,
            fast,
            port,
            admin: copied,
            databases: AtomicUsize::new(0),
        };
        let data = cluster.dir.join("data");
        cluster.pg(Command::new("initdb")
            .args([
                "--no-sync",
                "--no-locale",
                "--auth=trust",
                "--data-checksums",
                "-D",
            ])
            .arg(&data))?;
        let options = format!(
            "-k {} -c listen_addresses= -c shared_preload_libraries=timescaledb \
             -c timescaledb.telemetry_level=off -c timescaledb.enable_chunk_skipping=on \
             -c fsync=off -c synchronous_commit=off -c full_page_writes=off -p {}",
            cluster.socket().display(),
            port
        );
        cluster.pg(Command::new("pg_ctl")
            .arg("-D")
            .arg(&data)
            .arg("-l")
            .arg(cluster.dir.join("postgres.log"))
            .args(["-o", &options, "-w", "start"]))?;
        for role in ["sinex_writer", "sinex_reader"] {
            cluster.pg(Command::new("createuser").arg(role))?;
        }
        cluster.create_database("sinex")?;
        Ok(cluster)
    }

    /// A tmpfs directory removed with the cluster, for executables under test.
    pub fn fast_dir(&self) -> &Path {
        &self.fast
    }

    /// A new database with the schema applied, for one isolated test.
    pub fn fresh_database(&self) -> Result<String> {
        let name = format!("test_{}", self.databases.fetch_add(1, Ordering::Relaxed));
        self.create_database(&name)?;
        Ok(name)
    }

    fn create_database(&self, name: &str) -> Result {
        self.pg(Command::new("createdb").arg(name))?;
        let file = self.dir.join(format!("{name}.admin"));
        std::fs::write(&file, self.url("", name)).map_err(|e| e.to_string())?;
        self.pg(Command::new(&self.admin)
            .args(["admin", "schema", "--connection-file"])
            .arg(&file))
    }

    /// Environment for commands using database `name`: `DATABASE_URL` for
    /// SQLx, `SINEX_TEST_*` for database tests and scenarios, `PG*` for psql.
    pub fn env(&self, name: &str) -> Vec<(String, String)> {
        let store = self.dir.join(format!("{name}-store"));
        vec![
            ("DATABASE_URL".into(), self.url("", name)),
            ("SINEX_TEST_ADMIN_URL".into(), self.url("", name)),
            (
                "SINEX_TEST_WRITER_URL".into(),
                self.url("sinex_writer", name),
            ),
            (
                "SINEX_TEST_READER_URL".into(),
                self.url("sinex_reader", name),
            ),
            ("SINEX_TEST_STORE_DIR".into(), store.display().to_string()),
            ("PGHOST".into(), self.socket().display().to_string()),
            ("PGPORT".into(), self.port.to_string()),
        ]
    }

    fn url(&self, role: &str, database: &str) -> String {
        let user = if role.is_empty() {
            String::new()
        } else {
            format!("{role}@localhost")
        };
        format!(
            "postgresql://{user}/{database}?host={}&port={}",
            self.socket().display(),
            self.port
        )
    }

    fn socket(&self) -> &Path {
        &self.fast
    }

    fn pg(&self, command: &mut Command) -> Result {
        let shown = format!("{command:?}");
        let output = command
            .env("PGHOST", self.socket())
            .env("PGPORT", self.port.to_string())
            .env_remove("PGDATABASE")
            .env_remove("PGUSER")
            .output()
            .map_err(|e| format!("could not run {shown}: {e}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "{shown} failed: {}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new("pg_ctl")
            .arg("-D")
            .arg(self.dir.join("data"))
            .args(["-m", "immediate", "-w", "stop"])
            .output();
        let _ = std::fs::remove_dir_all(&self.dir);
        let _ = std::fs::remove_dir_all(&self.fast);
    }
}
